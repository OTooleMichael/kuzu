#include "processor/operator/persistent/reader/column_reader.h"

#include "common/exception/not_implemented.h"
#include "processor/operator/persistent/reader/struct_column_reader.h"
#include "processor/operator/persistent/reader/templated_column_reader.h"

namespace kuzu {
namespace processor {

using parquet::format::CompressionCodec;
using parquet::format::ConvertedType;
using parquet::format::Encoding;
using parquet::format::PageType;
using parquet::format::Type;

ColumnReader::ColumnReader(ParquetReader& reader, std::unique_ptr<common::LogicalType> type,
    const parquet::format::SchemaElement& schema, uint64_t fileIdx, uint64_t maxDefinition,
    uint64_t maxRepeat)
    : schema{schema}, file_idx{fileIdx}, max_define{maxDefinition},
      max_repeat{maxRepeat}, reader{reader}, type{std::move(type)}, page_rows_available{0} {}

std::unique_ptr<ColumnReader> ColumnReader::createReader(ParquetReader& reader,
    std::unique_ptr<common::LogicalType> type, const parquet::format::SchemaElement& schema,
    uint64_t fileIdx, uint64_t maxDefine, uint64_t maxRepeat) {
    switch (type->getLogicalTypeID()) {
    case common::LogicalTypeID::BOOL:
        assert(false);
        // return make_uniq<BooleanColumnReader>(reader, type, schema, fileIdx, maxDefine,
        // maxRepeat);
    case common::LogicalTypeID::INT16:
        return std::make_unique<
            TemplatedColumnReader<int8_t, TemplatedParquetValueConversion<int32_t>>>(
            reader, std::move(type), schema, fileIdx, maxDefine, maxRepeat);
    case common::LogicalTypeID::INT32:
        return std::make_unique<
            TemplatedColumnReader<int16_t, TemplatedParquetValueConversion<int32_t>>>(
            reader, std::move(type), schema, fileIdx, maxDefine, maxRepeat);
    case common::LogicalTypeID::INT64:
        return std::make_unique<
            TemplatedColumnReader<int32_t, TemplatedParquetValueConversion<int32_t>>>(
            reader, std::move(type), schema, fileIdx, maxDefine, maxRepeat);
    case common::LogicalTypeID::FLOAT:
        return std::make_unique<
            TemplatedColumnReader<float, TemplatedParquetValueConversion<float>>>(
            reader, std::move(type), schema, fileIdx, maxDefine, maxRepeat);
    case common::LogicalTypeID::DOUBLE:
        return std::make_unique<
            TemplatedColumnReader<double, TemplatedParquetValueConversion<double>>>(
            reader, std::move(type), schema, fileIdx, maxDefine, maxRepeat);
    case common::LogicalTypeID::DATE:
        assert(false);
        //        return std::make_unique<CallbackColumnReader<int32_t, date_t, ParquetIntToDate>>(
        //            reader, type, schema, fileIdx, maxDefine, maxRepeat);
    case common::LogicalTypeID::STRING:
        assert(false);
        //        return std::make_unique<StringColumnReader>(
        //            reader, type, schema, fileIdx, maxDefine, maxRepeat);
    default:
        throw common::NotImplementedException{"ColumnReader::createReader"};
    }
}

void ColumnReader::InitializeRead(uint64_t row_group_idx_p,
    const std::vector<parquet::format::ColumnChunk>& columns,
    apache::thrift::protocol::TProtocol& protocol_p) {
    assert(file_idx < columns.size());
    chunk = &columns[file_idx];
    protocol = &protocol_p;
    assert(chunk);
    assert(chunk->__isset.meta_data);

    if (chunk->__isset.file_path) {
        throw std::runtime_error("Only inlined data files are supported (no references)");
    }

    // ugh. sometimes there is an extra offset for the dict. sometimes it's wrong.
    chunk_read_offset = chunk->meta_data.data_page_offset;
    if (chunk->meta_data.__isset.dictionary_page_offset &&
        chunk->meta_data.dictionary_page_offset >= 4) {
        // this assumes the data pages follow the dict pages directly.
        chunk_read_offset = chunk->meta_data.dictionary_page_offset;
    }
    group_rows_available = chunk->meta_data.num_values;
}

uint64_t ColumnReader::TotalCompressedSize() {
    if (!chunk) {
        return 0;
    }

    return chunk->meta_data.total_compressed_size;
}

uint64_t ColumnReader::Read(uint64_t num_values, parquet_filter_t& filter, uint8_t* define_out,
    uint8_t* repeat_out, common::ValueVector* result) {
    // we need to reset the location because multiple column readers share the same protocol
    auto& trans = reinterpret_cast<ThriftFileTransport&>(*protocol->getTransport());
    trans.SetLocation(chunk_read_offset);

    // Perform any skips that were not applied yet.
    if (pending_skips > 0) {
        ApplyPendingSkips(pending_skips);
    }

    uint64_t result_offset = 0;
    auto to_read = num_values;

    while (to_read > 0) {
        while (page_rows_available == 0) {
            PrepareRead(filter);
        }

        assert(block);
        auto read_now = std::min<uint64_t>(to_read, page_rows_available);

        assert(read_now <= common::DEFAULT_VECTOR_CAPACITY);

        if (HasRepeats()) {
            assert(repeated_decoder);
            repeated_decoder->GetBatch<uint8_t>(repeat_out + result_offset, read_now);
        }

        if (HasDefines()) {
            assert(defined_decoder);
            defined_decoder->GetBatch<uint8_t>(define_out + result_offset, read_now);
        }

        uint64_t null_count = 0;

        if ((dict_decoder || dbp_decoder || rle_decoder) && HasDefines()) {
            // we need the null count because the dictionary offsets have no entries for nulls
            for (auto i = 0u; i < read_now; i++) {
                if (define_out[i + result_offset] != max_define) {
                    null_count++;
                }
            }
        }

        if (dict_decoder) {
            offset_buffer.resize(sizeof(uint32_t) * (read_now - null_count));
            dict_decoder->GetBatch<uint32_t>(offset_buffer.ptr, read_now - null_count);
            DictReference(result);
            Offsets(reinterpret_cast<uint32_t*>(offset_buffer.ptr), define_out, read_now, filter,
                result_offset, result);
        } else if (dbp_decoder) {
            // TODO keep this in the state
            auto read_buf = std::make_shared<ResizeableBuffer>();

            switch (type->getPhysicalType()) {
            case common::PhysicalTypeID::INT32:
                read_buf->resize(sizeof(int32_t) * (read_now - null_count));
                dbp_decoder->GetBatch<int32_t>(read_buf->ptr, read_now - null_count);

                break;
            case common::PhysicalTypeID::INT64:
                read_buf->resize(sizeof(int64_t) * (read_now - null_count));
                dbp_decoder->GetBatch<int64_t>(read_buf->ptr, read_now - null_count);
                break;

            default:
                throw std::runtime_error("DELTA_BINARY_PACKED should only be INT32 or INT64");
            }
            // Plain() will put NULLs in the right place
            Plain(read_buf, define_out, read_now, filter, result_offset, result);
        } else if (rle_decoder) {
            // RLE encoding for boolean
            assert(type->getLogicalTypeID() == common::LogicalTypeID::BOOL);
            auto read_buf = std::make_shared<ResizeableBuffer>();
            read_buf->resize(sizeof(bool) * (read_now - null_count));
            rle_decoder->GetBatch<uint8_t>(read_buf->ptr, read_now - null_count);
            PlainTemplated<bool, TemplatedParquetValueConversion<bool>>(
                read_buf, define_out, read_now, filter, result_offset, result);
        } else if (byte_array_data) {
            // DELTA_BYTE_ARRAY or DELTA_LENGTH_BYTE_ARRAY
            assert(false);
            // DeltaByteArray(define_out, read_now, filter, result_offset, result);
        } else {
            PlainReference(block, result);
            Plain(block, define_out, read_now, filter, result_offset, result);
        }

        result_offset += read_now;
        page_rows_available -= read_now;
        to_read -= read_now;
    }
    group_rows_available -= num_values;
    chunk_read_offset = trans.GetLocation();

    return num_values;
}

void ColumnReader::PrepareDataPage(parquet::format::PageHeader& page_hdr) {
    if (page_hdr.type == PageType::DATA_PAGE && !page_hdr.__isset.data_page_header) {
        throw std::runtime_error("Missing data page header from data page");
    }
    if (page_hdr.type == PageType::DATA_PAGE_V2 && !page_hdr.__isset.data_page_header_v2) {
        throw std::runtime_error("Missing data page header from data page v2");
    }

    bool is_v1 = page_hdr.type == PageType::DATA_PAGE;
    bool is_v2 = page_hdr.type == PageType::DATA_PAGE_V2;
    auto& v1_header = page_hdr.data_page_header;
    auto& v2_header = page_hdr.data_page_header_v2;

    page_rows_available = is_v1 ? v1_header.num_values : v2_header.num_values;
    auto page_encoding = is_v1 ? v1_header.encoding : v2_header.encoding;

    if (HasRepeats()) {
        uint32_t rep_length =
            is_v1 ? block->read<uint32_t>() : v2_header.repetition_levels_byte_length;
        block->available(rep_length);
        repeated_decoder = std::make_unique<RleBpDecoder>(
            block->ptr, rep_length, RleBpDecoder::ComputeBitWidth(max_repeat));
        block->inc(rep_length);
    } else if (is_v2 && v2_header.repetition_levels_byte_length > 0) {
        block->inc(v2_header.repetition_levels_byte_length);
    }

    if (HasDefines()) {
        uint32_t def_length =
            is_v1 ? block->read<uint32_t>() : v2_header.definition_levels_byte_length;
        block->available(def_length);
        defined_decoder = std::make_unique<RleBpDecoder>(
            block->ptr, def_length, RleBpDecoder::ComputeBitWidth(max_define));
        block->inc(def_length);
    } else if (is_v2 && v2_header.definition_levels_byte_length > 0) {
        block->inc(v2_header.definition_levels_byte_length);
    }

    switch (page_encoding) {
    case Encoding::RLE_DICTIONARY:
    case Encoding::PLAIN_DICTIONARY: {
        // where is it otherwise??
        auto dict_width = block->read<uint8_t>();
        // TODO somehow dict_width can be 0 ?
        dict_decoder = std::make_unique<RleBpDecoder>(block->ptr, block->len, dict_width);
        block->inc(block->len);
        break;
    }
    case Encoding::RLE: {
        if (type->getLogicalTypeID() != common::LogicalTypeID::BOOL) {
            throw std::runtime_error("RLE encoding is only supported for boolean data");
        }
        block->inc(sizeof(uint32_t));
        rle_decoder = std::make_unique<RleBpDecoder>(block->ptr, block->len, 1);
        break;
    }
    case Encoding::DELTA_BINARY_PACKED: {
        dbp_decoder = std::make_unique<DbpDecoder>(block->ptr, block->len);
        block->inc(block->len);
        break;
    }
    case Encoding::DELTA_LENGTH_BYTE_ARRAY: {
        assert(false);
        // PrepareDeltaLengthByteArray(*block);
        break;
    }
    case Encoding::DELTA_BYTE_ARRAY: {
        assert(false);
        // PrepareDeltaByteArray(*block);
        break;
    }
    case Encoding::PLAIN:
        // nothing to do here, will be read directly below
        break;

    default:
        throw std::runtime_error("Unsupported page encoding");
    }
}

void ColumnReader::PreparePage(parquet::format::PageHeader& page_hdr) {
    auto& trans = reinterpret_cast<ThriftFileTransport&>(*protocol->getTransport());

    AllocateBlock(page_hdr.uncompressed_page_size + 1);
    if (chunk->meta_data.codec == CompressionCodec::UNCOMPRESSED) {
        if (page_hdr.compressed_page_size != page_hdr.uncompressed_page_size) {
            throw std::runtime_error("Page size mismatch");
        }
        trans.read((uint8_t*)block->ptr, page_hdr.compressed_page_size);
        return;
    }

    AllocateCompressed(page_hdr.compressed_page_size + 1);
    trans.read((uint8_t*)compressed_buffer.ptr, page_hdr.compressed_page_size);

    DecompressInternal(chunk->meta_data.codec, compressed_buffer.ptr, page_hdr.compressed_page_size,
        block->ptr, page_hdr.uncompressed_page_size);
}

void ColumnReader::PrepareRead(parquet_filter_t& filter) {
    dict_decoder.reset();
    defined_decoder.reset();
    block.reset();
    parquet::format::PageHeader page_hdr;
    page_hdr.read(protocol);

    switch (page_hdr.type) {
    case PageType::DATA_PAGE_V2:
        PreparePageV2(page_hdr);
        PrepareDataPage(page_hdr);
        break;
    case PageType::DATA_PAGE:
        PreparePage(page_hdr);
        PrepareDataPage(page_hdr);
        break;
    case PageType::DICTIONARY_PAGE:
        PreparePage(page_hdr);
        Dictionary(std::move(block), page_hdr.dictionary_page_header.num_values);
        break;
    default:
        break; // ignore INDEX page type and any other custom extensions
    }
    ResetPage();
}

void ColumnReader::AllocateBlock(uint64_t size) {
    if (!block) {
        block = std::make_shared<ResizeableBuffer>(size);
    } else {
        block->resize(size);
    }
}

void ColumnReader::AllocateCompressed(uint64_t size) {
    compressed_buffer.resize(size);
}

void ColumnReader::PreparePageV2(parquet::format::PageHeader& page_hdr) {
    assert(page_hdr.type == PageType::DATA_PAGE_V2);

    auto& trans = reinterpret_cast<ThriftFileTransport&>(*protocol->getTransport());

    AllocateBlock(page_hdr.uncompressed_page_size + 1);
    bool uncompressed = false;
    if (page_hdr.data_page_header_v2.__isset.is_compressed &&
        !page_hdr.data_page_header_v2.is_compressed) {
        uncompressed = true;
    }
    if (chunk->meta_data.codec == CompressionCodec::UNCOMPRESSED) {
        if (page_hdr.compressed_page_size != page_hdr.uncompressed_page_size) {
            throw std::runtime_error("Page size mismatch");
        }
        uncompressed = true;
    }
    if (uncompressed) {
        trans.read(block->ptr, page_hdr.compressed_page_size);
        return;
    }

    // copy repeats & defines as-is because FOR SOME REASON they are uncompressed
    auto uncompressed_bytes = page_hdr.data_page_header_v2.repetition_levels_byte_length +
                              page_hdr.data_page_header_v2.definition_levels_byte_length;
    trans.read(block->ptr, uncompressed_bytes);

    auto compressed_bytes = page_hdr.compressed_page_size - uncompressed_bytes;

    AllocateCompressed(compressed_bytes);
    trans.read(compressed_buffer.ptr, compressed_bytes);

    DecompressInternal(chunk->meta_data.codec, compressed_buffer.ptr, compressed_bytes,
        block->ptr + uncompressed_bytes, page_hdr.uncompressed_page_size - uncompressed_bytes);
}

void ColumnReader::DecompressInternal(CompressionCodec::type codec, const uint8_t* src,
    uint64_t src_size, uint8_t* dst, uint64_t dst_size) {
    switch (codec) {
    case CompressionCodec::UNCOMPRESSED:
        throw common::CopyException("Parquet data unexpectedly uncompressed");
    case CompressionCodec::GZIP: {
        assert(false);
        //        MiniZStream s;
        //        s.Decompress(const_char_ptr_cast(src), src_size, char_ptr_cast(dst), dst_size);
        //        break;
    }
    case CompressionCodec::SNAPPY: {
        assert(false);
        //        {
        //            size_t uncompressed_size = 0;
        //            auto res = snappy::GetUncompressedLength(
        //                reinterpret_cast<const uint8_t*>(src), src_size, &uncompressed_size);
        //            if (!res) {
        //                throw std::runtime_error("Snappy decompression failure");
        //            }
        //            if (uncompressed_size != (size_t)dst_size) {
        //                throw std::runtime_error(
        //                    "Snappy decompression failure: Uncompressed data size mismatch");
        //            }
        //        }
        //        auto res =
        //            duckdb_snappy::RawUncompress(const_char_ptr_cast(src), src_size,
        //            char_ptr_cast(dst));
        //        if (!res) {
        //            throw std::runtime_error("Snappy decompression failure");
        //        }
        //        break;
    }
    case CompressionCodec::ZSTD: {
        assert(false);
        //        auto res = duckdb_zstd::ZSTD_decompress(dst, dst_size, src, src_size);
        //        if (duckdb_zstd::ZSTD_isError(res) || res != (size_t)dst_size) {
        //            throw std::runtime_error("ZSTD Decompression failure");
        //        }
        //        break;
    }
    default: {
        std::stringstream codec_name;
        codec_name << codec;
        throw std::runtime_error("Unsupported compression codec \"" + codec_name.str() +
                                 "\". Supported options are uncompressed, gzip, snappy or zstd");
    }
    }
}

void ColumnReader::ApplyPendingSkips(uint64_t num_values) {
    pending_skips -= num_values;

    dummy_define.zero();
    dummy_repeat.zero();

    // TODO this can be optimized, for example we dont actually have to bitunpack offsets
    auto dummy_result = std::make_unique<common::ValueVector>(*type);

    uint64_t remaining = num_values;
    uint64_t read = 0;

    while (remaining) {
        uint64_t to_read = std::min<uint64_t>(remaining, common::DEFAULT_VECTOR_CAPACITY);
        read += Read(to_read, none_filter, dummy_define.ptr, dummy_repeat.ptr, dummy_result.get());
        remaining -= to_read;
    }

    if (read != num_values) {
        throw std::runtime_error("Row count mismatch when skipping rows");
    }
}

StructColumnReader::StructColumnReader(ParquetReader& reader,
    std::unique_ptr<common::LogicalType> type_p, const parquet::format::SchemaElement& schema_p,
    uint64_t schema_idx_p, uint64_t max_define_p, uint64_t max_repeat_p,
    std::vector<std::unique_ptr<ColumnReader>> child_readers_p)
    : ColumnReader(reader, std::move(type_p), schema_p, schema_idx_p, max_define_p, max_repeat_p),
      child_readers(std::move(child_readers_p)) {
    assert(type->getPhysicalType() == common::PhysicalTypeID::STRUCT);
}

ColumnReader* StructColumnReader::getChildReader(uint64_t child_idx) {
    assert(child_idx < child_readers.size());
    return child_readers[child_idx].get();
}

void StructColumnReader::InitializeRead(uint64_t row_group_idx_p,
    const std::vector<parquet::format::ColumnChunk>& columns,
    apache::thrift::protocol::TProtocol& protocol_p) {
    for (auto& child : child_readers) {
        child->InitializeRead(row_group_idx_p, columns, protocol_p);
    }
}

uint64_t StructColumnReader::TotalCompressedSize() {
    uint64_t size = 0;
    for (auto& child : child_readers) {
        size += child->TotalCompressedSize();
    }
    return size;
}

const uint64_t ParquetDecodeUtils::BITPACK_MASKS[] = {0, 1, 3, 7, 15, 31, 63, 127, 255, 511, 1023,
    2047, 4095, 8191, 16383, 32767, 65535, 131071, 262143, 524287, 1048575, 2097151, 4194303,
    8388607, 16777215, 33554431, 67108863, 134217727, 268435455, 536870911, 1073741823, 2147483647,
    4294967295, 8589934591, 17179869183, 34359738367, 68719476735, 137438953471, 274877906943,
    549755813887, 1099511627775, 2199023255551, 4398046511103, 8796093022207, 17592186044415,
    35184372088831, 70368744177663, 140737488355327, 281474976710655, 562949953421311,
    1125899906842623, 2251799813685247, 4503599627370495, 9007199254740991, 18014398509481983,
    36028797018963967, 72057594037927935, 144115188075855871, 288230376151711743,
    576460752303423487, 1152921504606846975, 2305843009213693951, 4611686018427387903,
    9223372036854775807, 18446744073709551615ULL};

const uint64_t ParquetDecodeUtils::BITPACK_MASKS_SIZE =
    sizeof(ParquetDecodeUtils::BITPACK_MASKS) / sizeof(uint64_t);

const uint8_t ParquetDecodeUtils::BITPACK_DLEN = 8;

} // namespace processor
} // namespace kuzu
