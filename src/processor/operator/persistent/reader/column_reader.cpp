#include "processor/operator/persistent/reader/column_reader.h"

#include "common/exception/not_implemented.h"
#include "common/types/blob.h"
#include "common/types/date_t.h"
#include "processor/operator/persistent/reader/boolean_column_reader.h"
#include "processor/operator/persistent/reader/callback_column_reader.h"
#include "processor/operator/persistent/reader/list_column_reader.h"
#include "processor/operator/persistent/reader/string_column_reader.h"
#include "processor/operator/persistent/reader/struct_column_reader.h"
#include "processor/operator/persistent/reader/templated_column_reader.h"
#include "snappy/snappy.h"
#include "utf8proc_wrapper.h"

namespace kuzu {
namespace processor {

using kuzu_parquet::format::CompressionCodec;
using kuzu_parquet::format::ConvertedType;
using kuzu_parquet::format::Encoding;
using kuzu_parquet::format::PageType;
using kuzu_parquet::format::Type;

static common::date_t ParquetIntToDate(const int32_t& raw_date) {
    return common::date_t(raw_date);
}

ColumnReader::ColumnReader(ParquetReader& reader, std::unique_ptr<common::LogicalType> type,
    const kuzu_parquet::format::SchemaElement& schema, uint64_t fileIdx, uint64_t maxDefinition,
    uint64_t maxRepeat)
    : schema{schema}, file_idx{fileIdx}, max_define{maxDefinition},
      max_repeat{maxRepeat}, reader{reader}, type{std::move(type)}, page_rows_available{0} {}

std::unique_ptr<ColumnReader> ColumnReader::createReader(ParquetReader& reader,
    std::unique_ptr<common::LogicalType> type, const kuzu_parquet::format::SchemaElement& schema,
    uint64_t fileIdx, uint64_t maxDefine, uint64_t maxRepeat) {
    switch (type->getLogicalTypeID()) {
    case common::LogicalTypeID::BOOL:
        return std::make_unique<BooleanColumnReader>(
            reader, std::move(type), schema, fileIdx, maxDefine, maxRepeat);
    case common::LogicalTypeID::INT16:
        return std::make_unique<
            TemplatedColumnReader<int16_t, TemplatedParquetValueConversion<int32_t>>>(
            reader, std::move(type), schema, fileIdx, maxDefine, maxRepeat);
    case common::LogicalTypeID::INT32:
        return std::make_unique<
            TemplatedColumnReader<int32_t, TemplatedParquetValueConversion<int32_t>>>(
            reader, std::move(type), schema, fileIdx, maxDefine, maxRepeat);
    case common::LogicalTypeID::INT64:
        return std::make_unique<
            TemplatedColumnReader<int64_t, TemplatedParquetValueConversion<int64_t>>>(
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
        return std::make_unique<CallbackColumnReader<int32_t, common::date_t, ParquetIntToDate>>(
            reader, std::move(type), schema, fileIdx, maxDefine, maxRepeat);
    case common::LogicalTypeID::STRING:
        return std::make_unique<StringColumnReader>(
            reader, std::move(type), schema, fileIdx, maxDefine, maxRepeat);
    default:
        throw common::NotImplementedException{"ColumnReader::createReader"};
    }
}

void ColumnReader::InitializeRead(uint64_t row_group_idx_p,
    const std::vector<kuzu_parquet::format::ColumnChunk>& columns,
    kuzu_apache::thrift::protocol::TProtocol& protocol_p) {
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

void ColumnReader::PrepareDataPage(kuzu_parquet::format::PageHeader& page_hdr) {
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

void ColumnReader::PreparePage(kuzu_parquet::format::PageHeader& page_hdr) {
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
    kuzu_parquet::format::PageHeader page_hdr;
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

void ColumnReader::PreparePageV2(kuzu_parquet::format::PageHeader& page_hdr) {
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
        {
            size_t uncompressed_size = 0;
            auto res = kuzu_snappy::GetUncompressedLength(
                reinterpret_cast<const char*>(src), src_size, &uncompressed_size);
            if (!res) {
                throw std::runtime_error("Snappy decompression failure");
            }
            if (uncompressed_size != (size_t)dst_size) {
                throw std::runtime_error(
                    "Snappy decompression failure: Uncompressed data size mismatch");
            }
        }
        auto res = kuzu_snappy::RawUncompress(
            reinterpret_cast<const char*>(src), src_size, reinterpret_cast<char*>(dst));
        if (!res) {
            throw std::runtime_error("Snappy decompression failure");
        }
        break;
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
    std::unique_ptr<common::ValueVector> dummy_result =
        std::make_unique<common::ValueVector>(*type->copy());

    uint64_t remaining = num_values;
    uint64_t read = 0;

    while (remaining) {
        auto to_read = std::min<uint64_t>(remaining, common::DEFAULT_VECTOR_CAPACITY);
        read += Read(to_read, none_filter, dummy_define.ptr, dummy_repeat.ptr, dummy_result.get());
        remaining -= to_read;
    }

    if (read != num_values) {
        throw std::runtime_error("Row count mismatch when skipping rows");
    }
}

StructColumnReader::StructColumnReader(ParquetReader& reader,
    std::unique_ptr<common::LogicalType> type_p,
    const kuzu_parquet::format::SchemaElement& schema_p, uint64_t schema_idx_p,
    uint64_t max_define_p, uint64_t max_repeat_p,
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
    const std::vector<kuzu_parquet::format::ColumnChunk>& columns,
    kuzu_apache::thrift::protocol::TProtocol& protocol_p) {
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

StringColumnReader::StringColumnReader(ParquetReader& reader,
    std::unique_ptr<common::LogicalType> type_p,
    const kuzu_parquet::format::SchemaElement& schema_p, uint64_t schema_idx_p,
    uint64_t max_define_p, uint64_t max_repeat_p)
    : TemplatedColumnReader<common::ku_string_t, StringParquetValueConversion>(
          reader, std::move(type_p), schema_p, schema_idx_p, max_define_p, max_repeat_p) {
    fixed_width_string_length = 0;
    if (schema_p.type == Type::FIXED_LEN_BYTE_ARRAY) {
        assert(schema_p.__isset.type_length);
        fixed_width_string_length = schema_p.type_length;
    }
}

uint32_t StringColumnReader::VerifyString(
    const char* str_data, uint32_t str_len, const bool is_varchar) {
    if (!is_varchar) {
        return str_len;
    }
    // verify if a string is actually UTF8, and if there are no null bytes in the middle of the
    // string technically Parquet should guarantee this, but reality is often disappointing
    utf8proc::UnicodeInvalidReason reason;
    size_t pos;
    auto utf_type = utf8proc::Utf8Proc::analyze(str_data, str_len, &reason, &pos);
    if (utf_type == utf8proc::UnicodeType::INVALID) {
        throw common::CopyException{
            "Invalid string encoding found in Parquet file: value \"" +
            common::Blob::toString(reinterpret_cast<const uint8_t*>(str_data), str_len) +
            "\" is not valid UTF8!"};
    }
    return str_len;
}

uint32_t StringColumnReader::VerifyString(const char* str_data, uint32_t str_len) {
    return VerifyString(
        str_data, str_len, Type()->getLogicalTypeID() == common::LogicalTypeID::STRING);
}

void StringColumnReader::Dictionary(std::shared_ptr<ResizeableBuffer> data, uint64_t num_entries) {
    dict = std::move(data);
    dict_strings = std::unique_ptr<common::ku_string_t[]>(new common::ku_string_t[num_entries]);
    for (auto dict_idx = 0u; dict_idx < num_entries; dict_idx++) {
        uint32_t str_len;
        if (fixed_width_string_length == 0) {
            // variable length string: read from dictionary
            str_len = dict->read<uint32_t>();
        } else {
            // fixed length string
            str_len = fixed_width_string_length;
        }
        dict->available(str_len);

        auto dict_str = reinterpret_cast<const char*>(dict->ptr);
        auto actual_str_len = VerifyString(dict_str, str_len);
        dict_strings[dict_idx].set1(dict_str, actual_str_len);
        dict->inc(str_len);
    }
}

static std::shared_ptr<ResizeableBuffer> ReadDbpData(
    ResizeableBuffer& buffer, uint64_t& value_count) {
    auto decoder = std::make_unique<DbpDecoder>(buffer.ptr, buffer.len);
    value_count = decoder->TotalValues();
    auto result = std::make_shared<ResizeableBuffer>();
    result->resize(sizeof(uint32_t) * value_count);
    decoder->GetBatch<uint32_t>(result->ptr, value_count);
    decoder->Finalize();
    buffer.inc(buffer.len - decoder->BufferPtr().len);
    return result;
}

// void StringColumnReader::PrepareDeltaLengthByteArray(ResizeableBuffer& buffer) {
//    uint64_t value_count;
//    auto length_buffer = ReadDbpData(buffer, value_count);
//    if (value_count == 0) {
//        // no values
//        byte_array_data =
//            std::make_unique<common::ValueVector>(common::LogicalTypeID::STRING, nullptr);
//        return;
//    }
//    auto length_data = reinterpret_cast<uint32_t*>(length_buffer->ptr);
//    byte_array_data =
//        std::make_unique<common::ValueVector>(common::LogicalTypeID::STRING, value_count);
//    byte_array_count = value_count;
//    delta_offset = 0;
//    auto string_data = reinterpret_cast<common::ku_string_t*>(byte_array_data->getData());
//    for (idx_t i = 0; i < value_count; i++) {
//        auto str_len = length_data[i];
//        string_data[i] = StringVector::EmptyString(*byte_array_data, str_len);
//        auto result_data = string_data[i].GetDataWriteable();
//        memcpy(result_data, buffer.ptr, length_data[i]);
//        buffer.inc(length_data[i]);
//        string_data[i].Finalize();
//    }
//}
//
// void StringColumnReader::PrepareDeltaByteArray(ResizeableBuffer& buffer) {
//    uint64_t prefix_count, suffix_count;
//    auto prefix_buffer = ReadDbpData(buffer, prefix_count);
//    auto suffix_buffer = ReadDbpData(buffer, suffix_count);
//    if (prefix_count != suffix_count) {
//        throw std::runtime_error(
//            "DELTA_BYTE_ARRAY - prefix and suffix counts are different - corrupt file?");
//    }
//    if (prefix_count == 0) {
//        // no values
//        byte_array_data = make_uniq<Vector>(LogicalType::VARCHAR, nullptr);
//        return;
//    }
//    auto prefix_data = reinterpret_cast<uint32_t*>(prefix_buffer->ptr);
//    auto suffix_data = reinterpret_cast<uint32_t*>(suffix_buffer->ptr);
//    byte_array_data = make_uniq<Vector>(LogicalType::VARCHAR, prefix_count);
//    byte_array_count = prefix_count;
//    delta_offset = 0;
//    auto string_data = reinterpret_cast<common::ku_string_t*>(byte_array_data->getData());
//    for (auto i = 0u; i < prefix_count; i++) {
//        auto str_len = prefix_data[i] + suffix_data[i];
//        string_data[i] = StringVector::EmptyString(*byte_array_data, str_len);
//        auto result_data = string_data[i].GetDataWriteable();
//        if (prefix_data[i] > 0) {
//            if (i == 0 || prefix_data[i] > string_data[i - 1].len) {
//                throw std::runtime_error(
//                    "DELTA_BYTE_ARRAY - prefix is out of range - corrupt file?");
//            }
//            memcpy(result_data, string_data[i - 1].getData(), prefix_data[i]);
//        }
//        memcpy(result_data + prefix_data[i], buffer.ptr, suffix_data[i]);
//        buffer.inc(suffix_data[i]);
//        string_data[i].Finalize();
//    }
//}
//
// void StringColumnReader::DeltaByteArray(uint8_t* defines, uint64_t num_values,
//    parquet_filter_t& filter, uint64_t result_offset, common::ValueVector* result) {
//    if (!byte_array_data) {
//        throw std::runtime_error(
//            "Internal error - DeltaByteArray called but there was no byte_array_data set");
//    }
//    auto result_ptr = reinterpret_cast<common::ku_string_t*>(result->getData());
//    auto string_data = reinterpret_cast<common::ku_string_t*>(byte_array_data->getData());
//    for (auto row_idx = 0u; row_idx < num_values; row_idx++) {
//        if (HasDefines() && defines[row_idx + result_offset] != max_define) {
//            result->setNull(row_idx + result_offset, true);
//            continue;
//        }
//        if (filter[row_idx + result_offset]) {
//            if (delta_offset >= byte_array_count) {
//                throw common::CopyException{common::StringUtils::string_format(
//                    "DELTA_BYTE_ARRAY - length mismatch between values and byte "
//                    "array lengths (attempted "
//                    "read of {} from {} entries) - corrupt file?",
//                    delta_offset + 1, byte_array_count)};
//            }
//            result_ptr[row_idx + result_offset] = string_data[delta_offset++];
//        } else {
//            delta_offset++;
//        }
//    }
//    StringVector::AddHeapReference(result, *byte_array_data);
//}

// class ParquetStringVectorBuffer : public VectorBuffer {
// public:
//    explicit ParquetStringVectorBuffer(std::shared_ptr<ByteBuffer> buffer_p)
//        : VectorBuffer(VectorBufferType::OPAQUE_BUFFER), buffer(std::move(buffer_p)) {}
//
// private:
//    std::shared_ptr<ByteBuffer> buffer;
//};

// void StringColumnReader::PlainReference(
//    std::shared_ptr<ByteBuffer> plain_data, common::ValueVector* result) {
//    StringVector::AddBuffer(result,
//    make_buffer<ParquetStringVectorBuffer>(std::move(plain_data)));
//}

common::ku_string_t StringParquetValueConversion::DictRead(
    ByteBuffer& dict, uint32_t& offset, ColumnReader& reader) {
    auto& dict_strings = reinterpret_cast<StringColumnReader&>(reader).dict_strings;
    return dict_strings[offset];
}

common::ku_string_t StringParquetValueConversion::PlainRead(
    ByteBuffer& plain_data, ColumnReader& reader) {
    auto& scr = reinterpret_cast<StringColumnReader&>(reader);
    uint32_t str_len = scr.fixed_width_string_length == 0 ? plain_data.read<uint32_t>() :
                                                            scr.fixed_width_string_length;
    plain_data.available(str_len);
    auto plain_str = reinterpret_cast<char*>(plain_data.ptr);
    auto actual_str_len =
        reinterpret_cast<StringColumnReader&>(reader).VerifyString(plain_str, str_len);
    auto ret_str = common::ku_string_t();
    ret_str.set(plain_str, actual_str_len);
    plain_data.inc(str_len);
    return ret_str;
}

void StringParquetValueConversion::PlainSkip(ByteBuffer& plain_data, ColumnReader& reader) {
    auto& scr = reinterpret_cast<StringColumnReader&>(reader);
    uint32_t str_len = scr.fixed_width_string_length == 0 ? plain_data.read<uint32_t>() :
                                                            scr.fixed_width_string_length;
    plain_data.inc(str_len);
}

ListColumnReader::ListColumnReader(ParquetReader& reader,
    std::unique_ptr<common::LogicalType> type_p,
    const kuzu_parquet::format::SchemaElement& schema_p, uint64_t schema_idx_p,
    uint64_t max_define_p, uint64_t max_repeat_p,
    std::unique_ptr<ColumnReader> child_column_reader_p)
    : ColumnReader(reader, std::move(type_p), schema_p, schema_idx_p, max_define_p, max_repeat_p),
      child_column_reader(std::move(child_column_reader_p)),
      //      read_cache(common::VarListType::getChildType(type.get())), read_vector(read_cache),
      overflow_child_count(0) {

    child_defines.resize(common::DEFAULT_VECTOR_CAPACITY);
    child_repeats.resize(common::DEFAULT_VECTOR_CAPACITY);
    child_defines_ptr = (uint8_t*)child_defines.ptr;
    child_repeats_ptr = (uint8_t*)child_repeats.ptr;

    child_filter.set();
}

void ListColumnReader::ApplyPendingSkips(uint64_t num_values) {
    pending_skips -= num_values;

    auto define_out = std::unique_ptr<uint8_t[]>(new uint8_t[num_values]);
    auto repeat_out = std::unique_ptr<uint8_t[]>(new uint8_t[num_values]);

    uint64_t remaining = num_values;
    uint64_t read = 0;

    while (remaining) {
        auto result_out = std::make_unique<common::ValueVector>(*type);
        parquet_filter_t filter;
        auto to_read = std::min<uint64_t>(remaining, common::DEFAULT_VECTOR_CAPACITY);
        read += Read(to_read, filter, define_out.get(), repeat_out.get(), result_out.get());
        remaining -= to_read;
    }

    if (read != num_values) {
        throw common::CopyException("Not all skips done!");
    }
}

uint64_t ListColumnReader::Read(uint64_t num_values, parquet_filter_t& filter, uint8_t* define_out,
    uint8_t* repeat_out, common::ValueVector* result_out) {
    common::offset_t result_offset = 0;
    auto result_ptr = reinterpret_cast<common::list_entry_t*>(result_out->getData());

    if (pending_skips > 0) {
        ApplyPendingSkips(pending_skips);
    }

    // if an individual list is longer than STANDARD_VECTOR_SIZE we actually have to loop the child
    // read to fill it
    bool finished = false;
    while (!finished) {
        uint64_t child_actual_num_values = 0;

        // check if we have any overflow from a previous read
        if (overflow_child_count == 0) {
            // we don't: read elements from the child reader
            child_defines.zero();
            child_repeats.zero();
            // we don't know in advance how many values to read because of the beautiful
            // repetition/definition setup we just read (up to) a vector from the child column, and
            // see if we have read enough if we have not read enough, we read another vector if we
            // have read enough, we leave any unhandled elements in the overflow vector for a
            // subsequent read
            auto child_req_num_values = std::min<uint64_t>(
                common::DEFAULT_VECTOR_CAPACITY, child_column_reader->GroupRowsAvailable());
            // read_vector.ResetFromCache(read_cache);
            child_actual_num_values =
                child_column_reader->Read(child_req_num_values, child_filter, child_defines_ptr,
                    child_repeats_ptr, common::ListVector::getDataVector(result_out));
        } else {
            // we do: use the overflow values
            child_actual_num_values = overflow_child_count;
            overflow_child_count = 0;
        }

        if (child_actual_num_values == 0) {
            // no more elements available: we are done
            break;
        }
        // read_vector.Verify(child_actual_num_values);
        uint64_t current_chunk_offset = common::ListVector::getDataVectorSize(result_out);

        // hard-won piece of code this, modify at your own risk
        // the intuition is that we have to only collapse values into lists that are repeated *on
        // this level* the rest is pretty much handed up as-is as a single-valued list or NULL
        uint64_t child_idx;
        for (child_idx = 0; child_idx < child_actual_num_values; child_idx++) {
            if (child_repeats_ptr[child_idx] == max_repeat) {
                // value repeats on this level, append
                assert(result_offset > 0);
                result_ptr[result_offset - 1].size++;
                continue;
            }

            if (result_offset >= num_values) {
                // we ran out of output space
                finished = true;
                break;
            }
            if (child_defines_ptr[child_idx] >= max_define) {
                // value has been defined down the stack, hence its NOT NULL
                result_ptr[result_offset].offset = child_idx + current_chunk_offset;
                result_ptr[result_offset].size = 1;
            } else if (child_defines_ptr[child_idx] == max_define - 1) {
                // empty list
                result_ptr[result_offset].offset = child_idx + current_chunk_offset;
                result_ptr[result_offset].size = 0;
            } else {
                // value is NULL somewhere up the stack
                result_out->setNull(result_offset, true);
                result_ptr[result_offset].offset = 0;
                result_ptr[result_offset].size = 0;
            }

            repeat_out[result_offset] = child_repeats_ptr[child_idx];
            define_out[result_offset] = child_defines_ptr[child_idx];

            result_offset++;
        }

        // we have read more values from the child reader than we can fit into the result for this
        // read we have to pass everything from child_idx to child_actual_num_values into the next
        // call
        //        if (child_idx < child_actual_num_values && result_offset == num_values) {
        //            read_vector.Slice(read_vector, child_idx, child_actual_num_values);
        //            overflow_child_count = child_actual_num_values - child_idx;
        //            read_vector.Verify(overflow_child_count);
        //
        //            // move values in the child repeats and defines *backward* by child_idx
        //            for (idx_t repdef_idx = 0; repdef_idx < overflow_child_count; repdef_idx++) {
        //                child_defines_ptr[repdef_idx] = child_defines_ptr[child_idx + repdef_idx];
        //                child_repeats_ptr[repdef_idx] = child_repeats_ptr[child_idx + repdef_idx];
        //            }
        //        }
    }
    // result_out.Verify(result_offset);
    return result_offset;
}

uint64_t StructColumnReader::Read(uint64_t num_values, parquet_filter_t& filter,
    uint8_t* define_out, uint8_t* repeat_out, common::ValueVector* result) {
    auto& fieldVectors = common::StructVector::getFieldVectors(result);
    assert(common::StructType::getNumFields(type.get()) == fieldVectors.size());
    if (pending_skips > 0) {
        ApplyPendingSkips(pending_skips);
    }

    uint64_t read_count = num_values;
    for (auto i = 0u; i < fieldVectors.size(); i++) {
        auto child_num_values = child_readers[i]->Read(
            num_values, filter, define_out, repeat_out, fieldVectors[i].get());
        if (i == 0) {
            read_count = child_num_values;
        } else if (read_count != child_num_values) {
            throw std::runtime_error("Struct child row count mismatch");
        }
    }
    // set the validity mask for this level
    for (auto i = 0u; i < read_count; i++) {
        if (define_out[i] < max_define) {
            result->setNull(i, true);
        }
    }

    return read_count;
}

void StructColumnReader::Skip(uint64_t num_values) {
    for (auto& child_reader : child_readers) {
        child_reader->Skip(num_values);
    }
}

static bool TypeHasExactRowCount(const common::LogicalType* type) {
    switch (type->getLogicalTypeID()) {
    case common::LogicalTypeID::VAR_LIST:
    case common::LogicalTypeID::MAP:
        return false;
    case common::LogicalTypeID::STRUCT:
        for (auto& kv : common::StructType::getFieldTypes(type)) {
            if (TypeHasExactRowCount(kv)) {
                return true;
            }
        }
        return false;
    default:
        return true;
    }
}

uint64_t StructColumnReader::GroupRowsAvailable() {
    for (auto i = 0u; i < child_readers.size(); i++) {
        if (TypeHasExactRowCount(child_readers[i]->Type())) {
            return child_readers[i]->GroupRowsAvailable();
        }
    }
    return child_readers[0]->GroupRowsAvailable();
}

} // namespace processor
} // namespace kuzu
