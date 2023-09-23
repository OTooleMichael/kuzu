#pragma once

#include <bitset>

#include "common/constants.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "parquet/parquet_types.h"
#include "parquet_dbp_decoder.h"
#include "parquet_rle_bp_decoder.h"
#include "resizable_buffer.h"
#include "thrift_tools.h"

namespace kuzu {
namespace processor {
class ParquetReader;

typedef std::bitset<common::DEFAULT_VECTOR_CAPACITY> parquet_filter_t;

class ColumnReader {
public:
    ColumnReader(ParquetReader& reader, std::unique_ptr<common::LogicalType> type,
        const kuzu_parquet::format::SchemaElement& schema, uint64_t fileIdx, uint64_t maxDefinition,
        uint64_t maxRepeat);

    static std::unique_ptr<ColumnReader> createReader(ParquetReader& reader,
        std::unique_ptr<common::LogicalType> type,
        const kuzu_parquet::format::SchemaElement& schema, uint64_t fileIdx, uint64_t maxDefine,
        uint64_t maxRepeat);

    inline common::LogicalType* getDataType() const { return type.get(); }

    virtual void InitializeRead(uint64_t row_group_index,
        const std::vector<kuzu_parquet::format::ColumnChunk>& columns,
        kuzu_apache::thrift::protocol::TProtocol& protocol_p);

    virtual uint64_t TotalCompressedSize();

    virtual uint64_t GroupRowsAvailable() { return group_rows_available; }

    virtual void RegisterPrefetch(ThriftFileTransport& transport, bool allow_merge) {
        if (chunk) {
            uint64_t size = chunk->meta_data.total_compressed_size;
            transport.RegisterPrefetch(FileOffset(), size, allow_merge);
        }
    }

    virtual void Skip(uint64_t num_values) { pending_skips += num_values; }

    virtual uint64_t FileOffset() const {
        if (!chunk) {
            throw std::runtime_error("FileOffset called on ColumnReader with no chunk");
        }
        auto min_offset = UINT64_MAX;
        if (chunk->meta_data.__isset.dictionary_page_offset) {
            min_offset = std::min<uint64_t>(min_offset, chunk->meta_data.dictionary_page_offset);
        }
        if (chunk->meta_data.__isset.index_page_offset) {
            min_offset = std::min<uint64_t>(min_offset, chunk->meta_data.index_page_offset);
        }
        min_offset = std::min<uint64_t>(min_offset, chunk->meta_data.data_page_offset);

        return min_offset;
    }

    virtual void ApplyPendingSkips(uint64_t num_values);

    virtual uint64_t Read(uint64_t num_values, parquet_filter_t& filter, uint8_t* define_out,
        uint8_t* repeat_out, common::ValueVector* result_out);

    virtual void Offsets(uint32_t* offsets, uint8_t* defines, uint64_t num_values,
        parquet_filter_t& filter, uint64_t result_offset, common::ValueVector* result) {
        throw common::NotImplementedException{"ColumnReader::Offsets"};
    }

    virtual void Plain(std::shared_ptr<ByteBuffer> plain_data, uint8_t* defines,
        uint64_t num_values, parquet_filter_t& filter, uint64_t result_offset,
        common::ValueVector* result) {
        throw common::NotImplementedException{"ColumnReader::Plain"};
    }

    void PrepareRead(parquet_filter_t& filter);

    bool HasDefines() { return max_define > 0; }

    bool HasRepeats() { return max_repeat > 0; }

    template<class VALUE_TYPE, class CONVERSION>
    void PlainTemplated(std::shared_ptr<ByteBuffer> plain_data, uint8_t* defines,
        uint64_t num_values, parquet_filter_t& filter, uint64_t result_offset,
        common::ValueVector* result) {
        auto result_ptr = reinterpret_cast<VALUE_TYPE*>(result->getData());
        for (auto row_idx = 0u; row_idx < num_values; row_idx++) {
            if (HasDefines() && defines[row_idx + result_offset] != max_define) {
                result->setNull(row_idx + result_offset, true);
                continue;
            }
            if (filter[row_idx + result_offset]) {
                VALUE_TYPE val = CONVERSION::PlainRead(*plain_data, *this);
                result->setValue(row_idx + result_offset, val);
            } else { // there is still some data there that we have to skip over
                CONVERSION::PlainSkip(*plain_data, *this);
            }
        }
    }

    void AllocateBlock(uint64_t size);
    void AllocateCompressed(uint64_t size);

    void DecompressInternal(kuzu_parquet::format::CompressionCodec::type codec, const uint8_t* src,
        uint64_t src_size, uint8_t* dst, uint64_t dst_size);
    void PreparePageV2(kuzu_parquet::format::PageHeader& page_hdr);
    void PreparePage(kuzu_parquet::format::PageHeader& page_hdr);
    void PrepareDataPage(kuzu_parquet::format::PageHeader& page_hdr);

    virtual void Dictionary(std::shared_ptr<ResizeableBuffer> data, uint64_t num_entries) {
        throw common::NotImplementedException{"Dictionary"};
    }

    virtual void ResetPage() {}

    const common::LogicalType* Type() const { return type.get(); }

protected:
    const kuzu_parquet::format::SchemaElement& schema;

    uint64_t file_idx;
    uint64_t max_define;
    uint64_t max_repeat;

    ParquetReader& reader;
    std::unique_ptr<common::LogicalType> type;
    uint64_t byte_array_count = 0;

    uint64_t pending_skips = 0;

    const kuzu_parquet::format::ColumnChunk* chunk = nullptr;

    kuzu_apache::thrift::protocol::TProtocol* protocol;
    uint64_t page_rows_available;
    uint group_rows_available;
    uint64_t chunk_read_offset;

    std::shared_ptr<ResizeableBuffer> block;

    ResizeableBuffer compressed_buffer;
    ResizeableBuffer offset_buffer;

    std::unique_ptr<RleBpDecoder> dict_decoder;
    std::unique_ptr<RleBpDecoder> defined_decoder;
    std::unique_ptr<RleBpDecoder> repeated_decoder;
    std::unique_ptr<DbpDecoder> dbp_decoder;
    std::unique_ptr<RleBpDecoder> rle_decoder;

    std::unique_ptr<common::ValueVector> byte_array_data;

    // dummies for Skip()
    parquet_filter_t none_filter;
    ResizeableBuffer dummy_define;
    ResizeableBuffer dummy_repeat;
};

} // namespace processor
} // namespace kuzu
