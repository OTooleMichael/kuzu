#pragma once

#include "common/types/types.h"
#include "parquet/parquet_types.h"

namespace kuzu {
namespace processor {
class ParquetReader;

typedef std::bitset<STANDARD_VECTOR_SIZE> parquet_filter_t;

class ColumnReader {
public:
    ColumnReader(ParquetReader& reader, std::unique_ptr<common::LogicalType> type,
        const parquet::format::SchemaElement* schema, uint64_t fileIdx, uint64_t maxDefinition,
        uint64_t maxRepeat);

public:
    static unique_ptr<ColumnReader> CreateReader(ParquetReader& reader, const LogicalType& type_p,
        const SchemaElement& schema_p, idx_t schema_idx_p, idx_t max_define, idx_t max_repeat);
    virtual void InitializeRead(
        idx_t row_group_index, const vector<ColumnChunk>& columns, TProtocol& protocol_p);
    virtual idx_t Read(uint64_t num_values, parquet_filter_t& filter, data_ptr_t define_out,
        data_ptr_t repeat_out, Vector& result_out);

    virtual void Skip(idx_t num_values);

    ParquetReader& Reader();
    inline common::LogicalType* getDataType() const { return type.get(); }
    inline parquet::format::SchemaElement getSchema() const { return schema.get(); }
    inline uint64_t getFileIdx() const { return fileIdx; }
    inline uint64_t getMaxDefine() const { return maxDefine; }
    inline uint64_t getMaxRepeat() const { return maxRepeat; }

    virtual idx_t FileOffset() const;
    virtual uint64_t TotalCompressedSize();
    virtual idx_t GroupRowsAvailable();

    // register the range this reader will touch for prefetching
    virtual void RegisterPrefetch(ThriftFileTransport& transport, bool allow_merge);

    virtual unique_ptr<BaseStatistics> Stats(
        idx_t row_group_idx_p, const vector<ColumnChunk>& columns);

    template<class VALUE_TYPE, class CONVERSION>
    void PlainTemplated(shared_ptr<ByteBuffer> plain_data, uint8_t* defines, uint64_t num_values,
        parquet_filter_t& filter, idx_t result_offset, Vector& result) {
        auto result_ptr = FlatVector::GetData<VALUE_TYPE>(result);
        auto& result_mask = FlatVector::Validity(result);
        for (idx_t row_idx = 0; row_idx < num_values; row_idx++) {
            if (HasDefines() && defines[row_idx + result_offset] != maxDefine) {
                result_mask.SetInvalid(row_idx + result_offset);
                continue;
            }
            if (filter[row_idx + result_offset]) {
                VALUE_TYPE val = CONVERSION::PlainRead(*plain_data, *this);
                result_ptr[row_idx + result_offset] = val;
            } else { // there is still some data there that we have to skip over
                CONVERSION::PlainSkip(*plain_data, *this);
            }
        }
    }

protected:
    // readers that use the default Read() need to implement those
    virtual void Plain(shared_ptr<ByteBuffer> plain_data, uint8_t* defines, idx_t num_values,
        parquet_filter_t& filter, idx_t result_offset, Vector& result);
    virtual void Dictionary(shared_ptr<ResizeableBuffer> dictionary_data, idx_t num_entries);
    virtual void Offsets(uint32_t* offsets, uint8_t* defines, idx_t num_values,
        parquet_filter_t& filter, idx_t result_offset, Vector& result);

    // these are nops for most types, but not for strings
    virtual void DictReference(Vector& result);
    virtual void PlainReference(shared_ptr<ByteBuffer>, Vector& result);

    virtual void PrepareDeltaLengthByteArray(ResizeableBuffer& buffer);
    virtual void PrepareDeltaByteArray(ResizeableBuffer& buffer);
    virtual void DeltaByteArray(uint8_t* defines, idx_t num_values, parquet_filter_t& filter,
        idx_t result_offset, Vector& result);

    // applies any skips that were registered using Skip()
    virtual void ApplyPendingSkips(idx_t num_values);

    bool HasDefines() { return maxDefine > 0; }

    bool HasRepeats() { return maxRepeat > 0; }

protected:
    const parquet::format::SchemaElement* schema;

    uint64_t fileIdx;
    uint64_t maxDefine;
    uint64_t maxRepeat;

    ParquetReader& reader;
    std::unique_ptr<common::LogicalType> type;
    unique_ptr<Vector> byte_array_data;
    uint64_t byte_array_count = 0;

    uint64_t pending_skips = 0;

    virtual void ResetPage();

private:
    void AllocateBlock(idx_t size);
    void AllocateCompressed(idx_t size);
    void PrepareRead(parquet_filter_t& filter);
    void PreparePage(PageHeader& page_hdr);
    void PrepareDataPage(PageHeader& page_hdr);
    void PreparePageV2(PageHeader& page_hdr);
    void DecompressInternal(CompressionCodec::type codec, const_data_ptr_t src, idx_t src_size,
        data_ptr_t dst, idx_t dst_size);

    const duckdb_parquet::format::ColumnChunk* chunk = nullptr;

    duckdb_apache::thrift::protocol::TProtocol* protocol;
    uint64_t pageRowsAvailable;
    uint64_t groupRowsAvailable;
    uint64_t chunkReadOffset;

    shared_ptr<ResizeableBuffer> block;

    ResizeableBuffer compressed_buffer;
    ResizeableBuffer offset_buffer;

    unique_ptr<RleBpDecoder> dict_decoder;
    unique_ptr<RleBpDecoder> defined_decoder;
    unique_ptr<RleBpDecoder> repeated_decoder;
    unique_ptr<DbpDecoder> dbp_decoder;
    unique_ptr<RleBpDecoder> rle_decoder;

    // dummies for Skip()
    parquet_filter_t none_filter;

public:
    template<class TARGET>
    TARGET& Cast() {
        if (TARGET::TYPE != PhysicalType::INVALID && type.InternalType() != TARGET::TYPE) {
            throw InternalException("Failed to cast column reader to type - type mismatch");
        }
        return reinterpret_cast<TARGET&>(*this);
    }

    template<class TARGET>
    const TARGET& Cast() const {
        if (TARGET::TYPE != PhysicalType::INVALID && type.InternalType() != TARGET::TYPE) {
            throw InternalException("Failed to cast column reader to type - type mismatch");
        }
        return reinterpret_cast<const TARGET&>(*this);
    }
};

} // namespace processor
} // namespace kuzu
