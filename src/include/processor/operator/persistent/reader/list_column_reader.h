
#pragma once

#include "column_reader.h"
#include "templated_column_reader.h"

namespace kuzu {
namespace processor {

class ListColumnReader : public ColumnReader {
public:
    static constexpr const common::PhysicalTypeID TYPE = common::PhysicalTypeID::VAR_LIST;

public:
    ListColumnReader(ParquetReader& reader, std::unique_ptr<common::LogicalType> type_p,
        const kuzu_parquet::format::SchemaElement& schema_p, uint64_t schema_idx_p,
        uint64_t max_define_p, uint64_t max_repeat_p,
        std::unique_ptr<ColumnReader> child_column_reader_p);

    //    uint64_t Read(uint64_t num_values, parquet_filter_t& filter, uint8_t* define_out,
    //        uint8_t* repeat_out, common::ValueVector* result_out) override;
    //
    //    void ApplyPendingSkips(uint64_t num_values) override;

    void InitializeRead(uint64_t row_group_idx_p,
        const std::vector<kuzu_parquet::format::ColumnChunk>& columns,
        kuzu_apache::thrift::protocol::TProtocol& protocol_p) override {
        child_column_reader->InitializeRead(row_group_idx_p, columns, protocol_p);
    }

    uint64_t GroupRowsAvailable() override {
        return child_column_reader->GroupRowsAvailable() + overflow_child_count;
    }

    uint64_t TotalCompressedSize() override { return child_column_reader->TotalCompressedSize(); }

    void RegisterPrefetch(ThriftFileTransport& transport, bool allow_merge) override {
        child_column_reader->RegisterPrefetch(transport, allow_merge);
    }

private:
    std::unique_ptr<ColumnReader> child_column_reader;
    ResizeableBuffer child_defines;
    ResizeableBuffer child_repeats;
    uint8_t* child_defines_ptr;
    uint8_t* child_repeats_ptr;

    // VectorCache read_cache;
    // std::unique_ptr<common::ValueVector> read_vector;

    parquet_filter_t child_filter;

    uint64_t overflow_child_count;
};

} // namespace processor
} // namespace kuzu
