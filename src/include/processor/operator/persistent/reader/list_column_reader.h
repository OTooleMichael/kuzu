
#pragma once

#include "column_reader.h"
#include "templated_column_reader.hpp"

namespace kuzu {
namespace processor {
class ListColumnReader : public ColumnReader {
public:
    static constexpr const common::PhysicalTypeID TYPE = common::PhysicalTypeID::VAR_LIST;

public:
    ListColumnReader(ParquetReader& reader, std::unique_ptr<common::LogicalType> type_p,
        const parquet::format::SchemaElement& schema_p, uint64_t schema_idx_p,
        uint64_t max_define_p, uint64_t max_repeat_p,
        std::unique_ptr<ColumnReader> child_column_reader_p);

    idx_t Read(uint64_t num_values, parquet_filter_t& filter, data_ptr_t define_out,
        data_ptr_t repeat_out, Vector& result_out) override;

    void ApplyPendingSkips(idx_t num_values) override;

    void InitializeRead(
        idx_t row_group_idx_p, const vector<ColumnChunk>& columns, TProtocol& protocol_p) override {
        child_column_reader->InitializeRead(row_group_idx_p, columns, protocol_p);
    }

    idx_t GroupRowsAvailable() override {
        return child_column_reader->GroupRowsAvailable() + overflow_child_count;
    }

    uint64_t TotalCompressedSize() override { return child_column_reader->TotalCompressedSize(); }

    void RegisterPrefetch(ThriftFileTransport& transport, bool allow_merge) override {
        child_column_reader->RegisterPrefetch(transport, allow_merge);
    }

private:
    unique_ptr<ColumnReader> child_column_reader;
    ResizeableBuffer child_defines;
    ResizeableBuffer child_repeats;
    uint8_t* child_defines_ptr;
    uint8_t* child_repeats_ptr;

    VectorCache read_cache;
    Vector read_vector;

    parquet_filter_t child_filter;

    idx_t overflow_child_count;
};

} // namespace processor
} // namespace kuzu
