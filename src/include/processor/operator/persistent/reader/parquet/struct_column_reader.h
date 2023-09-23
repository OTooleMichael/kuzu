
#pragma once

#include "column_reader.h"
#include "templated_column_reader.h"

namespace kuzu {
namespace processor {

class StructColumnReader : public ColumnReader {
public:
    static constexpr const common::PhysicalTypeID TYPE = common::PhysicalTypeID::STRUCT;

public:
    StructColumnReader(ParquetReader& reader, std::unique_ptr<common::LogicalType> type_p,
        const kuzu_parquet::format::SchemaElement& schema_p, uint64_t schema_idx_p,
        uint64_t max_define_p, uint64_t max_repeat_p,
        std::vector<std::unique_ptr<ColumnReader>> child_readers_p);

    std::vector<std::unique_ptr<ColumnReader>> child_readers;

public:
    ColumnReader* getChildReader(uint64_t child_idx);

    void InitializeRead(uint64_t row_group_idx_p,
        const std::vector<kuzu_parquet::format::ColumnChunk>& columns,
        kuzu_apache::thrift::protocol::TProtocol& protocol_p) override;

    uint64_t TotalCompressedSize() override;

    void RegisterPrefetch(ThriftFileTransport& transport, bool allow_merge) override {
        for (auto& child : child_readers) {
            child->RegisterPrefetch(transport, allow_merge);
        }
    }

    uint64_t Read(uint64_t num_values, parquet_filter_t& filter, uint8_t* define_out,
        uint8_t* repeat_out, common::ValueVector* result) override;

    void Skip(uint64_t num_values) override;
    uint64_t GroupRowsAvailable() override;
};

} // namespace processor
} // namespace kuzu
