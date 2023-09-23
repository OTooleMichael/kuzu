#include "processor/operator/persistent/reader/parquet/struct_column_reader.h"

namespace kuzu {
namespace processor {

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