//===----------------------------------------------------------------------===//
//                         DuckDB
//
// callback_column_reader.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "column_reader.h"
#include "parquet_reader.h"
#include "templated_column_reader.h"

namespace kuzu {
namespace processor {

template<class PARQUET_PHYSICAL_TYPE, class DUCKDB_PHYSICAL_TYPE,
    DUCKDB_PHYSICAL_TYPE (*FUNC)(const PARQUET_PHYSICAL_TYPE& input)>
class CallbackColumnReader
    : public TemplatedColumnReader<DUCKDB_PHYSICAL_TYPE,
          CallbackParquetValueConversion<PARQUET_PHYSICAL_TYPE, DUCKDB_PHYSICAL_TYPE, FUNC>> {
    using BaseType = TemplatedColumnReader<DUCKDB_PHYSICAL_TYPE,
        CallbackParquetValueConversion<PARQUET_PHYSICAL_TYPE, DUCKDB_PHYSICAL_TYPE, FUNC>>;

public:
    static constexpr const common::PhysicalTypeID TYPE = common::PhysicalTypeID::ANY;

public:
    CallbackColumnReader(ParquetReader& reader, std::unique_ptr<common::LogicalType> type_p,
        const kuzu_parquet::format::SchemaElement& schema_p, uint64_t file_idx_p,
        uint64_t max_define_p, uint64_t max_repeat_p)
        : TemplatedColumnReader<DUCKDB_PHYSICAL_TYPE,
              CallbackParquetValueConversion<PARQUET_PHYSICAL_TYPE, DUCKDB_PHYSICAL_TYPE, FUNC>>(
              reader, std::move(type_p), schema_p, file_idx_p, max_define_p, max_repeat_p) {}

protected:
    void Dictionary(std::shared_ptr<ResizeableBuffer> dictionary_data, uint64_t num_entries) {
        BaseType::AllocateDict(num_entries * sizeof(DUCKDB_PHYSICAL_TYPE));
        auto dict_ptr = (DUCKDB_PHYSICAL_TYPE*)this->dict->ptr;
        for (auto i = 0u; i < num_entries; i++) {
            dict_ptr[i] = FUNC(dictionary_data->read<PARQUET_PHYSICAL_TYPE>());
        }
    }
};

} // namespace processor
} // namespace kuzu
