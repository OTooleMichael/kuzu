#pragma once

#include "column_reader.h"
#include "resizable_buffer.h"

namespace kuzu {
namespace processor {

template<class VALUE_TYPE>
struct TemplatedParquetValueConversion {
    static VALUE_TYPE DictRead(ByteBuffer& dict, uint32_t& offset, ColumnReader& reader) {
        assert(offset < dict.len / sizeof(VALUE_TYPE));
        return ((VALUE_TYPE*)dict.ptr)[offset];
    }

    static VALUE_TYPE PlainRead(ByteBuffer& plain_data, ColumnReader& reader) {
        return plain_data.read<VALUE_TYPE>();
    }

    static void PlainSkip(ByteBuffer& plain_data, ColumnReader& reader) {
        plain_data.inc(sizeof(VALUE_TYPE));
    }
};

template<class VALUE_TYPE, class VALUE_CONVERSION>
class TemplatedColumnReader : public ColumnReader {
public:
    static constexpr const common::PhysicalTypeID TYPE = common::PhysicalTypeID::ANY;

public:
    TemplatedColumnReader(ParquetReader& reader, std::unique_ptr<common::LogicalType> type_p,
        const parquet::format::SchemaElement& schema_p, uint64_t schema_idx_p,
        uint64_t max_define_p, uint64_t max_repeat_p)
        : ColumnReader(
              reader, std::move(type_p), schema_p, schema_idx_p, max_define_p, max_repeat_p){};

    std::shared_ptr<ResizeableBuffer> dict;

public:
    //    void AllocateDict(uint64_t size) {
    //        if (!dict) {
    //            dict = std::make_shared<ResizeableBuffer>(size);
    //        } else {
    //            dict->resize(size);
    //        }
    //    }
    //
    //    void Dictionary(std::shared_ptr<ResizeableBuffer> data, uint64_t num_entries) override {
    //        dict = std::move(data);
    //    }
    //
    void Offsets(uint32_t* offsets, uint8_t* defines, uint64_t num_values, parquet_filter_t& filter,
        uint64_t result_offset, common::ValueVector* result) override {
        auto result_ptr = reinterpret_cast<VALUE_TYPE*>(result->getData());

        uint64_t offset_idx = 0;
        for (auto row_idx = 0; row_idx < num_values; row_idx++) {
            if (HasDefines() && defines[row_idx + result_offset] != max_define) {
                result->setNull(row_idx + result_offset, true);
                continue;
            }
            if (filter[row_idx + result_offset]) {
                VALUE_TYPE val = VALUE_CONVERSION::DictRead(*dict, offsets[offset_idx++], *this);
                result_ptr[row_idx + result_offset] = val;
            } else {
                offset_idx++;
            }
        }
    }

    void Plain(std::shared_ptr<ByteBuffer> plain_data, uint8_t* defines, uint64_t num_values,
        parquet_filter_t& filter, uint64_t result_offset, common::ValueVector* result) override {
        PlainTemplated<VALUE_TYPE, VALUE_CONVERSION>(
            std::move(plain_data), defines, num_values, filter, result_offset, result);
    }

    void Dictionary(std::shared_ptr<ResizeableBuffer> data, uint64_t num_entries) override {
        dict = std::move(data);
    }
};

template<class PARQUET_PHYSICAL_TYPE, class DUCKDB_PHYSICAL_TYPE,
    DUCKDB_PHYSICAL_TYPE (*FUNC)(const PARQUET_PHYSICAL_TYPE& input)>
struct CallbackParquetValueConversion {
    static DUCKDB_PHYSICAL_TYPE DictRead(ByteBuffer& dict, uint32_t& offset, ColumnReader& reader) {
        return TemplatedParquetValueConversion<DUCKDB_PHYSICAL_TYPE>::DictRead(
            dict, offset, reader);
    }

    static DUCKDB_PHYSICAL_TYPE PlainRead(ByteBuffer& plain_data, ColumnReader& reader) {
        return FUNC(plain_data.read<PARQUET_PHYSICAL_TYPE>());
    }

    static void PlainSkip(ByteBuffer& plain_data, ColumnReader& reader) {
        plain_data.inc(sizeof(PARQUET_PHYSICAL_TYPE));
    }
};

} // namespace processor
} // namespace kuzu
