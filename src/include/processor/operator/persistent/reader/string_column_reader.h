

#pragma once

#include "column_reader.h"

namespace kuzu {
namespace processor {

struct StringParquetValueConversion {
    static common::ku_string_t DictRead(ByteBuffer& dict, uint32_t& offset, ColumnReader& reader);

    static common::ku_string_t PlainRead(ByteBuffer& plain_data, ColumnReader& reader);

    static void PlainSkip(ByteBuffer& plain_data, ColumnReader& reader);
};

class StringColumnReader
    : public TemplatedColumnReader<common::ku_string_t, StringParquetValueConversion> {
public:
    static constexpr const common::PhysicalTypeID TYPE = common::PhysicalTypeID::STRING;

public:
    StringColumnReader(ParquetReader& reader, std::unique_ptr<common::LogicalType> type_p,
        const kuzu_parquet::format::SchemaElement& schema_p, uint64_t schema_idx_p,
        uint64_t max_define_p, uint64_t max_repeat_p);

    std::unique_ptr<common::ku_string_t[]> dict_strings;
    uint64_t fixed_width_string_length;
    uint64_t delta_offset = 0;

public:
    void Dictionary(
        std::shared_ptr<ResizeableBuffer> dictionary_data, uint64_t num_entries) override;
    static uint32_t VerifyString(const char* str_data, uint32_t str_len, const bool isVarchar);
    uint32_t VerifyString(const char* str_data, uint32_t str_len);
};

} // namespace processor
} // namespace kuzu
