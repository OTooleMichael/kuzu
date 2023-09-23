#include "processor/operator/persistent/reader/parquet/string_column_reader.h"

#include "common/types/blob.h"
#include "common/types/ku_string.h"
#include "parquet/parquet_types.h"
#include "utf8proc_wrapper.h"

using kuzu_parquet::format::Type;

namespace kuzu {
namespace processor {

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
    ret_str.set1(plain_str, actual_str_len);
    plain_data.inc(str_len);
    return ret_str;
}

void StringParquetValueConversion::PlainSkip(ByteBuffer& plain_data, ColumnReader& reader) {
    auto& scr = reinterpret_cast<StringColumnReader&>(reader);
    uint32_t str_len = scr.fixed_width_string_length == 0 ? plain_data.read<uint32_t>() :
                                                            scr.fixed_width_string_length;
    plain_data.inc(str_len);
}

} // namespace processor
} // namespace kuzu
