#include "processor/operator/persistent/reader/parquet_reader.h"

#include "common/exception/copy.h"
#include "common/exception/not_implemented.h"
#include "common/file_utils.h"
#include "common/string_utils.h"
#include "processor/operator/persistent/reader/thrift_tools.h"

using namespace kuzu_parquet::format;

namespace kuzu {
namespace processor {

void ParquetReader::initMetadata() {
    auto proto = createThriftProtocol(fileInfo.get(), false);
    auto& transport = reinterpret_cast<ThriftFileTransport&>(*proto->getTransport());
    auto file_size = transport.GetSize();
    if (file_size < 12) {
        throw common::CopyException{common::StringUtils::string_format(
            "File {} too small to be a Parquet file", fileInfo->path.c_str())};
    }

    ResizeableBuffer buf;
    buf.resize(8);
    buf.zero();

    transport.SetLocation(file_size - 8);
    transport.read((uint8_t*)buf.ptr, 8);

    if (memcmp(buf.ptr + 4, "PAR1", 4) != 0) {
        if (memcmp(buf.ptr + 4, "PARE", 4) == 0) {
            throw common::CopyException{common::StringUtils::string_format(
                "Encrypted Parquet files are not supported for file {}", fileInfo->path.c_str())};
        }
        throw common::CopyException{common::StringUtils::string_format(
            "No magic bytes found at end of file {}", fileInfo->path.c_str())};
    }
    // read four-byte footer length from just before the end magic bytes
    auto footer_len = *reinterpret_cast<uint32_t*>(buf.ptr);
    if (footer_len == 0 || file_size < 12 + footer_len) {
        throw common::CopyException{common::StringUtils::string_format(
            "Footer length error in file {}", fileInfo->path.c_str())};
    }
    auto metadata_pos = file_size - (footer_len + 8);
    transport.SetLocation(metadata_pos);
    transport.Prefetch(metadata_pos, footer_len);

    metadata = std::make_unique<FileMetaData>();
    metadata->read(proto.get());
}

std::unique_ptr<ColumnReader> ParquetReader::createReaderRecursive(uint64_t depth,
    uint64_t max_define, uint64_t max_repeat, uint64_t& next_schema_idx, uint64_t& next_file_idx) {
    assert(next_schema_idx < metadata->schema.size());
    auto& s_ele = metadata->schema[next_schema_idx];
    auto this_idx = next_schema_idx;

    auto repetition_type = FieldRepetitionType::REQUIRED;
    if (s_ele.__isset.repetition_type && this_idx > 0) {
        repetition_type = s_ele.repetition_type;
    }
    if (repetition_type != FieldRepetitionType::REQUIRED) {
        max_define++;
    }
    if (repetition_type == FieldRepetitionType::REPEATED) {
        max_repeat++;
    }
    if (s_ele.__isset.num_children && s_ele.num_children > 0) { // inner node
        std::vector<std::unique_ptr<common::StructField>> structFields;
        std::vector<std::unique_ptr<ColumnReader>> childrenReaders;

        uint64_t c_idx = 0;
        while (c_idx < (uint64_t)s_ele.num_children) {
            next_schema_idx++;

            auto& child_ele = metadata->schema[next_schema_idx];

            auto child_reader = createReaderRecursive(
                depth + 1, max_define, max_repeat, next_schema_idx, next_file_idx);
            structFields.push_back(std::make_unique<common::StructField>(
                child_ele.name, child_reader->getDataType()->copy()));
            childrenReaders.push_back(std::move(child_reader));

            c_idx++;
        }
        assert(!structFields.empty());
        std::unique_ptr<ColumnReader> result;
        std::unique_ptr<common::LogicalType> result_type;

        bool is_repeated = repetition_type == FieldRepetitionType::REPEATED;
        bool is_list = s_ele.__isset.converted_type && s_ele.converted_type == ConvertedType::LIST;
        bool is_map = s_ele.__isset.converted_type && s_ele.converted_type == ConvertedType::MAP;
        bool is_map_kv =
            s_ele.__isset.converted_type && s_ele.converted_type == ConvertedType::MAP_KEY_VALUE;
        if (!is_map_kv && this_idx > 0) {
            // check if the parent node of this is a map
            auto& p_ele = metadata->schema[this_idx - 1];
            bool parent_is_map =
                p_ele.__isset.converted_type && p_ele.converted_type == ConvertedType::MAP;
            bool parent_has_children = p_ele.__isset.num_children && p_ele.num_children == 1;
            is_map_kv = parent_is_map && parent_has_children;
        }

        //        if (is_map_kv) {
        //            if (child_types.size() != 2) {
        //                throw IOException("MAP_KEY_VALUE requires two children");
        //            }
        //            if (!is_repeated) {
        //                throw IOException("MAP_KEY_VALUE needs to be repeated");
        //            }
        //            result_type = LogicalType::MAP(
        //                std::move(child_types[0].second), std::move(child_types[1].second));
        //
        //            auto struct_reader =
        //                make_uniq<StructColumnReader>(*this, ListType::GetChildType(result_type),
        //                s_ele,
        //                    this_idx, max_define - 1, max_repeat - 1, std::move(childrenReaders));
        //            return make_uniq<ListColumnReader>(*this, result_type, s_ele, this_idx,
        //            max_define,
        //                max_repeat, std::move(struct_reader));
        //        }
        if (structFields.size() > 1 || (!is_list && !is_map && !is_repeated)) {
            result_type = std::make_unique<common::LogicalType>(common::LogicalTypeID::STRUCT,
                std::make_unique<common::StructTypeInfo>(std::move(structFields)));
            result = std::make_unique<StructColumnReader>(*this, std::move(result_type), s_ele,
                this_idx, max_define, max_repeat, std::move(childrenReaders));
        } else {
            // if we have a struct with only a single type, pull up
            result_type = structFields[0]->getType()->copy();
            result = std::move(childrenReaders[0]);
        }
        if (is_repeated) {
            auto varListInfo = std::make_unique<common::VarListTypeInfo>(std::move(result_type));
            result_type = std::make_unique<common::LogicalType>(
                common::LogicalTypeID::VAR_LIST, std::move(varListInfo));
            assert(false);
            //            return std::make_unique<ListColumnReader>(*this, std::move(result_type),
            //            s_ele,
            //                this_idx, max_define, max_repeat, std::move(result));
        }
        return result;
    } else { // leaf node
        if (!s_ele.__isset.type) {
            throw common::CopyException{"Node has neither num_children nor type set - this "
                                        "violates the Parquet spec (corrupted file)"};
        }
        if (s_ele.repetition_type == FieldRepetitionType::REPEATED) {
            auto derivedType = deriveLogicalType(s_ele);
            auto varListTypeInfo = std::make_unique<common::VarListTypeInfo>(derivedType->copy());
            auto list_type = std::make_unique<common::LogicalType>(
                common::LogicalTypeID::VAR_LIST, std::move(varListTypeInfo));
            auto element_reader = ColumnReader::createReader(
                *this, std::move(derivedType), s_ele, next_file_idx++, max_define, max_repeat);
            //            return std::make_unique<ListColumnReader>(*this, std::move(list_type),
            //            s_ele, this_idx,
            //                max_define, max_repeat, std::move(element_reader));
            assert(false);
        }
        // TODO check return value of derive type or should we only do this on read()
        return ColumnReader::createReader(
            *this, deriveLogicalType(s_ele), s_ele, next_file_idx++, max_define, max_repeat);
    }
}

// TODO we don't need readers for columns we are not going to read ay
std::unique_ptr<ColumnReader> ParquetReader::createReader() {
    uint64_t next_schema_idx = 0;
    uint64_t next_file_idx = 0;

    if (metadata->schema.empty()) {
        throw common::CopyException{"Parquet reader: no schema elements found"};
    }
    if (metadata->schema[0].num_children == 0) {
        throw common::CopyException{"Parquet reader: root schema element has no children"};
    }
    auto ret = createReaderRecursive(0, 0, 0, next_schema_idx, next_file_idx);
    if (ret->getDataType()->getPhysicalType() != common::PhysicalTypeID::STRUCT) {
        throw common::CopyException{"Root element of Parquet file must be a struct"};
    }
    assert(next_schema_idx == metadata->schema.size() - 1);
    assert(metadata->row_groups.empty() || next_file_idx == metadata->row_groups[0].columns.size());

    //    auto& root_struct_reader = ret->Cast<StructColumnReader>();
    //    // add casts if required
    //    for (auto& entry : reader_data.cast_map) {
    //        auto column_idx = entry.first;
    //        auto& expected_type = entry.second;
    //        auto child_reader = std::move(root_struct_reader.child_readers[column_idx]);
    //        auto cast_reader = make_uniq<CastColumnReader>(std::move(child_reader),
    //        expected_type); root_struct_reader.child_readers[column_idx] = std::move(cast_reader);
    //    }
    return ret;
}
//
// void ParquetReader::InitializeSchema() {
//    auto file_meta_data = GetFileMetadata();
//
//    if (file_meta_data->__isset.encryption_algorithm) {
//        throw common::CopyException("Encrypted Parquet files are not supported");
//    }
//    // check if we like this schema
//    if (file_meta_data->schema.size() < 2) {
//        throw common::CopyException("Need at least one non-root column in the file");
//    }
//    root_reader = createReader();
//    auto& root_type = root_reader->getType();
//    auto& child_types = StructType::GetChildTypes(root_type);
//    D_ASSERT(root_type.id() == LogicalTypeId::STRUCT);
//    for (auto& type_pair : child_types) {
//        names.push_back(type_pair.first);
//        return_types.push_back(type_pair.second);
//    }
//}
//
// ParquetOptions::ParquetOptions(ClientContext& context) {
//    Value binary_as_string_val;
//    if (context.TryGetCurrentSetting("binary_as_string", binary_as_string_val)) {
//        binary_as_string = binary_as_string_val.GetValue<bool>();
//    }
//}
//
ParquetReader::ParquetReader(const std::string& filePath) : filePath{filePath} {
    fileInfo = common::FileUtils::openFile(filePath, O_RDONLY);
    initMetadata();
    // InitializeSchema();
}
//
// ParquetReader::ParquetReader(ClientContext& context_p, ParquetOptions parquet_options_p,
//    shared_ptr<ParquetFileMetadataCache> metadata_p)
//    : fs(FileSystem::GetFileSystem(context_p)), allocator(BufferAllocator::Get(context_p)),
//      metadata(std::move(metadata_p)), parquet_options(std::move(parquet_options_p)) {
//    InitializeSchema();
//}
//
// ParquetReader::~ParquetReader() {}
//
// unique_ptr<BaseStatistics> ParquetReader::ReadStatistics(const string& name) {
//    idx_t file_col_idx;
//    for (file_col_idx = 0; file_col_idx < names.size(); file_col_idx++) {
//        if (names[file_col_idx] == name) {
//            break;
//        }
//    }
//    if (file_col_idx == names.size()) {
//        return nullptr;
//    }
//
//    unique_ptr<BaseStatistics> column_stats;
//    auto column_reader = root_reader->Cast<StructColumnReader>().GetChildReader(file_col_idx);
//
//    for (idx_t row_group_idx = 0; row_group_idx < file_meta_data->row_groups.size();
//         row_group_idx++) {
//        auto& row_group = file_meta_data->row_groups[row_group_idx];
//        auto chunk_stats = column_reader->Stats(row_group_idx, row_group.columns);
//        if (!chunk_stats) {
//            return nullptr;
//        }
//        if (!column_stats) {
//            column_stats = std::move(chunk_stats);
//        } else {
//            column_stats->Merge(*chunk_stats);
//        }
//    }
//    return column_stats;
//}
//
// const ParquetRowGroup& ParquetReader::GetGroup(ParquetReaderScanState& state) {
//    auto file_meta_data = GetFileMetadata();
//    D_ASSERT(state.current_group >= 0 && (idx_t)state.current_group <
//    state.group_idx_list.size()); D_ASSERT(state.group_idx_list[state.current_group] >= 0 &&
//             state.group_idx_list[state.current_group] < file_meta_data->row_groups.size());
//    return file_meta_data->row_groups[state.group_idx_list[state.current_group]];
//}
//
// uint64_t ParquetReader::GetGroupCompressedSize(ParquetReaderScanState& state) {
//    auto& group = GetGroup(state);
//    auto total_compressed_size = group.total_compressed_size;
//
//    idx_t calc_compressed_size = 0;
//
//    // If the global total_compressed_size is not set, we can still calculate it
//    if (group.total_compressed_size == 0) {
//        for (auto& column_chunk : group.columns) {
//            calc_compressed_size += column_chunk.meta_data.total_compressed_size;
//        }
//    }
//
//    if (total_compressed_size != 0 && calc_compressed_size != 0 &&
//        (idx_t)total_compressed_size != calc_compressed_size) {
//        throw InvalidInputException(
//            "mismatch between calculated compressed size and reported compressed size");
//    }
//
//    return total_compressed_size ? total_compressed_size : calc_compressed_size;
//}
//
// uint64_t ParquetReader::GetGroupSpan(ParquetReaderScanState& state) {
//    auto& group = GetGroup(state);
//    idx_t min_offset = NumericLimits<idx_t>::Maximum();
//    idx_t max_offset = NumericLimits<idx_t>::Minimum();
//
//    for (auto& column_chunk : group.columns) {
//
//        // Set the min offset
//        idx_t current_min_offset = NumericLimits<idx_t>::Maximum();
//        if (column_chunk.meta_data.__isset.dictionary_page_offset) {
//            current_min_offset =
//                MinValue<idx_t>(current_min_offset,
//                column_chunk.meta_data.dictionary_page_offset);
//        }
//        if (column_chunk.meta_data.__isset.index_page_offset) {
//            current_min_offset =
//                MinValue<idx_t>(current_min_offset, column_chunk.meta_data.index_page_offset);
//        }
//        current_min_offset =
//            MinValue<idx_t>(current_min_offset, column_chunk.meta_data.data_page_offset);
//        min_offset = MinValue<idx_t>(current_min_offset, min_offset);
//        max_offset = MaxValue<idx_t>(
//            max_offset, column_chunk.meta_data.total_compressed_size + current_min_offset);
//    }
//
//    return max_offset - min_offset;
//}
//

void ParquetReader::prepareRowGroupBuffer(ParquetReaderScanState& state, uint64_t col_idx) {
    auto& group = getGroup(state);
    auto column_id = col_idx;
    auto column_reader =
        reinterpret_cast<StructColumnReader*>(state.rootReader.get())->getChildReader(column_id);
    state.rootReader->InitializeRead(
        state.group_idx_list[state.current_group], group.columns, *state.thriftFileProto);
}

uint64_t ParquetReader::GetGroupSpan(ParquetReaderScanState& state) {
    auto& group = getGroup(state);
    uint64_t min_offset = UINT64_MAX;
    uint64_t max_offset = 0;

    for (auto& column_chunk : group.columns) {

        // Set the min offset
        auto current_min_offset = UINT64_MAX;
        if (column_chunk.meta_data.__isset.dictionary_page_offset) {
            current_min_offset = std::min<uint64_t>(
                current_min_offset, column_chunk.meta_data.dictionary_page_offset);
        }
        if (column_chunk.meta_data.__isset.index_page_offset) {
            current_min_offset =
                std::min<uint64_t>(current_min_offset, column_chunk.meta_data.index_page_offset);
        }
        current_min_offset =
            std::min<uint64_t>(current_min_offset, column_chunk.meta_data.data_page_offset);
        min_offset = std::min<uint64_t>(current_min_offset, min_offset);
        max_offset = std::max<uint64_t>(
            max_offset, column_chunk.meta_data.total_compressed_size + current_min_offset);
    }

    return max_offset - min_offset;
}

void ParquetReader::initializeScan(
    ParquetReaderScanState& state, std::vector<uint64_t> groups_to_read) {
    state.current_group = -1;
    state.finished = false;
    state.group_offset = 0;
    state.group_idx_list = std::move(groups_to_read);
    if (!state.fileInfo || state.fileInfo->path != fileInfo->path) {
        state.prefetch_mode = false;
        state.fileInfo = common::FileUtils::openFile(fileInfo->path, O_RDONLY);
    }

    state.thriftFileProto = createThriftProtocol(state.fileInfo.get(), state.prefetch_mode);
    state.rootReader = createReader();
    state.define_buf.resize(common::DEFAULT_VECTOR_CAPACITY);
    state.repeat_buf.resize(common::DEFAULT_VECTOR_CAPACITY);
}

std::unique_ptr<common::LogicalType> ParquetReader::deriveLogicalType(
    const kuzu_parquet::format::SchemaElement& s_ele) {
    // inner node
    if (s_ele.type == Type::FIXED_LEN_BYTE_ARRAY && !s_ele.__isset.type_length) {
        throw common::CopyException("FIXED_LEN_BYTE_ARRAY requires length to be set");
    }
    //    if (s_ele.__isset.logicalType) {
    //        if (s_ele.logicalType.__isset.UUID) {
    //            if (s_ele.type == Type::FIXED_LEN_BYTE_ARRAY) {
    //                return LogicalType::UUID;
    //            }
    //        } else if (s_ele.logicalType.__isset.TIMESTAMP) {
    //            if (s_ele.logicalType.TIMESTAMP.isAdjustedToUTC) {
    //                return LogicalType::TIMESTAMP_TZ;
    //            }
    //            return LogicalType::TIMESTAMP;
    //        } else if (s_ele.logicalType.__isset.TIME) {
    //            if (s_ele.logicalType.TIME.isAdjustedToUTC) {
    //                return LogicalType::TIME_TZ;
    //            }
    //            return LogicalType::TIME;
    //        }
    //    }
    if (s_ele.__isset.converted_type) {
        switch (s_ele.converted_type) {
        case ConvertedType::INT_8:
            if (s_ele.type == Type::INT32) {
                return std::make_unique<common::LogicalType>(common::LogicalTypeID::INT8);
            } else {
                throw common::CopyException{
                    "INT8 converted type can only be set for value of Type::INT32"};
            }
        case ConvertedType::INT_16:
            if (s_ele.type == Type::INT32) {
                return std::make_unique<common::LogicalType>(common::LogicalTypeID::INT16);
            } else {
                throw common::CopyException{
                    "INT16 converted type can only be set for value of Type::INT32"};
            }
        case ConvertedType::INT_32:
            if (s_ele.type == Type::INT32) {
                return std::make_unique<common::LogicalType>(common::LogicalTypeID::INT32);
            } else {
                throw common::CopyException{
                    "INT32 converted type can only be set for value of Type::INT32"};
            }
        case ConvertedType::INT_64:
            if (s_ele.type == Type::INT64) {
                return std::make_unique<common::LogicalType>(common::LogicalTypeID::INT64);
            } else {
                throw common::CopyException{
                    "INT64 converted type can only be set for value of Type::INT64"};
            }
        case ConvertedType::DATE:
            if (s_ele.type == Type::INT32) {
                return std::make_unique<common::LogicalType>(common::LogicalTypeID::DATE);
            } else {
                throw common::CopyException{
                    "DATE converted type can only be set for value of Type::INT32"};
            }
        case ConvertedType::UTF8:
        case ConvertedType::ENUM:
            switch (s_ele.type) {
            case Type::BYTE_ARRAY:
            case Type::FIXED_LEN_BYTE_ARRAY:
                return std::make_unique<common::LogicalType>(common::LogicalTypeID::STRING);
            default:
                throw common::CopyException(
                    "UTF8 converted type can only be set for Type::(FIXED_LEN_)BYTE_ARRAY");
            }
        case ConvertedType::TIME_MILLIS:
        case ConvertedType::TIME_MICROS:
        case ConvertedType::INTERVAL:
        case ConvertedType::JSON:
        case ConvertedType::MAP:
        case ConvertedType::MAP_KEY_VALUE:
        case ConvertedType::LIST:
        case ConvertedType::BSON:
        case ConvertedType::UINT_8:
        case ConvertedType::UINT_16:
        case ConvertedType::UINT_32:
        case ConvertedType::UINT_64:
        case ConvertedType::TIMESTAMP_MICROS:
        case ConvertedType::TIMESTAMP_MILLIS:
        case ConvertedType::DECIMAL:
        default:
            throw common::CopyException{"Unsupported converted type"};
        }
    } else {
        // no converted type set
        // use default type for each physical type
        switch (s_ele.type) {
        case Type::BOOLEAN:
            return std::make_unique<common::LogicalType>(common::LogicalTypeID::BOOL);
        case Type::INT32:
            return std::make_unique<common::LogicalType>(common::LogicalTypeID::INT32);
        case Type::INT64:
            return std::make_unique<common::LogicalType>(common::LogicalTypeID::INT64);
        case Type::FLOAT:
            return std::make_unique<common::LogicalType>(common::LogicalTypeID::FLOAT);
        case Type::DOUBLE:
            return std::make_unique<common::LogicalType>(common::LogicalTypeID::DOUBLE);
        case Type::BYTE_ARRAY:
        case Type::FIXED_LEN_BYTE_ARRAY:
            return std::make_unique<common::LogicalType>(common::LogicalTypeID::STRING);
        default:
            return std::make_unique<common::LogicalType>(common::LogicalTypeID::ANY);
        }
    }
}

// void FilterIsNull(Vector& v, parquet_filter_t& filter_mask, idx_t count) {
//    if (v.GetVectorType() == VectorType::CONSTANT_VECTOR) {
//        auto& mask = ConstantVector::Validity(v);
//        if (mask.RowIsValid(0)) {
//            filter_mask.reset();
//        }
//        return;
//    }
//    D_ASSERT(v.GetVectorType() == VectorType::FLAT_VECTOR);
//
//    auto& mask = FlatVector::Validity(v);
//    if (mask.AllValid()) {
//        filter_mask.reset();
//    } else {
//        for (idx_t i = 0; i < count; i++) {
//            filter_mask[i] = filter_mask[i] && !mask.RowIsValid(i);
//        }
//    }
//}
//
// void FilterIsNotNull(Vector& v, parquet_filter_t& filter_mask, idx_t count) {
//    if (v.GetVectorType() == VectorType::CONSTANT_VECTOR) {
//        auto& mask = ConstantVector::Validity(v);
//        if (!mask.RowIsValid(0)) {
//            filter_mask.reset();
//        }
//        return;
//    }
//    D_ASSERT(v.GetVectorType() == VectorType::FLAT_VECTOR);
//
//    auto& mask = FlatVector::Validity(v);
//    if (!mask.AllValid()) {
//        for (idx_t i = 0; i < count; i++) {
//            filter_mask[i] = filter_mask[i] && mask.RowIsValid(i);
//        }
//    }
//}
//
// template<class T, class OP>
// void TemplatedFilterOperation(Vector& v, T constant, parquet_filter_t& filter_mask, idx_t count)
// {
//    if (v.GetVectorType() == VectorType::CONSTANT_VECTOR) {
//        auto v_ptr = ConstantVector::GetData<T>(v);
//        auto& mask = ConstantVector::Validity(v);
//
//        if (mask.RowIsValid(0)) {
//            if (!OP::Operation(v_ptr[0], constant)) {
//                filter_mask.reset();
//            }
//        }
//        return;
//    }
//
//    D_ASSERT(v.GetVectorType() == VectorType::FLAT_VECTOR);
//    auto v_ptr = FlatVector::GetData<T>(v);
//    auto& mask = FlatVector::Validity(v);
//
//    if (!mask.AllValid()) {
//        for (idx_t i = 0; i < count; i++) {
//            if (mask.RowIsValid(i)) {
//                filter_mask[i] = filter_mask[i] && OP::Operation(v_ptr[i], constant);
//            }
//        }
//    } else {
//        for (idx_t i = 0; i < count; i++) {
//            filter_mask[i] = filter_mask[i] && OP::Operation(v_ptr[i], constant);
//        }
//    }
//}

// template<class T, class OP>
// void TemplatedFilterOperation(
//    Vector& v, const Value& constant, parquet_filter_t& filter_mask, idx_t count) {
//    TemplatedFilterOperation<T, OP>(v, constant.template GetValueUnsafe<T>(), filter_mask, count);
//}
//
// template<class OP>
// static void FilterOperationSwitch(
//    Vector& v, Value& constant, parquet_filter_t& filter_mask, idx_t count) {
//    if (filter_mask.none() || count == 0) {
//        return;
//    }
//    switch (v.GetType().InternalType()) {
//    case PhysicalType::BOOL:
//        TemplatedFilterOperation<bool, OP>(v, constant, filter_mask, count);
//        break;
//    case PhysicalType::UINT8:
//        TemplatedFilterOperation<uint8_t, OP>(v, constant, filter_mask, count);
//        break;
//    case PhysicalType::UINT16:
//        TemplatedFilterOperation<uint16_t, OP>(v, constant, filter_mask, count);
//        break;
//    case PhysicalType::UINT32:
//        TemplatedFilterOperation<uint32_t, OP>(v, constant, filter_mask, count);
//        break;
//    case PhysicalType::UINT64:
//        TemplatedFilterOperation<uint64_t, OP>(v, constant, filter_mask, count);
//        break;
//    case PhysicalType::INT8:
//        TemplatedFilterOperation<int8_t, OP>(v, constant, filter_mask, count);
//        break;
//    case PhysicalType::INT16:
//        TemplatedFilterOperation<int16_t, OP>(v, constant, filter_mask, count);
//        break;
//    case PhysicalType::INT32:
//        TemplatedFilterOperation<int32_t, OP>(v, constant, filter_mask, count);
//        break;
//    case PhysicalType::INT64:
//        TemplatedFilterOperation<int64_t, OP>(v, constant, filter_mask, count);
//        break;
//    case PhysicalType::INT128:
//        TemplatedFilterOperation<hugeint_t, OP>(v, constant, filter_mask, count);
//        break;
//    case PhysicalType::FLOAT:
//        TemplatedFilterOperation<float, OP>(v, constant, filter_mask, count);
//        break;
//    case PhysicalType::DOUBLE:
//        TemplatedFilterOperation<double, OP>(v, constant, filter_mask, count);
//        break;
//    case PhysicalType::VARCHAR:
//        TemplatedFilterOperation<string_t, OP>(v, constant, filter_mask, count);
//        break;
//    default:
//        throw NotImplementedException("Unsupported type for filter %s", v.ToString());
//    }
//}

// static void ApplyFilter(
//    Vector& v, TableFilter& filter, parquet_filter_t& filter_mask, idx_t count) {
//    switch (filter.filter_type) {
//    case TableFilterType::CONJUNCTION_AND: {
//        auto& conjunction = filter.Cast<ConjunctionAndFilter>();
//        for (auto& child_filter : conjunction.child_filters) {
//            ApplyFilter(v, *child_filter, filter_mask, count);
//        }
//        break;
//    }
//    case TableFilterType::CONJUNCTION_OR: {
//        auto& conjunction = filter.Cast<ConjunctionOrFilter>();
//        parquet_filter_t or_mask;
//        for (auto& child_filter : conjunction.child_filters) {
//            parquet_filter_t child_mask = filter_mask;
//            ApplyFilter(v, *child_filter, child_mask, count);
//            or_mask |= child_mask;
//        }
//        filter_mask &= or_mask;
//        break;
//    }
//    case TableFilterType::CONSTANT_COMPARISON: {
//        auto& constant_filter = filter.Cast<ConstantFilter>();
//        switch (constant_filter.comparison_type) {
//        case ExpressionType::COMPARE_EQUAL:
//            FilterOperationSwitch<Equals>(v, constant_filter.constant, filter_mask, count);
//            break;
//        case ExpressionType::COMPARE_LESSTHAN:
//            FilterOperationSwitch<LessThan>(v, constant_filter.constant, filter_mask, count);
//            break;
//        case ExpressionType::COMPARE_LESSTHANOREQUALTO:
//            FilterOperationSwitch<LessThanEquals>(v, constant_filter.constant, filter_mask,
//            count); break;
//        case ExpressionType::COMPARE_GREATERTHAN:
//            FilterOperationSwitch<GreaterThan>(v, constant_filter.constant, filter_mask, count);
//            break;
//        case ExpressionType::COMPARE_GREATERTHANOREQUALTO:
//            FilterOperationSwitch<GreaterThanEquals>(
//                v, constant_filter.constant, filter_mask, count);
//            break;
//        default:
//            D_ASSERT(0);
//        }
//        break;
//    }
//    case TableFilterType::IS_NOT_NULL:
//        FilterIsNotNull(v, filter_mask, count);
//        break;
//    case TableFilterType::IS_NULL:
//        FilterIsNull(v, filter_mask, count);
//        break;
//    default:
//        D_ASSERT(0);
//        break;
//    }
//}

// void ParquetReader::Scan(ParquetReaderScanState& state, DataChunk& result) {
//    while (ScanInternal(state, result)) {
//        if (result.size() > 0) {
//            break;
//        }
//        result.Reset();
//    }
//}

bool ParquetReader::scanInternal(ParquetReaderScanState& state, common::DataChunk& result) {
    if (state.finished) {
        return false;
    }

    // see if we have to switch to the next row group in the parquet file
    if (state.current_group < 0 || (int64_t)state.group_offset >= getGroup(state).num_rows) {
        state.current_group++;
        state.group_offset = 0;

        auto& trans =
            reinterpret_cast<ThriftFileTransport&>(*state.thriftFileProto->getTransport());
        trans.ClearPrefetch();
        state.current_group_prefetched = false;

        if ((uint64_t)state.current_group == state.group_idx_list.size()) {
            state.finished = true;
            return false;
        }

        uint64_t to_scan_compressed_bytes = 0;
        for (auto col_idx = 0u; col_idx < metadata->column_orders.size(); col_idx++) {
            prepareRowGroupBuffer(state, col_idx);

            auto file_col_idx = col_idx;

            auto root_reader = reinterpret_cast<StructColumnReader*>(state.rootReader.get());
            to_scan_compressed_bytes +=
                root_reader->getChildReader(file_col_idx)->TotalCompressedSize();
        }

        auto& group = getGroup(state);
        if (state.prefetch_mode && state.group_offset != (uint64_t)group.num_rows) {

            uint64_t total_row_group_span = GetGroupSpan(state);

            double scan_percentage = (double)(to_scan_compressed_bytes) / total_row_group_span;

            if (to_scan_compressed_bytes > total_row_group_span) {
                throw common::CopyException("Malformed parquet file: sum of total compressed bytes "
                                            "of columns seems incorrect");
            }

            if (scan_percentage > ParquetReaderPrefetchConfig::WHOLE_GROUP_PREFETCH_MINIMUM_SCAN) {
                // Prefetch the whole row group
                if (!state.current_group_prefetched) {
                    auto total_compressed_size = GetGroupCompressedSize(state);
                    if (total_compressed_size > 0) {
                        trans.Prefetch(GetGroupOffset(state), total_row_group_span);
                    }
                    state.current_group_prefetched = true;
                }
            } else {
                // lazy fetching is when all tuples in a column can be skipped. With lazy fetching
                // the buffer is only fetched on the first read to that buffer.
                bool lazy_fetch = false;

                // Prefetch column-wise
                for (auto col_idx = 0u; col_idx < metadata->column_orders.size(); col_idx++) {
                    auto file_col_idx = col_idx;
                    auto root_reader =
                        reinterpret_cast<StructColumnReader*>(state.rootReader.get());

                    bool has_filter = false;
                    root_reader->getChildReader(file_col_idx)
                        ->RegisterPrefetch(trans, !(lazy_fetch && !has_filter));
                }

                trans.FinalizeRegistration();

                if (!lazy_fetch) {
                    trans.PrefetchRegistered();
                }
            }
        }
        return true;
    }

    auto this_output_chunk_rows = std::min<uint64_t>(
        common::DEFAULT_VECTOR_CAPACITY, getGroup(state).num_rows - state.group_offset);
    result.state->selVector->selectedSize = this_output_chunk_rows;

    if (this_output_chunk_rows == 0) {
        state.finished = true;
        return false; // end of last group, we are done
    }

    // we evaluate simple table filters directly in this scan so we can skip decoding column data
    // that's never going to be relevant
    parquet_filter_t filter_mask;
    filter_mask.set();

    // mask out unused part of bitset
    for (auto i = this_output_chunk_rows; i < common::DEFAULT_VECTOR_CAPACITY; i++) {
        filter_mask.set(i, false);
    }

    state.define_buf.zero();
    state.repeat_buf.zero();

    auto define_ptr = (uint8_t*)state.define_buf.ptr;
    auto repeat_ptr = (uint8_t*)state.repeat_buf.ptr;

    auto root_reader = reinterpret_cast<StructColumnReader*>(state.rootReader.get());

    for (auto col_idx = 0u; col_idx < metadata->column_orders.size(); col_idx++) {
        auto file_col_idx = col_idx;
        auto result_vector = result.getValueVector(col_idx);
        auto child_reader = root_reader->getChildReader(file_col_idx);
        auto rows_read = child_reader->Read(result_vector->state->selVector->selectedSize,
            filter_mask, define_ptr, repeat_ptr, result_vector.get());
        if (rows_read != result.state->selVector->selectedSize) {
            throw common::CopyException(common::StringUtils::string_format(
                "Mismatch in parquet read for column {}, expected {} rows, got {}", file_col_idx,
                result.state->selVector->selectedSize, rows_read));
        }
    }

    state.group_offset += this_output_chunk_rows;
    return true;
}

uint64_t ParquetReader::GetGroupCompressedSize(ParquetReaderScanState& state) {
    auto& group = getGroup(state);
    auto total_compressed_size = group.total_compressed_size;

    uint64_t calc_compressed_size = 0;

    // If the global total_compressed_size is not set, we can still calculate it
    if (group.total_compressed_size == 0) {
        for (auto& column_chunk : group.columns) {
            calc_compressed_size += column_chunk.meta_data.total_compressed_size;
        }
    }

    if (total_compressed_size != 0 && calc_compressed_size != 0 &&
        (uint64_t)total_compressed_size != calc_compressed_size) {
        throw common::CopyException(
            "mismatch between calculated compressed size and reported compressed size");
    }

    return total_compressed_size ? total_compressed_size : calc_compressed_size;
}

uint64_t ParquetReader::GetGroupOffset(ParquetReaderScanState& state) {
    auto& group = getGroup(state);
    uint64_t min_offset = UINT64_MAX;

    for (auto& column_chunk : group.columns) {
        if (column_chunk.meta_data.__isset.dictionary_page_offset) {
            min_offset =
                std::min<uint64_t>(min_offset, column_chunk.meta_data.dictionary_page_offset);
        }
        if (column_chunk.meta_data.__isset.index_page_offset) {
            min_offset = std::min<uint64_t>(min_offset, column_chunk.meta_data.index_page_offset);
        }
        min_offset = std::min<uint64_t>(min_offset, column_chunk.meta_data.data_page_offset);
    }

    return min_offset;
}

} // namespace processor
} // namespace kuzu
