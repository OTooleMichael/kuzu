#pragma once

#include "column_reader.h"
#include "common/data_chunk/data_chunk.h"
#include "common/types/types.h"
#include "parquet/parquet_types.h"
#include "resizable_buffer.h"
#include "struct_column_reader.h"
#include "thrift_tools.h"

namespace kuzu {
namespace processor {

struct ParquetReaderPrefetchConfig {
    // Percentage of data in a row group span that should be scanned for enabling whole group
    // prefetch
    static constexpr double WHOLE_GROUP_PREFETCH_MINIMUM_SCAN = 0.95;
};

struct ParquetReaderScanState {
    std::vector<uint64_t> group_idx_list;
    int64_t current_group = -1;
    uint64_t group_offset;
    std::unique_ptr<common::FileInfo> fileInfo;
    std::unique_ptr<ColumnReader> rootReader;
    std::unique_ptr<kuzu_apache::thrift::protocol::TProtocol> thriftFileProto;

    bool finished;

    ResizeableBuffer define_buf;
    ResizeableBuffer repeat_buf;

    bool prefetch_mode = false;
    bool current_group_prefetched = false;
};

class ParquetReader {
public:
    ParquetReader(const std::string& filePath);
    ~ParquetReader() = default;

    std::string filePath;
    //    std::vector<common::LogicalType> return_types;
    //    std::vector<std::string> names;
    std::unique_ptr<kuzu_parquet::format::FileMetaData> metadata;
    // std::unique_ptr<ColumnReader> root_reader;

public:
    void initializeScan(ParquetReaderScanState& state, std::vector<uint64_t> groups_to_read);
    bool scanInternal(ParquetReaderScanState& state, common::DataChunk& result);
    //
    //    unique_ptr<BaseStatistics> ReadStatistics(const std::string& name);
    static std::unique_ptr<common::LogicalType> deriveLogicalType(
        const kuzu_parquet::format::SchemaElement& s_ele);
    //
    //    FileHandle& GetHandle() { return *file_handle; }
    //
    //    const std::string& GetFileName() { return filePath; }

    void scan(ParquetReaderScanState& state, common::DataChunk& result) {
        while (scanInternal(state, result)) {
            if (result.state->selVector->selectedSize > 0) {
                break;
            }
        }
    }

    const kuzu_parquet::format::RowGroup& getGroup(ParquetReaderScanState& state) {
        assert(
            state.current_group >= 0 && (int64_t)state.current_group < state.group_idx_list.size());
        assert(state.group_idx_list[state.current_group] >= 0 &&
               state.group_idx_list[state.current_group] < metadata->row_groups.size());
        return metadata->row_groups[state.group_idx_list[state.current_group]];
    }

private:
    // void InitializeSchema();
    // bool ScanInternal(ParquetReaderScanState& state, common::DataChunk& output);

    //    const parquet::format::RowGroup& GetGroup(ParquetReaderScanState& state);
    //    uint64_t GetGroupCompressedSize(ParquetReaderScanState& state);
    //    common::offset_t GetGroupOffset(ParquetReaderScanState& state);
    //    // Group span is the distance between the min page offset and the max page offset plus the
    //    max
    //    // page compressed size
    //    uint64_t GetGroupSpan(ParquetReaderScanState& state);
    //    void PrepareRowGroupBuffer(ParquetReaderScanState& state, common::column_id_t
    //    out_col_idx);

    inline std::unique_ptr<kuzu_apache::thrift::protocol::TProtocol> createThriftProtocol(
        common::FileInfo* fileInfo_, bool prefetch_mode) {
        return std::make_unique<
            kuzu_apache::thrift::protocol::TCompactProtocolT<ThriftFileTransport>>(
            std::make_shared<ThriftFileTransport>(fileInfo_, prefetch_mode));
    }

    void initMetadata();

    std::unique_ptr<ColumnReader> createReader();

    std::unique_ptr<ColumnReader> createReaderRecursive(uint64_t depth, uint64_t maxDefine,
        uint64_t maxRepeat, uint64_t& nextSchemaIdx, uint64_t& nextFileIdx);

    void prepareRowGroupBuffer(ParquetReaderScanState& state, uint64_t col_idx);

    // Group span is the distance between the min page offset and the max page offset plus the max
    // page compressed size
    uint64_t GetGroupSpan(ParquetReaderScanState& state);

    uint64_t GetGroupCompressedSize(ParquetReaderScanState& state);

    uint64_t GetGroupOffset(ParquetReaderScanState& state);

private:
    std::unique_ptr<common::FileInfo> fileInfo;
};

} // namespace processor
} // namespace kuzu
