#pragma once

#include "column_reader.h"
#include "common/data_chunk/data_chunk.h"
#include "common/types/types.h"
//#include "list_column_reader.h"
#include "parquet/parquet_types.h"
#include "thrift_tools.h"
//#include "struct_column_reader.h"

namespace kuzu {
namespace processor {

// struct ParquetReaderScanState {
//    vector<idx_t> group_idx_list;
//    int64_t current_group;
//    idx_t group_offset;
//    unique_ptr<FileHandle> file_handle;
//    unique_ptr<ColumnReader> root_reader;
//    unique_ptr<duckdb_apache::thrift::protocol::TProtocol> thrift_file_proto;
//
//    bool finished;
//    SelectionVector sel;
//
//    ResizeableBuffer define_buf;
//    ResizeableBuffer repeat_buf;
//
//    bool prefetch_mode = false;
//    bool current_group_prefetched = false;
//};

class ParquetReader {
public:
    ParquetReader(const std::string& filePath);
    ~ParquetReader();

    std::string filePath;
    //    std::vector<common::LogicalType> return_types;
    //    std::vector<std::string> names;
    std::unique_ptr<parquet::format::FileMetaData> metadata;
    std::unique_ptr<ColumnReader> root_reader;

public:
    //    void InitializeScan(ParquetReaderScanState& state, vector<idx_t> groups_to_read);
    //    void Scan(ParquetReaderScanState& state, DataChunk& output);

    //    inline uint64_t getNumRows() const { return metadata->num_rows; }
    //
    //    inline uint64_t getNumRowsGroups() const { return metadata->row_groups.size(); }
    //
    //    const ParquetFileMetadata* GetFileMetadata();
    //
    //    unique_ptr<BaseStatistics> ReadStatistics(const std::string& name);
    //    static std::unique_ptr<common::LogicalType> DeriveLogicalType(
    //        const parquet::format::SchemaElement& s_ele);
    //
    //    FileHandle& GetHandle() { return *file_handle; }
    //
    //    const std::string& GetFileName() { return filePath; }

private:
    void InitializeSchema();
    // bool ScanInternal(ParquetReaderScanState& state, common::DataChunk& output);
    std::unique_ptr<ColumnReader> createReader();

    std::unique_ptr<ColumnReader> createReaderRecursive(uint64_t depth, uint64_t maxDefine,
        uint64_t maxRepeat, uint64_t& nextSchemaIdx, uint64_t& nextFileIdx);
    //    const parquet::format::RowGroup& GetGroup(ParquetReaderScanState& state);
    //    uint64_t GetGroupCompressedSize(ParquetReaderScanState& state);
    //    common::offset_t GetGroupOffset(ParquetReaderScanState& state);
    //    // Group span is the distance between the min page offset and the max page offset plus the
    //    max
    //    // page compressed size
    //    uint64_t GetGroupSpan(ParquetReaderScanState& state);
    //    void PrepareRowGroupBuffer(ParquetReaderScanState& state, common::column_id_t
    //    out_col_idx);

    void initMetadata();

    inline std::unique_ptr<apache::thrift::protocol::TProtocol> createThriftProtocol(
        bool prefetch_mode) {
        return std::make_unique<apache::thrift::protocol::TCompactProtocolT<ThriftFileTransport>>(
            std::make_shared<ThriftFileTransport>(fileInfo.get(), prefetch_mode));
    }

private:
    std::unique_ptr<common::FileInfo> fileInfo;
};

} // namespace processor
} // namespace kuzu
