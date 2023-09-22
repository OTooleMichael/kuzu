#include "processor/operator/persistent/reader_functions.h"

#include "processor/operator/persistent/reader/parquet_reader.h"

using namespace kuzu::common;
using namespace kuzu::catalog;
using namespace kuzu::storage;
using namespace kuzu::binder;

namespace kuzu {
namespace processor {

validate_func_t ReaderFunctions::getValidateFunc(FileType fileType) {
    switch (fileType) {
    case FileType::CSV:
        return validateCSVFiles;
    case FileType::PARQUET:
        return validateParquetFiles;
    case FileType::NPY:
        return validateNPYFiles;
    case FileType::TURTLE:
        return validateRDFFiles;
    default:
        throw NotImplementedException{"ReaderFunctions::getValidateFunc"};
    }
}

count_blocks_func_t ReaderFunctions::getCountBlocksFunc(FileType fileType, TableType tableType) {
    switch (fileType) {
    case FileType::CSV: {
        switch (tableType) {
        case TableType::NODE:
            return countRowsInNodeCSVFile;
        case TableType::REL:
            return countRowsInRelCSVFile;
        default:
            throw NotImplementedException{"ReaderFunctions::getCountBlocksFunc"};
        }
    }
    case FileType::PARQUET: {
        switch (tableType) {
        case TableType::NODE:
            return countRowsInNodeParquetFile;
        case TableType::REL:
            return countRowsInRelParquetFile;
        default:
            throw NotImplementedException{"ReaderFunctions::getCountBlocksFunc"};
        }
    }
    case FileType::NPY: {
        return countRowsInNPYFile;
    }
    case FileType::TURTLE: {
        return countRowsInRDFFile;
    }
    default: {
        throw NotImplementedException{"ReaderFunctions::getRowsCounterFunc"};
    }
    }
}

init_reader_data_func_t ReaderFunctions::getInitDataFunc(FileType fileType, TableType tableType) {
    switch (fileType) {
    case FileType::CSV: {
        switch (tableType) {
        case TableType::NODE:
            return initNodeCSVReadData;
        case TableType::REL:
            return initRelCSVReadData;
        default:
            throw NotImplementedException{"ReaderFunctions::getInitDataFunc"};
        }
    }
    case FileType::PARQUET: {
        switch (tableType) {
        case TableType::NODE:
            return initNodeParquetReadData;
        case TableType::REL:
            return initRelParquetReadData;
        default:
            throw NotImplementedException{"ReaderFunctions::getInitDataFunc"};
        }
    }
    case FileType::NPY: {
        return initNPYReadData;
    }
    case FileType::TURTLE: {
        return initRDFReadData;
    }
    default: {
        throw NotImplementedException{"ReaderFunctions::getInitDataFunc"};
    }
    }
}

read_rows_func_t ReaderFunctions::getReadRowsFunc(FileType fileType, common::TableType tableType) {
    switch (fileType) {
    case FileType::CSV: {
        switch (tableType) {
        case TableType::NODE:
            return readRowsFromNodeCSVFile;
        case TableType::REL:
            return readRowsFromRelCSVFile;
        default:
            throw NotImplementedException{"ReaderFunctions::getReadRowsFunc"};
        }
    }
    case FileType::PARQUET: {
        switch (tableType) {
        case TableType::NODE:
            return readRowsFromNodeParquetFile;
        case TableType::REL:
            return readRowsFromRelParquetFile;
        default:
            throw NotImplementedException{"ReaderFunctions::getReadRowsFunc"};
        }
    }
    case FileType::NPY: {
        return readRowsFromNPYFile;
    }
    case FileType::TURTLE: {
        return readRowsFromRDFFile;
    }
    default: {
        throw NotImplementedException{"ReaderFunctions::getReadRowsFunc"};
    }
    }
}

std::shared_ptr<ReaderFunctionData> ReaderFunctions::getReadFuncData(
    FileType fileType, TableType tableType) {
    switch (fileType) {
    case FileType::CSV: {
        switch (tableType) {
        case TableType::NODE:
            return std::make_shared<NodeCSVReaderFunctionData>();
        case TableType::REL:
            return std::make_shared<RelCSVReaderFunctionData>();
        default:
            throw NotImplementedException{"ReaderFunctions::getReadFuncData"};
        }
    }
    case FileType::PARQUET: {
        switch (tableType) {
        case TableType::NODE:
            return std::make_shared<NodeParquetReaderFunctionData>();
        case TableType::REL:
            return std::make_shared<RelParquetReaderFunctionData>();
        default:
            throw NotImplementedException{"ReaderFunctions::getReadFuncData"};
        }
    }
    case FileType::NPY: {
        return std::make_shared<NPYReaderFunctionData>();
    }
    case FileType::TURTLE: {
        return std::make_shared<RDFReaderFunctionData>();
    }
    default: {
        throw NotImplementedException{"ReaderFunctions::getReadFuncData"};
    }
    }
}

void ReaderFunctions::validateNPYFiles(const common::ReaderConfig& config) {
    // Validate one file for one column.
    assert(!config.filePaths.empty() && config.getNumFiles() == config.getNumColumns());
    row_idx_t numRows;
    for (auto i = 0u; i < config.getNumFiles(); i++) {
        auto reader = make_unique<NpyReader>(config.filePaths[i]);
        if (i == 0) {
            numRows = reader->getNumRows();
        }
        reader->validate(*config.columnTypes[i], numRows);
    }
}

std::vector<FileBlocksInfo> ReaderFunctions::countRowsInRelCSVFile(
    const common::ReaderConfig& config, MemoryManager* memoryManager) {
    std::vector<FileBlocksInfo> fileInfos(
        config.getNumFiles(), {INVALID_ROW_IDX, INVALID_BLOCK_IDX});
    return fileInfos;
}

static std::unique_ptr<BufferedCSVReader> createBufferedCSVReader(
    const std::string& path, const ReaderConfig& config) {
    return std::make_unique<BufferedCSVReader>(
        path, *config.csvReaderConfig, config.getNumColumns());
    ;
}

std::vector<FileBlocksInfo> ReaderFunctions::countRowsInNodeCSVFile(
    const common::ReaderConfig& config, MemoryManager* memoryManager) {
    std::vector<FileBlocksInfo> fileInfos;
    fileInfos.reserve(config.getNumFiles());
    auto dataChunk = getDataChunkToRead(config, memoryManager);
    // We should add a countNumRows() API to csvReader, so that it doesn't need to read data to
    // valueVector when counting the csv file.
    for (const auto& path : config.filePaths) {
        auto reader = createBufferedCSVReader(path, config);
        row_idx_t numRowsInFile = 0;
        block_idx_t numBlocks = 0;
        while (true) {
            dataChunk->state->selVector->selectedSize = 0;
            dataChunk->resetAuxiliaryBuffer();
            auto numRowsRead = reader->ParseCSV(*dataChunk);
            if (numRowsRead == 0) {
                break;
            }
            numRowsInFile += numRowsRead;
            numBlocks++;
        }
        FileBlocksInfo fileBlocksInfo{numRowsInFile, numBlocks};
        fileInfos.push_back(fileBlocksInfo);
    }
    return fileInfos;
}

std::vector<FileBlocksInfo> ReaderFunctions::countRowsInRelParquetFile(
    const common::ReaderConfig& config, MemoryManager* memoryManager) {
    std::vector<FileBlocksInfo> fileInfos;
    fileInfos.reserve(config.getNumFiles());
    for (const auto& path : config.filePaths) {
        std::unique_ptr<parquet::arrow::FileReader> reader =
            TableCopyUtils::createParquetReader(path, config);
        auto metadata = reader->parquet_reader()->metadata();
        FileBlocksInfo fileBlocksInfo{
            (row_idx_t)metadata->num_rows(), (block_idx_t)metadata->num_row_groups()};
        fileInfos.push_back(fileBlocksInfo);
    }
    return fileInfos;
}

std::vector<FileBlocksInfo> ReaderFunctions::countRowsInNodeParquetFile(
    const common::ReaderConfig& config, MemoryManager* memoryManager) {
    std::vector<FileBlocksInfo> fileInfos;
    fileInfos.reserve(config.filePaths.size());
    for (const auto& path : config.filePaths) {
        auto reader = std::make_unique<ParquetReader>(path);
        auto numRows = reader->metadata->num_rows;
        FileBlocksInfo fileBlocksInfo{
            (row_idx_t)numRows, (block_idx_t)(numRows / DEFAULT_VECTOR_CAPACITY +
                                              (numRows % DEFAULT_VECTOR_CAPACITY > 0 ? 1 : 0))};
        fileInfos.push_back(fileBlocksInfo);
    }
    return fileInfos;
}

std::vector<FileBlocksInfo> ReaderFunctions::countRowsInNPYFile(
    const common::ReaderConfig& config, MemoryManager* memoryManager) {
    assert(config.getNumFiles() != 0);
    auto reader = make_unique<NpyReader>(config.filePaths[0]);
    auto numRows = reader->getNumRows();
    auto numBlocks =
        (block_idx_t)((numRows + DEFAULT_VECTOR_CAPACITY - 1) / DEFAULT_VECTOR_CAPACITY);
    return {{numRows, numBlocks}};
}

std::vector<FileBlocksInfo> ReaderFunctions::countRowsInRDFFile(
    const common::ReaderConfig& config, MemoryManager* memoryManager) {
    assert(config.getNumFiles() == 1);
    auto reader = make_unique<RDFReader>(config.filePaths[0]);
    auto dataChunk = std::make_unique<DataChunk>(3);
    dataChunk->insert(0, std::make_unique<ValueVector>(LogicalTypeID::STRING, memoryManager));
    dataChunk->insert(1, std::make_unique<ValueVector>(LogicalTypeID::STRING, memoryManager));
    dataChunk->insert(2, std::make_unique<ValueVector>(LogicalTypeID::STRING, memoryManager));
    row_idx_t numRowsInFile = 0;
    block_idx_t numBlocks = 0;
    while (true) {
        dataChunk->resetAuxiliaryBuffer();
        auto numRowsRead = reader->read(dataChunk.get());
        if (numRowsRead == 0) {
            break;
        }
        numRowsInFile += numRowsRead;
        numBlocks++;
    }
    FileBlocksInfo fileBlocksInfo{numRowsInFile, numBlocks};
    return {fileBlocksInfo};
}

void ReaderFunctions::initRelCSVReadData(
    ReaderFunctionData& funcData, vector_idx_t fileIdx, const common::ReaderConfig& config) {
    assert(fileIdx < config.getNumFiles());
    funcData.fileIdx = fileIdx;
    reinterpret_cast<RelCSVReaderFunctionData&>(funcData).reader =
        TableCopyUtils::createRelTableCSVReader(config.filePaths[fileIdx], config);
}

void ReaderFunctions::initNodeCSVReadData(
    ReaderFunctionData& funcData, vector_idx_t fileIdx, const common::ReaderConfig& config) {
    assert(fileIdx < config.getNumFiles());
    funcData.fileIdx = fileIdx;
    reinterpret_cast<NodeCSVReaderFunctionData&>(funcData).reader =
        createBufferedCSVReader(config.filePaths[fileIdx], config);
}

void ReaderFunctions::initRelParquetReadData(
    ReaderFunctionData& funcData, vector_idx_t fileIdx, const common::ReaderConfig& config) {
    assert(fileIdx < config.getNumFiles());
    funcData.fileIdx = fileIdx;
    reinterpret_cast<RelCSVReaderFunctionData&>(funcData).reader =
        TableCopyUtils::createRelTableCSVReader(config.filePaths[fileIdx], config);
}

void ReaderFunctions::initNodeParquetReadData(
    ReaderFunctionData& funcData, vector_idx_t fileIdx, const common::ReaderConfig& config) {
    assert(fileIdx < config.getNumFiles());
    funcData.fileIdx = fileIdx;
    reinterpret_cast<NodeParquetReaderFunctionData&>(funcData).reader =
        std::make_unique<ParquetReader>(config.filePaths[fileIdx]);
}

void ReaderFunctions::initNPYReadData(
    ReaderFunctionData& funcData, vector_idx_t fileIdx, const common::ReaderConfig& config) {
    funcData.fileIdx = fileIdx;
    reinterpret_cast<NPYReaderFunctionData&>(funcData).reader =
        make_unique<NpyMultiFileReader>(config.filePaths);
}

void ReaderFunctions::initRDFReadData(
    ReaderFunctionData& funcData, vector_idx_t fileIdx, const common::ReaderConfig& config) {
    funcData.fileIdx = fileIdx;
    reinterpret_cast<RDFReaderFunctionData&>(funcData).reader =
        make_unique<RDFReader>(config.filePaths[0]);
}

void ReaderFunctions::readRowsFromRelCSVFile(
    const ReaderFunctionData& functionData, block_idx_t blockIdx, DataChunk* dataChunkToRead) {
    auto& readerData = reinterpret_cast<const RelCSVReaderFunctionData&>(functionData);
    std::shared_ptr<arrow::RecordBatch> recordBatch;
    TableCopyUtils::throwCopyExceptionIfNotOK(readerData.reader->ReadNext(&recordBatch));
    if (recordBatch == nullptr) {
        dataChunkToRead->state->selVector->selectedSize = 0;
        return;
    }
    for (auto i = 0u; i < dataChunkToRead->getNumValueVectors(); i++) {
        ArrowColumnVector::setArrowColumn(dataChunkToRead->getValueVector(i).get(),
            std::make_shared<arrow::ChunkedArray>(recordBatch->column((int)i)));
    }
    dataChunkToRead->state->selVector->selectedSize = recordBatch->num_rows();
}

void ReaderFunctions::readRowsFromNodeCSVFile(
    const ReaderFunctionData& functionData, block_idx_t blockIdx, DataChunk* dataChunkToRead) {
    auto& readerData = reinterpret_cast<const NodeCSVReaderFunctionData&>(functionData);
    readerData.reader->ParseCSV(*dataChunkToRead);
}

void ReaderFunctions::readRowsFromRelParquetFile(const ReaderFunctionData& functionData,
    block_idx_t blockIdx, common::DataChunk* dataChunkToRead) {
    auto& readerData = reinterpret_cast<const RelParquetReaderFunctionData&>(functionData);
    std::shared_ptr<arrow::Table> table;
    TableCopyUtils::throwCopyExceptionIfNotOK(
        readerData.reader->RowGroup(static_cast<int>(blockIdx))->ReadTable(&table));
    assert(table);
    for (auto i = 0u; i < dataChunkToRead->getNumValueVectors(); i++) {
        ArrowColumnVector::setArrowColumn(
            dataChunkToRead->getValueVector(i).get(), table->column((int)i));
    }
    dataChunkToRead->state->selVector->selectedSize = table->num_rows();
}

void ReaderFunctions::readRowsFromNodeParquetFile(const ReaderFunctionData& functionData,
    block_idx_t blockIdx, common::DataChunk* dataChunkToRead) {
    auto& readerData = reinterpret_cast<const NodeParquetReaderFunctionData&>(functionData);
    ParquetReaderScanState state;
    readerData.reader->initializeScan(state, {0});
    readerData.reader->scan(state, *dataChunkToRead);
    auto val = dataChunkToRead->getValueVector(0);
    auto d0 = val->getValue<int64_t>(0);
    auto d1 = val->getValue<int64_t>(1);
    auto d2 = val->getValue<int64_t>(2);
    auto d3 = val->getValue<int64_t>(3);
    auto c = 5;
}

void ReaderFunctions::readRowsFromNPYFile(const ReaderFunctionData& functionData,
    common::block_idx_t blockIdx, common::DataChunk* dataChunkToRead) {
    auto& readerData = reinterpret_cast<const NPYReaderFunctionData&>(functionData);
    auto recordBatch = readerData.reader->readBlock(blockIdx);
    for (auto i = 0u; i < dataChunkToRead->getNumValueVectors(); i++) {
        ArrowColumnVector::setArrowColumn(dataChunkToRead->getValueVector(i).get(),
            std::make_shared<arrow::ChunkedArray>(recordBatch->column((int)i)));
    }
    dataChunkToRead->state->selVector->selectedSize = recordBatch->num_rows();
}

void ReaderFunctions::readRowsFromRDFFile(const ReaderFunctionData& functionData,
    common::block_idx_t blockIdx, common::DataChunk* dataChunkToRead) {
    auto& readerData = reinterpret_cast<const RDFReaderFunctionData&>(functionData);
    readerData.reader->read(dataChunkToRead);
}

std::unique_ptr<common::DataChunk> ReaderFunctions::getDataChunkToRead(
    const common::ReaderConfig& config, MemoryManager* memoryManager) {
    std::vector<std::unique_ptr<ValueVector>> valueVectorsToRead;
    for (auto& dataType : config.columnTypes) {
        if (dataType->getLogicalTypeID() != LogicalTypeID::SERIAL) {
            valueVectorsToRead.emplace_back(
                std::make_unique<ValueVector>(*dataType, memoryManager));
        }
    }
    auto dataChunk = std::make_unique<DataChunk>(valueVectorsToRead.size());
    for (auto i = 0u; i < valueVectorsToRead.size(); i++) {
        dataChunk->insert(i, std::move(valueVectorsToRead[i]));
    }
    return dataChunk;
}

} // namespace processor
} // namespace kuzu
