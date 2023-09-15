#pragma once

#include <cstdint>
#include <string>

#include "common/copier_config/copier_config.h"
#include "common/data_chunk/data_chunk.h"
#include "common/types/types.h"

namespace kuzu {
namespace processor {

enum class ParserMode : uint8_t {
    PARSING = 0,
    PARSING_HEADER = 1,
    SNIFFING_DIALECT = 2,
    INVALID = 255
};

class BaseCSVReader {
    //! Initial buffer read size; can be extended for long values.
    static constexpr uint64_t INITIAL_BUFFER_SIZE = 16384;

public:
    BaseCSVReader(const std::string& filePath, const common::ReaderConfig& readerConfig);

    virtual ~BaseCSVReader();

    uint64_t ParseBlock(common::block_idx_t blockIdx, common::DataChunk& resultChunk);

protected:
    void AddValue(common::DataChunk& resultChunk, std::string strVal, common::column_id_t columnIdx,
        std::vector<uint64_t>& escapePositions);
    void AddRow(common::DataChunk&, common::column_id_t column);

    void ReadHeader();
    //! Reads a new buffer from the CSV file.
    //! Uses the start value to ensure the current value stays within the buffer.
    //! Modifies the start value to point to the new start of the current value.
    //! Returns false if the file has been exhausted.
    bool ReadBuffer(uint64_t& start);
    uint64_t ParseCSV(common::DataChunk& resultChunk);

    inline bool isNewLine(char c) { return c == '\n' || c == '\r'; }

    // Get the file offset of the current buffer position.
    uint64_t getFileOffset() const;
    uint64_t getLineNumber();

protected:
    virtual void handleQuotedNewline() const = 0;

    //! Called when starting the parsing of a new block.
    virtual void parseBlockHook() = 0;
    virtual bool finishedBlockDetail() const = 0;
    virtual bool finishedAfterHeader() const = 0;

private:
    void copyStringToVector(common::ValueVector*, std::string);
    //! Called after a row is finished to determine if we should keep processing.
    inline bool finishedBlock() const {
        return mode != ParserMode::PARSING || rowToAdd >= common::DEFAULT_VECTOR_CAPACITY ||
               finishedBlockDetail();
    }

protected:
    std::string filePath;
    common::CSVReaderConfig& csvReaderConfig;

    uint64_t expectedNumColumns;
    uint64_t numColumnsDetected;
    int fd;

    common::block_idx_t currentBlockIdx;

    std::unique_ptr<char[]> buffer;
    uint64_t bufferSize;
    uint64_t position;

    bool rowEmpty = false;

    ParserMode mode;

    uint64_t rowToAdd;
};

} // namespace processor
} // namespace kuzu
