#include "processor/operator/persistent/reader/parallel_csv_reader.h"

#include "common/exception/copy.h"
#include "common/string_utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

ParallelCSVReader::ParallelCSVReader(
    const std::string& filePath, const common::ReaderConfig& readerConfig)
    : BaseCSVReader{filePath, readerConfig} {}

uint64_t ParallelCSVReader::ContinueBlock(common::DataChunk& resultChunk) {
    // If we haven't started the first block yet or are done our block, quit.
    if (buffer == nullptr || finishedBlockDetail()) {
        return 0;
    }
    assert(mode == ParserMode::PARSING);
    return ParseCSV(resultChunk);
}

void ParallelCSVReader::parseBlockHook() {
    uint64_t oldOffset = getFileOffset();
    if (oldOffset != 0) {
        lseek(fd, oldOffset - 1, SEEK_SET);
        char buf[1];
        read(fd, buf, 1);
        assert(isNewLine(buf[0]));
    }

    // Seek to the proper location in the file.
    if (lseek(fd, currentBlockIdx * PARALLEL_BLOCK_SIZE, SEEK_SET) == -1) {
        throw CopyException(StringUtils::string_format("Failed to seek to block {} in file {}: {}",
            currentBlockIdx, filePath, strerror(errno)));
    }

    if (currentBlockIdx == 0) {
        // First block doesn't search for a newline.
        return;
    }

    // Reset the buffer.
    uint64_t start = 0;
    position = 0;
    bufferSize = 0;
    buffer.reset();
    if (!ReadBuffer(start)) {
        return;
    }

    // Find the start of the next line.
    do {
        for (; position < bufferSize; position++) {
            if (buffer[position] == '\r') {
                position++;
                if (position >= bufferSize && ReadBuffer(start) && buffer[position] == '\n') {
                    position++;
                }
                goto exit;
            } else if (buffer[position] == '\n') {
                position++;
                goto exit;
            }
        }
    } while (ReadBuffer(start));
exit:
    if (finishedBlockDetail()) {
        throw CopyException(
            StringUtils::string_format("Line {} in {} too long for parallel CSV reader. Please "
                                       "specify PARALLEL=FALSE in the options.",
                filePath, getLineNumber()));
    }
}

void ParallelCSVReader::handleQuotedNewline() const {
    throw CopyException(StringUtils::string_format(
        "Quoted newlines are not supported in parallel CSV reader (while parsing {}). Please "
        "specify PARALLEL=FALSE in the options.",
        filePath));
}

bool ParallelCSVReader::finishedBlockDetail() const {
    // Only stop if we've ventured into the next block by at least a byte.
    // Use `>` because `position` points to just past the newline right now.
    return getFileOffset() > (currentBlockIdx + 1) * PARALLEL_BLOCK_SIZE;
}

bool ParallelCSVReader::finishedAfterHeader() const {
    if (finishedBlockDetail()) {
        // Necessary because of the logic in the Reader class, which moves to the next file if
        // `readFunc` returns 0 at any point.
        throw CopyException(StringUtils::string_format(
            "Parallel CSV Reader does not support headers longer than 1MB (while parsing {}). "
            "Please specify PARALLEL=FALSE in the options.",
            filePath));
    }
    return false;
}

} // namespace processor
} // namespace kuzu
