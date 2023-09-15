#pragma once

#include "base_csv_reader.h"

namespace kuzu {
namespace processor {

//! ParallelCSVReader is a class that reads values from a stream in parallel.
class ParallelCSVReader final : public BaseCSVReader {
public:
    static constexpr uint64_t PARALLEL_BLOCK_SIZE = 1048576;

    ParallelCSVReader(const std::string& filePath, const common::ReaderConfig& readerConfig);

    uint64_t ContinueBlock(common::DataChunk& resultChunk);

protected:
    void parseBlockHook() override;
    void handleQuotedNewline() const override;
    bool finishedBlockDetail() const override;
    bool finishedAfterHeader() const override;
};

} // namespace processor
} // namespace kuzu
