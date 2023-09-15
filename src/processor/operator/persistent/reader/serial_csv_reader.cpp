#include "processor/operator/persistent/reader/serial_csv_reader.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

SerialCSVReader::SerialCSVReader(
    const std::string& filePath, const common::ReaderConfig& readerConfig)
    : BaseCSVReader{filePath, readerConfig} {}

void SerialCSVReader::SniffCSV() {
    mode = ParserMode::SNIFFING_DIALECT;
    DataChunk dummyChunk(0);
    ParseCSV(dummyChunk);
}

bool SerialCSVReader::finishedBlockDetail() const {
    // Never stop until we fill the chunk.
    return false;
}

bool SerialCSVReader::finishedAfterHeader() const {
    // Never stop after parsing the header.
    return false;
}

} // namespace processor
} // namespace kuzu
