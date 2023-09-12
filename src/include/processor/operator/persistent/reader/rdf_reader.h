#pragma once

#include "common/data_chunk/data_chunk.h"
#include "common/types/internal_id_t.h"
#include "common/vector/value_vector.h"
#include "serd.h"
#include "spdlog/spdlog.h"

using namespace kuzu::common;

namespace spdlog {
class logger;
}

namespace kuzu {
namespace storage {

class RDFReader {
public:
    explicit RDFReader(std::string filePath);

    ~RDFReader();

    offset_t read(DataChunk* dataChunkToRead);

    inline offset_t getFileSize() const { return fileSize; }

private:
    static SerdStatus errorHandle(void* handle, const SerdError* error);
    static SerdStatus handleStatements(void* handle, SerdStatementFlags flags,
        const SerdNode* graph, const SerdNode* subject, const SerdNode* predicate,
        const SerdNode* object, const SerdNode* object_datatype, const SerdNode* object_lang);
    static bool isSerdTypeSupported(SerdType serdType);

private:
    const std::string filePath;
    FILE* fp;
    offset_t fileSize;
    SerdReader* reader;
    offset_t numLinesRead;
    SerdStatus status;
    ValueVector* subjectVector;
    ValueVector* predicateVector;
    ValueVector* objectVector;
};

} // namespace storage
} // namespace kuzu
