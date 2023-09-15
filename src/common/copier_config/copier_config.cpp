#include "common/copier_config/copier_config.h"

#include "common/exception/copy.h"

namespace kuzu {
namespace common {

const static std::unordered_map<std::string, FileType> fileTypeMap{{".csv", FileType::CSV},
    {".parquet", FileType::PARQUET}, {".npy", FileType::NPY}, {".ttl", FileType::TURTLE}};

FileType FileTypeUtils::getFileTypeFromExtension(const std::string& extension) {
    auto entry = fileTypeMap.find(extension);
    if (entry == fileTypeMap.end()) {
        throw CopyException("Unsupported file type " + extension);
    }
    return entry->second;
}

} // namespace common
} // namespace kuzu
