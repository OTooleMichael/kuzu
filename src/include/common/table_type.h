#pragma once

#include <cstdint>
#include <string>

namespace kuzu {
namespace common {

enum class TableType : uint8_t {
    NODE = 0,
    REL = 1,
    RDF = 2,
    REL_GROUP = 3,
};

struct TableTypeUtils {
    static std::string toString(TableType tableType);
};

} // namespace common
} // namespace kuzu
