#pragma once

#include <atomic>
#include <mutex>

#include "storage/stats/metadata_dah_info.h"
#include "storage/stats/property_statistics.h"

namespace kuzu {
namespace catalog {
class TableSchema;
}
namespace storage {

class TableStatistics {
public:
    explicit TableStatistics(const catalog::TableSchema& schema);
    TableStatistics(common::TableType tableType, uint64_t numTuples, common::table_id_t tableID,
        std::unordered_map<common::property_id_t, std::unique_ptr<PropertyStatistics>>&&
            propertyStatistics);
    explicit TableStatistics(const TableStatistics& other);

    virtual ~TableStatistics() = default;

    inline bool isEmpty() const { return numTuples == 0; }
    inline uint64_t getNumTuples() const { return numTuples; }
    virtual inline void setNumTuples(uint64_t numTuples_) {
        assert(numTuples_ != UINT64_MAX);
        numTuples = numTuples_;
    }

    inline PropertyStatistics& getPropertyStatistics(common::property_id_t propertyID) {
        assert(propertyStatistics.contains(propertyID));
        return *(propertyStatistics.at(propertyID));
    }
    inline void setPropertyStatistics(
        common::property_id_t propertyID, PropertyStatistics newStats) {
        propertyStatistics[propertyID] = std::make_unique<PropertyStatistics>(newStats);
    }

    void serialize(common::FileInfo* fileInfo, uint64_t& offset);
    static std::unique_ptr<TableStatistics> deserialize(
        common::FileInfo* fileInfo, uint64_t& offset);
    virtual void serializeInternal(common::FileInfo* fileInfo, uint64_t& offset) = 0;

    virtual std::unique_ptr<TableStatistics> copy() = 0;

private:
    common::TableType tableType;
    uint64_t numTuples;
    common::table_id_t tableID;
    std::unordered_map<common::property_id_t, std::unique_ptr<PropertyStatistics>>
        propertyStatistics;
};

} // namespace storage
} // namespace kuzu