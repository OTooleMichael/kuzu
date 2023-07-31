#pragma once

#include <algorithm>
#include <unordered_map>
#include <unordered_set>

#include "common/constants.h"
#include "common/exception.h"
#include "common/file_utils.h"
#include "common/rel_direction.h"
#include "common/types/types_include.h"

namespace kuzu {
namespace storage {
class BMFileHandle;
}

namespace catalog {

enum class TableType : uint8_t { NODE, REL, INVALID };

enum class RelMultiplicity : uint8_t { MANY_MANY, MANY_ONE, ONE_MANY, ONE_ONE };
RelMultiplicity getRelMultiplicityFromString(const std::string& relMultiplicityString);
std::string getRelMultiplicityAsString(RelMultiplicity relMultiplicity);

// DAH is the abbreviation for Disk Array Header.
struct MetadataDAHInfo {
    common::page_idx_t dataDAHPageIdx = common::INVALID_PAGE_IDX;
    common::page_idx_t nullDAHPageIdx = common::INVALID_PAGE_IDX;
    std::vector<MetadataDAHInfo> childrenInfos;

    void serialize(common::FileInfo* fileInfo, uint64_t& offset) const;
    static std::unique_ptr<MetadataDAHInfo> deserialize(
        common::FileInfo* fileInfo, uint64_t& offset);
};

class Property {
public:
    static constexpr std::string_view REL_FROM_PROPERTY_NAME = "_FROM_";
    static constexpr std::string_view REL_TO_PROPERTY_NAME = "_TO_";

    Property(std::string name, common::LogicalType dataType)
        : Property{std::move(name), std::move(dataType), common::INVALID_PROPERTY_ID,
              common::INVALID_TABLE_ID} {}
    Property(std::string name, common::LogicalType dataType, common::property_id_t propertyID,
        common::table_id_t tableID)
        : name{std::move(name)}, dataType{std::move(dataType)},
          propertyID{propertyID}, tableID{tableID} {}

    void serialize(common::FileInfo* fileInfo, uint64_t& offset) const;
    static std::unique_ptr<Property> deserialize(common::FileInfo* fileInfo, uint64_t& offset);
    static void initMetadataDAHInfo(const common::LogicalType& dataType,
        storage::BMFileHandle* metadataFH, MetadataDAHInfo& metadataDAHInfo);

public:
    std::string name;
    common::LogicalType dataType;
    common::property_id_t propertyID;
    common::table_id_t tableID;
    MetadataDAHInfo metadataDAHInfo;
};

class TableSchema {
public:
    TableSchema(std::string tableName, common::table_id_t tableID, TableType tableType,
        std::vector<Property> properties)
        : tableName{std::move(tableName)}, tableID{tableID}, tableType{tableType},
          properties{std::move(properties)}, nextPropertyID{
                                                 (common::property_id_t)this->properties.size()} {}

    virtual ~TableSchema() = default;

    static bool isReservedPropertyName(const std::string& propertyName);

    inline uint32_t getNumProperties() const { return properties.size(); }

    inline void dropProperty(common::property_id_t propertyID) {
        properties.erase(std::remove_if(properties.begin(), properties.end(),
                             [propertyID](const Property& property) {
                                 return property.propertyID == propertyID;
                             }),
            properties.end());
    }

    inline bool containProperty(std::string propertyName) const {
        return std::any_of(properties.begin(), properties.end(),
            [&propertyName](const Property& property) { return property.name == propertyName; });
    }
    inline const std::vector<Property>& getProperties() const { return properties; }
    inline TableType getTableType() const { return tableType; }

    void addProperty(
        std::string propertyName, common::LogicalType dataType, storage::BMFileHandle* metadataFH);

    std::string getPropertyName(common::property_id_t propertyID) const;

    common::property_id_t getPropertyID(const std::string& propertyName) const;

    Property getProperty(common::property_id_t propertyID) const;

    void renameProperty(common::property_id_t propertyID, const std::string& newName);

    void serialize(common::FileInfo* fileInfo, uint64_t& offset);
    static std::unique_ptr<TableSchema> deserialize(common::FileInfo* fileInfo, uint64_t& offset);

private:
    inline common::property_id_t increaseNextPropertyID() { return nextPropertyID++; }

    virtual void serializeInternal(common::FileInfo* fileInfo, uint64_t& offset) = 0;

public:
    std::string tableName;
    common::table_id_t tableID;
    TableType tableType;
    std::vector<Property> properties;
    common::property_id_t nextPropertyID;
};

class NodeTableSchema : public TableSchema {
public:
    NodeTableSchema(common::property_id_t primaryPropertyId,
        std::unordered_set<common::table_id_t> fwdRelTableIDSet,
        std::unordered_set<common::table_id_t> bwdRelTableIDSet)
        : TableSchema{"", common::INVALID_TABLE_ID, TableType::NODE, std::vector<Property>{}},
          primaryKeyPropertyID{primaryPropertyId}, fwdRelTableIDSet{std::move(fwdRelTableIDSet)},
          bwdRelTableIDSet{std::move(bwdRelTableIDSet)} {}
    NodeTableSchema(std::string tableName, common::table_id_t tableID,
        common::property_id_t primaryPropertyId, std::vector<Property> properties)
        : TableSchema{std::move(tableName), tableID, TableType::NODE, std::move(properties)},
          primaryKeyPropertyID{primaryPropertyId} {}

    inline void addFwdRelTableID(common::table_id_t tableID) { fwdRelTableIDSet.insert(tableID); }
    inline void addBwdRelTableID(common::table_id_t tableID) { bwdRelTableIDSet.insert(tableID); }

    inline Property getPrimaryKey() const { return properties[primaryKeyPropertyID]; }

    static std::unique_ptr<NodeTableSchema> deserialize(
        common::FileInfo* fileInfo, uint64_t& offset);

private:
    void serializeInternal(common::FileInfo* fileInfo, uint64_t& offset) final;

public:
    // TODO(Semih): When we support updating the schemas, we need to update this or, we need
    // a more robust mechanism to keep track of which property is the primary key (e.g., store this
    // information with the property). This is an idx, not an ID, so as the columns/properties of
    // the table change, the idx can change.
    common::property_id_t primaryKeyPropertyID;
    std::unordered_set<common::table_id_t> fwdRelTableIDSet; // srcNode->rel
    std::unordered_set<common::table_id_t> bwdRelTableIDSet; // dstNode->rel
};

class RelTableSchema : public TableSchema {
public:
    static constexpr uint64_t INTERNAL_REL_ID_PROPERTY_ID = 0;

    RelTableSchema(RelMultiplicity relMultiplicity, common::table_id_t srcTableID,
        common::table_id_t dstTableID, common::LogicalType srcPKDataType,
        common::LogicalType dstPKDataType)
        : TableSchema{"", common::INVALID_TABLE_ID, TableType::REL, {} /* properties */},
          relMultiplicity{relMultiplicity}, srcTableID{srcTableID}, dstTableID{dstTableID},
          srcPKDataType{std::move(srcPKDataType)}, dstPKDataType{std::move(dstPKDataType)} {}
    RelTableSchema(std::string tableName, common::table_id_t tableID,
        RelMultiplicity relMultiplicity, std::vector<Property> properties,
        common::table_id_t srcTableID, common::table_id_t dstTableID,
        common::LogicalType srcPKDataType, common::LogicalType dstPKDataType)
        : TableSchema{std::move(tableName), tableID, TableType::REL, std::move(properties)},
          relMultiplicity{relMultiplicity}, srcTableID{srcTableID}, dstTableID{dstTableID},
          srcPKDataType{std::move(srcPKDataType)}, dstPKDataType{std::move(dstPKDataType)} {}

    inline bool isSingleMultiplicityInDirection(common::RelDataDirection direction) const {
        return relMultiplicity == RelMultiplicity::ONE_ONE ||
               relMultiplicity == (direction == common::RelDataDirection::FWD ?
                                          RelMultiplicity::MANY_ONE :
                                          RelMultiplicity::ONE_MANY);
    }

    inline bool isSrcOrDstTable(common::table_id_t tableID) const {
        return srcTableID == tableID || dstTableID == tableID;
    }

    inline common::table_id_t getBoundTableID(common::RelDataDirection relDirection) const {
        return relDirection == common::RelDataDirection::FWD ? srcTableID : dstTableID;
    }

    inline common::table_id_t getNbrTableID(common::RelDataDirection relDirection) const {
        return relDirection == common::RelDataDirection::FWD ? dstTableID : srcTableID;
    }

    static std::unique_ptr<RelTableSchema> deserialize(
        common::FileInfo* fileInfo, uint64_t& offset);

private:
    void serializeInternal(common::FileInfo* fileInfo, uint64_t& offset) final;

public:
    RelMultiplicity relMultiplicity;
    common::table_id_t srcTableID;
    common::table_id_t dstTableID;
    common::LogicalType srcPKDataType;
    common::LogicalType dstPKDataType;
};

} // namespace catalog
} // namespace kuzu
