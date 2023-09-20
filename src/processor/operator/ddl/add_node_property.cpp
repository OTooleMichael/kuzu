#include "processor/operator/ddl/add_node_property.h"

namespace kuzu {
namespace processor {

void AddNodeProperty::executeDDLInternal() {
    auto metadataDAHInfo = storageManager.createMetadataDAHInfo(*dataType);
    catalog->addNodeProperty(
        tableID, propertyName, std::move(dataType), std::move(metadataDAHInfo));
    auto schema = catalog->getWriteVersion()->getTableSchema(tableID);
    auto addedPropID = schema->getPropertyID(propertyName);
    auto addedProp = schema->getProperty(addedPropID);
    storageManager.getNodesStore().getNodeTable(tableID)->addColumn(
        *addedProp, getDefaultValVector(), transaction);
}

} // namespace processor
} // namespace kuzu
