#include "planner/operator/scan/logical_scan_node.h"
#include "processor/operator/scan_node_id.h"
#include "processor/plan_mapper.h"

using namespace kuzu::planner;

namespace kuzu {
namespace processor {
// TODO:rename file

std::unique_ptr<PhysicalOperator> PlanMapper::mapScanInternalID(LogicalOperator* logicalOperator) {
    auto logicalScan = (LogicalScanInternalID*)logicalOperator;
    auto outSchema = logicalScan->getSchema();
    auto& nodesStore = storageManager.getNodesStore();
    auto dataPos = DataPos(outSchema->getExpressionPos(*logicalScan->getInternalID()));
    auto sharedState = std::make_shared<ScanNodeIDSharedState>();
    for (auto& tableID : logicalScan->getTableIDs()) {
        auto nodeTable = nodesStore.getNodeTable(tableID);
        sharedState->addTableState(nodeTable);
    }
    return make_unique<ScanNodeID>(
        dataPos, sharedState, getOperatorID(), logicalScan->getExpressionsForPrinting());
}

} // namespace processor
} // namespace kuzu
