#pragma once

#include "mask.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

// Note: This class is not thread-safe. It relies on its caller to correctly synchronize its state.
class NodeTableScanState {
public:
    explicit NodeTableScanState(storage::NodeTable* table)
        : table{table}, maxNodeOffset{common::INVALID_OFFSET}, maxMorselIdx{UINT64_MAX},
          currentNodeOffset{0}, semiMask{std::make_unique<NodeOffsetAndMorselSemiMask>(table)} {}

    inline storage::NodeTable* getTable() { return table; }

    inline void initializeMaxOffset(transaction::Transaction* transaction) {
        maxNodeOffset = table->getMaxNodeOffset(transaction);
        maxMorselIdx = MaskUtil::getMorselIdx(maxNodeOffset);
    }

    inline bool isSemiMaskEnabled() { return semiMask->isEnabled(); }
    inline NodeOffsetAndMorselSemiMask* getSemiMask() { return semiMask.get(); }

    std::pair<common::offset_t, common::offset_t> getNextRangeToRead();

private:
    storage::NodeTable* table;
    common::offset_t maxNodeOffset;
    common::offset_t maxMorselIdx;
    common::offset_t currentNodeOffset;
    std::unique_ptr<NodeOffsetAndMorselSemiMask> semiMask;
};

class ScanNodeIDSharedState {
public:
    ScanNodeIDSharedState() : currentStateIdx{0} {};

    inline void addTableState(storage::NodeTable* table) {
        tableStates.push_back(std::make_unique<NodeTableScanState>(table));
    }
    inline uint32_t getNumTableStates() const { return tableStates.size(); }
    inline NodeTableScanState* getTableState(uint32_t idx) const { return tableStates[idx].get(); }

    inline void initialize(transaction::Transaction* transaction) {
        auto numMask = tableStates[0]->getSemiMask()->getNumMasks();
        for (auto& tableState : tableStates) {
            assert(tableState->getSemiMask()->getNumMasks() == numMask);
            tableState->initializeMaxOffset(transaction);
        }
    }

    std::tuple<NodeTableScanState*, common::offset_t, common::offset_t> getNextRangeToRead();

private:
    std::mutex mtx;
    std::vector<std::unique_ptr<NodeTableScanState>> tableStates;
    uint32_t currentStateIdx;
};

class ScanNodeID : public PhysicalOperator {
public:
    ScanNodeID(const DataPos& outDataPos, std::shared_ptr<ScanNodeIDSharedState> sharedState,
        uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::SCAN_NODE_ID, id, paramsString},
          outDataPos{outDataPos}, sharedState{std::move(sharedState)} {}

    bool isSource() const override { return true; }

    inline ScanNodeIDSharedState* getSharedState() const { return sharedState.get(); }

    void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override;

    bool getNextTuplesInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ScanNodeID>(outDataPos, sharedState, id, paramsString);
    }

private:
    inline void initGlobalStateInternal(ExecutionContext* context) override {
        sharedState->initialize(context->clientContext->getActiveTransaction());
    }

    void setSelVector(
        NodeTableScanState* tableState, common::offset_t startOffset, common::offset_t endOffset);

private:
    DataPos outDataPos;
    std::shared_ptr<ScanNodeIDSharedState> sharedState;
    std::shared_ptr<common::ValueVector> outValueVector;
};

class FillTableID : public PhysicalOperator {
public:
    FillTableID(const DataPos& internalIDPos, common::table_id_t tableID,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::FILL_TABLE_ID, std::move(child), id, paramsString},
          internalIDPos{internalIDPos}, tableID{tableID} {}

    inline void initLocalStateInternal(ResultSet* resultSet_, ExecutionContext* context) final {
        internalIDVector = resultSet_->getValueVector(internalIDPos).get();
    }

    bool getNextTuplesInternal(ExecutionContext* context) final;

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<FillTableID>(
            internalIDPos, tableID, children[0]->clone(), id, paramsString);
    }

private:
    DataPos internalIDPos;
    common::table_id_t tableID;
    common::ValueVector* internalIDVector;
};

} // namespace processor
} // namespace kuzu
