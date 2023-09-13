#pragma once

#include "common/copier_config/copier_config.h"
#include "processor/operator/sink.h"
#include "storage/copier/node_group.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace processor {

class CopyNodeSharedState {
public:
    CopyNodeSharedState(uint64_t& numRows, catalog::NodeTableSchema* tableSchema,
        storage::NodeTable* table, const common::CopyDescription& copyDesc,
        storage::MemoryManager* memoryManager);

    inline void initialize(const std::string& directory) { initializePrimaryKey(directory); };

    inline common::offset_t getNextNodeGroupIdx() {
        std::unique_lock<std::mutex> lck{mtx};
        return getNextNodeGroupIdxWithoutLock();
    }
    inline void setNextNodeGroupIdx(common::node_group_idx_t nextNodeGroupIdx) {
        std::unique_lock<std::mutex> lck{mtx};
        if (nextNodeGroupIdx > currentNodeGroupIdx) {
            currentNodeGroupIdx = nextNodeGroupIdx;
        }
    }

    inline uint64_t getCurNodeGroupIdx() const { return currentNodeGroupIdx; }

    void logCopyNodeWALRecord(storage::WAL* wal);

    void appendLocalNodeGroup(std::unique_ptr<storage::NodeGroup> localNodeGroup);

private:
    void initializePrimaryKey(const std::string& directory);
    inline common::offset_t getNextNodeGroupIdxWithoutLock() { return currentNodeGroupIdx++; }

public:
    std::mutex mtx;
    common::column_id_t pkColumnID;
    std::unique_ptr<storage::PrimaryKeyIndexBuilder> pkIndex;
    common::CopyDescription copyDesc;
    storage::NodeTable* table;
    catalog::NodeTableSchema* tableSchema;
    uint64_t& numRows;
    std::shared_ptr<FactorizedTable> fTable;
    bool hasLoggedWAL;
    uint64_t currentNodeGroupIdx;
    // The sharedNodeGroup is to accumulate left data within local node groups in CopyNode ops.
    std::unique_ptr<storage::NodeGroup> sharedNodeGroup;
    bool isCopyTurtle;
};

struct CopyNodeInfo {
    std::vector<DataPos> dataColumnPoses;
    common::CopyDescription copyDesc;
    storage::NodeTable* table;
    storage::RelsStore* relsStore;
    catalog::Catalog* catalog;
    storage::WAL* wal;
    bool containsSerial;
};

class CopyNode : public Sink {
public:
    CopyNode(std::shared_ptr<CopyNodeSharedState> sharedState, CopyNodeInfo copyNodeInfo,
        std::unique_ptr<ResultSetDescriptor> resultSetDescriptor,
        std::unique_ptr<PhysicalOperator> child, uint32_t id, const std::string& paramsString);

    inline void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) final {
        for (auto& arrowColumnPos : copyNodeInfo.dataColumnPoses) {
            dataColumnVectors.push_back(resultSet->getValueVector(arrowColumnPos).get());
        }
        localNodeGroup =
            std::make_unique<storage::NodeGroup>(sharedState->tableSchema, &sharedState->copyDesc);
    }

    void initGlobalStateInternal(ExecutionContext* context) final;

    void executeInternal(ExecutionContext* context) final;

    static void sliceDataChunk(const common::DataChunk& dataChunk,
        const std::vector<DataPos>& dataColumnPoses, common::offset_t offset);

    void finalize(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() final {
        return std::make_unique<CopyNode>(sharedState, copyNodeInfo, resultSetDescriptor->copy(),
            children[0]->clone(), id, paramsString);
    }

    static void writeAndResetNodeGroup(common::node_group_idx_t nodeGroupIdx,
        storage::PrimaryKeyIndexBuilder* pkIndex, common::column_id_t pkColumnID,
        storage::NodeTable* table, storage::NodeGroup* nodeGroup, bool isCopyTurtle);

private:
    inline bool isCopyAllowed() const {
        auto nodesStatistics = copyNodeInfo.table->getNodeStatisticsAndDeletedIDs();
        return nodesStatistics->getNodeStatisticsAndDeletedIDs(copyNodeInfo.table->getTableID())
                   ->getNumTuples() == 0;
    }

    static void populatePKIndex(storage::PrimaryKeyIndexBuilder* pkIndex,
        storage::ColumnChunk* chunk, common::offset_t startNodeOffset, common::offset_t numNodes);
    static void checkNonNullConstraint(
        storage::NullColumnChunk* nullChunk, common::offset_t numNodes);

    template<typename T>
    static uint64_t appendToPKIndex(storage::PrimaryKeyIndexBuilder* pkIndex,
        storage::ColumnChunk* chunk, common::offset_t startOffset, common::offset_t numNodes);

    void calculatePKToAppend(
        storage::PrimaryKeyIndexBuilder* pkIndex, common::ValueVector* vectorToAppend) {
        auto selVector = std::make_unique<common::SelectionVector>(common::DEFAULT_VECTOR_CAPACITY);
        selVector->resetSelectorToValuePosBuffer();
        common::sel_t nextPos = 0;
        common::offset_t result;
        auto offset = sharedState->getCurNodeGroupIdx() + localNodeGroup->getNumNodes();
        for (int i = 0; i < vectorToAppend->state->getNumSelectedValues(); i++) {
            auto uriString = vectorToAppend->getValue<common::ku_string_t>(i).getAsString();
            if (!pkIndex->lookup((int64_t)uriString.c_str(), result)) {
                pkIndex->append(uriString.c_str(), offset++);
                selVector->selectedPositions[nextPos++] = i;
            }
        }
        selVector->selectedSize = nextPos;
        vectorToAppend->state->selVector = std::move(selVector);
    }

private:
    std::shared_ptr<CopyNodeSharedState> sharedState;
    CopyNodeInfo copyNodeInfo;
    std::vector<common::ValueVector*> dataColumnVectors;
    std::unique_ptr<storage::NodeGroup> localNodeGroup;
};

template<>
uint64_t CopyNode::appendToPKIndex<int64_t>(storage::PrimaryKeyIndexBuilder* pkIndex,
    storage::ColumnChunk* chunk, common::offset_t startOffset, common::offset_t numNodes);
template<>
uint64_t CopyNode::appendToPKIndex<common::ku_string_t>(storage::PrimaryKeyIndexBuilder* pkIndex,
    storage::ColumnChunk* chunk, common::offset_t startOffset, common::offset_t numNodes);

} // namespace processor
} // namespace kuzu
