#pragma once

#include "expression_evaluator/expression_evaluator.h"
#include "processor/execution_context.h"
#include "storage/store/node_table.h"

namespace kuzu {
namespace processor {

// TODO(Guodong): the following class should be moved to storage.
class NodeInsertExecutor {
public:
    NodeInsertExecutor(storage::NodeTable* table, std::vector<storage::RelTable*> relTablesToInit,
        const DataPos& nodeIDVectorPos, std::vector<DataPos> propertyLhsPositions,
        std::vector<std::unique_ptr<evaluator::ExpressionEvaluator>> propertyRhsEvaluators)
        : table{table}, relTablesToInit{std::move(relTablesToInit)},
          nodeIDVectorPos{nodeIDVectorPos}, propertyLhsPositions{std::move(propertyLhsPositions)},
          propertyRhsEvaluators{std::move(propertyRhsEvaluators)}, nodeIDVector{nullptr} {}
    NodeInsertExecutor(const NodeInsertExecutor& other);

    void init(ResultSet* resultSet, ExecutionContext* context);

    void insert(transaction::Transaction* transaction);

    inline std::unique_ptr<NodeInsertExecutor> copy() {
        return std::make_unique<NodeInsertExecutor>(*this);
    }

    static std::vector<std::unique_ptr<NodeInsertExecutor>> copy(
        const std::vector<std::unique_ptr<NodeInsertExecutor>>& executors);

private:
    storage::NodeTable* table;
    std::vector<storage::RelTable*> relTablesToInit;
    DataPos nodeIDVectorPos;
    std::vector<DataPos> propertyLhsPositions;
    std::vector<std::unique_ptr<evaluator::ExpressionEvaluator>> propertyRhsEvaluators;

    common::ValueVector* nodeIDVector;
    std::vector<common::ValueVector*> propertyLhsVectors;
    std::vector<common::ValueVector*> propertyRhsVectors;
};

class RelInsertExecutor {
public:
    RelInsertExecutor(storage::RelsStatistics& relsStatistics, storage::RelTable* table,
        const DataPos& srcNodePos, const DataPos& dstNodePos,
        std::vector<DataPos> propertyLhsPositions,
        std::vector<std::unique_ptr<evaluator::ExpressionEvaluator>> propertyRhsEvaluators)
        : relsStatistics{relsStatistics}, table{table}, srcNodePos{srcNodePos},
          dstNodePos{dstNodePos}, propertyLhsPositions{std::move(propertyLhsPositions)},
          propertyRhsEvaluators{std::move(propertyRhsEvaluators)}, srcNodeIDVector{nullptr},
          dstNodeIDVector{nullptr} {}
    RelInsertExecutor(const RelInsertExecutor& other);

    void init(ResultSet* resultSet, ExecutionContext* context);

    void insert(transaction::Transaction* transaction);

    inline std::unique_ptr<RelInsertExecutor> copy() {
        return std::make_unique<RelInsertExecutor>(*this);
    }

    static std::vector<std::unique_ptr<RelInsertExecutor>> copy(
        const std::vector<std::unique_ptr<RelInsertExecutor>>& executors);

private:
    storage::RelsStatistics& relsStatistics;
    storage::RelTable* table;
    DataPos srcNodePos;
    DataPos dstNodePos;
    std::vector<DataPos> propertyLhsPositions;
    std::vector<std::unique_ptr<evaluator::ExpressionEvaluator>> propertyRhsEvaluators;

    common::ValueVector* srcNodeIDVector;
    common::ValueVector* dstNodeIDVector;
    std::vector<common::ValueVector*> propertyLhsVectors;
    std::vector<common::ValueVector*> propertyRhsVectors;
};

} // namespace processor
} // namespace kuzu
