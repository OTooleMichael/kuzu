#include "include/update_planner.h"

#include "include/enumerator.h"

#include "src/planner/logical_plan/logical_operator/include/logical_create.h"
#include "src/planner/logical_plan/logical_operator/include/logical_delete.h"
#include "src/planner/logical_plan/logical_operator/include/logical_set.h"
#include "src/planner/logical_plan/logical_operator/include/sink_util.h"

namespace graphflow {
namespace planner {

void UpdatePlanner::planUpdatingClause(BoundUpdatingClause& updatingClause, LogicalPlan& plan) {
    switch (updatingClause.getClauseType()) {
    case ClauseType::CREATE: {
        auto& createClause = (BoundCreateClause&)updatingClause;
        if (plan.isEmpty()) { // E.g. CREATE (a:Person {age:20})
            expression_vector expressions;
            for (auto& setItem : createClause.getAllSetItems()) {
                expressions.push_back(setItem.second);
            }
            Enumerator::appendExpressionsScan(expressions, plan);
        } else {
            Enumerator::appendAccumulate(plan);
        }
        planCreate((BoundCreateClause&)updatingClause, plan);
        return;
    }
    case ClauseType::SET: {
        Enumerator::appendAccumulate(plan);
        appendSet((BoundSetClause&)updatingClause, plan);
        return;
    }
    case ClauseType::DELETE: {
        Enumerator::appendAccumulate(plan);
        appendDelete((BoundDeleteClause&)updatingClause, plan);
        return;
    }
    default:
        assert(false);
    }
}

void UpdatePlanner::planSetItem(expression_pair setItem, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    auto lhs = setItem.first;
    auto rhs = setItem.second;
    // Check LHS
    assert(lhs->getChild(0)->dataType.typeID == NODE);
    auto nodeExpression = static_pointer_cast<NodeExpression>(lhs->getChild(0));
    auto lhsGroupPos = schema->getGroupPos(nodeExpression->getIDProperty());
    auto isLhsFlat = schema->getGroup(lhsGroupPos)->getIsFlat();
    // Check RHS
    auto rhsDependentGroupsPos = schema->getDependentGroupsPos(rhs);
    if (!rhsDependentGroupsPos.empty()) { // RHS is not constant
        auto rhsPos = Enumerator::appendFlattensButOne(rhsDependentGroupsPos, plan);
        auto isRhsFlat = schema->getGroup(rhsPos)->getIsFlat();
        // If both are unflat and from different groups, we flatten LHS.
        if (!isRhsFlat && !isLhsFlat && lhsGroupPos != rhsPos) {
            Enumerator::appendFlattenIfNecessary(lhsGroupPos, plan);
        }
    }
}

void UpdatePlanner::planCreate(BoundCreateClause& createClause, LogicalPlan& plan) {
    // Flatten all inputs. E.g. MATCH (a) CREATE (b). We need to create b for each tuple in the
    // match clause. This is to simplify operator implementation.
    for (auto groupPos = 0u; groupPos < plan.getSchema()->getNumGroups(); ++groupPos) {
        Enumerator::appendFlattenIfNecessary(groupPos, plan);
    }
    if (createClause.hasNodes()) {
        appendCreateNode(createClause, plan);
    }
    if (createClause.hasRels()) {
        appendCreateRel(createClause, plan);
    }
}

void UpdatePlanner::appendCreateNode(BoundCreateClause& createClause, LogicalPlan& plan) {
    auto schema = plan.getSchema();
    for (auto& node : createClause.getNodes()) {
        auto groupPos = schema->createGroup();
        schema->insertToGroupAndScope(node->getNodeIDPropertyExpression(), groupPos);
        schema->flattenGroup(groupPos); // create output is always flat
    }
    auto createNode =
        make_shared<LogicalCreateNode>(createClause.getNodes(), plan.getLastOperator());
    plan.setLastOperator(createNode);
    appendSet(createClause.getNodesSetItems(), plan);
}

void UpdatePlanner::appendCreateRel(BoundCreateClause& createClause, LogicalPlan& plan) {
    auto createRel = make_shared<LogicalCreateRel>(
        createClause.getRels(), createClause.getSetItemsPerRel(), plan.getLastOperator());
    plan.setLastOperator(createRel);
}

void UpdatePlanner::appendSet(vector<expression_pair> setItems, LogicalPlan& plan) {
    for (auto& setItem : setItems) {
        planSetItem(setItem, plan);
    }
    auto structuredSetItems = splitSetItems(setItems, true /* isStructured */);
    if (!structuredSetItems.empty()) {
        plan.setLastOperator(make_shared<LogicalSetNodeProperty>(
            std::move(structuredSetItems), false /* isUnstructured */, plan.getLastOperator()));
    }
    auto unstructuredSetItems = splitSetItems(setItems, false /* isStructured */);
    if (!unstructuredSetItems.empty()) {
        plan.setLastOperator(make_shared<LogicalSetNodeProperty>(
            std::move(unstructuredSetItems), true /* isUnstructured*/, plan.getLastOperator()));
    }
}

void UpdatePlanner::appendDelete(BoundDeleteClause& deleteClause, LogicalPlan& plan) {
    expression_vector nodeExpressions;
    expression_vector primaryKeyExpressions;
    for (auto i = 0u; i < deleteClause.getNumExpressions(); ++i) {
        auto expression = deleteClause.getExpression(i);
        assert(expression->dataType.typeID == NODE);
        auto& nodeExpression = (NodeExpression&)*expression;
        auto pk =
            catalog.getReadOnlyVersion()->getNodePrimaryKeyProperty(nodeExpression.getTableID());
        auto pkExpression =
            make_shared<PropertyExpression>(pk.dataType, pk.name, pk.propertyID, expression);
        enumerator->appendScanNodePropIfNecessarySwitch(pkExpression, nodeExpression, plan);
        nodeExpressions.push_back(expression);
        primaryKeyExpressions.push_back(pkExpression);
    }
    auto deleteOperator =
        make_shared<LogicalDelete>(nodeExpressions, primaryKeyExpressions, plan.getLastOperator());
    plan.setLastOperator(deleteOperator);
}

vector<expression_pair> UpdatePlanner::splitSetItems(
    vector<expression_pair> setItems, bool isStructured) {
    vector<expression_pair> result;
    for (auto& [lhs, rhs] : setItems) {
        auto property = static_pointer_cast<PropertyExpression>(lhs);
        auto isPropertyStructured = property->dataType.typeID != UNSTRUCTURED;
        if (isPropertyStructured == isStructured) {
            result.emplace_back(lhs, rhs);
        }
    }
    return result;
}

} // namespace planner
} // namespace graphflow
