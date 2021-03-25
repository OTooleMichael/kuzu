#pragma once

#include "src/common/include/expression_type.h"
#include "src/common/include/literal.h"
#include "src/common/include/types.h"

using namespace graphflow::common;
using namespace std;

namespace graphflow {
namespace expression {

class LogicalExpression {

public:
    // creates a non-leaf logical binary expression.
    LogicalExpression(ExpressionType expressionType, DataType dataType,
        unique_ptr<LogicalExpression> left, unique_ptr<LogicalExpression> right);

    // creates a non-leaf logical unary expression.
    LogicalExpression(
        ExpressionType expressionType, DataType dataType, unique_ptr<LogicalExpression> child);

    // creates a leaf variable expression.
    LogicalExpression(ExpressionType expressionType, DataType dataType, const string& variableName);

    // creates a leaf literal expression.
    LogicalExpression(
        ExpressionType expressionType, DataType dataType, const Literal& literalValue);

    inline const string& getVariableName() const { return variableName; }

    inline const Literal& getLiteralValue() const { return literalValue; }

    inline DataType getDataType() const { return dataType; }

    inline ExpressionType getExpressionType() const { return expressionType; }

    inline const LogicalExpression& getChildExpr(uint64_t pos) const { return *childrenExpr[pos]; }

protected:
    LogicalExpression(ExpressionType expressionType, DataType dataType)
        : expressionType{expressionType}, dataType{dataType} {}

private:
    // variable name for leaf variable expressions.
    string variableName;
    // value used by leaf literal expressions.
    Literal literalValue;
    vector<unique_ptr<LogicalExpression>> childrenExpr;
    ExpressionType expressionType;
    DataType dataType;
};

} // namespace expression
} // namespace graphflow
