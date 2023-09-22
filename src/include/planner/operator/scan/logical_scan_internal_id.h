#pragma once

#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalScanInternalID : public LogicalOperator {
public:
    explicit LogicalScanInternalID(
        std::shared_ptr<binder::Expression> internalID, std::vector<common::table_id_t> tableIDs)
        : LogicalOperator{LogicalOperatorType::SCAN_INTERNAL_ID},
          internalID{std::move(internalID)}, tableIDs{std::move(tableIDs)} {}

    void computeFactorizedSchema() final;
    void computeFlatSchema() final;

    inline std::string getExpressionsForPrinting() const final { return internalID->toString(); }

    inline std::shared_ptr<binder::Expression> getInternalID() const { return internalID; }
    inline std::vector<common::table_id_t> getTableIDs() const { return tableIDs; }

    inline std::unique_ptr<LogicalOperator> copy() final {
        return make_unique<LogicalScanInternalID>(internalID, tableIDs);
    }

private:
    std::shared_ptr<binder::Expression> internalID;
    std::vector<common::table_id_t> tableIDs;
};

class LogicalFillTableID : public LogicalOperator {
public:
    LogicalFillTableID(std::shared_ptr<binder::Expression> internalID, common::table_id_t tableID,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::FILL_TABLE_ID, std::move(child)},
          internalID{std::move(internalID)}, tableID{tableID} {}

    inline void computeFactorizedSchema() final { copyChildSchema(0); }
    inline void computeFlatSchema() final { copyChildSchema(0); }

    inline std::string getExpressionsForPrinting() const final { return internalID->toString(); }

    inline std::shared_ptr<binder::Expression> getInternalID() const { return internalID; }
    inline common::table_id_t getTableID() const { return tableID; }

    inline std::unique_ptr<LogicalOperator> copy() final {
        return std::make_unique<LogicalFillTableID>(internalID, tableID, children[0]->copy());
    }

private:
    std::shared_ptr<binder::Expression> internalID;
    common::table_id_t tableID;
};

} // namespace planner
} // namespace kuzu
