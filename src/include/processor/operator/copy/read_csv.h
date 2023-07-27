#pragma once

#include "processor/operator/copy/read_file.h"

namespace kuzu {
namespace processor {

class ReadCSV : public ReadFile {
public:
    ReadCSV(const DataPos& rowIdxVectorPos, std::vector<DataPos> arrowColumnPoses,
        std::shared_ptr<storage::ReadFileSharedState> sharedState, uint32_t id,
        const std::string& paramsString)
        : ReadFile{rowIdxVectorPos, std::move(arrowColumnPoses), std::move(sharedState),
              PhysicalOperatorType::READ_CSV, id, paramsString} {}

    inline std::shared_ptr<arrow::RecordBatch> readTuples(
        std::unique_ptr<storage::ReadFileMorsel> morsel) override {
        auto csvMorsel = reinterpret_cast<storage::ReadCSVMorsel*>(morsel.get());
        return csvMorsel->recordBatch;
    }

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ReadCSV>(
            rowIdxVectorPos, arrowColumnPoses, sharedState, id, paramsString);
    }
};

} // namespace processor
} // namespace kuzu
