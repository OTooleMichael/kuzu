#pragma once

#include "processor/operator/copy/read_file.h"

namespace kuzu {
namespace processor {

class ReadParquet : public ReadFile {
public:
    ReadParquet(const DataPos& offsetVectorPos, std::vector<DataPos> arrowColumnPoses,
        std::shared_ptr<storage::ReadFileSharedState> sharedState, uint32_t id,
        const std::string& paramsString)
        : ReadFile{offsetVectorPos, std::move(arrowColumnPoses), std::move(sharedState),
              PhysicalOperatorType::READ_PARQUET, id, paramsString} {}

    std::shared_ptr<arrow::RecordBatch> readTuples(
        std::unique_ptr<storage::ReadFileMorsel> morsel) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ReadParquet>(
            rowIdxVectorPos, arrowColumnPoses, sharedState, id, paramsString);
    }

private:
    std::unique_ptr<parquet::arrow::FileReader> reader;
    std::string filePath;
};

} // namespace processor
} // namespace kuzu
