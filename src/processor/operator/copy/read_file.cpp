#include "processor/operator/copy/read_file.h"

namespace kuzu {
namespace processor {

bool ReadFile::getNextTuplesInternal(kuzu::processor::ExecutionContext* context) {
    auto nodeGroupOffsetVector = resultSet->getValueVector(nodeGroupOffsetPos).get();
    auto morsel = sharedState->getMorsel();
    if (morsel == nullptr) {
        return false;
    }
    nodeGroupOffsetVector->setValue(
        nodeGroupOffsetVector->state->selVector->selectedPositions[0], morsel->rowIdxInFile);
    auto recordBatch = readTuples(std::move(morsel));
    for (auto i = 0u; i < dataColumnPoses.size(); i++) {
        common::ArrowColumnVector::setArrowColumn(
            resultSet->getValueVector(dataColumnPoses[i]).get(), recordBatch->column((int)i));
    }
    resultSet->dataChunks[0]->state->currIdx = -1;
    return true;
}

} // namespace processor
} // namespace kuzu
