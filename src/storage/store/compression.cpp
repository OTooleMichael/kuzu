#include "storage/store/compression.h"

#include <math.h>

#include <string>

#include "arrow/array.h"
#include "common/exception/not_implemented.h"
#include "common/exception/storage.h"
#include "common/null_mask.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"
#include "fastpfor/bitpackinghelpers.h"
#include "storage/store/node_column.h"
#include "storage/store/sign_extend.h"
#include <bit>

using namespace kuzu::common;
namespace arrow {
class Array;
}

namespace kuzu {
namespace storage {

uint32_t getDataTypeSizeInChunk(const common::LogicalType& dataType) {
    using namespace common;
    switch (dataType.getLogicalTypeID()) {
    case LogicalTypeID::STRUCT: {
        return 0;
    }
    case LogicalTypeID::STRING: {
        return sizeof(ku_string_t);
    }
    case LogicalTypeID::VAR_LIST:
    case LogicalTypeID::INTERNAL_ID: {
        return sizeof(offset_t);
    }
    case LogicalTypeID::SERIAL: {
        return sizeof(int64_t);
    }
    default: {
        auto size = StorageUtils::getDataTypeSize(dataType);
        assert(size <= BufferPoolConstants::PAGE_4KB_SIZE);
        return size;
    }
    }
}

bool CompressionMetadata::canAlwaysUpdateInPlace() const {
    switch (compression) {
    case CompressionType::BOOLEAN_BITPACKING:
    case CompressionType::UNCOMPRESSED: {
        return true;
    }
    case CompressionType::INTEGER_BITPACKING: {
        return false;
    }
    default: {
        throw common::StorageException(
            "Unknown compression type with ID " + std::to_string((uint8_t)compression));
    }
    }
}
bool CompressionMetadata::canUpdateInPlace(
    const ValueVector& vector, uint32_t pos, PhysicalTypeID physicalType) const {
    if (canAlwaysUpdateInPlace()) {
        return true;
    }
    switch (compression) {
    case CompressionType::BOOLEAN_BITPACKING:
    case CompressionType::UNCOMPRESSED: {
        return true;
    }
    case CompressionType::INTEGER_BITPACKING: {
        switch (physicalType) {
        case PhysicalTypeID::INT64: {
            auto value = vector.getValue<int64_t>(pos);
            return IntegerBitpacking<int64_t>::canUpdateInPlace(
                value, BitpackHeader::readHeader(data));
        }
        case PhysicalTypeID::INT32: {
            auto value = vector.getValue<int32_t>(pos);
            return IntegerBitpacking<int32_t>::canUpdateInPlace(
                value, BitpackHeader::readHeader(data));
        }
        default: {
            throw common::StorageException(
                "Attempted to read from a column chunk which uses integer bitpacking but does not "
                "have a supported integer physical type: " +
                PhysicalTypeUtils::physicalTypeToString(physicalType));
        }
        }
    }
    default: {
        throw common::StorageException(
            "Unknown compression type with ID " + std::to_string((uint8_t)compression));
    }
    }
}

uint64_t CompressionMetadata::numValues(uint64_t pageSize, const LogicalType& dataType) const {
    switch (compression) {
    case CompressionType::UNCOMPRESSED: {
        return Uncompressed::numValues(pageSize, dataType);
    }
    case CompressionType::INTEGER_BITPACKING: {
        switch (dataType.getPhysicalType()) {
        case PhysicalTypeID::INT64:
            return IntegerBitpacking<int64_t>::numValues(pageSize, BitpackHeader::readHeader(data));
        case PhysicalTypeID::INT32:
            return IntegerBitpacking<int32_t>::numValues(pageSize, BitpackHeader::readHeader(data));
        default: {
            throw common::StorageException(
                "Attempted to read from a column chunk which uses integer bitpacking but does not "
                "have a supported integer physical type: " +
                PhysicalTypeUtils::physicalTypeToString(dataType.getPhysicalType()));
        }
        }
    }
    case CompressionType::BOOLEAN_BITPACKING: {
        return BooleanBitpacking::numValues(pageSize);
    }
    default: {
        throw common::StorageException(
            "Unknown compression type with ID " + std::to_string((uint8_t)compression));
    }
    }
}

template<typename T>
BitpackHeader IntegerBitpacking<T>::getBitWidth(
    const uint8_t* srcBuffer, uint64_t numValues) const {
    auto max = 0ull;
    auto hasNegative = false;
    for (int i = 0; i < numValues; i++) {
        T value = ((T*)srcBuffer)[i];
        auto abs = std::abs(value);
        if (abs > max) {
            max = abs;
        }
        if (value < 0) {
            hasNegative = true;
        }
    }
    if (hasNegative) {
        // Needs an extra bit for two's complement encoding
        return BitpackHeader{static_cast<uint8_t>(std::bit_width(max) + 1), true};
    } else {
        return BitpackHeader{static_cast<uint8_t>(std::bit_width(max)), false};
    }
}

template<typename T>
bool IntegerBitpacking<T>::canUpdateInPlace(T value, const BitpackHeader& header) {
    // If there are negatives, the effective bit width is smaller
    auto valueSize = std::bit_width((U)std::abs(value));
    if (!header.hasNegative && value < 0) {
        return false;
    }
    if ((header.hasNegative && valueSize > header.bitWidth - 1) ||
        (!header.hasNegative && valueSize > header.bitWidth)) {
        return false;
    }
    return true;
}

template<typename T>
void IntegerBitpacking<T>::setValueFromUncompressed(uint8_t* srcBuffer, common::offset_t posInSrc,
    uint8_t* dstBuffer, common::offset_t posInDst, const CompressionMetadata& metadata) const {
    auto header = BitpackHeader::readHeader(metadata.data);
    // This is a fairly naive implementation which uses fastunpack/fastpack
    // to modify the data by decompressing/compressing a single chunk of values.
    //
    // TODO(bmwinger): modify the data in-place
    //
    // Data can be considered to be stored in aligned chunks of 32 values
    // with a size of 32 * bitWidth bits,
    // or bitWidth 32-bit values (we cast the buffer to a uint32_t* later).
    auto chunkStart = getChunkStart(dstBuffer, posInDst, header.bitWidth);
    auto posInChunk = posInDst % CHUNK_SIZE;
    auto value = ((T*)srcBuffer)[posInSrc];
    assert(canUpdateInPlace(value, header));

    U chunk[CHUNK_SIZE];
    FastPForLib::fastunpack((const uint32_t*)chunkStart, chunk, header.bitWidth);
    chunk[posInChunk] = (U)value;
    FastPForLib::fastpack(chunk, (uint32_t*)chunkStart, header.bitWidth);
}

template<typename T>
void IntegerBitpacking<T>::getValues(const uint8_t* chunkStart, uint8_t pos, uint8_t* dst,
    uint8_t numValuesToRead, const BitpackHeader& header) const {
    // TODO(bmwinger): optimize as in setValueFromUncompressed
    assert(pos + numValuesToRead <= CHUNK_SIZE);

    U chunk[CHUNK_SIZE];
    FastPForLib::fastunpack((const uint32_t*)chunkStart, chunk, header.bitWidth);
    if (header.hasNegative) {
        SignExtend<T, U, CHUNK_SIZE>((uint8_t*)chunk, header.bitWidth);
    }
    memcpy(dst, &chunk[pos], sizeof(T) * numValuesToRead);
}

template<typename T>
void IntegerBitpacking<T>::getValue(const uint8_t* buffer, offset_t posInBuffer, uint8_t* dst,
    offset_t posInDst, const CompressionMetadata& metadata) const {
    auto header = BitpackHeader::readHeader(metadata.data);
    auto chunkStart = getChunkStart(buffer, posInBuffer, header.bitWidth);
    getValues(chunkStart, posInBuffer % CHUNK_SIZE, dst + posInDst * sizeof(T), 1, header);
}

template<typename T>
uint64_t IntegerBitpacking<T>::compressNextPage(const uint8_t*& srcBuffer,
    uint64_t numValuesRemaining, uint8_t* dstBuffer, uint64_t dstBufferSize,
    const struct CompressionMetadata& metadata) const {
    auto header = BitpackHeader::readHeader(metadata.data);
    auto bitWidth = header.bitWidth;

    if (bitWidth == 0) {
        return 0;
    }
    auto numValuesToCompress = std::min(numValuesRemaining, numValues(dstBufferSize, header));
    // Round up to nearest byte
    auto sizeToCompress =
        numValuesToCompress * bitWidth / 8 + (numValuesToCompress * bitWidth % 8 != 0);
    assert(dstBufferSize >= CHUNK_SIZE);
    assert(dstBufferSize >= sizeToCompress);
    // This might overflow the source buffer if there are fewer values remaining than the chunk size
    // so we stop at the end of the last full chunk and use a temporary array to avoid overflow.
    auto lastFullChunkEnd = numValuesToCompress - numValuesToCompress % CHUNK_SIZE;
    for (auto i = 0ull; i < lastFullChunkEnd; i += CHUNK_SIZE) {
        FastPForLib::fastpack(
            (const U*)srcBuffer + i, (uint32_t*)(dstBuffer + i * bitWidth / 8), bitWidth);
    }
    // Pack last partial chunk, avoiding overflows
    if (numValuesToCompress % CHUNK_SIZE > 0) {
        // TODO(bmwinger): optimize to remove temporary array
        U chunk[CHUNK_SIZE] = {0};
        memcpy(chunk, (const U*)srcBuffer + lastFullChunkEnd,
            numValuesToCompress % CHUNK_SIZE * sizeof(U));
        FastPForLib::fastpack(
            chunk, (uint32_t*)(dstBuffer + lastFullChunkEnd * bitWidth / 8), bitWidth);
    }
    srcBuffer += numValuesToCompress * sizeof(U);
    return sizeToCompress;
}

template<typename T>
void IntegerBitpacking<T>::decompressFromPage(const uint8_t* srcBuffer, uint64_t srcOffset,
    uint8_t* dstBuffer, uint64_t dstOffset, uint64_t numValues,
    const CompressionMetadata& metadata) const {
    auto header = BitpackHeader::readHeader(metadata.data);

    auto srcCursor = getChunkStart(srcBuffer, srcOffset, header.bitWidth);
    auto valuesInFirstChunk = std::min(CHUNK_SIZE - (srcOffset % CHUNK_SIZE), numValues);
    auto bytesPerChunk = CHUNK_SIZE / 8 * header.bitWidth;
    auto dstIndex = dstOffset;

    // Copy values which aren't aligned to the start of the chunk
    if (valuesInFirstChunk < CHUNK_SIZE) {
        getValues(srcCursor, srcOffset % CHUNK_SIZE, dstBuffer + dstIndex * sizeof(U),
            valuesInFirstChunk, header);
        if (numValues == valuesInFirstChunk) {
            return;
        }
        // Start at the end of the first partial chunk
        srcCursor += bytesPerChunk;
        dstIndex += valuesInFirstChunk;
    }

    // Use fastunpack to directly unpack the full-sized chunks
    for (; dstIndex < dstOffset + numValues - numValues % CHUNK_SIZE; dstIndex += CHUNK_SIZE) {
        FastPForLib::fastunpack(
            (const uint32_t*)srcCursor, (U*)dstBuffer + dstIndex, header.bitWidth);
        if (header.hasNegative) {
            SignExtend<T, U, CHUNK_SIZE>(dstBuffer + dstIndex * sizeof(U), header.bitWidth);
        }
        srcCursor += bytesPerChunk;
    }
    // Copy remaining values from within the last chunk.
    if (dstIndex < dstOffset + numValues) {
        getValues(srcCursor, 0, dstBuffer + dstIndex * sizeof(U), dstOffset + numValues - dstIndex,
            header);
    }
}

// Uses unsigned types since the storage is unsigned
// TODO: Doesn't currently support int16
// template class IntegerBitpacking<int16_t>;
template class IntegerBitpacking<int32_t>;
template class IntegerBitpacking<int64_t>;

void BooleanBitpacking::setValueFromUncompressed(uint8_t* srcBuffer, offset_t posInSrc,
    uint8_t* dstBuffer, offset_t posInDst, const CompressionMetadata& metadata) const {
    auto val = ((bool*)srcBuffer)[posInSrc];
    common::NullMask::setNull((uint64_t*)dstBuffer, posInDst, val);
}

void BooleanBitpacking::getValue(const uint8_t* buffer, offset_t posInBuffer, uint8_t* dst,
    offset_t posInDst, const CompressionMetadata& metadata) const {
    *(dst + posInDst) = common::NullMask::isNull((uint64_t*)buffer, posInBuffer);
}

uint64_t BooleanBitpacking::compressNextPage(const uint8_t*& srcBuffer, uint64_t numValuesRemaining,
    uint8_t* dstBuffer, uint64_t dstBufferSize, const struct CompressionMetadata& metadata) const {
    // TODO(bmwinger): Optimize, e.g. using an integer bitpacking function
    auto numValuesToCompress = std::min(numValuesRemaining, numValues(dstBufferSize));
    for (auto i = 0ull; i < numValuesToCompress; i++) {
        common::NullMask::setNull((uint64_t*)dstBuffer, i, srcBuffer[i]);
    }
    srcBuffer += numValuesToCompress / 8;
    // Will be a multiple of 8 except for the last iteration
    return numValuesToCompress / 8 + (bool)(numValuesToCompress % 8);
}

void BooleanBitpacking::decompressFromPage(const uint8_t* srcBuffer, uint64_t srcOffset,
    uint8_t* dstBuffer, uint64_t dstOffset, uint64_t numValues,
    const CompressionMetadata& metadata) const {
    // TODO(bmwinger): Optimize, e.g. using an integer bitpacking function
    for (auto i = 0ull; i < numValues; i++) {
        ((bool*)dstBuffer)[dstOffset + i] =
            common::NullMask::isNull((uint64_t*)srcBuffer, srcOffset + i);
    }
}

uint8_t BitpackHeader::getDataByte() const {
    uint8_t data = bitWidth;
    if (hasNegative) {
        data |= NEGATIVE_FLAG;
    }
    return data;
}

BitpackHeader BitpackHeader::readHeader(uint8_t data) {
    BitpackHeader header;
    header.bitWidth = data & BITWIDTH_MASK;
    header.hasNegative = data & NEGATIVE_FLAG;
    return header;
}

void ReadCompressedValuesFromPageToVector::operator()(uint8_t* frame, PageElementCursor& pageCursor,
    common::ValueVector* resultVector, uint32_t posInVector, uint32_t numValuesToRead,
    const CompressionMetadata& metadata) {
    switch (metadata.compression) {
    case CompressionType::UNCOMPRESSED:
        return copy.decompressFromPage(frame, pageCursor.elemPosInPage, resultVector->getData(),
            posInVector, numValuesToRead, metadata);
    case CompressionType::INTEGER_BITPACKING: {
        switch (physicalType) {
        case PhysicalTypeID::INT64: {
            return IntegerBitpacking<int64_t>().decompressFromPage(frame, pageCursor.elemPosInPage,
                resultVector->getData(), posInVector, numValuesToRead, metadata);
        }
        case PhysicalTypeID::INT32: {
            return IntegerBitpacking<int32_t>().decompressFromPage(frame, pageCursor.elemPosInPage,
                resultVector->getData(), posInVector, numValuesToRead, metadata);
        }
        default: {
            throw NotImplementedException("INTEGER_BITPACKING is not implemented for type " +
                                          PhysicalTypeUtils::physicalTypeToString(physicalType));
        }
        }
    }
    case CompressionType::BOOLEAN_BITPACKING:
        return booleanBitpacking.decompressFromPage(frame, pageCursor.elemPosInPage,
            resultVector->getData(), posInVector, numValuesToRead, metadata);
    }
}

void ReadCompressedValuesFromPage::operator()(uint8_t* frame, PageElementCursor& pageCursor,
    uint8_t* result, uint32_t startPosInResult, uint64_t numValuesToRead,
    const CompressionMetadata& metadata) {
    switch (metadata.compression) {
    case CompressionType::UNCOMPRESSED:
        return copy.decompressFromPage(
            frame, pageCursor.elemPosInPage, result, startPosInResult, numValuesToRead, metadata);
    case CompressionType::INTEGER_BITPACKING: {
        switch (physicalType) {
        case PhysicalTypeID::INT64: {
            return IntegerBitpacking<int64_t>().decompressFromPage(frame, pageCursor.elemPosInPage,
                result, startPosInResult, numValuesToRead, metadata);
        }
        case PhysicalTypeID::INT32: {
            return IntegerBitpacking<int32_t>().decompressFromPage(frame, pageCursor.elemPosInPage,
                result, startPosInResult, numValuesToRead, metadata);
        }
        default: {
            throw NotImplementedException("INTEGER_BITPACKING is not implemented for type " +
                                          PhysicalTypeUtils::physicalTypeToString(physicalType));
        }
        }
    }
    case CompressionType::BOOLEAN_BITPACKING:
        return booleanBitpacking.decompressFromPage(
            frame, pageCursor.elemPosInPage, result, startPosInResult, numValuesToRead, metadata);
    }
}

void WriteCompressedValueToPage::operator()(uint8_t* frame, uint16_t posInFrame,
    common::ValueVector* vector, uint32_t posInVector, const CompressionMetadata& metadata) {
    switch (metadata.compression) {
    case CompressionType::UNCOMPRESSED:
        return copy.setValueFromUncompressed(
            vector->getData(), posInVector, frame, posInFrame, metadata);
    case CompressionType::INTEGER_BITPACKING: {
        switch (physicalType) {
        case PhysicalTypeID::INT64: {
            return IntegerBitpacking<int64_t>().setValueFromUncompressed(
                vector->getData(), posInVector, frame, posInFrame, metadata);
        }
        case PhysicalTypeID::INT32: {
            return IntegerBitpacking<int32_t>().setValueFromUncompressed(
                vector->getData(), posInVector, frame, posInFrame, metadata);
        }
        default: {
            throw NotImplementedException("INTEGER_BITPACKING is not implemented for type " +
                                          PhysicalTypeUtils::physicalTypeToString(physicalType));
        }
        }
    }
    case CompressionType::BOOLEAN_BITPACKING:
        return booleanBitpacking.setValueFromUncompressed(
            vector->getData(), posInVector, frame, posInFrame, metadata);
    }
}

} // namespace storage
} // namespace kuzu
