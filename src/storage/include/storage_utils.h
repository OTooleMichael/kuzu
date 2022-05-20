#pragma once

#include <string>

#include "src/common/include/file_utils.h"
#include "src/common/include/type_utils.h"
#include "src/common/include/utils.h"

namespace graphflow {
namespace storage {

constexpr uint8_t bitMasksWithSingle1s[8] = {
    0b10000000, 0b01000000, 0b00100000, 0b00010000, 0b00001000, 0b00000100, 0b00000010, 0b00000001};
constexpr uint8_t bitMasksWithSingle0s[8] = {
    0b01111111, 0b10111111, 0b11011111, 0b11101111, 0b11110111, 0b11111011, 0b11111101, 0b11111110};

struct PageByteCursor {

    PageByteCursor(uint64_t idx, uint16_t offset) : idx{idx}, offset{offset} {};
    PageByteCursor() : PageByteCursor{UINT64_MAX, UINT16_MAX} {};

    uint64_t idx;
    uint16_t offset;
};

struct PageElementCursor {

    PageElementCursor(uint64_t idx, uint16_t pos) : idx{idx}, pos{pos} {};
    PageElementCursor() : PageElementCursor{-1ul, (uint16_t)-1} {};

    uint64_t idx;
    uint16_t pos;
};

struct PageUtils {

    // The first is the NULLByte, and the second is the byteLevelOffset of the element, whose
    // elementPos (which is the position/offset within a page) is given.
    static inline pair<uint8_t, uint8_t> getNULLByteAndByteLevelOffsetPair(
        uint8_t* frame, uint16_t elementPos) {
        pair<uint8_t*, uint8_t> NULLBytePtrAndByteLevelOffset =
            getNULLBytePtrAndByteLevelOffsetPair(frame, elementPos);
        // Test if this makes a difference: frame + (DEFAULT_PAGE_SIZE - 1 - (elementPos >> 3)),
        // elementPos % 8
        return pair(*NULLBytePtrAndByteLevelOffset.first, NULLBytePtrAndByteLevelOffset.second);
    }

    static inline pair<uint8_t*, uint8_t> getNULLBytePtrAndByteLevelOffsetPair(
        uint8_t* frame, uint16_t elementPos) {
        return pair(frame + (DEFAULT_PAGE_SIZE - 1 - (elementPos >> 3)), elementPos % 8);
    }

    static uint32_t getNumElementsInAPageWithNULLBytes(uint32_t elementSize) {
        auto numNULLBytes = (uint32_t)ceil((double)DEFAULT_PAGE_SIZE / ((elementSize << 3) + 1));
        return (DEFAULT_PAGE_SIZE - numNULLBytes) / elementSize;
    }

    static uint32_t getNumElementsInAPageWithoutNULLBytes(uint32_t elementSize) {
        return DEFAULT_PAGE_SIZE / elementSize;
    }

    // This function returns the page idx of the page where element will be found and the pos of
    // the element in the page as the offset.
    static PageElementCursor getPageElementCursorForOffset(
        const uint64_t& elementOffset, const uint32_t numElementsPerPage) {
        return PageElementCursor{
            elementOffset / numElementsPerPage, (uint16_t)(elementOffset % numElementsPerPage)};
    }
};

struct StorageStructureUtils {
    static void pinEachPageOfFile(FileHandle& fileHandle, BufferManager& bufferManager) {
        for (auto pageIdx = 0u; pageIdx < fileHandle.getNumPages(); ++pageIdx) {
            bufferManager.pin(fileHandle, pageIdx);
        }
    }

    static void unpinEachPageOfFile(FileHandle& fileHandle, BufferManager& bufferManager) {
        for (auto pageIdx = 0u; pageIdx < fileHandle.getNumPages(); ++pageIdx) {
            bufferManager.unpin(fileHandle, pageIdx);
        }
    }
};

class StorageUtils {

public:
    static void saveListOfIntsToFile(const string& fName, uint8_t* data, uint32_t listSize);
    static uint32_t readListOfIntsFromFile(unique_ptr<uint32_t[]>& data, const string& fName);

    inline static string getNodeIndexFName(const string& directory, const label_t& nodeLabel) {
        auto fName = StringUtils::string_format("n-%d", nodeLabel);
        return FileUtils::joinPath(directory, fName + StorageConfig::INDEX_FILE_SUFFIX);
    }
    inline static string getNodePropertyColumnFName(
        const string& directory, const label_t& nodeLabel, const string& propertyName) {
        auto fName = StringUtils::string_format("n-%d-%s", nodeLabel, propertyName.data());
        return FileUtils::joinPath(directory, fName + StorageConfig::COLUMN_FILE_SUFFIX);
    }
    inline static string getNodeUnstrPropertyListsFName(
        const string& directory, const label_t& nodeLabel) {
        auto fName = StringUtils::string_format("n-%d", nodeLabel);
        return FileUtils::joinPath(directory, fName + StorageConfig::LISTS_FILE_SUFFIX);
    }

    inline static string getAdjColumnFName(const string& directory, const label_t& relLabel,
        const label_t& nodeLabel, const RelDirection& direction) {
        auto fName = StringUtils::string_format("r-%d-%d-%d", relLabel, nodeLabel, direction);
        return FileUtils::joinPath(directory, fName + StorageConfig::COLUMN_FILE_SUFFIX);
    }
    inline static string getAdjListsFName(const string& directory, const label_t& relLabel,
        const label_t& nodeLabel, const RelDirection& direction) {
        auto fName = StringUtils::string_format("r-%d-%d-%d", relLabel, nodeLabel, direction);
        return FileUtils::joinPath(directory, fName + StorageConfig::LISTS_FILE_SUFFIX);
    }
    inline static string getRelPropertyColumnFName(const string& directory, const label_t& relLabel,
        const label_t& nodeLabel, const string& propertyName) {
        auto fName =
            StringUtils::string_format("r-%d-%d-%s", relLabel, nodeLabel, propertyName.data());
        return FileUtils::joinPath(directory, fName + StorageConfig::COLUMN_FILE_SUFFIX);
    }
    inline static string getRelPropertyListsFName(const string& directory, const label_t& relLabel,
        const label_t& nodeLabel, const RelDirection& direction, const string& propertyName) {
        auto fName = StringUtils::string_format(
            "r-%d-%d-%d-%s", relLabel, nodeLabel, direction, propertyName.data());
        return FileUtils::joinPath(directory, fName + StorageConfig::LISTS_FILE_SUFFIX);
    }
};

} // namespace storage
} // namespace graphflow
