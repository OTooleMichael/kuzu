#pragma once

#include "common/utils.h"

namespace kuzu {
namespace processor {

class ByteBuffer { // on to the 10 thousandth impl
public:
    ByteBuffer(){};
    ByteBuffer(uint8_t* ptr, uint64_t len) : ptr{ptr}, len{len} {};

    uint8_t* ptr = nullptr;
    uint64_t len = 0;

public:
    void inc(uint64_t increment) {
        available(increment);
        len -= increment;
        ptr += increment;
    }

    template<class T>
    T read() {
        T val = get<T>();
        inc(sizeof(T));
        return val;
    }

    template<typename T>
    T Load(const uint8_t* ptr) {
        T ret;
        memcpy(&ret, ptr, sizeof(ret));
        return ret;
    }

    template<class T>
    T get() {
        available(sizeof(T));
        T val = Load<T>(ptr);
        return val;
    }

    void copy_to(char* dest, uint64_t len) {
        available(len);
        std::memcpy(dest, ptr, len);
    }

    void zero() { std::memset(ptr, 0, len); }

    void available(uint64_t req_len) {
        if (req_len > len) {
            throw std::runtime_error("Out of buffer");
        }
    }
};

class ResizeableBuffer : public ByteBuffer {
public:
    ResizeableBuffer() {}
    ResizeableBuffer(uint64_t new_size) { resize(new_size); }
    void resize(uint64_t new_size) {
        len = new_size;
        if (new_size == 0) {
            return;
        }
        if (new_size > alloc_len) {
            alloc_len = common::nextPowerOfTwo(new_size);
            allocated_data = std::make_unique<uint8_t[]>(alloc_len);
            ptr = allocated_data.get();
        }
    }

private:
    std::unique_ptr<uint8_t[]> allocated_data;
    uint64_t alloc_len = 0;
};

} // namespace processor
} // namespace kuzu
