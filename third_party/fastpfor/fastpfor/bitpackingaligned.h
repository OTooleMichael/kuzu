/**
 * This code is released under the
 * Apache License Version 2.0 http://www.apache.org/licenses/.
 *
 * (c) Daniel Lemire, http://lemire.me/en/
 */
#ifndef BITPACKINGALIGNED_H_
#define BITPACKINGALIGNED_H_

#include "common.h"

namespace FastPForLib {

const uint32_t *fastunpack_8(const uint32_t *__restrict in,
                             uint32_t *__restrict out, const uint32_t bit);
uint32_t *fastpackwithoutmask_8(const uint32_t *__restrict in,
                                uint32_t *__restrict out, const uint32_t bit);

const uint32_t *fastunpack_16(const uint32_t *__restrict in,
                              uint32_t *__restrict out, const uint32_t bit);
uint32_t *fastpackwithoutmask_16(const uint32_t *__restrict in,
                                 uint32_t *__restrict out,
                                 const uint32_t bit);

} // namespace FastPForLib

#endif /* BITPACKINGALIGNED_H_ */
