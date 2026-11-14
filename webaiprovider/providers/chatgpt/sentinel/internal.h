#ifndef SENTINEL_INTERNAL_H
#define SENTINEL_INTERNAL_H

#include "sentinel.h"

extern const char sentinel_b64_alphabet[65];

static inline uint32_t sentinel_fnv1a_step_unit(uint32_t hash, uint32_t unit) {
    hash ^= unit;
    return hash * SENTINEL_FNV1A_PRIME;
}

static inline uint32_t sentinel_fnv1a_step_codepoint(uint32_t hash, uint32_t code) {
    if (code <= UINT32_C(0xFFFF)) {
        return sentinel_fnv1a_step_unit(hash, code);
    }
    uint32_t adj = code - UINT32_C(0x10000);
    hash = sentinel_fnv1a_step_unit(hash, UINT32_C(0xD800) + (adj >> 10));
    return sentinel_fnv1a_step_unit(hash, UINT32_C(0xDC00) + (adj & UINT32_C(0x3FF)));
}

static inline void sentinel_b64_encode_triplet(
    uint8_t b0, uint8_t b1, uint8_t b2, char out[4]
) {
    uint32_t v = ((uint32_t)b0 << 16) | ((uint32_t)b1 << 8) | (uint32_t)b2;
    out[0] = sentinel_b64_alphabet[(v >> 18) & 0x3FU];
    out[1] = sentinel_b64_alphabet[(v >> 12) & 0x3FU];
    out[2] = sentinel_b64_alphabet[(v >> 6) & 0x3FU];
    out[3] = sentinel_b64_alphabet[v & 0x3FU];
}

static inline void sentinel_b64_encode_rem1(uint8_t b0, char out[4]) {
    uint32_t v = (uint32_t)b0;
    out[0] = sentinel_b64_alphabet[(v >> 2) & 0x3FU];
    out[1] = sentinel_b64_alphabet[(v & 0x03U) << 4];
    out[2] = '=';
    out[3] = '=';
}

static inline void sentinel_b64_encode_rem2(uint8_t b0, uint8_t b1, char out[4]) {
    uint32_t v = ((uint32_t)b0 << 8) | (uint32_t)b1;
    out[0] = sentinel_b64_alphabet[(v >> 10) & 0x3FU];
    out[1] = sentinel_b64_alphabet[(v >> 4) & 0x3FU];
    out[2] = sentinel_b64_alphabet[(v & 0x0FU) << 2];
    out[3] = '=';
}

#endif /* SENTINEL_INTERNAL_H */
