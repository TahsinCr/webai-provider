#include "sentinel.h"

int sentinel_xor_bytes(
    const uint8_t *data, size_t data_len,
    const uint8_t *key, size_t key_len,
    uint8_t *out
) {
    if (key == NULL || key_len == 0 || ((data == NULL || out == NULL) && data_len > 0)) {
        return SENTINEL_ERR_INVALID_ARG;
    }
    size_t ki = 0;
    for (size_t i = 0; i < data_len; i++) {
        out[i] = data[i] ^ key[ki];
        if (++ki == key_len) {
            ki = 0;
        }
    }
    return SENTINEL_OK;
}

int sentinel_xor_utf32(
    const uint32_t *data, size_t data_len,
    const uint32_t *key, size_t key_len,
    uint32_t *out, uint32_t *max_cp_out
) {
    if (key == NULL || key_len == 0 || ((data == NULL || out == NULL) && data_len > 0)) {
        return SENTINEL_ERR_INVALID_ARG;
    }
    uint32_t max_cp = 0;
    size_t ki = 0;
    for (size_t i = 0; i < data_len; i++) {
        uint32_t value = data[i] ^ key[ki];
        if (value > SENTINEL_MAX_CODEPOINT) {
            return SENTINEL_ERR_RANGE;
        }
        out[i] = value;
        if (value > max_cp) {
            max_cp = value;
        }
        if (++ki == key_len) {
            ki = 0;
        }
    }
    if (max_cp_out != NULL) {
        *max_cp_out = max_cp;
    }
    return SENTINEL_OK;
}
