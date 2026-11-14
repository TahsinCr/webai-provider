#include "internal.h"

static const char HEX_DIGITS[17] = "0123456789abcdef";

uint32_t sentinel_fnv1a_step_bytes(uint32_t hash, const uint8_t *data, size_t len) {
    for (size_t i = 0; i < len; i++) {
        hash = sentinel_fnv1a_step_unit(hash, data[i]);
    }
    return hash;
}

uint32_t sentinel_fnv1a_step_utf16(uint32_t hash, const uint16_t *units, size_t len) {
    for (size_t i = 0; i < len; i++) {
        hash = sentinel_fnv1a_step_unit(hash, units[i]);
    }
    return hash;
}

uint32_t sentinel_fnv1a_step_utf32(uint32_t hash, const uint32_t *cps, size_t len) {
    for (size_t i = 0; i < len; i++) {
        hash = sentinel_fnv1a_step_codepoint(hash, cps[i]);
    }
    return hash;
}

uint32_t sentinel_fnv1a_step_utf8(
    uint32_t hash, const char *utf8, size_t len, int *err_out
) {
    const uint8_t *p = (const uint8_t *)utf8;
    size_t i = 0;
    if (err_out != NULL) {
        *err_out = SENTINEL_OK;
    }
    if (utf8 == NULL && len > 0) {
        if (err_out != NULL) {
            *err_out = SENTINEL_ERR_INVALID_ARG;
        }
        return hash;
    }

    while (i < len) {
        uint8_t b0 = p[i];
        uint32_t code;
        if (b0 < 0x80U) {
            hash = sentinel_fnv1a_step_unit(hash, b0);
            i += 1;
            continue;
        }
        if ((b0 & 0xE0U) == 0xC0U) {
            if (i + 1 >= len) {
                goto invalid_utf8;
            }
            uint8_t b1 = p[i + 1];
            if ((b1 & 0xC0U) != 0x80U) {
                goto invalid_utf8;
            }
            code = ((uint32_t)(b0 & 0x1FU) << 6) | (uint32_t)(b1 & 0x3FU);
            if (code < 0x80U) {
                goto invalid_utf8;
            }
            i += 2;
        } else if ((b0 & 0xF0U) == 0xE0U) {
            if (i + 2 >= len) {
                goto invalid_utf8;
            }
            uint8_t b1 = p[i + 1];
            uint8_t b2 = p[i + 2];
            if ((b1 & 0xC0U) != 0x80U || (b2 & 0xC0U) != 0x80U) {
                goto invalid_utf8;
            }
            code = ((uint32_t)(b0 & 0x0FU) << 12) |
                   ((uint32_t)(b1 & 0x3FU) << 6) |
                   (uint32_t)(b2 & 0x3FU);
            if (code < 0x800U || (code >= 0xD800U && code <= 0xDFFFU)) {
                goto invalid_utf8;
            }
            i += 3;
        } else if ((b0 & 0xF8U) == 0xF0U) {
            if (i + 3 >= len) {
                goto invalid_utf8;
            }
            uint8_t b1 = p[i + 1];
            uint8_t b2 = p[i + 2];
            uint8_t b3 = p[i + 3];
            if ((b1 & 0xC0U) != 0x80U || (b2 & 0xC0U) != 0x80U || (b3 & 0xC0U) != 0x80U) {
                goto invalid_utf8;
            }
            code = ((uint32_t)(b0 & 0x07U) << 18) |
                   ((uint32_t)(b1 & 0x3FU) << 12) |
                   ((uint32_t)(b2 & 0x3FU) << 6) |
                   (uint32_t)(b3 & 0x3FU);
            if (code < 0x10000U || code > SENTINEL_MAX_CODEPOINT) {
                goto invalid_utf8;
            }
            i += 4;
        } else {
            goto invalid_utf8;
        }

        hash = sentinel_fnv1a_step_codepoint(hash, code);
    }
    return hash;

invalid_utf8:
    if (err_out != NULL) {
        *err_out = SENTINEL_ERR_INVALID_UTF8;
    }
    return hash;
}

uint32_t sentinel_fnv1a_finalize_u32(uint32_t hash) {
    hash ^= hash >> 16;
    hash *= UINT32_C(2246822507);
    hash ^= hash >> 13;
    hash *= UINT32_C(3266489909);
    hash ^= hash >> 16;
    return hash;
}

void sentinel_fnv1a_finalize_hex(uint32_t hash, char out_hex[9]) {
    uint32_t mixed = sentinel_fnv1a_finalize_u32(hash);
    for (int shift = 28, idx = 0; shift >= 0; shift -= 4, idx++) {
        out_hex[idx] = HEX_DIGITS[(mixed >> shift) & 0x0FU];
    }
    out_hex[8] = '\0';
}

int sentinel_fnv1a_avalanche_utf8(const char *utf8, size_t len, char out_hex[9]) {
    int err = SENTINEL_OK;
    if ((utf8 == NULL && len > 0) || out_hex == NULL) {
        return SENTINEL_ERR_INVALID_ARG;
    }
    uint32_t hash = sentinel_fnv1a_step_utf8(
        SENTINEL_FNV1A_OFFSET_BASIS, utf8 != NULL ? utf8 : "", len, &err
    );
    if (err != SENTINEL_OK) {
        return err;
    }
    sentinel_fnv1a_finalize_hex(hash, out_hex);
    return SENTINEL_OK;
}
