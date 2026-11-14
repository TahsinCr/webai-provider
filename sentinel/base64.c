#include "internal.h"

const char sentinel_b64_alphabet[65] =
    "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

size_t sentinel_b64_encoded_size(size_t src_len) {
    if (src_len == 0) {
        return 0;
    }
    size_t triplets = (src_len - 1U) / 3U + 1U;
    if (triplets > SIZE_MAX / 4U) {
        return 0;
    }
    return triplets * 4U;
}

size_t sentinel_b64_decoded_bound(size_t src_len) {
    if (src_len == 0 || (src_len % 4U) != 0) {
        return 0;
    }
    size_t quads = src_len / 4U;
    if (quads > SIZE_MAX / 3U) {
        return 0;
    }
    return quads * 3U;
}

int sentinel_b64_encode(
    const uint8_t *src, size_t src_len, char *dst, size_t dst_cap, size_t *out_len
) {
    if (dst == NULL || (src == NULL && src_len > 0)) {
        return SENTINEL_ERR_INVALID_ARG;
    }
    if (src_len == 0) {
        if (dst_cap == 0) {
            return SENTINEL_ERR_OVERFLOW;
        }
        dst[0] = '\0';
        if (out_len != NULL) {
            *out_len = 0;
        }
        return SENTINEL_OK;
    }
    size_t needed = sentinel_b64_encoded_size(src_len);
    if (needed == 0 || dst_cap <= needed) {
        return SENTINEL_ERR_OVERFLOW;
    }

    size_t di = 0;
    size_t i = 0;
    while (i + 3U <= src_len) {
        sentinel_b64_encode_triplet(src[i], src[i + 1U], src[i + 2U], dst + di);
        di += 4U;
        i += 3U;
    }
    size_t rem = src_len - i;
    if (rem == 1U) {
        sentinel_b64_encode_rem1(src[i], dst + di);
        di += 4U;
    } else if (rem == 2U) {
        sentinel_b64_encode_rem2(src[i], src[i + 1U], dst + di);
        di += 4U;
    }
    dst[di] = '\0';
    if (out_len != NULL) {
        *out_len = di;
    }
    return SENTINEL_OK;
}

static int b64_decode_char(uint8_t ch) {
    if (ch >= 'A' && ch <= 'Z') {
        return (int)(ch - 'A');
    }
    if (ch >= 'a' && ch <= 'z') {
        return (int)(ch - 'a' + 26);
    }
    if (ch >= '0' && ch <= '9') {
        return (int)(ch - '0' + 52);
    }
    if (ch == '+') {
        return 62;
    }
    if (ch == '/') {
        return 63;
    }
    return -1;
}

int sentinel_b64_decode(
    const char *src, size_t src_len, uint8_t *dst, size_t dst_cap, size_t *out_len
) {
    if ((src == NULL && src_len > 0) || (dst == NULL && dst_cap > 0)) {
        return SENTINEL_ERR_INVALID_ARG;
    }
    if (src_len == 0) {
        if (out_len != NULL) {
            *out_len = 0;
        }
        return SENTINEL_OK;
    }
    if ((src_len % 4U) != 0) {
        return SENTINEL_ERR_INVALID_B64;
    }
    size_t max_out = (src_len / 4U) * 3U;
    size_t pad = 0;
    if (src[src_len - 1U] == '=') {
        pad++;
        if (src[src_len - 2U] == '=') {
            pad++;
        }
    }
    size_t exact_out = max_out - pad;
    if (dst_cap < exact_out) {
        return SENTINEL_ERR_OVERFLOW;
    }

    size_t di = 0;
    for (size_t i = 0; i < src_len; i += 4U) {
        int a = b64_decode_char((uint8_t)src[i]);
        int b = b64_decode_char((uint8_t)src[i + 1U]);
        if (a < 0 || b < 0) {
            return SENTINEL_ERR_INVALID_B64;
        }
        uint8_t c_ch = (uint8_t)src[i + 2U];
        uint8_t d_ch = (uint8_t)src[i + 3U];
        int is_last = (i + 4U == src_len);

        if (c_ch == '=') {
            if (!is_last || d_ch != '=' || (b & 0x0F) != 0) {
                return SENTINEL_ERR_INVALID_B64;
            }
            dst[di++] = (uint8_t)(((uint32_t)a << 2) | ((uint32_t)b >> 4));
            continue;
        }
        int c = b64_decode_char(c_ch);
        if (c < 0) {
            return SENTINEL_ERR_INVALID_B64;
        }
        if (d_ch == '=') {
            if (!is_last || (c & 0x03) != 0) {
                return SENTINEL_ERR_INVALID_B64;
            }
            uint32_t v = ((uint32_t)a << 12) | ((uint32_t)b << 6) | (uint32_t)c;
            dst[di++] = (uint8_t)(v >> 10);
            dst[di++] = (uint8_t)((v >> 2) & 0xFFU);
            continue;
        }
        int d = b64_decode_char(d_ch);
        if (d < 0) {
            return SENTINEL_ERR_INVALID_B64;
        }
        uint32_t v = ((uint32_t)a << 18) | ((uint32_t)b << 12) | ((uint32_t)c << 6) | (uint32_t)d;
        dst[di++] = (uint8_t)((v >> 16) & 0xFFU);
        dst[di++] = (uint8_t)((v >> 8) & 0xFFU);
        dst[di++] = (uint8_t)(v & 0xFFU);
    }

    if (out_len != NULL) {
        *out_len = di;
    }
    return SENTINEL_OK;
}
