#include "internal.h"

#include <stdlib.h>
#include <string.h>

#define STACK_SCRATCH_CAP 4096U

void sentinel_pow_segment_result_reset(SentinelPowSegmentResult *result) {
    if (result == NULL) {
        return;
    }
    free(result->tail_b64);
    memset(result, 0, sizeof(*result));
}

void sentinel_pow_full_result_reset(SentinelPowFullResult *result) {
    if (result == NULL) {
        return;
    }
    free(result->token);
    memset(result, 0, sizeof(*result));
}

int sentinel_parse_difficulty(
    const char *difficulty, size_t len, uint32_t *out_target, uint8_t *out_shift
) {
    if (difficulty == NULL || len == 0 || len > 8 || out_target == NULL || out_shift == NULL) {
        return SENTINEL_ERR_INVALID_ARG;
    }
    uint32_t acc = 0;
    for (size_t i = 0; i < len; i++) {
        unsigned char c = (unsigned char)difficulty[i];
        uint32_t digit;
        if (c >= '0' && c <= '9') {
            digit = (uint32_t)(c - '0');
        } else if (c >= 'a' && c <= 'f') {
            digit = (uint32_t)(c - 'a' + 10);
        } else if (c >= 'A' && c <= 'F') {
            digit = (uint32_t)(c - 'A' + 10);
        } else {
            return SENTINEL_ERR_INVALID_ARG;
        }
        acc = (acc << 4) | digit;
    }
    *out_target = acc;
    *out_shift = (uint8_t)((8U - len) * 4U);
    return SENTINEL_OK;
}

static size_t u64_to_ascii(uint64_t val, uint8_t *dst) {
    uint8_t tmp[24];
    size_t n = 0;
    do {
        tmp[n++] = (uint8_t)('0' + (val % 10U));
        val /= 10U;
    } while (val > 0);
    for (size_t i = 0; i < n; i++) {
        dst[i] = tmp[n - 1U - i];
    }
    return n;
}

static int64_t round_ns_to_ms(int64_t ns) {
    if (ns <= 0) {
        return 0;
    }
    /* Match Python round(ns / 1_000_000): round half to even. */
    int64_t q = ns / INT64_C(1000000);
    int64_t r = ns % INT64_C(1000000);
    if (r > INT64_C(500000) || (r == INT64_C(500000) && (q & 1) != 0)) {
        return q + 1;
    }
    return q;
}

static size_t build_after_nonce(
    uint8_t *dst,
    const uint8_t *middle,
    size_t middle_len,
    uint64_t elapsed_ms,
    const uint8_t *suffix,
    size_t suffix_len
) {
    size_t pos = 0;
    if (middle_len > 0) {
        memcpy(dst + pos, middle, middle_len);
        pos += middle_len;
    }
    pos += u64_to_ascii(elapsed_ms, dst + pos);
    if (suffix_len > 0) {
        memcpy(dst + pos, suffix, suffix_len);
        pos += suffix_len;
    }
    return pos;
}

static uint32_t fnv1a_b64_tail_hash(uint32_t h, const uint8_t *data, size_t len) {
    char quad[4];
    size_t i = 0;
    while (i + 3U <= len) {
        sentinel_b64_encode_triplet(data[i], data[i + 1U], data[i + 2U], quad);
        h = sentinel_fnv1a_step_unit(h, (uint8_t)quad[0]);
        h = sentinel_fnv1a_step_unit(h, (uint8_t)quad[1]);
        h = sentinel_fnv1a_step_unit(h, (uint8_t)quad[2]);
        h = sentinel_fnv1a_step_unit(h, (uint8_t)quad[3]);
        i += 3U;
    }
    size_t rem = len - i;
    if (rem == 1U) {
        sentinel_b64_encode_rem1(data[i], quad);
    } else if (rem == 2U) {
        sentinel_b64_encode_rem2(data[i], data[i + 1U], quad);
    }
    if (rem > 0) {
        for (size_t j = 0; j < 4U; j++) {
            h = sentinel_fnv1a_step_unit(h, (uint8_t)quad[j]);
        }
    }
    return sentinel_fnv1a_finalize_u32(h);
}

int sentinel_solve_pow_segments(
    const SentinelPowSegments *params,
    SentinelPowSegmentResult *result
) {
    if (params == NULL || result == NULL || params->shift > 28U) {
        return SENTINEL_ERR_INVALID_ARG;
    }
    if ((params->tail_prefix == NULL && params->tail_prefix_len > 0) ||
        (params->middle == NULL && params->middle_len > 0) ||
        (params->suffix == NULL && params->suffix_len > 0)) {
        return SENTINEL_ERR_INVALID_ARG;
    }

    memset(result, 0, sizeof(*result));
    if (params->max_attempts == 0) {
        return SENTINEL_OK;
    }
    if (params->nonce_start > UINT64_MAX - params->max_attempts) {
        return SENTINEL_ERR_OVERFLOW;
    }

    size_t max_u64_digits = 24U;
    if (params->middle_len > SIZE_MAX - max_u64_digits - params->suffix_len) {
        return SENTINEL_ERR_OVERFLOW;
    }
    size_t after_cap = params->middle_len + max_u64_digits + params->suffix_len;
    if (params->tail_prefix_len > SIZE_MAX - max_u64_digits - after_cap) {
        return SENTINEL_ERR_OVERFLOW;
    }
    size_t scratch_cap = params->tail_prefix_len + max_u64_digits + after_cap;

    uint8_t stack_after[STACK_SCRATCH_CAP];
    uint8_t stack_scratch[STACK_SCRATCH_CAP];
    uint8_t *after_buf = stack_after;
    uint8_t *scratch = stack_scratch;
    int heap_alloc = 0;

    if (after_cap > STACK_SCRATCH_CAP || scratch_cap > STACK_SCRATCH_CAP) {
        after_buf = (uint8_t *)malloc(after_cap);
        scratch = (uint8_t *)malloc(scratch_cap);
        if (after_buf == NULL || scratch == NULL) {
            free(after_buf == stack_after ? NULL : after_buf);
            free(scratch == stack_scratch ? NULL : scratch);
            return SENTINEL_ERR_NOMEM;
        }
        heap_alloc = 1;
    }

    if (params->tail_prefix_len > 0) {
        memcpy(scratch, params->tail_prefix, params->tail_prefix_len);
    }

    int64_t entry_ns = (params->fixed_elapsed_ms < 0) ? sentinel_monotonic_ns() : 0;
    int64_t initial_ns = params->initial_elapsed_ns > 0 ? params->initial_elapsed_ns : 0;
    uint64_t elapsed_ms = 0;
    size_t after_len = 0;
    int status = SENTINEL_OK;

    for (uint64_t iter = 0; iter < params->max_attempts; iter++) {
        uint64_t nonce = params->nonce_start + iter;
        if (params->fixed_elapsed_ms < 0) {
            if (iter == 0 || (nonce & UINT64_C(0x7F)) == 0) {
                int64_t now_ns = sentinel_monotonic_ns();
                int64_t delta_ns = (now_ns > entry_ns ? now_ns - entry_ns : 0);
                if (delta_ns > INT64_MAX - initial_ns) {
                    delta_ns = INT64_MAX;
                } else {
                    delta_ns += initial_ns;
                }
                elapsed_ms = (uint64_t)round_ns_to_ms(delta_ns);
                after_len = build_after_nonce(
                    after_buf,
                    params->middle,
                    params->middle_len,
                    elapsed_ms,
                    params->suffix,
                    params->suffix_len
                );
            }
        } else if (iter == 0) {
            elapsed_ms = (uint64_t)params->fixed_elapsed_ms;
            after_len = build_after_nonce(
                after_buf,
                params->middle,
                params->middle_len,
                elapsed_ms,
                params->suffix,
                params->suffix_len
            );
        }

        size_t pos = params->tail_prefix_len;
        pos += u64_to_ascii(nonce, scratch + pos);
        memcpy(scratch + pos, after_buf, after_len);
        size_t total_len = pos + after_len;

        uint32_t mixed = fnv1a_b64_tail_hash(params->prefix_hash, scratch, total_len);
        if ((mixed >> params->shift) <= params->target_int) {
            size_t tail_b64_len = sentinel_b64_encoded_size(total_len);
            if ((total_len > 0 && tail_b64_len == 0) || tail_b64_len == SIZE_MAX) {
                status = SENTINEL_ERR_OVERFLOW;
                break;
            }
            char *tail_b64 = (char *)malloc(tail_b64_len + 1U);
            if (tail_b64 == NULL) {
                status = SENTINEL_ERR_NOMEM;
                break;
            }
            size_t written = 0;
            status = sentinel_b64_encode(
                scratch, total_len, tail_b64, tail_b64_len + 1U, &written
            );
            if (status != SENTINEL_OK) {
                free(tail_b64);
                break;
            }
            result->found = 1;
            result->nonce = nonce;
            result->elapsed_ms = elapsed_ms;
            result->tail_b64 = tail_b64;
            result->tail_b64_len = written;
            break;
        }
    }

    if (heap_alloc) {
        free(after_buf);
        free(scratch);
    }
    return status;
}

int sentinel_solve_pow(
    const SentinelPowRequest *req,
    SentinelPowFullResult *result
) {
    if (req == NULL || result == NULL || req->seed == NULL || req->seed_len == 0) {
        return SENTINEL_ERR_INVALID_ARG;
    }
    if ((req->prefix == NULL && req->prefix_len > 0) ||
        (req->middle == NULL && req->middle_len > 0) ||
        (req->suffix == NULL && req->suffix_len > 0)) {
        return SENTINEL_ERR_INVALID_ARG;
    }

    memset(result, 0, sizeof(*result));

    uint32_t target_int = 0;
    uint8_t shift = 0;
    int status = sentinel_parse_difficulty(
        req->difficulty, req->difficulty_len, &target_int, &shift
    );
    if (status != SENTINEL_OK) {
        return status;
    }

    int utf8_err = SENTINEL_OK;
    uint32_t prefix_hash = sentinel_fnv1a_step_utf8(
        SENTINEL_FNV1A_OFFSET_BASIS, req->seed, req->seed_len, &utf8_err
    );
    if (utf8_err != SENTINEL_OK) {
        return utf8_err;
    }

    size_t aligned = (req->prefix_len / 3U) * 3U;
    size_t head_b64_len = sentinel_b64_encoded_size(aligned);
    char *head_b64 = NULL;
    if (aligned > 0) {
        if (head_b64_len == 0 || head_b64_len == SIZE_MAX) {
            return SENTINEL_ERR_OVERFLOW;
        }
        head_b64 = (char *)malloc(head_b64_len + 1U);
        if (head_b64 == NULL) {
            return SENTINEL_ERR_NOMEM;
        }
        size_t written = 0;
        status = sentinel_b64_encode(
            req->prefix, aligned, head_b64, head_b64_len + 1U, &written
        );
        if (status != SENTINEL_OK) {
            free(head_b64);
            return status;
        }
        head_b64_len = written;
        prefix_hash = sentinel_fnv1a_step_bytes(
            prefix_hash, (const uint8_t *)head_b64, head_b64_len
        );
    }

    SentinelPowSegments seg = {
        .prefix_hash = prefix_hash,
        .tail_prefix = req->prefix + aligned,
        .tail_prefix_len = req->prefix_len - aligned,
        .middle = req->middle,
        .middle_len = req->middle_len,
        .suffix = req->suffix,
        .suffix_len = req->suffix_len,
        .target_int = target_int,
        .shift = shift,
        .nonce_start = 0,
        .max_attempts = req->max_attempts,
        .fixed_elapsed_ms = -1,
        .initial_elapsed_ns = req->initial_elapsed_ns,
    };
    SentinelPowSegmentResult seg_res;
    status = sentinel_solve_pow_segments(&seg, &seg_res);
    if (status != SENTINEL_OK || !seg_res.found) {
        free(head_b64);
        sentinel_pow_segment_result_reset(&seg_res);
        return status;
    }

    if (head_b64_len > SIZE_MAX - seg_res.tail_b64_len - 1U) {
        free(head_b64);
        sentinel_pow_segment_result_reset(&seg_res);
        return SENTINEL_ERR_OVERFLOW;
    }
    size_t total_len = head_b64_len + seg_res.tail_b64_len;
    char *token = (char *)malloc(total_len + 1U);
    if (token == NULL) {
        free(head_b64);
        sentinel_pow_segment_result_reset(&seg_res);
        return SENTINEL_ERR_NOMEM;
    }
    if (head_b64_len > 0) {
        memcpy(token, head_b64, head_b64_len);
    }
    if (seg_res.tail_b64_len > 0) {
        memcpy(token + head_b64_len, seg_res.tail_b64, seg_res.tail_b64_len);
    }
    token[total_len] = '\0';

    result->found = 1;
    result->nonce = seg_res.nonce;
    result->elapsed_ms = seg_res.elapsed_ms;
    result->token = token;
    result->token_len = total_len;

    free(head_b64);
    sentinel_pow_segment_result_reset(&seg_res);
    return SENTINEL_OK;
}
