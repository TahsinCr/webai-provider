#ifndef SENTINEL_H
#define SENTINEL_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define SENTINEL_VERSION_MAJOR 1
#define SENTINEL_VERSION_MINOR 0
#define SENTINEL_VERSION_PATCH 0

#if defined(SENTINEL_STATIC)
#  define SENTINEL_API
#elif defined(_WIN32)
#  if defined(SENTINEL_EXPORTS)
#    define SENTINEL_API __declspec(dllexport)
#  else
#    define SENTINEL_API __declspec(dllimport)
#  endif
#elif defined(__GNUC__) && __GNUC__ >= 4 || defined(__clang__)
#  define SENTINEL_API __attribute__((visibility("default")))
#else
#  define SENTINEL_API
#endif

/* Status codes returned by libsentinel APIs. */
#define SENTINEL_OK 0
#define SENTINEL_ERR_INVALID_ARG -1
#define SENTINEL_ERR_OVERFLOW -2
#define SENTINEL_ERR_NOMEM -3
#define SENTINEL_ERR_INVALID_UTF8 -4
#define SENTINEL_ERR_INVALID_B64 -5
#define SENTINEL_ERR_RANGE -6

/* FNV-1a 32-bit constants matching the Sentinel browser SDK. */
#define SENTINEL_FNV1A_OFFSET_BASIS UINT32_C(2166136261)
#define SENTINEL_FNV1A_PRIME UINT32_C(16777619)
#define SENTINEL_MAX_CODEPOINT UINT32_C(0x10FFFF)

/* Heap cleanup for strings/buffers allocated by libsentinel. */
SENTINEL_API void sentinel_free(void *ptr);

/* Monotonic nanosecond clock used by proof-of-work timing. */
SENTINEL_API int64_t sentinel_monotonic_ns(void);

/* FNV-1a step functions and Murmur3-style avalanche finalizer. */
SENTINEL_API uint32_t sentinel_fnv1a_step_bytes(
    uint32_t hash, const uint8_t *data, size_t len
);
SENTINEL_API uint32_t sentinel_fnv1a_step_utf16(
    uint32_t hash, const uint16_t *units, size_t len
);
SENTINEL_API uint32_t sentinel_fnv1a_step_utf32(
    uint32_t hash, const uint32_t *cps, size_t len
);
SENTINEL_API uint32_t sentinel_fnv1a_step_utf8(
    uint32_t hash, const char *utf8, size_t len, int *err_out
);
SENTINEL_API uint32_t sentinel_fnv1a_finalize_u32(uint32_t hash);
SENTINEL_API void sentinel_fnv1a_finalize_hex(uint32_t hash, char out_hex[9]);
SENTINEL_API int sentinel_fnv1a_avalanche_utf8(
    const char *utf8, size_t len, char out_hex[9]
);

/* RFC 4648 Base64 encoding and strict decoding. */
SENTINEL_API size_t sentinel_b64_encoded_size(size_t src_len);
SENTINEL_API size_t sentinel_b64_decoded_bound(size_t src_len);
SENTINEL_API int sentinel_b64_encode(
    const uint8_t *src, size_t src_len, char *dst, size_t dst_cap, size_t *out_len
);
SENTINEL_API int sentinel_b64_decode(
    const char *src, size_t src_len, uint8_t *dst, size_t dst_cap, size_t *out_len
);

/* Cyclic key XOR for byte strings and Unicode codepoint arrays. */
SENTINEL_API int sentinel_xor_bytes(
    const uint8_t *data, size_t data_len,
    const uint8_t *key, size_t key_len,
    uint8_t *out
);
SENTINEL_API int sentinel_xor_utf32(
    const uint32_t *data, size_t data_len,
    const uint32_t *key, size_t key_len,
    uint32_t *out, uint32_t *max_cp_out
);

/* Parse 1..8 hex digit Sentinel difficulty string into target and right-shift. */
SENTINEL_API int sentinel_parse_difficulty(
    const char *difficulty, size_t len, uint32_t *out_target, uint8_t *out_shift
);

/* Low-level segment proof-of-work solver (shared with language bindings). */
typedef struct {
    uint32_t prefix_hash;
    const uint8_t *tail_prefix;
    size_t tail_prefix_len;
    const uint8_t *middle;
    size_t middle_len;
    const uint8_t *suffix;
    size_t suffix_len;
    uint32_t target_int;
    uint8_t shift;
    uint64_t nonce_start;
    uint64_t max_attempts;
    int64_t fixed_elapsed_ms;   /* >= 0: fixed elapsed_ms; < 0: use monotonic clock */
    int64_t initial_elapsed_ns; /* baseline elapsed ns before entering solver */
} SentinelPowSegments;

typedef struct {
    int found;                  /* 1 solved, 0 exhausted attempts */
    uint64_t nonce;
    uint64_t elapsed_ms;
    char *tail_b64;             /* owned; release via sentinel_pow_segment_result_reset() */
    size_t tail_b64_len;
} SentinelPowSegmentResult;

SENTINEL_API int sentinel_solve_pow_segments(
    const SentinelPowSegments *params,
    SentinelPowSegmentResult *result
);
SENTINEL_API void sentinel_pow_segment_result_reset(SentinelPowSegmentResult *result);

/* High-level standalone proof-of-work solver (returns complete Base64 token). */
typedef struct {
    const char *seed;
    size_t seed_len;
    const uint8_t *prefix;
    size_t prefix_len;
    const uint8_t *middle;
    size_t middle_len;
    const uint8_t *suffix;
    size_t suffix_len;
    const char *difficulty;
    size_t difficulty_len;
    uint64_t max_attempts;
    int64_t initial_elapsed_ns;
} SentinelPowRequest;

typedef struct {
    int found;                  /* 1 solved, 0 exhausted attempts */
    uint64_t nonce;
    uint64_t elapsed_ms;
    char *token;                /* owned; release via sentinel_pow_full_result_reset() */
    size_t token_len;
} SentinelPowFullResult;

SENTINEL_API int sentinel_solve_pow(
    const SentinelPowRequest *req,
    SentinelPowFullResult *result
);
SENTINEL_API void sentinel_pow_full_result_reset(SentinelPowFullResult *result);

#ifdef __cplusplus
}
#endif

#endif /* SENTINEL_H */
