#include "sentinel.h"

#include <assert.h>
#include <stdio.h>
#include <string.h>

static void test_fnv1a_and_utf16(void) {
    char hex[9];

    /* Known UTF-16 code-unit vector: ASCII + BMP + astral emoji (surrogate pair). */
    const char *text = "seed-\xF0\x9F\x9A\x80"; /* seed-🚀 */
    assert(sentinel_fnv1a_avalanche_utf8(text, strlen(text), hex) == SENTINEL_OK);

    uint16_t utf16_units[] = {'s', 'e', 'e', 'd', '-', 0xD83D, 0xDE80};
    uint32_t h_u16 = sentinel_fnv1a_step_utf16(SENTINEL_FNV1A_OFFSET_BASIS, utf16_units, 7);
    char hex_u16[9];
    sentinel_fnv1a_finalize_hex(h_u16, hex_u16);
    assert(strcmp(hex, hex_u16) == 0);

    uint32_t cps[] = {'s', 'e', 'e', 'd', '-', 0x1F680};
    uint32_t h_u32 = sentinel_fnv1a_step_utf32(SENTINEL_FNV1A_OFFSET_BASIS, cps, 6);
    char hex_u32[9];
    sentinel_fnv1a_finalize_hex(h_u32, hex_u32);
    assert(strcmp(hex, hex_u32) == 0);

    int err = SENTINEL_OK;
    sentinel_fnv1a_step_utf8(SENTINEL_FNV1A_OFFSET_BASIS, "\xF0\x9F", 2, &err);
    assert(err == SENTINEL_ERR_INVALID_UTF8);
}

static void test_base64_roundtrip(void) {
    const uint8_t raw[] = "Sentinel-Payload-12345";
    char b64[128];
    size_t b64_len = 0;
    assert(sentinel_b64_encode(raw, sizeof(raw) - 1U, b64, sizeof(b64), &b64_len) == SENTINEL_OK);

    uint8_t decoded[128];
    size_t decoded_len = 0;
    assert(sentinel_b64_decode(b64, b64_len, decoded, sizeof(decoded), &decoded_len) == SENTINEL_OK);
    assert(decoded_len == sizeof(raw) - 1U);
    assert(memcmp(raw, decoded, decoded_len) == 0);

    /* Reject malformed padding/non-alphabet characters cleanly. */
    assert(sentinel_b64_decode("bad!", 4, decoded, sizeof(decoded), &decoded_len) == SENTINEL_ERR_INVALID_B64);
    assert(sentinel_b64_decode("AB", 2, decoded, sizeof(decoded), &decoded_len) == SENTINEL_ERR_INVALID_B64);
}

static void test_xor_helpers(void) {
    const uint8_t msg[] = "HelloSentinel";
    const uint8_t key[] = "k3y";
    uint8_t enc[sizeof(msg)];
    uint8_t dec[sizeof(msg)];
    assert(sentinel_xor_bytes(msg, sizeof(msg) - 1U, key, sizeof(key) - 1U, enc) == SENTINEL_OK);
    assert(sentinel_xor_bytes(enc, sizeof(msg) - 1U, key, sizeof(key) - 1U, dec) == SENTINEL_OK);
    assert(memcmp(msg, dec, sizeof(msg) - 1U) == 0);

    uint32_t cps[] = {0x41, 0x1F600, 0x4E2D};
    uint32_t ukey[] = {0x15, 0x03};
    uint32_t out[3];
    uint32_t max_cp = 0;
    assert(sentinel_xor_utf32(cps, 3, ukey, 2, out, &max_cp) == SENTINEL_OK);
    assert(max_cp == (0x1F600 ^ 0x03));
    uint32_t back[3];
    assert(sentinel_xor_utf32(out, 3, ukey, 2, back, NULL) == SENTINEL_OK);
    assert(memcmp(cps, back, sizeof(cps)) == 0);

    uint32_t overflow_cp[] = {0x100000};
    uint32_t overflow_key[] = {0x10000};
    assert(sentinel_xor_utf32(overflow_cp, 1, overflow_key, 1, out, NULL) == SENTINEL_ERR_RANGE);
}

static void test_pow_solver(void) {
    const char *prefix = "[1920,\"1080\",4000,";
    const char *middle = ",\"Mozilla/5.0\",\"\",";
    const char *suffix = ",\"en-US\",1]";
    const char *seed = "0.1234567890abcdef";
    const char *diff = "000fff";

    SentinelPowRequest req = {
        .seed = seed,
        .seed_len = strlen(seed),
        .prefix = (const uint8_t *)prefix,
        .prefix_len = strlen(prefix),
        .middle = (const uint8_t *)middle,
        .middle_len = strlen(middle),
        .suffix = (const uint8_t *)suffix,
        .suffix_len = strlen(suffix),
        .difficulty = diff,
        .difficulty_len = strlen(diff),
        .max_attempts = 500000U,
        .initial_elapsed_ns = 0,
    };

    SentinelPowFullResult res;
    int rc = sentinel_solve_pow(&req, &res);
    assert(rc == SENTINEL_OK);
    assert(res.found == 1);
    assert(res.token != NULL);
    assert(res.token_len > 0);

    uint8_t decoded[512];
    size_t decoded_len = 0;
    assert(
        sentinel_b64_decode(res.token, res.token_len, decoded, sizeof(decoded), &decoded_len)
        == SENTINEL_OK
    );

    uint32_t target_int = 0;
    uint8_t shift = 0;
    assert(sentinel_parse_difficulty(diff, strlen(diff), &target_int, &shift) == SENTINEL_OK);

    uint32_t check_hash = sentinel_fnv1a_step_bytes(
        SENTINEL_FNV1A_OFFSET_BASIS, (const uint8_t *)seed, strlen(seed)
    );
    check_hash = sentinel_fnv1a_step_bytes(
        check_hash, (const uint8_t *)res.token, res.token_len
    );
    uint32_t mixed = sentinel_fnv1a_finalize_u32(check_hash);
    assert((mixed >> shift) <= target_int);

    printf(
        "PASS sentinel PoW (diff=%s, nonce=%llu, elapsed_ms=%llu)\n",
        diff,
        (unsigned long long)res.nonce,
        (unsigned long long)res.elapsed_ms
    );

    sentinel_pow_full_result_reset(&res);
    assert(res.token == NULL);
    assert(res.token_len == 0);
}

int main(void) {
    test_fnv1a_and_utf16();
    test_base64_roundtrip();
    test_xor_helpers();
    test_pow_solver();
    puts("All libsentinel C99 tests passed.");
    return 0;
}
