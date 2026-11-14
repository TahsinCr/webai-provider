#if !defined(_WIN32) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include "sentinel.h"

#include <stdlib.h>

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>
#else
#include <time.h>
#endif

void sentinel_free(void *ptr) {
    free(ptr);
}

int64_t sentinel_monotonic_ns(void) {
#ifdef _WIN32
    static LARGE_INTEGER freq;
    LARGE_INTEGER counter;
    if (freq.QuadPart == 0) {
        QueryPerformanceFrequency(&freq);
    }
    QueryPerformanceCounter(&counter);
    if (freq.QuadPart <= 0) {
        return 0;
    }
    /* Split division/modulo to avoid 64-bit multiplication overflow on long uptimes. */
    int64_t secs = counter.QuadPart / freq.QuadPart;
    int64_t rem = counter.QuadPart % freq.QuadPart;
    return secs * INT64_C(1000000000) + (rem * INT64_C(1000000000)) / freq.QuadPart;
#else
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    return (int64_t)ts.tv_sec * INT64_C(1000000000) + (int64_t)ts.tv_nsec;
#endif
}
