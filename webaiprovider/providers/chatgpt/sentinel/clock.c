#if !defined(_WIN32) && !defined(_POSIX_C_SOURCE)
#define _POSIX_C_SOURCE 200809L
#endif

#include "sentinel.h"

#include <stdlib.h>

#ifdef _WIN32
#define WIN32_LEAN_AND_MEAN
#include <windows.h>

static INIT_ONCE clock_frequency_once = INIT_ONCE_STATIC_INIT;
static LARGE_INTEGER clock_frequency;

static BOOL CALLBACK initialize_clock_frequency(
    PINIT_ONCE once, PVOID parameter, PVOID *context
) {
    (void)once;
    (void)parameter;
    (void)context;
    return QueryPerformanceFrequency(&clock_frequency);
}
#else
#include <time.h>
#endif

void sentinel_free(void *ptr) {
    free(ptr);
}

int64_t sentinel_monotonic_ns(void) {
#ifdef _WIN32
    LARGE_INTEGER counter;
    if (!InitOnceExecuteOnce(
            &clock_frequency_once, initialize_clock_frequency, NULL, NULL
        ) ||
        clock_frequency.QuadPart <= 0 || !QueryPerformanceCounter(&counter)) {
        return 0;
    }
    /* Split division/modulo to avoid 64-bit multiplication overflow on long uptimes. */
    int64_t secs = counter.QuadPart / clock_frequency.QuadPart;
    int64_t rem = counter.QuadPart % clock_frequency.QuadPart;
    return secs * INT64_C(1000000000) +
           (rem * INT64_C(1000000000)) / clock_frequency.QuadPart;
#else
    struct timespec ts;
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return 0;
    }
    return (int64_t)ts.tv_sec * INT64_C(1000000000) + (int64_t)ts.tv_nsec;
#endif
}
