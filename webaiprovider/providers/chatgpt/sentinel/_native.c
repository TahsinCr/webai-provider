#define PY_SSIZE_T_CLEAN
#include <Python.h>

#include <stdint.h>
#include <stdlib.h>

#include "sentinel.h"

#define U32_STACK_CAP 512

#if PY_VERSION_HEX < 0x030C0000
#  define SENTINEL_PY_UNICODE_READY(obj) PyUnicode_READY(obj)
#else
#  define SENTINEL_PY_UNICODE_READY(obj) (0)
#endif

static uint32_t hash_pyunicode_utf16(uint32_t hash, PyObject *text) {
    Py_ssize_t len = PyUnicode_GET_LENGTH(text);
    if (len == 0) {
        return hash;
    }
    int kind = PyUnicode_KIND(text);
    if (kind == PyUnicode_1BYTE_KIND) {
        return sentinel_fnv1a_step_bytes(
            hash, (const uint8_t *)PyUnicode_1BYTE_DATA(text), (size_t)len
        );
    }
    if (kind == PyUnicode_2BYTE_KIND) {
        return sentinel_fnv1a_step_utf16(
            hash, (const uint16_t *)PyUnicode_2BYTE_DATA(text), (size_t)len
        );
    }
    return sentinel_fnv1a_step_utf32(
        hash, (const uint32_t *)PyUnicode_4BYTE_DATA(text), (size_t)len
    );
}

static void extract_pyunicode_u32(PyObject *text, uint32_t *dst) {
    Py_ssize_t len = PyUnicode_GET_LENGTH(text);
    int kind = PyUnicode_KIND(text);
    if (kind == PyUnicode_1BYTE_KIND) {
        const Py_UCS1 *p = PyUnicode_1BYTE_DATA(text);
        for (Py_ssize_t i = 0; i < len; i++) {
            dst[i] = (uint32_t)p[i];
        }
    } else if (kind == PyUnicode_2BYTE_KIND) {
        const Py_UCS2 *p = PyUnicode_2BYTE_DATA(text);
        for (Py_ssize_t i = 0; i < len; i++) {
            dst[i] = (uint32_t)p[i];
        }
    } else {
        const Py_UCS4 *p = PyUnicode_4BYTE_DATA(text);
        for (Py_ssize_t i = 0; i < len; i++) {
            dst[i] = (uint32_t)p[i];
        }
    }
}

static PyObject *py_fnv1a_step_utf16(PyObject *self, PyObject *args) {
    (void)self;
    unsigned int hash_value = 0;
    PyObject *text = NULL;
    if (!PyArg_ParseTuple(args, "IO!", &hash_value, &PyUnicode_Type, &text)) {
        return NULL;
    }
    if (SENTINEL_PY_UNICODE_READY(text) < 0) {
        return NULL;
    }
    uint32_t next = hash_pyunicode_utf16((uint32_t)hash_value, text);
    return PyLong_FromUnsignedLong((unsigned long)next);
}

static PyObject *py_fnv1a_step_bytes(PyObject *self, PyObject *args) {
    (void)self;
    unsigned int hash_value = 0;
    Py_buffer view;
    if (!PyArg_ParseTuple(args, "Iy*", &hash_value, &view)) {
        return NULL;
    }
    uint32_t next = sentinel_fnv1a_step_bytes(
        (uint32_t)hash_value, (const uint8_t *)view.buf, (size_t)view.len
    );
    PyBuffer_Release(&view);
    return PyLong_FromUnsignedLong((unsigned long)next);
}

static PyObject *py_fnv1a_avalanche(PyObject *self, PyObject *args) {
    (void)self;
    PyObject *text = NULL;
    if (!PyArg_ParseTuple(args, "O!", &PyUnicode_Type, &text)) {
        return NULL;
    }
    if (SENTINEL_PY_UNICODE_READY(text) < 0) {
        return NULL;
    }
    uint32_t hash = hash_pyunicode_utf16(SENTINEL_FNV1A_OFFSET_BASIS, text);
    char hex_buf[9];
    sentinel_fnv1a_finalize_hex(hash, hex_buf);
    return PyUnicode_FromStringAndSize(hex_buf, 8);
}

static PyObject *py_xor_strings(PyObject *self, PyObject *args) {
    (void)self;
    PyObject *data = NULL;
    PyObject *key = NULL;
    if (!PyArg_ParseTuple(args, "O!O!", &PyUnicode_Type, &data, &PyUnicode_Type, &key)) {
        return NULL;
    }
    if (SENTINEL_PY_UNICODE_READY(data) < 0 || SENTINEL_PY_UNICODE_READY(key) < 0) {
        return NULL;
    }

    Py_ssize_t klen = PyUnicode_GET_LENGTH(key);
    if (klen == 0) {
        PyErr_SetString(PyExc_ValueError, "Sentinel VM key must be non-empty");
        return NULL;
    }
    Py_ssize_t dlen = PyUnicode_GET_LENGTH(data);
    if (dlen == 0) {
        return PyUnicode_FromStringAndSize("", 0);
    }

    /* Fast path: both operands are 1-byte strings so result fits strictly in UCS1. */
    if (PyUnicode_KIND(data) == PyUnicode_1BYTE_KIND && PyUnicode_KIND(key) == PyUnicode_1BYTE_KIND) {
        PyObject *out = PyUnicode_New(dlen, 255);
        if (out == NULL) {
            return NULL;
        }
        int rc = sentinel_xor_bytes(
            (const uint8_t *)PyUnicode_1BYTE_DATA(data),
            (size_t)dlen,
            (const uint8_t *)PyUnicode_1BYTE_DATA(key),
            (size_t)klen,
            (uint8_t *)PyUnicode_1BYTE_DATA(out)
        );
        if (rc != SENTINEL_OK) {
            Py_DECREF(out);
            PyErr_SetString(PyExc_ValueError, "XOR failed on byte strings");
            return NULL;
        }
        return out;
    }

    if ((size_t)dlen > SIZE_MAX / sizeof(uint32_t) || (size_t)klen > SIZE_MAX / sizeof(uint32_t)) {
        return PyErr_NoMemory();
    }

    uint32_t stack_data[U32_STACK_CAP];
    uint32_t stack_key[U32_STACK_CAP];
    uint32_t stack_out[U32_STACK_CAP];

    uint32_t *data_u32 = dlen <= U32_STACK_CAP
        ? stack_data
        : (uint32_t *)PyMem_Malloc((size_t)dlen * sizeof(uint32_t));
    uint32_t *key_u32 = klen <= U32_STACK_CAP
        ? stack_key
        : (uint32_t *)PyMem_Malloc((size_t)klen * sizeof(uint32_t));
    uint32_t *out_u32 = dlen <= U32_STACK_CAP
        ? stack_out
        : (uint32_t *)PyMem_Malloc((size_t)dlen * sizeof(uint32_t));

    if (data_u32 == NULL || key_u32 == NULL || out_u32 == NULL) {
        if (data_u32 != stack_data) PyMem_Free(data_u32);
        if (key_u32 != stack_key) PyMem_Free(key_u32);
        if (out_u32 != stack_out) PyMem_Free(out_u32);
        return PyErr_NoMemory();
    }

    extract_pyunicode_u32(data, data_u32);
    extract_pyunicode_u32(key, key_u32);

    uint32_t max_cp = 0;
    int rc = sentinel_xor_utf32(data_u32, (size_t)dlen, key_u32, (size_t)klen, out_u32, &max_cp);
    PyObject *out = NULL;

    if (rc == SENTINEL_ERR_RANGE) {
        PyErr_SetString(PyExc_ValueError, "chr() arg not in range(0x110000)");
    } else if (rc != SENTINEL_OK) {
        PyErr_SetString(PyExc_ValueError, "Sentinel XOR failed");
    } else {
        out = PyUnicode_New(dlen, (Py_UCS4)max_cp);
        if (out != NULL) {
            int out_kind = PyUnicode_KIND(out);
            void *out_data = PyUnicode_DATA(out);
            for (Py_ssize_t i = 0; i < dlen; i++) {
                PyUnicode_WRITE(out_kind, out_data, i, (Py_UCS4)out_u32[i]);
            }
        }
    }

    if (data_u32 != stack_data) PyMem_Free(data_u32);
    if (key_u32 != stack_key) PyMem_Free(key_u32);
    if (out_u32 != stack_out) PyMem_Free(out_u32);
    return out;
}

static PyObject *py_solve_pow_segments(PyObject *self, PyObject *args) {
    (void)self;
    unsigned int prefix_hash = 0;
    PyObject *tail_prefix_obj = NULL;
    PyObject *middle_obj = NULL;
    PyObject *suffix_obj = NULL;
    unsigned int target_int = 0;
    unsigned int shift = 0;
    unsigned long long nonce_start = 0;
    unsigned long long max_attempts = 0;
    long long fixed_elapsed_ms = -1;
    long long initial_elapsed_ns = 0;

    if (!PyArg_ParseTuple(
            args,
            "IO!O!O!IIKKLL",
            &prefix_hash,
            &PyBytes_Type,
            &tail_prefix_obj,
            &PyBytes_Type,
            &middle_obj,
            &PyBytes_Type,
            &suffix_obj,
            &target_int,
            &shift,
            &nonce_start,
            &max_attempts,
            &fixed_elapsed_ms,
            &initial_elapsed_ns)) {
        return NULL;
    }

    if (shift > 28U) {
        PyErr_SetString(PyExc_ValueError, "difficulty shift out of range");
        return NULL;
    }

    SentinelPowSegments params = {
        .prefix_hash = (uint32_t)prefix_hash,
        .tail_prefix = (const uint8_t *)PyBytes_AS_STRING(tail_prefix_obj),
        .tail_prefix_len = (size_t)PyBytes_GET_SIZE(tail_prefix_obj),
        .middle = (const uint8_t *)PyBytes_AS_STRING(middle_obj),
        .middle_len = (size_t)PyBytes_GET_SIZE(middle_obj),
        .suffix = (const uint8_t *)PyBytes_AS_STRING(suffix_obj),
        .suffix_len = (size_t)PyBytes_GET_SIZE(suffix_obj),
        .target_int = (uint32_t)target_int,
        .shift = (uint8_t)shift,
        .nonce_start = (uint64_t)nonce_start,
        .max_attempts = (uint64_t)max_attempts,
        .fixed_elapsed_ms = (int64_t)fixed_elapsed_ms,
        .initial_elapsed_ns = (int64_t)initial_elapsed_ns,
    };
    SentinelPowSegmentResult res;
    int rc;

    Py_BEGIN_ALLOW_THREADS
    rc = sentinel_solve_pow_segments(&params, &res);
    Py_END_ALLOW_THREADS

    if (rc == SENTINEL_ERR_NOMEM) {
        return PyErr_NoMemory();
    }
    if (rc != SENTINEL_OK) {
        sentinel_pow_segment_result_reset(&res);
        PyErr_Format(PyExc_ValueError, "sentinel_solve_pow_segments failed (%d)", rc);
        return NULL;
    }
    if (!res.found) {
        sentinel_pow_segment_result_reset(&res);
        Py_RETURN_NONE;
    }

    uint64_t nonce = res.nonce;
    uint64_t elapsed_ms = res.elapsed_ms;
    PyObject *tail_b64 = PyUnicode_DecodeASCII(
        res.tail_b64, (Py_ssize_t)res.tail_b64_len, "strict"
    );
    sentinel_pow_segment_result_reset(&res);
    if (tail_b64 == NULL) {
        return NULL;
    }

    return Py_BuildValue(
        "(KKN)",
        (unsigned long long)nonce,
        (unsigned long long)elapsed_ms,
        tail_b64
    );
}

static PyMethodDef NativeMethods[] = {
    {"fnv1a_step_utf16", py_fnv1a_step_utf16, METH_VARARGS, "Step UTF-16 FNV-1a hash over str."},
    {"fnv1a_step_bytes", py_fnv1a_step_bytes, METH_VARARGS, "Step FNV-1a hash over ASCII/bytes."},
    {
        "fnv1a_avalanche",
        py_fnv1a_avalanche,
        METH_VARARGS,
        "Compute 8-hex UTF-16 FNV-1a avalanche digest.",
    },
    {"xor_strings", py_xor_strings, METH_VARARGS, "Cyclic codepoint XOR between two strings."},
    {
        "solve_pow_segments",
        py_solve_pow_segments,
        METH_VARARGS,
        "Solve Sentinel PoW segments without the GIL.",
    },
    {NULL, NULL, 0, NULL},
};

static struct PyModuleDef nativemodule = {
    PyModuleDef_HEAD_INIT,
    "_native",
    "C99-accelerated Sentinel proof-of-work and hashing primitives.",
    -1,
    NativeMethods,
    NULL,
    NULL,
    NULL,
    NULL,
};

PyMODINIT_FUNC PyInit__native(void) {
    return PyModule_Create(&nativemodule);
}
