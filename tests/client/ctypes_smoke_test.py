#!/usr/bin/env python3
"""ctypes smoke test for the C ABI shared library."""

import ctypes
import sys
from pathlib import Path


class MygramClientConfig(ctypes.Structure):
    _fields_ = [
        ("host", ctypes.c_char_p),
        ("port", ctypes.c_uint16),
        ("timeout_ms", ctypes.c_uint32),
        ("recv_buffer_size", ctypes.c_uint32),
    ]


class MygramClientConfigV2(ctypes.Structure):
    _fields_ = [
        ("struct_size", ctypes.c_uint32),
        ("version", ctypes.c_uint32),
        ("host", ctypes.c_char_p),
        ("port", ctypes.c_uint16),
        ("timeout_ms", ctypes.c_uint32),
        ("recv_buffer_size", ctypes.c_uint32),
        ("unix_socket_path", ctypes.c_char_p),
        ("dump_save_timeout_ms", ctypes.c_uint32),
    ]


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: ctypes_smoke_test.py <libmygramclient path>", file=sys.stderr)
        return 2

    library_path = Path(sys.argv[1]).resolve()
    lib = ctypes.CDLL(str(library_path))

    lib.mygramclient_create.argtypes = [ctypes.POINTER(MygramClientConfig)]
    lib.mygramclient_create.restype = ctypes.c_void_p
    lib.mygramclient_create_v2.argtypes = [ctypes.POINTER(MygramClientConfigV2)]
    lib.mygramclient_create_v2.restype = ctypes.c_void_p
    lib.mygramclient_connect.argtypes = [ctypes.c_void_p]
    lib.mygramclient_connect.restype = ctypes.c_int
    lib.mygramclient_get_last_error.argtypes = [ctypes.c_void_p]
    lib.mygramclient_get_last_error.restype = ctypes.c_char_p
    lib.mygramclient_get_last_error_code.argtypes = [ctypes.c_void_p]
    lib.mygramclient_get_last_error_code.restype = ctypes.c_int
    lib.mygramclient_destroy.argtypes = [ctypes.c_void_p]
    lib.mygramclient_destroy.restype = None
    lib.mygramclient_search_raw.argtypes = [
        ctypes.c_void_p,
        ctypes.c_char_p,
        ctypes.c_char_p,
        ctypes.c_uint32,
        ctypes.c_uint32,
        ctypes.c_void_p,
    ]
    lib.mygramclient_search_raw.restype = ctypes.c_int

    config = MygramClientConfig(b"127.0.0.1", 9, 100, 65536)
    client = lib.mygramclient_create(ctypes.byref(config))
    if not client:
        print("mygramclient_create returned NULL", file=sys.stderr)
        return 1

    try:
        rc = lib.mygramclient_connect(client)
        if rc == 0:
            print("connect to closed port unexpectedly succeeded", file=sys.stderr)
            return 1

        error = lib.mygramclient_get_last_error(client)
        error_code = lib.mygramclient_get_last_error_code(client)
        if not error:
            print("missing last error string", file=sys.stderr)
            return 1
        if error_code == 0:
            print("missing last error code", file=sys.stderr)
            return 1
    finally:
        lib.mygramclient_destroy(client)

    config_v2 = MygramClientConfigV2()
    config_v2.struct_size = ctypes.sizeof(config_v2)
    config_v2.version = 2
    config_v2.host = b"127.0.0.1"
    config_v2.port = 11016
    config_v2.timeout_ms = 100
    config_v2.dump_save_timeout_ms = 600000
    client_v2 = lib.mygramclient_create_v2(ctypes.byref(config_v2))
    if not client_v2:
        print("mygramclient_create_v2 returned NULL", file=sys.stderr)
        return 1
    try:
        if lib.mygramclient_search_raw(None, b"db.table", b"alpha AND beta", 10, 0, None) != -1:
            print("mygramclient_search_raw NULL guard failed", file=sys.stderr)
            return 1
    finally:
        lib.mygramclient_destroy(client_v2)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
