#!/usr/bin/env python3
"""ctypes smoke test for every public function in the C ABI shared library."""

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


def bind(lib: ctypes.CDLL, name: str, restype: object, *argtypes: object) -> object:
    function = getattr(lib, name)
    function.argtypes = list(argtypes)
    function.restype = restype
    return function


def bind_public_api(lib: ctypes.CDLL) -> None:
    pointer = ctypes.c_void_p
    uint32 = ctypes.c_uint32
    size = ctypes.c_size_t
    integer = ctypes.c_int

    bind(lib, "mygramclient_create", pointer, ctypes.POINTER(MygramClientConfig))
    bind(lib, "mygramclient_create_v2", pointer, ctypes.POINTER(MygramClientConfigV2))
    bind(lib, "mygramclient_destroy", None, pointer)
    bind(lib, "mygramclient_connect", integer, pointer)
    bind(lib, "mygramclient_disconnect", None, pointer)
    bind(lib, "mygramclient_is_connected", integer, pointer)
    for name in (
        "mygramclient_search",
        "mygramclient_search_raw",
        "mygramclient_search_with_highlights",
        "mygramclient_search_raw_with_highlights",
    ):
        bind(lib, name, integer, pointer, pointer, pointer, uint32, uint32, pointer)
    bind(lib, "mygramclient_search_with_options", integer, pointer, pointer, pointer, pointer, pointer)
    for name in ("mygramclient_search_with_highlights_advanced", "mygramclient_search_advanced"):
        bind(
            lib,
            name,
            integer,
            pointer,
            pointer,
            pointer,
            uint32,
            uint32,
            pointer,
            size,
            pointer,
            size,
            pointer,
            pointer,
            size,
            pointer,
            integer,
            pointer,
        )
    bind(lib, "mygramclient_count", integer, pointer, pointer, pointer, pointer)
    bind(
        lib,
        "mygramclient_count_advanced",
        integer,
        pointer,
        pointer,
        pointer,
        pointer,
        size,
        pointer,
        size,
        pointer,
        pointer,
        size,
        pointer,
    )
    bind(lib, "mygramclient_facet", integer, pointer, pointer, pointer, pointer, uint32, pointer)
    bind(lib, "mygramclient_facet_paged", integer, pointer, pointer, pointer, pointer, uint32, uint32, pointer)
    bind(
        lib,
        "mygramclient_facet_advanced",
        integer,
        pointer,
        pointer,
        pointer,
        pointer,
        uint32,
        pointer,
        size,
        pointer,
        size,
        pointer,
        pointer,
        size,
        pointer,
    )
    bind(
        lib,
        "mygramclient_facet_advanced_paged",
        integer,
        pointer,
        pointer,
        pointer,
        pointer,
        uint32,
        uint32,
        pointer,
        size,
        pointer,
        size,
        pointer,
        pointer,
        size,
        pointer,
    )
    bind(lib, "mygramclient_get", integer, pointer, pointer, pointer, pointer)
    bind(lib, "mygramclient_info", integer, pointer, pointer)
    bind(lib, "mygramclient_get_config", integer, pointer, pointer)
    bind(lib, "mygramclient_set_variable", integer, pointer, pointer, pointer)
    bind(lib, "mygramclient_show_variables", integer, pointer, pointer, pointer)
    bind(lib, "mygramclient_cache_clear", integer, pointer, pointer)
    bind(lib, "mygramclient_cache_stats", integer, pointer, pointer)
    bind(lib, "mygramclient_cache_enable", integer, pointer)
    bind(lib, "mygramclient_cache_disable", integer, pointer)
    bind(lib, "mygramclient_optimize", integer, pointer, pointer, pointer)
    bind(lib, "mygramclient_sync", integer, pointer, pointer, pointer)
    bind(lib, "mygramclient_sync_status", integer, pointer, pointer)
    bind(lib, "mygramclient_sync_stop", integer, pointer, pointer, pointer)
    bind(lib, "mygramclient_dump_info", integer, pointer, pointer, pointer)
    bind(lib, "mygramclient_dump_status", integer, pointer, pointer)
    bind(lib, "mygramclient_dump_verify", integer, pointer, pointer, pointer)
    bind(lib, "mygramclient_save", integer, pointer, pointer, pointer)
    bind(lib, "mygramclient_load", integer, pointer, pointer, pointer)
    bind(lib, "mygramclient_replication_status", integer, pointer, pointer)
    bind(lib, "mygramclient_free_replication_status", None, pointer)
    bind(lib, "mygramclient_replication_stop", integer, pointer)
    bind(lib, "mygramclient_replication_start", integer, pointer)
    bind(lib, "mygramclient_debug_on", integer, pointer)
    bind(lib, "mygramclient_debug_off", integer, pointer)
    bind(lib, "mygramclient_send_command", integer, pointer, pointer, pointer)
    bind(lib, "mygramclient_get_last_error", ctypes.c_char_p, pointer)
    bind(lib, "mygramclient_get_last_error_code", integer, pointer)
    for name in (
        "mygramclient_free_search_result",
        "mygramclient_free_search_result_with_highlights",
        "mygramclient_free_facet_result",
        "mygramclient_free_document",
        "mygramclient_free_server_info",
        "mygramclient_free_string",
        "mygramclient_free_parsed_expression",
    ):
        bind(lib, name, None, pointer)
    bind(lib, "mygramclient_parse_search_expression", integer, pointer, pointer)
    bind(lib, "mygramclient_parse_search_expression_ex", integer, pointer, pointer, pointer)
    bind(lib, "mygramclient_convert_search_expression", integer, pointer, pointer)
    bind(lib, "mygramclient_convert_search_expression_ex", integer, pointer, pointer, pointer)


def verify_null_guards(lib: ctypes.CDLL) -> None:
    if lib.mygramclient_create(None) is not None or lib.mygramclient_create_v2(None) is not None:
        raise AssertionError("create NULL guard failed")
    if lib.mygramclient_connect(None) != -1 or lib.mygramclient_is_connected(None) != 0:
        raise AssertionError("connection NULL guard failed")

    failing_calls = {
        "mygramclient_search": (None, None, None, 0, 0, None),
        "mygramclient_search_raw": (None, None, None, 0, 0, None),
        "mygramclient_search_with_highlights": (None, None, None, 0, 0, None),
        "mygramclient_search_raw_with_highlights": (None, None, None, 0, 0, None),
        "mygramclient_search_with_options": (None, None, None, None, None),
        "mygramclient_search_with_highlights_advanced": (
            None,
            None,
            None,
            0,
            0,
            None,
            0,
            None,
            0,
            None,
            None,
            0,
            None,
            0,
            None,
        ),
        "mygramclient_search_advanced": (
            None,
            None,
            None,
            0,
            0,
            None,
            0,
            None,
            0,
            None,
            None,
            0,
            None,
            0,
            None,
        ),
        "mygramclient_count": (None, None, None, None),
        "mygramclient_count_advanced": (None, None, None, None, 0, None, 0, None, None, 0, None),
        "mygramclient_facet": (None, None, None, None, 0, None),
        "mygramclient_facet_paged": (None, None, None, None, 0, 0, None),
        "mygramclient_facet_advanced": (None, None, None, None, 0, None, 0, None, 0, None, None, 0, None),
        "mygramclient_facet_advanced_paged": (
            None,
            None,
            None,
            None,
            0,
            0,
            None,
            0,
            None,
            0,
            None,
            None,
            0,
            None,
        ),
        "mygramclient_get": (None, None, None, None),
        "mygramclient_info": (None, None),
        "mygramclient_get_config": (None, None),
        "mygramclient_set_variable": (None, None, None),
        "mygramclient_show_variables": (None, None, None),
        "mygramclient_cache_clear": (None, None),
        "mygramclient_cache_stats": (None, None),
        "mygramclient_cache_enable": (None,),
        "mygramclient_cache_disable": (None,),
        "mygramclient_optimize": (None, None, None),
        "mygramclient_sync": (None, None, None),
        "mygramclient_sync_status": (None, None),
        "mygramclient_sync_stop": (None, None, None),
        "mygramclient_dump_info": (None, None, None),
        "mygramclient_dump_status": (None, None),
        "mygramclient_dump_verify": (None, None, None),
        "mygramclient_save": (None, None, None),
        "mygramclient_load": (None, None, None),
        "mygramclient_replication_status": (None, None),
        "mygramclient_replication_stop": (None,),
        "mygramclient_replication_start": (None,),
        "mygramclient_debug_on": (None,),
        "mygramclient_debug_off": (None,),
        "mygramclient_send_command": (None, None, None),
        "mygramclient_parse_search_expression": (None, None),
        "mygramclient_parse_search_expression_ex": (None, None, None),
        "mygramclient_convert_search_expression": (None, None),
        "mygramclient_convert_search_expression_ex": (None, None, None),
    }
    for name, arguments in failing_calls.items():
        if getattr(lib, name)(*arguments) != -1:
            raise AssertionError(f"{name} NULL guard failed")

    if not lib.mygramclient_get_last_error(None) or lib.mygramclient_get_last_error_code(None) == 0:
        raise AssertionError("last-error NULL guard failed")

    lib.mygramclient_disconnect(None)
    lib.mygramclient_destroy(None)
    for name in (
        "mygramclient_free_replication_status",
        "mygramclient_free_search_result",
        "mygramclient_free_search_result_with_highlights",
        "mygramclient_free_facet_result",
        "mygramclient_free_document",
        "mygramclient_free_server_info",
        "mygramclient_free_string",
        "mygramclient_free_parsed_expression",
    ):
        getattr(lib, name)(None)


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: ctypes_smoke_test.py <libmygramclient path>", file=sys.stderr)
        return 2

    library_path = Path(sys.argv[1]).resolve()
    lib = ctypes.CDLL(str(library_path))
    bind_public_api(lib)

    try:
        verify_null_guards(lib)
    except AssertionError as error:
        print(error, file=sys.stderr)
        return 1

    config = MygramClientConfig(b"127.0.0.1", 9, 100, 65536)
    client = lib.mygramclient_create(ctypes.byref(config))
    if not client:
        print("mygramclient_create returned NULL", file=sys.stderr)
        return 1
    try:
        if lib.mygramclient_connect(client) == 0:
            print("connect to closed port unexpectedly succeeded", file=sys.stderr)
            return 1
        if not lib.mygramclient_get_last_error(client) or lib.mygramclient_get_last_error_code(client) == 0:
            print("missing connection error details", file=sys.stderr)
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
    lib.mygramclient_destroy(client_v2)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
