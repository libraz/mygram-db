/**
 * @file surface_descriptor.h
 * @brief Deterministic text rendering of the server's static external surface
 */

#pragma once

#include <string>

namespace mygramdb::app {

/**
 * @brief Render the static external surface as deterministic text.
 *
 * The rendering covers the protocol commands, request bounds, HTTP routes,
 * error codes, configuration keys, persistence format versions and server
 * command-line flags. Every collection is sorted by an explicit key and no
 * value depends on the build version, the host, the clock or the locale, so
 * two calls in any two processes of the same build produce identical bytes.
 *
 * The first line carries the renderer's own format version, so a change to
 * the layout is distinguishable from a change to the surface it describes.
 *
 * @return The snapshot text, newline-terminated.
 */
[[nodiscard]] std::string RenderSurfaceSnapshot();

}  // namespace mygramdb::app
