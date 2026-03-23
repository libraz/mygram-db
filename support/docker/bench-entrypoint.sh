#!/bin/sh
# Bench-specific entrypoint wrapper for MygramDB.
# Detects the installed version and enables features accordingly,
# then delegates to the original entrypoint.

VERSION=$(/usr/local/bin/mygramdb --version 2>/dev/null | grep -oE '[0-9]+\.[0-9]+\.[0-9]+')
MAJOR=$(echo "$VERSION" | cut -d. -f1)
MINOR=$(echo "$VERSION" | cut -d. -f2)

echo "MygramDB version: ${VERSION:-unknown}"

# v1.5+: enable verify_text post-filter to eliminate n-gram false positives
if [ "${MAJOR:-0}" -gt 1 ] || { [ "${MAJOR:-0}" -eq 1 ] && [ "${MINOR:-0}" -ge 5 ]; }; then
    export MEMORY_VERIFY_TEXT="${MEMORY_VERIFY_TEXT:-on}"
    echo "  verify_text: enabled (v1.5+)"
fi

exec /usr/local/bin/entrypoint.sh "$@"
