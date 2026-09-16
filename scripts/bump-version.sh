#!/usr/bin/env bash
set -euo pipefail

# Navigate to the repository root directory
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [ -z "${1:-}" ]; then
    echo "Error: Version argument required."
    echo "Usage: $0 <new-version> (e.g. 1.2.2 or v1.2.2)"
    exit 1
fi

# Normalize version string (strip leading 'v' if provided)
TARGET_VERSION="${1#v}"

# Validate semantic version syntax (MAJOR.MINOR.PATCH with optional prerelease tag)
if [[ ! "$TARGET_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[a-zA-Z0-9.]+)?$ ]]; then
    echo "Error: '$TARGET_VERSION' is not a valid semantic version."
    echo "Expected format: X.Y.Z or X.Y.Z-prerelease (e.g. 1.2.2, 1.3.0-rc.1)"
    exit 1
fi

echo "Bumping Roomer monorepo packages to v${TARGET_VERSION}..."

# Use python3 to perform robust in-place replacements without platform-dependent sed differences
python3 -c "
import re
import sys
from pathlib import Path

version = sys.argv[1]

def replace_first(path_str: str, pattern: str, replacement: str):
    p = Path(path_str)
    if not p.exists():
        print(f'Warning: {path_str} not found, skipping.')
        return
    text = p.read_text(encoding='utf-8')
    new_text, count = re.subn(pattern, replacement, text, count=1)
    if count == 0:
        print(f'Warning: Pattern not matched in {path_str}')
    p.write_text(new_text, encoding='utf-8')

# 1. Rust Cargo.toml package version
replace_first(
    'server/rust/Cargo.toml',
    r'(?m)^version\s*=\s*\"[^\"]+\"',
    f'version = \"{version}\"'
)

# 2. Rust documentation root URL in lib.rs
replace_first(
    'server/rust/src/lib.rs',
    r'https://docs\.rs/roomer/[^\"]+',
    f'https://docs.rs/roomer/{version}'
)

# 3. Python SDK pyproject.toml version
replace_first(
    'client/python/pyproject.toml',
    r'(?m)^version\s*=\s*\"[^\"]+\"',
    f'version = \"{version}\"'
)

# 4. Node.js server package.json version
replace_first(
    'server/node/package.json',
    r'\"version\":\s*\"[^\"]+\"',
    f'\"version\": \"{version}\"'
)

# 5. Node.js package-lock.json (update first two occurrences: root & package entry)
lock_path = Path('server/node/package-lock.json')
if lock_path.exists():
    lock_text = lock_path.read_text(encoding='utf-8')
    new_lock_text, _ = re.subn(
        r'\"version\":\s*\"[^\"]+\"',
        f'\"version\": \"{version}\"',
        lock_text,
        count=2
    )
    lock_path.write_text(new_lock_text, encoding='utf-8')
" "$TARGET_VERSION"

echo "Version successfully updated to ${TARGET_VERSION} in:"
echo "  ✓ server/rust/Cargo.toml"
echo "  ✓ server/rust/src/lib.rs"
echo "  ✓ client/python/pyproject.toml"
echo "  ✓ server/node/package.json"
echo "  ✓ server/node/package-lock.json"
echo ""
echo "Current git diff for version files:"
git diff --stat server/rust/Cargo.toml server/rust/src/lib.rs client/python/pyproject.toml server/node/package.json server/node/package-lock.json
