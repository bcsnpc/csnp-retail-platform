#!/usr/bin/env bash
# Build the csnp-helpers wheel and install it into csnp_env CustomLibraries.
#
# Usage:
#   bash scripts/build_helpers.sh          # auto-bumps patch version
#   bash scripts/build_helpers.sh --no-bump  # build current version as-is (re-build only)
#
# Run this whenever helpers/src/csnp_helpers/ changes, then:
#   git add helpers/pyproject.toml fabric/csnp_env.Environment/Libraries/CustomLibraries/
#   git commit -m "chore(helpers): bump to X.Y.Z"
#   uv run csnp-deploy --environment dev   # triggers Fabric env rebuild (~5-10 min)

set -euo pipefail

PYPROJECT="helpers/pyproject.toml"
CUSTOM_LIBS="fabric/csnp_env.Environment/Libraries/CustomLibraries"
DIST_DIR="dist/csnp-helpers"
BUMP=true

if [[ "${1:-}" == "--no-bump" ]]; then
  BUMP=false
fi

# Use the venv Python for inline scripts (avoids Windows Store Python stub)
PY=".venv/Scripts/python.exe"
if [[ ! -x "$PY" ]]; then
  PY=".venv/bin/python"  # Linux/macOS
fi

# Read current version from pyproject.toml
CURRENT=$("$PY" -c "
import re, sys
m = re.search(r'^version = \"([\d.]+)\"', open('$PYPROJECT').read(), re.M)
if not m:
    sys.exit('ERROR: could not parse version from $PYPROJECT')
print(m.group(1))
")

if [[ "$BUMP" == "true" ]]; then
  # Auto-bump patch
  NEW_VERSION=$("$PY" -c "
parts = '$CURRENT'.split('.')
parts[2] = str(int(parts[2]) + 1)
print('.'.join(parts))
")
  echo "Bumping csnp-helpers: $CURRENT → $NEW_VERSION"
  "$PY" -c "
import re
path = '$PYPROJECT'
content = open(path).read()
content = re.sub(r'^version = \"[\d.]+\"', 'version = \"$NEW_VERSION\"', content, count=1, flags=re.M)
open(path, 'w').write(content)
"
else
  NEW_VERSION="$CURRENT"
  echo "Building csnp-helpers $NEW_VERSION (no version bump)"
fi

# Build wheel
mkdir -p "$DIST_DIR"
uv build --package csnp-helpers --out-dir "$DIST_DIR"

# Install into CustomLibraries — remove old versions first
mkdir -p "$CUSTOM_LIBS"
rm -f "${CUSTOM_LIBS}"/csnp_helpers-*.whl
WHEEL=$(ls "${DIST_DIR}"/csnp_helpers-"${NEW_VERSION}"-*.whl 2>/dev/null | head -1)
if [[ -z "$WHEEL" ]]; then
  echo "ERROR: wheel not found in $DIST_DIR" >&2
  exit 1
fi
cp "$WHEEL" "$CUSTOM_LIBS/"

echo ""
echo "Done: $(basename "$WHEEL") → $CUSTOM_LIBS/"
echo ""
echo "Next steps:"
echo "  git add $PYPROJECT $CUSTOM_LIBS/"
echo "  git commit -m 'chore(helpers): bump to $NEW_VERSION'"
echo "  uv run csnp-deploy --environment dev"
echo "  # Wait ~5-10 min for Fabric to rebuild csnp_env before running notebooks"
