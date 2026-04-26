# csnp-helpers development workflow

`csnp-helpers` is a Python wheel that ships shared PySpark logic to all CSNP
Fabric notebooks. It lives in `helpers/` as a uv workspace member and is
installed into the `csnp_env` Fabric Environment via a committed `.whl` binary.

---

## When to bump the version

**Any change to `helpers/src/csnp_helpers/`** requires a version bump and wheel
rebuild before deploying. Fabric caches the installed wheel by filename; if the
filename does not change, the cluster will keep the old version even after
re-deploy.

Patch bumps (0.1.0 → 0.1.1) are sufficient for bug fixes and new functions.
Use a minor bump (0.1.x → 0.2.0) for breaking API changes.

---

## How to build

```bash
# Auto-bumps patch version, builds wheel, installs into CustomLibraries/
bash scripts/build_helpers.sh

# Re-build current version without bumping (use when testing locally only)
bash scripts/build_helpers.sh --no-bump
```

Then commit both changed files:

```bash
git add helpers/pyproject.toml \
        fabric/csnp_env.Environment/Libraries/CustomLibraries/
git commit -m "chore(helpers): bump to X.Y.Z"
```

---

## Why the .whl is committed to git

Fabric Custom Libraries must be uploaded to the Environment item as a binary.
There is no pip-install-from-VCS or editable-install mechanism in Fabric
Environments. The wheel is small (<20 KB) and changes infrequently, so
committing the binary is the standard pattern for Fabric Custom Libraries.

The `dist/` directory is gitignored — only the file under `CustomLibraries/`
is tracked.

---

## Deploy sequence after a helpers change

1. Run `bash scripts/build_helpers.sh` — bumps version, builds, copies .whl.
2. Commit the two changed files (see above).
3. Push and merge via PR.
4. Deploy to the target environment:
   ```bash
   uv run csnp-deploy --environment dev
   ```
5. **Wait ~5–10 minutes** for Fabric to rebuild the `csnp_env` Environment.
   The portal shows a spinning indicator on the Environment item while it builds.
6. Verify the new version is active: open any silver notebook in the portal,
   run the import cell, and check `csnp_helpers.__version__` (or just confirm
   the notebook runs without ImportError).

---

## Troubleshooting

### `ModuleNotFoundError: No module named 'csnp_helpers'`

The Environment has not finished building, or the notebook is not attached to
`csnp_env`. Check:
- Environment item in portal: status must be **Published** (not building).
- Notebook settings → Environment: must show `csnp_env`, not the default runtime.

### Old version of a function still running

The wheel filename encodes the version. If the filename did not change between
deploys, Fabric uses the cached copy. Always bump the version before deploying
a helpers change — `bash scripts/build_helpers.sh` does this automatically.

### `ValueError: Unknown strategy` or `NotImplementedError: SCD2`

SCD2 is reserved. The `merge_to_silver` stub in `helpers/src/csnp_helpers/merge.py`
raises `NotImplementedError` until the SCD2 logic is implemented. Implement it
there (not in the notebook) before authoring `nb_*_silver_dim_product` or
`nb_*_silver_dim_customer`.

### Version mismatch between environments

Each environment (dev/test/prod) gets the wheel from the same git commit, so
versions are always in sync after a successful deploy to all three environments.
If you deploy to dev only and skip test/prod, those environments remain on the
previous version until their next deploy.

---

## File map

```
helpers/
├── pyproject.toml               ← bump version here (or let build_helpers.sh do it)
├── src/csnp_helpers/
│   ├── __init__.py              ← public API surface
│   ├── onelake.py               ← onelake_files_path()  [pure Python, CI-tested]
│   ├── lineage.py               ← add_lineage_columns() [lazy PySpark import]
│   ├── merge.py                 ← merge_to_silver()     [lazy PySpark + Delta]
│   └── validation.py           ← validate_silver()      [lazy PySpark]
└── tests/
    ├── test_onelake.py          ← pure-Python unit tests, run in CI
    └── test_public_api.py       ← import smoke test, run in CI

fabric/csnp_env.Environment/Libraries/CustomLibraries/
    └── csnp_helpers-X.Y.Z-py3-none-any.whl   ← committed binary, deployed by fabric-cicd

scripts/
    └── build_helpers.sh         ← build tool (auto-bumps patch, builds, copies)
```
