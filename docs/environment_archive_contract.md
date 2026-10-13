# Remote environment archive contract

`topcoffea.modules.remote_environment` is the source authority for resolving,
fingerprinting, building, caching, and validating analysis archives used by
remote executors. Typed source signatures and returned dictionaries remain
authoritative.

## Public workflow

The maintained caller normally uses `get_environment(...)`:

```python
from topcoffea.modules.remote_environment import get_environment

archive_path = get_environment(unstaged="rebuild")
```

It resolves the current request, derives a fingerprint-named path, validates
any cache hit and adjacent manifest, builds only when necessary, and writes a
fresh manifest atomically. Successful builds preserve older worker-environment
archives. Environment creation uses
`poncho_package_create`; callers should use the higher-level topeft CLI instead
of private builders.

| Fully qualified name | Signature and result | Contract |
| --- | --- | --- |
| `topcoffea.modules.remote_environment.resolve_environment_request` | `(extra_conda=None, extra_pip=None, extra_pip_local=None, unstaged="rebuild") -> dict` | Resolves/sanitizes the conda/pip request and editable-package state. `unstaged` is `rebuild` or `fail`. |
| `topcoffea.modules.remote_environment.environment_archive_path` | `(environment_fingerprint: str) -> str` | Returns `topeft-envs/env_spec_<first-16-hex>.tar.gz` under the cache root. |
| `topcoffea.modules.remote_environment.write_archive_manifest` | `(archive_path: str, environment_request: dict) -> str` | Hashes an existing archive and atomically writes its adjacent `.manifest.json`. |
| `topcoffea.modules.remote_environment.validate_environment_archive` | `(archive_path: str, current_environment_request=None, snapshot=False) -> dict` | Validates regular-file/tar integrity, manifest schema/fields, archive hash, and optionally current provenance. |
| `topcoffea.modules.remote_environment.get_environment` | `(extra_conda=None, extra_pip=None, extra_pip_local=None, force=False, unstaged="rebuild", cache_size=3) -> str` | Returns a current validated archive path, rebuilding only the resolved cache key. |

`UnstagedChanges` is raised when `unstaged="fail"` and a watched editable
package has tracked changes.

## Fingerprint inputs

The environment fingerprint is a SHA-256 digest of the running Python version,
the sanitized resolved conda/pip specification, and each watched editable
package's name, Git commit, watched-source fingerprint, clean/dirty state, and
relevant untracked-file evidence.

`pip_local_to_watch` currently watches `topcoffea` paths `topcoffea` and
`setup.py`. Tracked contents and status under those paths contribute to the
source fingerprint. Non-ignored untracked regular files contribute sorted
path/SHA-256 pairs and a separate count/fingerprint. Symlink or non-regular
untracked watched entries fail closed.

Changing the watch set is a compatibility decision: update
`pip_local_to_watch`, validate clean/dirty/untracked behavior, and update
`tests/test_remote_environment.py`. Do not add a second cache registry in a
consumer.

## Manifest schema

`MANIFEST_SCHEMA_VERSION` is currently `1`. The adjacent manifest contains
`schema_version`, archive basename/SHA-256, creation time, environment and
resolved-spec fingerprints, Python version, resolved spec, editable-package
states, and builder identity. It is written to a same-directory temporary file,
flushed/fsynced, and published with `os.replace`.

Archive existence without a readable matching manifest is not a current
validated cache hit.

## Strict and snapshot validation

Strict validation requires a readable non-empty tar.gz, a supported complete
manifest, matching basename/hash, and—when a current request is supplied—the
same environment fingerprint. A valid result has `status="valid"` and
`usable=True`.

`snapshot=True` relaxes only current-provenance compatibility for an explicitly
selected historical archive. It never relaxes tar integrity, archive digest,
basename, or unsupported-schema checks. A missing/incomplete manifest may be
reported as `unverifiable` but explicitly usable with warnings in snapshot
mode; an invalid archive remains unusable.

Important status values are `valid`, `stale`, `unverifiable`, and
`invalid_archive`. Callers must inspect `usable`, `mismatches`, `warnings`, and
`provenance_status`.

## Cache and rebuild behavior

Archive names use the first 16 hex characters of the full fingerprint. A cache
entry is reused only after strict validation. `force=True` rebuilds the resolved
key. Dirty watched sources with `unstaged="rebuild"` change the fingerprint and
select a distinct archive rather than overwriting a clean entry. The normal
successful build path does not evict historical archive tarballs; the existing
`cache_size` argument does not cause automatic trimming. Existing-target
rebuild and collision behavior for the resolved key remains separate from
retention of other archives.

Do not modify manifests or archives in place. After changing package inputs or
watched source, resolve a new request and let `get_environment` choose the new
key. Package installation is an operational action outside this reference.
