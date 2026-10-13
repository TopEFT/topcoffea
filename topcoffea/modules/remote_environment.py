#! /usr/bin/env python
"""Build and validate fingerprinted remote-executor environment archives.

Public callers should use :func:`resolve_environment_request`,
:func:`validate_environment_archive`, or :func:`get_environment`. Private
helpers own local Git inspection, packaging, and cache maintenance.
"""
import copy
import datetime
import json
import hashlib
import subprocess
import sys
import tempfile
import logging
import glob
import os
import re
import tarfile
from pathlib import Path

from typing import Any, Dict, List, Optional


logger = logging.getLogger()
logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(levelname)s:%(message)s')

env_dir_cache = Path.cwd().joinpath(Path('topeft-envs'))

_CORE_BOOTSTRAP_PACKAGES = {"pip", "conda", "python"}
_SAFE_CORE_DEFAULTS = {
    "pip": "pip>=24,<25",
    "conda": "conda>=24,<25",
    "python": f"python={sys.version_info[0]}.{sys.version_info[1]}",
}

py_version = "{}.{}.{}".format(
    sys.version_info[0], sys.version_info[1], sys.version_info[2]
)  # 3.8 or 3.9, or etc.

default_modules = {
    "conda": {
        "channels": ["conda-forge"],
        "packages": [
            f"python={py_version}",
            "pip",
            "conda<2025.1.0",
            "conda-pack",
            "ndcctools>=7.14.7",
            "xrootd",
            "setuptools==70.3.0",
            "pyyaml"
        ],
    },
    "pip": ["topcoffea", "coffea==0.7.26"],
}

pip_local_to_watch = {"topcoffea": ["topcoffea", "setup.py"]}


def _current_versions_conda(conda_env_path=None):
    if not conda_env_path:
        conda_env_path = os.environ['CONDA_PREFIX']

    proc = subprocess.run(
        ["conda", "list", "--export", "--json"],
        check=True,
        stdout=subprocess.PIPE,
        stdin=subprocess.DEVNULL,
    )
    raw_pkgs = json.loads(proc.stdout.decode())

    pkgs = {}
    for pkg in raw_pkgs:
        name = pkg['name']
        version = f"{pkg['version']}={pkg['build_string']}"
        pkgs[name] = f"{name}={version}"

    return pkgs


def _check_current_env(spec: Dict):
    with tempfile.NamedTemporaryFile() as f:
        # export current conda enviornment
        subprocess.check_call(['conda', 'env', 'export', '--json'], stdout=f, stdin=subprocess.DEVNULL)
        spec_file = open(f.name, "r")
        current_spec = json.load(spec_file)
        current_spec['pinning'] = {'conda': _current_versions_conda()}

        if 'dependencies' in current_spec:
            # get current conda packages
            conda_deps = {
                re.sub("[!~=<>].*$", "", x): x
                for x in current_spec["dependencies"]
                if not isinstance(x, dict)
            }
            # get current pip packages
            pip_deps = {
                re.sub("[!~=<>].*$", "", y): y
                for y in [
                    x
                    for x in current_spec["dependencies"]
                    if isinstance(x, dict) and "pip" in x
                    for x in x["pip"]
                ]
            }

            # replace any conda packages
            for i in range(len(spec['conda']['packages'])):
                # ignore packages where a version is already specified
                package = spec['conda']['packages'][i]
                pkg_name = _package_basename(package)
                if pkg_name in _CORE_BOOTSTRAP_PACKAGES:
                    continue
                if not re.search("[!~=<>].*$", package):
                    if package in conda_deps:
                        spec['conda']['packages'][i] = conda_deps[package]

            # replace any pip packages
            for i in range(len(spec['pip'])):
                # ignore packages where a version is already specified
                package = spec['pip'][i]
                pkg_name = _package_basename(package)
                if pkg_name in _CORE_BOOTSTRAP_PACKAGES:
                    continue
                if not re.search("[!~=<>].*$", package):
                    if package in pip_deps:
                        spec['pip'][i] = pip_deps[package]
    return spec


def _sanitize_spec(spec: Dict) -> Dict:
    """
    Relax pins for core bootstrap packages that may not exist on conda-forge and drop build strings.

    This helper is intentionally conservative: it keeps the original package set intact
    while normalizing package strings to avoid inheriting host-specific constraints.

    >>> _sanitize_spec({"conda": {"channels": ["conda-forge"], "packages": ["pip=25.1=py310"]}, "pip": []})
    {'conda': {'channels': ['conda-forge'], 'packages': ['pip>=24,<25']}, 'pip': []}
    """

    def _sanitize_conda_package(package: str) -> str:
        package = _strip_build_string(package)
        base = _package_basename(package)
        if base in _CORE_BOOTSTRAP_PACKAGES:
            return _sanitize_core_package(package)
        return package

    def _sanitize_pip_package(package: str) -> str:
        package = _strip_build_string(package)
        base = _package_basename(package)
        if base in _CORE_BOOTSTRAP_PACKAGES:
            return _sanitize_core_package(package)
        return package

    sanitized = copy.deepcopy(spec)
    sanitized["conda"]["packages"] = [_sanitize_conda_package(p) for p in sanitized["conda"]["packages"]]
    sanitized["pip"] = [_sanitize_pip_package(p) for p in sanitized.get("pip", [])]
    return sanitized


def _strip_build_string(package: str) -> str:
    """Drop build-string segments (the third '=' token) from conda package specs."""

    return re.sub(r"^([^=]+=[^=,]+)=.*$", r"\1", package)


def _package_basename(package: str) -> str:
    """Return the base package name without version or comparison operators."""

    # split on the first comparison/operator token
    return re.split(r"[=<>!~]", package, maxsplit=1)[0]


def _sanitize_core_package(package: str) -> str:
    package = _strip_build_string(package)
    base = _package_basename(package)
    version = _extract_equality_version(package, base)

    if base == "pip":
        if version and _version_at_least(version, (25,)):
            return _SAFE_CORE_DEFAULTS["pip"]
    elif base == "conda":
        if version and _version_at_least(version, (25,)):
            return _SAFE_CORE_DEFAULTS["conda"]
    elif base == "python":
        if version:
            python_mm = _major_minor(version)
            if python_mm:
                return f"python={python_mm[0]}.{python_mm[1]}"
    return package


def _extract_equality_version(package: str, base: str) -> Optional[str]:
    match = re.match(rf"^{re.escape(base)}={{1,2}}([^<>=!~]+)$", package)
    if match:
        return match.group(1)
    return None


def _major_minor(version: str) -> Optional[tuple[int, int]]:
    pieces = _version_tuple(version)
    if len(pieces) >= 2:
        return pieces[0], pieces[1]
    return None


def _version_at_least(version: str, minimum: tuple[int, ...]) -> bool:
    parsed = _version_tuple(version)
    if not parsed:
        return False
    padded = parsed + (0,) * (len(minimum) - len(parsed))
    target = minimum + (0,) * (len(padded) - len(minimum))
    return padded >= target


def _version_tuple(version: str) -> tuple[int, ...]:
    parts: List[int] = []
    for token in re.split(r"[._-]", version):
        if token.isdigit():
            parts.append(int(token))
        else:
            break
    return tuple(parts)


MANIFEST_SCHEMA_VERSION = 1


def _create_env(env_name: str, spec: Dict, force: bool = False):
    if force:
        logger.info("Forcing rebuilding of {}".format(env_name))
        Path(env_name).unlink(missing_ok=True)
    elif Path(env_name).exists():
        logger.info("Found in cache {}".format(env_name))
        return env_name

    with tempfile.NamedTemporaryFile() as f:
        logger.info("Checking current conda environment")
        spec = _check_current_env(spec)
        spec = _sanitize_spec(spec)
        packages_json = json.dumps(spec)
        logger.info("base env specification:{}".format(packages_json))
        f.write(packages_json.encode())
        f.flush()
        logger.info("Creating environment {}".format(env_name))

        try:
            subprocess.check_output(['poncho_package_create', f.name, env_name], stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            logger.error(f"poncho package creation failed with code {e.returncode}")
            logger.error(f"{e.output.decode()}")
            raise e
    return env_name

def _find_local_pip():
    edit_raw = subprocess.check_output(
        [sys.executable, "-m" "pip", "list", "--editable"], stdin=subprocess.DEVNULL
    ).decode()

    # drop first two lines, which are just a header
    edit_raw = edit_raw.split('\n')[2:]
    path_of = {}
    for line in edit_raw:
        if not line:
            # skip empty lines
            continue
        # we are only interested in the path information of the package, which
        # is in the last column
        (pkg, version, location) = line.split()
        path_of[pkg] = location
    return path_of


def _commits_local_pip(paths):
    commits = {}
    for (pkg, path) in paths.items():
        try:
            to_watch = []
            paths = pip_local_to_watch.get(pkg, None)
            if paths:
                to_watch = [":(top){}".format(d) for d in paths]

            try:
                commit = (
                    subprocess.check_output(
                        ["git", "rev-parse", "HEAD"], cwd=path, stdin=subprocess.DEVNULL
                    )
                    .decode()
                    .rstrip()
                )
            except FileNotFoundError:
                raise FileNotFoundError("Could not find the git executable in PATH")

            changed = True
            cmd = ['git', 'status', '--porcelain', '--untracked-files=no']
            try:
                changed = subprocess.check_output(cmd + to_watch, cwd=path, stdin=subprocess.DEVNULL).decode().rstrip()
            except subprocess.CalledProcessError:
                logger.warning("Could not apply git paths-to-watch filters. Trying without them...")
                changed = subprocess.check_output(cmd, cwd=path, stdin=subprocess.DEVNULL).decode().rstrip()

            if changed:
                logger.warning(
                    "Found unstaged changes in {}:\n{}".format(path, changed)
                )
                commits[pkg] = 'HEAD'
            else:
                commits[pkg] = commit
        except Exception as e:
            # on error, e.g., not a git repository, assume that current state
            # should be installed
            logger.warning(f"Could not get current commit of '{path}': {e}")
            commits[pkg] = "HEAD"
    return commits


def _compute_commit(paths, commits):
    if not commits:
        return "fixed"
    # list commits according to paths ordering
    values = [commits[p] for p in paths]
    if 'HEAD' in values:
        # if commit is HEAD, then return that, as we always rebuild the
        # environment in that case.
        return 'HEAD'
    return hashlib.sha256(''.join(values).encode()).hexdigest()[0:8]


def _clean_cache(cache_size, *current_files):
    envs = sorted(glob.glob(os.path.join(env_dir_cache, 'env_*.tar.gz')), key=lambda f: -os.stat(f).st_mtime)
    for f in envs[cache_size:]:
        if f not in current_files:
            logger.info("Trimming cached environment file {}".format(f))
            os.remove(f)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _run_git_text(path: str, arguments: List[str]) -> str:
    return subprocess.check_output(["git", *arguments], cwd=path, stdin=subprocess.DEVNULL).decode().strip()


def _relevant_untracked_files(path: str, watched_paths: List[str]) -> List[Dict[str, str]]:
    """Return non-ignored untracked regular files below declared watch paths."""
    try:
        raw_paths = subprocess.check_output(
            ["git", "ls-files", "--others", "--exclude-standard", "-z", "--", *watched_paths],
            cwd=path,
            stdin=subprocess.DEVNULL,
        )
    except (OSError, subprocess.CalledProcessError) as exc:
        raise RuntimeError(f"Could not enumerate untracked source files for {path}: {exc}") from exc

    files = []
    for relative_path in sorted(item.decode() for item in raw_paths.split(b"\0") if item):
        source = Path(path, relative_path)
        if source.is_symlink() or not source.is_file():
            raise RuntimeError(
                "Unsupported non-regular untracked watched source entry: {}".format(relative_path)
            )
        files.append({"path": relative_path, "sha256": _sha256_file(source)})
    return files


def _watched_source_fingerprint(path: str, watched_paths: List[str]) -> tuple[str, bool, List[Dict[str, str]]]:
    pathspecs = [":(top){}".format(item) for item in watched_paths]
    try:
        commit = _run_git_text(path, ["rev-parse", "HEAD"])
        status = _run_git_text(path, ["status", "--porcelain", "--untracked-files=no", "--", *pathspecs])
        names = _run_git_text(path, ["ls-files", "-z", "--", *pathspecs]).split("\0")
    except (OSError, subprocess.CalledProcessError) as exc:
        raise RuntimeError(f"Could not resolve editable package state for {path}: {exc}") from exc

    digest = hashlib.sha256()
    digest.update(commit.encode())
    digest.update(status.encode())
    for name in sorted(name for name in names if name):
        source = Path(path, name)
        if source.is_file():
            digest.update(name.encode())
            digest.update(_sha256_file(source).encode())
    untracked_files = _relevant_untracked_files(path, watched_paths)
    untracked_payload = json.dumps(untracked_files, sort_keys=True, separators=(",", ":"))
    digest.update(untracked_payload.encode())
    return digest.hexdigest(), bool(status), untracked_files


def _editable_package_states(watch_config: Dict[str, List[str]]) -> List[Dict[str, Any]]:
    installed = _find_local_pip()
    states = []
    for package_name, watched_paths in sorted(watch_config.items()):
        location = installed.get(package_name)
        if not location:
            continue
        commit = _run_git_text(location, ["rev-parse", "HEAD"])
        source_fingerprint, dirty, untracked_files = _watched_source_fingerprint(location, watched_paths)
        untracked_fingerprint = hashlib.sha256(
            json.dumps(untracked_files, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        states.append(
            {
                "package_name": package_name,
                "git_commit": commit,
                "watched_source_fingerprint": source_fingerprint,
                "clean_or_dirty": "dirty" if dirty else "clean",
                "untracked_relevant_count": len(untracked_files),
                "untracked_relevant_fingerprint": untracked_fingerprint,
            }
        )
    return states


def resolve_environment_request(
    extra_conda: Optional[List[str]] = None,
    extra_pip: Optional[List[str]] = None,
    extra_pip_local: Optional[Dict[str, List[str]]] = None,
    unstaged: str = "rebuild",
) -> Dict[str, Any]:
    """Resolve the current packaging request before choosing a cache path."""
    if unstaged not in {"rebuild", "fail"}:
        raise ValueError("unstaged must be 'rebuild' or 'fail'")

    spec = copy.deepcopy(default_modules)
    watch_config = copy.deepcopy(pip_local_to_watch)
    if extra_conda:
        spec["conda"]["packages"].extend(extra_conda)
    if extra_pip:
        spec["pip"].extend(extra_pip)
    if extra_pip_local:
        spec["pip"].extend(extra_pip_local)
        watch_config.update(extra_pip_local)

    resolved_spec = _sanitize_spec(_check_current_env(spec))
    editable_packages = _editable_package_states(watch_config)
    dirty_packages = [item["package_name"] for item in editable_packages if item["clean_or_dirty"] == "dirty"]
    if dirty_packages and unstaged == "fail":
        raise UnstagedChanges(dirty_packages)

    fingerprint_payload = {
        "python_version": py_version,
        "resolved_environment_spec": resolved_spec,
        "editable_packages": editable_packages,
    }
    resolved_spec_fingerprint = hashlib.sha256(
        json.dumps(resolved_spec, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    fingerprint = hashlib.sha256(
        json.dumps(fingerprint_payload, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    return {
        **fingerprint_payload,
        "resolved_environment_spec_fingerprint": resolved_spec_fingerprint,
        "environment_fingerprint": fingerprint,
        "dirty_packages": dirty_packages,
    }


def environment_archive_path(environment_fingerprint: str) -> str:
    """Return the cache path derived from a full environment fingerprint."""
    return str(env_dir_cache / f"env_spec_{environment_fingerprint[:16]}.tar.gz")


def _manifest_path(archive_path: str) -> Path:
    return Path(f"{archive_path}.manifest.json")


def write_archive_manifest(archive_path: str, environment_request: Dict[str, Any]) -> str:
    """Atomically write the schema-v1 integrity/provenance manifest."""
    archive = Path(archive_path)
    manifest = {
        "schema_version": MANIFEST_SCHEMA_VERSION,
        "archive_basename": archive.name,
        "archive_sha256": _sha256_file(archive),
        "created_at_utc": datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        "environment_fingerprint": environment_request["environment_fingerprint"],
        "python_version": environment_request["python_version"],
        "resolved_environment_spec": environment_request["resolved_environment_spec"],
        "resolved_environment_spec_fingerprint": environment_request["resolved_environment_spec_fingerprint"],
        "editable_packages": environment_request["editable_packages"],
        "builder": {"module": "topcoffea.modules.remote_environment", "manifest_schema_version": MANIFEST_SCHEMA_VERSION},
    }
    target = _manifest_path(archive_path)
    temporary = target.with_name(f".{target.name}.tmp.{os.getpid()}")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(manifest, handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
    finally:
        if temporary.exists():
            temporary.unlink()
    return str(target)


def validate_environment_archive(
    archive_path: str,
    current_environment_request: Optional[Dict[str, Any]] = None,
    snapshot: bool = False,
) -> Dict[str, Any]:
    """Validate archive integrity and, unless snapshot is selected, provenance."""
    archive = Path(archive_path).expanduser().resolve()
    result: Dict[str, Any] = {
        "status": "invalid_archive",
        "archive_path": str(archive),
        "archive_sha256": None,
        "manifest_path": str(_manifest_path(str(archive))),
        "environment_fingerprint": None,
        "current_environment_fingerprint": (current_environment_request or {}).get("environment_fingerprint"),
        "mismatches": [],
        "warnings": [],
        "usable": False,
        "provenance_status": "unknown",
        "editable_packages": [],
    }
    if not archive.is_file() or not os.access(archive, os.R_OK) or archive.stat().st_size == 0:
        result["mismatches"].append("archive must be a readable non-empty regular file")
        return result
    try:
        with tarfile.open(archive, "r:gz") as tar_handle:
            tar_handle.getmembers()
    except (OSError, tarfile.TarError) as exc:
        result["mismatches"].append(f"archive is not a readable tar.gz: {exc}")
        return result

    result["archive_sha256"] = _sha256_file(archive)
    manifest_path = _manifest_path(str(archive))
    if not manifest_path.is_file():
        result["status"] = "unverifiable"
        result["provenance_status"] = "incomplete"
        result["mismatches"].append("archive manifest is missing")
        if snapshot:
            result["usable"] = True
            result["warnings"].append("snapshot archive has no manifest; provenance is incomplete")
        return result
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        result["mismatches"].append(f"archive manifest is unreadable: {exc}")
        return result
    if manifest.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        result["status"] = "unverifiable"
        result["provenance_status"] = "unsupported_schema"
        result["mismatches"].append("archive manifest schema is unsupported")
        return result
    required_fields = {
        "archive_basename",
        "archive_sha256",
        "created_at_utc",
        "environment_fingerprint",
        "python_version",
        "resolved_environment_spec",
        "resolved_environment_spec_fingerprint",
        "editable_packages",
    }
    missing_fields = sorted(field for field in required_fields if field not in manifest)
    if missing_fields:
        result["status"] = "unverifiable"
        result["provenance_status"] = "incomplete"
        result["mismatches"].append("archive manifest is missing required fields: {}".format(", ".join(missing_fields)))
        if snapshot:
            result["usable"] = True
            result["warnings"].append("snapshot archive manifest has incomplete provenance")
        return result
    result["environment_fingerprint"] = manifest.get("environment_fingerprint")
    result["editable_packages"] = manifest.get("editable_packages", [])
    if manifest.get("archive_basename") != archive.name:
        result["mismatches"].append("archive basename does not match manifest")
        return result
    if manifest.get("archive_sha256") != result["archive_sha256"]:
        result["mismatches"].append("archive SHA256 does not match manifest")
        return result
    result["provenance_status"] = "complete"
    if current_environment_request and result["environment_fingerprint"] != result["current_environment_fingerprint"]:
        result["status"] = "stale"
        result["mismatches"].append("archive environment fingerprint differs from the current resolved environment")
        if snapshot:
            result["usable"] = True
            result["warnings"].append("snapshot compatibility mismatch accepted explicitly")
        return result
    result["status"] = "valid"
    result["usable"] = True
    return result


def get_environment(
    extra_conda: Optional[List[str]] = None,
    extra_pip: Optional[List[str]] = None,
    extra_pip_local: Optional[Dict[str, List[str]]] = None,
    force: bool = False,
    unstaged: str = "rebuild",
    cache_size: int = 3,
):
    """Return a current, manifest-validated archive, rebuilding only its cache key."""
    Path(env_dir_cache).mkdir(parents=True, exist_ok=True)
    request = resolve_environment_request(extra_conda, extra_pip, extra_pip_local, unstaged)
    env_name = environment_archive_path(request["environment_fingerprint"])
    current = validate_environment_archive(env_name, request)
    if current["status"] == "valid" and not force:
        logger.info("Found validated environment cache %s", env_name)
        return env_name

    created = _create_env(env_name, request["resolved_environment_spec"], force=force or Path(env_name).exists())
    write_archive_manifest(created, request)
    return created


class UnstagedChanges(Exception):
    pass


if __name__ == '__main__':
    print(get_environment())
