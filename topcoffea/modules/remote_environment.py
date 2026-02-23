#! /usr/bin/env python
import copy
import json
import hashlib
import subprocess
import sys
import tempfile
import logging
import glob
import os
import re
from pathlib import Path

from typing import Dict, List, Optional


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


def get_environment(
    extra_conda: Optional[List[str]] = None,
    extra_pip: Optional[List[str]] = None,
    extra_pip_local: Optional[dict[str, str]] = None,
    force: bool = False,
    unstaged: str = "rebuild",
    cache_size: int = 3,
):
    # ensure cache directory exists
    Path(env_dir_cache).mkdir(parents=True, exist_ok=True)

    spec = copy.deepcopy(default_modules)
    spec_pip_local_to_watch = copy.deepcopy(pip_local_to_watch)
    if extra_conda:
        spec["conda"]["packages"].extend(extra_conda)
    if extra_pip:
        spec["pip"].extend(extra_pip)
    if extra_pip_local:
        spec["pip"].extend(extra_pip_local)
        spec_pip_local_to_watch.update(extra_pip_local)

    packages_hash = hashlib.sha256(json.dumps(spec).encode()).hexdigest()[0:8]

    pip_paths = _find_local_pip()
    pip_commits = _commits_local_pip(pip_paths)
    pip_check = _compute_commit(pip_paths, pip_commits)

    env_name = str(Path(env_dir_cache).joinpath("env_spec_{}_edit_{}".format(packages_hash, pip_check)).with_suffix(".tar.gz"))
    _clean_cache(cache_size, env_name)

    if pip_check == 'HEAD':
        changed = [p for p in pip_commits if pip_commits[p] == 'HEAD']
        if unstaged == 'fail':
            raise UnstagedChanges(changed)
        if unstaged == 'rebuild':
            force = True
            logger.warning("Rebuilding environment because unstaged changes in {}".format(', '.join([Path(p).name for p in changed])))

    return _create_env(env_name, spec, force)


class UnstagedChanges(Exception):
    pass


if __name__ == '__main__':
    print(get_environment())
