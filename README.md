# topcoffea

Tools that sit on top of coffea to facilitate CMS analyses. The repository is set up as a pip installable package. To install this package into a conda environment:
```
git clone https://github.com/TopEFT/topcoffea.git
cd topcoffea
pip install -e .
```

Start with the [documentation index](docs/README.md) for current shared interfaces.

When pairing this repository with [`topeft`](https://github.com/TopEFT/topeft),
use the exact `topcoffea` ref selected by the consuming workflow (for example
`TOPCOFFEA_GIT_REF` in topeft's installer) and record both commits. Branch names
alone are not a compatibility guarantee.

Developer references for the current feature contracts:

* [Remote environment archive contract](docs/environment_archive_contract.md)
* [Correction interfaces reference](docs/correction_interfaces.md)
* [EFT and histogram interfaces](docs/eft_interfaces.md)
* [Physics extension guides](docs/physics_extension_guides.md)
* [topeft integration](docs/topeft_integration.md)

## Remote environment compatibility

`topcoffea.modules.remote_environment` assembles a reproducible environment specification before calling `poncho_package_create`. The spec is sanitized to stay compatible across hosts: build strings are stripped, and any strict pins for bootstrapping tools (`pip`, `conda`, `python`) that are unavailable on conda-forge are relaxed to safe defaults (for example `pip>=24,<25`). Downstream callers can rely on the sanitized spec to avoid inheriting site-specific package revisions while keeping the original package list intact.

### Side-by-side development checklist

When working on `topcoffea` and `topeft` together from sibling checkouts:

1. Activate the analysis environment (e.g., `conda activate <env>`).
2. Run `pip install -e ../topcoffea` from the `topeft` checkout to ensure the editable install is picked up.
3. Verify the import path with a quick smoke test: `python -c "import topcoffea"`.



Examples of analysis repositories making use of `topcoffea`:
* [`topeft`](https://github.com/TopEFT/topeft): EFT analyses in the top sector.
* `ewkcoffea`: Multi boson analyses.

## Accessing package resources

Data files, JSON payloads, and other resources that live inside the
`topcoffea` python package should always be opened via
`topcoffea.modules.paths.topcoffea_path`.  This helper resolves files relative
to the installed package directory so the correct path is returned even when
the repository is nested inside another checkout or installed as a dependency
of a larger project.

```python
import json

from topcoffea.modules.paths import topcoffea_path

with open(topcoffea_path("params/params.json")) as handle:
    cfg = json.load(handle)
```

Downstream projects should treat the returned path as read-only package data
and avoid constructing relative paths by hand (for example `../data/...`) so
they always inherit the correct behaviour. When integrating with
[`topeft`](https://github.com/TopEFT/topeft), the editable install performed by
[`scripts/install_topcoffea.sh`](https://github.com/TopEFT/topeft/blob/run3_test_mmerged/scripts/install_topcoffea.sh)
should be allowed to supply the packaged resources—avoid keeping in-tree
overlays that could mask the installed `topcoffea` package.

## Streaming histogram pickle files

Deferred workflows (such as the nonprompt scripts) often read very large
`*.pkl.gz` files.  Loading the entire histogram dictionary at once can exhaust
memory, so `topcoffea` exposes helpers that read one entry at a time directly
from the gzip stream:

```python
from topcoffea.modules.utils import iterate_hist_from_pkl

for key, hist in iterate_hist_from_pkl("/path/to/output.pkl.gz", allow_empty=False):
    process_histogram(key, hist)
```

Set `materialize=True` (or `materialize="eager"`) to recover the previous eager
behavior provided by `get_hist_from_pkl` when you explicitly need the full
mapping in memory.

When downstream projects want to decouple file IO from histogram materialization
they can opt into the new lazy iterator, which yields `(key, LazyHist)` pairs
that only deserialize their payload on demand:

```python
from topcoffea.modules.utils import iterate_histograms_from_pkl

for key, lazy_hist in iterate_histograms_from_pkl("/path/to/output.pkl.gz"):
    hist = lazy_hist.materialize()
    try:
        process_histogram(key, hist)
    finally:
        lazy_hist.release()  # drop the materialized histogram when done
```

Direct iteration avoids retaining every lazy entry and its serialized payload
in a list. Each `LazyHist` retains its serialized payload while referenced;
`release()` clears only the cached materialized histogram. The fallback loader
may still materialize the full histogram mapping, so this example does not
guarantee constant total process memory.

## Compatibility helpers

Projects running on older Python or ``numpy`` releases can import the
compatibility layer to smooth over missing annotations or optional streaming
support:

```python
from topcoffea.modules import compat

# Ensure ``numpy.typing.Self`` is available for histEFT annotations on Python 3.9
compat.ensure_histEFT_py39_compat()

# Load histogram utilities (including the streaming fallback) via a stable entrypoint
hist_utils = compat.ensure_hist_utils()
```
