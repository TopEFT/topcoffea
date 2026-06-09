# Vendored MUO ScaReKit Backend

Upstream repository:

```text
ssh://git@gitlab.cern.ch:7999/cms-muonPOG/muonscarekit.git
```

Vendored source:

```text
scripts/MuonScaRe.py
```

Source commit:

```text
8d0de18b88c7ba06abab31de97ef7f2b92906aa5
Merge branch 'random_fix' into 'master'
```

Behavior-bearing random-smearing commit:

```text
d393332
Fixing step2 polynomial fits binning, small fixes in ScaRe closure plots and random seed hashing through correctionlib
```

Local patches:

- `pt_resol(...)` no longer exposes or forwards the stale `rnd_gen`
  argument.  Upstream commit `d393332` moved random smearing to the
  correctionlib `RandomSmearing` payload and removed `rnd_gen` from
  `get_rndm(...)`, so the old selector has no effect in this backend.
- `get_rndm(..., nested=False)` now defines and validates flat
  `evtNr_f` and `lumiNr_f` arrays before evaluating `RandomSmearing`.
  Flat mode treats inputs as one-dimensional per-muon arrays; scalar
  event/lumi values are broadcast, while one-dimensional event/lumi
  arrays must match the flat muon length.

Files intentionally not vendored:

- `scripts/apply_corrections_coffea.py`
- C++ backend files
- derivation workflow code under `code/`
- the full `muonscarekit` repository

The physics formula code is otherwise preserved from the upstream Python
backend and is not rederived in `topcoffea`.
