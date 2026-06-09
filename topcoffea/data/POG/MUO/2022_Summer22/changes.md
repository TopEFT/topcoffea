## Changes: 2026-04-28 (Muo 2022 sns)

Merge Request: [!1](https://gitlab.cern.ch/cms-analysis-corrections/MUO/Run3-22CDSep23-Summer22-NanoAODv12/-/merge_requests/1)

We update the scale and smearing corrections: they were produced again to fix a bug in the previous version preventing the random (deterministic) seed used to extract the corrections to be properly initalized. As a result of this bug, muons with same properties would end up having the same random number used for the extraction of the correction factor, thus introducing a bias. Also, we upload the scale and smearing corrections in case the vertex is constrained to the beam spot (`*_VXBS.json.gz`).
