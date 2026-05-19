## Changes: 2026-04-13 ([Summer23BPixPrompt23] Fix minor run range bug and include Regrouped for AK8 jets)

Merge Request: [!14](https://gitlab.cern.ch/cms-analysis-corrections/JME/Run3-23DSep23-Summer23BPix-NanoAODv12/-/merge_requests/14)

In this MR, we fixed a minor bug reported [here](https://cms-talk.web.cern.ch/t/jerc-2025-correctionlib-bug/142489) for 2025, that affected all Run 3 `.json` files: previously, the last `run` of the last era for this tag, i.e. `run==372415` was not included in the run range for `L2L3Residual` JECs. This has now been fixed.

Additionally, we now include the reduced set of JES uncertainties ("Regrouped") for `AK8PFPuppi` jets, cloned from `AK4PFPuppi`.
