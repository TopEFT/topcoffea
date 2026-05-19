## Changes: 2026-04-13 ([Summer23Prompt23] Update JEC version to V3 with minor bug fixes)

Merge Request: [!2](https://gitlab.cern.ch/cms-analysis-corrections/JME/Run3-23CSep23-Summer23-NanoAODv12/-/merge_requests/2)

In this MR, we update the JEC version from V2 to V3, to match the recommendation from the [jerc-webpage](https://cms-jerc.web.cern.ch/Recommendations/#2023-prebpix). 

The new JEC version (V3) is identical to the previous one (V2) except for a bug fix in the `L2Relative` text files. In extremely rare cases, the V2 `L2Relative` JECs exhibited asymptotic behavior in a few high eta bins and a very tiny pT bin, leading to abnormally large corrected jet pT values. The fraction of affected events is exceedingly small (10e-7%), so the majority of analyses will not observe any noticeable effect. More information about this issue can be found in the corresponding [Gitlab issue](https://gitlab.cern.ch/cms-jetmet/coordination/coordination/-/issues/153#note_9486099) and the [presentation slides](https://indico.cern.ch/event/1545816/contributions/6507348/attachments/3066299/5423894/cms-jerc-news_13May2025.pdf#page=2). This issue is now fixed.

Additionally, we now include the reduced set of JES uncertainties ("Regrouped") for `AK8PFPuppi` jets, cloned from `AK4PFPuppi`.

Lastly, we fixed a minor bug reported [here](https://cms-talk.web.cern.ch/t/jerc-2025-correctionlib-bug/142489) for 2025, that affected all Run 3 `.json` files: previously, the last `run` of the last era for this tag, i.e. `run==369802` was not included in the run range for `L2L3Residual` JECs. This has now been fixed.
