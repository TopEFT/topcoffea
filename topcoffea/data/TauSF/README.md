
# JSONs containing corrections and systematics

## Current packaged interface

This directory contains custom analysis jet-to-tau fake payloads. Current
`topeft` registration uses Run 2 Loose and Medium payload roles and a combined
Run 3 jet-fake payload role. They are separate from the packaged POG `VSjet`,
`VSe`, `VSmu`, and tau-energy payload families under `data/POG/TAU`.

The consuming analysis owns era, working-point, and variation selection. The
generic legacy names and Tight fallback names below are not current active
selection authorities; retain them only as payload provenance, not as an
instruction to activate a correction. This README makes no nuisance-correlation
or scientific closure claim.

The Tau SFs are obtained from: https://github.com/cms-tau-pog/TauIDSFs

In their default format, they are in a root file which requires CMSSW to extract. They were dumped into json files, uploaded here, which is a format more easily handled in coffea. The json file contains the same granularity as the original format, and no information is lost. The SFs have been confirmed to be the same.


Apart from the files starting with `TauFakeSF_*`, the documentation and the source of the files contained here is listed TAU POG GH repo: https://github.com/cms-tau-pog/TauIDSFs.git. When necessary, the central files were translated in the usual JSON structure used for TOP-22-006, since the TAU POG json structure is not compatible with the structure used for TOP-22-006 

The SFs and systematic uncertainties contained in `TauFakeSF_*` represent the corrections to the estimate of the jet faking hadronic taus at a fixed WP of the `DeepTau` discriminator.
The procedure was presented to TAU POG: https://indico.cern.ch/event/1326438/#preview:4727537
