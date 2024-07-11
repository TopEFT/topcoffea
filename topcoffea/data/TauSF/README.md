
# JSONs containing corrections and systematics

The Tau SFs are obtained from: https://github.com/cms-tau-pog/TauIDSFs

In their default format, they are in a root file which requires CMSSW to extract. They were dumped into json files, uploaded here, which is a format more easily handled in coffea. The json file contains the same granularity as the original format, and no information is lost. The SFs have been confirmed to be the same.


Apart from the files starting with `TauFakeSF_*`, the documentation and the source of the files contained here is listed TAU POG GH repo: https://github.com/cms-tau-pog/TauIDSFs.git. When necessary, the central files were translated in the usual JSON structure used for TOP-22-006, since the TAU POG json structure is not compatible with the structure used for TOP-22-006 

The SFs and systematic uncertainties contained in `TauFakeSF_*` represent the corrections to the estimate of the jet faking hadronic taus at a fixed WP of the `DeepTau` discriminator.
The procedure was presented to TAU POG: https://indico.cern.ch/event/1326438/#preview:4727537
