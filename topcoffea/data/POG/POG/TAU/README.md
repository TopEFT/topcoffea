# TauPOG-recommended tau corrections

This repository contains the scale factors (SFs) and energy scales recommended by the TauPOG.
More detailed recommendations can be found on this TWiki page: https://twiki.cern.ch/twiki/bin/viewauth/CMS/TauIDRecommendationForRun2


## Summary of available SFs

This is a rough summary of the available SFs for `DeepTau2017v2p1`:

| Tau component  | `genmatch`  | `DeepTau2017v2p1` `VSjet`  | `DeepTau2017v2p1` `VSe`  | `DeepTau2017v2p1` `VSmu`  | energy scale   |
|:--------------:|:-----------:|:--------------------------:|:------------------------:|:-------------------------:|:--------------:|
| real tau       | `5`         | vs. pT, or vs. pT and DM   | – (*)                    | – (*)                     | vs. DM         |
| e -> tau fake  | `1`, `3`    | –                          | vs. eta                  | –                         | vs. DM and eta |
| mu -> tau fake | `2`, `4`    | –                          | –                        | vs. eta                   | – (±1% unc.)   |

and for `DeepTau2018v2p5`:

| Tau component  | `genmatch`  | `DeepTau2018v2p5` `VSjet`  | `DeepTau2018v2p5` `VSe`  | `DeepTau2018v2p5` `VSmu`  | energy scale   |
|:--------------:|:-----------:|:--------------------------:|:------------------------:|:-------------------------:|:--------------:|
| real tau       | `5`         | vs. pT (for pT>140 only), or vs. pT and DM   | – (*)                    | – (*)                     | vs. DM (*)     |
| e -> tau fake  | `1`, `3`    | –                          | –                        | –                         | –              |
| mu -> tau fake | `2`, `4`    | –                          | –                        | vs. eta (UL2018 only)     | – (±1% unc.)   |

(*) The scale factors are provided only for a sub-set of the working points. For the VSele discriminator, they are measured for the VVLoose and Tight WPs - users are strongly encoraged to use one of these two working points and should report to the TauPOG for approval if another working point is used. For the VSmu, they are measured for the Tight WP but we don't expect a large dependence on the chosen VSmu WP in this case so you are free to use any available WP you like for the muon rejection. 


The gen-matching is defined as:
* `1` for prompt electrons
* `2` for prompt muons
* `3` for electrons from tau decay
* `4` for muons from tau decay
* `5` for real taus
* `6` for no match, or jets faking taus.
For more info on gen-matching of taus, please see [here](https://twiki.cern.ch/twiki/bin/viewauth/CMS/HiggsToTauTauWorking2016#MC_Matching).
Note that in nanoAOD this is available as `Tau_GenPartFlav`, but jet or no match correspond to `Tau_GenPartFlav==0` instead of `6`.

The SFs are meant for the following campaigns:

| Year label   | MC campaign              | Data campaign           |
|:------------:|:------------------------:| :----------------------:|
| `2016Legacy` | `RunIISummer16MiniAODv3` | `17Jul2018`             |
| `2017ReReco` | `RunIIFall17MiniAODv2`   | `31Mar2018`             |
| `2018ReReco` | `RunIIAutumn18MiniAOD`   | `17Sep2018`/`22Jan2019` |
| `UL2016_preVFP`  | `RunIISummer20UL16*APV`  | `(HIPM_)UL2016_MiniAODv*` |
| `UL2016_postVFP` | `RunIISummer20UL16`      | `UL2016_MiniAODv*`        |
| `UL2017`         | `RunIISummer20UL17`      | `UL2017_MiniAODv*`        |
| `UL2018`         | `RunIISummer20UL18`      | `UL2018_MiniAODv*`        |

(*) The SFs provided for pre-UL samples follow the old conventions for the binning by either pT or DM, and follow the old uncertainty scheme where only total uncertainties are reported

## Usage

Please install the [`correctionlib`](https://github.com/cms-nanoAOD/correctionlib) tool to read these SFs.
There are several ways to install as documented
[here](https://cms-nanoaod.github.io/correctionlib/install.html),
but the best way is via `python3`, for example on lxplus,
```
source /cvmfs/sft.cern.ch/lcg/views/LCG_99/x86_64-centos7-gcc8-opt/setup.sh
git clone --recursive https://github.com/cms-tau-pog/correctionlib.git
cd correctionlib
python3 -m pip install .
python3 -c 'import correctionlib._core; import correctionlib.schemav2' # test
```
Alternatively, `correctionlib` is pre-installed starting from `CMSSW_12_1`:
```
source $VO_CMS_SW_DIR/cmsset_default.sh
export SCRAM_ARCH=slc7_amd64_gcc900
cmsrel CMSSW_12_2_0_pre3
cd CMSSW_12_2_0_pre3/src/
cmsenv
python3 -c 'import correctionlib._core; import correctionlib.schemav2' # test
```

Find out the content of the `tau.json` using
```
gunzip POG/TAU/2018_ReReco/tau.json.gz
correction summary POG/TAU/2018_ReReco/tau.json
```
An example is given in [`examples/tauExample.py`](../../examples/tauExample.py).
You can load the set of corrections as follows in python as
```
import correctionlib as _core
cset = _core.CorrectionSet.from_file("tau.json")
corr1 = cset["DeepTau2018v2p5VSjet"]
corr2 = cset["DeepTau2018v2p5VSe"]
corr3 = cset["tau_trigger"]
corr4 = cset["tau_energy_scale"]
```
And then on an event-by-event basis with reconstructed tau objects, you can do
```
sf1 = corr1.evaluate(pt,dm,genmatch,wp,wp_VSe,syst,"dm")
sf2 = corr2.evaluate(eta,genmatch,wp,syst)
sf3 = corr3.evaluate(pt,dm,"etau",wp,"sf",syst)
tes = corr4.evaluate(pt,eta,dm,genmatch,"DeepTau2018v2p5",wp,wp_VSe,syst)
```
Where `syst='nom'` to get nominal/default SFs or `syst`= a systematic variation as they are named in the jsons
To see  summary of the systematic variations use:
```
correction summary 2018_UL/tau_DeepTau2018v2p5_UL2018.json.gz
```

A C++ example can be found [here](https://github.com/cms-nanoAOD/correctionlib/blob/master/src/demo.cc).

Alternative way to load the JSON files (including gunzip'ed):
```
import correctionlib as _core
fname = "tau.json.gz"
if fname.endswith(".json.gz"):
  import gzip
  with gzip.open(fname,'rt') as file:
    data = file.read().strip()
  cset = _core.CorrectionSet.from_string(data)
else:
  cset = _core.CorrectionSet.from_file(fname)
```


## References

The TauPOG JSON files are created from scripts in https://github.com/cms-tau-pog/correctionlib

To update the TauPOG JSON files, please see [`README4UPDATES.md`](https://gitlab.cern.ch/cms-tau-pog/jsonpog-integration/-/blob/TauPOG_v2/POG/TAU/README4UPDATES.md) (TauPOG-internal).
