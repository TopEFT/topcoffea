# EGM POG-recommended corrections

This repository contains the scale factors (SFs) and the corrections for electrons and photons that are produced and supported by the EGM POG.
More detailed recommendations can be found on this TWiki page: https://twiki.cern.ch/twiki/bin/view/CMS/EgammSFandSSRun3

The available SFs are corrections are:
- electron Reco, ID, and HLT (only for Run3 and for some triggers) SFs;
- dedicated electron and photon SFs for high pT (>100 GeV) and tight working point of the cut-based ID;
- electron scale & smearing (SS) corrections;
- photon ID, Pixel veto and conversion-safe electron veto (only for Run3);
- photon scale & smearing corrections (only for Run3, whereas for Run2 there is only one set of corrections for both electrons and photons).

NOTE: the 2022 and 2023 Scale and Smearing corrections are provided separately for electrons and photons. In addition, two "flavours" can be used: a standard one, and an eT-dependent one (marked as such in the json file name). The eT-dependent corrections generally yield a better data/MC agreement for objects in the phase space they were derived upon. The Scale and Smearing corrections should not be used for electrons and photons below ~20 GeV and might also be ineffective at very high pT (hundreds of GeV). We recommend to use the eT-dependent corrections and to get in contact with the EGM POG should you observe unexpected/unwanted behaviours. For an explanation of the two different methods used to derive the [standard](https://indico.cern.ch/event/1259025/#49-introduction-to-residual-sc) and [et-dependent](https://indico.cern.ch/event/1441251/#3-hgg-et-dependent-scale-the-m) corrections, please follow the links.

The SFs and corrections are meant for the following campaigns:

| Year folder   | MC campaign              | Data campaign           |
|:------------:|:------------------------:| :----------------------:|
| `2016preVFP_UL`| `RunIISummer20UL16MiniAODAPVv2` - Nano v9 |`21Feb2020`|
| `2016postVFP_UL`| `RunIISummer20UL16MiniAODv2` - Nano v9 |`21Feb2020`|
| `2017_UL`| `RunIISummer20UL17MiniAODv2` - Nano v9 |`09Aug2019`|
| `2018_UL`| `RunIISummer20UL18MiniAODv2` - Nano v9 |`12Nov2019`|
| `2022_Prompt` | Winter22 | Prompt RunCDE |
| `2022_Summer22` | Summer22 | `22Sep2023` (ReReco CD) |
| `2022_Summer22EE` | Summer22EE | `22Sep2023` (ReReco E + Prompt RunFG) |
| `2023_Summer23` | Summer23 | Prompt23 RunC |
| `2023_Summer23BPix` | Summer23BPix | Prompt23 RunD |

NOTE: the scale & smearing corrections for Run2 UL (**Nano v9**) are meant to be used only to retrieve the correct scale uncertainty (which are mistakenly set to zero in NanoAOD), since the actual scale & smearing corrections are already embedded in both Mini and NanoAOD for the aforementioned campaigns.

# Usage

The corrections are provided in json-format, to be read using the [`correctionlib`](https://github.com/cms-nanoAOD/correctionlib) library.

Find out the content of the `electronSS_EtDependent.json.gz` doing
```
gunzip electronSS_EtDependent.json.gz
correction summary electronSS_EtDependent.json
```

Examples of how to read them are provided in [this twiki](https://twiki.cern.ch/twiki/bin/view/CMS/EgammSFandSSRun3). Dedicated scale & smearing examples are given [here](https://twiki.cern.ch/twiki/bin/view/CMS/EgammSFandSSRun3#2022_and_2023_Scale_and_Smearing). Please note that the eT-dependent scale corrections need to be accessed slightly differently from the others, namely using `compound()`, e.g.:

```
evaluator.compound()["EGMScale_Compound_Ele_2023preBPIX" (or others)].evaluate("scale" or "escale", run number, supercluster eta, r9, absolute supercluster eta, Et, (double) seed crystal gain)
```