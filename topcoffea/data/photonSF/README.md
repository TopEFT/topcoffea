## Photon scale factors
### 2016APV
**Medium ID** egammaEffi_EGM2D_Pho_Medium_UL16.root<br>
**Tight ID**  egammaEffi_EGM2D_Pho_Tight_UL16.root<br>
**EGM Twiki** https://twiki.cern.ch/twiki/bin/view/CMS/EgammaUL2016To2018#SFs_for_Photons_UL_2016_preVFP

### 2016
**Medium ID** egammaEffi_EGM2D_Pho_Medium_UL16_postVFP.root<br>
**Tight ID**  egammaEffi_EGM2D_Pho_Tight_UL16_postVFP.root<br>
**EGM Twiki** https://twiki.cern.ch/twiki/bin/view/CMS/EgammaUL2016To2018#SFs_for_Photons_UL_2016_postVFP

### 2017
**Medium ID** egammaEffi_EGM2D_PHO_Medium_UL17.root<br>
**Tight ID**  egammaEffi_EGM2D_PHO_Tight_UL17.root<br>
**EGM Twiki** https://twiki.cern.ch/twiki/bin/view/CMS/EgammaUL2016To2018#SFs_for_Photons_UL_2017

### 2018
**Medium ID** egammaEffi_EGM2D_Pho_Med_UL18.root<br>
**Tight ID**  egammaEffi_EGM2D_Pho_Tight_UL18.root<br>
**EGM Twiki** https://twiki.cern.ch/twiki/bin/view/CMS/EgammaUL2016To2018#SFs_for_Photons_UL_2018


### Scripts for making things that are inputs to the processors

* `add_err.py`:
    - This scripts adds error histograms (`EGamma_SF2D_err`) by reading the variances from the EGM POG SFs (`EGamma_SF2D`) and writing them to a second hist.
