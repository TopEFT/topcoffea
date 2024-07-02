import uproot
import hist
import copy
import numpy as np

def add_err(fname='egammaEffi_EGM2D_Pho_Tight_UL16.root'):
    fin = uproot.update(fname)
    sf_name = 'EGamma_SF2D'
    h_sfs = fin[sf_name].to_hist()
    sfs = h_sfs.values()
    err = h_sfs.variances()
    h_err = copy.deepcopy(h_sfs)
    h_err.view().value = err
    fin[f'{sf_name}_err'] = h_err
    fin.close()


if __name__ == '__main__':
    files = ['egammaEffi_EGM2D_Pho_Medium_UL16_postVFP.root', 'egammaEffi_EGM2D_PHO_Medium_UL17.root', 'egammaEffi_EGM2D_Pho_Tight_UL16_postVFP.root', 'egammaEffi_EGM2D_PHO_Tight_UL17.root', 'egammaEffi_EGM2D_Pho_Medium_UL16.root', 'egammaEffi_EGM2D_Pho_Med_UL18.root', 'egammaEffi_EGM2D_Pho_Tight_UL16.root', 'egammaEffi_EGM2D_Pho_Tight_UL18.root']
    for fname in files:
        add_err(fname)
