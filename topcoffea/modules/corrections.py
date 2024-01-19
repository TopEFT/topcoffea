import numpy as np
import awkward as ak
import uproot
from coffea import lookup_tools
import correctionlib

from topcoffea.modules.paths import topcoffea_path
from topcoffea.modules.get_param_from_jsons import GetParam
get_tc_param = GetParam(topcoffea_path("params/params.json"))

# Evaluate btag method 1a weight for a single WP (https://twiki.cern.ch/twiki/bin/viewauth/CMS/BTagSFMethods)
#   - Takes as input a given array of eff and sf and a mask for whether or not the events pass a tag
#   - Returns P(DATA)/P(MC)
#   - Where P(MC) = Product over tagged (eff) * Product over not tagged (1-eff)
#   - Where P(DATA) = Product over tagged (eff*sf) * Product over not tagged (1-eff*sf)
def get_method1a_wgt_singlewp(eff,sf,passes_tag):
    p_mc = ak.prod(eff[passes_tag],axis=-1) * ak.prod(1-eff[~passes_tag],axis=-1)
    p_data = ak.prod(eff[passes_tag]*sf[passes_tag],axis=-1) * ak.prod(1-eff[~passes_tag]*sf[~passes_tag],axis=-1)
    wgt = p_data/p_mc
    return wgt

# Evaluate btag sf from central correctionlib json
def btag_sf_eval(jet_collection,wp,year,method,syst):

    # Get the right sf json for the given year
    if year == "2016APV":
        fname = topcoffea_path("data/btag_sf_correctionlib/2016preVFP_UL_btagging.json")
    elif year == "2016":
        fname = topcoffea_path("data/btag_sf_correctionlib/2016postVFP_UL_btagging.json")
    elif year == "2017":
        fname = topcoffea_path("data/btag_sf_correctionlib/2017_UL_btagging.json")
    elif year == "2018":
        fname = topcoffea_path("data/btag_sf_correctionlib/2018_UL_btagging.json")
    else:
        raise Exception(f"Not a known year: {year}")

    # Flatten the input (until correctionlib handles jagged data natively)
    abseta_flat = ak.flatten(abs(jet_collection.eta))
    pt_flat = ak.flatten(jet_collection.pt)
    flav_flat = ak.flatten(jet_collection.hadronFlavour)

    # For now, cap all pt at 1000 https://cms-talk.web.cern.ch/t/question-about-evaluating-sfs-with-correctionlib/31763
    pt_flat = ak.where(pt_flat>1000.0,1000.0,pt_flat)

    # Evaluate the SF
    ceval = correctionlib.CorrectionSet.from_file(fname)
    sf_flat = ceval[method].evaluate(syst,wp,flav_flat,abseta_flat,pt_flat)
    sf = ak.unflatten(sf_flat,ak.num(jet_collection.pt))

    return sf


###############################################################
###### Pileup reweighing (as implimented for TOP-22-006) ######
###############################################################
## Get central PU data and MC profiles and calculate reweighting
## Using the current UL recommendations in:
##   https://twiki.cern.ch/twiki/bin/viewauth/CMS/PileupJSONFileforData
##   - 2018: /afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions18/13TeV/PileUp/UltraLegacy/
##   - 2017: /afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions17/13TeV/PileUp/UltraLegacy/
##   - 2016: /afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions16/13TeV/PileUp/UltraLegacy/
##
## MC histograms from:
##    https://github.com/CMS-LUMI-POG/PileupTools/

pudirpath = topcoffea_path('data/pileup/')

def GetDataPUname(year, var=0):
    ''' Returns the name of the file to read pu observed distribution '''
    if year == '2016APV': year = '2016-preVFP'
    if year == '2016': year = "2016-postVFP"
    if var == 'nominal':
        ppxsec = get_tc_param("pu_w")
    elif var == 'up':
        ppxsec = get_tc_param("pu_w_up")
    elif var == 'down':
        ppxsec = get_tc_param("pu_w_down")
    year = str(year)
    return 'PileupHistogram-goldenJSON-13tev-%s-%sub-99bins.root' % ((year), str(ppxsec))

MCPUfile = {'2016APV':'pileup_2016BF.root', '2016':'pileup_2016GH.root', '2017':'pileup_2017_shifts.root', '2018':'pileup_2018_shifts.root'}
def GetMCPUname(year):
    ''' Returns the name of the file to read pu MC profile '''
    return MCPUfile[str(year)]

PUfunc = {}
### Load histograms and get lookup tables (extractors are not working here...)
for year in ['2016', '2016APV', '2017', '2018']:
    PUfunc[year] = {}
    with uproot.open(pudirpath+GetMCPUname(year)) as fMC:
        hMC = fMC['pileup']
        PUfunc[year]['MC'] = lookup_tools.dense_lookup.dense_lookup(
            hMC.values() / np.sum(hMC.values()),
            hMC.axis(0).edges()
        )
    with uproot.open(pudirpath + GetDataPUname(year,'nominal')) as fData:
        hD = fData['pileup']
        PUfunc[year]['Data'] = lookup_tools.dense_lookup.dense_lookup(
            hD.values() / np.sum(hD.values()),
            hD.axis(0).edges()
        )
    with uproot.open(pudirpath + GetDataPUname(year,'up')) as fDataUp:
        hDUp = fDataUp['pileup']
        PUfunc[year]['DataUp'] = lookup_tools.dense_lookup.dense_lookup(
            hDUp.values() / np.sum(hDUp.values()),
            hD.axis(0).edges()
        )
    with uproot.open(pudirpath + GetDataPUname(year, 'down')) as fDataDo:
        hDDo = fDataDo['pileup']
        PUfunc[year]['DataDo'] = lookup_tools.dense_lookup.dense_lookup(
            hDDo.values() / np.sum(hDDo.values()),
            hD.axis(0).edges()
        )

def GetPUSF(nTrueInt, year, var='nominal'):
    year = str(year)
    if year not in ['2016','2016APV','2017','2018']:
        raise Exception(f"Error: Unknown year \"{year}\".")
    nMC = PUfunc[year]['MC'](nTrueInt+1)
    data_dir = 'Data'
    if var == 'up':
        data_dir = 'DataUp'
    elif var == 'down':
        data_dir = 'DataDo'
    nData = PUfunc[year][data_dir](nTrueInt)
    weights = np.divide(nData,nMC)
    return weights

###############################################################
