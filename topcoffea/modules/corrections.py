import numpy as np
import awkward as ak
import uproot
from coffea import lookup_tools
import correctionlib

from topcoffea.modules.paths import topcoffea_path
from topcoffea.modules.get_param_from_jsons import GetParam
get_tc_param = GetParam(topcoffea_path("params/params.json"))

### Btag corrections ###

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

def get_method1a_wgt_doublewp(effA, effB, sfA, sfB, cutA, cutB, cutC):
    effA_data = effA * sfA
    effB_data = effB * sfB

    pMC = ak.prod(effA[cutA], axis=-1) * ak.prod(effB[cutB] - effA[cutB], axis=-1) * ak.prod(1 - effB[cutC], axis=-1)
    pMC = ak.where(pMC==0,1,pMC) # removeing zeroes from denominator...
    pData = ak.prod(effA_data[cutA], axis=-1) * ak.prod(effB_data[cutB] - effA_data[cutB], axis=-1) * ak.prod(1 - effB_data[cutC], axis=-1)

    return pData, pMC

# Evaluate btag sf from central correctionlib json
def btag_sf_eval(jet_collection,wp,year,method,syst):
    # Get the right sf json for the given year
    if year.startswith("2016"):
        clib_year = "2016preVFP" if year == "2016APV" else "2016postVFP"
    if year == "2016preVFP":
        fname = topcoffea_path("data/btag_sf_correctionlib/2016preVFP_UL_btagging.json")
    elif year == "2016postVFP":
        fname = topcoffea_path("data/btag_sf_correctionlib/2016postVFP_UL_btagging.json")
    elif year == "2017":
        fname = topcoffea_path("data/btag_sf_correctionlib/2017_UL_btagging.json")
    elif year == "2018":
        fname = topcoffea_path("data/btag_sf_correctionlib/2018_UL_btagging.json")
    elif year == "2022":
        fname = topcoffea_path("data/btag_sf_correctionlib/2022_btagging.json")
    elif year == "2022EE":
        fname = topcoffea_path("data/btag_sf_correctionlib/2022EE_btagging.json")
    else:
        clib_year = year

    runII = ["16", "17", "18"]

    clib_year = clib_year + "_UL" if any(runIIyear in clib_year for runIIyear in runII) else clib_year

    fname = topcoffea_path(f"data/POG/BTV/{clib_year}/btagging.json.gz")

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

    if year.startswith("2016"):
        clib_year = "2016preVFP" if year == "2016APV" else "2016postVFP"
    else:
        clib_year = year

    runII = ["16", "17", "18"]
    clib_json = clib_year + "_UL" if any(runIIyear in clib_year for runIIyear in runII) else clib_year
    json_path = topcoffea_path(f"data/POG/LUM/{clib_json}/puWeights.json.gz")
    ceval = correctionlib.CorrectionSet.from_file(json_path)

    pucorr_tag = next((f"Collisions{runIIyear}_UltraLegacy_goldenJSON" for runIIyear in runII if runIIyear in year), "")
    pu_corr = ceval[pucorr_tag].evaluate(nTrueInt, var)
    return pu_corr



###############################################################
###### Scale, PS weights (as implimented for TOP-22-006) ######
###############################################################

def AttachPSWeights(events):
    '''
        Return a list of PS weights
        PS weights (w_var / w_nominal)
        [0] is ISR=0.5 FSR = 1
        [1] is ISR=1 FSR = 0.5
        [2] is ISR=2 FSR = 1
        [3] is ISR=1 FSR = 2
    '''
    ISR = 0
    FSR = 1
    ISRdown = 0
    FSRdown = 1
    ISRup = 2
    FSRup = 3
    if events.PSWeight is None:
        raise Exception('PSWeight not found!')
    # Add up variation event weights
    events['ISRUp'] = events.PSWeight[:, ISRup]
    events['FSRUp'] = events.PSWeight[:, FSRup]
    # Add down variation event weights
    events['ISRDown'] = events.PSWeight[:, ISRdown]
    events['FSRDown'] = events.PSWeight[:, FSRdown]

def AttachScaleWeights(events):
    '''
    Return a list of scale weights
    LHE scale variation weights (w_var / w_nominal)
    Case 1:
        [0] is renscfact = 0.5d0 facscfact = 0.5d0
        [1] is renscfact = 0.5d0 facscfact = 1d0
        [2] is renscfact = 0.5d0 facscfact = 2d0
        [3] is renscfact =   1d0 facscfact = 0.5d0
        [4] is renscfact =   1d0 facscfact = 1d0
        [5] is renscfact =   1d0 facscfact = 2d0
        [6] is renscfact =   2d0 facscfact = 0.5d0
        [7] is renscfact =   2d0 facscfact = 1d0
        [8] is renscfact =   2d0 facscfact = 2d0
    Case 2:
        [0] is MUF = "0.5" MUR = "0.5"
        [1] is MUF = "1.0" MUR = "0.5"
        [2] is MUF = "2.0" MUR = "0.5"
        [3] is MUF = "0.5" MUR = "1.0"
        [4] is MUF = "2.0" MUR = "1.0"
        [5] is MUF = "0.5" MUR = "2.0"
        [6] is MUF = "1.0" MUR = "2.0"
        [7] is MUF = "2.0" MUR = "2.0"
    '''
    # Determine if we are in case 1 or case 2 by checking if we have 8 or 9 weights
    len_of_wgts = ak.count(events.LHEScaleWeight,axis=-1)
    all_len_9_or_0_bool = ak.all((len_of_wgts==9) | (len_of_wgts==0))
    all_len_8_or_0_bool = ak.all((len_of_wgts==8) | (len_of_wgts==0))
    if all_len_9_or_0_bool:
        scale_weights = ak.fill_none(ak.pad_none(events.LHEScaleWeight, 9), 1) # FIXME this is a bandaid until we understand _why_ some are empty
        renormDown_factDown = 0
        renormDown          = 1
        renormDown_factUp   = 2
        factDown            = 3
        nominal             = 4
        factUp              = 5
        renormUp_factDown   = 6
        renormUp            = 7
        renormUp_factUp     = 8
    elif all_len_8_or_0_bool:
        scale_weights = ak.fill_none(ak.pad_none(events.LHEScaleWeight, 8), 1) # FIXME this is a bandaid until we understand _why_ some are empty
        renormDown_factDown = 0
        renormDown          = 1
        renormDown_factUp   = 2
        factDown            = 3
        factUp              = 4
        renormUp_factDown   = 5
        renormUp            = 6
        renormUp_factUp     = 7
    else:
        raise Exception("Unknown weight type")
    # Get the weights from the event
    events['renormfactDown'] = scale_weights[:,renormDown_factDown]
    events['renormDown']     = scale_weights[:,renormDown]
    events['factDown']       = scale_weights[:,factDown]
    events['factUp']         = scale_weights[:,factUp]
    events['renormUp']       = scale_weights[:,renormUp]
    events['renormfactUp']   = scale_weights[:,renormUp_factUp]
