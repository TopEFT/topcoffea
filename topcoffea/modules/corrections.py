import numpy as np
import awkward as ak
import uproot
from coffea import lookup_tools
import correctionlib
import re

from topcoffea.modules.paths import topcoffea_path
from topcoffea.modules.get_param_from_jsons import GetParam
get_tc_param = GetParam(topcoffea_path("params/params.json"))

clib_year_map = {
    "2016APV": "2016preVFP_UL",
    "2016preVFP": "2016preVFP_UL",
    "2016": "2016postVFP_UL",
    "2017": "2017_UL",
    "2018": "2018_UL",
    "2022": "2022_Summer22",
    "2022EE": "2022_Summer22EE",
    "2023": "2023_Summer23",
    "2023BPix": "2023_Summer23BPix",
}

goldenJSON_map = {
    "2016APV": "Collisions16_UltraLegacy_goldenJSON",
    "2016": "Collisions16_UltraLegacy_goldenJSON",
    "2017": "Collisions17_UltraLegacy_goldenJSON",
    "2018": "Collisions18_UltraLegacy_goldenJSON",
    "2022": "Collisions2022_355100_357900_eraBCD_GoldenJson",
    "2022EE": "Collisions2022_359022_362760_eraEFG_GoldenJson",
    "2023": "Collisions2023_366403_369802_eraBC_GoldenJson",
    "2023BPix": "Collisions2023_369803_370790_eraD_GoldenJson",
}

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

    dt_era = "Run3" if year[2] == "2" else "Run2"
    clib_year = clib_year_map[year]
    fname = topcoffea_path(f"data/POG/BTV/{clib_year}/btagging.json.gz")

    # Flatten the input (until correctionlib handles jagged data natively)
    abseta_flat = ak.flatten(abs(jet_collection.eta))
    pt_flat = ak.flatten(jet_collection.pt)
    flav_flat = ak.flatten(jet_collection.hadronFlavour)

    # For now, cap all pt at 1000 https://cms-talk.web.cern.ch/t/question-about-evaluating-sfs-with-correctionlib/31763
    pt_flat = ak.where(pt_flat>1000.0,1000.0,pt_flat)

    # Evaluate the SF
    if dt_era == "Run3":
        sf = ak.ones_like(jet_collection.pt)
    else:
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
    if year not in clib_year_map.keys():
        raise Exception(f"Error: Unknown year \"{year}\".")

    clib_year = clib_year_map[year]
    json_path = topcoffea_path(f"data/POG/LUM/{clib_year}/puWeights.json.gz")
    ceval = correctionlib.CorrectionSet.from_file(json_path)

    pucorr_tag = goldenJSON_map[year]
    pu_corr = ceval[pucorr_tag].evaluate(nTrueInt, var)
    return pu_corr



###############################################################
###### Scale, PS weights (as implimented for TOP-22-006) ######
###############################################################
def AttachPSWeights(events):
    '''
    Retrieve ISR and FSR variations from PS weights based on the docstring in events.PSWeight.__doc__
    Then, it saves them in events

    ISRDown == ISR=0.5 FSR=1
    ISRUp == ISR=2 FSR=1
    FSRDown == ISR=1 FSR=0.5
    FSRUp == ISR=1 FSR=2
    '''

    # Check if PSWeight exists in the event
    if events.PSWeight is None:
        raise Exception('PSWeight not found!')

    # Get the PSWeight documentation
    psweight_doc = events.PSWeight.__doc__

    # If PSWeight.__doc__ is empty or malformed
    if not psweight_doc:
        raise Exception('PSWeight.__doc__ is empty or not available!')

    # Define the mapping we are looking for
    ps_map = {
        'ISR=0.5 FSR=1': 'ISRDown',
        'ISR=2 FSR=1': 'ISRUp',
        'ISR=1 FSR=0.5': 'FSRDown',
        'ISR=1 FSR=2': 'FSRUp'
    }

    # Extract the relevant information from the docstring
    # Example pattern: [0] is ISR=2 FSR=1
    pattern = r'\[(\d+)\] is ISR=(\d+\.?\d*) FSR=(\d+\.?\d*)'
    matches = re.findall(pattern, psweight_doc)

    # Dictionary to hold the index of each variation
    ps_indices = {}

    for match in matches:
        index, isr, fsr = match
        key = f'ISR={isr} FSR={fsr}'
        if key in ps_map:
            ps_indices[ps_map[key]] = int(index)

    # Retrieve required keys from ps_map values
    required_keys = ps_map.values() # It's an iterable in py3, not a list anymore!

    # Check if all needed weights were found
    if not all(key in ps_indices for key in required_keys):
        raise Exception('Not all ISR/FSR weight variations found in PSWeight.__doc__!')

    # Add up variation event weights
    events['ISRUp'] = events.PSWeight[:, ps_indices['ISRUp']]
    events['FSRUp'] = events.PSWeight[:, ps_indices['FSRUp']]

    # Add down variation event weights
    events['ISRDown'] = events.PSWeight[:, ps_indices['ISRDown']]
    events['FSRDown'] = events.PSWeight[:, ps_indices['FSRDown']]

def AttachScaleWeights(events):
    """
    Dynamically retrieves scale weights from LHEScaleWeight based on its __doc__.

    LHE scale variation weights (w_var / w_nominal)
    Case 1: If there are 9 weights:
        [0] is renscfact = 0.5d0 facscfact = 0.5d0
        [1] is renscfact = 0.5d0 facscfact = 1d0
        [2] is renscfact = 0.5d0 facscfact = 2d0
        [3] is renscfact =   1d0 facscfact = 0.5d0
        [4] is renscfact =   1d0 facscfact = 1d0
        [5] is renscfact =   1d0 facscfact = 2d0
        [6] is renscfact =   2d0 facscfact = 0.5d0
        [7] is renscfact =   2d0 facscfact = 1d0
        [8] is renscfact =   2d0 facscfact = 2d0
    Case 2: If there are 8 weights:
        [0] is MUF = "0.5" MUR = "0.5"
        [1] is MUF = "1.0" MUR = "0.5"
        [2] is MUF = "2.0" MUR = "0.5"
        [3] is MUF = "0.5" MUR = "1.0"
        [4] is MUF = "2.0" MUR = "1.0"
        [5] is MUF = "0.5" MUR = "2.0"
        [6] is MUF = "1.0" MUR = "2.0"
        [7] is MUF = "2.0" MUR = "2.0"
    """

    # Check if LHEScaleWeight exists in the event
    if events.LHEScaleWeight is None:
        raise Exception('LHEScaleWeight not found!')

    # Get the LHEScaleWeight documentation
    scale_weight_doc = events.LHEScaleWeight.__doc__

    does_doc_exist = True
    if not scale_weight_doc:
        #raise Exception('LHEScaleWeight.__doc__ is empty or not available!')
        does_doc_exist = False

    # Define the mapping we are looking for the three scenarios
    scenarios_map = {
        # Scenario 1: renscfact and facscfact for 9 weights
        "renscfact": {
            "scale_map": {
                'renscfact=0.5d0 facscfact=0.5d0': 'renormfactDown',
                'renscfact=0.5d0 facscfact=1d0': 'renormDown',
                'renscfact=0.5d0 facscfact=2d0': 'renormDown_factUp',
                'renscfact=1d0 facscfact=0.5d0': 'factDown',
                #'renscfact=1d0 facscfact=1d0': 'nominal',  # Handle nominal
                'renscfact=1d0 facscfact=2d0': 'factUp',
                'renscfact=2d0 facscfact=0.5d0': 'renormUp_factDown',
                'renscfact=2d0 facscfact=1d0': 'renormUp',
                'renscfact=2d0 facscfact=2d0': 'renormfactUp'
            },
            "re_pattern": r'\[(\d+)\] is renscfact=(\d+\.?\d*)d0 facscfact=(\d+\.?\d*)d0',
            "key": lambda match: f'renscfact={match[1]}d0 facscfact={match[2]}d0'
        },
        # Scenario 2: MUF and MUR for 9 weights
        "MUF9": {
            "scale_map": {
                'MUF="0.5" MUR="0.5"': 'renormDown_factDown',
                'MUF="1.0" MUR="0.5"': 'renormDown',
                'MUF="2.0" MUR="0.5"': 'renormDown_factUp',
                'MUF="0.5" MUR="1.0"': 'factDown',
                #'MUF="1.0" MUR="1.0"': 'nominal',  # Explicitly handle the nominal case
                'MUF="2.0" MUR="1.0"': 'factUp',
                'MUF="0.5" MUR="2.0"': 'renormUp_factDown',
                'MUF="1.0" MUR="2.0"': 'renormUp',
                'MUF="2.0" MUR="2.0"': 'renormUp_factUp'
            },
            "re_pattern": r'\[(\d+)\] is MUF="(\d+\.?\d*)" MUR="(\d+\.?\d*)"',
            "key": lambda match: f'MUF="{match[1]}" MUR="{match[2]}"'
        },
        # Scenario 3: MUF and MUR for 8 weights
        "MUF8": {
            "scale_map": {
                'MUF="0.5" MUR="0.5"': 'renormDown_factDown',
                'MUF="1.0" MUR="0.5"': 'renormDown',
                'MUF="2.0" MUR="0.5"': 'renormDown_factUp',
                'MUF="0.5" MUR="1.0"': 'factDown',
                'MUF="2.0" MUR="1.0"': 'factUp',
                'MUF="0.5" MUR="2.0"': 'renormUp_factDown',
                'MUF="1.0" MUR="2.0"': 'renormUp',
                'MUF="2.0" MUR="2.0"': 'renormUp_factUp'
            },
            "re_pattern": r'\[(\d+)\] is MUF="(\d+\.?\d*)" MUR="(\d+\.?\d*)"',
            "key": lambda match: f'MUF="{match[1]}" MUR="{match[2]}"'
        }
    }

    # Determine the number of weights available
    len_of_wgts = ak.count(events.LHEScaleWeight, axis=-1)
    all_len_9_or_0_bool = ak.all((len_of_wgts == 9) | (len_of_wgts == 0))
    all_len_8_or_0_bool = ak.all((len_of_wgts == 8) | (len_of_wgts == 0))
    scale_weights = None
    scale_map = None
    matches = None
    scenario = None

    # Choose between the different cases based on the number of weights and the doc string
    if all_len_9_or_0_bool:
        if "renscfact" in scale_weight_doc:
            scenario = "renscfact"  # Scenario 1: renscfact/facscfact
        elif "MUF" in scale_weight_doc:
            scenario = "MUF9"       # Scenario 2: MUF/MUR with 9 weights
    elif all_len_8_or_0_bool:
        scenario = "MUF8"           # Scenario 3: MUF/MUR with 8 weights
    else:
        raise Exception("Unknown weight type")

    scale_weights = ak.fill_none(ak.pad_none(events.LHEScaleWeight, 9 if (scenario == "MUF9" or scenario == "renscfact" or scenario is None) else 8), 1)
    # Dictionary to hold the index of each variation
    scale_indices = {}

    if scenario is not None:
        matches = re.findall(scenarios_map[scenario]["re_pattern"], scale_weight_doc)
        scale_map = scenarios_map[scenario]["scale_map"]
        key = scenarios_map[scenario]["key"]

        # Parse the matches and build the scale indices dictionary
        for match in matches:
            index = int(match[0])  # Extract the index from the regex match
            key_str = key(match)  # Dynamically get the key string based on the case

            if key_str in scale_map:
                scale_indices[scale_map[key_str]] = index

        required_keys = list(scale_map.values())

        # Check if all needed weights were found
        if not all(key in scale_indices for key in required_keys):
            missing_keys = [key for key in required_keys if key not in scale_indices]
            raise Exception('Not all scale weight variations found in LHEScaleWeight.__doc__!')

    else:
        #This part of the code assumes that every entry is unit when LHEScaleWeight is not actually filled
        dummy_keys = list(scenarios_map["MUF9"]["scale_map"].values())
        for id_key, dummy_key in enumerate(dummy_keys):
            scale_indices[dummy_key] = id_key

    # Assign the weights from the event to the respective fields dynamically using a loop
    for key in scale_indices:
        events[key] = scale_weights[:, scale_indices[key]]
