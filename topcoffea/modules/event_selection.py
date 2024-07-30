# Event selection functions

import numpy as np
import awkward as ak

# This is a helper function called by trg_pass_no_overlap
#   - Takes events objects, and a lits of triggers
#   - Returns an array the same length as events, elements are true if the event passed at least one of the triggers and false otherwise
def passes_trg_inlst(events,trg_name_lst):
    tpass = np.zeros_like(np.array(events.MET.pt), dtype=bool)
    trg_info_dict = events.HLT

    # "fields" should be list of all triggers in the dataset
    common_triggers = set(trg_info_dict.fields) & set(trg_name_lst)

    # Check to make sure that at least one of our specified triggers is present in the dataset
    if len(common_triggers) == 0 and len(trg_name_lst):
        raise Exception("No triggers from the sample matched to the ones used in the analysis.")

    for trg_name in common_triggers:
        tpass = tpass | trg_info_dict[trg_name]
    return tpass


# This is what we call from the processor
#   - Returns an array the len of events
#   - Elements are false if they do not pass any of the triggers defined in dataset_dict
#   - In the case of data, events are also false if they overlap with another dataset
def trg_pass_no_overlap(events,is_data,dataset,year,dataset_dict,exclude_dict,era=None):

    # The triggers for 2016 and 2016APV are the same
    if year == "2016APV":
        year = "2016"
    # The triggers for 2022 and 2022EE are the same
    if year == "2022EE":
        year = "2022"
    # The triggers for 2023 and 2023BPix are the same
    if year == "2023BPix":
        year = "2023"

    # Initialize ararys and lists, get trg pass info from events
    trg_passes    = np.zeros_like(np.array(events.MET.pt), dtype=bool) # Array of False the len of events
    trg_overlaps  = np.zeros_like(np.array(events.MET.pt), dtype=bool) # Array of False the len of events
    trg_info_dict = events.HLT
    full_trg_lst  = []

    # Get the full list of triggers in all datasets
    for dataset_name in dataset_dict[year].keys():
        full_trg_lst = full_trg_lst + dataset_dict[year][dataset_name]

    # Check if events pass any of the triggers
    trg_passes = passes_trg_inlst(events,full_trg_lst)

    # In case of data, check if events overlap with other datasets
    if is_data:
        if era is not None: # Used for era dependency in Run3
            trg_passes = passes_trg_inlst(events,dataset_dict[year][dataset])
            trg_overlaps = passes_trg_inlst(events, exclude_dict[era][dataset])
        else:
            trg_passes = passes_trg_inlst(events,dataset_dict[year][dataset])
            trg_overlaps = passes_trg_inlst(events, exclude_dict[year][dataset])

    # Return true if passes trg and does not overlap
    return (trg_passes & ~trg_overlaps)


# Returns a mask for events with a same flavor opposite (same) sign pair close to the Z
# Mask will be True if any combination of 2 leptons from within the given collection satisfies the requirement
def get_Z_peak_mask(lep_collection,pt_window,flavor="os",zmass=91.2):
    ll_pairs = ak.combinations(lep_collection, 2, fields=["l0","l1"])
    zpeak_mask = (abs((ll_pairs.l0+ll_pairs.l1).mass - zmass)<pt_window)
    if flavor == "os":
        sf_mask = (ll_pairs.l0.pdgId == -ll_pairs.l1.pdgId)
    elif flavor == "ss":
        sf_mask = (ll_pairs.l0.pdgId == ll_pairs.l1.pdgId)
    elif flavor == "as": # Same flav any sign
        sf_mask = ((ll_pairs.l0.pdgId == ll_pairs.l1.pdgId) | (ll_pairs.l0.pdgId == -ll_pairs.l1.pdgId))
    else:
        raise Exception(f"Error: flavor requirement \"{flavor}\" is unknown.")
    sfosz_mask = ak.flatten(ak.any((zpeak_mask & sf_mask),axis=1,keepdims=True)) # Use flatten here because it is too nested (i.e. it looks like this [[T],[F],[T],...], and want this [T,F,T,...]))
    return sfosz_mask

# Returns a mask for all events with any os lepton pair within low region of off-Z
# Mask will be True if any combination of 2 leptons from within the given collection satisfies the requirement
def get_off_Z_mask_low(lep_collection,pt_window,flavor="os"):
    ll_pairs = ak.combinations(lep_collection, 2, fields=["l0","l1"])
    zpeak_mask = ((91.2-(ll_pairs.l0+ll_pairs.l1).mass)>pt_window)
    if flavor == "os":
        sf_mask = (ll_pairs.l0.pdgId == -ll_pairs.l1.pdgId)
    elif flavor == "ss":
        sf_mask = (ll_pairs.l0.pdgId == ll_pairs.l1.pdgId)
    elif flavor == "as": # Same flav any sign
        sf_mask = ((ll_pairs.l0.pdgId == ll_pairs.l1.pdgId) | (ll_pairs.l0.pdgId == -ll_pairs.l1.pdgId))
    else:
        raise Exception(f"Error: flavor requirement \"{flavor}\" is unknown.")
    sfosz_mask = ak.flatten(ak.any((zpeak_mask & sf_mask),axis=1,keepdims=True)) # Use flatten here because it is too nested (i.e. it looks like this [[T],[F],[T],...], and want this [T,F,T,...]))
    return sfosz_mask

# Returns a mask for all events with any os lepton pair
def get_any_sfos_pair(lep_collection):
    ll_pairs = ak.combinations(lep_collection, 2, fields=["l0","l1"])
    sf_mask = (ll_pairs.l0.pdgId == -ll_pairs.l1.pdgId)
    sfosz_mask = ak.flatten(ak.any(sf_mask,axis=1, keepdims=True))
    return sfosz_mask
