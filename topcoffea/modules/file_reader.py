import uproot

# Extracts event information from a root file
def get_info(fname, tree_name = "Events"):
    # The info we want to get
    raw_events = 0  # The raw number of entries as reported by TTree.num_entries
    gen_events = 0  # Number of gen events according to 'genEventCount' or set to raw_events if not found
    sow_events = 0  # Sum of weights
    is_data = False
    print(f"Opening with uproot: {fname}")
    with uproot.open(fname) as f:
        tree = f[tree_name]
        is_data = not "genWeight" in tree

        raw_events = int(tree.num_entries)
        if is_data:
            # Data doesn't have gen or weighted events!
            gen_events = raw_events
            sow_events = raw_events
        else:
            gen_events = raw_events
            sow_events = sum(tree["genWeight"])
            if "Runs" in f:
                # Instead get event from the "Runs" tree
                runs = f["Runs"]
                gen_key = "genEventCount" if "genEventCount" in runs else "genEventCount_"
                sow_key = "genEventSumw"  if "genEventSumw"  in runs else "genEventSumw_"
                gen_events = sum(runs[gen_key].array())
                sow_events = sum(runs[sow_key].array())
    return [raw_events, gen_events, sow_events, is_data]


# Get the list of WC names from an EFT sample
def get_list_of_wc_names(fname):
    ''' Retruns a list of the WC names from WCnames, (retruns [] if not an EFT sample) '''
    wc_names_lst = []
    tree = uproot.open(f'{fname}:Events')
    if 'WCnames' not in tree.keys():
        wc_names_lst = []
    else:
        wc_info = tree['WCnames'].array(entry_stop=1)[0]
        for idx,i in enumerate(wc_info):
            h = hex(i)[2:]                                 # Get rid of the first two characters
            wc_fragment = bytes.fromhex(h).decode('utf-8') # From: https://stackoverflow.com/questions/3283984/decode-hex-string-in-python-3
            # The WC names that are longer than 4 letters are too long to be encoded in a 64-bit integer:
            #   - They're instead stored in two subsequent entries in the list
            #   - This means that the decoded names in wc_info go like this [... 'ctlT' , '-i' ...]
            #   - The leading '-' indicates the given fragment is the trailing end of the previous WC name
            #   - The following logic is supposed to put those fragments back together into the WC name
            if not wc_fragment.startswith("-"):
                wc_names_lst.append(wc_fragment)
            else:
                leftover = wc_fragment[1:]                    # This leftover part of the WC goes with the previous one (but get rid of leading '-')
                wc_names_lst[-1] = wc_names_lst[-1]+leftover  # So append this trailing fragment to the leading framgenet to reconstruct the WC name
    return wc_names_lst
