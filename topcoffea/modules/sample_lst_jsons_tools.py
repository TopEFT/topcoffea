# This file is essentially a wrapper for createJSON.py:
#   - It runs createJSON.py for each sample that you include in a dictionary, and moves the resulting json file to the directory you specify

import json
import subprocess
import os
import yaml
from topcoffea.modules.paths import topcoffea_path

########### The XSs from xsec.cfg ###########
with open(topcoffea_path("params/xsec.yml")) as f:
    XSECDIC = yaml.load(f,Loader=yaml.CLoader)

########### Functions for makign the jsons ###########

# Replace a value in one of the JSONs
def replace_val_in_json(path_to_json_file,key,new_val,verbose=True):

    # Replace value if it's different than what's in the JSON
    with open(path_to_json_file) as json_file:
        json_dict = json.load(json_file)
    if new_val == json_dict[key]:
        if verbose:
            print(f"\tValues already agree, both are {new_val}")
    else:
        if verbose:
            print(f"\tOld value for {key}: {json_dict[key]}")
            print(f"\tNew value for {key}: {new_val}")
        json_dict[key] = new_val

        # Save new json
        with open(path_to_json_file, "w") as out_file:
            json.dump(json_dict, out_file, indent=2)


# Loop through a dictionary of samples and replace the xsec in the JSON with what's in xsec.cfg
def replace_xsec_for_dict_of_samples(samples_dict,out_dir):
    for sample_name,sample_info in samples_dict.items():
        path_to_json = os.path.join(out_dir,sample_name+".json")
        if not os.path.exists(path_to_json):
            print(f"\nWARNING: This json does not exist, continuing ({path_to_json})")
            continue
        xsecName = sample_info["xsecName"]
        new_xsec = XSECDIC[xsecName]
        print(f"\nReplacing XSEC for {sample_name} JSON with the value from xsec.cfg for \"{xsecName}\":")
        print("\tPath to json:",path_to_json)
        replace_val_in_json(path_to_json,"xsec",new_xsec)

# Wrapper for createJSON.py
def make_json(sample_dir,sample_name,prefix,sample_yr,xsec_name,hist_axis_name,xsec_value=None,era=None,on_das=False,include_lhe_wgts_arr=False,skip_file_name=[]):

    # If the sample is on DAS, inclue the DAS flag in the createJSON.py arguments
    das_flag = ""
    include_lhe_wgts_arr_flag = ""
    if on_das: das_flag = "--DAS"
    if include_lhe_wgts_arr: include_lhe_wgts_arr_flag = "--includeLheWgts"

    path_to_createJSON = topcoffea_path("modules/createJSON.py")
    path_to_xsecs = topcoffea_path("params/xsec.yml")

    args = [
        "python",
        path_to_createJSON,
        sample_dir,
        das_flag,
        include_lhe_wgts_arr_flag,
        "--sampleName"   , sample_name,
        "--prefix"       , prefix,
        "--xsec"         , path_to_xsecs,
        "--year"         , sample_yr,
        "--histAxisName" , hist_axis_name,
    ]

    if era is not None:
        args.extend(['--era', era])

    if xsec_name:
        args.extend(['--xsecName',xsec_name])

    if xsec_value:
        args.extend(['--xsecValue',xsec_value])

    if (xsec_value is not None) and (xsec_name is not None):
        raise Exception("Both xsec_name and value is passed. Pass only one.")

    if skip_file_name:
        if isinstance(skip_file_name,list):
            args.extend(['--skipFileName', *skip_file_name])
        elif isinstance(skip_file_name,str):
            args.extend(['--skipFileName', skip_file_name])

    # Run createJSON.py
    subprocess.run(args)

