import argparse
import json
import yaml

from topcoffea.modules.paths import topcoffea_path
from topcoffea.modules.DASsearch import GetDatasetFromDAS, RunDasGoClientCommand
from topcoffea.modules.utils import get_files, get_info, get_list_of_wc_names


def main():

    parser = argparse.ArgumentParser(description='Create json file with list of samples and metadata')
    parser.add_argument('path'              , default=''           , help = 'Path to directory or DAS dataset')
    parser.add_argument('--prefix','-p'     , default=''           , help = 'Prefix to add to the path (e.g. redirector)')
    parser.add_argument('--sampleName','-s' , default=''           , help = 'Sample name, used to find files and/or output name')
    parser.add_argument('--xsec','-x'       , default=1            , help = 'Cross section (number or file to read)')
    parser.add_argument('--xsecName'        ,                        help = 'Name in cross section .cfg (only if different from sampleName)')
    parser.add_argument('--year','-y'       , default=-1           , help = 'Year')
    parser.add_argument('--treename'        , default='Events'     , help = 'Name of the tree')
    parser.add_argument('--histAxisName'    , default=''           , help = 'Name for the samples axis of the coffea hist')
    parser.add_argument('--era'             , default=None         , help = 'Era Name') #Needed for Era dependency in Run3

    parser.add_argument('--DAS'             , action='store_true'  , help = 'Search files from DAS dataset')
    parser.add_argument('--nFiles'          , default=None         , help = 'Number of max files (for the moment, only applies for DAS)')

    parser.add_argument('--outname','-o'    , default=''           , help = 'Out name of the json file')
    parser.add_argument('--options'         , default=''           , help = 'Sample-dependent options to pass to your analysis')
    parser.add_argument('--verbose','-v'    , action='store_true'  , help = 'Activate the verbosing')

    parser.add_argument('--includeLheWgts'  , action='store_true' , help = 'Include the set of LHE weights')


    args, unknown = parser.parse_known_args()
    #cfgfile     = args.cfgfile
    path         = args.path
    prefix       = args.prefix
    sample       = args.sampleName
    xsec         = args.xsec
    xsecName     = args.xsecName
    year         = args.year
    era          = args.era
    options      = args.options
    treeName     = args.treename
    histAxisName = args.histAxisName
    outname      = args.outname
    isDAS        = args.DAS
    nFiles       = int(args.nFiles) if not args.nFiles is None else None
    verbose      = args.verbose

    with open(topcoffea_path("params/xsec.yml")) as f:
        xsecdic = yaml.load(f,Loader=yaml.CLoader)

    # Get the xsec for the dataset
    if xsecName in xsecdic.keys():
        xsec = xsecdic[xsecName]
    else:
        raise Exception(f"Error: There is no xsec for process \"{xsecName}\" included the xsec cfg file.")

    sampdic = {}
    sampdic['xsec']         = xsec
    sampdic['year']         = year
    sampdic['treeName']     = treeName
    sampdic['histAxisName'] = histAxisName
    sampdic['options']      = options
    if era is not None:
        sampdic['era']      = era

    print("prefix",prefix)
    print("path",path)
    print("sample",sample)

    ###### Get the list of root files ######

    # Get all rootfiles in a dir and all the sub dirs if not on das
    if not isDAS:
        files_with_prefix = get_files(prefix+path,match_files=["\.root"],recursive=True)
        files = [(f[len(prefix):]) for f in files_with_prefix]
        if len(files_with_prefix) == 0:
            raise Exception(f"ERROR: No files found for this path \"{prefix+path}\".")

    # Search files in DAS dataset
    #   NOTE: For DAS searches, the is_data flag is determined from DAS itself, not the files
    else:
        dataset = path
        dicFiles = GetDatasetFromDAS(dataset, nFiles, options='file', withRedirector=prefix)
        files = [f[len(prefix):] for f in dicFiles['files']]
        files_with_prefix = dicFiles['files']

        # This DAS command for some reason returns the output doubled and will look something like this:
        #   output = " \ndata  \ndata  \n "
        # So we strip off the whitespace and spurious newlines and then only take the first of the duplicates
        dataset_part = "{name} | grep dataset.datatype".format(name=path)
        output = RunDasGoClientCommand(dataset=dataset_part,mode='')
        cleaned_output = output.strip().replace('\n',' ').split()[0]
        if cleaned_output == 'data':
            is_data = True
        elif cleaned_output == 'mc':
            is_data = False
        else:
            raise RuntimeError("Unknown datatype returned by DAS: ---{}---".format(output))


    ###### Get sum of weights etc ######

    # When getting data from DAS, we don't need to query every single file to get the number of events
    if isDAS and is_data and nFiles is None:
        dataset_part = "{name} | grep dataset.nevents".format(name=path)
        output = RunDasGoClientCommand(dataset=dataset_part,mode='')
        output = float(output.split(':')[-1].strip())

        # For data this this should all be the same
        n_events, n_gen_events, n_sum_of_weights = output, output, output

    # Access the file locally
    else:
        n_events = 0
        n_gen_events = 0
        n_sum_of_weights = 0
        is_data_lst = []
        n_sum_of_lhe_weights = None
        for f in files_with_prefix:
            i_events, i_gen_events, i_sum_of_weights, i_sum_of_lhe_weights, is_data = get_info(f, treeName)
            n_events += i_events
            n_gen_events += i_gen_events
            n_sum_of_weights += i_sum_of_weights
            is_data_lst.append(is_data)
            # Get the sum of the up and down LHE weights
            if not is_data:
                if n_sum_of_lhe_weights is None:
                    n_sum_of_lhe_weights = list(i_sum_of_lhe_weights)
                else:
                    if len(n_sum_of_lhe_weights) != len(i_sum_of_lhe_weights):
                        raise Exception("Different length of LHE weight array in different files.")
                    for i in range(len(n_sum_of_lhe_weights)):
                        n_sum_of_lhe_weights[i] += i_sum_of_lhe_weights[i]

        # Raise error if there is a mix of data and mc files
        if len(set(is_data_lst)) != 1:
            raise Exception("ERROR: There are a mix of files that are data and mc")
        # Otherwise all are same, so we can take is_data for the full list to be just whatever it is for the first element
        else:
            is_data = is_data_lst[0]


    ###### Fill the sampdic with the values we've found  ######

    # Any samples coming from DAS won't have EFT weights/WCs, saves having to actually access remote files
    if isDAS: sampdic['WCnames'] = []
    else: sampdic['WCnames'] = get_list_of_wc_names(files_with_prefix[0])
    sampdic['files']         = files
    sampdic['nEvents']       = n_events
    sampdic['nGenEvents']    = n_gen_events
    sampdic['nSumOfWeights'] = n_sum_of_weights
    sampdic['isData']        = is_data
    sampdic['path']          = path
    if args.includeLheWgts:
        sampdic['nSumOfLheWeights'] = n_sum_of_lhe_weights

    if ((sample == '') and (outname == '')):
        raise Exception("ERROR: There is no specified outname or sample")
    elif (outname == ''):
        outname = sample
    if not outname.endswith('.json'): outname += '.json'
    with open(outname, 'w') as outfile:
        json.dump(sampdic, outfile, indent=4)
        print(f"\n New json file: {outname}")

if __name__ == '__main__':
    main()
