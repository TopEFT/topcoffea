# SFs in correctionlib format released by BTV, EGM, JME, LUM, MUO, and TAU POGs

Documentation and path to json files is listed here: https://gitlab.cern.ch/cms-nanoAOD/jsonpog-integration

This directory was obtained via the following command:
```
cp /cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG /path/to/topcoffea/data
```

The `json.gz` files can be directly used with the python `correction-lib` module as follows:
```
cset = correctionlib.CorrectionSet.from_file(json_path)
```
If one wants to inspect them by hand:
   - open it with emacs, without unzipping
   - unzip with `gzip -d json_name.json.gz`, and open `json_name.json` with the preferred text editor

For Run 2, only the corrections for UL conditions are retained.
For Run 3, `2022_Summer22, 2022_Summer22EE, 2023_Summer23, 2023_Summer23BPix` are the recommended set of corrections for `2022, 2022EE, 2023, 2023BPix` data taking periods (in Aug 23rd, 2024 -- to update whenever POGs update their recommendations)

