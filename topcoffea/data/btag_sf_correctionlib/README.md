# Btagging SFs in correctionlib format (from BTV POG)

Documentation and path to json files is listed here https://btv-wiki.docs.cern.ch/ScaleFactors/#sf-campaigns

The files in this directory were obtained via the following commands:
```
cp /cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/BTV/2016preVFP_UL/btagging.json.gz 2016preVFP_UL_btagging.json.gz
cp /cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/BTV/2016postVFP_UL/btagging.json.gz 2016postVFP_UL_btagging.json.gz
cp /cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/BTV/2017_UL/btagging.json.gz 2017_UL_btagging.json.gz
cp /cvmfs/cms.cern.ch/rsync/cms-nanoAOD/jsonpog-integration/POG/BTV/2018_UL/btagging.json.gz 2018_UL_btagging.json.gz

gzip -d 2016preVFP_UL_btagging.json.gz
gzip -d 2016postVFP_UL_btagging.json.gz
gzip -d 2017_UL_btagging.json.gz
gzip -d 2018_UL_btagging.json.gz
```

