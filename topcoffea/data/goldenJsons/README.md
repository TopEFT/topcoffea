## Run2 goldenJsons

This directory contains the text files corresponding to the "golden json" for 2016, 2017, and 2018. The information regarding these files can be found on this twiki: `https://twiki.cern.ch/twiki/bin/view/CMS/TWikiLUM`. The numbers in the `topcoffea/json/lumi.json` file are taken from that twiki.

The text files in this directory were copied from here:
```
/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions16/13TeV/Legacy_2016/Cert_271036-284044_13TeV_Legacy2016_Collisions16_JSON.txt
/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions17/13TeV/Legacy_2017/Cert_294927-306462_13TeV_UL2017_Collisions17_GoldenJSON.txt
/afs/cern.ch/cms/CAF/CMSCOMM/COMM_DQM/certification/Collisions18/13TeV/Legacy_2018/Cert_314472-325175_13TeV_Legacy2018_Collisions18_JSON.txt
```

The integrated luminosities for the three years cane be found in: `https://twiki.cern.ch/twiki/bin/viewauth/CMS/TopSystematics#Luminosity`


## Run3 goldenJsons

The information for the Run3 luminosity values (the numbers in 'topcoffea/json/lumi.json') and information on the goldenJsons can be found on this twiki: 'https://twiki.cern.ch/twiki/bin/viewauth/CMS/PdmVRun3Analysis'.

The Run3 JSONs can be copied from here:
```
/afs/cern.ch/cms/CAF/certification/Collisions22/Cert_Collisions2022_355100_362760_Golden.json
/afs/cern.ch/cms/CAF/certification/Collisions23/Cert_Collisions2023_366442_370790_Golden.json
```
The JSONs can then be copied into a .txt file:
```
cp Cert_Collisions2022_355100_362760_Golden.json Cert_Collisions2022_355100_362760_Golden.txt
cp Cert_Collisions2023_366442_370790_Golden.json Cert_Collisions2023_366442_370790_Golden.txt
```


