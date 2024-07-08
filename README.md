# topcoffea

Tools that sit on top of coffea to facilitate CMS analyses. The repository is set up as a pip installable package. To install this package into a conda environment: 
```
git clone https://github.com/TopEFT/topcoffea.git
cd topcoffea
pip install -e .
```



Examples of analysis repositories making use of `topcoffea`:
* [`topeft`](https://github.com/TopEFT/topeft): EFT analyses in the top sector. 
* [`ewkcoffea`](): Multi boson analyses. 


#Tau SFs

The Tau SFs are obtained from: https://github.com/cms-tau-pog/TauIDSFs

In their default format, they are in a root file which requires CMSSW to extract. They were dumped into json files, uploaded here, which is a format more easily handled in coffea. The json file contains the same granularity as the original format, and no information is lost. The SFs have been confirmed to be the same.