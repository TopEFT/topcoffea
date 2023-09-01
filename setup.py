import setuptools

setuptools.setup(
    name='topcoffea',
    version='0.0.0',
    description='Framework and tools that sit on top of coffea to facilitate analyses',
    packages=setuptools.find_packages(),
    install_requires=[
          'coffea',
          'numpy==1.23.5',
          'pytest',
    ],
    # Include data files (Note: "include_package_data=True" does not seem to work)
    package_data={
        "topcoffea" : [
            "params/*",
            "data/fromTTH/fakerate/*.root",
            "data/leptonSF/*/*.root",
            "data/leptonSF/*/*.json",
            "data/JEC/*.txt",
            "data/JER/*.txt",
            "data/pileup/*.root",
            "data/MuonScale/*txt",
            "data/goldenJsons/*.txt",
            "data/TauSF/*.json",
            "data/topmva/lepid_weights/*.bin",
        ],
    }
)
