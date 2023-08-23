import os
import json

from topcoffea.modules.paths import topcoffea_path
from topcoffea.modules.update_json import update_json
from topcoffea.modules.utils import load_sample_json_file

def test_update_json():
    src_fname = topcoffea_path("params/example_sample_json.json")
    dst_fname = "tmp_test_file.json"

    assert (os.path.exists(src_fname))

    if os.path.exists(dst_fname):
        os.remove(dst_fname)

    # Check to make sure that dry_run doesn't actually generate a file
    update_json(src_fname,dry_run=True,outname=dst_fname,verbose=True)

    assert (not os.path.exists(dst_fname))

    # No updates, should just create a copy
    update_json(src_fname,outname=dst_fname,verbose=True)

    assert (os.path.exists(dst_fname))
    with open(src_fname) as f:
        a = json.load(f)
    with open(dst_fname) as f:
        b = json.load(f)
    for k,v in a.items():
        assert (a[k] == b[k])

    updates = {
        "xsec": 909.090,
        "WCnames": ["ctG","ctZ","cpt"],
        "year": "1999",
        "files": [],
        "isData": True
    }

    # Apply the above updates and overwrite the previous test file
    update_json(src_fname,outname=dst_fname,verbose=True,**updates)

    # Check that the updates were all applied correctly
    test_jsn = load_sample_json_file(dst_fname)
    for k,v in updates.items():
        assert (test_jsn[k] == v)
