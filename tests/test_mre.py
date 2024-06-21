import numpy as np
import hist
from collections import defaultdict

from topcoffea.modules.histEFT import HistEFT

np.set_printoptions(linewidth=100, formatter={"float_kind": lambda x: f"{x:7.3f}"})


def test_add():
    h = HistEFT(
        hist.axis.StrCategory([], name="process", growth=True),
        hist.axis.StrCategory([], name="systematic", growth=True),
        hist.axis.Variable([0, 200, 300, 400, 500], name="ptz"),
        wc_names=[],
        rebin=False,
    )

    dense_vals = np.array([-1, 50, 250, 350, 450, 550])

    procs = ["ttll_2016", "ttll_2017", "ttll_2018"]
    systs = ["nominal", "syst_2016", "syst_2017", "syst_2018"]

    for p in procs:
        p_name, p_year = p.split("_")
        for s in systs:
            if s != "nominal":
                s_name, s_year = s.split("_")
                if s_year != p_year:
                    continue
            h.fill(ptz=dense_vals, process=p, systematic=s, weights=1.0, eft_coeff=None)

    v = h.eval({})
    for key in h.categorical_keys:
        print(f"{str(tuple(key)):<30} -- {v[key]}")
    # Expected output:
    # ('ttll_2016', 'nominal')       -- [  1.000   1.000   1.000   1.000   1.000   1.000]
    # ('ttll_2016', 'syst_2016')     -- [  1.000   1.000   1.000   1.000   1.000   1.000]
    # ('ttll_2017', 'nominal')       -- [  1.000   1.000   1.000   1.000   1.000   1.000]
    # ('ttll_2017', 'syst_2017')     -- [  1.000   1.000   1.000   1.000   1.000   1.000]
    # ('ttll_2018', 'nominal')       -- [  1.000   1.000   1.000   1.000   1.000   1.000]
    # ('ttll_2018', 'syst_2018')     -- [  1.000   1.000   1.000   1.000   1.000   1.000]

    print("-" * 50)

    # Add the nominal of other years to each per-year systematic
    h[("ttll_2016", "syst_2016")] += (
        h[("ttll_2017", "nominal")] + h[("ttll_2018", "nominal")]
    )
    h[("ttll_2017", "syst_2017")] += (
        h[("ttll_2016", "nominal")] + h[("ttll_2018", "nominal")]
    )
    h[("ttll_2018", "syst_2018")] += (
        h[("ttll_2016", "nominal")] + h[("ttll_2017", "nominal")]
    )

    v = h.eval({})
    for key in h.categorical_keys:
        print(f"{str(tuple(key)):<30} -- {v[key]}")
    # Expected output:
    # ('ttll_2016', 'nominal')       -- [  1.000   1.000   1.000   1.000   1.000   1.000]
    # ('ttll_2016', 'syst_2016')     -- [  3.000   3.000   3.000   3.000   3.000   3.000]
    # ('ttll_2017', 'nominal')       -- [  1.000   1.000   1.000   1.000   1.000   1.000]
    # ('ttll_2017', 'syst_2017')     -- [  3.000   3.000   3.000   3.000   3.000   3.000]
    # ('ttll_2018', 'nominal')       -- [  1.000   1.000   1.000   1.000   1.000   1.000]
    # ('ttll_2018', 'syst_2018')     -- [  3.000   3.000   3.000   3.000   3.000   3.000]

    print("-" * 50)

    # Sum over years
    grp_map = defaultdict(lambda: [])
    for x in h.axes["process"]:
        p = x.split("_")[0]
        grp_map[p].append(x)
    h = h.group("process", grp_map)

    v = h.eval({})
    for key in h.categorical_keys:
        print(f"{str(tuple(key)):<30} -- {v[key]}")
    # Expected output:
    # ('ttll', 'nominal')            -- [  3.000   3.000   3.000   3.000   3.000   3.000]
    # ('ttll', 'syst_2036')          -- [  3.000   3.000   3.000   3.000   3.000   3.000]
    # ('ttll', 'syst_2037')          -- [  3.000   3.000   3.000   3.000   3.000   3.000]
    # ('ttll', 'syst_2038')          -- [  3.000   3.000   3.000   3.000   3.000   3.000]]

    v = h.eval({})
    for key in h.categorical_keys:
        print(f"{str(tuple(key)):<30} -- {v[key]}")
        assert v[key].sum() == 18


if __name__ == "__main__":
    test_add()
