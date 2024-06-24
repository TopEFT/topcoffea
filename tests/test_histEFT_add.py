import numpy as np
import hist
from topcoffea.modules.histEFT import HistEFT
import topcoffea.modules.eft_helper as efth
import copy

import awkward as ak
import dask

# Let's generate some fake data to use for testing
wc_names_lst = [
    "cpt",
    "ctp",
    "cptb",
    "cQlMi",
    "cQq81",
    "cQq11",
    "cQl3i",
    "ctq8",
    "ctlTi",
    "ctq1",
    "ctli",
    "cQq13",
    "cbW",
    "cpQM",
    "cpQ3",
    "ctei",
    "cQei",
    "ctW",
    "ctlSi",
    "cQq83",
    "ctZ",
    "ctG",
]

nevts = 1000
wc_count = len(wc_names_lst)
ncoeffs = efth.n_quad_terms(wc_count)

rng = np.random.default_rng()
eft_fit_coeffs = dask.array.from_array(rng.normal(0.3, 0.5, (nevts, ncoeffs)))
eft_all_ones_coeffs = dask.array.ones((nevts, ncoeffs))

vals = dask.array.full(nevts, 0.5)

weight_val = 0.9
weights = dask.array.full(nevts, weight_val)

sums = np.sum(dask.compute(eft_fit_coeffs)[0], axis=0)

ad = HistEFT(
    category_axes=[hist.axis.StrCategory([], name="type", label="type", growth=True)],
    dense_axis=hist.axis.Regular(1, 0, 1, name="x", label="x"),
    wc_names=wc_names_lst,
)

# Just need another one where I won't fill the EFT coefficients
bd = HistEFT(
    category_axes=[hist.axis.StrCategory([], name="type", label="type", growth=True)],
    dense_axis=hist.axis.Regular(1, 0, 1, name="x", label="x"),
    wc_names=wc_names_lst,
)

# Fill the EFT histogram
ad.fill(type="eft", x=vals, eft_coeff=eft_fit_coeffs)
bd.fill(type="non-eft", x=vals)


a_wd = HistEFT(
    category_axes=[hist.axis.StrCategory([], name="type", label="type", growth=True)],
    dense_axis=hist.axis.Regular(1, 0, 1, name="x", label="x"),
    wc_names=wc_names_lst,
)

b_wd = HistEFT(
    category_axes=[hist.axis.StrCategory([], name="type", label="type", growth=True)],
    dense_axis=hist.axis.Regular(1, 0, 1, name="x", label="x"),
    wc_names=wc_names_lst,
)

# Fill the EFT histogram
a_wd.fill(
    type="eft",
    x=vals,
    eft_coeff=eft_fit_coeffs,
    weight=weights,
)

b_wd.fill(type="non-eft", x=vals, weight=weights, eft_coeff=1)

(a, b, a_w, b_w) = dask.compute(ad, bd, a_wd, b_wd)


def test_number_of_coefficients():
    assert ncoeffs == 276


def test_integral():
    # check we get the same result when integrating out an axis
    ones = np.ones(wc_count)
    assert np.all(
        np.abs(a.eval(ones)[("eft",)] - a.integrate("type", "eft").eval(ones)[()]) < 2e-10
    )


def test_scale_a_weights():
    assert np.all(
        np.abs(
            # [()] is only surviving entry after integrate. [0] is first bin index of the dense axis
            a_w.integrate("type", "eft").view(as_dict=True)[()][0] - weight_val * sums
        ) < 2e-10
    )

    integral = a_w.integrate("type", "eft").view(as_dict=True)[()].sum()

    # check that evaluating at 0 gives sm back
    assert a_w.integrate("type", "eft").eval({})[()].sum() != integral

    # add all coefficients together, using an array for eval
    ones = np.ones(wc_count)
    assert np.abs(a_w.integrate("type", "eft").eval(ones)[()].sum() - integral) < 2e-10

    # add all coefficients together, using a dictionary for eval
    ones = dict(zip(wc_names_lst, np.ones(wc_count)))
    assert np.abs(a_w.integrate("type", "eft").eval(ones)[()].sum() - integral) < 2e-10

    # check that weighted and non-weighted give the same result after adjustment
    for va, va_w in zip(a.view(as_dict=True).values(), a_w.view(as_dict=True).values()):
        assert np.all(np.abs((va * weight_val) - va_w < 2e-10))


def test_ac_deepcopy():
    c_w = copy.deepcopy(a_w)

    assert np.all(
        a_w.integrate("type", "eft").view(as_dict=True)[()] == c_w.integrate("type", "eft").view(as_dict=True)[()]
    )
    c_w.scale(1)
    c_w.reset()

    assert ak.sum(a_w.values()) != 0
    assert ak.sum(c_w.values()) == 0


def test_group():
    c_w = a_w + b_w
    g_w = c_w.group("type", {"all": ["eft", "non-eft"]})

    assert (
        g_w.integrate("type").view(as_dict=True)[()].sum() == c_w.integrate("type").view(as_dict=True)[()].sum()
    )


def test_add_ab():
    ab = a + b

    assert np.all(
        np.abs(ab.integrate("type", "eft").view(as_dict=True)[()][0] - sums) < 2e-10
    )
    assert ab.integrate("type", "non-eft").view(as_dict=True)[()][0][0] == nevts


def test_add_ba():
    ba = b + a
    assert np.all(
        np.abs(ba.integrate("type", "eft").view(as_dict=True)[()][0] - sums) < 2e-10
    )
    assert ba.integrate("type", "non-eft").view(as_dict=True)[()][0][0] == nevts


def test_add_aba():
    ab = a + b
    aba = ab + a
    assert np.all(
        np.abs(aba.integrate("type", "eft").view(as_dict=True)[()][0] - 2 * sums) < 2e-10
    )
    assert aba.integrate("type", "non-eft").view(as_dict=True)[()][0][0] == nevts


def test_add_baa():
    ba = b + a
    baa = ba + a
    assert np.all(
        np.abs(baa.integrate("type", "eft").view(as_dict=True)[()][0] - 2 * sums) < 2e-10
    )
    assert baa.integrate("type", "non-eft").view(as_dict=True)[()][0][0] == nevts


def test_add_abb():
    ab = a + b
    abb = ab + b

    assert np.all(
        np.abs(abb.integrate("type", "eft").view(as_dict=True)[()][0] - sums) < 2e-10
    )

    assert abb.integrate("type", "non-eft").view(as_dict=True)[()][0][0] == 2 * nevts


def test_add_bab():
    ba = b + a
    bab = ba + b
    assert np.all(
        np.abs(bab.integrate("type", "eft").view(as_dict=True)[()][0] - sums) < 2e-10
    )
    assert bab.integrate("type", "non-eft").view(as_dict=True)[()][0][0] == 2 * nevts


def test_add_ab_weights():
    ab = a_w + b_w
    assert np.all(
        np.abs(ab.integrate("type", "eft").view(flow=False)[()][0] - weight_val * sums) < 2e-10
    )

    assert np.all(
        np.abs(
            ab.integrate("type", "non-eft").view(flow=False)[()][0][0] - weight_val * nevts
        ) < 2e-10
    )


def test_add_ba_weights():
    ba = b_w + a_w
    assert np.all(
        np.abs(ba.integrate("type", "eft").view(flow=False)[()][0] - weight_val * sums) < 2e-10
    )
    assert np.all(
        np.abs(
            ba.integrate("type", "non-eft").view(flow=False)[()][0][0] - weight_val * nevts
        ) < 2e-10
    )


def test_add_aba_weights():
    ab = a_w + b_w
    aba = ab + a_w
    assert np.all(
        np.abs(
            aba.integrate("type", "eft").view(flow=False)[()][0] - 2 * weight_val * sums
        ) < 2e-10
    )
    assert np.all(
        np.abs(
            aba.integrate("type", "non-eft").view(flow=False)[()][0][0] - weight_val * nevts
        ) < 2e-10
    )


def test_add_baa_weights():
    ba = b_w + a_w
    baa = ba + a_w
    assert np.all(
        np.abs(
            baa.integrate("type", "eft").view(flow=False)[()][0] - 2 * weight_val * sums
        ) < 2e-10
    )
    assert np.all(
        np.abs(
            baa.integrate("type", "non-eft").view(flow=False)[()][0][0] - weight_val * nevts
        ) < 2e-10
    )


def test_add_abb_weights():
    ab = a_w + b_w
    abb = ab + b_w

    assert np.all(
        np.abs(abb.integrate("type", "eft").view(flow=False)[()][0] - weight_val * sums) < 2e-10
    )

    assert np.all(
        np.abs(
            abb.integrate("type", "non-eft").view(flow=False)[()][0][0] - 2 * weight_val * nevts
        ) < 2e-10
    )


def test_add_bab_weights():
    ba = b_w + a_w
    bab = ba + b_w
    assert np.all(
        np.abs(bab.integrate("type", "eft").view(flow=False)[()][0] - weight_val * sums) < 2e-10
    )

    assert np.all(
        np.abs(
            bab.integrate("type", "non-eft").view(flow=False)[()][0][0] - 2 * weight_val * nevts
        ) < 2e-10
    )


def split_by_terms():
    # split_by_terms not yet implemented
    raise NotImplementedError

    integral = a.sum("type").view(as_dict=True)[()].sum()
    c = a.split_by_terms(["x"], "type")
    assert (
        integral == c.integrate(
            "type", [k[0] for k in c.view(as_dict=True) if "eft" not in k[0]]
        )
        .view(as_dict=True)[()]
        .sum()
    )
