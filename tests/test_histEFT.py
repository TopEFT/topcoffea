import hist
from topcoffea.modules.histEFT import HistEFT

from collections import defaultdict

import numpy as np

eft_coeff = np.array(
    [
        [1.1, 2.1, 3.1],
        [1.2, 2.2, 3.2],
        [1.3, 2.3, 3.3],
        [1.4, 2.4, 3.4],
        [1.5, 2.5, 3.5],
    ]
)

ht = np.array([1, 1, 2, 15, 25])

h = HistEFT(
    hist.axis.StrCategory([], name="process", growth=True),
    hist.axis.StrCategory([], name="channel", growth=True),
    hist.axis.Regular(
        name="ht",
        label="ht [GeV]",
        bins=3,
        start=0,
        stop=30,
        flow=True,
    ),
    wc_names=["ctG"],
    label="Events",
)

h.fill(process="ttH", channel="ch0", ht=ht, eft_coeff=eft_coeff)

h.fill(
    process="ttH",
    channel="flown",
    ht=np.array([100, -100]),
    eft_coeff=[[110, 120, 130], [140, 150, 160]],
)


def test_select_sm():
    # manually sum sm coefficients
    dense_axis = h.axes["ht"]
    counts = defaultdict(int)
    for v, c in zip(ht, eft_coeff[:, 0]):
        counts[dense_axis.index(v)] += c

    sm = np.array(list(counts.values()))

    # get sm by selecting the correct column from values
    assert np.all(
        np.abs(h[{"process": "ttH", "channel": "ch0"}].values()[:, 0] - sm) < 1e-10
    )

    # get sm by selecting the correct column from view
    assert np.all(np.abs(h.view(as_dict=True)["ttH", "ch0"][:, 0] - sm) < 1e-10)

    # get sm by evaluating at zero, dropping under/overflow
    assert np.all(np.abs(h.eval({})["ttH", "ch0"][1:-1] - sm) < 1e-10)

    # get sm by integrating and then evaluating at zero, dropping under/overflow
    assert np.all(
        np.abs(h[{"process": "ttH", "channel": "ch0"}].eval({})[()][1:-1] - sm) < 1e-10
    )


def test_eval():
    sum_all = np.sum(eft_coeff)

    # select first
    ho = h["ttH", "ch0"]
    sum_one_p = np.sum(ho.eval({"ctG": 1})[()])
    assert abs(sum_all - sum_one_p) < 1e-10

    sum_one_m = np.sum(ho.eval({"ctG": -1})[()])
    sum_two_p = np.sum(ho.eval({"ctG": 2})[()])
    sum_two_m = np.sum(ho.eval({"ctG": -2})[()])
    sum_zero = np.sum(ho.eval({"ctG": 0})[()])

    # check linearity holds
    assert (
        abs(
            (sum_two_p + sum_two_m - 2 * sum_zero) / 4 (sum_one_p + sum_one_m - 2 * sum_zero)
        ) < 1e-10
    )


def test_flow():
    en = h["ttH", "ch0"].eval({"ctG": 1})[()]
    ef = h["ttH", sum].eval({"ctG": 1})[()]

    assert en[0] == 0 and en[-1] == 0
    assert ef[0] > 0 and ef[-1] > 0

    assert np.all(np.abs(en[1:-1] - ef[1:-1]) < 1e-10)
