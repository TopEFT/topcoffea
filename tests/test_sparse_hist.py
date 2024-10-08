import hist
from topcoffea.modules.sparseHist import SparseHist

import numpy as np
import awkward as ak
import pickle

import pytest

nbins = 12
data_ptz = np.arange(0, 600, 600 / nbins)


def make_hist():
    h = SparseHist(
        hist.axis.StrCategory([], name="process", growth=True),
        hist.axis.StrCategory([], name="channel", growth=True),
        hist.axis.Regular(nbins, 0, 600, name="ptz"),
    )
    h.fill(process="ttH", channel="ch0", ptz=data_ptz)

    return h


def test_simple_fill():
    h = make_hist()

    # expect one count per bin
    ones = np.ones((1, 1, nbins))
    values = h.values(flow=False)
    assert ak.all(values == ak.Array(ones))

    # expect one count per bin, plus 0s for overflow
    ones_with_flow = np.zeros((1, 1, nbins + 2))
    ones_with_flow[0, 0, 1:-1] += ones[0, 0, :]
    values = h.values(flow=True)
    assert ak.all(values == ones_with_flow)


def test_index():
    h = make_hist()
    h.fill(process="ttH", channel="ch1", ptz=data_ptz * 0.5)
    ho = make_hist()

    # on channel="ch0", counts should be the same pre/post second fill
    assert ho["ttH", "ch0", 0] == 1
    assert h["ttH", "ch0", 0] == 1
    assert ho["ttH", "ch0", -1] == 1
    assert h["ttH", "ch0", -1] == 1

    # same thing, but using indices
    assert ho[0, 0, 0] == 1
    assert h[0, 0, 0] == 1
    assert ho[0, 0, -1] == 1
    assert h[0, 0, -1] == 1

    # on channel="ch1", fill data was scaled down
    with pytest.raises(KeyError):
        ho["ttH", "ch1", 0] == 0

    assert h["ttH", "ch1", 0] == 2
    assert h["ttH", "ch1", -1] == 0

    # same thing but using dictionaries
    assert h[{"process": "ttH", "channel": "ch1", "ptz": 0}] == 2
    assert h[{"process": "ttH", "channel": "ch1", "ptz": -1}] == 0

    # same thing but using j
    assert h[{"process": "ttH", "channel": "ch1", "ptz": 0j}] == 2
    assert h[{"process": "ttH", "channel": "ch1", "ptz": data_ptz[-1] * 1j}] == 0

    # i.e.: h["ttH", "ch0", 0] + h["ttH", "ch1", 0]
    assert h[{"channel": sum}][0, 0] == 3

    assert ak.all(
        ho[{}].values()[0, 0] == h[{"process": "ttH", "channel": "ch0"}].values()
    )


def test_integrate():
    h = make_hist()
    r1 = h.integrate("channel", "ch0").values()

    h.fill(process="ttH", channel="ch1", ptz=data_ptz * 2)

    r2 = h.integrate("channel", "ch0").values()
    r3 = h.integrate("channel", "ch1").values()

    assert ak.all(r1 == r2)
    assert ak.any(r2 != r3)
    assert ak.sum(h.values()) == ak.sum(r2 + r3)


def test_slice():
    h = make_hist()
    h.fill(process="ttH", channel="ch1", ptz=data_ptz * 0.5)

    h0 = h[{"channel": "ch0"}]
    h1 = h[{"channel": "ch1"}]

    assert ak.all(h.values()[:, 0, :] == h0.values())
    assert ak.all(h.values()[:, 1, :] == h1.values())
    assert ak.sum(h.values()) == ak.sum(h0.values()) + ak.sum(h1.values())


def test_remove():
    h = make_hist()
    h.fill(process="ttH", channel="ch1", ptz=data_ptz * 0.5)

    ha = h[{"channel": ["ch1"]}]

    hr = h.remove("channel", ["ch0"])

    assert ak.all(hr.values() == ha.values())


def test_flow():
    h = make_hist()

    flowed = np.array([-10000, -10000, -10000, 10000, 10000, 10000, 10000])
    h.fill(process="ttH", channel="ch0", ptz=flowed)

    # expect one count per bin, plus the overflow
    ones_with_flow = np.ones((1, 1, nbins + 2))
    ones_with_flow[0, 0, 0] = np.count_nonzero(flowed < 0)
    ones_with_flow[0, 0, -1] = np.count_nonzero(flowed > 1000)

    values = h.values(flow=True)
    assert ak.all(values == ones_with_flow)


def test_addition():
    h = make_hist()

    flowed = np.array([-10000, -10000, -10000, 10000, 10000, 10000, 10000])
    h.fill(process="ttH", channel="ch0", ptz=flowed)

    values = h.values(flow=True)
    values2 = values * 2

    h2 = h + h
    assert ak.all(h2.values(flow=True) == values2)


def test_scale():
    h = make_hist()
    values = h.values(flow=True)

    h *= 3
    h12 = 4 * h
    values12 = values * 12

    assert ak.all(h12.values(flow=True) == values12)


def test_pickle():
    h = make_hist()

    x = pickle.dumps(h)
    h2 = pickle.loads(x)

    assert ak.all(h.values(flow=True) == h2.values(flow=True))


def test_assignment():
    h = make_hist()
    hs = h * 2
    h2 = h.empty_from_axes()

    for k, vs in h.view(as_dict=True, flow=True).items():
        h2[k] = vs + vs

    assert np.all(np.abs(hs.values(flow=True) - h2.values(flow=True) < 1e-10))

    # same as above but one bin at a time
    h2b = h.empty_from_axes()
    for k, vs in h.view(as_dict=True, flow=False).items():
        for i, v in enumerate(vs):
            h2b[(*k, i)] = v + v
        h2b[(*k, hist.underflow)] = h[(*k, hist.underflow)]
        h2b[(*k, hist.overflow)] = h[(*k, hist.overflow)]

    assert np.all(np.abs(hs.values(flow=True) - h2b.values(flow=True) < 1e-10))
