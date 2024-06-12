from topcoffea.modules.sparseHist import SparseHist
import dask
import hist.dask as dah

import numpy as np
import awkward as ak
import pickle

import pytest


nbins = 12
data_ptz = dask.array.arange(0, 600, 600 / nbins)
data_ptz_b = dask.array.random.random_integers(low=100, high=500, size=1200)


def make_hist():
    h = SparseHist(["process", "channel"], dense_axes=[dah.Hist.new.Reg(nbins, 0, 600, name="ptz")])
    h.fill(process="ttH", channel="ch0", ptz=data_ptz)

    return h


def test_simple_fill():
    h = make_hist()

    (output_h,) = dask.compute(h)

    ones = np.ones((1, 1, nbins))
    values = output_h.values(flow=False)

    assert ak.all(values == ones)

    # expect one count per bin, plus 0s for overflow
    ones_with_flow = np.zeros((1, 1, nbins + 2))
    ones_with_flow[0, 0, 1:-1] += ones[0, 0, :]
    values = output_h.values(flow=True)

    assert ak.all(values == ones_with_flow)


def test_index():
    h = make_hist()
    h.fill(process="ttH", channel="ch1", ptz=data_ptz * 0.5)
    ho = make_hist()
    (output_h, output_ho,) = dask.compute(h, ho)

    # on channel="ch0", counts should be the same pre/post second fill
    assert output_ho["ttH", "ch0", 0] == 1
    assert output_h["ttH", "ch0", 0] == 1
    assert output_ho["ttH", "ch0", -1] == 1
    assert output_h["ttH", "ch0", -1] == 1

    # on channel="ch1", fill data was scaled down
    with pytest.raises(KeyError):
        output_ho["ttH", "ch1", 0] == 0

    assert output_h["ttH", "ch1", 0] == 2
    assert output_h["ttH", "ch1", -1] == 0

    # same thing but using dictionaries
    assert output_h[{"process": "ttH", "channel": "ch1", "ptz": 0}] == 2
    assert output_h[{"process": "ttH", "channel": "ch1", "ptz": -1}] == 0

    for key in output_ho.categorical_keys:
        assert ak.all(
            output_ho[key].values() == output_h[key].values()
        )


def test_integrate():
    h = make_hist()
    h.fill(process="ttH", channel="ch1", ptz=data_ptz * 2)

    (output_h,) = dask.compute(h)

    r1 = output_h.integrate("channel", "ch0").values()
    r2 = output_h.integrate("channel", "ch0").values()
    r3 = output_h.integrate("channel", "ch1").values()

    assert ak.sum(output_h.values()) == ak.sum(r2 + r3)
    assert ak.all(r1 == r2)
    assert ak.any(r2 != r3)


def test_remove():
    h = make_hist()
    ho = make_hist()
    ho.fill(process="ttH", channel="ch1", ptz=data_ptz * 0.5)
    (output_h, output_ho,) = dask.compute(h, ho)

    removed = output_ho.remove("channel", ["ch1"])

    assert ak.sum(output_h.values()) != ak.sum(output_ho.values())
    assert ak.all(output_h.values() == removed.values())


def test_flow():
    h = make_hist()

    flowed = np.array([-10000, -10000, -10000, 10000, 10000, 10000, 10000])
    h.fill(process="ttH", channel="ch0", ptz=flowed)

    (output_h,) = dask.compute(h)

    # expect one count per bin, plus the overflow
    ones_with_flow = np.ones((1, 1, nbins + 2))
    ones_with_flow[0, 0, 0] = np.count_nonzero(flowed < 0)
    ones_with_flow[0, 0, -1] = np.count_nonzero(flowed > 1000)

    values = output_h.values(flow=True)
    assert ak.all(values == ones_with_flow)


def test_addition():
    h = make_hist()

    flowed = np.array([-10000, -10000, -10000, 10000, 10000, 10000, 10000])
    h.fill(process="ttH", channel="ch0", ptz=flowed)

    (output_h,) = dask.compute(h)

    values = output_h.values(flow=True)
    values2 = values * 2

    h2 = output_h + output_h
    assert ak.all(h2.values(flow=True) == values2)


def test_scale():
    h = make_hist()
    (output_h,) = dask.compute(h)

    values = output_h.values(flow=True)

    output_h *= 3
    h12 = 4 * output_h
    values12 = values * 12

    assert ak.all(h12.values(flow=True) == values12)


def test_pickle():
    h = make_hist()
    (output_h,) = dask.compute(h)

    x = pickle.dumps(output_h)
    loaded_h = pickle.loads(x)

    assert ak.all(output_h.values(flow=True) == loaded_h.values(flow=True))


def test_assignment():
    h = make_hist()
    (output_h,) = dask.compute(h)

    h2 = output_h.empty_from_axes()
    for k in output_h.categorical_keys:
        h2[k] = output_h[k]

    assert np.all(np.abs(output_h.values(flow=True) - h2.values(flow=True) < 1e-10))
