import pickle

import cloudpickle
import hist
import numpy as np
import pytest

from topcoffea.modules.histEFT import HistEFT


KEY = ("signal", "central")
X = np.array([-1., .25, .25, 1.25, 3.25, 5.])
COEFFS = np.array([[1, 2, 3], [2, 3, 4], [-2, 5, 6],
                   [0, 1, 2], [1, 2, 1], [3, 4, 5]])
WEIGHTS = np.array([2., 3., 3., 0., -2., .5])
SM = np.array([2., 0., 0., 0., -2., 1.5])
SUMW2 = np.array([4., 72., 0., 0., 4., 2.25])
RAW = np.array([1, 2, 1, 0, 1, 1], dtype=np.uint64)


def make(*, multicell=True, sumw2=True, tracked=True):
    return HistEFT(
        hist.axis.StrCategory([], name="process", growth=True),
        hist.axis.StrCategory([], name="systematic", growth=True),
        hist.axis.Regular(4, 0, 4, name="x"),
        wc_names=["ctG"], use_multicell=multicell, store_sumw2=sumw2,
        track_raw_counts=tracked,
    )


def fill(h, **overrides):
    values = dict(process=KEY[0], systematic=KEY[1], x=X,
                  eft_coeff=COEFFS, weight=WEIGHTS)
    if h.track_raw_counts:
        values["record_raw_count"] = True
    values.update(overrides)
    h.fill(**values)
    return h


def test_multicell_fill_matches_legacy_and_weights_are_applied_once():
    legacy = fill(make(multicell=False, sumw2=False))
    h = fill(make())
    yield_only = fill(make(sumw2=False))
    assert h.store_sumw2 is True
    assert legacy.store_sumw2 is False
    assert yield_only.store_sumw2 is False
    with pytest.raises(AttributeError):
        h.store_sumw2 = False
    np.testing.assert_array_equal(h.eval({})[KEY], SM)
    np.testing.assert_array_equal(h.nominal_sumw2(flow=True)[KEY], SUMW2)
    np.testing.assert_array_equal(h.nominal_sumw2()[KEY], SUMW2[1:-1])
    for source in (legacy, h, yield_only):
        np.testing.assert_array_equal(source.eval({"ctG": 1})[KEY],
                                      [12, 54, 0, 0, -8, 6])
    assert legacy.dense_axes.name == ("x", "quadratic_term")
    assert h.dense_axes.name == ("x",)
    scalar = h.as_hist({"ctG": 1})
    assert scalar.storage_type is hist.storage.Double
    np.testing.assert_array_equal(scalar[dict(zip(h.categorical_axes.name, KEY))]
                                  .values(flow=True), h.eval({"ctG": 1})[KEY])
    # Already weighted coefficients are equivalent when weight is omitted.
    preweighted = fill(make(), eft_coeff=COEFFS * WEIGHTS[:, None], weight=None)
    np.testing.assert_array_equal(preweighted.view(flow=True)[KEY], h.view(flow=True)[KEY])
    default = fill(make(), x=np.array([.25]), eft_coeff=None, weight=-2.)
    np.testing.assert_array_equal(default.eval({})[KEY], [0, -2, 0, 0, 0, 0])
    np.testing.assert_array_equal(default.nominal_sumw2(flow=True)[KEY], [0, 4, 0, 0, 0, 0])
    fill(default, x=np.array([]), eft_coeff=np.empty((0, 3)), weight=np.array([]))
    assert default.nominal_sumw2(flow=True)[KEY].sum() == 4


def test_multicell_nominal_gate_preserves_raw_count_semantics():
    h = fill(make())
    np.testing.assert_array_equal(h.raw_counts(flow=True)[KEY], RAW)
    fill(h, systematic="variation", fill_sumw2=False, record_raw_count=False)
    varied = (KEY[0], "variation")
    np.testing.assert_array_equal(h.eval({})[varied], SM)
    np.testing.assert_array_equal(h.nominal_sumw2(flow=True)[varied], np.zeros(6))
    assert varied not in h.raw_counts(flow=True)
    assert h._validated_raw_count_states()[h.categories_to_index(varied)] is None
    with pytest.raises(RuntimeError, match="explicitly true or false"):
        fill(h, record_raw_count=None)
    with pytest.raises(RuntimeError, match="classification|unrecorded"):
        fill(h, systematic="variation", record_raw_count=True)
    with pytest.raises(RuntimeError, match="Dense-axis selection"):
        h[{"x": slice(None, None, hist.rebin(2))}]


def test_multicell_accumulation_and_scalar_scaling():
    source = fill(make())
    accumulated = sum([source, source])
    accumulated += source
    np.testing.assert_array_equal(accumulated.eval({})[KEY], 3 * SM)
    np.testing.assert_array_equal(accumulated.nominal_sumw2(flow=True)[KEY], 3 * SUMW2)
    scaled = (-2 * accumulated) / 4
    np.testing.assert_array_equal(scaled.eval({})[KEY], -1.5 * SM)
    np.testing.assert_array_equal(scaled.nominal_sumw2(flow=True)[KEY], .75 * SUMW2)
    np.testing.assert_array_equal(scaled.raw_counts(flow=True)[KEY], 3 * RAW)
    np.testing.assert_array_equal(source.nominal_sumw2(flow=True)[KEY], SUMW2)
    with pytest.raises(ValueError, match="matching EFT storage"):
        accumulated += fill(make(sumw2=False))
    with pytest.raises(NotImplementedError, match="Subtraction"):
        accumulated -= source
    with pytest.raises(ZeroDivisionError):
        scaled /= 0
    scaled.scale(0)
    assert not np.any(scaled.view(flow=True)[KEY])
    np.testing.assert_array_equal(scaled.raw_counts(flow=True)[KEY], 3 * RAW)


def test_multicell_serialization_and_categorical_transforms():
    source = fill(make())
    fill(source, systematic="variation", fill_sumw2=False, record_raw_count=False)
    for serializer in (pickle, cloudpickle):
        restored = serializer.loads(serializer.dumps(source))
        np.testing.assert_array_equal(restored.view(flow=True)[KEY], source.view(flow=True)[KEY])
        np.testing.assert_array_equal(restored.raw_counts(flow=True)[KEY], RAW)
        varied = restored.categories_to_index((KEY[0], "variation"))
        assert restored._validated_raw_count_states()[varied] is None
        selected = restored.copy().group("process", {"all": [KEY[0]]})
        selected = selected.integrate("systematic", KEY[1])
        np.testing.assert_array_equal(selected.nominal_sumw2(flow=True)[("all",)], SUMW2)
        fill(restored)
        np.testing.assert_array_equal(restored.nominal_sumw2(flow=True)[KEY], 2 * SUMW2)
        np.testing.assert_array_equal(restored.raw_counts(flow=True)[KEY], 2 * RAW)


def test_multicell_untracked_rebinning_and_make_scaling():
    h = fill(make(tracked=False))
    rebinned = h[{"x": slice(None, None, hist.rebin(2))}]
    assert rebinned.dense_axis.size == 2
    np.testing.assert_array_equal(rebinned.eval({})[KEY], [2, 0, -2, 1.5])
    np.testing.assert_array_equal(rebinned.nominal_sumw2(flow=True)[KEY], [4, 72, 4, 2.25])
    legacy = make(multicell=False, sumw2=False, tracked=False)
    multicell = make(tracked=False)
    # Positive SM contributions avoid the existing undefined-scaling guard.
    coefficients = np.tile([2., 4., 6.], (len(X), 1))
    fill(legacy, eft_coeff=coefficients, weight=1.)
    fill(multicell, eft_coeff=coefficients, weight=1.)
    np.testing.assert_allclose(multicell.make_scaling(flow="sum", wc_list=["ctW", "ctG"]),
                               legacy.make_scaling(flow="sum", wc_list=["ctW", "ctG"]))
