"""Combined numerical storage and upstream raw-count contract."""

import copy
import gzip
import pickle

import cloudpickle
import hist
import numpy as np
import pytest
from coffea.processor.accumulator import accumulate

from topcoffea.modules import eft_helper
from topcoffea.modules.histEFT import HistEFT
from topcoffea.modules.sparseHist import SparseHist


X = np.array([-1., .25, .25, 1.25, 3.])
WEIGHTS = np.array([2., -3., 0., 4., -2.])
COEFFICIENTS = np.array([[1.5, 2, 3], [-2, 4, 5], [7, 8, 9], [0, 6, 2], [-.5, 3, 1]])
RAW = np.array([1, 2, 1, 1], dtype=np.uint64)
KEY = ("signal", "nominal")
LAYOUT_FIELDS = ("_use_multicell", "_store_sumw2", "_yield_slice", "_sumw2_index", "_cell_count")


def make(*, tracked=False, embedded=True, multicell=True, wcs=("ctG",), flow=True):
    return HistEFT(
        hist.axis.StrCategory([], name="process", growth=True),
        hist.axis.StrCategory([], name="systematic", growth=True),
        hist.axis.Regular(2, 0., 2., name="x", flow=flow),
        wc_names=list(wcs), use_multicell=multicell, store_sumw2=embedded,
        track_raw_counts=tracked, label="Events",
    )


def fill(h, *, coefficients=COEFFICIENTS, fill_sumw2=True, systematic="nominal", **extra):
    kwargs = dict(process="signal", systematic=systematic, x=X, weight=WEIGHTS,
                  eft_coeff=coefficients, fill_sumw2=fill_sumw2)
    if h.track_raw_counts:
        kwargs["record_raw_count"] = True
    kwargs.update(extra)
    h.fill(**kwargs)
    return h


def binned(weights):
    return np.array([weights[0], weights[1] + weights[2], weights[3], weights[4]])


def assert_raw(h, expected=RAW, key=KEY):
    raw = h.raw_counts(flow=True)[key]
    assert raw.dtype == np.uint64
    np.testing.assert_array_equal(raw, expected)


@pytest.mark.parametrize("tracked", [False, True])
@pytest.mark.parametrize("embedded", [False, True])
@pytest.mark.parametrize("fill_sumw2", [False, True])
@pytest.mark.parametrize("scalar", [False, True])
def test_independent_layout_fill_and_raw_count_flags(tracked, embedded, fill_sumw2, scalar):
    h = make(tracked=tracked, embedded=embedded, wcs=() if scalar else ("ctG",))
    coefficients = None if scalar else COEFFICIENTS
    contribution = WEIGHTS if scalar else WEIGHTS * COEFFICIENTS[:, 0]
    fill(h, coefficients=coefficients, fill_sumw2=fill_sumw2)
    np.testing.assert_array_equal(h.eval({})[KEY], binned(contribution))
    assert h.dense_axes.name == ("x",)
    if embedded:
        expected = binned(contribution**2) if fill_sumw2 else np.zeros(4)
        np.testing.assert_array_equal(h.nominal_sumw2(flow=True)[KEY], expected)
    else:
        with pytest.raises(RuntimeError, match="does not contain embedded"):
            h.nominal_sumw2()
    if tracked:
        assert_raw(h)
    else:
        assert not hasattr(h, "_raw_counts")
        with pytest.raises(RuntimeError, match="disabled"):
            h.raw_counts()


@pytest.mark.parametrize("tracked", [False, True])
def test_repeated_fills_cancellation_and_sumw2_gate(tracked):
    h = make(tracked=tracked)
    fill(h, x=np.array([.25, .25]), weight=np.array([2., 2.]),
         coefficients=np.array([[1.5, 9, 3], [-1.5, 2, 4]]))
    assert h.eval({})[KEY].sum() == 0
    assert h.nominal_sumw2(flow=True)[KEY].sum() == 18
    fill(h, x=np.array([.25]), weight=np.array([7.]), coefficients=None, fill_sumw2=False)
    assert h.eval({})[KEY].sum() == 7
    assert h.nominal_sumw2(flow=True)[KEY].sum() == 18
    if tracked:
        assert_raw(h, [0, 3, 0, 0])


@pytest.mark.parametrize("fill_sumw2", [False, True])
def test_raw_fill_classification_remains_explicit_and_weight_independent(fill_sumw2):
    h = make(tracked=True)
    with pytest.raises(RuntimeError, match="explicitly true or false"):
        h.fill(process="signal", systematic="nominal", x=X, weight=WEIGHTS,
               eft_coeff=COEFFICIENTS, fill_sumw2=fill_sumw2)
    assert h._raw_counts == {}
    fill(h, fill_sumw2=fill_sumw2, weight=np.zeros(5))
    assert_raw(h)
    assert h.eval({})[KEY].sum() == 0
    assert h.nominal_sumw2(flow=True)[KEY].sum() == 0
    fill(h, systematic="up", record_raw_count=False, fill_sumw2=fill_sumw2)
    assert ("signal", "up") not in h.raw_counts(flow=True)
    assert h._validated_raw_count_states()[h.categories_to_index(("signal", "up"))] is None
    with pytest.raises(RuntimeError, match="classification|unrecorded"):
        fill(h, systematic="up", record_raw_count=True)
    with pytest.raises(RuntimeError, match="disabled"):
        fill(make(), record_raw_count=True, fill_sumw2=fill_sumw2)


@pytest.mark.parametrize("tracked", [False, True])
@pytest.mark.parametrize("flow", [False, True])
def test_empty_fills_and_flow_bins(tracked, flow):
    h = make(tracked=tracked, flow=flow)
    fill(h, x=np.array([]), weight=np.array([]), coefficients=np.empty((0, 3)))
    assert h.eval({})[KEY].sum() == 0
    assert h.nominal_sumw2(flow=True)[KEY].sum() == 0
    if tracked:
        assert_raw(h, np.zeros(4 if flow else 2, dtype=np.uint64))
    fill(h)
    expected_yield = binned(WEIGHTS * COEFFICIENTS[:, 0])
    expected_variance = binned((WEIGHTS * COEFFICIENTS[:, 0])**2)
    if not flow:
        expected_yield = expected_yield[1:-1]
        expected_variance = expected_variance[1:-1]
    np.testing.assert_array_equal(h.eval({})[KEY], expected_yield)
    np.testing.assert_array_equal(h.nominal_sumw2(flow=True)[KEY], expected_variance)
    if tracked:
        assert_raw(h, RAW if flow else RAW[1:-1])


@pytest.mark.parametrize("tracked", [False, True])
@pytest.mark.parametrize("operation", ["add", "iadd", "accumulate", "sum"])
def test_addition_and_accumulation(tracked, operation):
    a, b = fill(make(tracked=tracked)), fill(make(tracked=tracked))
    if operation == "add":
        result = a + b
    elif operation == "iadd":
        result = a.copy()
        result += b
    elif operation == "sum":
        result = sum([a, b])
    else:
        result = accumulate([a, b])
    np.testing.assert_array_equal(result.eval({})[KEY], 2 * a.eval({})[KEY])
    np.testing.assert_array_equal(result.nominal_sumw2(flow=True)[KEY], 2 * a.nominal_sumw2(flow=True)[KEY])
    if tracked:
        assert_raw(result, 2 * RAW)
    # Accumulation must not mutate its inputs.
    np.testing.assert_array_equal(a.eval({})[KEY], binned(WEIGHTS * COEFFICIENTS[:, 0]))


@pytest.mark.parametrize("incompatibility", ["backend", "variance", "wcs", "bins", "tracking", "scalar"])
def test_incompatible_additions_rejected_before_numerical_mutation(incompatibility):
    left = fill(make(tracked=True))
    if incompatibility == "backend":
        right = fill(make(multicell=False, embedded=False, tracked=True))
    elif incompatibility == "variance":
        right = fill(make(embedded=False, tracked=True))
    elif incompatibility == "wcs":
        right = fill(make(wcs=("ctW",), tracked=True))
    elif incompatibility == "bins":
        right = HistEFT(*list(left.categorical_axes), hist.axis.Regular(3, 0, 3, name="x"),
                        wc_names=["ctG"], use_multicell=True, store_sumw2=True, track_raw_counts=True)
    elif incompatibility == "tracking":
        right = fill(make(tracked=False))
    else:
        right = 2.
    before = left.yield_coefficients(flow=True)[KEY].copy()
    variance = left.nominal_sumw2(flow=True)[KEY].copy()
    with pytest.raises((ValueError, TypeError, RuntimeError)):
        left += right
    np.testing.assert_array_equal(left.yield_coefficients(flow=True)[KEY], before)
    np.testing.assert_array_equal(left.nominal_sumw2(flow=True)[KEY], variance)
    assert_raw(left)


def test_raw_merge_state_and_overflow_guards_apply_to_multicell():
    left = fill(make(tracked=True))
    unrecorded = left.with_raw_counts_unrecorded()
    with pytest.raises(RuntimeError, match="recorded.*unrecorded"):
        left += unrecorded
    key = left.categories_to_index(KEY)
    left._raw_counts[key][1] = np.iinfo(np.uint64).max
    before = left.view(flow=True)[KEY].copy()
    with pytest.raises(OverflowError, match="overflow"):
        left += fill(make(tracked=True))
    np.testing.assert_array_equal(left.view(flow=True)[KEY], before)
    assert left._raw_counts[key][1] == np.iinfo(np.uint64).max


@pytest.mark.parametrize("tracked", [False, True])
@pytest.mark.parametrize("factor", [2., -3., 0.])
@pytest.mark.parametrize("operation", ["scale", "multiply", "divide"])
def test_scaling_yields_variance_and_raw_counts(tracked, factor, operation):
    h = fill(make(tracked=tracked))
    before = h.yield_coefficients(flow=True)[KEY].copy()
    variance = h.nominal_sumw2(flow=True)[KEY].copy()
    if operation == "divide":
        if factor == 0:
            with pytest.raises(ZeroDivisionError):
                h /= factor
            np.testing.assert_array_equal(h.yield_coefficients(flow=True)[KEY], before)
            return
        result, scale = h / factor, 1 / factor
    elif operation == "multiply":
        result, scale = factor * h, factor
    else:
        result, scale = h.scale(factor), factor
    np.testing.assert_allclose(result.yield_coefficients(flow=True)[KEY], scale * before)
    np.testing.assert_allclose(result.nominal_sumw2(flow=True)[KEY], scale**2 * variance)
    if tracked:
        assert_raw(result)


def test_embedded_subtraction_and_nonscalar_scaling_rejected():
    h = fill(make(tracked=True))
    with pytest.raises(NotImplementedError, match="Subtraction"):
        h -= h.copy()
    with pytest.raises(TypeError, match="real numeric scalar"):
        h *= np.array([2., 3.])
    assert_raw(h)


@pytest.mark.parametrize("tracked", [False, True])
@pytest.mark.parametrize("operation", ["copy", "deepcopy", "slice", "integrate", "group"])
def test_copy_and_categorical_operations_transport_all_state(tracked, operation):
    source = fill(make(tracked=tracked))
    if operation == "copy":
        result, key = source.copy(), KEY
    elif operation == "deepcopy":
        result, key = copy.deepcopy(source), KEY
    elif operation == "slice":
        result, key = source[{"process": "signal"}], ("nominal",)
    elif operation == "integrate":
        result, key = source.integrate("systematic", "nominal"), ("signal",)
    else:
        result, key = source.group("process", {"combined": ["signal"]}), ("combined", "nominal")
    np.testing.assert_array_equal(result.eval({})[key], source.eval({})[KEY])
    np.testing.assert_array_equal(result.nominal_sumw2(flow=True)[key], source.nominal_sumw2(flow=True)[KEY])
    if tracked:
        assert_raw(result, key=key)
    result.scale(-2)
    assert source.nominal_sumw2(flow=True)[KEY].sum() == 46
    if tracked:
        assert_raw(source)


def test_dense_slicing_retains_upstream_raw_count_restriction():
    tracked = fill(make(tracked=True))
    with pytest.raises(RuntimeError, match="Dense-axis selection"):
        tracked[{"x": slice(0, 1)}]
    untracked = fill(make())
    sliced = untracked[{"x": slice(0, 1)}]
    assert sliced.dense_axis.size == 1
    assert sliced.store_sumw2


@pytest.mark.parametrize("tracked", [False, True])
@pytest.mark.parametrize("point", [{}, {"ctG": 2.}, {"ctG": -1.}])
def test_evaluation_and_as_hist_are_scalar_and_do_not_mutate_counts(tracked, point):
    h = fill(make(tracked=tracked))
    wc = point.get("ctG", 0.)
    expected = binned(WEIGHTS * (COEFFICIENTS[:, 0] + wc * COEFFICIENTS[:, 1] + wc**2 * COEFFICIENTS[:, 2]))
    np.testing.assert_array_equal(h.eval(point)[KEY], expected)
    converted = h.as_hist(point)
    assert type(converted) is hist.Hist
    assert converted.storage_type is hist.storage.Double
    assert converted.axes.name == ("process", "systematic", "x")
    np.testing.assert_array_equal(converted[{"process": "signal", "systematic": "nominal"}].values(flow=True), expected)
    assert h.nominal_sumw2(flow=True)[KEY].sum() == 46
    if tracked:
        assert_raw(h)


def test_coefficient_remapping_and_make_scaling_use_only_yield_cells():
    remapped = eft_helper.remap_coeffs(["ctG"], ["ctW", "ctG"], COEFFICIENTS)
    a = fill(make(wcs=("ctW", "ctG")), coefficients=remapped)
    b = fill(make())
    np.testing.assert_array_equal(a.eval({"ctW": 10., "ctG": 2.})[KEY], b.eval({"ctG": 2.})[KEY])
    np.testing.assert_array_equal(a.nominal_sumw2(flow=True)[KEY], b.nominal_sumw2(flow=True)[KEY])
    # make_scaling keeps upstream multi-category support and WC remapping.
    positive = COEFFICIENTS.copy()
    positive[:, 0] = 2.
    legacy = fill(make(multicell=False, embedded=False), coefficients=positive)
    multicell = fill(make(), coefficients=positive)
    for h in (legacy, multicell):
        fill(h, coefficients=positive, process="other")
    for flow in ("show", "sum"):
        np.testing.assert_allclose(multicell.make_scaling(flow=flow, wc_list=["ctW", "ctG"]),
                                   legacy.make_scaling(flow=flow, wc_list=["ctW", "ctG"]))


@pytest.mark.parametrize("tracked", [False, True])
@pytest.mark.parametrize("embedded", [False, True])
@pytest.mark.parametrize("serializer", [pickle, cloudpickle])
def test_gzip_serialization_preserves_layout_and_independent_states(tracked, embedded, serializer):
    source = fill(make(tracked=tracked, embedded=embedded), fill_sumw2=False)
    fill(source, systematic="up", record_raw_count=False if tracked else None)
    restored = serializer.loads(gzip.decompress(gzip.compress(serializer.dumps(source))))
    assert restored._use_multicell
    assert restored.store_sumw2 is embedded
    assert restored.track_raw_counts is tracked
    for key, coefficients in source.yield_coefficients(flow=True).items():
        np.testing.assert_array_equal(restored.yield_coefficients(flow=True)[key], coefficients)
        if embedded:
            np.testing.assert_array_equal(restored.nominal_sumw2(flow=True)[key], source.nominal_sumw2(flow=True)[key])
    if tracked:
        assert_raw(restored)
        assert ("signal", "up") not in restored.raw_counts(flow=True)
    fill(restored, fill_sumw2=True)
    if tracked:
        assert_raw(restored, 2 * RAW)


class HistoricalPayload:
    def __init__(self, source, arguments):
        self.source = source
        self.arguments = arguments

    def __reduce__(self):
        return (HistEFT._read_from_reduce,
                (list(self.source.categorical_axes), [self.source.dense_axis],
                 self.arguments, self.source._dense_hists))


@pytest.mark.parametrize("multicell,embedded", [(False, False), (True, False), (True, True)])
@pytest.mark.parametrize("serializer", [pickle, cloudpickle])
def test_historical_reducers_without_new_flags(multicell, embedded, serializer):
    source = fill(make(multicell=multicell, embedded=embedded))
    arguments = dict(source._init_args, wc_names=source.wc_names)
    # Old reducers predate both store_sumw2 and track_raw_counts.
    restored = serializer.loads(serializer.dumps(HistoricalPayload(source, arguments)))
    assert restored._use_multicell is multicell
    assert restored.store_sumw2 is embedded
    assert not restored.track_raw_counts
    np.testing.assert_array_equal(restored.eval({})[KEY], source.eval({})[KEY])
    if embedded:
        np.testing.assert_array_equal(restored.nominal_sumw2(flow=True)[KEY], source.nominal_sumw2(flow=True)[KEY])
    fill(restored)
    assert restored.eval({})[KEY].sum() == 2 * source.eval({})[KEY].sum()


@pytest.mark.parametrize("multicell,embedded", [(False, False), (True, False), (True, True)])
def test_historical_live_state_without_layout_or_raw_count_attributes(multicell, embedded):
    source = fill(make(multicell=multicell, embedded=embedded))
    for name in (*LAYOUT_FIELDS, "_track_raw_counts"):
        source.__dict__.pop(name, None)
    source._init_args_eft = {"wc_names": source.wc_names}
    restored = pickle.loads(pickle.dumps(source))
    assert restored._use_multicell is multicell
    assert restored.store_sumw2 is embedded
    assert not restored.track_raw_counts
    np.testing.assert_array_equal(restored.eval({})[KEY], binned(WEIGHTS * COEFFICIENTS[:, 0]))
    assert restored.copy()._use_multicell is multicell


@pytest.mark.parametrize("corruption", ["flags", "cells", "payload", "raw"])
def test_historical_inconsistent_layouts_fail_closed(corruption):
    source = fill(make())
    args = dict(source._init_args, wc_names=source.wc_names)
    if corruption == "flags":
        args["use_multicell"] = False
    elif corruption == "cells":
        args["storage"] = hist.storage.MultiCell(9)
    elif corruption == "payload":
        args["storage"] = hist.storage.MultiCell(3)
    else:
        args["track_raw_counts"] = True
    with pytest.raises((ValueError, RuntimeError), match="layout|storage|raw-count"):
        pickle.loads(pickle.dumps(HistoricalPayload(source, args)))


def test_upstream_default_layout_and_raw_count_api_are_unchanged():
    h = HistEFT(hist.axis.StrCategory([], name="process", growth=True),
                hist.axis.Regular(2, 0, 2, name="x"), wc_names=["ctG"], track_raw_counts=True)
    assert not h._use_multicell
    assert not h.store_sumw2
    assert h.dense_axes.name == ("x", "quadratic_term")
    h.fill(None, True, process="signal", x=np.array([.5]), weight=np.array([-2.]))
    np.testing.assert_array_equal(h.raw_counts(flow=True)[("signal",)], [0, 1, 0, 0])
