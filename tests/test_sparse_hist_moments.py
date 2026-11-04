"""Independent Weight contributions, retaining the upstream raw-count contract."""

import copy
import gzip
import pickle

import cloudpickle
import hist
import numpy as np
import pytest

from topcoffea.modules.sparseHist import SparseHist


def make_hist(*, tracked=False, storage="Weight", axis=None):
    return SparseHist(
        hist.axis.StrCategory([], name="process", growth=True),
        hist.axis.StrCategory([], name="systematic", growth=True),
        hist.axis.Regular(2, 0, 2, name="x") if axis is None else axis,
        storage=storage,
        track_raw_counts=tracked,
    )


def fill(h, x, w, v, *, process="a", systematic="nominal", **kwargs):
    return h.fill_with_moments(
        process=process, systematic=systematic, x=x,
        value_weight=w, second_moment=v, **kwargs,
    )


def view(h, process="a", systematic="nominal"):
    return h.view(flow=True)[(process, systematic)]


@pytest.mark.parametrize("storage", ["Weight", hist.storage.Weight()])
def test_ordinary_fill_and_explicit_repeated_fills(storage):
    h = make_hist(storage=storage)
    h.fill(process="a", systematic="nominal", x=[0.2, 0.2], weight=[-2, 3])
    np.testing.assert_array_equal(view(h).value, [0, 1, 0, 0])
    np.testing.assert_array_equal(view(h).variance, [0, 13, 0, 0])
    assert fill(h, [0.2, 0.2, 1.2], [-4, 2, -5], [7, 0, 11]) is h
    fill(h, 0.2, 3, 19)
    np.testing.assert_array_equal(view(h).value, [0, 2, -5, 0])
    np.testing.assert_array_equal(view(h).variance, [0, 39, 11, 0])


@pytest.mark.parametrize("tracked", [False, True])
def test_flow_categories_empty_fills_and_zero_moments(tracked):
    h = make_hist(tracked=tracked)
    record = {"record_raw_count": True} if tracked else {}
    fill(h, [-1, 0.2, 0.2, 1.2, 3], [-2, 3, -3, 0, 7], [4, 0, 0, 0, 8], **record)
    fill(h, [0.2], 9, 0, process="b", **record)
    fill(h, [], [], [], process="empty", **record)
    np.testing.assert_array_equal(view(h).value, [-2, 0, 0, 7])
    np.testing.assert_array_equal(view(h).variance, [4, 0, 0, 8])
    np.testing.assert_array_equal(view(h, "b").value, [0, 9, 0, 0])
    np.testing.assert_array_equal(view(h, "b").variance, np.zeros(4))
    np.testing.assert_array_equal(view(h, "empty").value, np.zeros(4))
    if tracked:
        raw = h.raw_counts(flow=True)
        np.testing.assert_array_equal(raw[("a", "nominal")], [1, 2, 1, 1])
        np.testing.assert_array_equal(raw[("empty", "nominal")], np.zeros(4))
        assert raw[("a", "nominal")].dtype == np.uint64
    else:
        assert not hasattr(h, "_raw_counts")
        with pytest.raises(RuntimeError, match="disabled"):
            h.raw_counts()


def test_broadcasting_grid_counts_events_independently_of_contributions():
    h = make_hist(tracked=True)
    fill(h, [[0.2], [1.2]], [0, -2, 4], 0, record_raw_count=True)
    np.testing.assert_array_equal(view(h).value, [0, 2, 2, 0])
    np.testing.assert_array_equal(view(h).variance, np.zeros(4))
    np.testing.assert_array_equal(h.raw_counts(flow=True)[("a", "nominal")], [0, 3, 3, 0])


def test_multidimensional_dense_broadcast_and_flow():
    axes = [hist.axis.Regular(2, 0, 2, name="x"),
            hist.axis.Variable([0, 1, 3], name="y")]
    h = SparseHist(hist.axis.StrCategory([], name="process", growth=True),
                   *axes, storage="Weight")
    x = np.array([[-1], [0.2], [3]])
    y = np.array([-1, 0.2, 2, 4])
    w = np.array([-2, 0, 3, 4])
    v = np.array([7, 0, 1, 12])
    h.fill_with_moments(process="a", x=x, y=y, value_weight=w, second_moment=v)
    result = h.view(flow=True)[("a",)]
    expected_values = np.zeros((4, 4))
    expected_variance = np.zeros((4, 4))
    expected_values[[0, 1, 3], :] = w
    expected_variance[[0, 1, 3], :] = v
    np.testing.assert_array_equal(result.value, expected_values)
    np.testing.assert_array_equal(result.variance, expected_variance)
    with pytest.raises(ValueError, match="exactly one physical dense axis"):
        SparseHist(*axes, storage="Weight", track_raw_counts=True)


@pytest.mark.parametrize("axis", [
    hist.axis.Regular(2, 0, 2, name="x", underflow=False, overflow=False),
    hist.axis.Regular(2, 1, 100, name="x", transform=hist.axis.transform.log),
    hist.axis.Regular(2, 0, 2, name="x", growth=True),
])
def test_backend_axis_binning_and_growth(axis):
    h = make_hist(axis=axis)
    reference_value = hist.Hist(axis, storage="Double")
    reference_moment = hist.Hist(axis, storage="Double")
    for x, w, v in [([-2, 0.5, 1.5, 4], [-3, 2, 0, 5], [8, 0, 1, 7]),
                    ([8, 30], [-2, 1], [9, 2])]:
        fill(h, x, w, v)
        reference_value.fill(x=x, weight=w)
        reference_moment.fill(x=x, weight=v)
    np.testing.assert_array_equal(view(h).value, reference_value.values(flow=True))
    np.testing.assert_array_equal(view(h).variance, reference_moment.values(flow=True))


def test_raw_count_classification_and_failed_fill_preserve_numerical_payload():
    h = make_hist(tracked=True)
    with pytest.raises(RuntimeError, match="explicitly true or false"):
        fill(h, 0.2, -2, 0)
    with pytest.raises(TypeError, match="record_raw_count"):
        fill(h, 0.2, -2, 0, record_raw_count=1)
    assert not h._dense_hists
    fill(h, [0.2, 0.2], [-2, 0], [9, 0], record_raw_count=True)
    fill(h, 0.2, -3, 0, systematic="up", record_raw_count=False)
    assert set(h.raw_counts()) == {("a", "nominal")}
    key = h.categories_to_index(("a", "up"))
    assert h._raw_counts[key] is None
    before = view(h).copy()
    with pytest.raises(RuntimeError):
        fill(h, 0.2, 1, 1, record_raw_count=False)
    np.testing.assert_array_equal(view(h), before)
    key = h.categories_to_index(("a", "nominal"))
    h._raw_counts[key][1] = np.iinfo(np.uint64).max
    with pytest.raises(OverflowError, match="overflow"):
        fill(h, 0.2, 1, 1, record_raw_count=True)
    np.testing.assert_array_equal(view(h), before)
    with pytest.raises(RuntimeError, match="disabled"):
        fill(make_hist(), 0.2, 1, 1, record_raw_count=True)


@pytest.mark.parametrize("tracked", [False, True])
def test_addition_copy_slice_group_and_scaling(tracked):
    left, right = make_hist(tracked=tracked), make_hist(tracked=tracked)
    record = {"record_raw_count": True} if tracked else {}
    fill(left, [0.2, 0.2], [-3, 2], [8, 1], **record)
    fill(right, [0.2, 1.2], [4, -2], [0, 7], **record)
    fill(right, 1.2, 5, 13, process="b", **record)
    merged = left + right
    np.testing.assert_array_equal(view(merged).value, [0, 3, -2, 0])
    np.testing.assert_array_equal(view(merged).variance, [0, 9, 7, 0])
    inplace = left.copy()
    inplace += right
    np.testing.assert_array_equal(view(inplace), view(merged))
    np.testing.assert_array_equal(view(left).value, [0, -1, 0, 0])
    for selected in [merged[{"process": ["a"]}], copy.deepcopy(merged), merged.copy()]:
        np.testing.assert_array_equal(view(selected), view(merged))
        if tracked:
            np.testing.assert_array_equal(selected.raw_counts(flow=True)[("a", "nominal")], [0, 3, 1, 0])
    grouped = merged.group("process", {"all": ["a", "b"]})
    np.testing.assert_array_equal(view(grouped, "all").value, [0, 3, 3, 0])
    np.testing.assert_array_equal(view(grouped, "all").variance, [0, 9, 20, 0])
    for factor in [2, -3, 0]:
        scaled = merged.copy().scale(factor)
        np.testing.assert_array_equal(view(scaled).value, view(merged).value * factor)
        np.testing.assert_array_equal(view(scaled).variance, view(merged).variance * factor**2)
        if tracked:
            np.testing.assert_array_equal(scaled.raw_counts(flow=True)[("a", "nominal")], [0, 3, 1, 0])
    divided = merged / -2
    np.testing.assert_array_equal(view(divided).variance, view(merged).variance / 4)
    if tracked:
        with pytest.raises(RuntimeError, match="Dense-axis selection"):
            merged[{"x": slice(0, 1)}]
    else:
        sliced = merged[{"x": slice(0, 1)}]
        np.testing.assert_array_equal(sliced.view()[("a", "nominal")].value, [3])
        np.testing.assert_array_equal(sliced.view()[("a", "nominal")].variance, [9])


@pytest.mark.parametrize("tracked", [False, True])
@pytest.mark.parametrize("serializer", [pickle, cloudpickle])
def test_gzip_serialization(tmp_path, tracked, serializer):
    h = make_hist(tracked=tracked)
    record = {"record_raw_count": True} if tracked else {}
    fill(h, [-1, 0.2, 1.2, 3], [-3, 2, 0, 7], [8, 0, 1, 13], **record)
    filename = tmp_path / "hist.pkl.gz"
    with gzip.open(filename, "wb") as stream:
        serializer.dump(h, stream)
    with gzip.open(filename, "rb") as stream:
        restored = serializer.load(stream)
    assert restored.track_raw_counts is tracked
    np.testing.assert_array_equal(view(restored), view(h))
    fill(restored, 0.2, 4, 9, **record)
    assert view(restored).value[1] == 6
    assert view(restored).variance[1] == 9
    if tracked:
        np.testing.assert_array_equal(restored.raw_counts(flow=True)[("a", "nominal")], [1, 2, 1, 1])


def test_historical_weight_reducer_and_default_double_fill():
    historical = make_hist()
    historical.fill(process="a", systematic="nominal", x=[0.2], weight=[-2])
    del historical._track_raw_counts
    restore, state = historical.__reduce__()
    assert len(state) == 4
    restored = restore(*state)
    assert not restored.track_raw_counts
    assert not hasattr(restored, "_raw_counts")
    fill(restored, 0.2, 3, 11)
    assert view(restored).value[1] == 1
    assert view(restored).variance[1] == 15
    double = SparseHist(hist.axis.Regular(2, 0, 2, name="x"))
    double.fill(x=[0.2, 0.2], weight=[-2, 3])
    np.testing.assert_array_equal(double.view()[()], [1, 0])
    with pytest.raises(TypeError, match="Weight storage"):
        double.fill_with_moments(x=0.2, value_weight=1, second_moment=2)


@pytest.mark.parametrize("storage", ["Double", "Int64", "Mean", hist.storage.MultiCell(2)])
def test_nonweight_storage_rejected(storage):
    h = make_hist(storage=storage)
    with pytest.raises(TypeError, match="Weight storage"):
        fill(h, 0.2, -2, 3)
    assert not h._dense_hists


@pytest.mark.parametrize("moment", [-1, -1e-30, np.nan, np.inf, -np.inf, [1, -1]])
def test_invalid_second_moment_rejected_before_mutation(moment):
    h = make_hist()
    with pytest.raises(ValueError, match="finite and nonnegative"):
        fill(h, 0.2, 2, moment)
    assert not h._dense_hists
    assert len(h.axes["process"]) == 0


@pytest.mark.parametrize("x,w,v", [([0.2, 1.2], [1, 2, 3], 0),
                                     ([0.2, 1.2], 1, [1, 2, 3])])
def test_incompatible_shapes_rejected(x, w, v):
    h = make_hist()
    with pytest.raises(ValueError, match="broadcast-compatible shapes"):
        fill(h, x, w, v)
    assert not h._dense_hists


@pytest.mark.parametrize("w,v", [(1, 1j), (1j, 1), (1, "3"), (1, None)])
def test_nonreal_contributions_rejected(w, v):
    with pytest.raises(TypeError, match="real numeric"):
        fill(make_hist(), 0.2, w, v)


def test_misnamed_weight_and_vector_sparse_labels_rejected():
    with pytest.raises(ValueError, match="Unknown fill axis"):
        fill(make_hist(), 0.2, 1, 2, weight=3)
    with pytest.raises(ValueError, match="scalar labels"):
        fill(make_hist(), 0.2, 1, 2, process=["a", "b"])


@pytest.mark.parametrize("reverse", [False, True])
@pytest.mark.parametrize("materialized", [False, True])
def test_incompatible_storage_addition_rejected(reverse, materialized):
    weight, double = make_hist(), make_hist(storage="Double")
    if materialized:
        fill(weight, 0.2, -2, 7)
        double.fill(process="a", systematic="nominal", x=0.2, weight=3)
    left, right = (double, weight) if reverse else (weight, double)
    with pytest.raises(ValueError, match="matching Weight storage"):
        left += right
    if materialized:
        assert view(weight).value[1] == -2
        assert view(weight).variance[1] == 7


def test_incompatible_layout_and_raw_tracking_addition_rejected():
    h = make_hist()
    fill(h, 0.2, -2, 7)
    with pytest.raises(ValueError, match="compatible dense axes"):
        h += make_hist(axis=hist.axis.Regular(3, 0, 2, name="x"))
    with pytest.raises(ValueError, match="matching categorical axes"):
        h += SparseHist(hist.axis.StrCategory([], name="other", growth=True),
                        hist.axis.Regular(2, 0, 2, name="x"), storage="Weight")
    with pytest.raises(RuntimeError, match="untracked and tracked"):
        h += make_hist(tracked=True)
    assert view(h).value[1] == -2
    assert view(h).variance[1] == 7
