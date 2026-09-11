import pickle

import cloudpickle
import hist
import numpy as np
import pytest

from topcoffea.modules.histEFT import HistEFT
from topcoffea.modules.sparseHist import SparseHist


def make_sparse(*, track_raw_counts=True):
    return SparseHist(
        hist.axis.StrCategory([], name="process", growth=True),
        hist.axis.StrCategory([], name="systematic", growth=True),
        hist.axis.Regular(2, 0.0, 2.0, name="x"),
        storage="Double",
        track_raw_counts=track_raw_counts,
    )


def only_raw(histogram, *, flow=True):
    values = histogram.raw_counts(flow=flow)
    assert len(values) == 1
    return next(iter(values.values()))


def fill_recorded(histogram, process, values, weights):
    histogram.fill(
        process=process,
        systematic="nominal",
        x=np.asarray(values),
        weight=np.asarray(weights),
        record_raw_count=True,
    )


def test_disabled_tracking_preserves_fill_and_serializes_no_raw_payload():
    histogram = make_sparse(track_raw_counts=False)
    histogram.fill(
        process="mc",
        systematic="nominal",
        x=np.asarray([0.25, 1.25]),
        weight=np.asarray([2.0, -3.0]),
    )

    assert histogram.track_raw_counts is False
    assert not hasattr(histogram, "_raw_counts")
    assert len(histogram.__reduce__()[1]) == 4
    with pytest.raises(RuntimeError, match="disabled"):
        histogram.raw_counts()

    restored = pickle.loads(pickle.dumps(histogram))
    assert restored.track_raw_counts is False
    assert not hasattr(restored, "_raw_counts")
    np.testing.assert_array_equal(
        next(iter(restored.view(flow=True, as_dict=True).values())),
        np.asarray([0.0, 2.0, -3.0, 0.0]),
    )


def test_enabled_fill_is_explicit_and_counts_weight_independent_events_with_flow():
    histogram = make_sparse()
    with pytest.raises(RuntimeError, match="explicitly true or false"):
        histogram.fill(
            process="mc",
            systematic="nominal",
            x=np.asarray([0.25]),
        )
    assert histogram._dense_hists == {}
    assert histogram._raw_counts == {}

    fill_recorded(
        histogram,
        "mc",
        [-1.0, 0.25, 0.25, 1.25, 3.0],
        [-100.0, 0.5, -2.0, 7.0, 0.0],
    )
    histogram.fill(
        process="mc",
        systematic="up",
        x=np.asarray([0.25]),
        weight=np.asarray([11.0]),
        record_raw_count=False,
    )

    raw = histogram.raw_counts(flow=True)
    assert set(raw) == {("mc", "nominal")}
    assert raw[("mc", "nominal")].dtype == np.dtype(np.uint64)
    np.testing.assert_array_equal(raw[("mc", "nominal")], [1, 2, 1, 1])
    np.testing.assert_array_equal(
        histogram.raw_counts(flow=False)[("mc", "nominal")],
        [2, 1],
    )
    assert histogram._raw_counts[histogram.categories_to_index(("mc", "up"))] is None


def test_zero_missing_and_malformed_raw_state_are_distinct():
    histogram = make_sparse()
    fill_recorded(histogram, "zero", [], [])
    np.testing.assert_array_equal(only_raw(histogram), np.zeros(4, dtype=np.uint64))

    key = next(iter(histogram._dense_hists))
    state = histogram._raw_counts.pop(key)
    with pytest.raises(RuntimeError, match="coverage is incomplete"):
        histogram.raw_counts()

    histogram._raw_counts[key] = state.astype(np.int64)
    with pytest.raises(TypeError, match="dtype uint64"):
        histogram.raw_counts()

    histogram._raw_counts[key] = np.zeros(3, dtype=np.uint64)
    with pytest.raises(ValueError, match="expected"):
        histogram.raw_counts()


@pytest.mark.parametrize("serializer", [pickle, cloudpickle])
def test_serialization_preserves_exact_integer_state(serializer):
    histogram = make_sparse()
    fill_recorded(histogram, "mc", [-1.0, 0.25, 1.25, 3.0], [2.0, -4.0, 0.5, 9.0])

    restored = serializer.loads(serializer.dumps(histogram))

    np.testing.assert_array_equal(only_raw(restored), [1, 1, 1, 1])
    assert only_raw(restored).dtype == np.dtype(np.uint64)
    np.testing.assert_array_equal(
        next(iter(restored.view(flow=True, as_dict=True).values())),
        next(iter(histogram.view(flow=True, as_dict=True).values())),
    )


def test_merge_group_slice_remove_prune_scale_and_reset_transport_raw_state():
    left = make_sparse()
    fill_recorded(left, "a", [-1.0, 0.25], [2.0, -3.0])
    fill_recorded(left, "b", [1.25, 3.0], [5.0, 7.0])
    right = make_sparse()
    fill_recorded(right, "a", [0.25, 1.25], [-11.0, 13.0])

    merged = left + right
    np.testing.assert_array_equal(
        merged.raw_counts(flow=True)[("a", "nominal")],
        [1, 2, 1, 0],
    )

    grouped = merged.group("process", {"all_mc": ["a", "b"]})
    np.testing.assert_array_equal(only_raw(grouped), [1, 2, 2, 1])

    selected = merged[{"process": "a"}]
    integrated = merged.integrate("process", "a")
    removed = merged.remove("process", ["b"])
    pruned = merged.prune("process", ["a"])
    for transformed in (selected, integrated, removed, pruned):
        np.testing.assert_array_equal(only_raw(transformed), [1, 2, 1, 0])

    raw_before_scale = {
        key: np.array(value, copy=True)
        for key, value in merged.raw_counts(flow=True).items()
    }
    weighted_before = {
        key: np.array(value, copy=True)
        for key, value in merged.view(flow=True, as_dict=True).items()
    }
    merged.scale(-2.0)
    for key, value in merged.raw_counts(flow=True).items():
        np.testing.assert_array_equal(value, raw_before_scale[key])
    for key, value in merged.view(flow=True, as_dict=True).items():
        np.testing.assert_array_equal(value, -2.0 * weighted_before[key])

    merged.reset()
    for value in merged.raw_counts(flow=True).values():
        np.testing.assert_array_equal(value, np.zeros(4, dtype=np.uint64))


def test_incompatible_arithmetic_dense_slice_and_mixed_tracking_fail_closed():
    tracked = make_sparse()
    fill_recorded(tracked, "mc", [0.25], [2.0])
    untracked = make_sparse(track_raw_counts=False)
    untracked.fill(process="mc", systematic="nominal", x=[0.25], weight=[2.0])

    with pytest.raises(RuntimeError, match="tracked and untracked"):
        tracked += untracked
    with pytest.raises(RuntimeError, match="Only additive"):
        tracked -= tracked
    with pytest.raises(RuntimeError, match="arithmetic operation"):
        tracked += 1
    with pytest.raises(RuntimeError, match="Dense-axis selection"):
        tracked[{"x": slice(0, 1)}]


def test_checked_merge_raises_before_uint64_wraparound():
    left = make_sparse()
    right = make_sparse()
    fill_recorded(left, "mc", [0.25], [2.0])
    fill_recorded(right, "mc", [0.25], [3.0])
    key = next(iter(left._raw_counts))
    left._raw_counts[key][1] = np.iinfo(np.uint64).max
    weighted_before = np.array(left._dense_hists[key].view(flow=True), copy=True)

    with pytest.raises(OverflowError, match="overflow"):
        left += right

    assert left._raw_counts[key][1] == np.iinfo(np.uint64).max
    np.testing.assert_array_equal(left._dense_hists[key].view(flow=True), weighted_before)


def test_histeft_counts_source_events_once_before_coefficient_expansion():
    histogram = HistEFT(
        hist.axis.StrCategory([], name="process", growth=True),
        hist.axis.StrCategory([], name="systematic", growth=True),
        hist.axis.Regular(2, 0.0, 2.0, name="x"),
        wc_names=["ctG", "cpt"],
        track_raw_counts=True,
    )
    coefficients = np.asarray(
        [
            [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            [7.0, 8.0, 9.0, 10.0, 11.0, 12.0],
        ]
    )
    histogram.fill(
        process="eft",
        systematic="nominal",
        x=np.asarray([0.25, 1.25]),
        weight=np.asarray([-2.0, 0.5]),
        eft_coeff=coefficients,
        record_raw_count=True,
    )

    np.testing.assert_array_equal(only_raw(histogram), [0, 1, 1, 0])
    assert only_raw(histogram).dtype == np.dtype(np.uint64)
    np.testing.assert_allclose(
        histogram.eval({})[("eft", "nominal")],
        [0.0, -2.0, 3.5, 0.0],
    )


def test_copy_raw_counts_from_histeft_preserves_recorded_and_unrecorded_state():
    source = HistEFT(
        hist.axis.StrCategory([], name="process", growth=True),
        hist.axis.StrCategory([], name="systematic", growth=True),
        hist.axis.Regular(2, 0.0, 2.0, name="x"),
        wc_names=["ctG"],
        track_raw_counts=True,
    )
    source.fill(
        process="eft",
        systematic="nominal",
        x=np.asarray([0.25, 1.25]),
        weight=np.asarray([-2.0, 0.5]),
        eft_coeff=np.asarray([[1.5, 2.0, 3.0], [4.0, 5.0, 6.0]]),
        record_raw_count=True,
    )
    source.fill(
        process="eft",
        systematic="generated",
        x=np.asarray([0.25]),
        weight=np.asarray([7.0]),
        record_raw_count=False,
    )
    destination = SparseHist(
        *list(source.categorical_axes),
        source.dense_axis,
        storage="Double",
    )
    evaluated = source.eval({})
    for categories, values in evaluated.items():
        destination[tuple(categories)] = values

    values_before = {
        key: np.array(values, copy=True)
        for key, values in destination.view(flow=True, as_dict=True).items()
    }
    destination.copy_raw_counts_from(source)

    assert destination.track_raw_counts is True
    assert destination.categorical_axes == source.categorical_axes
    assert destination.dense_axes.name == ("x",)
    np.testing.assert_array_equal(
        destination.raw_counts(flow=True)[("eft", "nominal")],
        np.asarray([0, 1, 1, 0], dtype=np.uint64),
    )
    assert ("eft", "generated") not in destination.raw_counts(flow=True)
    assert destination._validated_raw_count_states()[
        destination.categories_to_index(("eft", "generated"))
    ] is None
    for key, values in destination.view(flow=True, as_dict=True).items():
        np.testing.assert_array_equal(values, values_before[key])

    source._raw_counts[source.categories_to_index(("eft", "nominal"))][1] += 10
    np.testing.assert_array_equal(
        destination.raw_counts(flow=True)[("eft", "nominal")],
        np.asarray([0, 1, 1, 0], dtype=np.uint64),
    )


def test_copy_raw_counts_from_rejects_incompatible_or_conflicting_state():
    source = make_sparse()
    fill_recorded(source, "mc", [0.25], [2.0])

    wrong_axis = SparseHist(
        *list(source.categorical_axes),
        hist.axis.Regular(3, 0.0, 3.0, name="x"),
        storage="Double",
    )
    wrong_axis.fill(
        process="mc", systematic="nominal", x=np.asarray([0.25]), weight=2.0
    )
    with pytest.raises(ValueError, match="Physical dense axes"):
        wrong_axis.copy_raw_counts_from(source)
    assert wrong_axis.track_raw_counts is False

    wrong_categories = SparseHist(
        hist.axis.StrCategory([], name="dataset", growth=True),
        hist.axis.StrCategory([], name="systematic", growth=True),
        *list(source.dense_axes),
        storage="Double",
    )
    wrong_categories.fill(
        dataset="mc", systematic="nominal", x=np.asarray([0.25]), weight=2.0
    )
    with pytest.raises(ValueError, match="Categorical axes"):
        wrong_categories.copy_raw_counts_from(source)
    assert wrong_categories.track_raw_counts is False

    missing_support = make_sparse(track_raw_counts=False)
    missing_support.fill(
        process="other", systematic="nominal", x=np.asarray([0.25]), weight=2.0
    )
    with pytest.raises(ValueError, match="Categorical support"):
        missing_support.copy_raw_counts_from(source)
    assert missing_support.track_raw_counts is False

    conflicting = make_sparse()
    fill_recorded(conflicting, "mc", [0.25], [3.0])
    with pytest.raises(RuntimeError, match="already has raw-count state"):
        conflicting.copy_raw_counts_from(source)

    untracked_source = make_sparse(track_raw_counts=False)
    untracked_source.fill(
        process="mc", systematic="nominal", x=np.asarray([0.25]), weight=2.0
    )
    compatible_destination = make_sparse(track_raw_counts=False)
    compatible_destination.fill(
        process="mc", systematic="nominal", x=np.asarray([0.25]), weight=2.0
    )
    with pytest.raises(RuntimeError, match="Source raw-count tracking is disabled"):
        compatible_destination.copy_raw_counts_from(untracked_source)


@pytest.mark.parametrize("serializer", [pickle, cloudpickle])
def test_with_raw_counts_unrecorded_preserves_payload_and_serializes(serializer):
    source = make_sparse()
    fill_recorded(source, "mc", [0.25, 1.25], [-2.0, 3.0])
    source.fill(
        process="derived",
        systematic="nominal",
        x=np.asarray([0.25]),
        weight=np.asarray([7.0]),
        record_raw_count=False,
    )
    values_before = {
        key: np.array(values, copy=True)
        for key, values in source.view(flow=True, as_dict=True).items()
    }

    output = source.with_raw_counts_unrecorded()

    assert output is not source
    assert output.track_raw_counts is True
    assert output.axes.name == source.axes.name
    assert tuple(type(axis) for axis in output.axes) == tuple(
        type(axis) for axis in source.axes
    )
    assert [list(axis) for axis in output.categorical_axes] == [
        list(axis) for axis in source.categorical_axes
    ]
    assert tuple(output.dense_axes) == tuple(source.dense_axes)
    assert output.raw_counts(flow=True) == {}
    assert all(state is None for state in output._validated_raw_count_states().values())
    assert source.raw_counts(flow=True)
    for key, values in output.view(flow=True, as_dict=True).items():
        np.testing.assert_array_equal(values, values_before[key])

    restored = serializer.loads(serializer.dumps(output))
    assert restored.track_raw_counts is True
    assert restored.raw_counts(flow=True) == {}
    assert all(
        state is None for state in restored._validated_raw_count_states().values()
    )
    for key, values in restored.view(flow=True, as_dict=True).items():
        np.testing.assert_array_equal(values, values_before[key])

    untracked = make_sparse(track_raw_counts=False)
    with pytest.raises(RuntimeError, match="disabled"):
        untracked.with_raw_counts_unrecorded()


def test_recorded_unrecorded_same_key_guard_remains_fail_closed():
    recorded = make_sparse()
    fill_recorded(recorded, "mc", [0.25], [2.0])
    unrecorded = recorded.with_raw_counts_unrecorded()
    weighted_before = np.array(
        next(iter(recorded.view(flow=True, as_dict=True).values())), copy=True
    )

    with pytest.raises(RuntimeError, match="recorded and explicitly unrecorded"):
        recorded += unrecorded

    np.testing.assert_array_equal(
        next(iter(recorded.view(flow=True, as_dict=True).values())), weighted_before
    )


@pytest.mark.parametrize("serializer", [pickle, cloudpickle])
def test_tracked_reconstruction_bypasses_legacy_public_reducer_patch(
    monkeypatch,
    serializer,
):
    untracked_sparse = make_sparse(track_raw_counts=False)
    untracked_sparse.fill(
        process="mc",
        systematic="nominal",
        x=np.asarray([0.25]),
        weight=np.asarray([2.0]),
    )
    tracked_sparse = make_sparse()
    fill_recorded(tracked_sparse, "mc", [0.25], [-3.0])

    untracked_eft = HistEFT(
        hist.axis.StrCategory([], name="process", growth=True),
        hist.axis.StrCategory([], name="systematic", growth=True),
        hist.axis.Regular(2, 0.0, 2.0, name="x"),
        wc_names=["ctG"],
    )
    untracked_eft.fill(
        process="eft",
        systematic="nominal",
        x=np.asarray([0.25]),
        weight=np.asarray([5.0]),
    )
    tracked_eft = HistEFT(
        hist.axis.StrCategory([], name="process", growth=True),
        hist.axis.StrCategory([], name="systematic", growth=True),
        hist.axis.Regular(2, 0.0, 2.0, name="x"),
        wc_names=["ctG"],
        track_raw_counts=True,
    )
    tracked_eft.fill(
        process="eft",
        systematic="nominal",
        x=np.asarray([1.25]),
        weight=np.asarray([-7.0]),
        eft_coeff=np.asarray([[1.0, 2.0, 3.0]]),
        record_raw_count=True,
    )

    payload = serializer.dumps(
        {
            "untracked_sparse": untracked_sparse,
            "tracked_sparse": tracked_sparse,
            "untracked_eft": untracked_eft,
            "tracked_eft": tracked_eft,
        }
    )
    original_legacy_reconstructor = SparseHist._read_from_reduce.__func__
    legacy_calls = []

    def legacy_reconstructor(cls, cat_axes, dense_axes, init_args, dense_hists):
        legacy_calls.append(cls)
        return original_legacy_reconstructor(
            cls,
            cat_axes,
            dense_axes,
            init_args,
            dense_hists,
        )

    monkeypatch.setattr(
        SparseHist,
        "_read_from_reduce",
        classmethod(legacy_reconstructor),
    )
    restored = serializer.loads(payload)

    expected_legacy_calls = (
        [SparseHist, HistEFT]
        if serializer is pickle
        else [HistEFT]
    )
    assert legacy_calls == expected_legacy_calls
    assert restored["untracked_sparse"].track_raw_counts is False
    assert restored["untracked_eft"].track_raw_counts is False
    np.testing.assert_array_equal(only_raw(restored["tracked_sparse"]), [0, 1, 0, 0])
    np.testing.assert_array_equal(only_raw(restored["tracked_eft"]), [0, 0, 1, 0])
    np.testing.assert_allclose(
        restored["tracked_eft"].eval({})[("eft", "nominal")],
        [0.0, 0.0, -7.0, 0.0],
    )
