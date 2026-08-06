from __future__ import annotations

import hist
import numpy as np
import pytest

from topcoffea.modules.histEFT import HistEFT


WC_NAMES = ["ctG", "ctW"]
N_QUAD = 6
WC_POINTS = [
    {},
    {"ctG": 1.0},
    {"ctG": -1.0},
    {"ctG": 0.5, "ctW": -0.3},
]


def _make_histeft(*, use_multicell: bool, store_sumw2: bool = False) -> HistEFT:
    histogram = HistEFT(
        hist.axis.StrCategory([], name="process", growth=True),
        hist.axis.StrCategory([], name="systematic", growth=True),
        hist.axis.Regular(2, 0.0, 2.0, name="observable"),
        wc_names=WC_NAMES,
        use_multicell=use_multicell,
        store_sumw2=store_sumw2,
        label="Events",
    )

    observable = np.array([-0.5, 0.25, 1.25, 2.5])
    weight = np.array([1.5, -2.0, 0.5, 3.0])
    eft_coeff = np.array(
        [
            [1.0, 0.5, 0.25, -0.5, 0.75, 0.125],
            [2.0, -1.0, 0.5, 0.25, -0.75, 1.5],
            [0.75, 0.25, -0.5, 1.0, 0.5, -0.25],
            [1.25, -0.5, 1.0, -1.5, 0.25, 0.75],
        ]
    )

    for process, process_scale in (("ttH", 1.0), ("ttW", -0.4)):
        for systematic in ("nominal", "JERUp"):
            histogram.fill(
                process=process,
                systematic=systematic,
                observable=observable,
                weight=weight * process_scale,
                eft_coeff=eft_coeff,
                fill_sumw2=store_sumw2 and systematic == "nominal",
            )

    return histogram


def _normalized(mapping):
    return {tuple(key): np.asarray(value) for key, value in mapping.items()}


def _hist_arrays(histogram, sparse_names, sparse_keys, *, flow=True):
    return {
        sparse_key: np.asarray(
            histogram[dict(zip(sparse_names, sparse_key))].values(flow=flow)
        )
        for sparse_key in sparse_keys
    }


def _assert_scalar_evaluated_hist(source, output, wc_point):
    expected = _normalized(source.eval(wc_point))
    actual = _hist_arrays(
        output,
        source.categorical_axes.name,
        expected,
        flow=True,
    )

    assert type(output) is hist.Hist
    assert output.storage_type is hist.storage.Double
    assert "quadratic_term" not in output.axes.name
    assert output.axes.name == tuple(
        axis.name for axis in source.axes if axis.name != "quadratic_term"
    )
    assert output.label == "Events"
    assert set(actual) == set(expected)
    for sparse_key in expected:
        assert expected[sparse_key].shape == (4,)
        assert np.any(expected[sparse_key] != 0.0)
        np.testing.assert_allclose(actual[sparse_key], expected[sparse_key])


@pytest.mark.parametrize(
    "use_multicell,store_sumw2",
    [
        (False, False),
        (True, False),
        (True, True),
    ],
)
def test_direct_as_hist_returns_scalar_evaluated_histogram(
    use_multicell,
    store_sumw2,
):
    source = _make_histeft(
        use_multicell=use_multicell,
        store_sumw2=store_sumw2,
    )
    wc_point = {"ctG": 0.5, "ctW": -0.3}

    _assert_scalar_evaluated_hist(source, source.as_hist(wc_point), wc_point)


@pytest.mark.parametrize("wc_point", WC_POINTS)
def test_legacy_and_multicell_as_hist_outputs_agree(wc_point):
    legacy = _make_histeft(use_multicell=False)
    multicell = _make_histeft(use_multicell=True)
    legacy_output = legacy.as_hist(wc_point)
    multicell_output = multicell.as_hist(wc_point)

    np.testing.assert_allclose(
        multicell_output.values(flow=True),
        legacy_output.values(flow=True),
    )
    assert np.any(multicell_output.values(flow=True) != 0.0)


@pytest.mark.parametrize("wc_point", WC_POINTS)
def test_embedded_sumw2_does_not_enter_as_hist_evaluation(wc_point):
    yield_only = _make_histeft(use_multicell=True, store_sumw2=False)
    with_sumw2 = _make_histeft(use_multicell=True, store_sumw2=True)

    assert any(
        np.any(values != 0.0)
        for values in with_sumw2.nominal_sumw2(flow=True).values()
    )
    np.testing.assert_allclose(
        with_sumw2.as_hist(wc_point).values(flow=True),
        yield_only.as_hist(wc_point).values(flow=True),
    )


def _transform(source, operation):
    if operation == "integrate":
        return source.integrate("systematic", "nominal")
    if operation == "group":
        return source.group("process", {"signal": ["ttH", "ttW"]})
    if operation == "integrate_group":
        return source.integrate("systematic", "nominal").group(
            "process",
            {"signal": ["ttH", "ttW"]},
        )
    raise AssertionError(f"Unknown operation: {operation}")


@pytest.mark.parametrize(
    "use_multicell,store_sumw2",
    [
        (False, False),
        (True, False),
        (True, True),
    ],
)
@pytest.mark.parametrize("operation", ["integrate", "group", "integrate_group"])
def test_as_hist_after_sparse_structural_transformations(
    use_multicell,
    store_sumw2,
    operation,
):
    source = _transform(
        _make_histeft(
            use_multicell=use_multicell,
            store_sumw2=store_sumw2,
        ),
        operation,
    )
    wc_point = {"ctG": -0.75, "ctW": 0.2}

    _assert_scalar_evaluated_hist(source, source.as_hist(wc_point), wc_point)


@pytest.mark.parametrize(
    "use_multicell,store_sumw2",
    [(False, False), (True, False), (True, True)],
)
def test_as_hist_preserves_physical_flow_bins(use_multicell, store_sumw2):
    source = _make_histeft(
        use_multicell=use_multicell,
        store_sumw2=store_sumw2,
    ).integrate("systematic", "nominal")
    wc_point = {"ctG": 0.25, "ctW": -0.5}
    expected = _normalized(source.eval(wc_point))
    output = source.as_hist(wc_point)
    with_flow = _hist_arrays(
        output,
        source.categorical_axes.name,
        expected,
        flow=True,
    )
    without_flow = _hist_arrays(
        output,
        source.categorical_axes.name,
        expected,
        flow=False,
    )

    for sparse_key, expected_values in expected.items():
        np.testing.assert_allclose(with_flow[sparse_key], expected_values)
        np.testing.assert_allclose(without_flow[sparse_key], expected_values[1:-1])
        assert expected_values[0] != 0.0
        assert expected_values[-1] != 0.0


@pytest.mark.parametrize(
    "use_multicell,store_sumw2",
    [(False, False), (True, False), (True, True)],
)
def test_empty_as_hist_preserves_axes_with_scalar_storage(
    use_multicell,
    store_sumw2,
):
    source = HistEFT(
        hist.axis.StrCategory([], name="process", growth=True),
        hist.axis.StrCategory([], name="systematic", growth=True),
        hist.axis.Regular(2, 0.0, 2.0, name="observable"),
        wc_names=WC_NAMES,
        use_multicell=use_multicell,
        store_sumw2=store_sumw2,
        label="Events",
    )

    output = source.as_hist({"ctG": 1.0, "ctW": -0.5})

    assert type(output) is hist.Hist
    assert output.storage_type is hist.storage.Double
    assert output.axes.name == ("process", "systematic", "observable")
    assert output.values(flow=True).shape == (0, 0, 4)
    assert output.sum(flow=True) == 0.0
