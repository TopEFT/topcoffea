import awkward as ak
import numpy as np
import pytest

from topcoffea.modules.CorrectedJetsFactory import jer_smear


def _inputs():
    return {
        "pt_gen": ak.Array([0.0, 95.0, 50.0, 0.0, 0.0, 0.0, 0.0]),
        "jet_pt": ak.Array([100.0] * 7),
        "eta": ak.Array([2.7, -2.7, 2.7, 2.4, 2.5, 3.0, 3.1]),
        "resolution": ak.Array([0.1] * 7),
        "rand_gauss": ak.Array([1.0] * 7),
        "scale_factor": ak.Array([[1.2, 1.3, 1.1]] * 7),
    }


def _smear(variation, enabled=False):
    inputs = _inputs()
    return np.asarray(
        ak.to_numpy(
            jer_smear(
                variation,
                False,
                inputs["pt_gen"],
                inputs["jet_pt"],
                inputs["eta"],
                inputs["resolution"],
                inputs["rand_gauss"],
                inputs["scale_factor"],
                suppress_forward_eta_stochastic_jer=enabled,
            )
        )
    )


def _stochastic(scale_factor):
    return 1.0 + np.sqrt(scale_factor**2 - 1.0) * 0.1


def _deterministic(scale_factor):
    return 1.0 + (scale_factor - 1.0) * 0.05


@pytest.mark.parametrize(
    "variation,scale_factor",
    [
        (0, 1.2),
        (1, 1.3),
        (2, 1.1),
    ],
)
def test_forward_eta_stochastic_suppression_default_is_unchanged(variation, scale_factor):
    expected = np.array(
        [
            _stochastic(scale_factor),
            _deterministic(scale_factor),
            _stochastic(scale_factor),
            _stochastic(scale_factor),
            _stochastic(scale_factor),
            _stochastic(scale_factor),
            _stochastic(scale_factor),
        ]
    )

    np.testing.assert_allclose(_smear(variation), expected)
    np.testing.assert_allclose(_smear(variation, enabled=False), expected)


@pytest.mark.parametrize(
    "variation,scale_factor",
    [
        (0, 1.2),
        (1, 1.3),
        (2, 1.1),
    ],
)
def test_forward_eta_stochastic_suppression_uses_unity_for_non_hybrid_target_jets(
    variation, scale_factor
):
    expected = np.array(
        [
            1.0,
            _deterministic(scale_factor),
            1.0,
            _stochastic(scale_factor),
            _stochastic(scale_factor),
            _stochastic(scale_factor),
            _stochastic(scale_factor),
        ]
    )

    np.testing.assert_allclose(_smear(variation, enabled=True), expected)
