"""Category-local Method-1a regression tests (double working point)."""
import awkward as ak
import numpy as np
import pytest

from topcoffea.modules.corrections import get_method1a_wgt_doublewp


def _evaluate(eff_m, eff_l, sf_m, sf_l, categories):
    categories = ak.Array(categories)
    return get_method1a_wgt_doublewp(
        ak.Array(eff_m), ak.Array(eff_l), ak.Array(sf_m), ak.Array(sf_l),
        categories == 0, categories == 1, categories == 2,
    )


def test_accepted_pathological_jet_has_unity_factor():
    eff_m = 0.8696965801856155
    eff_l = 0.9495840951077332
    sf_m = 1.1189123148223499
    sf_l = 0.9981717698816024
    data_interval = eff_l * sf_l - eff_m * sf_m
    mc_interval = eff_l - eff_m
    np.testing.assert_allclose(data_interval, -0.025266176863462553, rtol=1e-14)
    np.testing.assert_allclose(mc_interval, 0.07988751492211776, rtol=1e-14)
    np.testing.assert_allclose(data_interval / mc_interval, -0.3162719091724723, rtol=1e-14)
    data, mc = _evaluate([[eff_m]], [[eff_l]], [[sf_m]], [[sf_l]], [[1]])
    np.testing.assert_array_equal(ak.to_numpy(data), ak.to_numpy(mc))
    np.testing.assert_array_equal(ak.to_numpy(data / mc), [1.])


@pytest.mark.parametrize("sf_m,sf_l", [
    (1.1189123148223499, 0.9981717698816024),  # central corner
    (1.2, 1.0),  # varied SF remains negative
    (1.0, 1.05),  # varied SF crosses to positive
    (0.95, 0.98),
])
def test_shared_helper_applies_rule_to_each_sf_input(sf_m, sf_l):
    eff_m, eff_l = 0.8696965801856155, 0.9495840951077332
    data, mc = _evaluate([[eff_m]], [[eff_l]], [[sf_m]], [[sf_l]], [[1]])
    interval = eff_l * sf_l - eff_m * sf_m
    expected = 1. if interval < 0 else interval / (eff_l - eff_m)
    np.testing.assert_allclose(ak.to_numpy(data / mc), [expected], rtol=1e-14)


@pytest.mark.parametrize("category,sf_m,sf_l,expected", [
    (0, 1.2, 0.5, 1.2),  # negative interval must not affect Medium
    (0, -0.4, 1.0, -0.4),  # other negative mechanism preserved
    (2, 1.2, 0.5, (1 - .8 * .5) / (1 - .8)),
    (2, 1.0, 1.5, (1 - .8 * 1.5) / (1 - .8)),  # negative Not-Loose
    (1, 1.1, 1.0, (.8 - .4 * 1.1) / (.8 - .4)),
    (1, 2.0, 1.0, 0.),  # exactly zero is not the fallback
])
def test_native_categories_and_nonnegative_intervals(category, sf_m, sf_l, expected):
    data, mc = _evaluate([[.4]], [[.8]], [[sf_m]], [[sf_l]], [[category]])
    np.testing.assert_allclose(ak.to_numpy(data / mc), [expected], rtol=1e-14, atol=0)


def test_mixed_and_empty_events_use_only_affected_jet_interval():
    data, mc = _evaluate(
        [[.4, .9, .4], [], [.4, .4]],
        [[.8, .95, .8], [], [.8, .8]],
        [[1.1, 1.2, 1.], [], [-.5, 1.]],
        [[1., 1., 1.05], [], [1., 1.]],
        [[0, 1, 2], [], [0, 1]],
    )
    expected_mc = [.4 * (.95 - .9) * (1 - .8), 1., .4 * (.8 - .4)]
    expected_data = [.4 * 1.1 * (.95 - .9) * (1 - .8 * 1.05), 1., (.4 * -.5) * (.8 - .4)]
    np.testing.assert_allclose(ak.to_numpy(mc), expected_mc, rtol=1e-14)
    np.testing.assert_allclose(ak.to_numpy(data), expected_data, rtol=1e-14)
    assert ak.to_list(data / mc)[-1] < 0  # no final-weight clipping


@pytest.mark.parametrize("eff_m,eff_l,sf_m,sf_l,category,expected_data", [
    (0., .8, 1.1, 1., 0, 0.),
    (.4, .4, 1., 1.2, 1, .4 * 1.2 - .4),
    (.4, .4, 1.2, 1., 1, 0.),
    (.4, 1., 1., .8, 2, 1 - .8),
])
def test_existing_zero_mc_product_guard(eff_m, eff_l, sf_m, sf_l, category, expected_data):
    data, mc = _evaluate([[eff_m]], [[eff_l]], [[sf_m]], [[sf_l]], [[category]])
    np.testing.assert_array_equal(ak.to_numpy(mc), [1.])
    np.testing.assert_allclose(ak.to_numpy(data), [expected_data], rtol=1e-14, atol=0)
