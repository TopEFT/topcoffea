import math

import awkward as ak
import pytest

from topcoffea.modules.CorrectedMETFactory import CorrectedMETFactory


NAME_MAP = {
    "METpt": "pt",
    "METphi": "phi",
    "JetPt": "pt",
    "JetPhi": "phi",
    "ptRaw": "pt_raw",
    "UnClusteredEnergyDeltaX": "MetUnclustEnUpDeltaX",
    "UnClusteredEnergyDeltaY": "MetUnclustEnUpDeltaY",
}


def _jets():
    return ak.Array([[{"pt": 50.0, "phi": 0.0, "pt_raw": 40.0}]])


def _value(array):
    return float(ak.to_numpy(ak.materialized(array))[0])


def test_met_unclustered_energy_legacy_delta_xy_mode():
    met = ak.Array(
        [
            {
                "pt": 100.0,
                "phi": 0.0,
                "MetUnclustEnUpDeltaX": 10.0,
                "MetUnclustEnUpDeltaY": 0.0,
            }
        ]
    )

    corrected = CorrectedMETFactory(NAME_MAP).build(met, _jets(), lazy_cache={})

    assert _value(corrected.pt) == pytest.approx(110.0)
    assert _value(corrected.MET_UnclusteredEnergy.up.pt) == pytest.approx(120.0)
    assert _value(corrected.MET_UnclusteredEnergy.down.pt) == pytest.approx(100.0)
    assert _value(corrected.MET_UnclusteredEnergy.up.phi) == pytest.approx(0.0)
    assert _value(corrected.MET_UnclusteredEnergy.down.phi) == pytest.approx(0.0)


def test_met_unclustered_energy_direct_pt_phi_mode():
    met = ak.Array(
        [
            {
                "pt": 100.0,
                "phi": 0.0,
                "ptUnclusteredUp": 120.0,
                "ptUnclusteredDown": 80.0,
                "phiUnclusteredUp": 0.0,
                "phiUnclusteredDown": 0.0,
            }
        ]
    )

    corrected = CorrectedMETFactory(NAME_MAP).build(met, _jets(), lazy_cache={})

    assert _value(corrected.pt) == pytest.approx(110.0)
    assert _value(corrected.MET_UnclusteredEnergy.up.pt) == pytest.approx(130.0)
    assert _value(corrected.MET_UnclusteredEnergy.up.phi) == pytest.approx(0.0)
    assert _value(corrected.MET_UnclusteredEnergy.down.pt) == pytest.approx(90.0)
    assert _value(corrected.MET_UnclusteredEnergy.down.phi) == pytest.approx(0.0)


def test_met_unclustered_energy_missing_fields_fails_clearly():
    met = ak.Array([{"pt": 100.0, "phi": 0.0}])

    with pytest.raises(ValueError, match="could not build MET_UnclusteredEnergy"):
        CorrectedMETFactory(NAME_MAP).build(met, _jets(), lazy_cache={})
