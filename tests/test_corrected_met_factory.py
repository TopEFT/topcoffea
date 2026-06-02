import inspect
import math

import awkward as ak
import numpy as np
import pytest

from topcoffea.modules.CorrectedMETFactory import CorrectedMETFactory
from topcoffea.modules.Type1CorrectedMETFactory import Type1CorrectedMETFactory


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


class _CorrectionInput:
    def __init__(self, name):
        self.name = name


class _ConstantCorrection:
    def __init__(self, scale):
        self.scale = np.float32(scale)
        self.inputs = [
            _CorrectionInput("JetPt"),
            _CorrectionInput("JetEta"),
            _CorrectionInput("JetA"),
            _CorrectionInput("Rho"),
        ]

    def evaluate(self, *inputs):
        return np.ones(len(np.asarray(inputs[0])), dtype=np.float32) * self.scale


class _PtScaledCorrection:
    def __init__(self, reference_pt):
        self.reference_pt = np.float32(reference_pt)
        self.seen_pt = []
        self.inputs = [
            _CorrectionInput("JetPt"),
            _CorrectionInput("JetEta"),
            _CorrectionInput("JetA"),
            _CorrectionInput("Rho"),
        ]

    def evaluate(self, *inputs):
        jet_pt = np.asarray(inputs[0], dtype=np.float32)
        self.seen_pt.append(jet_pt.copy())
        return jet_pt / self.reference_pt


class _FakeJECStack:
    def __init__(self, l1_scale=1.0, l2_scale=2.0, l2_correction=None):
        self.use_clib = True
        self.jec_names_clib = [
            "Test_L1FastJet_AK4PFPuppi",
            "Test_L2Relative_AK4PFPuppi",
        ]
        self.corrections = {
            self.jec_names_clib[0]: _ConstantCorrection(l1_scale),
            self.jec_names_clib[1]: l2_correction or _ConstantCorrection(l2_scale),
        }

    def get_l1_jec_names(self):
        return [self.jec_names_clib[0]]

    def get_full_jec_names(self):
        return list(self.jec_names_clib)


def _make_fake_corrected_jets_factory(
    nominal_pt=50.0,
    jer_up_pt=75.0,
    jer_down_pt=25.0,
    jes_up_pt=60.0,
    jes_down_pt=45.0,
):
    class _FakeCorrectedJetsFactory:
        instances = []

        def __init__(
            self,
            name_map,
            jec_stack,
            run,
            suppress_forward_eta_stochastic_jer=False,
        ):
            self.name_map = name_map
            self.jec_stack = jec_stack
            self.run = run
            self.suppress_forward_eta_stochastic_jer = suppress_forward_eta_stochastic_jer
            self.build_calls = 0
            type(self).instances.append(self)

        def build(self, jets, lazy_cache):
            self.build_calls += 1
            self.lazy_cache = lazy_cache
            self.jets = jets
            pt_field = self.name_map["JetPt"]
            nominal = ak.ones_like(jets[pt_field]) * nominal_pt
            out = ak.with_field(jets, nominal, pt_field)

            jer_up = ak.with_field(out, ak.ones_like(jets[pt_field]) * jer_up_pt, pt_field)
            jer_down = ak.with_field(out, ak.ones_like(jets[pt_field]) * jer_down_pt, pt_field)
            jes_up = ak.with_field(out, ak.ones_like(jets[pt_field]) * jes_up_pt, pt_field)
            jes_down = ak.with_field(out, ak.ones_like(jets[pt_field]) * jes_down_pt, pt_field)

            out = ak.with_field(out, ak.zip({"up": jer_up, "down": jer_down}), "JER")
            return ak.with_field(
                out,
                ak.zip({"up": jes_up, "down": jes_down}),
                "JES_Total",
            )

    return _FakeCorrectedJetsFactory


TYPE1_NAME_MAP = {
    "METpt": "pt",
    "METphi": "phi",
    "RawMETpt": "pt",
    "RawMETphi": "phi",
    "JetPt": "pt",
    "JetMass": "mass",
    "JetPhi": "phi",
    "JetEta": "eta",
    "JetA": "area",
    "ptRaw": "pt_raw",
    "massRaw": "mass_raw",
    "JetRawFactor": "rawFactor",
    "JetMuonSubtrFactor": "muonSubtrFactor",
    "JetMuonSubtrDeltaPhi": "muonSubtrDeltaPhi",
    "JetChEmEF": "chEmEF",
    "JetNeEmEF": "neEmEF",
    "CorrT1JetPt": "rawPt",
    "CorrT1JetPhi": "phi",
    "CorrT1JetEta": "eta",
    "CorrT1JetArea": "area",
    "CorrT1JetMuonSubtrFactor": "muonSubtrFactor",
    "CorrT1JetMuonSubtrDeltaPhi": "muonSubtrDeltaPhi",
    "CorrT1JetEmEF": "EmEF",
    "Rho": "rho",
    "UnClusteredEnergyDeltaX": "MetUnclustEnUpDeltaX",
    "UnClusteredEnergyDeltaY": "MetUnclustEnUpDeltaY",
}


def _stored_puppimet():
    return ak.Array(
        [
            {
                "pt": 120.0,
                "phi": 0.0,
                "ptUnclusteredUp": 130.0,
                "ptUnclusteredDown": 110.0,
                "phiUnclusteredUp": 0.0,
                "phiUnclusteredDown": 0.0,
            }
        ]
    )


def _raw_puppimet():
    return ak.Array([{"pt": 100.0, "phi": 0.0}])


def _type1_jets(
    include_delta_phi=False,
    em_fail=False,
    low_pt=False,
    pt=50.0,
    raw_factor=0.2,
    pt_raw=None,
    mass=10.0,
    mass_raw=None,
    include_pt_raw=True,
    include_mass_raw=True,
):
    jet_pt = pt if not low_pt else 10.0
    if pt_raw is None:
        pt_raw = jet_pt * (1.0 - raw_factor)
    if mass_raw is None:
        mass_raw = mass * (1.0 - raw_factor)
    jet = {
        "pt": jet_pt,
        "mass": mass,
        "phi": 0.0,
        "eta": 0.2,
        "area": 0.5,
        "rawFactor": raw_factor,
        "muonSubtrFactor": 0.1,
        "chEmEF": 0.2 if not em_fail else 0.6,
        "neEmEF": 0.1 if not em_fail else 0.35,
        "rho": 20.0,
    }
    if include_pt_raw:
        jet["pt_raw"] = pt_raw
    if include_mass_raw:
        jet["mass_raw"] = mass_raw
    if include_delta_phi:
        jet["muonSubtrDeltaPhi"] = math.pi / 2.0
    return ak.Array([[jet]])


def _corr_t1_jets(include_delta_phi=False, include_emef=False, em_fail=False):
    corr = {
        "rawPt": 20.0,
        "phi": 0.0,
        "eta": 0.3,
        "area": 0.4,
        "muonSubtrFactor": 0.5,
        "rho": 20.0,
    }
    if include_delta_phi:
        corr["muonSubtrDeltaPhi"] = math.pi / 2.0
    if include_emef:
        corr["EmEF"] = 0.95 if em_fail else 0.1
    return ak.Array([[corr]])


def _type1_factory(
    corrected_jets_factory_cls=None,
    suppress_forward_eta_stochastic_jer=False,
    jec_stack=None,
):
    if corrected_jets_factory_cls is None:
        corrected_jets_factory_cls = _make_fake_corrected_jets_factory()
    test_factory_cls = corrected_jets_factory_cls

    class _TestType1CorrectedMETFactory(Type1CorrectedMETFactory):
        pass

    _TestType1CorrectedMETFactory._corrected_jets_factory_cls = test_factory_cls
    return _TestType1CorrectedMETFactory(
        TYPE1_NAME_MAP,
        _FakeJECStack() if jec_stack is None else jec_stack,
        suppress_forward_eta_stochastic_jer=suppress_forward_eta_stochastic_jer,
    )


def _build_type1(
    jets=None,
    corr_t1=None,
    corrected_jets_factory_cls=None,
    suppress_forward_eta_stochastic_jer=False,
    jec_stack=None,
):
    return _type1_factory(
        corrected_jets_factory_cls=corrected_jets_factory_cls,
        suppress_forward_eta_stochastic_jer=suppress_forward_eta_stochastic_jer,
        jec_stack=jec_stack,
    ).build(
        _stored_puppimet(),
        _raw_puppimet(),
        _type1_jets() if jets is None else jets,
        _corr_t1_jets() if corr_t1 is None else corr_t1,
        lazy_cache={},
    )


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


def test_type1_met_nominal_formula_and_v12_fallbacks():
    corrected = _build_type1()

    # Jet: caller-provided pt_raw 40 * (1 - 0.1) = 36, full-L1 delta = 36.
    # CorrT1METJet: 20 * (1 - 0.5) = 10, full-L1 delta = 10.
    # RawPuppiMET x is 100, so Type-1 x is 100 - 46.
    assert _value(corrected.pt) == pytest.approx(54.0)
    assert _value(corrected.phi) == pytest.approx(0.0)


def test_type1_met_nominal_uses_original_raw_jet_pt_not_corrected_pt():
    corrected_jets_factory = _make_fake_corrected_jets_factory(
        nominal_pt=500.0,
        jer_up_pt=750.0,
        jer_down_pt=250.0,
        jes_up_pt=600.0,
        jes_down_pt=450.0,
    )

    corrected = _build_type1(corrected_jets_factory_cls=corrected_jets_factory)

    assert _value(corrected.pt) == pytest.approx(54.0)
    assert corrected_jets_factory.instances[-1].build_calls == 1


def test_type1_met_jet_jec_uses_raw_pt_input_and_applies_to_no_mu_raw_pt():
    l2_correction = _PtScaledCorrection(reference_pt=15.0)
    jec_stack = _FakeJECStack(
        l1_scale=1.0,
        l2_correction=l2_correction,
    )

    corrected = _build_type1(
        jets=_type1_jets(pt=50.0, raw_factor=0.2, pt_raw=30.0, mass_raw=7.0),
        corr_t1=_corr_t1_jets(include_emef=True, em_fail=True),
        jec_stack=jec_stack,
    )

    # The L2 factor is JetPt / 15. The caller-provided pt_raw is 30, while
    # pt * (1 - rawFactor) would be 40. The JEC input must see 30, then the
    # factor is applied to no-muon raw pT, 30 * (1 - 0.1) = 27.
    np.testing.assert_allclose(l2_correction.seen_pt[0], np.array([30.0], dtype=np.float32))
    assert _value(corrected.pt) == pytest.approx(73.0)


def test_type1_met_requires_regular_jet_pt_raw():
    with pytest.raises(
        ValueError,
        match="Type1CorrectedMETFactory.*regular Jet pt_raw.*caller-prepared raw pT",
    ):
        _build_type1(jets=_type1_jets(include_pt_raw=False))


def test_type1_met_requires_regular_jet_mass_raw():
    with pytest.raises(
        ValueError,
        match="Type1CorrectedMETFactory.*regular Jet mass_raw.*caller-prepared raw mass",
    ):
        _build_type1(jets=_type1_jets(include_mass_raw=False))


def test_type1_met_requires_pt_raw_name_map_key():
    name_map = dict(TYPE1_NAME_MAP)
    del name_map["ptRaw"]

    with pytest.raises(ValueError, match="ptRaw.*Type1CorrectedMETFactory"):
        Type1CorrectedMETFactory(name_map, _FakeJECStack())


def test_type1_met_requires_mass_raw_name_map_key():
    name_map = dict(TYPE1_NAME_MAP)
    del name_map["massRaw"]

    with pytest.raises(ValueError, match="massRaw.*Type1CorrectedMETFactory"):
        Type1CorrectedMETFactory(name_map, _FakeJECStack())


def test_type1_met_uses_caller_provided_raw_mass_for_jec_inputs():
    corrected_jets_factory = _make_fake_corrected_jets_factory()

    _build_type1(
        jets=_type1_jets(mass=10.0, raw_factor=0.2, mass_raw=7.0),
        corrected_jets_factory_cls=corrected_jets_factory,
    )

    np.testing.assert_allclose(
        ak.to_numpy(ak.flatten(corrected_jets_factory.instances[-1].jets.mass_raw)),
        np.array([7.0], dtype=np.float32),
    )


def test_type1_met_corr_t1_jec_uses_raw_pt_input_and_applies_to_no_mu_raw_pt():
    jec_stack = _FakeJECStack(
        l1_scale=1.0,
        l2_correction=_PtScaledCorrection(reference_pt=10.0),
    )

    corrected = _build_type1(
        jets=_type1_jets(em_fail=True),
        jec_stack=jec_stack,
    )

    # The CorrT1 L2 factor is JetPt / 10. With the intended raw JEC input,
    # JetPt is rawPt = 20, so L2 = 2. That factor is then applied to
    # no-muon raw pT, 20 * (1 - 0.5) = 10, so the delta is 10.
    # If the JEC input were no-muon raw pT, L2 would instead see 10.
    assert _value(corrected.pt) == pytest.approx(90.0)


def test_type1_met_corrected_jets_factory_hook_is_private():
    assert "corrected_jets_factory_cls" not in inspect.signature(Type1CorrectedMETFactory).parameters

    corrected_jets_factory = _make_fake_corrected_jets_factory()
    factory = _type1_factory(corrected_jets_factory_cls=corrected_jets_factory)
    factory.build(
        _stored_puppimet(),
        _raw_puppimet(),
        _type1_jets(),
        _corr_t1_jets(),
        lazy_cache={},
    )

    assert corrected_jets_factory.instances[-1].build_calls == 1


def test_type1_met_threads_forward_jer_suppression_to_internal_factory():
    corrected_jets_factory = _make_fake_corrected_jets_factory()

    _build_type1(
        corrected_jets_factory_cls=corrected_jets_factory,
        suppress_forward_eta_stochastic_jer=True,
    )

    assert corrected_jets_factory.instances[-1].suppress_forward_eta_stochastic_jer


def test_type1_met_missing_delta_phi_fallback_uses_collection_phi():
    nominal = _build_type1()
    shifted = _build_type1(
        jets=_type1_jets(include_delta_phi=True),
        corr_t1=_corr_t1_jets(include_delta_phi=True),
    )

    assert _value(nominal.pt) == pytest.approx(54.0)
    assert _value(shifted.pt) == pytest.approx(math.hypot(100.0, 46.0))
    assert _value(shifted.phi) == pytest.approx(math.atan2(-46.0, 100.0))


def test_type1_met_missing_corr_t1_emef_skips_only_corr_t1_cut():
    missing_emef = _build_type1(corr_t1=_corr_t1_jets(include_emef=False))
    failing_emef = _build_type1(corr_t1=_corr_t1_jets(include_emef=True, em_fail=True))

    assert _value(missing_emef.pt) == pytest.approx(54.0)
    assert _value(failing_emef.pt) == pytest.approx(64.0)


def test_type1_met_jet_em_and_pt_cuts():
    jet_em_fail = _build_type1(jets=_type1_jets(em_fail=True))
    jet_low_pt = _build_type1(jets=_type1_jets(low_pt=True))

    assert _value(jet_em_fail.pt) == pytest.approx(90.0)
    assert _value(jet_low_pt.pt) == pytest.approx(90.0)


def test_type1_met_direct_unclustered_recentering():
    corrected = _build_type1()

    assert _value(corrected.MET_UnclusteredEnergy.up.pt) == pytest.approx(64.0)
    assert _value(corrected.MET_UnclusteredEnergy.down.pt) == pytest.approx(44.0)
    assert _value(corrected.MET_UnclusteredEnergy.up.phi) == pytest.approx(0.0)
    assert _value(corrected.MET_UnclusteredEnergy.down.phi) == pytest.approx(0.0)


def test_type1_met_legacy_delta_xy_unclustered_recentering():
    stored_met = ak.Array(
        [
            {
                "pt": 120.0,
                "phi": 0.0,
                "MetUnclustEnUpDeltaX": 10.0,
                "MetUnclustEnUpDeltaY": 0.0,
            }
        ]
    )

    corrected = _type1_factory().build(
        stored_met,
        _raw_puppimet(),
        _type1_jets(),
        _corr_t1_jets(),
        lazy_cache={},
    )

    assert _value(corrected.pt) == pytest.approx(54.0)
    assert _value(corrected.MET_UnclusteredEnergy.up.pt) == pytest.approx(64.0)
    assert _value(corrected.MET_UnclusteredEnergy.down.pt) == pytest.approx(44.0)
    assert _value(corrected.MET_UnclusteredEnergy.up.phi) == pytest.approx(0.0)
    assert _value(corrected.MET_UnclusteredEnergy.down.phi) == pytest.approx(0.0)


def test_type1_met_jes_jer_variations_vary_jet_full_leg_only():
    corrected = _build_type1()

    assert "MET_UnclusteredEnergy" in ak.fields(corrected)
    assert "JER" in ak.fields(corrected)
    assert "JES_Total" in ak.fields(corrected)
    assert {"up", "down"} == set(ak.fields(corrected.JER))
    assert {"up", "down"} == set(ak.fields(corrected.JES_Total))

    # JER up scales the Jet full leg by 75/50, while the Jet L1 leg and
    # CorrT1METJet contribution stay nominal: raw 100 - (72 + 10) = 18.
    assert _value(corrected.JER.up.pt) == pytest.approx(18.0)
    # JER down scales the Jet full leg by 25/50, below the L1 leg:
    # raw 100 - ((36 - 36) + 10) = 90.
    assert _value(corrected.JER.down.pt) == pytest.approx(90.0)
    # JES_Total up/down use the same public field shape.
    assert _value(corrected.JES_Total.up.pt) == pytest.approx(39.6)
    assert _value(corrected.JES_Total.down.pt) == pytest.approx(61.2)
