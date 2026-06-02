from copy import copy
from functools import reduce

import awkward
import numpy

from topcoffea.modules.CorrectedJetsFactory import CorrectedJetsFactory
from topcoffea.modules.JECStack import JECStack


_TYPE1_REQUIRED_KEYS = [
    "METpt",
    "METphi",
    "RawMETpt",
    "RawMETphi",
    "JetPt",
    "JetPhi",
    "JetEta",
    "JetA",
    # Regular-Jet Type-1 raw inputs are caller-prepared by contract.
    "ptRaw",
    "massRaw",
    "JetMuonSubtrFactor",
    "JetChEmEF",
    "JetNeEmEF",
    "CorrT1JetPt",
    "CorrT1JetPhi",
    "CorrT1JetEta",
    "CorrT1JetArea",
    "CorrT1JetMuonSubtrFactor",
    "Rho",
]


def _has_field(obj, field):
    return field is not None and field in awkward.fields(obj)


def _field_or_zeros(obj, field, like):
    if _has_field(obj, field):
        return obj[field]
    return awkward.zeros_like(like)


def _require_field(obj, field, message):
    if not _has_field(obj, field):
        raise ValueError(message)
    return obj[field]


def _object_counts(obj):
    return awkward.num(obj)


def _rawvar_jec(jecval, rawvar):
    return jecval * rawvar


def _input_values(objs, corr_obj, name_map, run, current_correction=None):
    input_values = []
    run_flat = None
    if any(inp.name == "run" for inp in corr_obj.inputs):
        if run is None:
            raise ValueError("A JEC correction requires a run input, but run=None was provided.")
        run_flat = awkward.flatten(
            awkward.ones_like(objs[name_map["JetPt"]], dtype=numpy.int32) * run
        )

    for inp in corr_obj.inputs:
        if inp.name == "systematic":
            continue
        if inp.name == "run":
            input_value = run_flat
        elif inp.name == "JetPt" and current_correction is not None:
            input_value = _rawvar_jec(
                current_correction,
                awkward.flatten(objs[name_map["JetPt"]]),
            )
        else:
            input_value = awkward.flatten(objs[name_map[inp.name]])
        input_values.append(input_value)

    return input_values


def _evaluate_jec_sequence(objs, name_map, jec_stack, correction_names, run):
    corrections_list = []
    total_correction = None

    for correction_name in correction_names:
        current_correction = None
        if corrections_list:
            ones = numpy.ones_like(corrections_list[-1], dtype=numpy.float32)
            current_correction = reduce(
                lambda x, y: y * x,
                corrections_list,
                ones,
            ).astype(dtype=numpy.float32)

        correction = jec_stack.corrections.get(correction_name)
        if correction is None:
            raise ValueError(f"Correction {correction_name} not found in the JEC stack.")

        inputs = _input_values(
            objs,
            correction,
            name_map,
            run,
            current_correction=current_correction,
        )
        evaluated = correction.evaluate(*inputs).astype(dtype=numpy.float32)
        corrections_list.append(evaluated)
        if total_correction is None:
            total_correction = numpy.ones_like(evaluated, dtype=numpy.float32)
        total_correction *= evaluated

    if total_correction is None:
        raise ValueError("No JEC corrections were configured for Type-1 MET.")

    return awkward.unflatten(total_correction, _object_counts(objs))


def _met_from_xy(px, py):
    return awkward.zip(
        {"pt": numpy.hypot(px, py), "phi": numpy.arctan2(py, px)},
        depth_limit=1,
    )


def _type1_met(raw_met, name_map, delta_px, delta_py):
    raw_px = raw_met[name_map["RawMETpt"]] * numpy.cos(raw_met[name_map["RawMETphi"]])
    raw_py = raw_met[name_map["RawMETpt"]] * numpy.sin(raw_met[name_map["RawMETphi"]])
    return _met_from_xy(raw_px - delta_px, raw_py - delta_py)


def _copy_with_met(stored_met, name_map, met_vector):
    out = copy(stored_met)
    out[name_map["METpt"]] = met_vector.pt
    out[name_map["METphi"]] = met_vector.phi
    return out


class Type1CorrectedMETFactory(object):
    _corrected_jets_factory_cls = CorrectedJetsFactory
    _direct_unc_fields = {
        "pt_up": "ptUnclusteredUp",
        "pt_down": "ptUnclusteredDown",
        "phi_up": "phiUnclusteredUp",
        "phi_down": "phiUnclusteredDown",
    }
    _delta_unc_fields = ("UnClusteredEnergyDeltaX", "UnClusteredEnergyDeltaY")

    def __init__(
        self,
        name_map,
        jec_stack,
        run=None,
        suppress_forward_eta_stochastic_jer=False,
        unclustered_mode="auto",
    ):
        if not isinstance(jec_stack, JECStack):
            if not (
                hasattr(jec_stack, "corrections")
                and hasattr(jec_stack, "get_l1_jec_names")
                and hasattr(jec_stack, "get_full_jec_names")
            ):
                raise TypeError("jec_stack must be a JECStack or expose the JECStack Type-1 helper API.")

        for name in _TYPE1_REQUIRED_KEYS:
            if name not in name_map or name_map[name] is None:
                raise ValueError(
                    f"There is no name mapping for {name}, which is needed for Type1CorrectedMETFactory."
                )

        if unclustered_mode not in ("auto", "direct_pt_phi", "delta_xy"):
            raise ValueError(f"Unknown unclustered_mode '{unclustered_mode}'.")

        self.name_map = name_map
        self.jec_stack = jec_stack
        self.run = run
        self.suppress_forward_eta_stochastic_jer = suppress_forward_eta_stochastic_jer
        self.unclustered_mode = unclustered_mode

    def _get_unclustered_mode(self, stored_met):
        if self.unclustered_mode != "auto":
            return self.unclustered_mode

        if all(_has_field(stored_met, field) for field in self._direct_unc_fields.values()):
            return "direct_pt_phi"

        delta_fields = [self.name_map.get(name) for name in self._delta_unc_fields]
        if all(_has_field(stored_met, field) for field in delta_fields):
            return "delta_xy"

        raise ValueError(
            "Type1CorrectedMETFactory could not build MET_UnclusteredEnergy. "
            "Expected either direct pt/phi unclustered fields "
            f"{list(self._direct_unc_fields.values())} or legacy delta fields {delta_fields}."
        )

    def _evaluate_l1_and_full(self, objs, name_map):
        l1_factor = _evaluate_jec_sequence(
            objs,
            name_map,
            self.jec_stack,
            self.jec_stack.get_l1_jec_names(),
            self.run,
        )
        full_factor = _evaluate_jec_sequence(
            objs,
            name_map,
            self.jec_stack,
            self.jec_stack.get_full_jec_names(),
            self.run,
        )
        return l1_factor, full_factor

    def _jet_jec_name_map(self):
        name_map = dict(self.name_map)
        name_map["JetPt"] = "pt_type1Raw"
        name_map["JetMass"] = "mass_type1Raw"
        return name_map

    def _corr_t1_jec_name_map(self):
        name_map = dict(self.name_map)
        name_map["JetPt"] = "pt_type1Raw"
        name_map["JetEta"] = self.name_map["CorrT1JetEta"]
        name_map["JetA"] = self.name_map["CorrT1JetArea"]
        return name_map

    def _regular_jet_raw_field(self, raw_jets, map_key, field_label, description):
        field = self.name_map.get(map_key)
        return _require_field(
            raw_jets,
            field,
            f"Type1CorrectedMETFactory requires regular Jet {field_label} "
            f"as a caller-prepared {description}; no fallback reconstruction is provided.",
        )

    def _jet_raw_pt(self, raw_jets):
        return self._regular_jet_raw_field(raw_jets, "ptRaw", "pt_raw", "raw pT")

    def _jet_no_mu_raw_pt(self, raw_jets):
        # Type-1 JEC inputs use caller-prepared raw pT. The no-muon vector leg
        # is derived from that same field; there is intentionally no rawFactor fallback.
        return self._jet_raw_pt(raw_jets) * (
            1.0 - raw_jets[self.name_map["JetMuonSubtrFactor"]]
        )

    def _jet_raw_mass(self, raw_jets):
        return self._regular_jet_raw_field(raw_jets, "massRaw", "mass_raw", "raw mass")

    def _corr_t1_raw_pt(self, corr_t1_met_jets):
        return corr_t1_met_jets[self.name_map["CorrT1JetPt"]]

    def _corr_t1_no_mu_raw_pt(self, corr_t1_met_jets):
        return self._corr_t1_raw_pt(corr_t1_met_jets) * (
            1.0 - corr_t1_met_jets[self.name_map["CorrT1JetMuonSubtrFactor"]]
        )

    def _prepare_jets_for_jec(self, raw_jets):
        prepared = awkward.with_field(raw_jets, self._jet_raw_pt(raw_jets), "pt_type1Raw")
        return awkward.with_field(prepared, self._jet_raw_mass(raw_jets), "mass_type1Raw")

    def _prepare_corr_t1_for_jec(self, corr_t1_met_jets):
        return awkward.with_field(
            corr_t1_met_jets,
            self._corr_t1_raw_pt(corr_t1_met_jets),
            "pt_type1Raw",
        )

    def _prepare_jets_for_corrected_factory(self, raw_jets):
        self._jet_raw_pt(raw_jets)
        self._jet_raw_mass(raw_jets)
        return raw_jets

    def _make_corrected_jets_factory(self):
        return self._corrected_jets_factory_cls(
            dict(self.name_map),
            self.jec_stack,
            self.run,
            suppress_forward_eta_stochastic_jer=self.suppress_forward_eta_stochastic_jer,
        )

    def _build_corrected_jets_for_variations(self, raw_jets, lazy_cache):
        factory = self._make_corrected_jets_factory()
        return factory.build(
            self._prepare_jets_for_corrected_factory(raw_jets),
            lazy_cache={} if lazy_cache is None else lazy_cache,
        )

    def _jet_type1_deltas(self, jets, factor_l1, factor_full, pt_scale_factor=None):
        pt_no_mu_raw = self._jet_no_mu_raw_pt(jets)
        delta_phi = _field_or_zeros(
            jets,
            self.name_map.get("JetMuonSubtrDeltaPhi"),
            jets[self.name_map["JetPhi"]],
        )
        phi_no_mu_raw = jets[self.name_map["JetPhi"]] + delta_phi

        pt_no_mu_l1 = pt_no_mu_raw * factor_l1
        pt_no_mu_full = pt_no_mu_raw * factor_full
        if pt_scale_factor is not None:
            pt_no_mu_full = pt_no_mu_full * pt_scale_factor

        mask = (
            (pt_no_mu_full > 15.0)
            & ((jets[self.name_map["JetChEmEF"]] + jets[self.name_map["JetNeEmEF"]]) < 0.9)
        )
        diff_pt = awkward.where(mask, pt_no_mu_full - pt_no_mu_l1, 0.0)
        return awkward.zip(
            {
                "delta_px": awkward.sum(diff_pt * numpy.cos(phi_no_mu_raw), axis=1),
                "delta_py": awkward.sum(diff_pt * numpy.sin(phi_no_mu_raw), axis=1),
            },
            depth_limit=1,
        )

    def _corr_t1_type1_deltas(self, corr_t1_met_jets, factor_l1, factor_full):
        pt_no_mu_raw = self._corr_t1_no_mu_raw_pt(corr_t1_met_jets)
        delta_phi = _field_or_zeros(
            corr_t1_met_jets,
            self.name_map.get("CorrT1JetMuonSubtrDeltaPhi"),
            corr_t1_met_jets[self.name_map["CorrT1JetPhi"]],
        )
        phi_no_mu_raw = corr_t1_met_jets[self.name_map["CorrT1JetPhi"]] + delta_phi

        pt_no_mu_l1 = pt_no_mu_raw * factor_l1
        pt_no_mu_full = pt_no_mu_raw * factor_full

        mask = pt_no_mu_full > 15.0
        em_ef_field = self.name_map.get("CorrT1JetEmEF")
        if _has_field(corr_t1_met_jets, em_ef_field):
            mask = mask & (corr_t1_met_jets[em_ef_field] < 0.9)

        diff_pt = awkward.where(mask, pt_no_mu_full - pt_no_mu_l1, 0.0)
        return awkward.zip(
            {
                "delta_px": awkward.sum(diff_pt * numpy.cos(phi_no_mu_raw), axis=1),
                "delta_py": awkward.sum(diff_pt * numpy.sin(phi_no_mu_raw), axis=1),
            },
            depth_limit=1,
        )

    def _make_unclustered(self, stored_met, nominal_met):
        mode = self._get_unclustered_mode(stored_met)
        nominal_px = nominal_met[self.name_map["METpt"]] * numpy.cos(nominal_met[self.name_map["METphi"]])
        nominal_py = nominal_met[self.name_map["METpt"]] * numpy.sin(nominal_met[self.name_map["METphi"]])

        if mode == "direct_pt_phi":
            stored_px = stored_met[self.name_map["METpt"]] * numpy.cos(stored_met[self.name_map["METphi"]])
            stored_py = stored_met[self.name_map["METpt"]] * numpy.sin(stored_met[self.name_map["METphi"]])
            up_px = stored_met[self._direct_unc_fields["pt_up"]] * numpy.cos(
                stored_met[self._direct_unc_fields["phi_up"]]
            )
            up_py = stored_met[self._direct_unc_fields["pt_up"]] * numpy.sin(
                stored_met[self._direct_unc_fields["phi_up"]]
            )
            down_px = stored_met[self._direct_unc_fields["pt_down"]] * numpy.cos(
                stored_met[self._direct_unc_fields["phi_down"]]
            )
            down_py = stored_met[self._direct_unc_fields["pt_down"]] * numpy.sin(
                stored_met[self._direct_unc_fields["phi_down"]]
            )
            up_vector = _met_from_xy(nominal_px + (up_px - stored_px), nominal_py + (up_py - stored_py))
            down_vector = _met_from_xy(
                nominal_px + (down_px - stored_px),
                nominal_py + (down_py - stored_py),
            )
        else:
            dx = stored_met[self.name_map["UnClusteredEnergyDeltaX"]]
            dy = stored_met[self.name_map["UnClusteredEnergyDeltaY"]]
            up_vector = _met_from_xy(nominal_px + dx, nominal_py + dy)
            down_vector = _met_from_xy(nominal_px - dx, nominal_py - dy)

        return awkward.zip(
            {
                "up": _copy_with_met(stored_met, self.name_map, up_vector),
                "down": _copy_with_met(stored_met, self.name_map, down_vector),
            },
            depth_limit=1,
            with_name="METSystematic",
        )

    def build(self, stored_met, raw_met, raw_jets, corr_t1_met_jets, lazy_cache=None):
        if not isinstance(stored_met, awkward.highlevel.Array):
            raise TypeError("'stored_met' must be an awkward array.")
        if not isinstance(raw_met, awkward.highlevel.Array):
            raise TypeError("'raw_met' must be an awkward array.")
        if not isinstance(raw_jets, awkward.highlevel.Array):
            raise TypeError("'raw_jets' must be an awkward array.")
        if not isinstance(corr_t1_met_jets, awkward.highlevel.Array):
            raise TypeError("'corr_t1_met_jets' must be an awkward array.")

        jec_jets = self._prepare_jets_for_jec(raw_jets)
        jet_l1, jet_full = self._evaluate_l1_and_full(jec_jets, self._jet_jec_name_map())
        jet_deltas = self._jet_type1_deltas(raw_jets, jet_l1, jet_full)

        corr_jec_jets = self._prepare_corr_t1_for_jec(corr_t1_met_jets)
        corr_l1, corr_full = self._evaluate_l1_and_full(corr_jec_jets, self._corr_t1_jec_name_map())
        corr_deltas = self._corr_t1_type1_deltas(corr_t1_met_jets, corr_l1, corr_full)

        total_delta_px = jet_deltas.delta_px + corr_deltas.delta_px
        total_delta_py = jet_deltas.delta_py + corr_deltas.delta_py

        nominal_vector = _type1_met(raw_met, self.name_map, total_delta_px, total_delta_py)
        out = _copy_with_met(stored_met, self.name_map, nominal_vector)
        out[self.name_map["METpt"] + "_orig"] = raw_met[self.name_map["RawMETpt"]]
        out[self.name_map["METphi"] + "_orig"] = raw_met[self.name_map["RawMETphi"]]

        out_dict = {field: out[field] for field in awkward.fields(out)}
        out_dict["MET_UnclusteredEnergy"] = self._make_unclustered(stored_met, out)

        variation_jets = self._build_corrected_jets_for_variations(raw_jets, lazy_cache)

        # CorrT1METJet is included in the nominal Type-1 correction but kept nominal
        # under JES/JER variations, following the coffea PR reference implementation.
        for unc in filter(lambda x: x.startswith(("JER", "JES")), awkward.fields(variation_jets)):
            nominal_pt = variation_jets[self.name_map["JetPt"]]
            safe_nominal = awkward.where(nominal_pt > 0, nominal_pt, 1.0)
            scale_up = variation_jets[unc].up[self.name_map["JetPt"]] / safe_nominal
            scale_down = variation_jets[unc].down[self.name_map["JetPt"]] / safe_nominal

            up_jet_deltas = self._jet_type1_deltas(
                raw_jets,
                jet_l1,
                jet_full,
                pt_scale_factor=scale_up,
            )
            down_jet_deltas = self._jet_type1_deltas(
                raw_jets,
                jet_l1,
                jet_full,
                pt_scale_factor=scale_down,
            )

            up_vector = _type1_met(
                raw_met,
                self.name_map,
                up_jet_deltas.delta_px + corr_deltas.delta_px,
                up_jet_deltas.delta_py + corr_deltas.delta_py,
            )
            down_vector = _type1_met(
                raw_met,
                self.name_map,
                down_jet_deltas.delta_px + corr_deltas.delta_px,
                down_jet_deltas.delta_py + corr_deltas.delta_py,
            )

            out_dict[unc] = awkward.zip(
                {
                    "up": _copy_with_met(stored_met, self.name_map, up_vector),
                    "down": _copy_with_met(stored_met, self.name_map, down_vector),
                },
                depth_limit=1,
                with_name="METSystematic",
            )

        return awkward.zip(
            out_dict,
            depth_limit=1,
            parameters=out.layout.parameters,
            behavior=out.behavior,
        )

    def uncertainties(self):
        return ["MET_UnclusteredEnergy"]
