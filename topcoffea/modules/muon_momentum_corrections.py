"""Run 3 muon momentum corrections backed by the vendored ScaReKit API."""

import importlib
from pathlib import Path

import awkward as ak
import correctionlib

from topcoffea.modules.paths import topcoffea_path


RUN3_MUON_CAMPAIGNS = {
    "2022": "2022_Summer22",
    "2022EE": "2022_Summer22EE",
    "2023": "2023_Summer23",
    "2023BPix": "2023_Summer23BPix",
}

MUON_MOMENTUM_VARIATIONS = (
    "nominal",
    "MuonScaleUp",
    "MuonScaleDown",
    "MuonResolutionUp",
    "MuonResolutionDown",
)

_SCAREKIT_FUNCTIONS = (
    "pt_scale",
    "pt_resol",
    "pt_scale_var",
    "pt_resol_var",
)

_SCAREKIT_PT_MIN_GEV = 26.0
_SCAREKIT_PT_MAX_GEV = 200.0


def get_run3_muon_campaign(year):
    """Return the ScaReKit campaign name for a supported analysis year."""
    try:
        return RUN3_MUON_CAMPAIGNS[str(year)]
    except KeyError as exc:
        supported = ", ".join(RUN3_MUON_CAMPAIGNS)
        raise ValueError(
            f'Unsupported Run 3 muon correction campaign "{year}". '
            f"Supported campaigns: {supported}."
        ) from exc


def get_scarekit_payload_path(year, payload_directory=None):
    """Build the expected standard ScaReKit payload path."""
    campaign = get_run3_muon_campaign(year)
    if payload_directory is None:
        payload_directory = topcoffea_path("data/POG/MUO")
    return Path(payload_directory) / campaign / "muon_scalesmearing.json.gz"


def _validate_scarekit_backend(backend):
    missing = [name for name in _SCAREKIT_FUNCTIONS if not hasattr(backend, name)]
    if missing:
        raise RuntimeError(
            "The ScaReKit backend is missing required functions: "
            + ", ".join(missing)
        )
    return backend


def _load_scarekit_backend():
    try:
        backend = importlib.import_module(
            "topcoffea.modules.muon_scarekit_backend"
        )
    except ModuleNotFoundError as exc:
        raise RuntimeError(
            "Run 3 muon momentum corrections require the vendored ScaReKit "
            "backend at topcoffea.modules.muon_scarekit_backend."
        ) from exc

    return _validate_scarekit_backend(backend)


def _load_correction_set(year, correction_set, payload_directory):
    if correction_set is not None and payload_directory is not None:
        raise ValueError(
            "Pass either correction_set or payload_directory, not both."
        )
    if correction_set is not None:
        return correction_set

    payload_path = get_scarekit_payload_path(year, payload_directory)
    if not payload_path.exists():
        raise FileNotFoundError(
            "Run 3 muon momentum correction payload not found: "
            f"{payload_path}"
        )
    return correctionlib.CorrectionSet.from_file(str(payload_path))


def _require_muon_fields(muons, is_data):
    required = {"pt", "eta", "phi", "charge"}
    if not is_data:
        required.add("nTrackerLayers")
    missing = sorted(required.difference(ak.fields(muons)))
    if missing:
        raise ValueError(
            "Muon collection is missing fields required by ScaReKit: "
            + ", ".join(missing)
        )


def _apply_muonscarekit_pt_domain_fallback(muons, corrected_pt):
    outside_domain = (
        (muons.pt < _SCAREKIT_PT_MIN_GEV)
        | (muons.pt > _SCAREKIT_PT_MAX_GEV)
    )
    return ak.where(outside_domain, muons.pt, corrected_pt)


def apply_muon_momentum_corrections(
    muons,
    year,
    is_data,
    variation="nominal",
    *,
    event_numbers=None,
    luminosity_blocks=None,
    correction_set=None,
    payload_directory=None,
    backend=None,
):
    """Return corrected muon ``pt`` with the same nested shape as ``muons.pt``.

    Data nominal applies the ScaReKit scale correction. MC nominal applies
    scale followed by resolution. MC variations are object-momentum shifts
    around that nominal result and must be propagated by callers through
    object selection and event-variable rebuilding.
    """
    get_run3_muon_campaign(year)
    if variation not in MUON_MOMENTUM_VARIATIONS:
        supported = ", ".join(MUON_MOMENTUM_VARIATIONS)
        raise ValueError(
            f'Unsupported muon momentum variation "{variation}". '
            f"Supported variations: {supported}."
        )
    if is_data and variation != "nominal":
        raise ValueError(
            f'Muon momentum variation "{variation}" is not applicable to data.'
        )

    _require_muon_fields(muons, is_data)
    correction_set = _load_correction_set(
        year, correction_set, payload_directory
    )
    backend = (
        _load_scarekit_backend()
        if backend is None
        else _validate_scarekit_backend(backend)
    )

    scaled_pt = backend.pt_scale(
        is_data,
        muons.pt,
        muons.eta,
        muons.phi,
        muons.charge,
        correction_set,
        nested=True,
    )
    scaled_pt = _apply_muonscarekit_pt_domain_fallback(muons, scaled_pt)
    if is_data:
        return scaled_pt

    if event_numbers is None or luminosity_blocks is None:
        raise ValueError(
            "MC ScaReKit resolution correction requires event_numbers and "
            "luminosity_blocks."
        )

    nominal_pt = backend.pt_resol(
        scaled_pt,
        muons.eta,
        muons.phi,
        muons.nTrackerLayers,
        event_numbers,
        luminosity_blocks,
        correction_set,
        nested=True,
    )
    nominal_pt = _apply_muonscarekit_pt_domain_fallback(muons, nominal_pt)
    if variation == "nominal":
        return nominal_pt
    if variation == "MuonScaleUp":
        varied_pt = backend.pt_scale_var(
            nominal_pt,
            muons.eta,
            muons.phi,
            muons.charge,
            "up",
            correction_set,
            nested=True,
        )
        return _apply_muonscarekit_pt_domain_fallback(muons, varied_pt)
    if variation == "MuonScaleDown":
        varied_pt = backend.pt_scale_var(
            nominal_pt,
            muons.eta,
            muons.phi,
            muons.charge,
            "dn",
            correction_set,
            nested=True,
        )
        return _apply_muonscarekit_pt_domain_fallback(muons, varied_pt)
    if variation == "MuonResolutionUp":
        direction = "up"
    else:
        direction = "dn"
    varied_pt = backend.pt_resol_var(
        scaled_pt,
        nominal_pt,
        muons.eta,
        direction,
        correction_set,
        nested=True,
    )
    return _apply_muonscarekit_pt_domain_fallback(muons, varied_pt)
