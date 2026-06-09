import importlib
import inspect

import awkward as ak
import numpy as np
import pytest


def _length(values):
    return len(np.atleast_1d(np.asarray(values)))


class _FakeCorrection:
    def __init__(self, name, recorder):
        self.name = name
        self.recorder = recorder

    def evaluate(self, *args):
        nentries = _length(args[0])
        if self.name == "RandomSmearing":
            self.recorder["events"] = np.asarray(args[0]).tolist()
            self.recorder["lumis"] = np.asarray(args[1]).tolist()
            self.recorder["phis"] = np.asarray(args[2]).tolist()
            return np.linspace(0.2, 0.8, nentries)
        if self.name == "cb_params":
            values = {0: 0.0, 1: 1.0, 2: 2.0, 3: 1.0}
            return np.full(nentries, values[args[2]])
        if self.name == "poly_params":
            return np.full(nentries, 0.01 if args[2] == 0 else 0.0)
        if self.name == "k_data":
            return np.full(nentries, 0.2)
        if self.name == "k_mc":
            return np.full(nentries, 0.1 if args[1] == "nom" else 0.01)
        raise AssertionError(f"Unexpected correction {self.name}")


class _FakeCorrectionSet:
    def __init__(self, recorder):
        self.recorder = recorder

    def get(self, name):
        return _FakeCorrection(name, self.recorder)


class _FakeCrystalBall:
    def __init__(self, mean, sigma, alpha, n):
        pass

    def invcdf(self, values):
        return values


def test_vendored_backend_imports_without_local_syspath_mutation():
    backend = importlib.import_module("topcoffea.modules.muon_scarekit_backend")

    assert "muonscarekit/scripts" not in str(backend.__file__)
    assert "muon_scarekit_backend" in str(backend.__file__)


def test_vendored_backend_exposes_required_functions():
    backend = importlib.import_module("topcoffea.modules.muon_scarekit_backend")

    for name in ("pt_scale", "pt_resol", "pt_scale_var", "pt_resol_var"):
        assert callable(getattr(backend, name))
    assert "rnd_gen" not in inspect.signature(backend.pt_resol).parameters


def test_nested_random_smearing_broadcasts_event_lumi_and_unflattens(monkeypatch):
    impl = importlib.import_module(
        "topcoffea.modules.muon_scarekit_backend.muon_scarekit"
    )
    monkeypatch.setattr(impl, "CrystallBall", _FakeCrystalBall)
    recorder = {}

    result = impl.get_rndm(
        ak.Array([[0.1, -1.2], [], [1.3]]),
        ak.Array([[0.2, -0.4], [], [2.0]]),
        ak.Array([[12, 14], [], [10]]),
        ak.Array([1001, 1002, 1003]),
        ak.Array([11, 12, 13]),
        _FakeCorrectionSet(recorder),
        nested=True,
    )

    assert recorder["events"] == [1001, 1001, 1003]
    assert recorder["lumis"] == [11, 11, 13]
    assert recorder["phis"] == [0.2, -0.4, 2.0]
    assert ak.to_list(result) == [[0.2, 0.5], [], [0.8]]


def test_flat_random_smearing_accepts_per_muon_event_lumi(monkeypatch):
    impl = importlib.import_module(
        "topcoffea.modules.muon_scarekit_backend.muon_scarekit"
    )
    monkeypatch.setattr(impl, "CrystallBall", _FakeCrystalBall)
    recorder = {}

    result = impl.get_rndm(
        np.array([0.1, -1.2, 1.3]),
        np.array([0.2, -0.4, 2.0]),
        np.array([12, 14, 10]),
        np.array([1001, 1001, 1003]),
        np.array([11, 11, 13]),
        _FakeCorrectionSet(recorder),
        nested=False,
    )

    assert recorder["events"] == [1001, 1001, 1003]
    assert recorder["lumis"] == [11, 11, 13]
    assert result.tolist() == [0.2, 0.5, 0.8]


def test_flat_random_smearing_broadcasts_scalar_event_lumi(monkeypatch):
    impl = importlib.import_module(
        "topcoffea.modules.muon_scarekit_backend.muon_scarekit"
    )
    monkeypatch.setattr(impl, "CrystallBall", _FakeCrystalBall)
    recorder = {}

    impl.get_rndm(
        np.array([0.1, -1.2, 1.3]),
        np.array([0.2, -0.4, 2.0]),
        np.array([12, 14, 10]),
        1001,
        11,
        _FakeCorrectionSet(recorder),
        nested=False,
    )

    assert recorder["events"] == [1001, 1001, 1001]
    assert recorder["lumis"] == [11, 11, 11]


def test_flat_random_smearing_rejects_ambiguous_event_lumi_length(monkeypatch):
    impl = importlib.import_module(
        "topcoffea.modules.muon_scarekit_backend.muon_scarekit"
    )
    monkeypatch.setattr(impl, "CrystallBall", _FakeCrystalBall)

    with pytest.raises(ValueError, match="evtNr must be scalar"):
        impl.get_rndm(
            np.array([0.1, -1.2, 1.3]),
            np.array([0.2, -0.4, 2.0]),
            np.array([12, 14, 10]),
            np.array([1001, 1003]),
            np.array([11, 13]),
            _FakeCorrectionSet({}),
            nested=False,
        )


def test_flat_pt_resol_runs_with_per_muon_event_lumi(monkeypatch):
    impl = importlib.import_module(
        "topcoffea.modules.muon_scarekit_backend.muon_scarekit"
    )
    monkeypatch.setattr(impl, "CrystallBall", _FakeCrystalBall)

    result = impl.pt_resol(
        np.array([30.0, 45.0, 60.0]),
        np.array([0.1, -1.2, 1.3]),
        np.array([0.2, -0.4, 2.0]),
        np.array([12, 14, 10]),
        np.array([1001, 1001, 1003]),
        np.array([11, 11, 13]),
        _FakeCorrectionSet({}),
        nested=False,
    )

    assert len(result) == 3
    assert np.all(np.isfinite(ak.to_numpy(result)))
