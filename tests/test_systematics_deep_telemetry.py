"""Operation accounting and real method-1a/correction OFF/ON equivalence."""
from types import SimpleNamespace

import awkward as ak
import numpy as np
import pytest

from topcoffea.modules import corrections as corrections


def reporter(rows):
    return SimpleNamespace(mark=lambda phase, **row: rows.append(dict(phase=phase, **row)))


def test_exact_immediate_child_subtraction_and_aggregation(monkeypatch):
    now = [0]
    monkeypatch.setattr(corrections, '_telemetry_time', SimpleNamespace(
        perf_counter_ns=lambda: now[0], process_time_ns=lambda: 2 * now[0]))
    rows = []
    with corrections.telemetry_context(reporter(rows), 'JESUp', 7):
        with corrections.telemetry_operation('parent'):
            now[0] = 10
            for stop in [20, 30]:
                with corrections.telemetry_operation('child'):
                    now[0] = stop
            now[0] = 50
    by_name = {r['operation']: r for r in rows}
    assert by_name['child']['parent_operation'] == 'parent'
    assert by_name['child']['calls'] == 2
    assert by_name['child']['inclusive_wall_ns'] == 20
    assert by_name['parent']['inclusive_wall_ns'] == 50
    assert by_name['parent']['exclusive_wall_ns'] == 30
    assert by_name['parent']['exclusive_cpu_ns'] == 60
    assert all(r['syst_var'] == 'JESUp' and r['iteration_index'] == 7 for r in rows)
    assert [r['operation'] for r in rows] == ['child', 'parent']
    assert corrections._correction_telemetry.get() is None


def test_off_never_reads_clocks_or_emits(monkeypatch):
    def unexpected():
        raise AssertionError('disabled telemetry used a clock')
    monkeypatch.setattr(corrections, '_telemetry_time', SimpleNamespace(
        perf_counter_ns=unexpected, process_time_ns=unexpected))
    with corrections.telemetry_context(None):
        with corrections.telemetry_operation('not_recorded'):
            value = 17
    assert value == 17
    assert corrections._correction_telemetry.get() is None


def test_optional_emission_failure_preserves_scientific_result(capsys):
    def unavailable_sink(*args, **kwargs):
        raise OSError('sink unavailable')
    with corrections.telemetry_context(SimpleNamespace(mark=unavailable_sink)):
        with corrections.telemetry_operation('result'):
            result = 42
    assert result == 42
    assert 'optional correction telemetry emission failed' in capsys.readouterr().err
    assert corrections._correction_telemetry.get() is None


def test_failed_calls_and_nested_context_restore():
    outer, inner = [], []
    with corrections.telemetry_context(reporter(outer), 'nominal', 0):
        state = corrections._correction_telemetry.get()
        with pytest.raises(ValueError, match='actual exception'):
            with corrections.telemetry_context(reporter(inner), 'JERDown', 1):
                with corrections.telemetry_operation('failed'):
                    raise ValueError('actual exception')
        assert corrections._correction_telemetry.get() is state
        with corrections.telemetry_operation('after_failure'):
            pass
    assert inner[0]['calls'] == inner[0]['failures'] == 1
    assert outer[0]['operation'] == 'after_failure' and outer[0]['failures'] == 0
    assert corrections._correction_telemetry.get() is None


@pytest.mark.parametrize('variation', ['central', 'up_correlated', 'down_correlated'])
@pytest.mark.parametrize('year,method', [('2018', 'deepJet_comb'), ('2022', 'deepJet_comb')])
def test_native_btag_payload_and_method1a_equivalence(year, method, variation):
    jets = ak.Array([[{'pt': 55., 'eta': .3, 'hadronFlavour': 5},
                      {'pt': 80., 'eta': -1., 'hadronFlavour': 4}], []])
    off = corrections.btag_sf_eval(jets, 'M', year, method, variation)
    rows = []
    with corrections.telemetry_context(reporter(rows), variation, 3):
        on = corrections.btag_sf_eval(jets, 'M', year, method, variation)
    assert ak.to_list(on) == ak.to_list(off)
    operations = {r['operation'] for r in rows}
    assert 'topcoffea.btag_sf_eval.payload_read_decompress_and_correctionset' in operations
    assert 'topcoffea._evaluate_correctionlib.evaluate' in operations
    assert 'topcoffea.btag_sf_eval.unflatten' in operations
    eff_a, eff_b = ak.Array([[.6, .4], []]), ak.Array([[.8, .7], []])
    cut_a, cut_b, cut_c = ak.Array([[True, False], []]), ak.Array([[False, True], []]), ak.Array([[False, False], []])
    expected = corrections.get_method1a_wgt_doublewp(eff_a, eff_b, on, on, cut_a, cut_b, cut_c)
    with corrections.telemetry_context(reporter(rows), variation, 3):
        actual = corrections.get_method1a_wgt_doublewp(eff_a, eff_b, on, on, cut_a, cut_b, cut_c)
    for left, right in zip(actual, expected):
        np.testing.assert_array_equal(ak.to_numpy(left), ak.to_numpy(right))
    assert all(r['failures'] == 0 for r in rows)


def test_telemetry_does_not_materialize_lazy_objects():
    class Deferred:
        def __array__(self, *args):
            raise AssertionError('forced materialization')
        def __iter__(self):
            raise AssertionError('forced iteration')
    lazy = Deferred()
    rows = []
    with corrections.telemetry_context(reporter(rows)):
        with corrections.telemetry_operation('setup_only'):
            result = lazy
    assert result is lazy
    assert rows[0]['operation'] == 'setup_only' and rows[0]['calls'] == 1


def test_native_factory_off_on_equivalence(tmp_path):
    import json
    from topcoffea.modules.JECStack import JECStack
    from topcoffea.modules.CorrectedJetsFactory import CorrectedJetsFactory
    from topcoffea.modules.CorrectedMETFactory import CorrectedMETFactory
    from topcoffea.modules.Type1CorrectedMETFactory import Type1CorrectedMETFactory
    payload = tmp_path / 'native_test_jec.json'
    payload.write_text(json.dumps({'schema_version': 2, 'corrections': [
        {'name': 'Test_' + level + '_AK4Test', 'version': 1,
         'inputs': [{'name': 'JetPt', 'type': 'real'}],
         'output': {'name': 'correction', 'type': 'real'}, 'data': factor}
        for level, factor in [('L1FastJet', 1.05), ('L2Relative', 1.1)]]}))
    name_map = {'JetPt': 'pt', 'JetMass': 'mass', 'JetEta': 'eta', 'JetPhi': 'phi',
                'JetA': 'area', 'ptRaw': 'pt_raw', 'massRaw': 'mass_raw', 'Rho': 'rho',
                'ptGenJet': 'pt_gen', 'METpt': 'pt', 'METphi': 'phi',
                'RawMETpt': 'pt', 'RawMETphi': 'phi', 'JetMuonSubtrFactor': 'muonSubtrFactor',
                'JetChEmEF': 'chEmEF', 'JetNeEmEF': 'neEmEF', 'CorrT1JetPt': 'rawPt',
                'CorrT1JetPhi': 'phi', 'CorrT1JetEta': 'eta', 'CorrT1JetArea': 'area',
                'CorrT1JetMuonSubtrFactor': 'muonSubtrFactor',
                'UnClusteredEnergyDeltaX': 'MetUnclustEnUpDeltaX',
                'UnClusteredEnergyDeltaY': 'MetUnclustEnUpDeltaY'}
    jets = ak.Array([[{'pt': 50., 'mass': 10., 'pt_raw': 40., 'mass_raw': 8.,
                      'eta': .2, 'phi': .1, 'area': .5, 'rho': 20., 'pt_gen': 42.,
                      'muonSubtrFactor': .1, 'chEmEF': .2, 'neEmEF': .1}]])
    corr_t1 = ak.Array([[{'rawPt': 20., 'phi': .3, 'eta': .3, 'area': .4,
                         'muonSubtrFactor': .1, 'rho': 20.}]])
    met = ak.Array([{'pt': 100., 'phi': .2, 'MetUnclustEnUpDeltaX': 2.,
                    'MetUnclustEnUpDeltaY': 3.}])
    rows, results = [], []
    for enabled in [False, True]:
        with corrections.telemetry_context(reporter(rows) if enabled else None, 'nominal', 0):
            stack = JECStack(use_clib=True, jec_tag='Test', jec_levels=['L1FastJet', 'L2Relative'],
                             jet_algo='AK4Test', json_path=str(payload))
            corrected = CorrectedJetsFactory(dict(name_map), stack, run=None).build(jets, {})
            corrected_met = CorrectedMETFactory(dict(name_map)).build(met, corrected, {})
            type1_met = Type1CorrectedMETFactory(dict(name_map), stack).build(met, met, jets, corr_t1, {})
        # Consumer assertions materialize only the outputs of both identical
        # native calls. Instrumentation itself never forces those values.
        results.append((ak.to_list(corrected.pt), ak.to_list(corrected.mass),
                        ak.to_list(corrected_met.pt), ak.to_list(corrected_met.phi),
                        ak.to_list(type1_met.pt), ak.to_list(type1_met.phi)))
    assert results[0] == results[1]
    operations = {r['operation'] for r in rows}
    assert operations >= {'JECStack._initialize_clib.payload_read_decompress_and_correctionset',
                          'CorrectedJetsFactory.build.total', 'CorrectedMETFactory.build.total',
                          'Type1CorrectedMETFactory.build.total'}
    assert all(r['failures'] == 0 for r in rows)
