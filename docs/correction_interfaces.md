# Correction interfaces reference

This page covers correction contracts analysts and developers are expected to
call or extend. Source signatures remain authoritative; this page owns semantic
and schema details that signatures do not express.

## Physics mechanism and policy boundary

`topcoffea` owns reusable evaluation algorithms, array and schema contracts,
corrected-object construction, variation products, and packaged shared payload
interfaces. A consuming analysis owns the concrete era, collection, tag,
working point, payload family, enabled variation, and downstream selection or
category policy. In particular, the forward stochastic-JER option is a factory
mechanism here; its default and activation for the maintained `topeft` analysis
belong to `topeft`.

This page describes what each mechanism changes. It does not infer why an
analysis chose a calibration family, working point, mass window, uncertainty
subset, or era policy from the implementation alone.

## Shared weight and selection mechanisms

`topcoffea.modules.corrections` supplies reusable b-tag Method1a, pileup,
parton-shower, and renormalization/factorization-scale weight mechanisms. The
caller supplies the applicable era, sample branches, tagger or working point,
and activation policy. Central and exposed variations change the event weights
returned to the consumer; the consuming analysis decides which histogram
templates or nuisance groups use them.

`topcoffea.modules.object_selection` supplies maintained tight-jet and Run-3
jet-ID predicates. `topcoffea.modules.event_selection` supplies generic
dataset-overlap, SFOS, and Z-window utilities. Their input/output and failure
contracts are shared here. Concrete jet cuts, trigger lists, dataset priority,
mass windows, and CR/SR use remain analysis policy.

The LHE scale-weight helper does not make the deprecated `topeft`
renormalization-envelope option active; envelope and downstream template policy
belong to the caller.

## Correctionlib NumPy boundary

Correctionlib evaluators receive flat NumPy arrays, not Awkward-1 high-level
arrays. `topcoffea.modules.corrections._evaluate_correctionlib` converts
high-level Awkward arguments with `ak.to_numpy` and passes scalar/string inputs
through. Public helpers flatten jagged inputs, evaluate the payload, and
reconstruct jagged output where required.

Jet/MET factories follow the same rule. In
`topcoffea.modules.CorrectedJetsFactory.get_corr_inputs`, mapped jet fields are
converted to NumPy before `Correction.evaluate`. Type-1 MET JEC inputs and
uncertainties are evaluated on flat NumPy arrays. This interoperability
boundary does not change correction formulas.

The vendored muon ScaReKit backend also converts correctionlib inputs at its
evaluation boundary. Its formulas and upstream provenance remain documented in
`topcoffea/modules/muon_scarekit_backend/VERSION.md`.

## JER scale-factor schemas

`topcoffea.modules.CorrectedJetsFactory.get_jer_sf_variations(jets,
scale_factor, correction_set, name_map, run)` returns `numpy.float32` arrays in
fixed `(nominal, up, down)` order. It supports:

1. a direct correction with a string `systematic` input, evaluated at `nom`,
   `up`, and `down`;
2. a paired Run 3 form whose `ScaleFactor` has no `systematic` input and whose
   sibling `SFUncertainty` supplies a fractional uncertainty. Results are
   `nominal`, `nominal * (1 + uncertainty)`, and
   `nominal * (1 - uncertainty)`.

The companion is mandatory for the paired form. Both corrections must be
usable correctionlib objects with real output, supported input types, and
identical non-systematic signatures. Missing, misnamed, or incompatible
companions raise `ValueError`; there is no nominal-only fallback.

`get_corr_inputs(jets, corr_obj, name_map, run, cache=None, corrections=None,
variation=None)` resolves correction inputs by declared name. `systematic`
requires an explicit variation; `run` is broadcast over the jet shape; all
other inputs require mappings. Missing mappings and unsupported systematic
schemas fail closed.

## `CorrectedJetsFactory`

`CorrectedJetsFactory(name_map, jec_stack, run,
suppress_forward_eta_stochastic_jer=False)` accepts a `JECStack`. It loads
correctionlib or legacy corrections, validates required mappings, and selects
hybrid or stochastic JER according to the generator match. `build(jets,
lazy_cache)` requires an Awkward array and a cache and returns corrected jets
with configured JER/JES variations.

The optional forward-eta suppression affects only stochastic smearing for
`2.5 < abs(eta) < 3.0`; callers own the policy selecting it. The factory clips
smeared pT to its positive minimum and preserves float32 correction arrays.

Payload names are supplied by `JECStack` and packaged JME JSON. Do not hard-code
a second campaign map in an analysis. `topcoffea.modules.corrections.clib_year_map`
and installed package data are the package authorities.

## Type-1 corrected MET

`topcoffea.modules.Type1CorrectedMETFactory.Type1CorrectedMETFactory` owns
Type-1 MET construction from stored/raw MET, raw jets, and corrected Type-1
jets. `build(...)` evaluates L1/full corrections, transports JES/JER jet
variations into MET, and preserves available unclustered-energy variations.
`uncertainties()` reports configured variation names. Inputs must satisfy the
factory's `name_map` and Awkward collection contracts.

The current `topeft` Type-1 path consumes the maintained helper in
`topcoffea.modules.corrections`. The older `CorrectedMETFactory` remains a
maintained specialist/noncore interface and must not be substituted as the
current Type-1 owner without a proven consumer.

## Shared default and payload authorities

`topcoffea/params/params.json` owns shared era luminosity values and the
packaged tagger/era/working-point values. Files under `topcoffea/data` and
`topcoffea/params` are package payload
authorities selected through maintained loaders. Consumers should link these
files rather than copying numeric tables into analysis prose.

Payload provenance may name a POG or calibration source. That identifies the
external authority without re-establishing its derivation in this repository.

## TAU packaged-payload boundary

The package provides two distinct TAU payload surfaces:

- `topcoffea/data/POG/TAU` contains packaged POG tau correction and energy
  payloads;
- `topcoffea/data/TauSF` contains the custom analysis jet-to-tau fake payloads.

The consuming `topeft` analysis owns era, working-point, variation, and
payload-family selection, along with the analysis meaning of the resulting
weights or shifts. This package documents payload location and provenance; it
does not duplicate an analysis nuisance map or define downstream correlations.

The maintained concrete authorities are
[`params.json`](../topcoffea/params/params.json) and the packaged
[`data/`](../topcoffea/data) tree. Mechanisms without one universal payload
default are parameterized by the caller's era, collection, and selector.

## Representative shared-helper use

The tight-jet predicate is parameterized rather than tied to a `topeft`
working point:

```python
from topcoffea.modules.object_selection import is_tight_jet

mask = is_tight_jet(
    jets.pt,
    jets.eta,
    jets.jetId,
    pt_cut=30.0,
    eta_cut=2.4,
    id_cut=1,
)
```

The numbers illustrate the call shape; they are not a shared analysis default.
The consumer owns its thresholds, and the
[physics extension guide](physics_extension_guides.md) owns the safe mechanism
change route.

## Packaged Run 3 JME payloads

| Year token | Package directory |
| --- | --- |
| `2022` | `data/POG/JME/2022_Summer22` |
| `2022EE` | `data/POG/JME/2022_Summer22EE` |
| `2023` | `data/POG/JME/2023_Summer23` |
| `2023BPix` | `data/POG/JME/2023_Summer23BPix` |

Use `topcoffea.modules.paths.topcoffea_path`. Each campaign's `changes.md`
records refreshed payload identity. JSON schema and factory validation—not a
release-note filename—determine whether JER uses the direct or paired form.

## Extension and validation

When adding or updating a payload, update its file and release-note provenance,
change the package campaign/name map only if selection changes, preserve the
NumPy evaluation boundary, validate declared inputs/output/companion signature
and nominal/up/down order, and update focused tests:

- `tests/test_corrected_jets_factory_jer_sf.py`;
- `tests/test_corrected_met_factory.py`;
- `tests/test_correctionlib_numpy_boundaries.py`.

For the vendored muon backend, also update `VERSION.md` with upstream identity
and the exact adapter patch. Do not alter vendored physics formulas while
changing only the array boundary.

Supported mechanism changes and the corresponding consumer boundary are
collected in [physics extension guides](physics_extension_guides.md). Concrete
`topeft` policy and downstream use are described in
[topeft integration](topeft_integration.md) and the consuming
[`topeft` correction reference](https://github.com/TopEFT/topeft/blob/HEAD/docs/reference/corrections_weights_and_systematics.md).
