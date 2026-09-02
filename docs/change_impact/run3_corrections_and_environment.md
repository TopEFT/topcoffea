# Run 3 correction and environment contract changes

> Historical change-impact record. This page describes a bounded implementation
> state and is not the current operating procedure. Use the maintained
> [environment archive contract](../environment_archive_contract.md) for
> archive lifecycle and validation behavior.

## Summary

This branch is an intentionally broad shared-library integration update.
Relative to `run3_test_mmerged_anpicci`, it refreshes packaged Run 3 JME
payloads, supports the paired Run 3 JER scale-factor schema, validates and
fingerprints remote-executor environment archives, enforces NumPy/scalar inputs
at correctionlib boundaries, and documents the shared-mechanism boundary with
consumers such as `topeft`.

## Semantic domains

| Domain | Main behavior change | Validation basis |
| --- | --- | --- |
| Run 3 JME payloads and JER schemas | Refreshes four packaged NanoAODv12 JME payload families and supports direct-systematic or paired scale-factor/uncertainty JER schemas | Exact source/destination digest and gzip checks, thirteen focused JER tests and payload probes, and broader shared/consumer JME regression suites |
| Remote environment identity | Adds archive manifests, digest/fingerprint validation, cache lifecycle, snapshot semantics, and relevant untracked-source identity | Six archive tests plus four consumer tests, followed by eight focused untracked-source/cache tests |
| Correctionlib NumPy boundaries | Converts high-level Awkward arguments to flat NumPy arrays while preserving scalar/string inputs, formulas, output order, and reconstructed shapes | Thirty-three focused boundary tests, forty-six broader shared JME tests, seventy-four consumer tests, a 63-callsite audit, and zero-difference bounded file probes |
| Shared interface documentation | Records correction, environment, EFT, extension, and consuming-analysis ownership boundaries | Static link, source-consistency, and navigation review |

The validation above is targeted. It does not claim a full test-suite run, a
new environment archive build, or a production campaign.

## Impact classification

- `invocation_change`: **none for supported consumers**. The corrected-object,
  correction-helper, and environment entry points retain their normal call
  roles.
- `configuration_change`: **none at the shared-library boundary**. Consuming
  analyses still own concrete era, collection, tag, working point, enabled
  variation, and downstream policy.
- `artifact_contract_change`: **behavior changed compatibly** for JME payloads
  and environment archives; the NumPy boundary is backward-compatible at the
  public helper level.
- `existing_artifact_status`: **mixed by artifact class**. Environment archives
  and analysis outputs require separate handling below.
- `downstream_workflow_change`: **required for dependent JME consumers**. A
  consumer using the refreshed payload/JER behavior must install or deploy this
  compatible update before running.
- `operational_practice_change`: **required**. Record exact shared and consumer
  revisions, validate cached archives, and do not substitute stale payload or
  manifest copies.

## Affected interfaces

| Interface | Invocation | Configuration and output impact |
| --- | --- | --- |
| `topcoffea.modules.CorrectedJetsFactory.CorrectedJetsFactory` | Existing factory construction and `build` role remain; forward stochastic-JER suppression is an optional mechanism | Uses the refreshed correction set and emits nominal/up/down JER variations in fixed order |
| `get_jer_sf_variations` | Same jet/factory inputs | Accepts a direct `systematic` schema or requires a compatible `SFUncertainty` companion for the paired schema; missing/incompatible companions fail instead of falling back to nominal |
| `topcoffea.modules.corrections` helpers | Public helper call patterns remain | Correctionlib receives NumPy/scalar inputs; correction formulas and consumer-owned era/policy choices are unchanged |
| `topcoffea.modules.remote_environment.get_environment` | Existing high-level archive resolver remains | Reuses only a manifest-validated matching cache key; otherwise creates a new resolved archive and manifest |
| `validate_environment_archive` | Explicit archive validation | Distinguishes valid, stale, unverifiable, and invalid records; snapshot mode relaxes provenance comparison only, never tar or digest integrity |

## Artifact and compatibility contract

- The packaged Run 3 JME payload files are runtime inputs, not generated
  documentation. Use `topcoffea_path` so an installed package resolves the
  selected payload rather than a duplicate local copy.
- Direct JER scale factors with a `systematic` input remain supported. Paired
  Run 3 payloads require a signature-compatible uncertainty companion and emit
  nominal, up, and down arrays in that order.
- Correctionlib receives flat NumPy arrays and scalar/string arguments. Jagged
  structure is restored by the owning public helper; formulas and physics policy
  are not moved into the representation adapter.
- A current environment archive consists of the tarball and adjacent manifest.
  The manifest binds the archive digest, resolved specification, Python version,
  editable-package commits and source fingerprints, and relevant untracked
  source evidence.
- Do not edit an archive or manifest in place. A changed source/specification
  resolves to a new fingerprinted cache key.

## Existing artifacts

| Artifact class | Status | Required action |
| --- | --- | --- |
| Valid archive with matching current manifest and fingerprint | Reusable as-is | Validate before use and preserve the pair |
| Stale archive with intact manifest/digest | Rerun/rebuild required for strict current execution | Resolve the current request and create its cache key |
| Unverifiable historical archive | Unsupported for strict current execution | Snapshot inspection may retain historical evidence; rebuild before current use |
| Invalid archive or digest mismatch | Unsupported | Discard from execution and rebuild from the authoritative request |
| Analysis output produced with older JME/JER payload behavior | Reusable only as historical output | Rerun when making claims that depend on refreshed corrections or paired JER variations |
| Analysis output produced with compatible revisions and validated contracts | Reusable for its certified purpose | Preserve exact consumer/shared revisions and the consumer-owned provenance |

## Downstream workflows

`topeft` is the directly inspected consumer for the changed JME, correctionlib,
and environment mechanisms. It owns concrete tags, era selection,
forward-stochastic-JER policy, enabled variations, processor dispatch, and
downstream categories. Therefore:

- PR opening and review can proceed in parallel across the two repositories;
- this shared-library update must be integrated or deployed before dependent
  topeft JME/correction runtime;
- availability of a mechanism here does not select it as analysis policy;
- historical analysis artifacts are not made current by relabeling their
  metadata after the shared library changes.

## Operational practice

1. Install/select the exact `topcoffea` revision required by the consuming
   analysis and record both revisions with campaign evidence.
2. Resolve packaged data through `topcoffea_path`; do not maintain a shadow
   payload map in the consumer.
3. Let `get_environment` choose the fingerprinted cache key and validate the
   archive/manifest before remote execution.
4. Rebuild rather than editing stale environment records in place.
5. Treat missing or incompatible paired JER uncertainty payloads as errors, not
   nominal-only operation.

## Unchanged interfaces

- `CorrectedJetsFactory` remains the shared corrected-jet factory and preserves
  nominal/up/down variation order.
- Public correction helpers retain their call roles; the NumPy conversion is at
  the correctionlib boundary and does not change correction formulas.
- `get_environment` remains the high-level environment resolver; callers do not
  need a second cache registry.
- Concrete analysis era/tag, working-point, activation, and category choices
  remain consumer-owned.

These are scoped stability statements, not a claim that payload contents,
archive acceptance, or all internal interfaces are unchanged.

## Known limitations and deferred follow-ups

No real environment archive or production campaign was created for this impact
record. Axis-merge prevalidation, family-mapping insertion order, period-name
cleanup, a campaign-level CR plotting/output interface, generated API work, and
the TAU workstream remain outside this integration.
