# topcoffea documentation

This index links maintained `topcoffea` documentation. Use
[topeft integration](topeft_integration.md) to identify the `topcoffea`
interface and corresponding `topeft` configuration to inspect or change.

`topcoffea` owns reusable mechanisms: correction and selection algorithms,
corrected-object factories, EFT algebra, histogram interfaces, and packaged
shared payload interfaces. A consuming analysis such as
[`topeft`](https://github.com/TopEFT/topeft/blob/run3_test_mmerged/docs/README.md) owns the
concrete era, sample, working-point, category, and activation policy. These
pages describe the shared contract; they do not replace the consuming
analysis's policy documentation.

| Page | Use it for |
| --- | --- |
| [topeft integration](topeft_integration.md) | Start here for the shared-mechanism versus `topeft` analysis-policy boundary and the compatible-checkout contract. |
| [correction interfaces](correction_interfaces.md) | Learn the reusable correction, selection, calibrated-object, variation, and packaged-payload contracts. |
| [EFT interfaces](eft_interfaces.md) | Learn the shared coefficient algebra, `HistEFT`, and generic histogram-storage boundary. |
| [physics extension guides](physics_extension_guides.md) | Safely extend a shared correction, selection, EFT, or histogram mechanism and close the change with its consumers. |
| [remote environment archive contract](environment_archive_contract.md) | Resolve, validate, and reason about remote-executor environment/archive identity. |

For a concrete shared-mechanism contract, read the corresponding interface page
first; for a supported shared change, continue to the extension guide. Source
signatures, focused tests, configuration, and packaged payloads remain the
machine-near authorities where the individual pages identify them.
