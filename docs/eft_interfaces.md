# EFT and histogram interfaces

This page owns the reusable EFT algebra and histogram-mechanism contracts in
`topcoffea`. The consuming analysis owns which samples receive EFT treatment,
which coefficient set is requested, and how coefficient-bearing histograms are
used in categories, plots, cards, or scalings.

## Maintained interface boundary

`topcoffea.modules.eft_helper` supplies the maintained coefficient-array
transformations and quadratic evaluation helpers. `topcoffea.modules.histEFT`
owns `HistEFT`, the coefficient-bearing histogram interface consumed by
`topeft`. `topcoffea.modules.sparseHist` supplies generic sparse/category-axis
storage behavior used underneath that interface.

`WCPoint` and `WCFit` remain legacy/test-oriented classes in the audited
current consumer graph. They are not presented here as the current `topeft`
analysis authority.

## Coefficient representation and evaluation

EFT coefficient arrays encode the polynomial terms in an ordered Wilson-
coefficient basis. The helper contract defines the ordering, number of terms,
and evaluation transformation. These are mathematical/software properties of
the reusable representation; they do not select a physics benchmark or decide
whether a sample should be treated as EFT-capable.

The helper signatures and focused tests are authority for accepted array
shapes, ordering, and evaluation. A caller-supplied coefficient list or sample
metadata is the authority for a concrete analysis instance.

## `HistEFT`

`HistEFT` stores ordinary histogram axes together with EFT coefficient content.
Its public contract covers construction with a coefficient basis, filling
coefficient-bearing or ordinary content, evaluation at a coefficient point,
conversion to an ordinary histogram view, and scaling export. Nominal and
systematic coordinates remain caller-supplied histogram semantics.

There is no single physics default coefficient assignment for this reusable
class. The consumer owns the coefficient names and evaluation point. The class
and its API tests own accepted shapes, storage behavior, and failures.

## `SparseHist` boundary

`SparseHist` provides generic storage and category-axis mechanics. It is a
maintained software substrate, not a physics policy. Analysis documentation
should normally reach it through the `HistEFT` contract rather than assigning
it sample, category, or EFT interpretation.

## Ownership and downstream effects

`topcoffea` owns coefficient algebra and the reusable histogram API. `topeft`
owns per-sample EFT/SM/ignored treatment, coefficient preparation from source
metadata, category and observable filling, SM-point use in data-driven
subtraction, and later scaling consumers. See the consuming
[`topeft` HistEFT reference](https://github.com/TopEFT/topeft/blob/run3_test_mmerged/docs/reference/histeft.md)
and the
[integration ownership map](topeft_integration.md).

Implementation and tests establish the algebra and storage semantics. They do
not establish the scientific motivation for a coefficient basis or benchmark.

## Representative use and modification route

The maintained `tests/test_histEFT.py` pattern constructs a `HistEFT` with
ordinary axes and a caller-supplied WC list, fills coefficient arrays, and
evaluates at an explicit point. The SM point is an empty mapping; a nonzero
point such as `{"ctG": 1.0}` is representative and not a package default.

See [physics extension guides](physics_extension_guides.md) before changing
coefficient ordering, helper transformations, `HistEFT`, or the generic
`SparseHist` substrate. Consumer-specific coefficient lists and sample
treatment remain outside this repository's default policy.
