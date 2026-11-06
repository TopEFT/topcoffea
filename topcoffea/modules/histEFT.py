#! /usr/bin/env python

import hist
import boost_histogram as bh
import awkward as ak
import numpy as np

from typing import Any, List, Mapping, Union

from topcoffea.modules.sparseHist import (
    SparseHist,
    _read_tracked_sparsehist_from_reduce,
)
import topcoffea.modules.eft_helper as efth

try:
    from numpy.typing import ArrayLike, Self
except ImportError:
    ArrayLike = Any
    Number = Any
    Self = Any


_family = hist


class HistEFT(SparseHist, family=_family):
    """Histogram specialized to hold Wilson Coefficients.
    Example:
    ```
    h = HistEFT(
        hist.axis.StrCategory(["ttH"], name="process", growth=True),
        hist.axis.Regular(
            name="ht",
            label="ht [GeV]",
            bins=3,
            start=0,
            stop=30,
            flow=True,
        ),
        wc_names=["ctG"],
        label="Events",
    )

    h.fill(
        process="ttH",
        ht=np.array([1, 1, 2, 15, 25, 100, -100]),
        # per row, quadratic coefficient values associated with one event.
        eft_coeff=[
            [1.1, 2.1, 3.1],     # to (ttH, 1j) bins (one bin per coefficient)
            [1.2, 2.2, 3.2],     # to (ttH, 1j) bins
            [1.3, 2.3, 3.3],     # to (ttH, 2j) bins
            [1.4, 2.4, 3.4],     # to (ttH, 15j) bins
            [1.5, 2.5, 3.5],     # to (ttH, 25j) bins
            [100, 200, 300],     # to (ttH, overflow given 100 >= stop) bins
            [-100, -200, -300],  # to (ttH, underflow given -100 < start) bins
        ],
    )

    # eval at 0, returns a dictionary from categorical axes bins to array, same as just sm,
    # {('ttH',): array([-100. ,   3.6,    1.4,    1.5,  600. ])}
    h.eval({})
    h.eval({"ctG": 0})     # same thing
    h.eval(np.zeros((1,))  # same thing

    # eval at 1, same as adding all bins together per bins of dense axis.
    # {('ttH',): array([-600. ,   19.8,    7.2,    7.5,  600. ])}
    h.eval({"ctG": 1})     # same thing
    h.eval(np.ones((1,))  # same thing

    # instead of h.eval(...), h.as_hist(...) may be used to create a standard hist.Hist with the
    # result of the evaluation:
    hn = h.as_hist({"ctG": 0.02})
    hn.plot1d()
    ```
    """

    def __init__(
        self,
        *args,
        wc_names: Union[List[str], None] = None,
        use_multicell: bool = False,
        store_sumw2: bool = False,
        **kwargs,
    ) -> None:
        """HistEFT initialization is similar to hist.Hist, with the following restrictions:
        - All axes should have a name.
        - Exactly one axis can be dense (i.e. hist.axis.Regular, hist.axis.Variable, or his.axis.Integer)
        - The dense axis should be the last specified in the list of arguments.
        - Categorical axes should be specified with growth=True.

        use_multicell stores coefficients in cells instead of an extra axis.
        store_sumw2 requires MultiCell and adds one SM sumw2 cell, independent
        of raw-count tracking. For nominal-only sumw2, callers must pass
        fill_sumw2=False for non-nominal fills; no systematic names are assumed.
        """

        if not wc_names:
            wc_names = []

        n = len(wc_names)
        self._wc_names = {n: i for i, n in enumerate(wc_names)}
        self._wc_count = n
        self._quad_count = efth.n_quad_terms(n)
        self._use_multicell = bool(use_multicell)
        self._store_sumw2 = bool(store_sumw2)
        if self._store_sumw2 and not self._use_multicell:
            raise ValueError("store_sumw2=True requires use_multicell=True")

        self._init_args_eft = {
            "wc_names": wc_names,
            "use_multicell": self._use_multicell,
            "store_sumw2": self._store_sumw2,
        }

        self._needs_rebinning = kwargs.pop("rebin", False)
        if self._needs_rebinning:
            raise ValueError("Do not know how to rebin yet...")

        if self._use_multicell:
            kwargs["storage"] = hist.storage.MultiCell(
                self._quad_count + int(self._store_sumw2)
            )
        else:
            kwargs.setdefault("storage", "Double")
            if kwargs["storage"] != "Double":
                raise ValueError("only 'Double' storage is supported")

        if args[-1].name == "quadratic_term":
            self._coeff_axis = args[-1]
            args = args[:-1]
        else:
            # no axis for quadratic_term found, creating our own.
            self._coeff_axis = hist.axis.Integer(
                start=0, stop=self._quad_count, name="quadratic_term"
            )
        if self._use_multicell:
            self._coeff_axis = None

        self._dense_axis = args[-1]
        if not isinstance(
            self._dense_axis, (bh.axis.Regular, bh.axis.Variable, bh.axis.Integer)
        ):
            raise ValueError("dense axis should be the last specified")

        reserved_names = ["quadratic_term", "sample", "weight", "thread"]
        if any([axis.name in reserved_names for axis in args]):
            raise ValueError(
                f"No axis may have one of the following names: {','.join(reserved_names)}"
            )

        if self._use_multicell:
            super().__init__(*args, **kwargs)
        else:
            super().__init__(*args, self._coeff_axis, **kwargs)

    def empty_from_axes(self, categorical_axes=None, dense_axes=None, **kwargs):
        return super().empty_from_axes(
            categorical_axes, dense_axes, **self._init_args_eft, **kwargs
        )

    def _raw_count_dense_axes(self):
        return hist.axis.NamedAxesTuple([self._dense_axis])

    @property
    def wc_names(self):
        return list(self._wc_names)

    @property
    def store_sumw2(self) -> bool:
        """Whether this histogram contains an embedded SM sumw2 cell."""
        return self._store_sumw2

    def index_of_wc(self, wc: str):
        return self._wc_names[wc]

    def quadratic_term_index(self, *wcs: List[str]):
        """Given the name of two coefficients, it returns the index
        of the corresponding quadratic coefficient. E.g., if the
        histogram was defined with wc_names=["ctG"]:

        h.quadratic_term_index("sm", "sm")   -> 0
        h.quadratic_term_index("sm", "ctG")  -> 1
        h.quadratic_term_index("ctG", "ctG") -> 2
        """

        def str_to_index(s):
            if s == "sm":
                return 0
            else:
                return self.index_of_wc(s) + 1

        if len(wcs) != 2:
            raise ValueError("List of coefficient names should have length 2")

        wc1, wc2 = map(str_to_index, wcs)
        if wc1 < wc2:
            wc1, wc2 = wc2, wc1

        return int((((wc1 + 1) * wc1) / 2) + wc2)

    def should_rebin(self):
        return self._needs_rebinning

    @property
    def dense_axis(self):
        return self._dense_axis

    def _fill_flatten(self, a, n_events):
        # manipulate input arrays into flat arrays. broadcast_to and ravel used so that arrays are not duplicated in memory
        a = np.asarray(a)
        if a.ndim > 2 or (a.ndim == 2 and (a.shape != (n_events, 1))):
            raise ValueError(
                "Incompatible dimensions between data and Wilson coefficients."
            )

        if a.ndim > 1:
            a = a.ravel()

        # turns [e0, e1, ...] into [[e0, e0, ...],
        #                           [e1, e1, ...],
        #                            [...       ]]
        # and then into       [e0, e0, ..., e1, e1, ..., e2, e2, ...]
        # each value repeated the number of quadratic coefficients.
        return np.broadcast_to(a, (self._quad_count, n_events)).T.ravel()

    def _fill_indices(self, n_events):
        # turns [0, 1, 2, ..., num of quadratic coeffs - 1]
        # into:
        # [0, 1, 2, ..., 0, 1, 2 ...,]
        # repeated n_events times.
        return np.broadcast_to(np.ogrid[0: self._quad_count], (n_events, self._quad_count)).ravel()

    def fill(
        self,
        eft_coeff: ArrayLike = None,  # [num of events x (num of wc coeffs + 1)]
        record_raw_count=None,
        *,
        fill_sumw2: bool = True,
        **values,
    ) -> Self:
        """
        Insert data into the histogram using names and indices, return
        a HistEFT object.

        cat axes:  "s1"                    each categorical axis with one value to fill
        dense axis:[ e0, e1, ... ]         each entry is the value for one event.
        weight:    [ w0, w1, ... ]         weight per event
        eft_coeff: [[c00, c01, c02, ...]   each row is the coefficient values for one event,
                    [c10, c11, c12, ...]
                    ...                 ]  cij is the value of jth coefficient for the ith event.
                                           ei, wi, and ci* go together.

        If eft_coeff is not given, then it is assumed to be [[1, 0, 0, ...], [1, 0, 0, ...], ...]
        weight multiplies eft_coeff once. If coefficients already contain the
        full event weight, omit weight. Embedded SM sumw2 is (weight*c0)**2.
        fill_sumw2=False skips only that contribution, preserving yields and
        raw-count classification. It has no effect without embedded sumw2.
        """

        n_events = len(values[self.dense_axis.name])
        raw_count_values = {self.dense_axis.name: values[self.dense_axis.name]}

        if eft_coeff is None:
            # if eft_coeff not given, assume values only for sm
            eft_coeff = np.broadcast_to(
                np.concatenate((np.ones((1,)), np.zeros((self._quad_count - 1,)))),
                (n_events, self._quad_count),
            )

        eft_coeff = np.asarray(eft_coeff)

        if self._use_multicell:
            if eft_coeff.shape != (n_events, self._quad_count):
                raise ValueError(
                    f"eft_coeff must have shape ({n_events}, {self._quad_count})"
                )
            weight = values.pop("weight", None)
            weight = np.asarray(1.0 if weight is None else weight, dtype=np.float64)
            if weight.ndim == 2 and weight.shape == (n_events, 1):
                weight = weight.ravel()
            if weight.ndim > 1 or (weight.ndim == 1 and weight.shape != (n_events,)):
                raise ValueError("weight must be scalar or have one entry per event")
            payload = eft_coeff * np.broadcast_to(weight, (n_events,))[:, None]
            if self._store_sumw2:
                sumw2 = np.square(payload[:, 0]) if fill_sumw2 else np.zeros(n_events)
                payload = np.column_stack((payload, sumw2))
            values[self.dense_axis.name] = np.asarray(
                values[self.dense_axis.name]
            ).reshape(n_events)
            super().fill(
                **values,
                weight=payload,
                record_raw_count=record_raw_count,
                _raw_count_values=raw_count_values,
            )
            return self

        # turn into [e0, e0, ..., e1, e1, ..., e2, e2, ...]
        values[self._dense_axis.name] = self._fill_flatten(
            values[self._dense_axis.name], n_events
        )

        # turn into: [c00, c01, c02, ..., c10, c11, c12, ...]
        eft_coeff = eft_coeff.ravel()

        # index for coefficient axes.
        # [ 0, 1, 2, ..., 0, 1, 2, ...]
        indices = self._fill_indices(n_events)

        weight = values.pop("weight", None)
        if weight is not None:
            weight = self._fill_flatten(weight, n_events)
            eft_coeff = eft_coeff * weight

        # fills:
        # [e0,      e0,      e0    ..., e1,     e1,     e1,     ...]
        # [ 0,      1,       2,    ..., 0,      1,      2,      ...]
        # [c00*w0, c01*w0, c02*w0, ..., c10*w1, c11*w1, c12*w1, ...]
        super().fill(
            quadratic_term=indices,
            **values,
            weight=eft_coeff,
            record_raw_count=record_raw_count,
            _raw_count_values=raw_count_values,
        )

    def _wc_for_eval(self, values):
        """Set the WC values used to evaluate the bin contents of this histogram
        where the WCs are specified as keyword arguments.  Any WCs not listed are set to zero.
        """
        if values is None:
            return np.zeros(self._wc_count)

        result = values
        if isinstance(values, Mapping):
            result = np.zeros(self._wc_count)
            for wc, val in values.items():
                try:
                    index = self._wc_names[wc]
                    result[index] = val
                except KeyError:
                    msg = f'This HistEFT does not know about the "{wc}" Wilson coefficient. Known coefficients: {list(self._wc_names.keys())}'
                    raise LookupError(msg)

        return np.asarray(result)

    def _coefficient_view(self, values, *, flow):
        """Normalize storage views to (..., physical bins, coefficients)."""
        values = np.asarray(values)
        if self._use_multicell:
            return np.moveaxis(values[..., :self._quad_count, :], -2, -1)
        return values[..., 1:-1] if flow else values

    def nominal_sumw2(self, *, flow=False):
        """Return embedded SM sumw2, without extrapolating to other WC points."""
        if not self._store_sumw2:
            raise RuntimeError("This HistEFT does not contain embedded nominal sumw2")
        return {
            key: values[self._quad_count, ...]
            for key, values in self.view(flow=flow).items()
        }

    def _ibinary_op(self, other, op: str):
        if isinstance(other, SparseHist) and (
            self._use_multicell or getattr(other, "_use_multicell", False)
        ):
            if (
                not isinstance(other, HistEFT)
                or self._use_multicell != other._use_multicell
                or self._store_sumw2 != other._store_sumw2
                or self.wc_names != other.wc_names
                or tuple(self.dense_axes) != tuple(other.dense_axes)
            ):
                raise ValueError("HistEFT arithmetic requires matching EFT storage and axes")

        if op == "__isub__" and (
            self._store_sumw2 or getattr(other, "_store_sumw2", False)
        ):
            raise NotImplementedError("Subtraction with embedded sumw2 is unsupported")

        if self._store_sumw2:
            if op == "__iadd__" and not isinstance(other, HistEFT):
                if np.isscalar(other) and other == 0:
                    return self
                raise TypeError("Embedded sumw2 supports only histogram addition or zero")
            if op in ("__imul__", "__itruediv__", "__idiv__"):
                if not np.isscalar(other) or np.asarray(other).dtype.kind not in "iuf":
                    raise TypeError("Embedded sumw2 scaling requires a real numeric scalar")
                if op != "__imul__" and other == 0:
                    raise ZeroDivisionError("Cannot divide a HistEFT by zero")
                super()._ibinary_op(other, op)
                # SparseHist scales every cell once; sumw2 needs a second power.
                for dense in self._dense_hists.values():
                    variance = dense.view(flow=True)[self._quad_count, ...]
                    if op == "__imul__":
                        variance *= other
                    else:
                        variance /= other
                return self
        return super()._ibinary_op(other, op)

    def eval(self, values):
        """Extract the sum of weights arrays from this histogram
        Parameters
        ----------
        values: ArrayLike or Mapping or None
            The WC values used to evaluate the bin contents of this histogram. Either an array with the values, or a dictionary. If None, use an array of zeros.
        """

        values = self._wc_for_eval(values)

        out = {}
        for sparse_key, hvs in self.view(flow=True, as_dict=True).items():
            out[sparse_key] = efth.calc_eft_weights(
                self._coefficient_view(hvs, flow=True), values
            )
        return out

    def as_hist(self, values):
        """Construct a regular histogram evaluated at values.
        (Like self.eval(...) but result is a histogram.)
        Parameters
        ----------
        values: ArrayLike or Mapping or None
            The WC values used to evaluate the bin contents of this histogram. Either an array with the values, or a dictionary. If None, use an array of zeros.
        overflow: bool
            Whether to include under and overflow bins.
        """
        evals = self.eval(values=values)
        hist_args = dict(self._init_args, storage="Double")
        nhist = hist.Hist(
            *[axis for axis in self.axes if axis != self._coeff_axis], **hist_args
        )

        sparse_names = self.categorical_axes.name
        for sp_val, arrs in evals.items():
            sp_key = dict(zip(sparse_names, sp_val))
            nhist[sp_key] = arrs
        return nhist

    def __reduce__(self):
        args = dict(self._init_args)
        args.update(self._init_args_eft)

        if self.track_raw_counts:
            return (
                _read_tracked_sparsehist_from_reduce,
                (
                    type(self),
                    list(self.categorical_axes),
                    [self.dense_axis],
                    args,
                    self._dense_hists,
                    self._validated_raw_count_states(),
                ),
            )

        return (
            type(self)._read_from_reduce,
            (
                list(self.categorical_axes),
                [self.dense_axis],
                args,
                self._dense_hists,
            ),
        )

    def make_scaling(self, flow='show', wc_list=None):
        """
        returns np.Array of scaling for scalings.json with the interference model list with flow bins
        ----------
        wc_list: list or array of WCs
            if None: will use self.wc_names for WCs
            if list or array: will use wc_list for WCs
        """
        scaling = self._coefficient_view(self.values(flow=True), flow=True).copy()
        if wc_list is not None:
            scaling = efth.remap_coeffs(self.wc_names,wc_list,scaling)
        else:
            wc_list = self.wc_names
        #check if any non-flow bins have zero sm contribution
        if bool(ak.any((scaling[...,0] == 0) & ak.any(scaling != 0, axis=-1))):
            raise Exception('At least one bin found with no SM contribution and a BSM contribution!')
        skip = 0
        step = 2
        for i in np.ogrid[0: efth.n_quad_terms(len(wc_list))]:
            if i == skip:
                skip += step
                step += 1
            else:
                scaling[...,i] /= 2
        if flow=='sum':
            scaling[...,-2,:] += scaling[...,-1,:]
            scaling[...,1,:] += scaling[...,0,:]
            scaling = scaling[...,1:-1,:]
        elif flow !='show':
            raise Exception(f'Invalid flow options {flow} selected! Please select from "show" or "sum".')
        mask = scaling[...,0] != 0
        scaling[mask,:] = scaling[mask,:]/np.expand_dims(scaling[mask,0], -1) #divide by sm
        return scaling
    @classmethod
    def _read_from_reduce(
        cls,
        cat_axes,
        dense_axes,
        init_args,
        dense_hists,
    ):
        return super()._read_from_reduce(
            cat_axes,
            dense_axes,
            init_args,
            dense_hists,
        )

    # this method should be moved to eft_helper once HistEFT is replaced.
    # the only change is that hist.view includes a under/overflow columns, thus
    # the index should start at 1
    def calc_eft_weights(self, q_coeffs, wc_values):
        """Calculate the weights for a specific set of WC values.

        Args:
            q_coeffs: Array specifying a set of quadric coefficients parameterizing the weights.
                    The last dimension should specify the coefficients, while any earlier dimensions
                    might be for different histogram bins, events, etc.
            wc_values: A 1D array specifying the Wilson coefficients corrersponding to the desired weight.

        Returns:
            An array of the weight values calculated from the quadratic parameterization.
        """

        # Prepend "1" to the start of the WC array to account for constant and linear terms
        wcs = np.hstack((np.ones(1), wc_values))

        # Initialize the array that will return the coefficients.  It
        # should be the same shape as q_coeffs except missing the last
        # dimension.
        out = np.zeros_like(q_coeffs[..., 0])

        # Now loop over the terms and multiply them out
        index = 1  # start at second column, as first is 0s from boost_histogram underflow (real underflow is row 0)
        for i in range(len(wcs)):
            for j in range(i + 1):
                out += q_coeffs[..., index] * wcs[i] * wcs[j]
                index += 1
        return out
