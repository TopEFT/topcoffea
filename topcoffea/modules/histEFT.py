#! /usr/bin/env python

import hist
import boost_histogram as bh
import numpy as np

from typing import Any, List, Mapping, Union

from topcoffea.modules.sparseHist import SparseHist
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
        use_multicell: bool = True,
        store_sumw2: bool = False,
        **kwargs,
    ) -> None:
        """HistEFT initialization is similar to hist.Hist, with the following restrictions:
        - All axes should have a name.
        - Exactly one axis can be dense (i.e. hist.axis.Regular, hist.axis.Variable, or his.axis.Integer)
        - The dense axis should be the last specified in the list of arguments.
        - Categorical axes should be specified with growth=True.
        """

        if not wc_names:
            wc_names = []

        n = len(wc_names)

        self._use_multicell = use_multicell
        self._wc_names = {n: i for i, n in enumerate(wc_names)}
        self._wc_count = n
        self._quad_count = efth.n_quad_terms(n)
        self._store_sumw2 = bool(store_sumw2)

        if self._store_sumw2 and not self._use_multicell:
            raise ValueError("store_sumw2=True requires use_multicell=True")

        self._yield_slice = slice(0, self._quad_count)
        self._sumw2_index = (
            self._quad_count
            if self._store_sumw2
            else None
        )
        self._cell_count = (
            self._quad_count + 1 if self._store_sumw2 else self._quad_count
        )

        # Preserve the backend selection when recreating histograms from axes.
        self._init_args_eft = {
            "wc_names": list(wc_names),
            "use_multicell": use_multicell,
            "store_sumw2": self._store_sumw2,
        }

        self._needs_rebinning = kwargs.pop("rebin", False)
        if self._needs_rebinning:
            raise ValueError("Do not know how to rebin yet...")
        
        # Use vector-valued MultiCell storage for EFT coefficients.
        if self._use_multicell:
            kwargs["storage"] = hist.storage.MultiCell(self._cell_count)
            self._coeff_axis = None

            if args[-1].name == "quadratic_term":
                args = args[:-1]

        else:
            # Legacy storage keeps EFT coefficients on a quadratic_term axis.
            kwargs.setdefault("storage", "Double")
            if kwargs["storage"] != "Double":
                raise ValueError("only 'Double' storage is supported")

            if args[-1].name == "quadratic_term":
                self._coeff_axis = args[-1]
                args = args[:-1]
            else:
                self._coeff_axis = hist.axis.Integer(
                    start=0, stop=self._quad_count, name="quadratic_term"
                )

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
        
        # MultiCell embeds coefficients in storage and needs no coefficient axis.
        if self._use_multicell:
            super().__init__(*args, **kwargs)
        else:
            super().__init__(*args, self._coeff_axis, **kwargs)
        
    def empty_from_axes(self, categorical_axes=None, dense_axes=None, **kwargs):
        return super().empty_from_axes(
            categorical_axes, dense_axes, **self._init_args_eft, **kwargs
        )

    @property
    def wc_names(self):
        return list(self._wc_names)

    @property
    def store_sumw2(self) -> bool:
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
        eft_coeff: ArrayLike = None,  # [num of events x (num of wc coeffs + 1)] # type: ignore
        fill_sumw2: bool = True,
        **values,
    ) -> Self: # type: ignore
        
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
        """

        n_events = len(values[self.dense_axis.name])

        if eft_coeff is None:
            # if eft_coeff not given, assume values only for SM
            eft_coeff = np.broadcast_to(
                np.concatenate((np.ones((1,)), np.zeros((self._quad_count - 1,)))),
                (n_events, self._quad_count),
            )

        eft_coeff = np.asarray(eft_coeff)

        # MultiCell fills once per event with the full EFT coefficient vector.
        if self._use_multicell:
            if eft_coeff.shape != (n_events, self._quad_count):
                raise ValueError(
                    f"eft_coeff must have shape ({n_events}, {self._quad_count}) "
                    f"for MultiCell mode, got {eft_coeff.shape}"
                )

            weight = values.pop("weight", None)

            if weight is None:
                weight = np.ones(n_events, dtype=np.float64)
            else:
                weight = np.asarray(weight)

                if weight.ndim == 0:
                    weight = np.full(n_events, weight.item(), dtype=np.float64)
                elif weight.ndim == 1:
                    if weight.shape != (n_events,):
                        raise ValueError(
                            f"weight must have shape ({n_events},), got {weight.shape}"
                        )
                    weight = weight.astype(np.float64, copy=False)
                elif weight.ndim == 2 and weight.shape == (n_events, 1):
                    weight = weight[:, 0].astype(np.float64, copy=False)
                else:
                    raise ValueError(
                        "weight must be a scalar or have shape "
                        f"({n_events},) or ({n_events}, 1), got {weight.shape}"
                    )

            yield_payload = eft_coeff * weight[:, None]

            if self._store_sumw2: 
                if fill_sumw2:
                    nominal_event_weight = weight * eft_coeff[:, 0] # (weight *a_0)^2
                    nominal_sumw2_payload = np.square(nominal_event_weight)
                else:
                    nominal_sumw2_payload = np.zeros(
                        n_events,
                        dtype=yield_payload.dtype,
                    )

                payload = np.concatenate(
                    (yield_payload, nominal_sumw2_payload[:, None]),
                    axis=1,
                )
            else:
                payload = yield_payload

            super().fill(**values, weight=payload)
            return self
        # Legacy storage expands each event over the quadratic_term axis.

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
        super().fill(quadratic_term=indices, **values, weight=eft_coeff)
        return self

    def _wc_for_eval(self, values):
        """Set the WC values used to evaluate the bin contents of this histogram
        where the WCs are specified as keyword arguments. Any WCs not listed are set to zero.
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
                    msg = (
                        f'This HistEFT does not know about the "{wc}" Wilson coefficient. '
                        f"Known coefficients: {list(self._wc_names.keys())}"
                    )
                    raise LookupError(msg)

        return np.asarray(result)

    # Normalize MultiCell and legacy coefficient views to a common layout.
    def _coefficient_view(self, hvs, *, flow: bool):
        """
        Return yield EFT coefficients in a common layout:

            (... dense bins ..., quadratic terms)

        Legacy HistEFT:
            - flow=True includes flow bins on the quadratic_term axis,
            so they must be removed with [..., 1:-1].
            - flow=False already excludes those flow bins.

        MultiCell:
            coefficients are stored in the cell dimension, not in a
            quadratic_term axis.
        """

        raw = np.asarray(hvs)

        if self._use_multicell:
            selected = raw[self._yield_slice, ...]
            return np.moveaxis(selected, 0, -1)

        if flow:
            return raw[..., 1:-1]

        return raw


    def yield_coefficients(self, *, flow: bool = False):
        """Return yield coefficients with the quadratic-term dimension last."""

        return {
            sparse_key: self._coefficient_view(hvs, flow=flow)
            for sparse_key, hvs in self.view(
                flow=flow,
                as_dict=True,
            ).items()
        }

    def nominal_sumw2(self, *, flow: bool = False):
        """Return the exact statistical sumw2 at the nominal EFT point."""

        if not self._store_sumw2:
            raise RuntimeError(
                "This HistEFT does not contain embedded nominal sumw2 data."
            )

        return {
            sparse_key: np.asarray(hvs)[self._sumw2_index, ...]
            for sparse_key, hvs in self.view(flow=flow, as_dict=True).items()
        }

    _EMBEDDED_SUBTRACTION_ERROR = (
        "Subtraction is not defined for HistEFT objects with embedded nominal "
        "sumw2. While yields subtract, variances of statistically independent "
        "quantities add: Var(A - B) = Var(A) + Var(B). For correlated inputs, "
        "a covariance term is also required, but HistEFT does not store "
        "covariances. Use an explicit analysis-specific subtraction procedure "
        "with a documented statistical policy."
    )

    @staticmethod
    def _is_real_numeric_scalar(value):
        """Return whether value is a scalar supported by variance scaling."""

        if not np.isscalar(value):
            return False
        value_array = np.asarray(value)
        return np.issubdtype(value_array.dtype, np.number) and np.isrealobj(
            value_array
        )

    def _validate_embedded_scalar(self, other, operation):
        if not self._is_real_numeric_scalar(other):
            raise TypeError(
                f"Embedded nominal sumw2 requires a real numeric scalar for "
                f"{operation}; got {type(other).__name__}."
            )

    def _validate_histogram_addition(self, other):
        """Validate EFT and histogram layout compatibility before mutation."""

        if not isinstance(other, HistEFT):
            raise TypeError(
                "HistEFT addition requires another HistEFT object; "
                f"got {type(other).__name__}."
            )

        if tuple(self.categorical_axes.name) != tuple(other.categorical_axes.name):
            raise ValueError(
                "HistEFT addition requires identical categorical-axis names "
                "in the same order."
            )

        if self._use_multicell != other._use_multicell:
            raise ValueError(
                "HistEFT addition requires matching storage backends; cannot "
                "combine MultiCell and legacy histograms."
            )

        if len(self.dense_axes) != len(other.dense_axes) or any(
            type(left) is not type(right) or left != right
            for left, right in zip(self.dense_axes, other.dense_axes)
        ):
            raise ValueError(
                "HistEFT addition requires compatible dense axes with "
                "identical names, ordering, and binning."
            )

        if self.wc_names != other.wc_names:
            raise ValueError(
                "HistEFT addition requires identical WC names in the same "
                f"order; got {self.wc_names} and {other.wc_names}."
            )

        if self._wc_count != other._wc_count:
            raise ValueError("HistEFT addition requires matching WC counts.")
        if self._quad_count != other._quad_count:
            raise ValueError(
                "HistEFT addition requires matching quadratic coefficient counts."
            )
        if self._store_sumw2 != other._store_sumw2:
            raise ValueError(
                "HistEFT addition requires both histograms to have the same "
                "embedded nominal-sumw2 layout."
            )
        if self._cell_count != other._cell_count:
            raise ValueError("HistEFT addition requires matching cell layouts.")

        self_storage = self._init_args.get("storage")
        other_storage = other._init_args.get("storage")
        if type(self_storage) is not type(other_storage):
            raise ValueError(
                "HistEFT addition requires compatible storage types; got "
                f"{type(self_storage).__name__} and "
                f"{type(other_storage).__name__}."
            )

    @staticmethod
    def _has_embedded_sumw2(value):
        return isinstance(value, HistEFT) and value.store_sumw2

    def _ibinary_op(self, other, op: str):
        """Apply arithmetic while preserving embedded variance semantics."""

        if op == "__iadd__":
            self._validate_histogram_addition(other)
            return super()._ibinary_op(other, op)

        if op == "__isub__" and (
            self._store_sumw2 or self._has_embedded_sumw2(other)
        ):
            raise NotImplementedError(self._EMBEDDED_SUBTRACTION_ERROR)

        if self._store_sumw2 and op in ("__imul__", "__itruediv__"):
            operation = "multiplication" if op == "__imul__" else "division"
            self._validate_embedded_scalar(other, operation)
            if op == "__itruediv__" and other == 0:
                raise ZeroDivisionError(
                    "Cannot divide a HistEFT with embedded nominal sumw2 by zero."
                )

            super()._ibinary_op(other, op)

            # SparseHist has already applied one power of the factor to every
            # cell. Apply the second power only to the nominal-sumw2 cell.
            for dense_hist in self._dense_hists.values():
                raw = np.asarray(dense_hist.view(flow=True))
                if op == "__imul__":
                    raw[self._sumw2_index, ...] *= other
                else:
                    raw[self._sumw2_index, ...] /= other
            return self

        return super()._ibinary_op(other, op)

    def __isub__(self, other):
        if self._store_sumw2 or self._has_embedded_sumw2(other):
            raise NotImplementedError(self._EMBEDDED_SUBTRACTION_ERROR)
        return self._ibinary_op(other, "__isub__")

    def __sub__(self, other):
        if self._store_sumw2 or self._has_embedded_sumw2(other):
            raise NotImplementedError(self._EMBEDDED_SUBTRACTION_ERROR)
        return self._binary_op(other, "__sub__")

    def __rsub__(self, other):
        if self._store_sumw2 or self._has_embedded_sumw2(other):
            raise NotImplementedError(self._EMBEDDED_SUBTRACTION_ERROR)
        return super().__rsub__(other)

    def eval(self, values):
        """Extract the sum of weights arrays from this histogram.

        Parameters
        ----------
        values: ArrayLike or Mapping or None
            The WC values used to evaluate the bin contents of this histogram.
            Either an array with the values, or a dictionary. If None, use an array of zeros.
        """

        values = self._wc_for_eval(values)

        out = {}
        for sparse_key, coeffs in self.yield_coefficients(flow=True).items():
            out[sparse_key] = efth.calc_eft_weights(coeffs, values)

        return out

    def as_hist(self, values):
        """Construct a regular histogram evaluated at values.
        (Like self.eval(...) but result is a histogram.)

        Parameters
        ----------
        values: ArrayLike or Mapping or None
            The WC values used to evaluate the bin contents of this histogram.
            Either an array with the values, or a dictionary. If None, use an array of zeros.
        """
        evals = self.eval(values=values)

        if self._use_multicell:
            axes = list(self.axes)
        else:
            axes = [axis for axis in self.axes if axis != self._coeff_axis]

        # EFT evaluation reduces each coefficient vector to one scalar yield
        # per bin, regardless of the HistEFT storage backend.
        hist_args = dict(self._init_args)
        hist_args["storage"] = hist.storage.Double()
        nhist = hist.Hist(*axes, **hist_args)

        sparse_names = self.categorical_axes.name
        for sp_val, arrs in evals.items():
            sp_key = dict(zip(sparse_names, sp_val))
            nhist[sp_key] = arrs

        return nhist

    def __reduce__(self):
        args = dict(self._init_args)
        args.update(self._init_args_eft)

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
        coefficients = self.yield_coefficients(flow=True)
        if len(coefficients) != 1:
            raise ValueError(
                "make_scaling requires exactly one populated sparse category; "
                f"found {len(coefficients)}."
            )

        scaling = np.array(next(iter(coefficients.values())), copy=True)
        if wc_list is not None:
            scaling = efth.remap_coeffs(self.wc_names, wc_list, scaling)
        else:
            wc_list = self.wc_names
        #check if any non-flow bins have zero sm contribution
        if ((scaling[:,0] == 0) & (scaling != 0).any(axis=1)).any():
            raise Exception('At least one bin found with no SM contribution and a BSM contribution!')
        skip = 0
        step = 2
        for i in np.ogrid[0: efth.n_quad_terms(len(wc_list))]:
            if i == skip:
                skip += step
                step += 1
            else:
                scaling[:,i] /= 2
        if flow=='sum':
            scaling[-2] += scaling[-1]
            scaling[1] += scaling[0]
            scaling = scaling[1:-1]
        elif flow !='show':
            raise Exception(f'Invalid flow options {flow} selected! Please select from "show" or "sum".')
        mask = scaling[:,0] != 0
        scaling[mask,:] = scaling[mask,:]/np.expand_dims(scaling[mask,0], 1) #divide by sm
        return scaling
    @classmethod
    def _read_from_reduce(cls, cat_axes, dense_axes, init_args, dense_hists):
        return super()._read_from_reduce(cat_axes, dense_axes, init_args, dense_hists)

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
