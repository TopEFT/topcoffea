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
        self._wc_names = {n: i for i, n in enumerate(wc_names)}
        self._wc_count = n
        self._quad_count = efth.n_quad_terms(n)

        self._init_args_eft = {"wc_names": wc_names}

        self._needs_rebinning = kwargs.pop("rebin", False)
        if self._needs_rebinning:
            raise ValueError("Do not know how to rebin yet...")

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

        super().__init__(*args, self._coeff_axis, **kwargs)

    def empty_from_axes(self, categorical_axes=None, dense_axes=None, **kwargs):
        return super().empty_from_axes(
            categorical_axes, dense_axes, **self._init_args_eft, **kwargs
        )

    @property
    def wc_names(self):
        return list(self._wc_names)

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
        """

        n_events = len(values[self.dense_axis.name])

        if eft_coeff is None:
            # if eft_coeff not given, assume values only for sm
            eft_coeff = np.broadcast_to(
                np.concatenate((np.ones((1,)), np.zeros((self._quad_count - 1,)))),
                (n_events, self._quad_count),
            )

        eft_coeff = np.asarray(eft_coeff)

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
            out[sparse_key] = self.calc_eft_weights(hvs, values)
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
        nhist = hist.Hist(
            *[axis for axis in self.axes if axis != self._coeff_axis], **self._init_args
        )

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

    def make_scalings(self,wc_scalings_lst=None):
        scalingsbins = np.array(self.values(flow=True)[1:])[:, 1:-1]
        scalingsbins_normalized = np.nan_to_num(scalingsbins / scalingsbins[:,0].reshape(-1,1), nan=0.0)
        scalings = []
        for numbers in scalingsbins_normalized:
            n = len(self._wc_names) + 1
            lower_matrix = np.zeros((n, n))
            idx = 0

            for i in range(n):
                lower_matrix[i, :i + 1] = numbers[idx:idx + i + 1]
                idx += i + 1

            lower_matrix[np.tril_indices(n, -1)] /= 2
            scalings.append(lower_matrix[np.tril_indices(n)].tolist())
        if wc_scalings_lst and wc_scalings_lst != self._wc_names:
            scalings = efth.remap_coeffs(self._wc_names,wc_scalings_lst,np.array(scalings)).tolist()

        return scalings

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
