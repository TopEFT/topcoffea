#! /usr/bin/env python

import hist
import numpy as np
import dask.array as da
import hist.dask as dah
import dask

from itertools import chain

from typing import Any, List, Mapping, Union

from topcoffea.modules.sparseHist import SparseState, SparseHist, SparseHistResult
import topcoffea.modules.eft_helper as efth

try:
    from numpy.typing import ArrayLike, Self
except ImportError:
    ArrayLike = Any
    Number = Any
    Self = Any


class HistEFTState(SparseState):
    def __init__(
        self,
        category_names,
        dense_axis,
        hist_cls,
        array_backend,
        category_labels=None,
        wc_names: Union[List[str], None] = None,
        **kwargs,
    ) -> None:
        """HistEFT initialization is similar to hist.Hist, with the following restrictions:
        - Exactly one axis can be dense (i.e. hist.axis.Regular, hist.axis.Variable, or his.axis.Integer)
        - The dense axis should be the last specified in the list of arguments.
        - Categorical axes should be specified with growth=True.
        """

        if not wc_names:
            wc_names = []

        n = len(wc_names)
        self.wc_names = {n: i for i, n in enumerate(wc_names)}
        self.wc_count = n
        self.quad_count = efth.n_quad_terms(n)
        self.array_backend = array_backend

        self._needs_rebinning = kwargs.pop("rebin", False)
        if self._needs_rebinning:
            raise ValueError("Do not know how to rebin yet...")

        if "quadratic_term" in kwargs:
            self.coeff_axis = kwargs.pop("quadratic_term")
        else:
            # no axis for quadratic_term found, creating our own.
            self.coeff_axis = hist.axis.Integer(
                start=0, stop=self.quad_count, name="quadratic_term"
            )

        self.dense_axis = dense_axis
        try:
            self.dense_axis_name = dense_axis.name
        except AttributeError:
            # weird construct because the axis may be a dask proxy
            self.dense_axis_name = dense_axis.axes[0].name

        reserved_names = ["quadratic_term", "sample", "weight", "thread"]
        if any(name in reserved_names for name in chain([self.dense_axis_name], category_names)):
            raise ValueError(
                f"No axis may have one of the following names: {','.join(reserved_names)}"
            )
        super().__init__(category_names, dense_axes=[self.dense_axis, self.coeff_axis], hist_cls=hist_cls, **kwargs)

    def index_of_wc(self, wc: str):
        return self.wc_names[wc]

    def should_rebin(self):
        return self._needs_rebinning

    def _fill_flatten(self, a, n_events):
        # manipulate input arrays into flat arrays. broadcast_to and ravel
        # used so that arrays are not duplicated in memory
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
        return self.array_backend.broadcast_to(a, (n_events, self.quad_count)).ravel()

    def _fill_indices(self, n_events):
        # turns [0, 1, 2, ..., num of quadratic coeffs - 1]
        # into:
        # [0, 1, 2, ..., 0, 1, 2 ...,]
        # repeated n_events times.
        return self.array_backend.broadcast_to(np.ogrid[0: self.quad_count], (n_events, self.quad_count)).ravel()

    def fill(
        self,
        weight=None,
        sample=None,
        threads=None,
        eft_coeff: ArrayLike = None,  # [num of events x (num of wc coeffs + 1)]
        **kwargs
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

        if eft_coeff is None:
            eft_coeff = 1    # if no eft_coeff, then it is simply sm, which does not weight the event
            indices = 0      # instead of an array, just fill the first coefficint (sm)

        # if weight is also given, comine it with eft_coeff. We use weight in the call to fill to pass the
        # coefficients
        weight = kwargs.pop("weight", None)
        if weight:
            eft_coeff = eft_coeff * weight

        n_events = kwargs[self.dense_axis_name].shape[0]

        # turn into [e0, e0, ..., e1, e1, ..., e2, e2, ...]
        kwargs[self.dense_axis_name] = self._fill_flatten(
            kwargs[self.dense_axis_name], n_events
        )

        # turn into: [c00, c01, c02, ..., c10, c11, c12, ...]
        eft_coeff = eft_coeff.ravel()

        # index for coefficient axes.
        # [ 0, 1, 2, ..., 0, 1, 2, ...]
        indices = self._fill_indices(n_events)

        # fills:
        # [e0,      e0,      e0    ..., e1,     e1,     e1,     ...]
        # [ 0,      1,       2,    ..., 0,      1,      2,      ...]
        # [c00*w0, c01*w0, c02*w0, ..., c10*w1, c11*w1, c12*w1, ...]
        super().fill(quadratic_term=indices, **kwargs, weight=eft_coeff)


class HistEFT(SparseHist):
    """Histogram specialized to hold Wilson Coefficients.
    Example:
    h = HistEFT(
            ["process"],
            category_labels={},
            dense_axis=hist.dask.Hist.new.Reg(
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

    (out,) = dask.compute(h)

    # eval at 0, returns a dictionary from categorical axes bins to array, same as just sm,
    # {('ttH',): array([-100. ,   3.6,    1.4,    1.5,  600. ])}
    out.eval({})
    out.eval({"ctG": 0})     # same thing
    out.eval(np.zeros((1,))  # same thing

    # eval at 1, same as adding all bins together per bins of dense axis.
    # {('ttH',): array([-600. ,   19.8,    7.2,    7.5,  600. ])}
    out.eval({"ctG": 1})     # same thing
    out.eval(np.ones((1,))  # same thing

    # instead of h.eval(...), h.as_hist(...) may be used to create a standard hist.Hist with the
    # result of the evaluation:
    hn = out.as_hist({"ctG": 0.02})
    hn.plot1d()
    ```
    """

    def __init__(
        self,
        category_names,
        dense_axis,
        category_labels=None,
        wc_names: Union[List[str], None] = None,
        state_cls=HistEFTState,
    ) -> None:
        """HistEFT initialization is similar to hist.Hist, with the following restrictions:
        - Exactly one axis can be dense (i.e. hist.axis.Regular, hist.axis.Variable, or his.axis.Integer)
        - The dense axis should be the last specified in the list of arguments.
        - Categorical axes should be specified with growth=True.
        """
        self.state = state_cls(
            category_names,
            dense_axis=dense_axis,
            array_backend=da,
            hist_cls=dah.Hist,
            category_labels=category_labels,
            wc_names=wc_names,
        )

    __dask_scheduler__ = staticmethod(dask.threaded.get)

    def __dask_postcompute__(self):
        def post(vs):
            pairs = sorted((k, h) for (k, h) in vs[0])
            return HistEFTResult(
                self.state.category_names,
                histograms={k: h for (k, h) in pairs},
                wc_names=self.state.wc_names,
            )

        return post, ()

    def fill(
        self,
        weight=None,
        sample=None,
        threads=None,
        eft_coeff: ArrayLike = None,  # [num of events x (num of wc coeffs + 1)]
        **kwargs
    ) -> Self:
        self.state.fill(weight=weight, sample=sample, threads=threads, eft_coeff=eft_coeff, **kwargs)


class HistEFTResult(SparseHistResult):
    def __init__(
            self,
            category_names,
            histograms=None,
            dense_axis=None,
            category_labels=None,
            wc_names: Union[List[str], None] = None,
            state_cls=HistEFTState,
            ) -> None:
        """Result from compute of SparseHist.
        - histograms is a dictionary
        """
        if (not histograms and not dense_axis):
            raise ValueError("At least one one of histograms or dense_axis should be specified.")

        if not dense_axis:
            first = next(iter(histograms.values()))
            dense_axis = first.axes[0]

        self.state = state_cls(
            category_names,
            dense_axis=dense_axis,
            array_backend=np,
            hist_cls=hist.Hist,
            category_labels=category_labels,
            wc_names=wc_names,
        )
        if histograms:
            for k, h in histograms.items():
                self.state.dense_hists[k] += h

    def _wc_for_eval(self, values):
        """Set the WC values used to evaluate the bin contents of this histogram
        where the WCs are specified as keyword arguments.  Any WCs not listed are set to zero.
        """
        if values is None:
            return np.zeros(self.state.wc_count)

        result = values
        if isinstance(values, Mapping):
            result = np.zeros(self.state.wc_count)
            for wc, val in values.items():
                try:
                    index = self.state.index_of_wc(wc)
                    result[index] = val
                except KeyError:
                    msg = f'Unknown Wilson coefficient "{wc}". Known are coefficients: {list(self.wc_names.keys())}'
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
        first = next(iter(self.state.dense_hists.values()))
        dense_axes = first.axes

        cat_axes = []
        for i, name in enumerate(self.state.category_names):
            vs = [key[i] for key in self.state.categorical_keys]
            cat_axes.append(hist.axis.StrCategory(vs, name=name, growth=True))

        evals = self.eval(values=values)
        nhist = hist.Hist(
            *cat_axes,
            *[axis for axis in dense_axes if axis.name != self.state.coeff_axis.name]
        )

        for sp_val, arrs in evals.items():
            sp_key = dict(zip(self.state.category_names, sp_val))
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

    def fill(
        self,
        weight=None,
        sample=None,
        threads=None,
        eft_coeff: ArrayLike = None,  # [num of events x (num of wc coeffs + 1)]
        **kwargs
    ) -> Self:
        self.state.fill(weight=weight, sample=sample, threads=threads, eft_coeff=eft_coeff, **kwargs)

    def __copy__(self):
        """Empty histograms with the same bins."""
        other = type(self)(
            category_names=self.state.category_names,
            dense_axis=self.state.dense_axis,
            category_labels=self.state.category_labels,
            state_cls=type(self.state),
            wc_names=self.state.wc_names,
        )

        for k, h in self.state.dense_hists.items():
            other[k] = self.state.make_dense()
        return other
