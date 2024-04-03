#! /usr/bin/env python

import hist
import boost_histogram as bh
import dask
import dask.array as da
import hist.dask as dah

import awkward as ak
import numpy as np

from itertools import chain, product
from collections import defaultdict

from typing import Mapping, Union, Sequence


class SparseHist(defaultdict):
    """Histogram specialized for sparse categorical data."""

    def __init__(self, *category_names, dense_axis, **kwargs):
        """SparseHist initialization is similar to hist.Hist, with the following restrictions:
        - Categorical axes are just given by their name.
        - Exactly one axis should be dense (i.e. hist.dask.Hist.new.Reg), and specified as a keyword argument.
        - kwargs: Same as for hist.dask.Hist
        """

        self._init_args = dict(kwargs)
        self._check_args(category_names, dense_axis)
        self._category_names = category_names
        self._dense_axis = dense_axis
        self._n_categories = len(self._category_names)
        # self.axes = hist.axis.NamedAxesTuple(axes)
        self._dense_hists = defaultdict(lambda: self.make_dense())

    def _check_args(self, category_names, dense_axis):
        if not all(isinstance(name, str) for name in category_names):
            raise ValueError("All category names should be strings")

    def __getattr__(self, *args):
        return self._dense_hists.__getattribute__(*args)

    def empty_from_axes(self, category_names=None, dense_axis=None, **kwargs):
        """Create an empty histogram like the current one, but with the axes provided.
        If axes are None, use those of current histogram.
        """
        if category_names is None:
            category_names = self.category_names

        if dense_axis is None:
            dense_axis = self.dense_axis

        return type(self)(*category_names, dense_axis=dense_axis, **kwargs, **self._init_args)

    def make_dense(self):
        return dah.Hist(self.dense_axis)

    def __copy__(self):
        """Empty histograms with the same bins."""
        return self.empty_from_axes(category_names=self.category_names)

    def __deepcopy__(self, memo):
        if len(self) < 1:
            return self.empty_from_axes(category_names=self.category_names)
        else:
            return self._dense_hists[{}]

    def __str__(self):
        return repr(self)

    @property
    def category_names(self):
        return self._category_names

    @property
    def dense_axis(self):
        return self._dense_axis

    @property
    def categorical_keys(self):
        return self._dense_hists.keys()

    def hist_values(self):
        ...

    def _check_axes_order(self, axes_names):
        if any(a != name_p for a, name_p in zip(self.category_names, axes_names)):
            raise ValueError(f"Axes should be specified in the order {list(self.axes)}, not {axes_names}")

    def fill(self, weight=None, sample=None, threads=None, **kwargs):
        axes_names = list(kwargs.keys())
        self._check_axes_order(axes_names)

        cats = tuple(kwargs.pop(cat) for cat in self.category_names)

        h = self._dense_hists[cats]

        print("|---->", kwargs, cats)

        return h.fill(**kwargs, weight=weight, sample=sample, threads=threads)

    def _from_hists(
        self,
        hists: dict,
        category_names: list,
        included_axes: Union[None, Sequence] = None,
    ):
        """Construct a sparse hist from a dictionary of dense histograms.
        hists: a dictionary of dense histograms.
        category_names: axes to use for the new histogram.
        included_axes: mask that indicates which category axes are present in the new histogram.
                  (I.e., the new category_names correspond to True values in included_axes. Axes with False collapsed
                   because of integration, etc.)
        """
        dense_axes = list(hists.values())[0].axes

        new_hist = self.empty_from_axes(
            category_names=category_names, dense_axes=dense_axes
        )
        for index_key, dense_hist in hists.items():
            named_key = self.index_to_categories(index_key)
            new_named = new_hist._make_tuple(named_key, included_axes)
            new_index = new_hist._fill_bookkeep(*new_named)
            new_hist._dense_hists[new_index] += dense_hist
        return new_hist

    def _from_hists_no_dense(
        self,
        hists: dict,
        category_names: list,
    ):
        """Construct a hist.Hist from a dictionary of histograms where all the dense axes have collapsed."""
        new_hist = hist.Hist(*category_names, **self._init_args)
        for index_key, weight in hists.items():
            named_key = ()
            new_hist.fill(*named_key, weight=weight)
        return new_hist

    def _from_no_bins_found(self, index_key, cat_axes):
        # If no bins are found, we need to check whether those bins would be present in a completely dense histogram.
        # If so, we return either the zero value for that histogram, or an empty histogram without the collapsed axes.
        dummy_zeros = hist.Hist(*self.dense_axes, storage=self._init_args.get('storage', None))
        try:
            sliced_zeros = dummy_zeros[{a.name: index_key[a.name] for a in self.dense_axes}]
        except KeyError:
            raise KeyError("No bins found")

        if isinstance(sliced_zeros, hist.Hist):
            return self.empty_from_axes(category_names=cat_axes, dense_axes=sliced_zeros.axes)
        else:
            return sliced_zeros

    def _filter_dense(self, index_key, filter_dense=True):
        def asseq(cat_name, x):
            if isinstance(x, int):
                return range(x, x + 1)
            elif isinstance(x, slice):
                step = x.step if isinstance(x.step, int) else 1
                return range(x.start, x.stop, step)
            elif x == sum:
                return range(len(self.axes[cat_name]))
            return x

        cats, nocats = self._split_axes(index_key)
        filtered = {}
        for sparse_key in product(*(asseq(name, v) for name, v in cats.items())):
            if sparse_key in self._dense_hists:
                filtered[sparse_key] = self._dense_hists[sparse_key]
                if filter_dense:
                    filtered[sparse_key] = filtered[sparse_key][tuple(nocats.values())]
        return filtered

    def __setitem__(self, key, value):
        if not isinstance(key, tuple) or len(key) != self._n_categories:
            raise ValueError(f"{key} does not refer to a key in the histogram.")

        new_hist = key not in self._dense_hists
        h = self._dense_hists[key]
        try:
            if isinstance(value, hist.Hist):
                h[self.dense_axis.name] = value.values(flow=True)
            else:
                h = value
        except Exception as e:
            if new_hist:
                del self._dense_hists[key]
            raise e

    def __getitem__(self, key):
        if not isinstance(key, tuple) or len(key) != self._n_categories:
            raise ValueError(f"{key} does not refer to a key in the histogram.")
        return self._dense_hists[key]

    def _ak_rec_op(self, op_on_dense):
        if len(self.categorical_axes) == 0:
            return op_on_dense(self._dense_hists[()])

        builder = ak.ArrayBuilder()

        def rec(key, depth):
            axis = list(self.category_names)[-1 * depth]
            for i in range(len(axis)):
                next_key = (*key, i) if key else (i,)
                if depth > 1:
                    with builder.list():
                        rec(next_key, depth - 1)
                else:
                    if next_key in self._dense_hists:
                        builder.append(op_on_dense(self._dense_hists[next_key]))
                    else:
                        builder.append(None)

        rec(None, len(self.category_names.name))
        return builder.snapshot()

    def values(self, flow=False):
        return self._ak_rec_op(lambda h: h.values(flow=flow))

    def counts(self, flow=False):
        return self._ak_rec_op(lambda h: h.counts(flow=flow))

    def _do_op(self, op_on_dense):
        for h in self._dense_hists.values():
            op_on_dense(h)

    def reset(self):
        self._do_op(lambda h: h.reset())

    def view(self, flow=False, as_dict=True):
        if not as_dict:
            key = ", ".join([f"'{name}': ..." for name in self.categorical_axes.name])
            raise ValueError(
                f"If not a dict, only view of particular dense histograms is currently supported. Use h[{{{key}}}].view(flow=...) instead."
            )
        return {
            self.index_to_categories(k): h.view(flow=flow)
            for k, h in self._dense_hists.items()
        }

    def integrate(self, name: str, value=None):
        if value is None:
            value = sum
        return self._dense_hists[{name: value}]

    def group(self, axis_name: str, groups: dict[str, list[str]]):
        """Generate a new SparseHist where bins of axis are merged
        according to the groups mapping.
        """
        old_axis = self.axes[axis_name]
        new_axis = hist.axis.StrCategory(
            groups.keys(), name=axis_name, label=old_axis.label, growth=True
        )

        cat_axes = []
        for axis in self.category_names:
            if axis.name == axis_name:
                cat_axes.append(new_axis)
            else:
                cat_axes.append(axis)

        hnew = self.empty_from_axes(category_names=cat_axes)
        for target, sources in groups.items():
            old_key = self._make_index_key({axis_name: sources})
            filtered = self._filter_dense(old_key)

            for old_index, dense in filtered.items():
                new_key = self.index_to_categories(old_index)._asdict()
                new_key[axis_name] = target
                new_index = hnew.categories_to_index(new_key.values())

                hnew._fill_bookkeep(*new_key.values())
                hnew._dense_hists[new_index] += dense
        return hnew

    def remove(self, axis_name, bins):
        """Remove bins from a categorical axis

        Parameters
        ----------
            bins : iterable
                A list of bin identifiers to remove
            axis : str
                Sparse axis name

        Returns a copy of the histogram with specified bins removed.
        """
        if axis_name not in self.category_names.name:
            raise ValueError(f"{axis_name} is not a categorical axis of the histogram.")

        axis = self.axes[axis_name]
        keep = [bin for bin in axis if bin not in bins]
        index = [axis.index(bin) for bin in keep]

        full_slice = tuple(slice(None) if ax != axis else index for ax in self.axes)
        return self._dense_hists[full_slice]

    def prune(self, axis, to_keep):
        """Convenience method to remove all categories except for a selected subset."""
        to_remove = [x for x in self.axes[axis] if x not in to_keep]
        return self.remove(axis, to_remove)

    def scale(self, factor: float):
        self *= factor
        return self

    def empty(self):
        for h in self._dense_hists.values():
            if np.any(h.view(flow=True) != 0):
                return False
        return True

    def _ibinary_op(self, other, op: str):
        if not isinstance(other, SparseHist):
            for h in self._dense_hists.values():
                getattr(h, op)(other)
        else:
            if self.categorical_axes.name != other.categorical_axes.name:
                raise ValueError(
                    "Category names are different, or in different order, and therefore cannot be merged."
                )
            for index_oh, oh in other._dense_hists.items():
                cats = other.index_to_categories(index_oh)
                self._fill_bookkeep(*cats)
                index = self.categories_to_index(cats)
                getattr(self._dense_hists[index], op)(oh)
        return self

    def _binary_op(self, other, op: str):
        h = self.copy()
        op = op.replace("__", "__i", 1)
        return h._ibinary_op(other, op)

    def __reduce__(self):
        return (
            type(self)._read_from_reduce,
            (
                list(self.category_names),
                list(self.dense_axes),
                self._init_args,
                self._dense_hists,
            ),
        )

    @classmethod
    def _read_from_reduce(cls, cat_axes, dense_axes, init_args, dense_hists):
        hnew = cls(*cat_axes, *dense_axes, **init_args)
        for k, h in dense_hists.items():
            hnew._fill_bookkeep(*hnew.index_to_categories(k))
            hnew._dense_hists[k] = h
        return hnew

    def __iadd__(self, other):
        return self._ibinary_op(other, "__iadd__")

    def __add__(self, other):
        return self._binary_op(other, "__add__")

    def __radd__(self, other):
        return self._binary_op(other, "__add__")

    def __isub(self, other):
        return self._ibinary_op(other, "__isub__")

    def __sub__(self, other):
        return self._binary_op(other, "__sub__")

    def __rsub__(self, other):
        return self._binary_op(other, "__sub__")

    def __imul__(self, other):
        return self._ibinary_op(other, "__imul__")

    def __mul__(self, other):
        return self._binary_op(other, "__mul__")

    def __rmul__(self, other):
        return self._binary_op(other, "__mul__")

    def __idiv__(self, other):
        return self._ibinary_op(other, "__idiv__")

    def __div__(self, other):
        return self._binary_op(other, "__div__")

    def __itruediv__(self, other):
        return self._ibinary_op(other, "__itruediv__")

    def __truediv__(self, other):
        return self._binary_op(other, "__truediv__")

    # compatibility methods for old coffea
    # all of these are deprecated
    def identity(self):
        h = self.copy(deep=False)
        h.reset()
        return h

    def compute(self, *args, **kwargs):
        return dask.compute(self, *args, **kwargs)
