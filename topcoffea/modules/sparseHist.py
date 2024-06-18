#! /usr/bin/env python

import hist
import dask
import hist.dask as dah
import copy

import awkward as ak
import numpy as np

from itertools import chain, product
from collections import defaultdict

from typing import Mapping, Union, Sequence


class SparseState():
    def __init__(self, category_names, dense_axes, hist_cls, category_labels=None):
        self.category_names = list(category_names)
        self.dense_axes = list(dense_axes)
        self.n_categories = len(self.category_names)
        self.dense_hists = defaultdict(lambda: self.make_dense())
        self.hist_cls = hist_cls

        if category_labels:
            self.category_labels = dict(category_labels)
        else:
            self.category_labels = {}

    def __str__(self):
        return repr(self)

    def make_dense(self):
        return self.hist_cls(*self.dense_axes)

    def label(self, category_name):
        return self.category_labels.get(category_name, category_name)

    @property
    def categorical_keys(self):
        return sorted(self.dense_hists.keys())

    def fill(self, weight=None, sample=None, threads=None, **kwargs):
        cats = tuple(kwargs.pop(cat) for cat in self.category_names)
        h = self.dense_hists[cats]
        return h.fill(**kwargs, weight=weight, sample=sample, threads=threads)


class SparseHist():
    """Histogram specialized for sparse categorical data. This dask version only supports fills.
    Any other computation should be done SparseHist after calling dask.compute. """

    __dask_scheduler__ = staticmethod(dask.threaded.get)

    def __init__(self, category_names, dense_axes, category_labels=None, state_cls=SparseState):
        """SparseHist initialization is similar to hist.Hist, with the following restrictions:
        - Categorical axes are just given by their name.
        """
        self.state = state_cls(category_names, dense_axes, dah.Hist, category_labels)

    def __dask_graph__(self):
        dsk = {}
        inter = []
        for k, v in self.state.dense_hists.items():
            dsk.update(v.__dask_graph__())
            tk = (f"spareHist-{id(self)}", *k)
            dsk[tk] = (lambda kr, vr: (kr, vr), k, v.__dask_keys__()[0])
            inter.append(tk)
        dsk[self.__dask_keys__()[0]] = inter
        return dsk

    def __dask_keys__(self):
        return [(f"spareHist-{id(self)}", 0)]

    def __dask_optimize__(self, dsk, keys):
        return dsk

    def __dask_postcompute__(self):
        def post(vs):
            pairs = sorted((k, h) for (k, h) in vs[0])
            return SparseHistResult(self.state.category_names, histograms={k: h for (k, h) in pairs})
        return post, ()

    def fill(self, weight=None, sample=None, threads=None, **kwargs):
        return self.state.fill(weight=weight, sample=sample, threads=threads, **kwargs)


class SparseHistResult(SparseState):
    def __init__(self, category_names, histograms=None, dense_axes=None, category_labels=None, state_cls=SparseState):
        """Result from compute of SparseHist.
        - histograms is a dictionary
        """
        if (not histograms and not dense_axes):
            raise ValueError("At least one one of histograms or dense_axes should be specified.")

        if not dense_axes:
            first = next(iter(histograms.values()))
            dense_axes = list(first.axes)

        self.state = state_cls(category_names, dense_axes, hist.Hist, category_labels)

        if histograms:
            for k, h in histograms.items():
                self.state.dense_hists[k] += h

    @property
    def categorical_keys(self):
        return sorted(self.state.dense_hists.keys())

    def op_as_dict(self, op):
        return {key: op(self.state.dense_hists[key]) for key in self.state.categorical_keys}

    def apply_to_dense(self, method_name, *args, **kwargs):
        return self._ak_rec_op(lambda h: h.__getattribute__(method_name)(*args, **kwargs))

    def values(self, flow=False):
        return self.apply_to_dense("values", flow=flow)

    def counts(self, flow=False):
        return self.apply_to_dense("counts", flow=flow)

    def view(self, flow=False, as_dict=True):
        if not as_dict:
            key = ", ".join([f"'{name}': ..." for name in self.state.categorical_axes.name])
            raise ValueError(
                f"If not as_dict, only view of single dense histograms is supported. Use h[{{{key}}}].view(flow=...)."
            )
        return self.op_as_dict(lambda h: h.view(flow=flow))

    def _ak_rec_op(self, op_on_dense):
        all_keys = self.state.categorical_keys
        builder = ak.ArrayBuilder()

        def transverse_cut(index, keys=all_keys):
            last = object()
            for key in keys:
                if key[index] != last:
                    last = key[index]
                    yield last

        def rec(key, depth):
            for k in transverse_cut(depth):
                next_key = (*key, k) if key else (k,)
                if depth < len(self.state.category_names) - 1:
                    with builder.list():
                        rec(next_key, depth + 1)
                else:
                    if next_key in self.state.dense_hists:
                        builder.append(op_on_dense(self.state.dense_hists[next_key]))
                    else:
                        builder.append(None)
        rec(None, 0)
        return builder.snapshot()

    def _do_op(self, op_on_dense):
        for h in self.state.dense_hists.values():
            op_on_dense(h)

    def __copy__(self):
        """Empty histograms with the same bins."""
        other = type(self)(
            category_names=self.state.category_names,
            dense_axes=self.state.dense_axes,
            category_labels=self.state.category_labels,
            state_cls=type(self.state),
        )

        for k, h in self.state.dense_hists.items():
            other[k] = self.state.make_dense()
        return other

    def __deepcopy__(self, memo):
        other = self.__copy__()
        for k, h in self.state.dense_hists.items():
            other[k] += h
        return other

    def __setitem__(self, key, value):
        if not isinstance(key, tuple) or len(key) != self.state.n_categories:
            raise ValueError(f"{key} does not refer to a key in the histogram.")

        new_hist = key not in self.state.dense_hists
        try:
            self.state.dense_hists[key] = self.state.make_dense() + value
        except Exception as e:
            if new_hist and key in self.state.dense_hists:
                del self.state.dense_hists[key]
            raise e

    def __getitem__(self, key):
        if isinstance(key, dict):
            if len(key) == self.state.n_categories:
                axes = self.state.category_names
            elif len(key) == self.state.n_categories + 1:
                axes = chain(self.state.category_names, [a.name for a in self.state.dense_axes])
            else:
                raise KeyError(key)
            key = tuple(key[c] for c in axes)

        if not isinstance(key, tuple):
            raise ValueError(f"{key} is not a tuple")

        if len(key) == self.state.n_categories:
            if key not in self.state.dense_hists:
                raise KeyError(key)
            return self.state.dense_hists[key]
        elif len(key) == self.state.n_categories + 1:
            cat_key = tuple(key[:self.state.n_categories])
            dense_key = key[-1]
            if cat_key not in self.state.dense_hists:
                raise KeyError(cat_key)
            return self.state.dense_hists[cat_key][dense_key]

        raise KeyError(key)

    def reset(self):
        self._do_op(lambda h: h.reset())

    def integrate(self, axis_name: str, value=None):
        # name is category name
        if value is None:
            value = sum

        index = self.state.category_names.index(axis_name)
        new_categories = [n for n in self.state.category_names if n != axis_name]
        new_hists = defaultdict(lambda: self.state.make_dense())
        for k, h, in self.state.dense_hists.items():
            if value == sum or k[index] == value:
                new_key = (*k[:index], *k[index+1:])
                new_hists[new_key] += h
        return SparseHistResult(new_categories, histograms=new_hists)

    def group(self, axis_name: str, groups: dict[str, list[str]]):
        """Generate a new SparseHistResult where bins of axis are merged
        according to the groups mapping.
        """
        rev_map = {}
        for g, ms in groups.items():
            for m in ms:
                rev_map[m] = g
        index = self.state.category_names.index(axis_name)
        new_hists = defaultdict(lambda: self.state.make_dense())
        for k, h, in self.state.dense_hists.items():
            new_name = rev_map.get(k[index], k[index])
            new_key = (*k[:index], new_name, *k[index+1:])
            new_hists[new_key] += h
        return SparseHistResult(self.state.category_names, histograms=new_hists)

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
        bins = set(bins)
        index = self.state.category_names.index(axis_name)
        new_hists = {}
        for k, h, in self.state.dense_hists.items():
            if k[index] in bins:
                continue
            new_hists[k] = h
        return SparseHistResult(self.state.category_names, histograms=new_hists)

    def prune(self, axis_name, to_keep):
        """Remove bins from a categorical axis that are not in to_keep

        Parameters
        ----------
            bins : iterable
                A list of bin identifiers to remove
            axis : str
                Sparse axis name

        Returns a copy of the histogram with specified bins removed.
        """
        to_keep = set(to_keep)
        index = self.state.category_names.index(axis_name)
        new_hists = {}
        for k, h, in self.state.dense_hists.items():
            if k[index] in to_keep:
                new_hists[k] = h
        return SparseHistResult(self.state.category_names, histograms=new_hists)

    def scale(self, factor: float):
        for h in self.state.dense_hists.values():
            h *= factor
        return self

    def empty(self):
        for h in self.state.dense_hists.values():
            if np.any(h.view(flow=True) != 0):
                return False
        return True

    def _ibinary_op(self, other, op: str):
        if not isinstance(other, SparseHistResult):
            for h in self.state.dense_hists.values():
                getattr(h, op)(other)
        else:
            if self.state.category_names != other.state.category_names:
                raise ValueError(
                    "Category names are different, or in different order, and therefore cannot be merged."
                )
            for key_oh, oh in other.state.dense_hists.items():
                getattr(self.state.dense_hists[key_oh], op)(oh)
        return self

    def _binary_op(self, other, op: str):
        h = copy.deepcopy(self)
        op = op.replace("__", "__i", 1)
        return h._ibinary_op(other, op)

    def __reduce__(self):
        return (
            type(self)._read_from_reduce,
            (
                list(self.state.category_names),
                self.state.dense_axes,
                self.state.category_labels,
                list(self.state.dense_hists.keys()),
                list(self.state.dense_hists.values()),
                type(self.state),
            ),
        )

    @classmethod
    def _read_from_reduce(cls, cat_axes, dense_axes, labels, cat_keys, dense_values, state_cls):
        return cls(
            cat_axes,
            dense_axes=dense_axes,
            category_labels=labels,
            histograms={k: h for k, h in zip(cat_keys, dense_values)},
            state_cls=state_cls,
        )

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
