#! /usr/bin/env python

import hist
import boost_histogram as bh

import awkward as ak
import numpy as np

from itertools import chain, product
from collections import namedtuple

from typing import Mapping, Union, Sequence


def _restore_sparsehist_from_reduce(
    cls,
    cat_axes,
    dense_axes,
    init_args,
    dense_hists,
    *,
    track_raw_counts,
    raw_counts=None,
):
    """Canonical reconstruction owner for SparseHist and its subclasses."""

    hnew = cls(
        *cat_axes,
        *dense_axes,
        track_raw_counts=track_raw_counts,
        **init_args,
    )
    for key, dense_histogram in dense_hists.items():
        new_key = hnew._fill_bookkeep(*hnew.index_to_categories(key))
        hnew._dense_hists[new_key] = dense_histogram
        if track_raw_counts:
            if raw_counts is None or key not in raw_counts:
                raise RuntimeError("Serialized raw-count state is incomplete.")
            state = raw_counts[key]
            hnew._raw_counts[new_key] = (
                None
                if state is None
                else np.array(state, dtype=np.uint64, copy=True)
            )
    if track_raw_counts:
        hnew._validated_raw_count_states()
    return hnew


def _read_tracked_sparsehist_from_reduce(
    cls,
    cat_axes,
    dense_axes,
    init_args,
    dense_hists,
    raw_counts,
):
    """Reconstruct extended raw-count state outside the patchable legacy hook."""

    return _restore_sparsehist_from_reduce(
        cls,
        cat_axes,
        dense_axes,
        init_args,
        dense_hists,
        track_raw_counts=True,
        raw_counts=raw_counts,
    )


class SparseHist(hist.Hist, family=hist):
    """Histogram specialized for sparse categorical data."""

    def __init__(self, *axes, **kwargs):
        """Arguments:
        axes: List of categorical and regular/variable axes. Categorical access should come first. At least one regular or variable axis should be specified.
        kwargs: Same as for hist.Hist
        """

        track_raw_counts = kwargs.pop("track_raw_counts", False)
        if not isinstance(track_raw_counts, (bool, np.bool_)):
            raise TypeError("track_raw_counts must be a boolean.")

        self._init_args = dict(kwargs)

        categorical_axes, dense_axes = self._check_args(axes)

        self._tuple_t = namedtuple(
            f"SparseHistTuple{id(self)}", [a.name for a in categorical_axes]
        )
        self._dense_hists: dict[self._tuple_t, hist.Hist] = {}

        # we use self to keep track of the bins in the categorical axes.
        super().__init__(*categorical_axes, storage="Double")

        self._categorical_axes = super().axes
        self._dense_axes = hist.axis.NamedAxesTuple(dense_axes)

        self.axes = hist.axis.NamedAxesTuple(chain(super().axes, dense_axes))

        self._track_raw_counts = bool(track_raw_counts)
        if self._track_raw_counts:
            raw_axes = self._raw_count_dense_axes()
            if len(raw_axes) != 1:
                raise ValueError(
                    "Raw-count tracking requires exactly one physical dense axis."
                )
            self._raw_counts = {}

    def _check_args(self, axes):
        on_cats = True
        categorical_axes = []
        dense_axes = []

        for axis in axes:
            if isinstance(axis, (hist.axis.StrCategory, hist.axis.IntCategory)):
                if not on_cats:
                    ValueError("All categorical axes should be specified first.")
                categorical_axes.append(axis)
            else:
                on_cats = False
                dense_axes.append(axis)

        if len(dense_axes) < 1:
            raise ValueError("At least one dense axis should be specified.")

        return categorical_axes, dense_axes

    def empty_from_axes(self, categorical_axes=None, dense_axes=None, **kwargs):
        """Create an empty histogram like the current one, but with the axes provided.
        If axes are None, use those of current histogram.
        """
        if categorical_axes is None:
            categorical_axes = self.categorical_axes

        if dense_axes is None:
            dense_axes = self.dense_axes

        kwargs.setdefault("track_raw_counts", self.track_raw_counts)
        return type(self)(*categorical_axes, *dense_axes, **kwargs, **self._init_args)

    def make_dense(self, *axes, **kwargs):
        return hist.Hist(*axes, **self._init_args, **kwargs)

    def __copy__(self):
        """Empty histograms with the same bins."""
        return self.empty_from_axes(categorical_axes=self.categorical_axes)

    def __deepcopy__(self, memo):
        if len(self._dense_hists) < 1:
            return self.empty_from_axes(categorical_axes=self.categorical_axes)
        else:
            return self[{}]

    def __str__(self):
        return repr(self)

    def _split_axes(self, axes: dict):
        """Split axes dictionaries in categorical or dense.
        Axes returned in the order they were created. All axes of the histogram should be specified.
        """
        cats = {axis.name: axes[axis.name] for axis in self.categorical_axes}
        nocats = {axis.name: axes[axis.name] for axis in self._dense_axes}
        return (cats, nocats)

    def _make_tuple(self, other, mask=None):
        if mask is None:
            args = list(other)
        else:
            args = [o for o, m in zip(other, mask) if m]
        return self._tuple_t(*args)

    def categories_to_index(self, bins: Union[Sequence, Mapping]):
        return tuple(axis.index(bin) for axis, bin in zip(self.categorical_axes, bins))

    def index_to_categories(self, indices: Sequence):
        return self._make_tuple(
            axis[index] for index, axis in zip(indices, self.categorical_axes)
        )

    @property
    def categorical_axes(self):
        return self._categorical_axes

    @property
    def dense_axes(self):
        return self._dense_axes

    @property
    def track_raw_counts(self):
        return getattr(self, "_track_raw_counts", False)

    def _raw_count_dense_axes(self):
        return self._dense_axes

    def _raw_count_shape(self):
        return tuple(axis.extent for axis in self._raw_count_dense_axes())

    @staticmethod
    def _checked_add_raw_count_arrays(destination, addend, *, context):
        if destination.dtype != np.dtype(np.uint64):
            raise TypeError(f"{context} destination must have dtype uint64.")
        increment = np.asarray(addend)
        if increment.shape != destination.shape:
            raise ValueError(
                f"{context} shape mismatch: {increment.shape} != {destination.shape}."
            )
        if np.issubdtype(increment.dtype, np.signedinteger) and np.any(increment < 0):
            raise ValueError(f"{context} cannot add negative raw counts.")
        if not np.issubdtype(increment.dtype, np.integer):
            raise TypeError(f"{context} addend must have an integer dtype.")
        increment = increment.astype(np.uint64, copy=False)
        maximum = np.iinfo(np.uint64).max
        if np.any(increment > maximum - destination):
            raise OverflowError(f"{context} would overflow uint64 raw-count storage.")
        destination += increment

    def _validated_raw_count_states(self):
        if not self.track_raw_counts:
            raise RuntimeError("Raw-count tracking is disabled for this histogram.")
        if not hasattr(self, "_raw_counts"):
            raise RuntimeError("Raw-count state is missing from a tracked histogram.")
        dense_keys = set(self._dense_hists)
        raw_keys = set(self._raw_counts)
        if raw_keys != dense_keys:
            missing = dense_keys - raw_keys
            extra = raw_keys - dense_keys
            raise RuntimeError(
                "Raw-count classification coverage is incomplete: "
                f"missing={len(missing)}, extra={len(extra)}."
            )
        expected_shape = self._raw_count_shape()
        for key, state in self._raw_counts.items():
            if state is None:
                continue
            if not isinstance(state, np.ndarray):
                raise TypeError(f"Raw-count state for {key} is not an ndarray.")
            if state.dtype != np.dtype(np.uint64):
                raise TypeError(
                    f"Raw-count state for {key} must have dtype uint64, got {state.dtype}."
                )
            if state.shape != expected_shape:
                raise ValueError(
                    f"Raw-count state for {key} has shape {state.shape}, "
                    f"expected {expected_shape}."
                )
        return self._raw_counts

    def _merge_raw_count_state(self, index_key, incoming, *, context):
        if index_key not in self._raw_counts:
            self._raw_counts[index_key] = (
                None if incoming is None else np.array(incoming, dtype=np.uint64, copy=True)
            )
            return
        current = self._raw_counts[index_key]
        if current is None and incoming is None:
            return
        if current is None or incoming is None:
            raise RuntimeError(
                f"{context} mixes recorded and explicitly unrecorded raw-count state."
            )
        self._checked_add_raw_count_arrays(current, incoming, context=context)

    def _record_raw_count_fill(self, index_key, raw_values, record_raw_count):
        if not self.track_raw_counts:
            if record_raw_count is True:
                raise RuntimeError(
                    "Cannot record raw counts when histogram tracking is disabled."
                )
            return
        if record_raw_count is None:
            raise RuntimeError(
                "record_raw_count must be explicitly true or false when tracking is enabled."
            )
        if not isinstance(record_raw_count, (bool, np.bool_)):
            raise TypeError("record_raw_count must be true, false, or unspecified.")
        if not record_raw_count:
            self._merge_raw_count_state(
                index_key,
                None,
                context="Raw-count fill classification",
            )
            return

        axes = self._raw_count_dense_axes()
        counter = hist.Hist(*axes, storage="Int64")
        counter.fill(**{axis.name: raw_values[axis.name] for axis in axes})
        increment = np.asarray(counter.view(flow=True), dtype=np.uint64)
        if index_key not in self._raw_counts:
            self._raw_counts[index_key] = np.zeros(
                self._raw_count_shape(), dtype=np.uint64
            )
        elif self._raw_counts[index_key] is None:
            raise RuntimeError(
                "Raw-count fill classification changed from false to true for one sparse key."
            )
        self._checked_add_raw_count_arrays(
            self._raw_counts[index_key],
            increment,
            context="Raw-count fill",
        )

    def raw_counts(self, flow=False, as_dict=True):
        if not as_dict:
            raise ValueError("Raw counts are currently available only as a dictionary.")
        states = self._validated_raw_count_states()
        raw_axis = self._raw_count_dense_axes()[0]
        start = 1 if raw_axis.traits.underflow and not flow else 0
        stop = -1 if raw_axis.traits.overflow and not flow else None
        return {
            self.index_to_categories(key): np.array(state[start:stop], copy=True)
            for key, state in states.items()
            if state is not None
        }

    def copy_raw_counts_from(self, source):
        """Enable tracking by copying compatible raw-count state from ``source``.

        The destination numerical payload is unchanged. Categorical support and
        the physical raw-count axes must match exactly, and an existing
        destination raw-count state is never overwritten.
        """

        if not isinstance(source, SparseHist):
            raise TypeError("Raw-count state can be copied only from a SparseHist.")
        if self.track_raw_counts or hasattr(self, "_raw_counts"):
            raise RuntimeError("Destination already has raw-count state.")
        if not source.track_raw_counts:
            raise RuntimeError("Source raw-count tracking is disabled.")

        source_states = source._validated_raw_count_states()
        if (
            self.categorical_axes.name != source.categorical_axes.name
            or tuple(type(axis) for axis in self.categorical_axes)
            != tuple(type(axis) for axis in source.categorical_axes)
        ):
            raise ValueError("Categorical axes are incompatible for raw-count copying.")
        if tuple(self._raw_count_dense_axes()) != tuple(
            source._raw_count_dense_axes()
        ):
            raise ValueError("Physical dense axes are incompatible for raw-count copying.")

        source_indices = {
            tuple(source.index_to_categories(index)): index for index in source_states
        }
        destination_indices = {
            tuple(self.index_to_categories(index)): index for index in self._dense_hists
        }
        if set(source_indices) != set(destination_indices):
            missing = set(source_indices) - set(destination_indices)
            extra = set(destination_indices) - set(source_indices)
            raise ValueError(
                "Categorical support is incompatible for raw-count copying: "
                f"missing={len(missing)}, extra={len(extra)}."
            )

        self._track_raw_counts = True
        self._raw_counts = {}
        try:
            for categories, destination_index in destination_indices.items():
                self._merge_raw_count_state(
                    destination_index,
                    source_states[source_indices[categories]],
                    context="Raw-count state copy",
                )
            self._validated_raw_count_states()
        except Exception:
            del self._raw_counts
            self._track_raw_counts = False
            raise
        return self

    def with_raw_counts_unrecorded(self):
        """Return a numerical copy with every tracked raw-count cell unrecorded."""

        self._validated_raw_count_states()
        output = self.copy()
        output._raw_counts = {}
        for index in output._dense_hists:
            output._merge_raw_count_state(
                index,
                None,
                context="Explicit unrecorded raw-count classification",
            )
        output._validated_raw_count_states()
        return output

    @property
    def categorical_keys(self):
        for indices in self._dense_hists:
            yield self.index_to_categories(indices)

    def _fill_bookkeep(self, *args):
        super().fill(*args)
        index_key = self.categories_to_index(args)
        if index_key not in self._dense_hists:
            h = self.make_dense(*self._dense_axes)
            self._dense_hists[index_key] = h
        return index_key

    def fill(
        self,
        weight=None,
        sample=None,
        threads=None,
        record_raw_count=None,
        _raw_count_values=None,
        **kwargs,
    ):
        cats, nocats = self._split_axes(kwargs)

        if self.track_raw_counts and record_raw_count is None:
            raise RuntimeError(
                "record_raw_count must be explicitly true or false when tracking is enabled."
            )

        # fill the bookkeeping first, so that the index of the key exists.
        index_key = self._fill_bookkeep(*list(cats.values()))
        h = self._dense_hists[index_key]

        raw_values = nocats if _raw_count_values is None else _raw_count_values
        self._record_raw_count_fill(index_key, raw_values, record_raw_count)

        return h.fill(weight=weight, sample=sample, threads=threads, **nocats)

    def _to_bin(self, cat_name, value, offset=0):
        """Converts category value into its index slice in a StrCategory or IntCategory axis."""
        if isinstance(value, int):
            # already an index
            if value > -1:
                return value + offset
            else:
                return len(self._bookkeep_hist.axes[cat_name]) + value + offset
        elif isinstance(value, str):
            return self.categorical_axes[cat_name].index(value) + offset
        elif isinstance(value, complex):
            return self.categorical_axes[cat_name].index(int(value.imag)) + offset
        elif isinstance(value, bh.tag.loc):
            return self._to_bin(cat_name, value.value, value.offset)
        elif isinstance(value, slice):
            start = value.start if value.start else 0
            stop = value.stop if value.stop else len(self.axes[cat_name])
            step = value.step if value.step else 1
            return slice(
                self._to_bin(cat_name, start, offset),
                self._to_bin(
                    cat_name,
                    stop,
                    offset + (stop < 0),  # add 1 if stop negative, e.g. [-1] index
                ),
                step,
            )
        elif value == sum:
            return sum
        elif isinstance(value, Sequence):
            return tuple(self._to_bin(cat_name, v, offset) for v in value)
        raise ValueError(f"Invalid index specification: {cat_name}: {value}")

    def _make_index_key(self, key):
        if isinstance(key, Mapping):
            index_key = {axis.name: slice(None) for axis in self.axes}
            index_key.update(key)
            for k in key:
                if k not in self.axes.name:
                    raise ValueError(
                        f"Incorrect dimensions were specified. '{k}' is not a known axes."
                    )
        else:
            if not isinstance(key, tuple):
                key = (key,)
            if len(key) == len(self.categorical_axes):
                # assume just the name of the categories
                index_key = dict(zip(self.categorical_axes.name, key))
                index_key.update({axis.name: slice(None) for axis in self._dense_axes})
            elif len(key) == len(self.axes):
                # assume all axes specified, including dense axes
                index_key = dict(zip((a.name for a in self.axes), key))
            else:
                raise ValueError(
                    f"Incorrect dimensions were specified. Got {len(key)} values but expected {len(self.axes)}."
                )

        for a in self.categorical_axes:
            index_key[a.name] = self._to_bin(a.name, index_key[a.name])
        return index_key

    def _from_hists(
        self,
        hists: dict,
        categorical_axes: list,
        included_axes: Union[None, Sequence] = None,
        raw_states=None,
    ):
        """Construct a sparse hist from a dictionary of dense histograms.
        hists: a dictionary of dense histograms.
        categorical_axes: axes to use for the new histogram.
        included_axes: mask that indicates which category axes are present in the new histogram.
                  (I.e., the new categorical_axes correspond to True values in included_axes. Axes with False collapsed
                   because of integration, etc.)
        """
        dense_axes = list(hists.values())[0].axes

        new_hist = self.empty_from_axes(
            categorical_axes=categorical_axes, dense_axes=dense_axes
        )
        for index_key, dense_hist in hists.items():
            named_key = self.index_to_categories(index_key)
            new_named = new_hist._make_tuple(named_key, included_axes)
            new_index = new_hist._fill_bookkeep(*new_named)
            new_hist._dense_hists[new_index] += dense_hist
            if new_hist.track_raw_counts:
                new_hist._merge_raw_count_state(
                    new_index,
                    raw_states[index_key],
                    context="Sparse histogram selection",
                )
        return new_hist

    def _from_hists_no_dense(
        self,
        hists: dict,
        categorical_axes: list,
    ):
        """Construct a hist.Hist from a dictionary of histograms where all the dense axes have collapsed."""
        new_hist = hist.Hist(*categorical_axes, **self._init_args)
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
            return self.empty_from_axes(categorical_axes=cat_axes, dense_axes=sliced_zeros.axes)
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
        if self.track_raw_counts:
            raise RuntimeError(
                "Direct assignment cannot preserve tracked raw-count semantics."
            )
        index_key = self._make_index_key(key)
        cats, nocats = self._split_axes(index_key)
        filtered = self._filter_dense(index_key, filter_dense=False)

        if len(filtered) > 1:
            raise ValueError("Cannot assign to more than one set of categorical keys at a time.")

        new_hist = False
        if len(filtered) == 0:
            cat_index = self._fill_bookkeep(*self.index_to_categories(cats.values()))
            h = self._dense_hists[cat_index]
            new_hist = True
        else:
            h = list(filtered.values())[0]

        try:
            if isinstance(value, hist.Hist):
                h[nocats] = value.values(flow=True)
            else:
                h[nocats] = value
        except Exception as e:
            if new_hist:
                del self._dense_hists[cat_index]
            raise e

    def __getitem__(self, key):
        index_key = self._make_index_key(key)
        filtered = self._filter_dense(index_key)

        raw_states = None
        if self.track_raw_counts:
            _, dense_selection = self._split_axes(index_key)
            if any(
                not (
                    isinstance(selector, slice)
                    and selector.start is None
                    and selector.stop is None
                    and selector.step is None
                )
                for selector in dense_selection.values()
            ):
                raise RuntimeError(
                    "Dense-axis selection cannot currently preserve tracked raw-count semantics."
                )
            states = self._validated_raw_count_states()
            raw_states = {source_key: states[source_key] for source_key in filtered}

        preserve = [
            not (index_key[name] is sum or isinstance(index_key[name], int))
            for name in self.categorical_axes.name
        ]
        new_cats = [
            type(axis)([], growth=True, name=axis.name, label=axis.label)
            for axis, mask in zip(self.categorical_axes, preserve)
            if mask
        ]

        if len(filtered) == 0:
            return self._from_no_bins_found(index_key, new_cats)

        first = list(filtered.values())[0]
        if not isinstance(first, hist.Hist):
            if len(new_cats) == 0:
                # whole histogram collapsed to singe value
                return first
            else:
                # dense axes have collapsed to a single value
                return self._from_hists_no_dense(filtered, new_cats)
        else:
            return self._from_hists(filtered, new_cats, preserve, raw_states)

    def _ak_rec_op(self, op_on_dense):
        if len(self.categorical_axes) == 0:
            return op_on_dense(self._dense_hists[()])

        builder = ak.ArrayBuilder()

        def rec(key, depth):
            axis = list(self.categorical_axes)[-1 * depth]
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

        rec(None, len(self.categorical_axes.name))
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
        if self.track_raw_counts:
            for state in self._validated_raw_count_states().values():
                if state is not None:
                    state.fill(0)

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
        return self[{name: value}]

    def group(self, axis_name: str, groups: dict[str, list[str]]):
        """Generate a new SparseHist where bins of axis are merged
        according to the groups mapping.
        """
        old_axis = self.axes[axis_name]
        new_axis = hist.axis.StrCategory(
            groups.keys(), name=axis_name, label=old_axis.label, growth=True
        )

        cat_axes = []
        for axis in self.categorical_axes:
            if axis.name == axis_name:
                cat_axes.append(new_axis)
            else:
                cat_axes.append(axis)

        hnew = self.empty_from_axes(categorical_axes=cat_axes)
        raw_states = self._validated_raw_count_states() if self.track_raw_counts else None
        for target, sources in groups.items():
            old_key = self._make_index_key({axis_name: sources})
            filtered = self._filter_dense(old_key)

            for old_index, dense in filtered.items():
                new_key = self.index_to_categories(old_index)._asdict()
                new_key[axis_name] = target
                new_index = hnew.categories_to_index(new_key.values())

                hnew._fill_bookkeep(*new_key.values())
                hnew._dense_hists[new_index] += dense
                if hnew.track_raw_counts:
                    hnew._merge_raw_count_state(
                        new_index,
                        raw_states[old_index],
                        context="Sparse histogram group",
                    )
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
        if axis_name not in self.categorical_axes.name:
            raise ValueError(f"{axis_name} is not a categorical axis of the histogram.")

        axis = self.axes[axis_name]
        keep = [bin for bin in axis if bin not in bins]
        index = [axis.index(bin) for bin in keep]

        full_slice = tuple(slice(None) if ax != axis else index for ax in self.axes)
        return self[full_slice]

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
        if self.track_raw_counts:
            if isinstance(other, SparseHist):
                if not other.track_raw_counts:
                    raise RuntimeError(
                        "Cannot combine tracked and untracked sparse histograms."
                    )
                if op != "__iadd__":
                    raise RuntimeError(
                        "Only additive histogram merges preserve raw-count semantics."
                    )
                self._validated_raw_count_states()
                other_states = other._validated_raw_count_states()
            elif op in {"__imul__", "__idiv__", "__itruediv__"}:
                other_states = None
            elif op == "__iadd__" and np.isscalar(other) and other == 0:
                return self
            else:
                raise RuntimeError(
                    "This arithmetic operation cannot preserve tracked raw-count semantics."
                )
        elif isinstance(other, SparseHist) and other.track_raw_counts:
            raise RuntimeError("Cannot combine untracked and tracked sparse histograms.")

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
                if self.track_raw_counts:
                    self._merge_raw_count_state(
                        index,
                        other_states[index_oh],
                        context="Sparse histogram merge",
                    )
                getattr(self._dense_hists[index], op)(oh)
        return self

    def _binary_op(self, other, op: str):
        h = self.copy()
        op = op.replace("__", "__i", 1)
        return h._ibinary_op(other, op)

    def __reduce__(self):
        if self.track_raw_counts:
            return (
                _read_tracked_sparsehist_from_reduce,
                (
                    type(self),
                    list(self.categorical_axes),
                    list(self.dense_axes),
                    self._init_args,
                    self._dense_hists,
                    self._validated_raw_count_states(),
                ),
            )
        return (
            type(self)._read_from_reduce,
            (
                list(self.categorical_axes),
                list(self.dense_axes),
                self._init_args,
                self._dense_hists,
            ),
        )

    @classmethod
    def _read_from_reduce(
        cls,
        cat_axes,
        dense_axes,
        init_args,
        dense_hists,
    ):
        return _restore_sparsehist_from_reduce(
            cls,
            cat_axes,
            dense_axes,
            init_args,
            dense_hists,
            track_raw_counts=False,
        )

    def __iadd__(self, other):
        return self._ibinary_op(other, "__iadd__")

    def __add__(self, other):
        return self._binary_op(other, "__add__")

    def __radd__(self, other):
        return self._binary_op(other, "__add__")

    def __isub__(self, other):
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
