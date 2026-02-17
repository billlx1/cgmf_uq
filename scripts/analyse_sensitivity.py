#!/usr/bin/env python3
"""
analyse_sensitivity.py
Phase-I sensitivity post-processor: CGMF prompt gammas/neutrons.

Changes vs original:
- NaN causes are tracked explicitly via a SensitivityMask enum
- Zero-signal observables are masked BEFORE regression (not via eps_y1 after)
- Heatmap uses distinct colours for each NaN cause:
    grey   = no data / fewer than 2 runs          (INSUFFICIENT_DATA)
    white  = observable is zero / near-zero signal (ZERO_SIGNAL)
    black  = scale variance is zero                (ZERO_SCALE_VAR)
    yellow = regression produced non-finite result (REGRESSION_FAILURE)
    colour = valid sensitivity coefficient         (OK)
- A legend patch is added to every heatmap
- clip_pct (default 99.5) clips extreme |S| values before colour scaling
  so a handful of large outliers don't wash out everything else
- --max_gamma_energy_MeV / --max_neutron_energy_MeV optionally truncate
  the spectra to only bins whose centres are at or below the given energy
- The combined full-observable heatmap has been removed; only per-group
  heatmaps are produced
- Per-group heatmaps now have meaningful x-axes:
    spectrum groups    → Energy (MeV),    ticked at bin centres
    multiplicity groups → Multiplicity,   ticked at integer values
- Multiplicity heatmaps are split by fragment (total / light_fragment /
  heavy_fragment), each rendered on its own axes / figure
- --drop_all_zero_params: if set, rows where every entry in the group
  submatrix is non-finite or exactly zero are omitted from that group's
  heatmap (reason for zero is irrelevant)
"""

from __future__ import annotations

import argparse
import csv
import glob
import json
import os
from dataclasses import dataclass
from enum import IntEnum
from typing import Dict, List, Optional, Tuple

import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.colors as mcolors

# ----------------------------- Utilities ----------------------------------- #

def _mkdir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def _isclose_all(a: np.ndarray, b: np.ndarray, rtol=1e-10, atol=1e-12) -> bool:
    if a.shape != b.shape:
        return False
    return np.allclose(a, b, rtol=rtol, atol=atol)

def _nanpercentile_abs(x: np.ndarray, pct: float) -> float:
    x = np.asarray(x, dtype=float)
    x = np.abs(x)
    x = x[np.isfinite(x)]
    if x.size == 0:
        return 1.0
    return float(np.percentile(x, pct))

def _debug_print(debug: bool, msg: str) -> None:
    if debug:
        print(msg, flush=True)

# ----------------------------- Data model ---------------------------------- #

@dataclass(frozen=True)
class ManifestEntry:
    task_id: int
    parameter: str
    scale: float
    config_file: str

@dataclass
class TaskData:
    task_id: int
    parameter: str
    scale: float
    json_path: str
    config_file: str
    gamma_edges: np.ndarray
    gamma_centers: np.ndarray
    gamma_spectrum: np.ndarray
    neutron_edges: np.ndarray
    neutron_centers: np.ndarray
    neutron_spectrum: np.ndarray
    gamma_m_range: np.ndarray
    gamma_mult: Dict[str, Dict[str, Dict[int, float]]]
    neutron_m_range: np.ndarray
    neutron_mult: Dict[str, Dict[str, Dict[int, float]]]

@dataclass
class ObservableSchema:
    groups: List[str]
    labels: List[str]
    group_boundaries: Dict[str, Tuple[int, int]]

# ----------------------------- Mask enum ----------------------------------- #

class SensMask(IntEnum):
    """
    Codes stored in the mask matrix (n_params × n_obs).
    Only OK cells carry a meaningful S value; all others are NaN in S.
    """
    OK               = 0  # valid coefficient — rendered in coolwarm
    INSUFFICIENT_DATA = 1  # < 2 runs loaded for this parameter → grey
    ZERO_SCALE_VAR   = 2  # all scale values identical → black
    ZERO_SIGNAL      = 3  # observable near-zero across all runs → white
    REGRESSION_FAILURE = 4  # non-finite a, b, or y(s=1) → yellow

# Colours used in the heatmap overlay (one per non-OK code)
_MASK_COLOURS = {
    SensMask.INSUFFICIENT_DATA: (0.65, 0.65, 0.65, 1.0),  # grey
    SensMask.ZERO_SCALE_VAR:   (0.05, 0.05, 0.05, 1.0),  # near-black
    SensMask.ZERO_SIGNAL:      (1.00, 1.00, 1.00, 1.0),  # white
    SensMask.REGRESSION_FAILURE:(1.00, 0.90, 0.00, 1.0),  # yellow
}

_MASK_LABELS = {
    SensMask.INSUFFICIENT_DATA: "Insufficient data (< 2 runs)",
    SensMask.ZERO_SCALE_VAR:   "Zero scale variance",
    SensMask.ZERO_SIGNAL:      "Zero / near-zero observable",
    SensMask.REGRESSION_FAILURE:"Regression failure (non-finite)",
}

# ----------------------------- JSON extraction ----------------------------- #

def _get(obj: dict, key: str) -> Optional[object]:
    return obj.get(key, None)

def _get_nested(obj: dict, path: List[str]) -> Optional[object]:
    cur = obj
    for p in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(p)
        if cur is None:
            return None
    return cur

def _extract_spectrum_block(
    d: dict,
    block_name: str
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    blk = _get(d, block_name)
    if blk is None:
        raise KeyError(f"Missing JSON block: '{block_name}'")
    edges   = _get(blk, "bin_edges_MeV")
    centers = _get(blk, "bin_centers_MeV")
    spec    = _get(blk, "spectrum")
    if edges is None or centers is None or spec is None:
        raise KeyError(
            f"Block '{block_name}' missing required keys "
            "(bin_edges_MeV, bin_centers_MeV, spectrum)"
        )
    return (
        np.asarray(edges,   dtype=float),
        np.asarray(centers, dtype=float),
        np.asarray(spec,    dtype=float),
    )

def _extract_multiplicity_block(
    d: dict,
    block_name: str
) -> Tuple[np.ndarray, Dict[str, Dict[str, Dict[int, float]]]]:
    blk = _get(d, block_name)
    if blk is None:
        raise KeyError(f"Missing JSON block: '{block_name}'")
    m_range = _get(blk, "multiplicity_range")
    if m_range is None:
        raise KeyError(f"Block '{block_name}' missing 'multiplicity_range'")
    m_range = np.asarray(m_range, dtype=int)

    fragments = ["total", "light_fragment", "heavy_fragment"]
    fields    = ["counts", "probabilities"]
    out: Dict[str, Dict[str, Dict[int, float]]] = {}
    for frag in fragments:
        out[frag] = {}
        for field in fields:
            nested = _get_nested(blk, [frag, field])
            if nested is None:
                nested = _get(blk, f"{frag}.{field}")
            if nested is None:
                raise KeyError(
                    f"Block '{block_name}' missing '{frag}/{field}'"
                )
            arr = np.asarray(nested, dtype=float)
            if arr.shape[0] != m_range.shape[0]:
                raise ValueError(
                    f"Multiplicity length mismatch in '{block_name}' "
                    f"{frag}/{field}: len={arr.shape[0]} vs m_range={m_range.shape[0]}"
                )
            out[frag][field] = {
                int(m): float(v)
                for m, v in zip(m_range.tolist(), arr.tolist())
            }
    return m_range, out

# ----------------------------- Manifest ------------------------------------ #

def read_manifest(manifest_path: str) -> List[ManifestEntry]:
    entries: List[ManifestEntry] = []
    with open(manifest_path, "r", newline="") as f:
        rdr = csv.DictReader(f)
        required = {"task_id", "parameter", "scale", "config_file"}
        for r in required:
            if r not in (rdr.fieldnames or []):
                raise ValueError(
                    f"Manifest missing required column '{r}'. "
                    f"Found: {rdr.fieldnames}"
                )
        for row in rdr:
            entries.append(ManifestEntry(
                task_id=int(row["task_id"]),
                parameter=str(row["parameter"]),
                scale=float(row["scale"]),
                config_file=str(row["config_file"]),
            ))
    return entries

def find_task_json(runs_dir: str, task_id: int) -> Optional[str]:
    task_dir = os.path.join(runs_dir, f"task_{task_id}")
    if not os.path.isdir(task_dir):
        return None
    pat = os.path.join(task_dir, f"analysis_{task_id}_*.json")
    matches = sorted(glob.glob(pat))
    return matches[-1] if matches else None

def load_task_data(
    runs_dir: str,
    entry: ManifestEntry,
    debug: bool = False
) -> Optional[TaskData]:
    json_path = find_task_json(runs_dir, entry.task_id)
    if json_path is None:
        _debug_print(
            debug,
            f"[WARN] Missing JSON for task_id={entry.task_id} "
            "(dir or file not found)"
        )
        return None
    try:
        with open(json_path, "r") as f:
            d = json.load(f)
        g_edges,  g_centers,  g_spec = _extract_spectrum_block(d, "gamma_spectrum")
        n_edges,  n_centers,  n_spec = _extract_spectrum_block(d, "neutron_spectrum")
        gm_range, gm = _extract_multiplicity_block(d, "gamma_multiplicity_distributions")
        nm_range, nm = _extract_multiplicity_block(d, "neutron_multiplicity_distributions")
        td = TaskData(
            task_id=entry.task_id,
            parameter=entry.parameter,
            scale=entry.scale,
            config_file=entry.config_file,
            json_path=json_path,
            gamma_edges=g_edges,
            gamma_centers=g_centers,
            gamma_spectrum=g_spec,
            neutron_edges=n_edges,
            neutron_centers=n_centers,
            neutron_spectrum=n_spec,
            gamma_m_range=gm_range,
            gamma_mult=gm,
            neutron_m_range=nm_range,
            neutron_mult=nm,
        )
        _debug_print(
            debug,
            f"[OK] Loaded task {entry.task_id} "
            f"({entry.parameter}, scale={entry.scale}) from {json_path}"
        )
        return td
    except Exception as e:
        _debug_print(
            debug,
            f"[ERROR] Failed to load/parse task_id={entry.task_id} "
            f"JSON: {json_path}\n  {repr(e)}"
        )
        return None

# ----------------------------- Schema building ----------------------------- #

def build_schema(
    all_tasks: List[TaskData],
    max_gamma_energy_MeV: Optional[float] = None,
    max_neutron_energy_MeV: Optional[float] = None,
    debug: bool = False,
) -> Tuple[ObservableSchema, dict]:
    if not all_tasks:
        raise RuntimeError("No task data loaded. Cannot build schema.")

    ref = all_tasks[0]
    g_edges_ref   = ref.gamma_edges
    g_centers_ref = ref.gamma_centers
    n_edges_ref   = ref.neutron_edges
    n_centers_ref = ref.neutron_centers

    for td in all_tasks[1:]:
        if (
            not _isclose_all(td.gamma_edges,   g_edges_ref)
            or not _isclose_all(td.gamma_centers, g_centers_ref)
        ):
            raise ValueError(
                f"Gamma spectrum binning mismatch between tasks.\n"
                f"  Ref task: {ref.task_id}\n  Bad task: {td.task_id}"
            )
        if (
            not _isclose_all(td.neutron_edges,   n_edges_ref)
            or not _isclose_all(td.neutron_centers, n_centers_ref)
        ):
            raise ValueError(
                f"Neutron spectrum binning mismatch between tasks.\n"
                f"  Ref task: {ref.task_id}\n  Bad task: {td.task_id}"
            )

    # Apply energy cutoffs: keep only bins whose centres are <= the threshold.
    # A cutoff of None means keep all bins (original behaviour).
    if max_gamma_energy_MeV is not None:
        g_mask = g_centers_ref <= max_gamma_energy_MeV
        if not np.any(g_mask):
            raise ValueError(
                f"--max_gamma_energy_MeV={max_gamma_energy_MeV} excludes ALL "
                f"gamma spectrum bins (min centre = {g_centers_ref.min():.6g} MeV)."
            )
        g_centers_ref = g_centers_ref[g_mask]
        # Edges: keep the left edge of the first kept bin through the right edge
        # of the last kept bin.  bin_edges has length n_bins+1, so the right
        # edge of bin i is g_edges_ref[i+1].
        kept_indices = np.where(g_mask)[0]
        g_edges_ref  = g_edges_ref[kept_indices[0] : kept_indices[-1] + 2]
        _debug_print(
            debug,
            f"[SCHEMA] Gamma energy cutoff : <= {max_gamma_energy_MeV} MeV "
            f"-> {g_centers_ref.size} bins retained "
            f"(max centre = {g_centers_ref.max():.6g} MeV)"
        )
    else:
        g_mask = np.ones(g_centers_ref.size, dtype=bool)

    if max_neutron_energy_MeV is not None:
        n_mask = n_centers_ref <= max_neutron_energy_MeV
        if not np.any(n_mask):
            raise ValueError(
                f"--max_neutron_energy_MeV={max_neutron_energy_MeV} excludes ALL "
                f"neutron spectrum bins (min centre = {n_centers_ref.min():.6g} MeV)."
            )
        n_centers_ref = n_centers_ref[n_mask]
        kept_indices  = np.where(n_mask)[0]
        n_edges_ref   = n_edges_ref[kept_indices[0] : kept_indices[-1] + 2]
        _debug_print(
            debug,
            f"[SCHEMA] Neutron energy cutoff: <= {max_neutron_energy_MeV} MeV "
            f"-> {n_centers_ref.size} bins retained "
            f"(max centre = {n_centers_ref.max():.6g} MeV)"
        )
    else:
        n_mask = np.ones(n_centers_ref.size, dtype=bool)

    gamma_m_set   = set()
    neutron_m_set = set()
    for td in all_tasks:
        gamma_m_set.update(int(x) for x in td.gamma_m_range.tolist())
        neutron_m_set.update(int(x) for x in td.neutron_m_range.tolist())
    gamma_m_vals  = np.array(sorted(gamma_m_set),  dtype=int)
    neutron_m_vals = np.array(sorted(neutron_m_set), dtype=int)

    _debug_print(debug, f"[SCHEMA] Gamma spectrum bins : {g_centers_ref.size}")
    _debug_print(debug, f"[SCHEMA] Neutron spectrum bins: {n_centers_ref.size}")
    _debug_print(
        debug,
        f"[SCHEMA] Gamma mult union : {gamma_m_vals.size} bins "
        f"({gamma_m_vals.min() if gamma_m_vals.size else 'NA'}"
        f"..{gamma_m_vals.max() if gamma_m_vals.size else 'NA'})"
    )
    _debug_print(
        debug,
        f"[SCHEMA] Neutron mult union : {neutron_m_vals.size} bins "
        f"({neutron_m_vals.min() if neutron_m_vals.size else 'NA'}"
        f"..{neutron_m_vals.max() if neutron_m_vals.size else 'NA'})"
    )

    groups: List[str] = []
    labels: List[str] = []
    boundaries: Dict[str, Tuple[int, int]] = {}

    def _start(name: str):
        boundaries[name] = (len(labels), -1)

    def _end(name: str):
        s, _ = boundaries[name]
        boundaries[name] = (s, len(labels))

    _start("Gamma Spectrum")
    for i, E in enumerate(g_centers_ref.tolist()):
        groups.append("Gamma Spectrum")
        labels.append(f"Gspec_bin{i:03d}_E{E:.6g}MeV")
    _end("Gamma Spectrum")

    _start("Neutron Spectrum")
    for i, E in enumerate(n_centers_ref.tolist()):
        groups.append("Neutron Spectrum")
        labels.append(f"Nspec_bin{i:03d}_E{E:.6g}MeV")
    _end("Neutron Spectrum")

    _start("Gamma Multiplicity")
    for frag in ["total", "light_fragment", "heavy_fragment"]:
        for field in ["counts", "probabilities"]:
            for m in gamma_m_vals.tolist():
                groups.append("Gamma Multiplicity")
                labels.append(f"Gmult_{frag}_{field}_m{int(m)}")
    _end("Gamma Multiplicity")

    _start("Neutron Multiplicity")
    for frag in ["total", "light_fragment", "heavy_fragment"]:
        for field in ["counts", "probabilities"]:
            for m in neutron_m_vals.tolist():
                groups.append("Neutron Multiplicity")
                labels.append(f"Nmult_{frag}_{field}_m{int(m)}")
    _end("Neutron Multiplicity")

    schema = ObservableSchema(
        groups=groups,
        labels=labels,
        group_boundaries=boundaries
    )
    aux = {
        "gamma_centers":        g_centers_ref,
        "neutron_centers":      n_centers_ref,
        "gamma_m_vals":         gamma_m_vals,
        "neutron_m_vals":       neutron_m_vals,
        # Boolean masks into the original full arrays — used by vectorize_task
        "gamma_spectrum_mask":  g_mask,
        "neutron_spectrum_mask": n_mask,
    }
    return schema, aux

# ----------------------------- Vectorisation ------------------------------- #

def vectorize_task(
    td: TaskData,
    schema: ObservableSchema,
    aux: dict,
    debug: bool = False,
) -> np.ndarray:
    gamma_m_vals:  np.ndarray = aux["gamma_m_vals"]
    neutron_m_vals: np.ndarray = aux["neutron_m_vals"]
    g_mask: np.ndarray = aux["gamma_spectrum_mask"]
    n_mask: np.ndarray = aux["neutron_spectrum_mask"]

    vec_parts: List[np.ndarray] = []

    # Apply the energy cutoff masks so the vectors stay consistent with schema
    vec_parts.append(np.asarray(td.gamma_spectrum,  dtype=float)[g_mask].copy())
    vec_parts.append(np.asarray(td.neutron_spectrum, dtype=float)[n_mask].copy())

    def _vec_mult(
        mult: Dict[str, Dict[str, Dict[int, float]]],
        m_vals: np.ndarray,
        tag: str,
    ) -> np.ndarray:
        out = []
        for frag in ["total", "light_fragment", "heavy_fragment"]:
            for field in ["counts", "probabilities"]:
                d = mult[frag][field]
                arr = np.zeros(m_vals.shape[0], dtype=float)
                missing = 0
                for i, m in enumerate(m_vals.tolist()):
                    if int(m) in d:
                        arr[i] = float(d[int(m)])
                    else:
                        missing += 1
                if missing and debug:
                    _debug_print(
                        debug,
                        f"[VEC] task {td.task_id}: {tag} {frag}/{field} "
                        f"missing {missing} mult bins -> filled 0"
                    )
                out.append(arr)
        return np.concatenate(out, axis=0)

    vec_parts.append(_vec_mult(td.gamma_mult,   gamma_m_vals,  "gamma"))
    vec_parts.append(_vec_mult(td.neutron_mult, neutron_m_vals, "neutron"))

    vec = np.concatenate(vec_parts, axis=0)
    if vec.shape[0] != len(schema.labels):
        raise RuntimeError(
            f"Vector length {vec.shape[0]} != schema length {len(schema.labels)}"
        )
    return vec

# ----------------------------- Sensitivity --------------------------------- #

# Threshold: an observable column is "zero-signal" if the maximum absolute
# value across ALL runs is below this fraction of the global max-abs per column.
# Tweak via --zero_signal_frac if needed.
_DEFAULT_ZERO_SIGNAL_FRAC = 1e-6

def compute_sensitivity(
    scales: np.ndarray,
    Y: np.ndarray,
    zero_signal_frac: float = _DEFAULT_ZERO_SIGNAL_FRAC,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Returns
    -------
    S    : (n_obs,) sensitivity coefficients, NaN where not OK
    mask : (n_obs,) SensitivityMask codes (int8)
    """
    scales = np.asarray(scales, dtype=float)
    Y      = np.asarray(Y,      dtype=float)
    n_obs  = Y.shape[1]
    S      = np.full(n_obs, np.nan,       dtype=float)
    mask   = np.full(n_obs, SensMask.OK,  dtype=np.int8)

    # ---- global check: fewer than 2 runs --------------------------------- #
    if scales.shape[0] < 2:
        mask[:] = SensMask.INSUFFICIENT_DATA
        return S, mask

    # ---- global check: zero scale variance ------------------------------- #
    s_mean = scales.mean()
    s_var  = np.sum((scales - s_mean) ** 2)
    if s_var == 0.0:
        mask[:] = SensMask.ZERO_SCALE_VAR
        return S, mask

    # ---- per-column: zero-signal detection ------------------------------- #
    # An observable is "zero-signal" if its max |value| across all runs is
    # below zero_signal_frac × (global max |value| across all obs and runs).
    col_max  = np.max(np.abs(Y), axis=0)  # (n_obs,)
    glob_max = col_max.max() if col_max.size else 0.0
    threshold = zero_signal_frac * (glob_max if glob_max > 0 else 1.0)
    zero_cols = col_max < threshold
    mask[zero_cols] = SensMask.ZERO_SIGNAL
    # Leave those columns as NaN in S — do not run regression on them

    # ---- regression on the remaining columns ----------------------------- #
    active = ~zero_cols  # boolean mask
    if not np.any(active):
        return S, mask

    Ya     = Y[:, active]  # (n_runs, n_active)
    y_mean = Ya.mean(axis=0)
    cov    = np.sum((scales[:, None] - s_mean) * (Ya - y_mean[None, :]), axis=0)
    b      = cov / s_var
    a      = y_mean - b * s_mean
    y1     = a + b * 1.0  # predicted at scale = 1

    # b, y1 are compact arrays of length n_active — all indexing below uses
    # local indices (0..n_active-1), mapped back to global via active_idx.
    active_idx = np.where(active)[0]  # global col indices, len=n_active

    # Mark non-finite regression results (local boolean, length n_active)
    bad_reg = ~(np.isfinite(b) & np.isfinite(y1))
    mask[active_idx[bad_reg]] = SensMask.REGRESSION_FAILURE

    # Also guard exact zero y1 (rare after zero_signal check, but possible)
    zero_y1 = (np.abs(y1) == 0.0)
    mask[active_idx[zero_y1]] = SensMask.REGRESSION_FAILURE

    # Assign S for all remaining OK columns in one vectorised step
    ok_local  = ~bad_reg & ~zero_y1   # local boolean, length n_active
    ok_global = active_idx[ok_local]  # global column indices
    S[ok_global] = b[ok_local] / y1[ok_local]

    return S, mask

# ----------------------------- Output helpers ------------------------------ #

def write_observable_index(out_path: str, schema: ObservableSchema) -> None:
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["col_index", "group", "label"])
        for i, (g, lab) in enumerate(zip(schema.groups, schema.labels)):
            w.writerow([i, g, lab])

def write_sensitivity_csv(
    out_path: str,
    param_names: List[str],
    schema: ObservableSchema,
    S: np.ndarray,
) -> None:
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["parameter"] + schema.labels)
        for i, p in enumerate(param_names):
            row = [p] + [
                ("" if not np.isfinite(x) else f"{x:.12e}")
                for x in S[i, :].tolist()
            ]
            w.writerow(row)

def write_mask_csv(
    out_path: str,
    param_names: List[str],
    schema: ObservableSchema,
    MASK: np.ndarray,
) -> None:
    """Write the integer mask matrix — useful for diagnostics."""
    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["parameter"] + schema.labels)
        for i, p in enumerate(param_names):
            w.writerow([p] + MASK[i, :].tolist())

def _build_legend_patches() -> List[mpatches.Patch]:
    patches = []
    for code, colour in _MASK_COLOURS.items():
        patches.append(
            mpatches.Patch(facecolor=colour, edgecolor="k", linewidth=0.5,
                           label=_MASK_LABELS[code])
        )
    return patches

def plot_heatmap(
    out_path: str,
    S: np.ndarray,
    MASK: np.ndarray,
    param_names: List[str],
    schema: ObservableSchema,
    vmax_pct: float,
    clip_pct: float,
    fig_dpi: int,
    title: str,
    x_coords: Optional[np.ndarray] = None,
    x_label: str = "Observable index",
) -> None:
    """
    Render the sensitivity heatmap with per-cause NaN colouring.

    The approach:
    1. Draw the coolwarm image for all cells (NaNs rendered transparent).
    2. Overlay a solid-colour layer for each NaN cause using an RGBA image.

    Parameters
    ----------
    x_coords : 1-D array of length n_obs, optional
        Physical x-axis coordinates for each column (e.g. bin-centre energies
        in MeV, or integer multiplicity values).  When supplied the x-axis is
        ticked at these values; otherwise it shows the column index.
    x_label : str
        Label for the x-axis (e.g. "Energy (MeV)" or "Multiplicity").
    """
    S    = np.asarray(S,    dtype=float)
    MASK = np.asarray(MASK, dtype=np.int8)
    n_params, n_obs = S.shape

    # Colour scale: clip extreme values so outliers don't swamp the image
    vmax = _nanpercentile_abs(S, vmax_pct)
    if not np.isfinite(vmax) or vmax <= 0:
        vmax = 1.0
    clip_val = _nanpercentile_abs(S, clip_pct)
    if np.isfinite(clip_val) and clip_val > 0:
        S    = np.clip(S, -clip_val, clip_val)
        vmax = min(vmax, clip_val)

    width  = max(14.0, n_obs    / 35.0)
    height = max(8.0,  n_params /  5.0)
    fig, ax = plt.subplots(figsize=(width, height), dpi=fig_dpi)

    # ---- step 1: coolwarm base ------------------------------------------- #
    # Use x_coords to set the physical extent of the image on the x-axis so
    # that matplotlib's tick locator works in meaningful units.
    cmap = plt.cm.coolwarm.copy()
    cmap.set_bad(color=(1, 1, 1, 0))  # transparent for NaN → overridden below

    if x_coords is not None and x_coords.size == n_obs:
        # Half-bin width in data units for correct pixel alignment
        if n_obs > 1:
            half = (x_coords[-1] - x_coords[0]) / (2.0 * (n_obs - 1))
        else:
            half = 0.5
        x_left  = float(x_coords[0])  - half
        x_right = float(x_coords[-1]) + half
    else:
        x_left, x_right = -0.5, n_obs - 0.5

    img_extent = [x_left, x_right, n_params - 0.5, -0.5]

    im = ax.imshow(
        S,
        aspect="auto",
        cmap=cmap,
        vmin=-vmax, vmax=vmax,
        interpolation="nearest",
        extent=img_extent,
    )

    # ---- step 2: overlay NaN-cause colours -------------------------------- #
    overlay = np.zeros((n_params, n_obs, 4), dtype=float)
    for code, colour in _MASK_COLOURS.items():
        rows, cols = np.where(MASK == int(code))
        if rows.size:
            overlay[rows, cols, :] = colour
    ax.imshow(overlay, aspect="auto", interpolation="nearest", extent=img_extent)

    # ---- labels & ticks -------------------------------------------------- #
    # Title: bold, with sufficient top padding to avoid overlap with the axes.
    # The colour-scale info is placed on a second line, also bold.
    ax.set_title(
        f"{title}\n"
        f"$\\bf{{Colour\\ scale:\\ \\pm P{{{int(vmax_pct)}}}(|S|)"
        f"\\ =\\ \\pm {vmax:.3g}"
        f"\\ (clipped\\ at\\ P{{{int(clip_pct)}}}\\ =\\ \\pm {clip_val:.3g})}}$",
        fontsize=9,
        fontweight="bold",
        pad=14,          # extra vertical clearance so the two-line title
    )                    # doesn't sit on top of the image

    # y-axis: parameter names in bold
    ax.set_yticks(np.arange(n_params))
    ax.set_yticklabels(param_names, fontsize=6, fontweight="bold")

    # x-axis: straight (horizontal) labels, meaningful units
    ax.set_xlabel(x_label, fontsize=8)
    ax.tick_params(axis="x", labelsize=7, rotation=0)

    # ---- group boundary lines -------------------------------------------- #
    # In physical coordinates, group boundaries sit between columns.
    # Convert column-index boundaries to physical x positions.
    for g, (gs, ge) in schema.group_boundaries.items():
        if x_coords is not None and x_coords.size == n_obs:
            if n_obs > 1:
                col_width = (x_coords[-1] - x_coords[0]) / (n_obs - 1)
            else:
                col_width = 1.0
            left_x  = float(x_coords[gs])      - 0.5 * col_width if gs  < n_obs else x_right
            right_x = float(x_coords[ge - 1])  + 0.5 * col_width if ge <= n_obs else x_right
            mid_x   = (left_x + right_x) / 2.0
            ax.axvline(left_x, color="k", linewidth=0.7)
        else:
            ax.axvline(gs - 0.5, color="k", linewidth=0.7)
            mid_x = (gs + ge) / 2.0
       # ax.text(mid_x, -1.5, g,
       #         ha="center", va="bottom", fontsize=8, rotation=0,
       #         clip_on=False,
       #         transform=ax.get_xaxis_transform() if x_coords is None else ax.transData)

    ax.axvline(x_right, color="k", linewidth=0.7)

    # ---- colour bar -------------------------------------------------------- #
    cbar = fig.colorbar(im, ax=ax, fraction=0.025, pad=0.02)
    cbar.set_label("S = (dy/ds) / y(s=1)", fontsize=8)
    cbar.ax.tick_params(labelsize=7)

    # ---- legend for NaN causes ------------------------------------------- #
    patches = _build_legend_patches()
    ax.legend(
        handles=patches,
        loc="lower right",
        bbox_to_anchor=(1.18, 0.0),
        fontsize=7,
        framealpha=0.9,
        title="Masked cells",
        title_fontsize=7,
    )

    fig.tight_layout()
    fig.savefig(out_path, bbox_inches="tight")
    plt.close(fig)


# --------------- Per-fragment multiplicity subplot helper ------------------ #

# Ordered list of (fragment_key, display_name) pairs — drives both column
# slicing and subplot titles.
_MULT_FRAGMENTS = [
    ("total",          "Total"),
    ("light_fragment", "Light Fragment"),
    ("heavy_fragment", "Heavy Fragment"),
]
_MULT_FIELDS = ["counts", "probabilities"]
# Each fragment contributes len(fields) * len(m_vals) columns in the schema.
_N_FIELDS = len(_MULT_FIELDS)  # = 2


def _mult_fragment_col_slices(
    m_vals: np.ndarray,
) -> List[Tuple[str, str, np.ndarray]]:
    """
    Return a list of (fragment_key, display_name, col_indices_within_group)
    for a multiplicity group whose columns are ordered as:
        [frag0/field0, frag0/field1, frag1/field0, frag1/field1, ...]
    with each sub-block of length len(m_vals).

    col_indices_within_group are 0-based offsets relative to the start of
    the multiplicity group in the full schema.
    """
    n_m = m_vals.shape[0]
    result = []
    for fi, (fkey, fname) in enumerate(_MULT_FRAGMENTS):
        start = fi * _N_FIELDS * n_m
        end   = start + _N_FIELDS * n_m
        result.append((fkey, fname, np.arange(start, end)))
    return result


def plot_multiplicity_heatmaps(
    out_dir: str,
    gname: str,
    S_group: np.ndarray,
    MASK_group: np.ndarray,
    param_names: List[str],
    m_vals: np.ndarray,
    vmax_pct: float,
    clip_pct: float,
    fig_dpi: int,
    drop_all_zero_params: bool = False,
) -> None:
    """
    Produce one heatmap figure per fragment (total / light / heavy) for a
    single multiplicity group (gamma or neutron).

    Each figure contains the columns belonging to that fragment only
    (both 'counts' and 'probabilities' sub-blocks), with the x-axis
    showing repeated integer multiplicity values.

    Parameters
    ----------
    gname       : e.g. "Gamma Multiplicity"
    S_group     : (n_params, n_group_cols)  — already sliced to this group
    MASK_group  : (n_params, n_group_cols)  — same slice
    m_vals      : integer multiplicity values for this group
    """
    frag_slices = _mult_fragment_col_slices(m_vals)

    for fkey, fname, col_idx in frag_slices:
        Sf    = S_group[:,    col_idx]
        MASKf = MASK_group[:, col_idx]

        # x-coords: m_vals repeated once per field
        x_coords = np.tile(m_vals.astype(float), _N_FIELDS)

        pnames_f = param_names

        # Optionally drop all-zero / all-masked rows
        if drop_all_zero_params:
            keep = [
                i for i in range(Sf.shape[0])
                if not np.all(~np.isfinite(Sf[i, :]) | (Sf[i, :] == 0.0))
            ]
            if not keep:
                print(
                    f"[PLOT] {gname} / {fname}: all parameters are "
                    f"zero/masked after --drop_all_zero_params filter, "
                    f"skipping.",
                    flush=True,
                )
                continue
            n_dropped = Sf.shape[0] - len(keep)
            if n_dropped:
                print(
                    f"[PLOT] {gname} / {fname}: dropped {n_dropped} "
                    f"all-zero parameter row(s) from heatmap.",
                    flush=True,
                )
            Sf       = Sf[keep,    :]
            MASKf    = MASKf[keep, :]
            pnames_f = [param_names[i] for i in keep]

        # Build a minimal sub-schema just so plot_heatmap can draw boundary
        # lines between the counts and probabilities sub-blocks.
        n_m   = m_vals.shape[0]
        sub_labels: List[str] = []
        sub_groups: List[str] = []
        sub_bounds: Dict[str, Tuple[int, int]] = {}
        prefix = gname[0]  # 'G' or 'N'
        for field in _MULT_FIELDS:
            sub_bounds[field] = (len(sub_labels), -1)
            for m in m_vals.tolist():
                sub_groups.append(field)
                sub_labels.append(f"{prefix}mult_{fkey}_{field}_m{int(m)}")
            s0, _ = sub_bounds[field]
            sub_bounds[field] = (s0, len(sub_labels))

        sub_schema = ObservableSchema(
            groups=sub_groups,
            labels=sub_labels,
            group_boundaries=sub_bounds,
        )

        safe_fkey = fkey.replace(" ", "_")
        safe_gname = gname.replace(" ", "_")
        out_path = os.path.join(
            out_dir,
            f"sensitivity_heatmap_{safe_gname}_{safe_fkey}.png",
        )
        plot_heatmap(
            out_path=out_path,
            S=Sf,
            MASK=MASKf,
            param_names=pnames_f,
            schema=sub_schema,
            vmax_pct=vmax_pct,
            clip_pct=clip_pct,
            fig_dpi=fig_dpi,
            title=f"Sensitivity Heatmap — {gname} / {fname}",
            x_coords=x_coords,
            x_label="Multiplicity",
        )


def plot_group_heatmaps(
    out_dir: str,
    S: np.ndarray,
    MASK: np.ndarray,
    param_names: List[str],
    schema: ObservableSchema,
    aux: dict,
    vmax_pct: float,
    clip_pct: float,
    fig_dpi: int,
    drop_all_zero_params: bool = False,
) -> None:
    for gname, (s, e) in schema.group_boundaries.items():
        Sg    = S[:,    s:e]
        MASKg = MASK[:, s:e]

        # ---- Multiplicity groups: one figure per fragment ----------------- #
        if gname in ("Gamma Multiplicity", "Neutron Multiplicity"):
            m_vals = (
                aux["gamma_m_vals"]
                if gname == "Gamma Multiplicity"
                else aux["neutron_m_vals"]
            )
            plot_multiplicity_heatmaps(
                out_dir=out_dir,
                gname=gname,
                S_group=Sg,
                MASK_group=MASKg,
                param_names=param_names,
                m_vals=m_vals,
                vmax_pct=vmax_pct,
                clip_pct=clip_pct,
                fig_dpi=fig_dpi,
                drop_all_zero_params=drop_all_zero_params,
            )
            continue

        # ---- Spectrum groups: single figure (original behaviour) ---------- #

        # Optionally drop rows that are entirely non-finite or zero
        if drop_all_zero_params:
            keep = []
            for i in range(Sg.shape[0]):
                row = Sg[i, :]
                all_zero = np.all(~np.isfinite(row) | (row == 0.0))
                if not all_zero:
                    keep.append(i)
            if not keep:
                print(
                    f"[PLOT] {gname}: all parameters are zero/masked after "
                    f"--drop_all_zero_params filter, skipping.",
                    flush=True,
                )
                continue
            Sg    = Sg[keep,    :]
            MASKg = MASKg[keep, :]
            pnames_g = [param_names[i] for i in keep]
            n_dropped = len(param_names) - len(keep)
            if n_dropped:
                print(
                    f"[PLOT] {gname}: dropped {n_dropped} all-zero parameter "
                    f"row(s) from heatmap.",
                    flush=True,
                )
        else:
            pnames_g = param_names

        sub_schema = ObservableSchema(
            groups=schema.groups[s:e],
            labels=schema.labels[s:e],
            group_boundaries={gname: (0, e - s)},
        )

        if gname == "Gamma Spectrum":
            x_coords = aux["gamma_centers"].copy()
            x_label  = "Energy (MeV)"
        elif gname == "Neutron Spectrum":
            x_coords = aux["neutron_centers"].copy()
            x_label  = "Energy (MeV)"
        else:
            x_coords = None
            x_label  = "Observable index"

        out_path = os.path.join(
            out_dir,
            f"sensitivity_heatmap_{gname.replace(' ', '_')}.png"
        )
        plot_heatmap(
            out_path=out_path,
            S=Sg,
            MASK=MASKg,
            param_names=pnames_g,
            schema=sub_schema,
            vmax_pct=vmax_pct,
            clip_pct=clip_pct,
            fig_dpi=fig_dpi,
            title=f"Sensitivity Heatmap — {gname}",
            x_coords=x_coords,
            x_label=x_label,
        )

# ----------------------------- Main --------------------------------------- #

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs_dir",  required=True)
    ap.add_argument("--manifest",  required=True)
    ap.add_argument("--output_dir", required=True)
    ap.add_argument("--vmax_pct",  type=float, default=99.0,
                    help="Percentile of |S| used for colour axis (default 99)")
    ap.add_argument("--clip_pct",  type=float, default=99.5,
                    help="Hard-clip |S| above this percentile before plotting "
                         "(default 99.5).  Prevents a handful of large outliers "
                         "from washing out the colour scale.")
    ap.add_argument("--zero_signal_frac", type=float,
                    default=_DEFAULT_ZERO_SIGNAL_FRAC,
                    help="Observables whose max|value| < frac × global_max are "
                         "masked as ZERO_SIGNAL (default 1e-6).")
    ap.add_argument("--fig_dpi", type=int, default=150)
    ap.add_argument("--debug", action="store_true")
    ap.add_argument("--max_gamma_energy_MeV", type=float, default=None,
                    help="Discard gamma spectrum bins whose centres exceed this "
                         "energy (MeV).  Default: keep all bins.")
    ap.add_argument("--max_neutron_energy_MeV", type=float, default=None,
                    help="Discard neutron spectrum bins whose centres exceed this "
                         "energy (MeV).  Default: keep all bins.")
    ap.add_argument("--drop_all_zero_params", action="store_true",
                    help="If set, parameters whose entire row in a group "
                         "submatrix is non-finite or zero are omitted from "
                         "that group's heatmap.  The reason for being zero "
                         "(any mask code, or a genuine S=0) is irrelevant.")
    args = ap.parse_args()

    runs_dir      = args.runs_dir
    manifest_path = args.manifest
    out_dir       = args.output_dir
    debug         = args.debug

    _mkdir(out_dir)

    print("=" * 60, flush=True)
    print("Phase-I Sensitivity Analysis (CGMF prompt gammas/neutrons)", flush=True)
    print("=" * 60, flush=True)
    print(f"runs_dir               : {runs_dir}",               flush=True)
    print(f"manifest               : {manifest_path}",          flush=True)
    print(f"output_dir             : {out_dir}",                 flush=True)
    print(f"vmax_pct               : {args.vmax_pct}",           flush=True)
    print(f"clip_pct               : {args.clip_pct}",           flush=True)
    print(f"zero_signal_frac       : {args.zero_signal_frac}",   flush=True)
    print(f"fig_dpi                : {args.fig_dpi}",            flush=True)
    print(f"max_gamma_energy_MeV   : {args.max_gamma_energy_MeV}",  flush=True)
    print(f"max_neutron_energy_MeV : {args.max_neutron_energy_MeV}", flush=True)
    print(f"drop_all_zero_params   : {args.drop_all_zero_params}", flush=True)
    print(f"debug                  : {debug}",                   flush=True)
    print("-" * 60, flush=True)

    # 1) Read manifest
    entries = read_manifest(manifest_path)
    print(f"[MANIFEST] Entries          : {len(entries)}", flush=True)
    by_param: Dict[str, List[ManifestEntry]] = {}
    for e in entries:
        by_param.setdefault(e.parameter, []).append(e)
    for p in by_param:
        by_param[p] = sorted(by_param[p], key=lambda x: x.scale)
    param_names = sorted(by_param.keys())
    print(f"[MANIFEST] Unique parameters: {len(param_names)}", flush=True)

    # 2) Load tasks
    all_tasks: List[TaskData] = []
    td_by_taskid: Dict[int, TaskData] = {}
    missing_tasks = 0
    failed_tasks  = 0
    for e in entries:
        td = load_task_data(runs_dir, e, debug=debug)
        if td is None:
            task_dir = os.path.join(runs_dir, f"task_{e.task_id}")
            if not os.path.isdir(task_dir):
                missing_tasks += 1
            else:
                failed_tasks += 1
            continue
        all_tasks.append(td)
        td_by_taskid[td.task_id] = td

    print(f"[LOAD] Loaded task JSONs   : {len(all_tasks)} / {len(entries)}", flush=True)
    print(f"[LOAD] Missing task dirs   : {missing_tasks}",  flush=True)
    print(f"[LOAD] Failed/invalid JSON : {failed_tasks}",   flush=True)

    if not all_tasks:
        print("[FATAL] No valid task JSONs loaded.  Exiting.", flush=True)
        return 2

    # 3) Build schema
    schema, aux = build_schema(
        all_tasks,
        max_gamma_energy_MeV=args.max_gamma_energy_MeV,
        max_neutron_energy_MeV=args.max_neutron_energy_MeV,
        debug=debug,
    )
    n_obs = len(schema.labels)
    print(f"[SCHEMA] Total observables : {n_obs}", flush=True)
    for g, (s, e) in schema.group_boundaries.items():
        print(f"[SCHEMA] {g:20s}: cols [{s}:{e}) n={e-s}", flush=True)

    # 4) Compute sensitivity + mask
    S    = np.full((len(param_names), n_obs), np.nan,                dtype=float)
    MASK = np.full((len(param_names), n_obs), SensMask.INSUFFICIENT_DATA, dtype=np.int8)

    diag_lines = ["parameter,n_manifest,n_loaded,scales_loaded\n"]

    for ip, p in enumerate(param_names):
        m_entries = by_param[p]
        scales, Y_rows = [], []
        for me in m_entries:
            td = td_by_taskid.get(me.task_id)
            if td is None:
                continue
            vec = vectorize_task(td, schema, aux, debug=debug)
            scales.append(td.scale)
            Y_rows.append(vec)

        n_loaded   = len(scales)
        scales_str = ";".join(f"{x:.6g}" for x in scales)
        diag_lines.append(f"{p},{len(m_entries)},{n_loaded},{scales_str}\n")

        if n_loaded < 2:
            _debug_print(
                debug,
                f"[SENS] '{p}': only {n_loaded} runs -> INSUFFICIENT_DATA"
            )
            # MASK row already set to INSUFFICIENT_DATA
            continue

        scales_arr = np.asarray(scales, dtype=float)
        Y          = np.vstack(Y_rows)
        Sp, Mp     = compute_sensitivity(
            scales_arr, Y, zero_signal_frac=args.zero_signal_frac
        )
        S[ip, :]    = Sp
        MASK[ip, :] = Mp

        if debug:
            # Print per-cause counts
            for code in SensMask:
                n_code = int(np.sum(Mp == int(code)))
                if n_code:
                    print(
                        f"[SENS] '{p}' {code.name:20s}: {n_code} cols",
                        flush=True
                    )
            # Top-5 |S|
            finite = np.isfinite(Sp)
            if np.any(finite):
                idx  = np.argsort(np.abs(Sp[finite]))[::-1]
                fidx = np.where(finite)[0]
                topk = min(5, idx.size)
                print(f"[SENS] '{p}' top-{topk} |S|:", flush=True)
                for k in range(topk):
                    j = fidx[idx[k]]
                    print(
                        f"  {schema.labels[j]:45s}  S={Sp[j]: .3e}",
                        flush=True
                    )

    # 5) Summary of mask codes across entire matrix
    print("-" * 60, flush=True)
    print("[MASK] Global counts across all (param × obs) cells:", flush=True)
    total_cells = len(param_names) * n_obs
    for code in SensMask:
        n_code = int(np.sum(MASK == int(code)))
        pct    = 100.0 * n_code / total_cells if total_cells else 0.0
        print(
            f"[MASK] {code.name:22s}: {n_code:8d} ({pct:5.1f}%)",
            flush=True
        )

    # 6) Write outputs
    obs_index_csv = os.path.join(out_dir, "observable_index.csv")
    write_observable_index(obs_index_csv, schema)

    sens_csv = os.path.join(out_dir, "sensitivity_matrix.csv")
    write_sensitivity_csv(sens_csv, param_names, schema, S)

    sens_npy = os.path.join(out_dir, "sensitivity_matrix.npy")
    np.save(sens_npy, S)

    mask_csv = os.path.join(out_dir, "sensitivity_mask.csv")
    write_mask_csv(mask_csv, param_names, schema, MASK)

    mask_npy = os.path.join(out_dir, "sensitivity_mask.npy")
    np.save(mask_npy, MASK)

    diag_path = os.path.join(out_dir, "parameter_load_diagnostics.csv")
    with open(diag_path, "w") as f:
        f.writelines(diag_lines)

    summary_path = os.path.join(out_dir, "summary.txt")
    with open(summary_path, "w") as f:
        f.write("Phase-I Sensitivity Analysis Summary\n")
        f.write("-" * 37 + "\n")
        f.write(f"Manifest entries          : {len(entries)}\n")
        f.write(f"Unique parameters         : {len(param_names)}\n")
        f.write(f"Loaded task JSONs         : {len(all_tasks)}\n")
        f.write(f"Missing task directories  : {missing_tasks}\n")
        f.write(f"Failed/invalid JSONs      : {failed_tasks}\n")
        f.write(f"Total observables (cols)  : {n_obs}\n")
        if args.max_gamma_energy_MeV is not None:
            f.write(f"Gamma energy cutoff       : <= {args.max_gamma_energy_MeV} MeV\n")
        if args.max_neutron_energy_MeV is not None:
            f.write(f"Neutron energy cutoff     : <= {args.max_neutron_energy_MeV} MeV\n")
        f.write("\nGroup boundaries:\n")
        for g, (s, e) in schema.group_boundaries.items():
            f.write(f"  {g:20s}: cols [{s}:{e}) n={e-s}\n")
        f.write("\nMask code counts:\n")
        for code in SensMask:
            n_code = int(np.sum(MASK == int(code)))
            pct    = 100.0 * n_code / total_cells if total_cells else 0.0
            f.write(f"  {code.name:22s}: {n_code:8d} ({pct:5.1f}%)\n")
        vmax = _nanpercentile_abs(S, args.vmax_pct)
        f.write(f"\nPlot colour scale: ±P{args.vmax_pct}(|S|) = ±{vmax:.6g}\n")

    # 7) Plots
    plot_group_heatmaps(
        out_dir=out_dir,
        S=S,
        MASK=MASK,
        param_names=param_names,
        schema=schema,
        aux=aux,
        vmax_pct=args.vmax_pct,
        clip_pct=args.clip_pct,
        fig_dpi=args.fig_dpi,
        drop_all_zero_params=args.drop_all_zero_params,
    )

    print("-" * 60, flush=True)
    print("[DONE] Wrote outputs:", flush=True)
    for p in [sens_npy, sens_csv, mask_npy, mask_csv,
              obs_index_csv, diag_path, summary_path]:
        print(f"  {p}", flush=True)
    print("=" * 60, flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

