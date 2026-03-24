#!/usr/bin/env python3
"""
analyse_sensitivity.py
======================
Phase-II Sampling post-processor.
 
Reads the pre-computed JSON/CSV outputs from each task_* directory and
produces ensemble visualisations showing the spread of all observables
across the perturbed parameter set, with a "default" reference run
overlaid throughout.
 
Optionally overlays evaluated (experimental/library) data for:
  - Prompt Fission Neutron Spectrum  (PFNS)
  - Prompt Fission Gamma Spectrum    (PFGS)
  - Average neutron multiplicity nubar_n (with uncertainty)
  - Average gamma  multiplicity nubar_g (with uncertainty)
 
Also computes χ² fit quality scores against ENDF-8 evaluated data,
producing 5 values per run:
  chi2_pfns     – reduced χ² vs ENDF PFNS  (over optional energy window)
  chi2_pfgs     – reduced χ² vs ENDF PFGS  (over optional energy window)
  chi2_nubar_n  – scalar χ² for ν̄ₙ
  chi2_nubar_g  – scalar χ² for ν̄ᵧ
  chi2_combined – user-weighted sum of all available terms
 
Optionally filters to the N_accepted best runs (lowest chi2_combined) and
re-generates figures 1–5 for that sub-set in a dedicated sub-directory.
 
Usage
-----
    python analyse_sensitivity.py \\
        --runs_dir    runs/ \\
        --manifest    configs/manifest.csv \\
        --default_dir /path/to/default_task/ \\
        --output_dir  sensitivity_results/ \\
        --vmax_pct    85 \\
        --fig_dpi     250 \\
        --max_gamma_energy_MeV   4.3 \\
        --max_neutron_energy_MeV 5.5 \\
        --pfns_file   data/U235_PFNS_wUNC.txt \\
        --pfgs_file   data/My_UNCS_v1.txt \\
        --pfns_emin   0.1 --pfns_emax 5.5 \\
        --pfgs_emin   0.2 --pfgs_emax 4.3 \\
        --eval_nubar_n 2.4355 --eval_nubar_n_unc 0.005 \\
        --eval_nubar_g 8.02   --eval_nubar_g_unc 0.30 \\
        --w_pfns 0.4 --w_pfgs 0.4 --w_nubar_n 0.1 --w_nubar_g 0.1 \\
        --n_accepted  20 \\
        --drop_all_zero_params \\
        --debug
 
Output files
------------
    sensitivity_results/
    ├── 01_scalar_observables.png
    ├── 02_gamma_spectrum_envelope.png
    ├── 03_neutron_spectrum_envelope.png
    ├── 04_gamma_multiplicity_envelope.png
    ├── 05_neutron_multiplicity_envelope.png
    ├── 06_chi2_fit_scores.png
    ├── summary_statistics.csv          (includes χ² columns)
    └── accepted_N/                     (only when --n_accepted is used)
        ├── accepted_task_ids.txt
        ├── 01_scalar_observables.png
        ├── 02_gamma_spectrum_envelope.png
        ├── 03_neutron_spectrum_envelope.png
        ├── 04_gamma_multiplicity_envelope.png
        └── 05_neutron_multiplicity_envelope.png
"""
 
import argparse
import glob
import json
import logging
import os
import re
import sys
from pathlib import Path
 
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.patches as mpatches
from matplotlib.lines import Line2D
import csv
 
# ──────────────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────────────
 
log = logging.getLogger("analyse_sensitivity")
 
 
# ──────────────────────────────────────────────────────────────────────────────
# Helpers – file discovery
# ──────────────────────────────────────────────────────────────────────────────
 
def find_json_in_dir(task_dir: str) -> str | None:
    """Return the first *.json file found in task_dir, or None."""
    hits = glob.glob(os.path.join(task_dir, "*.json"))
    hits = [h for h in hits if os.path.basename(h) != "metadata.json"]
    if not hits:
        return None
    if len(hits) > 1:
        log.debug("Multiple JSON files in %s – using %s", task_dir, hits[0])
    return hits[0]
 
 
def load_task_json(task_dir: str) -> dict | None:
    """Load the analysis JSON for one task directory."""
    path = find_json_in_dir(task_dir)
    if path is None:
        log.warning("No analysis JSON found in %s – skipping", task_dir)
        return None
    try:
        with open(path) as fh:
            return json.load(fh)
    except Exception as exc:
        log.warning("Could not parse %s: %s – skipping", path, exc)
        return None
 
 
def collect_task_dirs(runs_dir: str) -> list[str]:
    """Return sorted list of task_* directories under runs_dir."""
    pattern = os.path.join(runs_dir, "task_*")
    dirs = sorted(glob.glob(pattern),
                  key=lambda p: int(os.path.basename(p).split("_")[1]))
    return dirs
 
 
# ──────────────────────────────────────────────────────────────────────────────
# Data ingestion
# ──────────────────────────────────────────────────────────────────────────────
 
def ingest_all_tasks(runs_dir: str, debug: bool = False) -> list[dict]:
    """Load JSON data for every task, returning only successfully parsed entries."""
    task_dirs = collect_task_dirs(runs_dir)
    log.info("Found %d task directories in %s", len(task_dirs), runs_dir)
 
    records = []
    for td in task_dirs:
        task_id = os.path.basename(td)
        data = load_task_json(td)
        if data is None:
            continue
        data["_task_id"] = task_id
        data["_task_dir"] = td
        records.append(data)
        if debug:
            log.debug("Loaded %s → %s", task_id, find_json_in_dir(td))
 
    log.info("Successfully loaded %d / %d tasks", len(records), len(task_dirs))
    return records
 
 
# ──────────────────────────────────────────────────────────────────────────────
# Evaluated data loaders
# ──────────────────────────────────────────────────────────────────────────────
 
def load_pfgs_file(path: str) -> dict | None:
    """
    Load an evaluated Prompt Fission Gamma Spectrum file.
 
    Expected format (whitespace-delimited, # comments ignored):
        Column 0 : Photon energy  (MeV)
        Column 1 : Yield          (Gammas / MeV / fission)
        Column 2 : Lower bound    (absolute, same units as Column 1)
        Column 3 : Upper bound    (absolute, same units as Column 1)
 
    Returns dict with keys: energy_MeV, yield, lower, upper
    """
    try:
        data = np.loadtxt(path, comments="#")
        if data.ndim == 1:
            data = data.reshape(1, -1)
        if data.shape[1] < 4:
            log.error("PFGS file %s must have at least 4 columns – skipping", path)
            return None
        return {
            "energy_MeV": data[:, 0],
            "yield":      data[:, 1],
            "lower":      data[:, 2],
            "upper":      data[:, 3],
        }
    except Exception as exc:
        log.error("Could not load PFGS file %s: %s", path, exc)
        return None
 
 
# U-235 thermal nubar used to scale normalised Chi(E) → absolute n/MeV/fission
_NUBAR_U235_THERMAL = 2.4355
 
_ENDF_EXPONENT_RE = re.compile(r'(\d)([-+])(\d)')
 
 
def _fix_endf_float(token: str) -> str:
    """Inject missing 'e' in ENDF shorthand: 9.999-6 → 9.999e-6."""
    return _ENDF_EXPONENT_RE.sub(r'\1e\2\3', token)
 
 
def load_pfns_file(path: str) -> dict | None:
    """
    Load an evaluated Prompt Fission Neutron Spectrum (ENDF-style) file.
 
    Expected format (whitespace-delimited, # comments ignored):
        Column 0 : Energy   in eV       (will be converted to MeV)
        Column 1 : Chi(E)   in 1/eV     (normalised PDF; will be scaled)
        Column 2 : Unc      in 1/eV     (1-sigma; will be scaled)
 
    Conversions applied:
        energy   : eV → MeV  (÷ 1e6)
        Chi, Unc : 1/eV → 1/MeV  (× 1e6), then × nubar_U235_thermal
                   → final units: Neutrons / MeV / fission
 
    Returns dict with keys: energy_MeV, yield, lower, upper
    """
    rows = []
    try:
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                tokens = [_fix_endf_float(t) for t in line.split()]
                if len(tokens) < 3:
                    continue
                try:
                    rows.append([float(t) for t in tokens[:3]])
                except ValueError as exc:
                    log.debug("Skipping unparseable line in PFNS file: %s (%s)", line, exc)
    except Exception as exc:
        log.error("Could not load PFNS file %s: %s", path, exc)
        return None
 
    if not rows:
        log.error("No data parsed from PFNS file %s", path)
        return None
 
    arr = np.array(rows)
    energy_MeV = arr[:, 0] / 1.0e6
    chi_per_MeV = arr[:, 1] * 1.0e6
    unc_per_MeV = arr[:, 2] * 1.0e6
 
    yield_abs = chi_per_MeV * _NUBAR_U235_THERMAL
    unc_abs   = unc_per_MeV * _NUBAR_U235_THERMAL
 
    return {
        "energy_MeV": energy_MeV,
        "yield":      yield_abs,
        "lower":      yield_abs - unc_abs,
        "upper":      yield_abs + unc_abs,
    }
 
 
# ──────────────────────────────────────────────────────────────────────────────
# Palette / style helpers
# ──────────────────────────────────────────────────────────────────────────────
 
PALETTE = {
    "gamma":     "b",
    "neutron":   "r",
    "light":     "g",
    "heavy":     "m",
    "default":   "k",
    "evaluated": "#e07b00",   # orange – evaluated / experimental reference
}
 
plt.rcParams.update({
    "axes.titlesize":  12,
    "axes.labelsize":  11,
    "xtick.labelsize": 8,
    "ytick.labelsize": 8,
    "legend.fontsize": 9,
})
 
 
def _save(fig, path: str, dpi: int):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fig.savefig(path, dpi=dpi, bbox_inches="tight")
    plt.close(fig)
    log.info("Saved → %s", path)
 
 
# ──────────────────────────────────────────────────────────────────────────────
# Figure 1 – Scalar observables  (violin + strip)
# ──────────────────────────────────────────────────────────────────────────────
 
SCALAR_KEYS = [
    ("avg_gamma_multiplicity",       "ν̄_γ  (γ/fission)",     PALETTE["gamma"]),
    ("avg_neutron_multiplicity",     "ν̄_n  (n/fission)",     PALETTE["neutron"]),
    ("avg_single_gamma_energy_MeV",  "ε̄_γ  (MeV)",           PALETTE["light"]),
    ("avg_total_gamma_energy_MeV",   "ε̄_tot  (MeV/fission)", PALETTE["heavy"]),
]
 
# Map observable key → (eval_value, eval_unc) supplied via CLI args.
# Populated in main() when the user supplies --eval_nubar_* flags.
_EVAL_SCALARS: dict[str, tuple[float, float]] = {}
 
 
def plot_scalar_observables(records: list[dict],
                            default_record: dict | None,
                            output_dir: str,
                            dpi: int,
                            title_suffix: str = ""):
    n_label = f"{len(records)} perturbed runs"
    if title_suffix:
        n_label += f"  |  {title_suffix}"
    fig, axes = plt.subplots(1, 4, figsize=(18, 6))
    fig.suptitle(f"Scalar Observables – Ensemble Spread\n({n_label})",
                 fontsize=13, y=1.01)
 
    for ax, (key, label, colour) in zip(axes, SCALAR_KEYS):
        values = []
        for rec in records:
            v = rec.get("observables", {}).get(key)
            if v is not None:
                values.append(v)
 
        if not values:
            ax.set_title(label)
            continue
 
        values = np.array(values)
 
        # ── violin ──────────────────────────────────────────────────────────
        parts = ax.violinplot([values], positions=[0], widths=0.7,
                              showmedians=False, showextrema=False)
        for pc in parts["bodies"]:
            pc.set_facecolor(colour)
            pc.set_alpha(0.45)
            pc.set_edgecolor(colour)
 
        # percentile box
        p5, p25, p50, p75, p95 = np.percentile(values, [5, 25, 50, 75, 95])
        ax.vlines(0, p25, p75, color=colour, linewidth=6, alpha=0.7, zorder=3)
        ax.vlines(0, p5,  p95, color=colour, linewidth=2, alpha=0.5, zorder=2)
        ax.scatter([0], [p50], color="white", s=40, zorder=5)
 
        # ── strip (individual points) ────────────────────────────────────────
        jitter = np.random.default_rng(42).uniform(-0.18, 0.18, len(values))
        ax.scatter(jitter, values, color=colour, alpha=0.25, s=8, zorder=2)
 
        # ── default reference ────────────────────────────────────────────────
        handles = []
        if default_record is not None:
            dv = default_record.get("observables", {}).get(key)
            if dv is not None:
                ax.axhline(dv, color=PALETTE["default"], linewidth=1.8,
                           linestyle="--", zorder=6)
                handles.append(Line2D([0], [0], color=PALETTE["default"],
                                      linewidth=1.8, linestyle="--",
                                      label="Default"))
 
        # ── evaluated reference (with optional uncertainty band) ─────────────
        if key in _EVAL_SCALARS:
            eval_val, eval_unc = _EVAL_SCALARS[key]
            ax.axhline(eval_val, color=PALETTE["evaluated"], linewidth=1.8,
                       linestyle="-.", zorder=7)
            if eval_unc > 0:
                ax.axhspan(eval_val - eval_unc, eval_val + eval_unc,
                           color=PALETTE["evaluated"], alpha=0.15, zorder=1)
            handles.append(Line2D([0], [0], color=PALETTE["evaluated"],
                                  linewidth=1.8, linestyle="-.",
                                  label="Evaluated"))
 
        if handles:
            ax.legend(handles=handles, loc="upper right")
 
        ax.set_title(label, pad=8)
        ax.set_xticks([])
        ax.set_xlim(-0.6, 0.6)
        ax.grid(axis="y", alpha=0.4)
 
        ax.text(0.5, -0.08,
                f"μ={values.mean():.4f}\nσ={values.std():.4f}",
                transform=ax.transAxes,
                ha="center", va="top", fontsize=7.5)
 
    fig.tight_layout()
    _save(fig, os.path.join(output_dir, "01_scalar_observables.png"), dpi)
 
 
# ──────────────────────────────────────────────────────────────────────────────
# Helper – build ensemble spectral arrays (handles variable-length bins)
# ──────────────────────────────────────────────────────────────────────────────
 
def _build_spectrum_ensemble(records: list[dict],
                              spec_key: str,
                              emax: float | None = None):
    reference_centers = None
    rows = []
 
    for rec in records:
        block = rec.get(spec_key, {})
        centers = block.get("bin_centers_MeV")
        spectrum = block.get("spectrum")
        if centers is None or spectrum is None:
            continue
        centers  = np.array(centers)
        spectrum = np.array(spectrum)
 
        if emax is not None:
            mask     = centers <= emax
            centers  = centers[mask]
            spectrum = spectrum[mask]
 
        if reference_centers is None:
            reference_centers = centers
 
        rows.append(np.interp(reference_centers, centers, spectrum))
 
    if not rows:
        return None, None
 
    return reference_centers, np.vstack(rows)
 
 
def _extract_default_spectrum(default_record: dict | None,
                               spec_key: str,
                               ref_centers: np.ndarray,
                               emax: float | None = None):
    if default_record is None:
        return None
    block = default_record.get(spec_key, {})
    centers  = np.array(block.get("bin_centers_MeV", []))
    spectrum = np.array(block.get("spectrum", []))
    if centers.size == 0:
        return None
    if emax is not None:
        mask = centers <= emax
        centers, spectrum = centers[mask], spectrum[mask]
    return np.interp(ref_centers, centers, spectrum)
 
 
# ──────────────────────────────────────────────────────────────────────────────
# Figure 2 – Gamma spectrum envelope
# ──────────────────────────────────────────────────────────────────────────────
 
def _spectrum_envelope_figure(centers, ensemble, default_spec,
                               colour, particle_label,
                               ylabel, title, output_path, dpi,
                               logscale=True,
                               eval_data: dict | None = None,
                               emax: float | None = None):
    """Generic routine for gamma or neutron spectrum envelope."""
    p5  = np.percentile(ensemble, 5,  axis=0)
    p25 = np.percentile(ensemble, 25, axis=0)
    p50 = np.percentile(ensemble, 50, axis=0)
    p75 = np.percentile(ensemble, 75, axis=0)
    p95 = np.percentile(ensemble, 95, axis=0)
 
    fig, ax = plt.subplots(figsize=(11, 6))
    fig.suptitle(title, fontsize=13)
 
    # All individual runs – thin, transparent
    for row in ensemble:
        ax.plot(centers, row, color=colour, alpha=0.06, linewidth=0.5)
 
    # Percentile bands
    ax.fill_between(centers, p5,  p95, color=colour, alpha=0.18, label="5–95 pct")
    ax.fill_between(centers, p25, p75, color=colour, alpha=0.35, label="25–75 pct")
 
    # Median
    ax.plot(centers, p50, color=colour, linewidth=1.8,
            alpha=0.9, label="Median")
 
    # CGMF default reference
    if default_spec is not None:
        ax.plot(centers, default_spec, color=PALETTE["default"],
                linewidth=2.0, linestyle="--", zorder=5, label="CGMF Default")
 
    # ── Evaluated / experimental reference ───────────────────────────────────
    if eval_data is not None:
        ev_e  = eval_data["energy_MeV"]
        ev_y  = eval_data["yield"]
        ev_lo = eval_data["lower"]
        ev_hi = eval_data["upper"]
 
        # Optionally clip to emax
        if emax is not None:
            mask = ev_e <= emax
            ev_e, ev_y, ev_lo, ev_hi = ev_e[mask], ev_y[mask], ev_lo[mask], ev_hi[mask]
 
        if ev_e.size > 0:
            ax.plot(ev_e, ev_y, color=PALETTE["evaluated"],
                    linewidth=2.0, linestyle="-.", zorder=6, label="Evaluated")
            ax.fill_between(ev_e, ev_lo, ev_hi,
                            color=PALETTE["evaluated"], alpha=0.20, zorder=1,
                            label="Evaluated unc.")
 
    if logscale:
        ax.set_yscale("log")
 
    ax.set_xlabel("Energy  (MeV)", fontsize=10)
    ax.set_ylabel(ylabel, fontsize=10)
    ax.legend(loc="upper right")
    ax.grid(True, which="both", alpha=0.3)
    ax.set_xlim(centers[0], centers[-1])
 
    fig.tight_layout()
    _save(fig, output_path, dpi)
 
 
def plot_gamma_spectrum(records, default_record, output_dir, dpi,
                        emax=None, eval_data=None, title_suffix: str = ""):
    centers, ensemble = _build_spectrum_ensemble(records, "gamma_spectrum", emax)
    if ensemble is None:
        log.warning("No gamma spectrum data – skipping Fig 2")
        return
    default_spec = _extract_default_spectrum(default_record, "gamma_spectrum",
                                             centers, emax)
    n_label = f"{len(ensemble)} runs"
    if title_suffix:
        n_label += f"  |  {title_suffix}"
    _spectrum_envelope_figure(
        centers, ensemble, default_spec,
        colour=PALETTE["gamma"],
        particle_label="γ",
        ylabel="Yield  (γ / MeV / fission)",
        title=f"Gamma Energy Spectrum – Ensemble Envelope  ({n_label})",
        output_path=os.path.join(output_dir, "02_gamma_spectrum_envelope.png"),
        dpi=dpi,
        eval_data=eval_data,
        emax=emax,
    )
 
 
def plot_neutron_spectrum(records, default_record, output_dir, dpi,
                          emax=None, eval_data=None, title_suffix: str = ""):
    centers, ensemble = _build_spectrum_ensemble(records, "neutron_spectrum", emax)
    if ensemble is None:
        log.warning("No neutron spectrum data – skipping Fig 3")
        return
    default_spec = _extract_default_spectrum(default_record, "neutron_spectrum",
                                             centers, emax)
    n_label = f"{len(ensemble)} runs"
    if title_suffix:
        n_label += f"  |  {title_suffix}"
    _spectrum_envelope_figure(
        centers, ensemble, default_spec,
        colour=PALETTE["neutron"],
        particle_label="n",
        ylabel="Yield  (n / MeV / fission)",
        title=f"Neutron Energy Spectrum – Ensemble Envelope  ({n_label})",
        output_path=os.path.join(output_dir, "03_neutron_spectrum_envelope.png"),
        dpi=dpi,
        eval_data=eval_data,
        emax=emax,
    )
 
 
# ──────────────────────────────────────────────────────────────────────────────
# Helper – multiplicity ensembles
# ──────────────────────────────────────────────────────────────────────────────
 
def _build_mult_ensemble(records, mult_key, sub_key):
    max_len = 0
    for rec in records:
        probs = rec.get(mult_key, {}).get(sub_key, {}).get("probabilities", [])
        max_len = max(max_len, len(probs))
 
    if max_len == 0:
        return None, None
 
    rows = []
    for rec in records:
        probs = rec.get(mult_key, {}).get(sub_key, {}).get("probabilities", [])
        if not probs:
            continue
        padded = np.zeros(max_len)
        padded[:len(probs)] = probs
        rows.append(padded)
 
    common_range = np.arange(max_len)
    return common_range, np.vstack(rows)
 
 
def _extract_default_mult(default_record, mult_key, sub_key, max_len):
    if default_record is None:
        return None
    probs = default_record.get(mult_key, {}).get(sub_key, {}).get("probabilities", [])
    if not probs:
        return None
    out = np.zeros(max_len)
    out[:len(probs)] = probs
    return out
 
 
# ──────────────────────────────────────────────────────────────────────────────
# Figure 4/5 – Multiplicity distribution envelopes
# ──────────────────────────────────────────────────────────────────────────────
 
def _multiplicity_figure(records, default_record, mult_key,
                         panel_configs, title, output_path, dpi,
                         xlim=None):
    """
    3-panel figure: Total | Light Fragment | Heavy Fragment
    panel_configs: list of (sub_key, label, colour)
    """
    fig, axes = plt.subplots(1, 3, figsize=(18, 6), sharey=False)
    fig.suptitle(title, fontsize=13)
 
    for ax, (sub_key, panel_label, colour) in zip(axes, panel_configs):
        nu_range, ensemble = _build_mult_ensemble(records, mult_key, sub_key)
        if ensemble is None:
            ax.set_title(panel_label)
            continue
 
        max_len = ensemble.shape[1]
        default_probs = _extract_default_mult(default_record, mult_key,
                                              sub_key, max_len)
 
        p5  = np.percentile(ensemble,  5, axis=0)
        p25 = np.percentile(ensemble, 25, axis=0)
        p50 = np.percentile(ensemble, 50, axis=0)
        p75 = np.percentile(ensemble, 75, axis=0)
        p95 = np.percentile(ensemble, 95, axis=0)
 
        # Individual runs
        for row in ensemble:
            ax.plot(nu_range, row, color=colour, alpha=0.06, linewidth=0.6)
 
        # Bands
        ax.fill_between(nu_range, p5,  p95, color=colour, alpha=0.18,
                        label="5–95 pct")
        ax.fill_between(nu_range, p25, p75, color=colour, alpha=0.35,
                        label="25–75 pct")
 
        # Median
        ax.plot(nu_range, p50, color=colour, linewidth=1.8, label="Median")
 
        # Default
        if default_probs is not None:
            ax.plot(nu_range, default_probs, color=PALETTE["default"],
                    linewidth=2.0, linestyle="--", zorder=5, label="CGMF Default")
 
        ax.set_title(panel_label, pad=8)
        ax.set_xlabel("Multiplicity  ν", fontsize=9)
        ax.set_ylabel("P(ν)", fontsize=9)
        ax.legend(loc="upper right")
        ax.grid(True, alpha=0.3)
 
        if xlim is not None:
            ax.set_xlim(0, min(xlim, max_len - 1))
        else:
            ax.set_xlim(0, max_len - 1)
 
    fig.tight_layout()
    _save(fig, output_path, dpi)
 
 
def plot_gamma_multiplicity(records, default_record, output_dir, dpi,
                            title_suffix: str = ""):
    n_label = f"{len(records)} runs"
    if title_suffix:
        n_label += f"  |  {title_suffix}"
    _multiplicity_figure(
        records, default_record,
        mult_key="gamma_multiplicity_distributions",
        panel_configs=[
            ("total",          "Total",          PALETTE["gamma"]),
            ("light_fragment", "Light Fragment",  PALETTE["light"]),
            ("heavy_fragment", "Heavy Fragment",  PALETTE["heavy"]),
        ],
        title=f"Gamma Multiplicity Distributions – Ensemble  ({n_label})",
        output_path=os.path.join(output_dir, "04_gamma_multiplicity_envelope.png"),
        dpi=dpi,
        xlim=30,
    )
 
 
def plot_neutron_multiplicity(records, default_record, output_dir, dpi,
                              title_suffix: str = ""):
    n_label = f"{len(records)} runs"
    if title_suffix:
        n_label += f"  |  {title_suffix}"
    _multiplicity_figure(
        records, default_record,
        mult_key="neutron_multiplicity_distributions",
        panel_configs=[
            ("total",          "Total",          PALETTE["neutron"]),
            ("light_fragment", "Light Fragment",  PALETTE["light"]),
            ("heavy_fragment", "Heavy Fragment",  PALETTE["heavy"]),
        ],
        title=f"Neutron Multiplicity Distributions – Ensemble  ({n_label})",
        output_path=os.path.join(output_dir, "05_neutron_multiplicity_envelope.png"),
        dpi=dpi,
        xlim=15,
    )
 
 
# ──────────────────────────────────────────────────────────────────────────────
# Fit quality  –  χ² vs ENDF-8 evaluated data
# ──────────────────────────────────────────────────────────────────────────────
 
def _energy_window(energy: np.ndarray,
                   emin: float | None,
                   emax: float | None) -> np.ndarray:
    """Return boolean mask selecting [emin, emax] from energy array."""
    mask = np.ones(len(energy), dtype=bool)
    if emin is not None:
        mask &= energy >= emin
    if emax is not None:
        mask &= energy <= emax
    return mask
 
 
def _reduced_chi2_spectral(cgmf_centers: np.ndarray,
                            cgmf_spectrum: np.ndarray,
                            eval_data: dict,
                            emin: float | None = None,
                            emax: float | None = None) -> float | None:
    """
    Compute reduced χ² between one CGMF spectrum and an evaluated reference.
 
    The CGMF spectrum is linearly interpolated onto the ENDF energy grid.
    Only points within [emin, emax] are included.
    σᵢ = (upper_i - lower_i) / 2  from the evaluated file.
 
    Returns None if fewer than 2 valid points exist in the window,
    or if all evaluated uncertainties are zero.
    """
    ev_e  = eval_data["energy_MeV"]
    ev_y  = eval_data["yield"]
    ev_lo = eval_data["lower"]
    ev_hi = eval_data["upper"]
 
    # Apply energy window to ENDF grid
    mask = _energy_window(ev_e, emin, emax)
    ev_e  = ev_e[mask]
    ev_y  = ev_y[mask]
    ev_lo = ev_lo[mask]
    ev_hi = ev_hi[mask]
 
    if len(ev_e) < 2:
        log.warning("Fewer than 2 ENDF points in energy window "
                    "[%s, %s] MeV – cannot compute χ²", emin, emax)
        return None
 
    # σ from evaluated uncertainty band
    sigma = (ev_hi - ev_lo) / 2.0
 
    # Exclude points where σ ≤ 0 (ill-defined contribution)
    valid = sigma > 0
    if valid.sum() < 2:
        log.warning("Fewer than 2 ENDF points with σ > 0 in window – "
                    "cannot compute χ²")
        return None
 
    ev_e  = ev_e[valid]
    ev_y  = ev_y[valid]
    sigma = sigma[valid]
 
    # Interpolate CGMF onto ENDF energy grid (extrapolation → 0)
    cgmf_interp = np.interp(ev_e, cgmf_centers, cgmf_spectrum,
                             left=0.0, right=0.0)
 
    residuals    = cgmf_interp - ev_y
    chi2_reduced = float(np.sum((residuals / sigma) ** 2) / len(ev_e))
    return chi2_reduced
 
 
def _chi2_scalar(cgmf_value: float,
                 eval_value: float,
                 eval_unc: float) -> float | None:
    """
    Single-term χ²: ((cgmf - eval) / sigma)².
 
    Returns None if the evaluated uncertainty is zero (metric is undefined).
    """
    if eval_unc <= 0:
        log.warning("Scalar evaluated uncertainty is zero – χ² undefined")
        return None
    return float(((cgmf_value - eval_value) / eval_unc) ** 2)
 
 
def compute_fit_scores(records: list[dict],
                       pfns_data: dict | None,
                       pfgs_data: dict | None,
                       weights: dict,
                       pfns_emin: float | None = None,
                       pfns_emax: float | None = None,
                       pfgs_emin: float | None = None,
                       pfgs_emax: float | None = None) -> list[dict]:
    """
    For every task record compute up to 5 χ² values and attach them in-place
    under record["fit_scores"].
 
    Individual (unweighted) scores
    ───────────────────────────────
    chi2_pfns     – reduced χ² vs ENDF PFNS over [pfns_emin, pfns_emax]
    chi2_pfgs     – reduced χ² vs ENDF PFGS over [pfgs_emin, pfgs_emax]
    chi2_nubar_n  – scalar χ² for ν̄ₙ
    chi2_nubar_g  – scalar χ² for ν̄ᵧ
 
    Combined score
    ──────────────
    chi2_combined – weighted sum of available individual scores.
                    Weights are renormalised over the available (non-None)
                    contributors so the combined score is always well-defined
                    even when some reference data are absent.
 
    Parameters
    ----------
    weights : dict with keys "pfns", "pfgs", "nubar_n", "nubar_g"
              Raw weights (need not sum to 1; normalised internally).
    """
    w_total = sum(weights.values())
    if abs(w_total) < 1e-12:
        raise ValueError("All fit weights are zero – cannot compute combined χ².")
 
    # Normalise to unit sum
    w = {k: v / w_total for k, v in weights.items()}
 
    # Map fit_score key → normalised weight key
    key_map = {
        "chi2_pfns":    "pfns",
        "chi2_pfgs":    "pfgs",
        "chi2_nubar_n": "nubar_n",
        "chi2_nubar_g": "nubar_g",
    }
 
    for rec in records:
        scores: dict[str, float | None] = {
            "chi2_pfns":     None,
            "chi2_pfgs":     None,
            "chi2_nubar_n":  None,
            "chi2_nubar_g":  None,
            "chi2_combined": None,
        }
 
        obs = rec.get("observables", {})
 
        # ── PFNS ──────────────────────────────────────────────────────────────
        if pfns_data is not None:
            block    = rec.get("neutron_spectrum", {})
            centers  = block.get("bin_centers_MeV")
            spectrum = block.get("spectrum")
            if centers is not None and spectrum is not None:
                scores["chi2_pfns"] = _reduced_chi2_spectral(
                    np.array(centers), np.array(spectrum),
                    pfns_data, pfns_emin, pfns_emax,
                )
 
        # ── PFGS ──────────────────────────────────────────────────────────────
        if pfgs_data is not None:
            block    = rec.get("gamma_spectrum", {})
            centers  = block.get("bin_centers_MeV")
            spectrum = block.get("spectrum")
            if centers is not None and spectrum is not None:
                scores["chi2_pfgs"] = _reduced_chi2_spectral(
                    np.array(centers), np.array(spectrum),
                    pfgs_data, pfgs_emin, pfgs_emax,
                )
 
        # ── Scalar: nubar_n ───────────────────────────────────────────────────
        if "avg_neutron_multiplicity" in _EVAL_SCALARS:
            cgmf_val = obs.get("avg_neutron_multiplicity")
            if cgmf_val is not None:
                ev, eu = _EVAL_SCALARS["avg_neutron_multiplicity"]
                scores["chi2_nubar_n"] = _chi2_scalar(cgmf_val, ev, eu)
 
        # ── Scalar: nubar_g ───────────────────────────────────────────────────
        if "avg_gamma_multiplicity" in _EVAL_SCALARS:
            cgmf_val = obs.get("avg_gamma_multiplicity")
            if cgmf_val is not None:
                ev, eu = _EVAL_SCALARS["avg_gamma_multiplicity"]
                scores["chi2_nubar_g"] = _chi2_scalar(cgmf_val, ev, eu)
 
        # ── Combined (renormalise weights over available terms) ───────────────
        available = {k: v for k, v in scores.items()
                     if k in key_map and v is not None}
 
        if available:
            w_avail_sum = sum(w[key_map[k]] for k in available)
            if w_avail_sum > 0:
                scores["chi2_combined"] = float(
                    sum(scores[k] * w[key_map[k]] / w_avail_sum
                        for k in available)
                )
 
        rec["fit_scores"] = scores
 
    return records
 
 
def log_fit_scores(records: list[dict], weights: dict):
    """
    Print a compact per-task χ² table to the logger, followed by
    ensemble summary statistics (mean ± σ, min, max) for each metric.
    """
    col_w = 13
 
    def _fmt(v: float | None) -> str:
        return f"{v:{col_w}.4f}" if v is not None else f"{'N/A':>{col_w}}"
 
    # ── header ────────────────────────────────────────────────────────────────
    header = (
        f"{'Task':<22}"
        f"{'χ²_PFNS':>{col_w}}"
        f"{'χ²_PFGS':>{col_w}}"
        f"{'χ²_ν̄ₙ':>{col_w}}"
        f"{'χ²_ν̄ᵧ':>{col_w}}"
        f"{'χ²_combined':>{col_w}}"
    )
    separator = "─" * len(header)
 
    # Weights annotation
    w_total = sum(weights.values()) or 1.0
    wn = {k: v / w_total for k, v in weights.items()}
    weight_note = (
        f"  Weights → PFNS:{wn['pfns']:.3f}  "
        f"PFGS:{wn['pfgs']:.3f}  "
        f"ν̄ₙ:{wn['nubar_n']:.3f}  "
        f"ν̄ᵧ:{wn['nubar_g']:.3f}  "
        f"(renormalised over available terms per run)"
    )
 
    log.info(separator)
    log.info("  χ² Fit Scores vs ENDF-8 Reference Data")
    log.info(weight_note)
    log.info(separator)
    log.info(header)
    log.info(separator)
 
    # ── per-task rows ─────────────────────────────────────────────────────────
    col_arrays: dict[str, list[float]] = {
        "chi2_pfns":     [],
        "chi2_pfgs":     [],
        "chi2_nubar_n":  [],
        "chi2_nubar_g":  [],
        "chi2_combined": [],
    }
 
    for rec in records:
        fs = rec.get("fit_scores", {})
        log.info(
            "%-22s %s %s %s %s %s",
            rec.get("_task_id", "?"),
            _fmt(fs.get("chi2_pfns")),
            _fmt(fs.get("chi2_pfgs")),
            _fmt(fs.get("chi2_nubar_n")),
            _fmt(fs.get("chi2_nubar_g")),
            _fmt(fs.get("chi2_combined")),
        )
        for key in col_arrays:
            v = fs.get(key)
            if v is not None:
                col_arrays[key].append(v)
 
    # ── ensemble summary ──────────────────────────────────────────────────────
    log.info(separator)
    log.info("  Ensemble summary  (over runs with non-null values)")
    log.info(separator)
 
    stat_labels = [
        ("Mean",   lambda a: np.mean(a)),
        ("Std",    lambda a: np.std(a)),
        ("Min",    lambda a: np.min(a)),
        ("Max",    lambda a: np.max(a)),
        ("Median", lambda a: np.median(a)),
    ]
 
    for stat_name, stat_fn in stat_labels:
        row_parts = [f"{stat_name:<22}"]
        for key in col_arrays:
            arr = col_arrays[key]
            row_parts.append(_fmt(stat_fn(arr) if arr else None))
        log.info("".join(row_parts))
 
    log.info(separator)
 
 
# ──────────────────────────────────────────────────────────────────────────────
# Figure 6 – χ² fit scores
# ──────────────────────────────────────────────────────────────────────────────
 
# Metadata for each individual χ² metric panel (top row of Fig 6).
_CHI2_PANEL_META = [
    ("chi2_pfns",    "χ²  PFNS",        "#1f77b4"),   # mpl blue
    ("chi2_pfgs",    "χ²  PFGS",        "#2ca02c"),   # mpl green
    ("chi2_nubar_n", "χ²  ν̄ₙ",          "#d62728"),   # mpl red
    ("chi2_nubar_g", "χ²  ν̄ᵧ",          "#9467bd"),   # mpl purple
]
_CHI2_COMBINED_COLOUR = "#e07b00"   # orange – matches PALETTE["evaluated"]
 
 
def plot_chi2_scores(records: list[dict],
                     default_record: dict | None,
                     output_dir: str,
                     dpi: int,
                     weights: dict[str, float]):
    """
    Figure 6 – χ² Fit Score Summary  (two-row layout)
 
    Top row  (4 panels): violin + strip for each individual reduced χ²
                         metric, consistent with the style of Fig 1.
                         A dashed horizontal line marks χ²=1 (ideal fit).
                         The default-run score is overlaid when available.
 
    Bottom row (2 panels):
      Left  – Ranked scatter of χ²_combined across all runs, coloured by
              value (viridis), so outliers are immediately identifiable.
              The best-fit run is annotated.  χ²=1 reference shown.
      Right – Pair-plot / component scatter: each individual χ² vs
              χ²_combined, coloured by metric, to reveal which observable
              drives the combined score.
 
    Skips gracefully if no fit_scores have been attached to records
    (i.e. no reference data was supplied).
    """
    # ── Guard: check that fit scores were actually computed ───────────────────
    if not any("fit_scores" in rec for rec in records):
        log.warning("No fit_scores found on records – skipping Fig 6")
        return
 
    # ── Collect arrays per metric ─────────────────────────────────────────────
    metric_values: dict[str, list[float]] = {k: [] for k, *_ in _CHI2_PANEL_META}
    metric_values["chi2_combined"] = []
    task_ids: list[str] = []
 
    for rec in records:
        fs = rec.get("fit_scores", {})
        task_ids.append(rec.get("_task_id", ""))
        for key in metric_values:
            v = fs.get(key)
            metric_values[key].append(v)   # keep None so indices align
 
    n_runs = len(records)
 
    # ── Check whether any metric has at least one non-None value ─────────────
    has_data = any(
        any(v is not None for v in vals)
        for vals in metric_values.values()
    )
    if not has_data:
        log.warning("All χ² scores are None – skipping Fig 6")
        return
 
    # ── Default-run scores (if available) ────────────────────────────────────
    default_scores: dict[str, float | None] = {}
    if default_record is not None:
        dfs = default_record.get("fit_scores", {})
        for key in metric_values:
            default_scores[key] = dfs.get(key)
 
    # ── Layout ───────────────────────────────────────────────────────────────
    fig = plt.figure(figsize=(20, 11))
    gs = gridspec.GridSpec(
        2, 4,
        figure=fig,
        hspace=0.42,
        wspace=0.32,
        height_ratios=[1.0, 1.1],
    )
 
    top_axes  = [fig.add_subplot(gs[0, i]) for i in range(4)]
    ax_rank   = fig.add_subplot(gs[1, :2])   # bottom-left: spans 2 cols
    ax_scatter = fig.add_subplot(gs[1, 2:])  # bottom-right: spans 2 cols
 
    fig.suptitle(
        f"χ² Fit Scores vs ENDF-8 Reference  ({n_runs} perturbed runs)",
        fontsize=14, y=1.01,
    )
 
    rng = np.random.default_rng(42)
 
    # ═══════════════════════════════════════════════════════════════════════════
    # TOP ROW – individual χ² metrics (violin + strip, mirrors Fig 1 style)
    # ═══════════════════════════════════════════════════════════════════════════
    for ax, (key, label, colour) in zip(top_axes, _CHI2_PANEL_META):
        vals_raw = metric_values[key]
        vals = np.array([v for v in vals_raw if v is not None], dtype=float)
 
        ax.set_title(label, pad=8)
        ax.set_xticks([])
        ax.set_xlim(-0.6, 0.6)
        ax.grid(axis="y", alpha=0.4)
 
        # Reference line χ²=1 (ideal reduced chi-squared)
        ax.axhline(1.0, color="grey", linewidth=1.0, linestyle=":",
                   zorder=1, label="χ²=1")
 
        if vals.size == 0:
            ax.text(0.5, 0.5, "N/A", transform=ax.transAxes,
                    ha="center", va="center", fontsize=10, color="grey")
            continue
 
        # Violin
        parts = ax.violinplot([vals], positions=[0], widths=0.7,
                              showmedians=False, showextrema=False)
        for pc in parts["bodies"]:
            pc.set_facecolor(colour)
            pc.set_alpha(0.45)
            pc.set_edgecolor(colour)
 
        # Percentile box
        p5, p25, p50, p75, p95 = np.percentile(vals, [5, 25, 50, 75, 95])
        ax.vlines(0, p25, p75, color=colour, linewidth=6, alpha=0.7, zorder=3)
        ax.vlines(0, p5,  p95, color=colour, linewidth=2, alpha=0.5, zorder=2)
        ax.scatter([0], [p50], color="white", s=40, zorder=5)
 
        # Strip
        jitter = rng.uniform(-0.18, 0.18, len(vals))
        ax.scatter(jitter, vals, color=colour, alpha=0.30, s=9, zorder=2)
 
        # Default reference
        handles = [Line2D([0], [0], color="grey", linewidth=1.0,
                          linestyle=":", label="χ²=1")]
        dv = default_scores.get(key)
        if dv is not None:
            ax.axhline(dv, color=PALETTE["default"], linewidth=1.8,
                       linestyle="--", zorder=6)
            handles.append(Line2D([0], [0], color=PALETTE["default"],
                                  linewidth=1.8, linestyle="--",
                                  label="Default"))
 
        ax.legend(handles=handles, loc="upper right", fontsize=7.5)
 
        # Summary stats annotation
        ax.text(0.5, -0.10,
                f"μ={vals.mean():.3f}  σ={vals.std():.3f}\n"
                f"min={vals.min():.3f}  max={vals.max():.3f}",
                transform=ax.transAxes,
                ha="center", va="top", fontsize=7.0)
 
    # ═══════════════════════════════════════════════════════════════════════════
    # BOTTOM LEFT – Ranked χ²_combined scatter
    # ═══════════════════════════════════════════════════════════════════════════
    combined_raw = metric_values["chi2_combined"]
    combined_vals = np.array(
        [v if v is not None else np.nan for v in combined_raw]
    )
 
    ax_rank.set_title("χ²_combined – Ranked Across Runs", pad=8)
    ax_rank.set_xlabel("Rank  (best → worst)", fontsize=9)
    ax_rank.set_ylabel("χ²_combined", fontsize=9)
    ax_rank.grid(True, alpha=0.3)
 
    valid_mask = ~np.isnan(combined_vals)
    if valid_mask.sum() > 0:
        valid_vals  = combined_vals[valid_mask]
        valid_ids   = [task_ids[i] for i in np.where(valid_mask)[0]]
 
        sort_order  = np.argsort(valid_vals)
        sorted_vals = valid_vals[sort_order]
        sorted_ids  = [valid_ids[i] for i in sort_order]
        ranks       = np.arange(1, len(sorted_vals) + 1)
 
        # Colour by value (viridis – low χ² = good = dark purple)
        sc = ax_rank.scatter(
            ranks, sorted_vals,
            c=sorted_vals, cmap="viridis_r",
            s=22, zorder=4, edgecolors="none",
        )
        fig.colorbar(sc, ax=ax_rank, label="χ²_combined", pad=0.01)
 
        # Connect with thin line for readability
        ax_rank.plot(ranks, sorted_vals,
                     color="grey", linewidth=0.6, alpha=0.5, zorder=3)
 
        # Annotate best run (rank 1)
        ax_rank.annotate(
            sorted_ids[0],
            xy=(1, sorted_vals[0]),
            xytext=(max(3, len(ranks) * 0.05), sorted_vals[0]),
            arrowprops=dict(arrowstyle="->", color="black", lw=0.8),
            fontsize=7.5, color="black",
        )
 
        # χ²=1 reference
        ax_rank.axhline(1.0, color="grey", linewidth=1.0, linestyle=":",
                        zorder=1, label="χ²=1")
 
        # Default reference
        dv_combined = default_scores.get("chi2_combined")
        if dv_combined is not None:
            ax_rank.axhline(dv_combined, color=PALETTE["default"],
                            linewidth=1.8, linestyle="--", zorder=5,
                            label=f"Default  ({dv_combined:.3f})")
 
        ax_rank.legend(loc="upper left", fontsize=8)
        ax_rank.set_xlim(0, len(ranks) + 1)
    else:
        ax_rank.text(0.5, 0.5, "N/A – no χ²_combined values",
                     transform=ax_rank.transAxes,
                     ha="center", va="center", fontsize=10, color="grey")
 
    # ═══════════════════════════════════════════════════════════════════════════
    # BOTTOM RIGHT – Effective component contributions vs χ²_combined
    # Shows weighted contribution of each observable to the combined score.
    # ═══════════════════════════════════════════════════════════════════════════
    ax_scatter.set_title("Effective χ² Contributions vs χ²_combined", pad=8)
    ax_scatter.set_xlabel("χ²_combined", fontsize=9)
    ax_scatter.set_ylabel("Effective contribution  (wᵢ χ²ᵢ)", fontsize=9)
    ax_scatter.grid(True, alpha=0.3)
    ax_scatter.set_xscale("log")
    ax_scatter.set_yscale("log")
 
    legend_handles_scatter = []
    any_scatter_data = False
 
    # Normalised weights (same logic as compute_fit_scores)
    w_total = sum(weights.values()) or 1.0
    w_norm = {k: v / w_total for k, v in weights.items()}
 
    key_map = {
        "chi2_pfns":    "pfns",
        "chi2_pfgs":    "pfgs",
        "chi2_nubar_n": "nubar_n",
        "chi2_nubar_g": "nubar_g",
    }
 
    for key, label, colour in _CHI2_PANEL_META:
        ind_raw = metric_values[key]
 
        x_vals, y_vals = [], []
 
        for combined_v, ind_v in zip(combined_raw, ind_raw):
            if combined_v is None or ind_v is None:
                continue
 
            # Determine which terms are available for THIS run
            available_keys = [
                k for k in key_map
                if metric_values[k][combined_raw.index(combined_v)] is not None
            ]
 
            if not available_keys:
                continue
 
            # Renormalise weights over available terms
            w_avail_sum = sum(w_norm[key_map[k]] for k in available_keys)
            if w_avail_sum <= 0:
                continue
 
            w_eff = w_norm[key_map[key]] / w_avail_sum
 
            # Effective contribution
            chi2_eff = ind_v * w_eff
 
            x_vals.append(combined_v)
            y_vals.append(chi2_eff)
 
        if not x_vals:
            continue
 
        ax_scatter.scatter(
            x_vals, y_vals,
            color=colour, alpha=0.5, s=16, zorder=3, edgecolors="none",
        )
 
        legend_handles_scatter.append(
            Line2D([0], [0], marker="o", color="w",
                   markerfacecolor=colour, markersize=7,
                   label=f"{label} (weighted)")
        )
 
        any_scatter_data = True
 
    # Reference: y = x (complete dominance by a single observable)
    if any_scatter_data:
        all_x = [v for v in combined_raw if v is not None]
        if all_x:
            lim_lo = 0.0
            lim_hi = max(all_x) * 1.05
 
            ax_scatter.plot(
                [lim_lo, lim_hi], [lim_lo, lim_hi],
                color="grey", linewidth=0.9, linestyle="--",
                zorder=1, label="dominance line (y = x)",
            )
 
            legend_handles_scatter.append(
                Line2D([0], [0], color="grey", linewidth=0.9,
                       linestyle="--", label="y = x (dominance)")
            )
 
        # Also show equal-share reference (optional but useful)
        # e.g. if 4 observables → y = x / 4
        n_terms = len(_CHI2_PANEL_META)
        if n_terms > 1:
            ax_scatter.plot(
                [lim_lo, lim_hi], [lim_lo, lim_hi / n_terms],
                color="grey", linewidth=0.8, linestyle=":",
                zorder=1,
            )
            legend_handles_scatter.append(
                Line2D([0], [0], color="grey", linewidth=0.8,
                       linestyle=":", label=f"equal share (y = x/{n_terms})")
            )
 
        ax_scatter.legend(handles=legend_handles_scatter,
                          loc="lower right", fontsize=8)
 
    else:
        ax_scatter.text(0.5, 0.5, "N/A – insufficient data",
                        transform=ax_scatter.transAxes,
                        ha="center", va="center", fontsize=10, color="grey")
    fig.tight_layout()
    _save(fig, os.path.join(output_dir, "06_chi2_fit_scores.png"), dpi)
 
 
# ──────────────────────────────────────────────────────────────────────────────
# Summary CSV
# ──────────────────────────────────────────────────────────────────────────────
 
# Full ordered list of CSV columns (χ² columns appended at end)
_CSV_FIELDNAMES = [
    "task_id",
    "n_events",
    "avg_gamma_multiplicity",
    "avg_neutron_multiplicity",
    "avg_single_gamma_energy_MeV",
    "avg_total_gamma_energy_MeV",
    "total_gammas",
    "total_neutrons",
    "chi2_pfns",
    "chi2_pfgs",
    "chi2_nubar_n",
    "chi2_nubar_g",
    "chi2_combined",
]
 
 
def _build_obs_row(task_id: str, rec: dict) -> dict:
    """Extract observable + fit-score fields from a record into a CSV row dict."""
    obs = rec.get("observables", {})
    fs  = rec.get("fit_scores",  {})
    return {
        "task_id":                      task_id,
        "n_events":                     rec.get("metadata", {}).get("n_events", ""),
        "avg_gamma_multiplicity":       obs.get("avg_gamma_multiplicity",       ""),
        "avg_neutron_multiplicity":     obs.get("avg_neutron_multiplicity",     ""),
        "avg_single_gamma_energy_MeV":  obs.get("avg_single_gamma_energy_MeV", ""),
        "avg_total_gamma_energy_MeV":   obs.get("avg_total_gamma_energy_MeV",  ""),
        "total_gammas":                 obs.get("total_gammas",                 ""),
        "total_neutrons":               obs.get("total_neutrons",               ""),
        # χ² columns – empty string when not computed (cleaner than NaN in CSV)
        "chi2_pfns":     "" if fs.get("chi2_pfns")     is None else fs["chi2_pfns"],
        "chi2_pfgs":     "" if fs.get("chi2_pfgs")     is None else fs["chi2_pfgs"],
        "chi2_nubar_n":  "" if fs.get("chi2_nubar_n")  is None else fs["chi2_nubar_n"],
        "chi2_nubar_g":  "" if fs.get("chi2_nubar_g")  is None else fs["chi2_nubar_g"],
        "chi2_combined": "" if fs.get("chi2_combined") is None else fs["chi2_combined"],
    }
 
 
def write_summary_csv(records: list[dict], default_record: dict | None,
                      output_dir: str):
    path = os.path.join(output_dir, "summary_statistics.csv")
    os.makedirs(output_dir, exist_ok=True)
 
    rows = [_build_obs_row(rec.get("_task_id", ""), rec) for rec in records]
 
    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=_CSV_FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
 
    # ── Default reference row ─────────────────────────────────────────────────
    if default_record is not None:
        default_row = _build_obs_row("DEFAULT", default_record)
        with open(path, "a", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=_CSV_FIELDNAMES)
            writer.writerow(default_row)
 
    # ── Evaluated scalar reference rows ──────────────────────────────────────
    eval_entries = []
    if "avg_neutron_multiplicity" in _EVAL_SCALARS:
        v, u = _EVAL_SCALARS["avg_neutron_multiplicity"]
        eval_entries.append({
            **{f: "" for f in _CSV_FIELDNAMES},
            "task_id": "EVALUATED_nubar_n",
            "avg_neutron_multiplicity": f"{v} ± {u}",
        })
    if "avg_gamma_multiplicity" in _EVAL_SCALARS:
        v, u = _EVAL_SCALARS["avg_gamma_multiplicity"]
        eval_entries.append({
            **{f: "" for f in _CSV_FIELDNAMES},
            "task_id": "EVALUATED_nubar_g",
            "avg_gamma_multiplicity": f"{v} ± {u}",
        })
 
    if eval_entries:
        with open(path, "a", newline="") as fh:
            writer = csv.DictWriter(fh, fieldnames=_CSV_FIELDNAMES)
            for row in eval_entries:
                writer.writerow(row)
 
    log.info("Saved → %s", path)
 
 
# ──────────────────────────────────────────────────────────────────────────────
# N_accepted filtering  (NEW)
# ──────────────────────────────────────────────────────────────────────────────
 
def select_accepted_records(records: list[dict], n_accepted: int) -> list[dict]:
    """
    Return the *n_accepted* records with the lowest chi2_combined value.
 
    Records that have no chi2_combined score (None) are placed after all
    scored records in the ranking and are only included in the accepted set
    if n_accepted exceeds the number of scored records.
 
    Parameters
    ----------
    records    : full list of task records (must have "fit_scores" attached)
    n_accepted : number of top-ranked records to keep
 
    Returns
    -------
    Sorted list of accepted records (best chi2_combined first).
    """
    if n_accepted <= 0:
        raise ValueError(f"--n_accepted must be a positive integer, got {n_accepted}")
 
    scored   = [(rec.get("fit_scores", {}).get("chi2_combined"), rec)
                for rec in records]
    with_val = sorted(
        [(v, r) for v, r in scored if v is not None],
        key=lambda x: x[0],
    )
    without  = [r for v, r in scored if v is None]
 
    ranked = [r for _, r in with_val] + without
    selected = ranked[:n_accepted]
 
    log.info(
        "N_accepted=%d: selected %d / %d records  "
        "(chi2_combined range: %s … %s)",
        n_accepted,
        len(selected),
        len(records),
        f"{with_val[0][0]:.4f}"  if with_val else "N/A",
        f"{with_val[min(n_accepted, len(with_val)) - 1][0]:.4f}"
        if with_val and len(with_val) >= 1 else "N/A",
    )
    return selected
 
 
def write_accepted_task_ids(accepted_records: list[dict],
                             output_dir: str,
                             n_accepted: int):
    """
    Write a plain-text file listing the accepted task IDs in rank order
    (best chi2_combined first), together with their chi2_combined values.
 
    File: <output_dir>/accepted_task_ids.txt
    """
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, "accepted_task_ids.txt")
 
    lines = [
        f"# Accepted tasks – top {n_accepted} by chi2_combined (ascending)",
        f"# Total accepted: {len(accepted_records)}",
        f"# Columns: rank  task_id  chi2_combined",
        "#",
    ]
    for rank, rec in enumerate(accepted_records, start=1):
        task_id = rec.get("_task_id", "unknown")
        chi2    = rec.get("fit_scores", {}).get("chi2_combined")
        chi2_str = f"{chi2:.6f}" if chi2 is not None else "N/A"
        lines.append(f"{rank:>4}  {task_id:<30}  {chi2_str}")
 
    with open(path, "w") as fh:
        fh.write("\n".join(lines) + "\n")
 
    log.info("Accepted task IDs written → %s", path)
 
 
def generate_accepted_figures(accepted_records: list[dict],
                               default_record: dict | None,
                               base_output_dir: str,
                               n_accepted: int,
                               dpi: int,
                               pfgs_data: dict | None,
                               pfns_data: dict | None,
                               max_gamma_energy_MeV: float | None,
                               max_neutron_energy_MeV: float | None):
    """
    Re-generate figures 1–5 for the accepted sub-set of records and write
    them into <base_output_dir>/accepted_<n_accepted>/.
 
    Also writes the accepted_task_ids.txt manifest into the same directory.
    """
    sub_dir = os.path.join(base_output_dir, f"accepted_{n_accepted}")
    os.makedirs(sub_dir, exist_ok=True)
 
    title_suffix = f"top {n_accepted} by χ²_combined"
 
    log.info("Generating accepted-subset figures in %s", sub_dir)
 
    # ── Task-ID manifest ──────────────────────────────────────────────────────
    write_accepted_task_ids(accepted_records, sub_dir, n_accepted)
 
    # ── Figure 1 – Scalar observables ────────────────────────────────────────
    log.info("[A1/5] Accepted scalar observables")
    plot_scalar_observables(accepted_records, default_record, sub_dir, dpi,
                            title_suffix=title_suffix)
 
    # ── Figure 2 – Gamma spectrum ─────────────────────────────────────────────
    log.info("[A2/5] Accepted gamma spectrum envelope")
    plot_gamma_spectrum(accepted_records, default_record, sub_dir, dpi,
                        emax=max_gamma_energy_MeV,
                        eval_data=pfgs_data,
                        title_suffix=title_suffix)
 
    # ── Figure 3 – Neutron spectrum ───────────────────────────────────────────
    log.info("[A3/5] Accepted neutron spectrum envelope")
    plot_neutron_spectrum(accepted_records, default_record, sub_dir, dpi,
                          emax=max_neutron_energy_MeV,
                          eval_data=pfns_data,
                          title_suffix=title_suffix)
 
    # ── Figure 4 – Gamma multiplicity ─────────────────────────────────────────
    log.info("[A4/5] Accepted gamma multiplicity distributions")
    plot_gamma_multiplicity(accepted_records, default_record, sub_dir, dpi,
                            title_suffix=title_suffix)
 
    # ── Figure 5 – Neutron multiplicity ──────────────────────────────────────
    log.info("[A5/5] Accepted neutron multiplicity distributions")
    plot_neutron_multiplicity(accepted_records, default_record, sub_dir, dpi,
                              title_suffix=title_suffix)
 
    log.info("Accepted-subset figures complete → %s", sub_dir)
 
 
# ──────────────────────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────────────────────
 
def parse_args():
    p = argparse.ArgumentParser(
        description="CGMF sensitivity sweep – ensemble visualisation + χ² fit scoring",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
 
    # ── Required / core ───────────────────────────────────────────────────────
    p.add_argument("--runs_dir",    required=True,
                   help="Directory containing task_* sub-directories")
    p.add_argument("--manifest",    default=None,
                   help="Path to manifest CSV (informational only)")
    p.add_argument("--default_dir", default=None,
                   help="Path to the unperturbed (default) CGMF task directory")
    p.add_argument("--output_dir",  default="sensitivity_results",
                   help="Directory to write figures and CSV")
 
    # ── Plotting options ──────────────────────────────────────────────────────
    p.add_argument("--vmax_pct",   type=float, default=85,
                   help="Percentile for colour scale cap (reserved)")
    p.add_argument("--fig_dpi",    type=int,   default=250,
                   help="Figure DPI")
    p.add_argument("--max_gamma_energy_MeV",   type=float, default=None,
                   help="Truncate gamma spectra at this energy (MeV)")
    p.add_argument("--max_neutron_energy_MeV", type=float, default=None,
                   help="Truncate neutron spectra at this energy (MeV)")
 
    # ── Evaluated spectral data ───────────────────────────────────────────────
    eval_group = p.add_argument_group(
        "Evaluated data (optional)",
        "Supply any combination to overlay evaluated/experimental references "
        "on the relevant figures and to enable χ² scoring."
    )
    eval_group.add_argument(
        "--pfns_file", default=None, metavar="FILE",
        help=(
            "Evaluated PFNS text file.  "
            "Columns: Energy(eV)  Chi(1/eV)  Unc(1/eV).  "
            "ENDF shorthand floats accepted.  "
            "Converted eV→MeV, scaled by nubar_U235=2.4355."
        ),
    )
    eval_group.add_argument(
        "--pfgs_file", default=None, metavar="FILE",
        help=(
            "Evaluated PFGS text file.  "
            "Columns: Energy(MeV)  Yield(γ/MeV/fission)  LowerBound  UpperBound.  "
            "Plotted as-is (no unit conversion)."
        ),
    )
    eval_group.add_argument(
        "--eval_nubar_n", type=float, default=None, metavar="VALUE",
        help="Evaluated average neutron multiplicity (nubar_n).",
    )
    eval_group.add_argument(
        "--eval_nubar_n_unc", type=float, default=0.0, metavar="UNC",
        help="1-sigma uncertainty on --eval_nubar_n.",
    )
    eval_group.add_argument(
        "--eval_nubar_g", type=float, default=None, metavar="VALUE",
        help="Evaluated average gamma multiplicity (nubar_g).",
    )
    eval_group.add_argument(
        "--eval_nubar_g_unc", type=float, default=0.0, metavar="UNC",
        help="1-sigma uncertainty on --eval_nubar_g.",
    )
 
    # ── χ² fit quality ────────────────────────────────────────────────────────
    fit_group = p.add_argument_group(
        "Fit quality – χ² vs ENDF-8",
        "Compute reduced χ² between each CGMF run and the ENDF-8 references.  "
        "Energy windows restrict which ENDF points enter the spectral χ².  "
        "Weights need not sum to 1; they are normalised internally."
    )
    fit_group.add_argument(
        "--pfns_emin", type=float, default=None, metavar="MeV",
        help="Lower energy bound (MeV) for PFNS χ² evaluation window.",
    )
    fit_group.add_argument(
        "--pfns_emax", type=float, default=None, metavar="MeV",
        help="Upper energy bound (MeV) for PFNS χ² evaluation window.",
    )
    fit_group.add_argument(
        "--pfgs_emin", type=float, default=None, metavar="MeV",
        help="Lower energy bound (MeV) for PFGS χ² evaluation window.",
    )
    fit_group.add_argument(
        "--pfgs_emax", type=float, default=None, metavar="MeV",
        help="Upper energy bound (MeV) for PFGS χ² evaluation window.",
    )
    fit_group.add_argument(
        "--w_pfns", type=float, default=0.25, metavar="W",
        help="Weight for PFNS contribution to combined χ².",
    )
    fit_group.add_argument(
        "--w_pfgs", type=float, default=0.25, metavar="W",
        help="Weight for PFGS contribution to combined χ².",
    )
    fit_group.add_argument(
        "--w_nubar_n", type=float, default=0.25, metavar="W",
        help="Weight for nubar_n contribution to combined χ².",
    )
    fit_group.add_argument(
        "--w_nubar_g", type=float, default=0.25, metavar="W",
        help="Weight for nubar_g contribution to combined χ².",
    )
 
    # ── Accepted-subset filtering (NEW) ───────────────────────────────────────
    p.add_argument(
        "--n_accepted", type=int, default=None, metavar="N",
        help=(
            "If specified, select the N runs with the lowest chi2_combined "
            "score and re-generate figures 1–5 for that accepted sub-set.  "
            "Requires at least one source of evaluated reference data so that "
            "chi2_combined values are available.  The accepted-subset figures "
            "and a task-ID manifest are written to "
            "<output_dir>/accepted_<N>/."
        ),
    )
 
    # ── Misc ──────────────────────────────────────────────────────────────────
    p.add_argument("--drop_all_zero_params", action="store_true",
                   help="(Reserved) Drop parameters with zero variance")
    p.add_argument("--debug", action="store_true",
                   help="Enable verbose per-task logging")
 
    return p.parse_args()
 
 
# ──────────────────────────────────────────────────────────────────────────────
# Entry point
# ──────────────────────────────────────────────────────────────────────────────
 
def main():
    args = parse_args()
 
    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
    )
 
    log.info("=" * 60)
    log.info("  CGMF Sensitivity Analysis – Phase I Post-Processor")
    log.info("=" * 60)
    log.info("  runs_dir   : %s", args.runs_dir)
    log.info("  default    : %s", args.default_dir or "not specified")
    log.info("  output_dir : %s", args.output_dir)
 
    # ── Populate evaluated scalar registry ───────────────────────────────────
    if args.eval_nubar_n is not None:
        _EVAL_SCALARS["avg_neutron_multiplicity"] = (
            args.eval_nubar_n, args.eval_nubar_n_unc
        )
        log.info("  eval nubar_n : %.4f ± %.4f",
                 args.eval_nubar_n, args.eval_nubar_n_unc)
 
    if args.eval_nubar_g is not None:
        _EVAL_SCALARS["avg_gamma_multiplicity"] = (
            args.eval_nubar_g, args.eval_nubar_g_unc
        )
        log.info("  eval nubar_g : %.4f ± %.4f",
                 args.eval_nubar_g, args.eval_nubar_g_unc)
 
    # ── Load optional evaluated spectral data ────────────────────────────────
    pfns_data = None
    if args.pfns_file:
        log.info("  PFNS file    : %s", args.pfns_file)
        pfns_data = load_pfns_file(args.pfns_file)
        if pfns_data is not None:
            log.info("  → %d PFNS data points loaded", len(pfns_data["energy_MeV"]))
 
    pfgs_data = None
    if args.pfgs_file:
        log.info("  PFGS file    : %s", args.pfgs_file)
        pfgs_data = load_pfgs_file(args.pfgs_file)
        if pfgs_data is not None:
            log.info("  → %d PFGS data points loaded", len(pfgs_data["energy_MeV"]))
 
    # ── χ² energy windows / weights summary ──────────────────────────────────
    weights = {
        "pfns":    args.w_pfns,
        "pfgs":    args.w_pfgs,
        "nubar_n": args.w_nubar_n,
        "nubar_g": args.w_nubar_g,
    }
    if pfns_data is not None or pfgs_data is not None:
        log.info("  χ² PFNS window  : [%s, %s] MeV",
                 args.pfns_emin or "−∞", args.pfns_emax or "+∞")
        log.info("  χ² PFGS window  : [%s, %s] MeV",
                 args.pfgs_emin or "−∞", args.pfgs_emax or "+∞")
        w_total = sum(weights.values()) or 1.0
        log.info("  χ² weights      : PFNS=%.3f  PFGS=%.3f  ν̄ₙ=%.3f  ν̄ᵧ=%.3f "
                 "(normalised)",
                 args.w_pfns / w_total, args.w_pfgs / w_total,
                 args.w_nubar_n / w_total, args.w_nubar_g / w_total)
 
    os.makedirs(args.output_dir, exist_ok=True)
 
    # ── Load perturbed ensemble ──────────────────────────────────────────────
    records = ingest_all_tasks(args.runs_dir, debug=args.debug)
    if not records:
        log.error("No task data loaded – aborting")
        sys.exit(1)
 
    # ── Load default reference ───────────────────────────────────────────────
    default_record = None
    if args.default_dir:
        default_record = load_task_json(args.default_dir)
        if default_record:
            log.info("Default reference loaded from %s", args.default_dir)
        else:
            log.warning("Could not load default reference from %s", args.default_dir)
 
    # ── Figures ──────────────────────────────────────────────────────────────
    log.info("Generating figures…")
 
    log.info("[1/5] Scalar observables")
    plot_scalar_observables(records, default_record, args.output_dir, args.fig_dpi)
 
    log.info("[2/5] Gamma spectrum envelope")
    plot_gamma_spectrum(records, default_record, args.output_dir, args.fig_dpi,
                        emax=args.max_gamma_energy_MeV,
                        eval_data=pfgs_data)
 
    log.info("[3/5] Neutron spectrum envelope")
    plot_neutron_spectrum(records, default_record, args.output_dir, args.fig_dpi,
                          emax=args.max_neutron_energy_MeV,
                          eval_data=pfns_data)
 
    log.info("[4/5] Gamma multiplicity distributions")
    plot_gamma_multiplicity(records, default_record, args.output_dir, args.fig_dpi)
 
    log.info("[5/5] Neutron multiplicity distributions")
    plot_neutron_multiplicity(records, default_record, args.output_dir, args.fig_dpi)
 
    # ── χ² fit quality scores ────────────────────────────────────────────────
    has_any_reference = any([
        pfns_data is not None,
        pfgs_data is not None,
        args.eval_nubar_n is not None,
        args.eval_nubar_g is not None,
    ])
 
    if has_any_reference:
        log.info("Computing χ² fit scores against ENDF-8 references…")
        records = compute_fit_scores(
            records,
            pfns_data=pfns_data,
            pfgs_data=pfgs_data,
            weights=weights,
            pfns_emin=args.pfns_emin,
            pfns_emax=args.pfns_emax,
            pfgs_emin=args.pfgs_emin,
            pfgs_emax=args.pfgs_emax,
        )
        log_fit_scores(records, weights)
 
        log.info("[6/6] χ² fit score summary")
        plot_chi2_scores(records, default_record, args.output_dir, args.fig_dpi, weights)
    else:
        log.info("No ENDF reference data supplied – skipping χ² computation")
 
    # ── Summary CSV ──────────────────────────────────────────────────────────
    log.info("Writing summary CSV")
    write_summary_csv(records, default_record, args.output_dir)
 
    # ── Accepted-subset figures (NEW) ─────────────────────────────────────────
    if args.n_accepted is not None:
        if not has_any_reference:
            log.error(
                "--n_accepted requires evaluated reference data (--pfns_file, "
                "--pfgs_file, --eval_nubar_n, or --eval_nubar_g) so that "
                "chi2_combined values can be computed.  Skipping accepted-subset "
                "figures."
            )
        elif not any(
            rec.get("fit_scores", {}).get("chi2_combined") is not None
            for rec in records
        ):
            log.error(
                "--n_accepted: no chi2_combined values were computed "
                "(check that reference data files are valid).  "
                "Skipping accepted-subset figures."
            )
        else:
            log.info("=" * 60)
            log.info("  Accepted-subset analysis  (n_accepted=%d)", args.n_accepted)
            log.info("=" * 60)
 
            accepted_records = select_accepted_records(records, args.n_accepted)
 
            generate_accepted_figures(
                accepted_records=accepted_records,
                default_record=default_record,
                base_output_dir=args.output_dir,
                n_accepted=args.n_accepted,
                dpi=args.fig_dpi,
                pfgs_data=pfgs_data,
                pfns_data=pfns_data,
                max_gamma_energy_MeV=args.max_gamma_energy_MeV,
                max_neutron_energy_MeV=args.max_neutron_energy_MeV,
            )
 
    log.info("=" * 60)
    log.info("  ALL DONE  –  outputs in %s", args.output_dir)
    log.info("=" * 60)
 
 
if __name__ == "__main__":
    main()
