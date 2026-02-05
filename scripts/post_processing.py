#!/usr/bin/env python3
"""
CGMF Fission Fragment De-Excitation Data Post-Processing Script

Extracts observables and exports machine/human-readable data for ENDF library generation

===============================================================================
USAGE EXAMPLES:
===============================================================================

Basic usage (default output prefix based on input filename):
    python process_cgmf.py histories.cgmf

Custom output prefix:
    python process_cgmf.py histories.cgmf --output u235_thermal_0p0253eV

Custom gamma spectrum binning (0-22 MeV, 440 bins = 0.05 MeV width):
    python process_cgmf.py histories.cgmf --g-bins 440 --g-emax 22.0

Custom neutron spectrum binning (0-20 MeV, 400 bins = 0.05 MeV width):
    python process_cgmf.py histories.cgmf --n-bins 400 --n-emax 20.0

Full customization:
    python process_cgmf.py histories.cgmf \
        --output u235_14MeV_realization_001 \
        --g-bins 220 --g-emax 22.0 \
        --n-bins 200 --n-emax 20.0

Disable plotting:
    python process_cgmf.py histories.cgmf --no-plot

===============================================================================
COMMAND-LINE ARGUMENTS:
===============================================================================

Positional Arguments:
    histories_file              Path to CGMF histories file
                                Default: "histories.cgmf"

Optional Arguments:
    -h, --help                  Show help message and exit
    
    -o PREFIX, --output PREFIX  Output file prefix for all generated files
                                Default: "{histories_filename}_processed"
                                
                                Generated files:
                                • {PREFIX}.json
                                • {PREFIX}.txt
                                • {PREFIX}_gamma_spectrum.csv
                                • {PREFIX}_neutron_spectrum.csv
                                • {PREFIX}_gamma_multiplicity.csv
                                • {PREFIX}_neutron_multiplicity.csv
                                • {PREFIX}_summary_plot.png

Gamma Spectrum Options:
    --g-bins INT                Number of bins for gamma spectrum
                                Default: 220 (results in 0.1 MeV bin width)
    
    --g-emax FLOAT              Maximum gamma energy in MeV
                                Default: 22.0 MeV
                                Note: Minimum energy is always 0.0 MeV

Neutron Spectrum Options:
    --n-bins INT                Number of bins for neutron spectrum
                                Default: 200 (results in 0.1 MeV bin width)
    
    --n-emax FLOAT              Maximum neutron energy in MeV
                                Default: 20.0 MeV
                                Note: Minimum energy is always 0.0 MeV

Plotting Options:
    --no-plot                   Disable generation of summary plot

===============================================================================
OUTPUT FILES:
===============================================================================

1. {PREFIX}.json
   Complete machine-readable data in JSON format containing:
   • Metadata (n_events, timestamp, source file)
   • Observables (multiplicities, average energies, particle counts)
   • Full gamma spectrum (0-22 MeV, including zero bins)
   • Full neutron spectrum (0-20 MeV)
   • Event-wise multiplicity distributions (total, light, heavy)
   • Bin edges, bin centers, and normalized yields

2. {PREFIX}.txt
   Human-readable summary report containing:
   • Analysis metadata
   • Key observables table
   • Multiplicity statistics
   • Spectrum binning information

3. {PREFIX}_gamma_spectrum.csv
   Two-column CSV: Energy (MeV), Yield (γ/MeV/fission)

4. {PREFIX}_neutron_spectrum.csv
   Two-column CSV: Energy (MeV), Yield (n/MeV/fission)

5. {PREFIX}_gamma_multiplicity.csv
   Four-column CSV: Multiplicity, P(Total), P(Light), P(Heavy)

6. {PREFIX}_neutron_multiplicity.csv
   Four-column CSV: Multiplicity, P(Total), P(Light), P(Heavy)

7. {PREFIX}_summary_plot.png
   2×2 panel figure:
   • Top-left: Gamma energy spectrum
   • Top-right: Neutron energy spectrum
   • Bottom-left: Gamma multiplicity distributions
   • Bottom-right: Neutron multiplicity distributions

===============================================================================
EXTRACTED OBSERVABLES:
===============================================================================

Particle Counts:
    • Total gammas emitted across all events
    • Total neutrons emitted across all events

Average Multiplicities:
    • ν̄_γ: Average gamma multiplicity per fission
    • ν̄_n: Average neutron multiplicity per fission

Average Energies:
    • ε̄_γ: Average energy per individual gamma (MeV)
    • ε̄_E_γ: Average total gamma energy release per fission (MeV/fission)

Energy Spectra:
    • Gamma: Normalized yield in gammas/(MeV·fission)
    • Neutron: Normalized yield in neutrons/(MeV·fission)

Multiplicity Distributions:
    • P(ν_γ): Probability distribution for gamma multiplicity
    • P(ν_n): Probability distribution for neutron multiplicity
    • Separate distributions for total, light fragment, heavy fragment

===============================================================================
"""

import sys
import os
import numpy as np
import json
from datetime import datetime
import argparse

# ========== USER CONFIGURATION ==========

DEFAULT_HISTORIES_FILE = "histories.cgmf"
DEFAULT_OUTPUT_PREFIX = "cgmf_output"

# Neutron spectrum binning
NEUTRON_EMIN = 0.0
NEUTRON_EMAX = 20.0
NEUTRON_BINS = 200

# Gamma spectrum binning
GAMMA_EMIN = 0.0
GAMMA_EMAX = 22.0  # Extended to 22 MeV as requested
GAMMA_BINS = 220   # 0.1 MeV bins

# ========== CGMFTK PATH CONFIGURATION ==========

CGMF_INSTALL_PATH = os.environ.get("CGMFPATH", "")
COMMON_PATHS = [
    CGMF_INSTALL_PATH,
    os.path.expanduser("~/CGMF"),
    os.path.expanduser("~/Documents/CGMF"),
    "/usr/local/CGMF",
]


def find_cgmftk():
    """Locate CGMFtk in common installation locations"""
    for path in COMMON_PATHS:
        if path and os.path.exists(path):
            cgmftk_path = os.path.join(path, "tools")
            if os.path.exists(os.path.join(cgmftk_path, "CGMFtk")):
                return cgmftk_path
    return None


cgmftk_location = find_cgmftk()
if cgmftk_location and cgmftk_location not in sys.path:
    sys.path.insert(0, cgmftk_location)

try:
    from CGMFtk import histories as fh
    CGMFTK_AVAILABLE = True
except ImportError as e:
    print("✗ ERROR: CGMFtk not found in Python path")
    print(f"  Import error: {e}")
    print("\nTo fix this:")
    print("  1. Set environment variable: export CGMFPATH=/path/to/CGMF")
    print("  2. Or install CGMFtk: cd $CGMFPATH/tools/CGMFtk && pip install -e .")
    CGMFTK_AVAILABLE = False
    sys.exit(1)

# ========== MATPLOTLIB CONFIGURATION ==========

try:
    import matplotlib
    matplotlib.use('Agg')  # Non-interactive backend
    import matplotlib.pyplot as plt
    MATPLOTLIB_AVAILABLE = True
except ImportError:
    print("⚠ WARNING: matplotlib not available - plotting disabled")
    MATPLOTLIB_AVAILABLE = False


class CGMFDataExtractor:
    """Extract and export CGMF fission data for ENDF library generation"""
    
    def __init__(self, histories_file):
        self.histories_file = histories_file
        self.histories = None
        self.n_events = 0
        
        # Observables
        self.avg_gamma_mult = 0.0
        self.avg_neutron_mult = 0.0
        self.avg_single_gamma_energy = 0.0
        self.avg_total_gamma_energy = 0.0
        self.total_gammas = 0
        self.total_neutrons = 0
        
        # Spectra
        self.neutron_spectrum = None
        self.gamma_spectrum = None
        self.neutron_bins = None
        self.gamma_bins = None
        
        # Multiplicity distributions
        self.gamma_mult_total = None
        self.gamma_mult_light = None
        self.gamma_mult_heavy = None
        self.neutron_mult_total = None
        self.neutron_mult_light = None
        self.neutron_mult_heavy = None
        
        # Multiplicity probability distributions
        self.gamma_mult_total_prob = None
        self.gamma_mult_light_prob = None
        self.gamma_mult_heavy_prob = None
        self.neutron_mult_total_prob = None
        self.neutron_mult_light_prob = None
        self.neutron_mult_heavy_prob = None
        
        # Multiplicity ranges (for binning)
        self.gamma_mult_range = None
        self.neutron_mult_range = None
    
    def load_histories(self):
        """Load CGMF histories file"""
        print(f"\n{'='*70}")
        print(f"Loading: {self.histories_file}")
        print(f"{'='*70}")
        
        if not os.path.exists(self.histories_file):
            raise FileNotFoundError(f"File not found: {self.histories_file}")
        
        self.histories = fh.Histories(self.histories_file)
        self.n_events = self.histories.getNumberEvents()
        
        print(f"✓ Loaded {self.n_events:,} fission events")
    
    def calculate_observables(self, n_bins_n=200, n_bins_g=220,
                            e_min_n=0.0, e_max_n=20.0,
                            e_min_g=0.0, e_max_g=22.0,
                            gamma_threshold=0.0):
        """Calculate all observables and spectra"""
        print(f"\n{'='*70}")
        print("Calculating Observables")
        print(f"{'='*70}")
        
        # Store threshold for metadata
        self.gamma_threshold = gamma_threshold
        
        # === Gamma Data ===
        # Get gamma energies - returns list per fragment [LF₀, HF₀, LF₁, HF₁, ...]
        gamma_energies_per_fragment = self.histories.getGammaElab()
        gamma_energies_lf = gamma_energies_per_fragment[::2]   # Light fragments
        gamma_energies_hf = gamma_energies_per_fragment[1::2]  # Heavy fragments
        
        all_gamma_energies = []
        total_energy_per_event = np.zeros(self.n_events)
        
        # Event-wise multiplicity arrays (threshold-aware from the start)
        gamma_mult_total_events = np.zeros(self.n_events, dtype=int)
        gamma_mult_light_events = np.zeros(self.n_events, dtype=int)
        gamma_mult_heavy_events = np.zeros(self.n_events, dtype=int)
        
        for i in range(self.n_events):
            lf_gammas = gamma_energies_lf[i]
            hf_gammas = gamma_energies_hf[i]
            
            # Apply threshold consistently to all gamma processing
            if gamma_threshold > 0:
                lf_gammas = [e for e in lf_gammas if e >= gamma_threshold]
                hf_gammas = [e for e in hf_gammas if e >= gamma_threshold]
            
            event_gammas = lf_gammas + hf_gammas
            
            # Store multiplicities (threshold already applied)
            gamma_mult_light_events[i] = len(lf_gammas)
            gamma_mult_heavy_events[i] = len(hf_gammas)
            gamma_mult_total_events[i] = len(event_gammas)
            
            total_energy_per_event[i] = sum(event_gammas)
            all_gamma_energies.extend(event_gammas)
        
        # All gamma observables now threshold-consistent
        self.total_gammas = len(all_gamma_energies)
        self.avg_gamma_mult = np.mean(gamma_mult_total_events)
        self.avg_single_gamma_energy = np.mean(all_gamma_energies) if self.total_gammas > 0 else 0.0
        self.avg_total_gamma_energy = np.mean(total_energy_per_event)
        
        # Calculate multiplicity distributions
        max_gamma_mult = np.max(gamma_mult_total_events)
        self.gamma_mult_range = np.arange(0, max_gamma_mult + 1)
        
        self.gamma_mult_total = np.bincount(gamma_mult_total_events, minlength=max_gamma_mult + 1)
        self.gamma_mult_light = np.bincount(gamma_mult_light_events, minlength=max_gamma_mult + 1)
        self.gamma_mult_heavy = np.bincount(gamma_mult_heavy_events, minlength=max_gamma_mult + 1)
        
        # Normalize to probabilities
        self.gamma_mult_total_prob = self.gamma_mult_total / self.n_events
        self.gamma_mult_light_prob = self.gamma_mult_light / self.n_events
        self.gamma_mult_heavy_prob = self.gamma_mult_heavy / self.n_events
        
        # === Neutron Data ===
        # Get neutron energies - returns list per fragment [LF₀, HF₀, LF₁, HF₁, ...]
        neutron_energies_per_fragment = self.histories.getNeutronElab()
        neutron_energies_lf = neutron_energies_per_fragment[::2]  # Light fragments
        neutron_energies_hf = neutron_energies_per_fragment[1::2] # Heavy fragments
        
        all_neutron_energies = []
        neutron_mult_total_events = np.zeros(self.n_events, dtype=int)
        neutron_mult_light_events = np.zeros(self.n_events, dtype=int)
        neutron_mult_heavy_events = np.zeros(self.n_events, dtype=int)
        
        for i in range(self.n_events):
            lf_neutrons = neutron_energies_lf[i]
            hf_neutrons = neutron_energies_hf[i]
            event_neutrons = lf_neutrons + hf_neutrons
            
            # Store multiplicities
            neutron_mult_light_events[i] = len(lf_neutrons)
            neutron_mult_heavy_events[i] = len(hf_neutrons)
            neutron_mult_total_events[i] = len(event_neutrons)
            
            all_neutron_energies.extend(event_neutrons)
        
        self.total_neutrons = len(all_neutron_energies)
        self.avg_neutron_mult = np.mean(neutron_mult_total_events)
        
        # Calculate neutron multiplicity distributions
        max_neutron_mult = np.max(neutron_mult_total_events)
        self.neutron_mult_range = np.arange(0, max_neutron_mult + 1)
        
        self.neutron_mult_total = np.bincount(neutron_mult_total_events, minlength=max_neutron_mult + 1)
        self.neutron_mult_light = np.bincount(neutron_mult_light_events, minlength=max_neutron_mult + 1)
        self.neutron_mult_heavy = np.bincount(neutron_mult_heavy_events, minlength=max_neutron_mult + 1)
        
        # Normalize to probabilities
        self.neutron_mult_total_prob = self.neutron_mult_total / self.n_events
        self.neutron_mult_light_prob = self.neutron_mult_light / self.n_events
        self.neutron_mult_heavy_prob = self.neutron_mult_heavy / self.n_events
        
        # === Gamma Spectrum (0-22 MeV, threshold-consistent) ===
        print(f"\nGamma Spectrum: {n_bins_g} bins from {e_min_g} to {e_max_g} MeV")
        if gamma_threshold > 0:
            print(f"  Gamma threshold: {gamma_threshold} MeV (applied to ALL gamma observables)")
        self.gamma_bins = np.linspace(e_min_g, e_max_g, n_bins_g + 1)
        bin_width_g = self.gamma_bins[1] - self.gamma_bins[0]
        gamma_hist, _ = np.histogram(all_gamma_energies, bins=self.gamma_bins)
        self.gamma_spectrum = gamma_hist / (self.n_events * bin_width_g)
        
        # === Neutron Spectrum (0-20 MeV) ===
        print(f"Neutron Spectrum: {n_bins_n} bins from {e_min_n} to {e_max_n} MeV")
        self.neutron_bins = np.linspace(e_min_n, e_max_n, n_bins_n + 1)
        bin_width_n = self.neutron_bins[1] - self.neutron_bins[0]
        neutron_hist, _ = np.histogram(all_neutron_energies, bins=self.neutron_bins)
        self.neutron_spectrum = neutron_hist / (self.n_events * bin_width_n)
        
        # === Print Summary ===
        print(f"\n{'─'*70}")
        print("RESULTS:")
        print(f"{'─'*70}")
        if gamma_threshold > 0:
            print(f"  ⚠ Gamma Threshold: {gamma_threshold} MeV (ALL observables)")
        print(f"  Total Gammas:                      {self.total_gammas:,}")
        print(f"  Total Neutrons:                    {self.total_neutrons:,}")
        print(f"  ν̄ (Gamma Multiplicity):            {self.avg_gamma_mult:.6f} γ/fission")
        print(f"  ν̄ (Neutron Multiplicity):          {self.avg_neutron_mult:.6f} n/fission")
        print(f"  ε̄ (Single Gamma Energy):           {self.avg_single_gamma_energy:.6f} MeV")
        print(f"  ε̄ (Total Gamma Energy):            {self.avg_total_gamma_energy:.6f} MeV/fission")
        print(f"\nMultiplicity Distribution Statistics:")
        print(f"  Gamma multiplicity range:          0 - {max_gamma_mult}")
        print(f"  Neutron multiplicity range:        0 - {max_neutron_mult}")
        print(f"  Most probable γ (total):           {np.argmax(self.gamma_mult_total_prob)}")
        print(f"  Most probable γ (light fragment):  {np.argmax(self.gamma_mult_light_prob)}")
        print(f"  Most probable γ (heavy fragment):  {np.argmax(self.gamma_mult_heavy_prob)}")
        print(f"  Most probable n (total):           {np.argmax(self.neutron_mult_total_prob)}")
        print(f"  Most probable n (light fragment):  {np.argmax(self.neutron_mult_light_prob)}")
        print(f"  Most probable n (heavy fragment):  {np.argmax(self.neutron_mult_heavy_prob)}")
        print(f"{'─'*70}")
    
    def export_json(self, filename):
        """Export machine-readable JSON file"""
        print(f"\n{'='*70}")
        print(f"Exporting JSON: {filename}")
        print(f"{'='*70}")
        
        # Prepare bin centers for spectra
        neutron_bin_centers = ((self.neutron_bins[:-1] + self.neutron_bins[1:]) / 2).tolist()
        gamma_bin_centers = ((self.gamma_bins[:-1] + self.gamma_bins[1:]) / 2).tolist()
        
        data = {
            "metadata": {
                "histories_file": os.path.basename(self.histories_file),
                "timestamp": datetime.now().isoformat(),
                "n_events": self.n_events,
                "cgmftk_version": "unknown"
            },
            
            "physics_definitions": {
                "gamma_threshold_MeV": self.gamma_threshold,
                "threshold_applies_to": [
                    "gamma_multiplicities",
                    "gamma_spectrum",
                    "avg_gamma_multiplicity",
                    "avg_single_gamma_energy",
                    "avg_total_gamma_energy"
                ] if self.gamma_threshold > 0 else [],
                "fragment_ordering": "Light fragment (LF), Heavy fragment (HF) per event",
                "spectrum_normalization": "per fission event",
                "energy_units": "MeV",
                "multiplicity_units": "particles per fission"
            },
            
            "observables": {
                "total_gammas": self.total_gammas,
                "total_neutrons": self.total_neutrons,
                "avg_gamma_multiplicity": self.avg_gamma_mult,
                "avg_neutron_multiplicity": self.avg_neutron_mult,
                "avg_single_gamma_energy_MeV": self.avg_single_gamma_energy,
                "avg_total_gamma_energy_MeV": self.avg_total_gamma_energy
            },
            
            "gamma_spectrum": {
                "description": "Prompt fission gamma energy distribution",
                "units": "gammas per MeV per fission",
                "threshold_applied": self.gamma_threshold > 0,
                "energy_range_MeV": [float(self.gamma_bins[0]), float(self.gamma_bins[-1])],
                "n_bins": len(self.gamma_bins) - 1,
                "bin_width_MeV": float(self.gamma_bins[1] - self.gamma_bins[0]),
                "bin_edges_MeV": self.gamma_bins.tolist(),
                "bin_centers_MeV": gamma_bin_centers,
                "spectrum": self.gamma_spectrum.tolist()
            },
            
            "neutron_spectrum": {
                "description": "Prompt fission neutron energy distribution",
                "units": "neutrons per MeV per fission",
                "energy_range_MeV": [float(self.neutron_bins[0]), float(self.neutron_bins[-1])],
                "n_bins": len(self.neutron_bins) - 1,
                "bin_width_MeV": float(self.neutron_bins[1] - self.neutron_bins[0]),
                "bin_edges_MeV": self.neutron_bins.tolist(),
                "bin_centers_MeV": neutron_bin_centers,
                "spectrum": self.neutron_spectrum.tolist()
            },
            
            "gamma_multiplicity_distributions": {
                "description": "Event-wise gamma multiplicity probability distributions",
                "units": "probability per fission event",
                "threshold_applied": self.gamma_threshold > 0,
                "multiplicity_range": self.gamma_mult_range.tolist(),
                "total": {
                    "counts": self.gamma_mult_total.tolist(),
                    "probabilities": self.gamma_mult_total_prob.tolist()
                },
                "light_fragment": {
                    "counts": self.gamma_mult_light.tolist(),
                    "probabilities": self.gamma_mult_light_prob.tolist()
                },
                "heavy_fragment": {
                    "counts": self.gamma_mult_heavy.tolist(),
                    "probabilities": self.gamma_mult_heavy_prob.tolist()
                }
            },
            
            "neutron_multiplicity_distributions": {
                "description": "Event-wise neutron multiplicity probability distributions",
                "units": "probability per fission event",
                "multiplicity_range": self.neutron_mult_range.tolist(),
                "total": {
                    "counts": self.neutron_mult_total.tolist(),
                    "probabilities": self.neutron_mult_total_prob.tolist()
                },
                "light_fragment": {
                    "counts": self.neutron_mult_light.tolist(),
                    "probabilities": self.neutron_mult_light_prob.tolist()
                },
                "heavy_fragment": {
                    "counts": self.neutron_mult_heavy.tolist(),
                    "probabilities": self.neutron_mult_heavy_prob.tolist()
                }
            }
        }
        
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        
        print(f"✓ JSON saved: {filename}")
    
    def export_text_report(self, filename):
        """Export human-readable text report"""
        print(f"\n{'='*70}")
        print(f"Exporting Text Report: {filename}")
        print(f"{'='*70}")
        
        with open(filename, 'w') as f:
            # Header
            f.write("="*70 + "\n")
            f.write("CGMF FISSION FRAGMENT DE-EXCITATION DATA ANALYSIS\n")
            f.write("="*70 + "\n\n")
            
            # Metadata
            f.write(f"Histories File: {os.path.basename(self.histories_file)}\n")
            f.write(f"Analysis Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Number of Events: {self.n_events:,}\n")
            
            f.write("\n" + "="*70 + "\n")
            f.write("OBSERVABLES\n")
            f.write("="*70 + "\n\n")
            
            # Statistics
            f.write("Particle Counts:\n")
            f.write(f"  Total Gammas:                      {self.total_gammas:,}\n")
            f.write(f"  Total Neutrons:                    {self.total_neutrons:,}\n\n")
            
            f.write("Average Multiplicities:\n")
            f.write(f"  ν̄ (Gamma):                         {self.avg_gamma_mult:.6f} γ/fission\n")
            f.write(f"  ν̄ (Neutron):                       {self.avg_neutron_mult:.6f} n/fission\n\n")
            
            f.write("Average Energies:\n")
            f.write(f"  ε̄ (Single Gamma):                  {self.avg_single_gamma_energy:.6f} MeV\n")
            f.write(f"  ε̄ (Total Gamma Release):          {self.avg_total_gamma_energy:.6f} MeV/fission\n\n")
            
            # Multiplicity distributions
            f.write("="*70 + "\n")
            f.write("MULTIPLICITY DISTRIBUTIONS\n")
            f.write("="*70 + "\n\n")
            
            f.write("Gamma Multiplicities:\n")
            f.write(f"  Range:                             0 - {len(self.gamma_mult_range)-1}\n")
            f.write(f"  Most probable (total):             {np.argmax(self.gamma_mult_total_prob)}\n")
            f.write(f"  Most probable (light fragment):    {np.argmax(self.gamma_mult_light_prob)}\n")
            f.write(f"  Most probable (heavy fragment):    {np.argmax(self.gamma_mult_heavy_prob)}\n\n")
            
            f.write("Neutron Multiplicities:\n")
            f.write(f"  Range:                             0 - {len(self.neutron_mult_range)-1}\n")
            f.write(f"  Most probable (total):             {np.argmax(self.neutron_mult_total_prob)}\n")
            f.write(f"  Most probable (light fragment):    {np.argmax(self.neutron_mult_light_prob)}\n")
            f.write(f"  Most probable (heavy fragment):    {np.argmax(self.neutron_mult_heavy_prob)}\n\n")
            
            # Spectra info
            f.write("="*70 + "\n")
            f.write("ENERGY SPECTRA\n")
            f.write("="*70 + "\n\n")
            
            f.write("Gamma Spectrum:\n")
            f.write(f"  Energy Range:                      {self.gamma_bins[0]:.2f} - {self.gamma_bins[-1]:.2f} MeV\n")
            f.write(f"  Number of Bins:                    {len(self.gamma_bins)-1}\n")
            f.write(f"  Bin Width:                         {self.gamma_bins[1]-self.gamma_bins[0]:.4f} MeV\n")
            f.write(f"  Units:                             gammas/(MeV·fission)\n\n")
            
            f.write("Neutron Spectrum:\n")
            f.write(f"  Energy Range:                      {self.neutron_bins[0]:.2f} - {self.neutron_bins[-1]:.2f} MeV\n")
            f.write(f"  Number of Bins:                    {len(self.neutron_bins)-1}\n")
            f.write(f"  Bin Width:                         {self.neutron_bins[1]-self.neutron_bins[0]:.4f} MeV\n")
            f.write(f"  Units:                             neutrons/(MeV·fission)\n\n")
            
            f.write("="*70 + "\n")
            f.write("NOTE: Full spectral and multiplicity data available in JSON/CSV files\n")
            f.write("="*70 + "\n")
        
        print(f"✓ Text report saved: {filename}")
    
    def export_spectrum_csv(self, gamma_file, neutron_file):
        """Export spectra as CSV for plotting/external analysis"""
        print(f"\n{'='*70}")
        print("Exporting CSV Spectra")
        print(f"{'='*70}")
        
        # Gamma CSV
        gamma_centers = (self.gamma_bins[:-1] + self.gamma_bins[1:]) / 2
        with open(gamma_file, 'w') as f:
            f.write("# Prompt Fission Gamma Spectrum\n")
            f.write("# Energy (MeV), Yield (gammas per MeV per fission)\n")
            for e, y in zip(gamma_centers, self.gamma_spectrum):
                f.write(f"{e:.6f},{y:.10e}\n")
        print(f"✓ Gamma spectrum CSV: {gamma_file}")
        
        # Neutron CSV
        neutron_centers = (self.neutron_bins[:-1] + self.neutron_bins[1:]) / 2
        with open(neutron_file, 'w') as f:
            f.write("# Prompt Fission Neutron Spectrum\n")
            f.write("# Energy (MeV), Yield (neutrons per MeV per fission)\n")
            for e, y in zip(neutron_centers, self.neutron_spectrum):
                f.write(f"{e:.6f},{y:.10e}\n")
        print(f"✓ Neutron spectrum CSV: {neutron_file}")
    
    def export_multiplicity_csv(self, gamma_file, neutron_file):
        """Export multiplicity distributions as CSV"""
        print(f"\n{'='*70}")
        print("Exporting CSV Multiplicity Distributions")
        print(f"{'='*70}")
        
        # Gamma multiplicity CSV
        with open(gamma_file, 'w') as f:
            f.write("# Prompt Fission Gamma Multiplicity Distributions\n")
            f.write(f"# Gamma detection threshold applied: {self.gamma_threshold:.3f} MeV\n")
            f.write("# Multiplicity, P(Total), P(Light Fragment), P(Heavy Fragment)\n")
            for nu, p_tot, p_lf, p_hf in zip(self.gamma_mult_range,
                                             self.gamma_mult_total_prob,
                                             self.gamma_mult_light_prob,
                                             self.gamma_mult_heavy_prob):
                f.write(f"{nu},{p_tot:.10e},{p_lf:.10e},{p_hf:.10e}\n")
        print(f"✓ Gamma multiplicity CSV: {gamma_file}")
        
        # Neutron multiplicity CSV
        with open(neutron_file, 'w') as f:
            f.write("# Prompt Fission Neutron Multiplicity Distributions\n")
            f.write("# Multiplicity, P(Total), P(Light Fragment), P(Heavy Fragment)\n")
            
            for nu, p_tot, p_lf, p_hf in zip(self.neutron_mult_range,
                    self.neutron_mult_total_prob,
                    self.neutron_mult_light_prob,
                    self.neutron_mult_heavy_prob):
                    
                f.write(f"{nu},{p_tot:.10e},{p_lf:.10e},{p_hf:.10e}\n")
            print(f"✓ Neutron multiplicity CSV: {neutron_file}")

    def generate_summary_plot(self, filename):
        """Generate 2x2 summary plot with spectra and multiplicities"""
        if not MATPLOTLIB_AVAILABLE:
            print(f"\n{'='*70}")
            print("⚠ Skipping plot generation (matplotlib not available)")
            print(f"{'='*70}")
            return
        
        print(f"\n{'='*70}")
        print(f"Generating Summary Plot: {filename}")
        print(f"{'='*70}")
        
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        
        # Top-left: Gamma energy spectrum
        ax = axes[0, 0]
        gamma_centers = (self.gamma_bins[:-1] + self.gamma_bins[1:]) / 2
        ax.plot(gamma_centers, self.gamma_spectrum, 'b-', linewidth=1.5)
        ax.set_xlabel('Energy (MeV)', fontsize=11)
        ax.set_ylabel('Yield (γ/MeV/fission)', fontsize=11)
        ax.set_yscale('log')  # <--- ADD THIS LINE
        ax.set_title('Prompt Fission Gamma Spectrum', fontsize=12, fontweight='bold')
        ax.grid(True, alpha=0.3)
        ax.set_xlim(self.gamma_bins[0], self.gamma_bins[-1])
        
        # Top-right: Neutron energy spectrum
        ax = axes[0, 1]
        neutron_centers = (self.neutron_bins[:-1] + self.neutron_bins[1:]) / 2
        ax.plot(neutron_centers, self.neutron_spectrum, 'r-', linewidth=1.5)
        ax.set_xlabel('Energy (MeV)', fontsize=11)
        ax.set_ylabel('Yield (n/MeV/fission)', fontsize=11)
        ax.set_yscale('log')  # <--- ADD THIS LINE
        ax.set_title('Prompt Fission Neutron Spectrum', fontsize=12, fontweight='bold')
        ax.grid(True, alpha=0.3)
        ax.set_xlim(self.neutron_bins[0], self.neutron_bins[-1])
        
        # Bottom-left: Gamma multiplicity distributions
        ax = axes[1, 0]
        ax.plot(self.gamma_mult_range, self.gamma_mult_total_prob, 'b-o',
                markersize=4, linewidth=1.5, label='Total')
        ax.plot(self.gamma_mult_range, self.gamma_mult_light_prob, 'g--s',
                markersize=3, linewidth=1.2, label='Light Fragment')
        ax.plot(self.gamma_mult_range, self.gamma_mult_heavy_prob, 'm--^',
                markersize=3, linewidth=1.2, label='Heavy Fragment')
        ax.set_xlabel('Multiplicity', fontsize=11)
        ax.set_ylabel('Probability', fontsize=11)
        ax.set_title('Gamma Multiplicity Distribution', fontsize=12, fontweight='bold')
        ax.legend(fontsize=9, loc='best')
        ax.grid(True, alpha=0.3)
        ax.set_xlim(0, min(30, len(self.gamma_mult_range)))  # Limit x-axis for clarity
        
        # Bottom-right: Neutron multiplicity distributions
        ax = axes[1, 1]
        ax.plot(self.neutron_mult_range, self.neutron_mult_total_prob, 'r-o',
                markersize=4, linewidth=1.5, label='Total')
        ax.plot(self.neutron_mult_range, self.neutron_mult_light_prob, 'c--s',
                markersize=3, linewidth=1.2, label='Light Fragment')
        ax.plot(self.neutron_mult_range, self.neutron_mult_heavy_prob, 'y--^',
                markersize=3, linewidth=1.2, label='Heavy Fragment')
        ax.set_xlabel('Multiplicity', fontsize=11)
        ax.set_ylabel('Probability', fontsize=11)
        ax.set_title('Neutron Multiplicity Distribution', fontsize=12, fontweight='bold')
        ax.legend(fontsize=9, loc='best')
        ax.grid(True, alpha=0.3)
        ax.set_xlim(0, min(15, len(self.neutron_mult_range)))  # Limit x-axis for clarity
        
        plt.tight_layout()
        plt.savefig(filename, dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"✓ Summary plot saved: {filename}")
    
def main():
    parser = argparse.ArgumentParser(
    description='Post-process CGMF histories for ENDF library generation',
    formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    parser.add_argument('histories_file', type=str, nargs='?',
                       default=DEFAULT_HISTORIES_FILE,
                       help='CGMF histories file')

    parser.add_argument('--output', '-o', type=str, default=None,
                       help='Output file prefix (default: cgmf_output)')

    # Binning options
    parser.add_argument('--n-bins', type=int, default=NEUTRON_BINS,
                       help='Neutron spectrum bins')
    parser.add_argument('--n-emax', type=float, default=NEUTRON_EMAX,
                       help='Neutron max energy (MeV)')
    parser.add_argument('--g-bins', type=int, default=GAMMA_BINS,
                       help='Gamma spectrum bins')
    parser.add_argument('--g-emax', type=float, default=GAMMA_EMAX,
                       help='Gamma max energy (MeV)')

    # Plotting options
    parser.add_argument('--no-plot', action='store_true',
                       help='Disable generation of summary plot')
                       
    parser.add_argument('--g-threshold', type=float, default=0.0,
                   help='Gamma detection threshold energy (MeV)')

    args = parser.parse_args()

    # Determine output prefix
    if args.output:
        output_prefix = args.output
    else:
        # Generate from input filename
        base = os.path.splitext(os.path.basename(args.histories_file))[0]
        output_prefix = f"{base}_processed"

    # Initialize extractor
    extractor = CGMFDataExtractor(args.histories_file)
    extractor.load_histories()

    # Calculate observables
    extractor.calculate_observables(
        n_bins_n=args.n_bins,
        n_bins_g=args.g_bins,
        e_max_n=args.n_emax,
        e_max_g=args.g_emax,
        gamma_threshold=args.g_threshold
    )

    # Export files
    extractor.export_json(f"{output_prefix}.json")
    extractor.export_text_report(f"{output_prefix}.txt")
    extractor.export_spectrum_csv(
        f"{output_prefix}_gamma_spectrum.csv",
        f"{output_prefix}_neutron_spectrum.csv"
    )
    extractor.export_multiplicity_csv(
        f"{output_prefix}_gamma_multiplicity.csv",
        f"{output_prefix}_neutron_multiplicity.csv"
    )

    # Generate plot unless disabled
    if not args.no_plot:
        extractor.generate_summary_plot(f"{output_prefix}_summary_plot.png")

    print(f"\n{'='*70}")
    print("✓ POST-PROCESSING COMPLETE")
    print(f"{'='*70}")
    print(f"\nOutput files:")
    print(f"  • {output_prefix}.json (machine-readable)")
    print(f"  • {output_prefix}.txt (human-readable report)")
    print(f"  • {output_prefix}_gamma_spectrum.csv")
    print(f"  • {output_prefix}_neutron_spectrum.csv")
    print(f"  • {output_prefix}_gamma_multiplicity.csv")
    print(f"  • {output_prefix}_neutron_multiplicity.csv")
    if not args.no_plot and MATPLOTLIB_AVAILABLE:
        print(f"  • {output_prefix}_summary_plot.png")
    print()

if __name__ == "__main__":
    if not CGMFTK_AVAILABLE:
        sys.exit(1)
    main()

