"""
=============================================================================
  DroneLogAnalyzer  —  ArduPilot .BIN Log GUI Analyzer
  Based on : https://github.com/ajaydcm/CSV_Conversion

  TABS:
    1. Overview      — verdict banner, stats, message types
    2. Vibration     — dedicated per-axis vibration analysis + IMU clipping
    3. Analysis      — full crash parameter table (all categories)
    4. Charts        — 9 interactive charts
    5. Report        — full text report
    6. Output Files  — manifest of all saved files
    7. Console Log   — live processing log

  OUTPUT FOLDER (auto-created):
    output/<log_name>/
      ├── csv/               one CSV per message type
      ├── crash_report.csv
      ├── crash_report.json
      ├── crash_report.txt
      ├── vibration_report.csv   ← NEW: full per-axis vibration table
      └── summary.json
=============================================================================
"""

import os, sys, csv, json, shutil, threading, warnings, traceback
from pathlib import Path
from datetime import datetime
from collections import defaultdict

warnings.filterwarnings("ignore")

import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext

import pandas as pd
import numpy as np

try:
    from pymavlink import mavutil
    PYMAVLINK_OK = True
except ImportError:
    PYMAVLINK_OK = False

try:
    import matplotlib
    matplotlib.use("TkAgg")
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
    from matplotlib.figure import Figure
    import matplotlib.gridspec as gridspec
    MATPLOTLIB_OK = True
except ImportError:
    MATPLOTLIB_OK = False

# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS
# ─────────────────────────────────────────────────────────────────────────────

APP_TITLE   = "DroneLogAnalyzer — ArduPilot Crash Inspector"
APP_VERSION = "3.0"
OUTPUT_ROOT = "output"

THRESHOLDS = {
    "roll_max_deg":        45.0,
    "pitch_max_deg":       45.0,
    "roll_rate_max_dps":   200.0,
    "pitch_rate_max_dps":  200.0,
    "vibe_max_ms2":        30.0,     # m/s²  — above this = high vibration
    "vibe_warn_ms2":       15.0,     # m/s²  — caution zone
    "vibe_clip_max":       100,      # total clip count — above = IMU saturated
    "vibe_clip_warn":      10,       # warning clip count
    "gps_hdop_max":        2.0,
    "gps_nsats_min":       6,
    "voltage_min_v":       10.5,
    "current_max_a":       80.0,
    "voltage_drop_rate":   0.5,
    "alt_drop_m":          5.0,
    "ekf_variance_max":    1.0,
    "rc_lost_pct":         5.0,
    "motor_imbalance_pct": 30.0,
    "motor_min_us":        1050,
    "motor_min_frames":    20,
}

IMPORTANT_MSG_TYPES = {
    "ATT","CTUN","GPS","IMU","RCIN","RCOU","BARO","CURR",
    "MODE","ERR","EV","MSG","PM","VIBE","XKF1","XKF4",
    "NKF1","NKF4","POWR","AHR2","MAG","BAT","MOTB","TERR",
}
SKIP_MSG_TYPES = {"BAD_DATA","FMT","FMTU","UNIT","MULT","PARM"}

SEVERITY_RANK  = {"[CRITICAL]":0,"[WARNING]":1,"[INFO]":2,"[OK]":3}
SEVERITY_COLOR = {"[CRITICAL]":"#FF4444","[WARNING]":"#FF9900",
                  "[INFO]":"#3399FF","[OK]":"#44BB44"}
CRASH_KEYWORDS = ["crash","flip","loss of control","altitude loss","freefall",
                  "motor failure","ekf","voltage collapse","rc signal loss",
                  "cutout","failsafe","nose-dive"]

BG_DARK    = "#1E1E2E"
BG_MID     = "#2A2A3E"
BG_LIGHT   = "#313145"
FG_WHITE   = "#EEEEF5"
FG_GREY    = "#AAAACC"
ACCENT_BLUE= "#5B8DEF"
ACCENT_TEAL= "#3EC9A7"
VIBE_X_CLR = "#5B8DEF"
VIBE_Y_CLR = "#3EC9A7"
VIBE_Z_CLR = "#FF9900"
VIBE_THRESH= "#FF4444"
VIBE_WARN  = "#FFDD44"


# ─────────────────────────────────────────────────────────────────────────────
# OUTPUT MANAGER
# ─────────────────────────────────────────────────────────────────────────────

class OutputManager:
    def __init__(self, root: str, log_stem: str):
        self.root    = Path(root)
        self.base    = self.root / log_stem
        self.csv_dir = self.base / "csv"
        self._manifest: list[dict] = []
        self.base.mkdir(parents=True, exist_ok=True)
        self.csv_dir.mkdir(parents=True, exist_ok=True)

    def _record(self, path: Path, description: str):
        size_kb = path.stat().st_size / 1024
        self._manifest.append({
            "file": path.name, "folder": str(path.parent.resolve()),
            "full_path": str(path.resolve()), "size_kb": round(size_kb, 2),
            "description": description,
            "saved_at": datetime.now().strftime("%H:%M:%S"),
        })

    def save_raw_csv(self, msg_type: str, df: pd.DataFrame) -> Path:
        path = self.csv_dir / f"{msg_type}.csv"
        df.to_csv(path, index=False)
        self._record(path, f"Telemetry — {msg_type} ({len(df):,} rows)")
        return path

    def save_crash_report_csv(self, events: list) -> Path:
        path = self.base / "crash_report.csv"
        fields = ["category","parameter","status","max_value",
                  "threshold","anomaly_count","interpretation"]
        with open(path, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=fields)
            w.writeheader(); w.writerows(events)
        self._record(path, f"Crash analysis — {len(events)} checks")
        return path

    def save_vibration_report_csv(self, vib_rows: list) -> Path:
        path = self.base / "vibration_report.csv"
        if not vib_rows:
            return path
        fields = list(vib_rows[0].keys())
        with open(path, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=fields)
            w.writeheader(); w.writerows(vib_rows)
        self._record(path, f"Vibration per-axis report — {len(vib_rows)} rows")
        return path

    def save_crash_report_json(self, payload: dict) -> Path:
        path = self.base / "crash_report.json"
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
        self._record(path, "Full JSON report")
        return path

    def save_crash_report_txt(self, text: str) -> Path:
        path = self.base / "crash_report.txt"
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(text)
        self._record(path, "Human-readable text report")
        return path

    def save_summary_json(self, summary: dict) -> Path:
        path = self.base / "summary.json"
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(summary, fh, indent=2)
        self._record(path, "Quick-look summary")
        return path

    def get_manifest(self) -> list:
        return self._manifest


# ─────────────────────────────────────────────────────────────────────────────
# VIBRATION ANALYSIS ENGINE  (comprehensive)
# ─────────────────────────────────────────────────────────────────────────────

def compute_vibration_stats(df: pd.DataFrame) -> dict:
    """
    Comprehensive per-axis vibration statistics from VIBE DataFrame.

    Computes for each of VibeX / VibeY / VibeZ:
      • Basic stats  : mean, max, min, std, RMS, peak-to-peak
      • Percentiles  : P50 (median), P90, P95, P99
      • Threshold    : % frames above warning / critical, frames_over count
      • Rolling RMS  : 50-sample rolling RMS for trend analysis
      • FFT          : dominant frequency and its amplitude (if scipy available)
      • Health score : 0–100 composite score (100 = perfect, 0 = catastrophic)
      • Timeline     : list of (sample_index, value) spikes above critical
      • Diagnosis    : human-readable root-cause hint

    Also computes per Clip0/Clip1/Clip2 and overall combined metrics.
    """
    T   = THRESHOLDS
    out = {"has_data": False, "axes": {}, "clips": {}, "overall": {}}

    axis_cols = {
        "VibeX": safe_col(df, "VibeX", "vibex", "vibe_x"),
        "VibeY": safe_col(df, "VibeY", "vibey", "vibe_y"),
        "VibeZ": safe_col(df, "VibeZ", "vibez", "vibe_z"),
    }
    clip_cols = {
        "Clip0": safe_col(df, "Clip0", "clip0"),
        "Clip1": safe_col(df, "Clip1", "clip1"),
        "Clip2": safe_col(df, "Clip2", "clip2"),
    }

    # Try to import scipy for FFT — graceful fallback if not installed
    try:
        from scipy.fft import rfft, rfftfreq
        SCIPY_OK = True
    except ImportError:
        SCIPY_OK = False

    any_axis = False
    for axis, col in axis_cols.items():
        if col is None:
            continue
        any_axis = True
        series = df[col].dropna().reset_index(drop=True)
        n      = len(series)
        arr    = series.values.astype(float)

        # ── Basic statistics ──────────────────────────────────────────────
        mean_v = float(arr.mean())
        max_v  = float(arr.max())
        min_v  = float(arr.min())
        std_v  = float(arr.std())
        rms_v  = float(np.sqrt((arr**2).mean()))

        # ── Percentiles ───────────────────────────────────────────────────
        p50 = float(np.percentile(arr, 50))
        p90 = float(np.percentile(arr, 90))
        p95 = float(np.percentile(arr, 95))
        p99 = float(np.percentile(arr, 99))

        # ── Threshold counts ──────────────────────────────────────────────
        pct_warn     = float((arr > T["vibe_warn_ms2"]).sum() / n * 100) if n else 0
        pct_critical = float((arr > T["vibe_max_ms2"]).sum()  / n * 100) if n else 0
        frames_over  = int((arr > T["vibe_max_ms2"]).sum())
        frames_warn  = int((arr > T["vibe_warn_ms2"]).sum())

        # ── Peak-to-peak (rolling 50-sample window) ───────────────────────
        win = min(50, n)
        rolling_range = series.rolling(window=win).apply(
            lambda x: x.max() - x.min(), raw=True)
        peak_to_peak_max = float(rolling_range.max()) if not rolling_range.empty else 0.0

        # ── Rolling RMS (50-sample) ───────────────────────────────────────
        rolling_rms = series.rolling(window=win).apply(
            lambda x: float(np.sqrt((x**2).mean())), raw=True)
        rolling_rms_max = float(rolling_rms.max()) if not rolling_rms.empty else rms_v

        # ── FFT — dominant frequency ──────────────────────────────────────
        fft_dom_freq   = None
        fft_dom_amp    = None
        fft_freqs      = None   # first 40 freq bins for plotting
        fft_amplitudes = None
        if SCIPY_OK and n >= 64:
            try:
                # Assumed sample rate ~400 Hz for ArduPilot VIBE logs
                sample_rate = 400.0
                yf  = np.abs(rfft(arr - arr.mean()))
                xf  = rfftfreq(n, d=1.0 / sample_rate)
                # Ignore DC (index 0)
                peak_idx       = int(np.argmax(yf[1:]) + 1)
                fft_dom_freq   = round(float(xf[peak_idx]), 2)
                fft_dom_amp    = round(float(yf[peak_idx]), 4)
                # Store first 80 bins for plotting
                plot_n         = min(80, len(xf))
                fft_freqs      = [round(float(f), 2) for f in xf[:plot_n]]
                fft_amplitudes = [round(float(a), 4) for a in yf[:plot_n]]
            except Exception:
                pass

        # ── Spike timeline (sample indices where value > critical threshold) ─
        spike_indices = list(np.where(arr > T["vibe_max_ms2"])[0][:20].astype(int))
        spike_values  = [round(float(arr[i]), 3) for i in spike_indices]
        spikes = list(zip(spike_indices, spike_values))

        # ── Consecutive bad frames (longest run above critical) ───────────
        above    = arr > T["vibe_max_ms2"]
        max_run  = 0
        cur_run  = 0
        for v in above:
            cur_run = cur_run + 1 if v else 0
            max_run = max(max_run, cur_run)

        # ── Health score 0–100 ────────────────────────────────────────────
        # Deduct points for: RMS, P99, % over threshold, max spikes
        score = 100.0
        score -= min(40, (rms_v / T["vibe_max_ms2"]) * 40)
        score -= min(20, (p99  / T["vibe_max_ms2"]) * 20)
        score -= min(25, pct_critical * 5)
        score -= min(15, (max_run / max(n, 1)) * 1500)
        health_score = max(0, round(score))
        if   health_score >= 80: health_label = "GOOD"
        elif health_score >= 60: health_label = "FAIR"
        elif health_score >= 40: health_label = "POOR"
        else:                    health_label = "CRITICAL"

        # ── Status ────────────────────────────────────────────────────────
        if max_v > T["vibe_max_ms2"] or pct_critical > 5:
            status = "[CRITICAL]"
        elif max_v > T["vibe_warn_ms2"] or pct_warn > 10:
            status = "[WARNING]"
        else:
            status = "[OK]"

        # ── Diagnosis ─────────────────────────────────────────────────────
        if status == "[CRITICAL]":
            if axis == "VibeZ" and max_v > T["vibe_max_ms2"] * 2:
                diagnosis = "Extreme Z-axis vibration — frame resonance / prop strike / crash impact"
            elif max_run > 50:
                diagnosis = "Sustained high vibration — persistent motor / ESC fault or prop imbalance"
            else:
                diagnosis = "Severe vibration — motor failure / loose prop / frame damage"
        elif status == "[WARNING]":
            if std_v > mean_v * 0.5:
                diagnosis = "Irregular vibration spikes — intermittent fault or loose hardware"
            else:
                diagnosis = "Elevated vibration — check motor balance and prop condition"
        else:
            diagnosis = "Normal vibration levels — no action required"

        out["axes"][axis] = {
            "col":              col,
            "samples":          n,
            # Basic
            "mean":             round(mean_v, 3),
            "max":              round(max_v,  3),
            "min":              round(min_v,  3),
            "std":              round(std_v,  3),
            "rms":              round(rms_v,  3),
            "peak_to_peak":     round(peak_to_peak_max, 3),
            # Percentiles
            "p50":              round(p50, 3),
            "p90":              round(p90, 3),
            "p95":              round(p95, 3),
            "p99":              round(p99, 3),
            # Threshold
            "pct_above_warn":   round(pct_warn,     2),
            "pct_above_crit":   round(pct_critical, 2),
            "frames_over":      frames_over,
            "frames_warn":      frames_warn,
            # Rolling
            "rolling_rms_max":  round(rolling_rms_max, 3),
            "rolling_rms":      rolling_rms,      # Series — for plotting
            "max_consecutive":  max_run,
            # FFT
            "fft_dom_freq":     fft_dom_freq,
            "fft_dom_amp":      fft_dom_amp,
            "fft_freqs":        fft_freqs,
            "fft_amplitudes":   fft_amplitudes,
            # Spikes
            "spikes":           spikes,
            # Health
            "health_score":     health_score,
            "health_label":     health_label,
            # Status / diagnosis
            "status":           status,
            "diagnosis":        diagnosis,
        }

    # ── Clip channels ─────────────────────────────────────────────────────
    for clip, col in clip_cols.items():
        if col is None:
            continue
        series = df[col].dropna()
        total  = int(series.max()) if len(series) > 0 else 0
        rate   = float(series.diff().clip(lower=0).mean()) if len(series) > 1 else 0.0
        # Clip increment per sample (how fast clipping is accumulating)
        increments = series.diff().clip(lower=0)
        burst_max  = int(increments.max()) if len(increments) > 0 else 0
        if total > T["vibe_clip_max"]:
            clip_status = "[CRITICAL]"
        elif total > T["vibe_clip_warn"]:
            clip_status = "[WARNING]"
        else:
            clip_status = "[OK]"
        out["clips"][clip] = {
            "total":     total,
            "rate":      round(rate, 6),
            "burst_max": burst_max,
            "status":    clip_status,
            "series":    series,    # for plotting cumulative clip chart
        }

    # ── Overall combined metrics ──────────────────────────────────────────
    rms_vals = []
    for axis, col in axis_cols.items():
        if col and col in df.columns:
            rms_vals.append(float(np.sqrt((df[col].dropna()**2).mean())))
    combined_rms  = float(np.sqrt(sum(v**2 for v in rms_vals))) if rms_vals else 0.0
    total_clips   = sum(v["total"] for v in out["clips"].values())
    avg_health    = int(np.mean([s["health_score"] for s in out["axes"].values()])) \
                    if out["axes"] else 0

    overall_stat = "[OK]"
    if combined_rms > T["vibe_max_ms2"] or total_clips > T["vibe_clip_max"]:
        overall_stat = "[CRITICAL]"
    elif combined_rms > T["vibe_warn_ms2"] or total_clips > T["vibe_clip_warn"]:
        overall_stat = "[WARNING]"

    # Worst single axis
    worst_axis = ""
    worst_rms  = 0.0
    for axis, s in out["axes"].items():
        if s["rms"] > worst_rms:
            worst_rms  = s["rms"]
            worst_axis = axis

    out["overall"] = {
        "combined_rms":   round(combined_rms, 3),
        "total_clips":    total_clips,
        "status":         overall_stat,
        "samples":        len(df),
        "duration_est":   f"~{len(df) / 400:.0f}s" if len(df) > 0 else "N/A",
        "avg_health":     avg_health,
        "worst_axis":     worst_axis,
        "worst_rms":      round(worst_rms, 3),
    }
    out["has_data"] = any_axis
    return out


def vibration_stats_to_events(stats: dict) -> list:
    """Convert vibration stats dict to standard event list for analysis tab."""
    events = []
    T = THRESHOLDS
    for axis, s in stats.get("axes", {}).items():
        # Max / RMS
        events.append(mk("Vibration", f"{axis} — Max / RMS",
            s["status"],
            f"max={s['max']:.3f} m/s²  RMS={s['rms']:.3f}  health={s['health_score']}/100 ({s['health_label']})",
            f"< {T['vibe_max_ms2']} m/s²",
            s["frames_over"], s["diagnosis"]))
        # Basic stats
        events.append(mk("Vibration", f"{axis} — Mean / Std / Pk-Pk",
            "[INFO]",
            f"mean={s['mean']:.3f}  std={s['std']:.3f}  pk-pk={s['peak_to_peak']:.3f}  min={s['min']:.3f}",
            "informational", 0, f"{axis} basic statistics"))
        # Percentiles
        events.append(mk("Vibration", f"{axis} — Percentiles",
            "[INFO]",
            f"P50={s['p50']:.3f}  P90={s['p90']:.3f}  P95={s['p95']:.3f}  P99={s['p99']:.3f}",
            "P99 < threshold ideal", 0,
            "P99 near/above threshold indicates persistent vibration" if s["p99"] > T["vibe_warn_ms2"] else "Percentiles nominal"))
        # Threshold breach
        events.append(mk("Vibration", f"{axis} — Threshold Breach",
            s["status"] if s["pct_above_crit"] > 0 else ("[WARNING]" if s["pct_above_warn"] > 0 else "[OK]"),
            f"{s['pct_above_crit']:.1f}% frames > {T['vibe_max_ms2']}  |  {s['pct_above_warn']:.1f}% > {T['vibe_warn_ms2']}  |  max_run={s['max_consecutive']} frames",
            f"< 5% frames over {T['vibe_max_ms2']} m/s²",
            s["frames_over"], s["diagnosis"]))
        # FFT
        if s.get("fft_dom_freq") is not None:
            events.append(mk("Vibration", f"{axis} — Dominant Frequency",
                "[INFO]",
                f"{s['fft_dom_freq']} Hz  (amplitude={s['fft_dom_amp']:.4f})",
                "informational", 0,
                "High-frequency dominance may indicate motor resonance" if s["fft_dom_freq"] > 80 else "Frequency within normal range"))
    # Clips
    for clip, c in stats.get("clips", {}).items():
        events.append(mk("Vibration", f"IMU Clipping — {clip}",
            c["status"],
            f"total={c['total']}  rate={c['rate']:.6f}/sample  burst_max={c['burst_max']}",
            f"< {T['vibe_clip_max']} total  warn>{T['vibe_clip_warn']}",
            c["total"],
            "IMU fully saturated — attitude data is INVALID during clipping events" if c["total"] > T["vibe_clip_max"]
            else "Elevated clipping — check dampening" if c["total"] > T["vibe_clip_warn"]
            else "Normal"))
    # Overall
    ov = stats.get("overall", {})
    if ov:
        events.append(mk("Vibration", "Overall — Combined RMS + Health",
            ov["status"],
            f"combined_rms={ov['combined_rms']:.3f} m/s²  clips={ov['total_clips']}  health={ov['avg_health']}/100  worst={ov['worst_axis']}",
            f"< {T['vibe_max_ms2']} m/s²",
            ov["total_clips"], "Overall vibration health across all axes"))
    return events


def build_vibration_csv_rows(stats: dict, log_name: str) -> list:
    """Build rows for vibration_report.csv — full per-axis data including percentiles, FFT, health."""
    rows = []
    ts = datetime.now().isoformat()
    for axis, s in stats.get("axes", {}).items():
        rows.append({
            "log_file":              log_name,
            "generated":             ts,
            "axis":                  axis,
            "samples":               s["samples"],
            # Basic
            "mean_ms2":              s["mean"],
            "max_ms2":               s["max"],
            "min_ms2":               s["min"],
            "std_ms2":               s["std"],
            "rms_ms2":               s["rms"],
            "rolling_rms_max_ms2":   s["rolling_rms_max"],
            "peak_to_peak_ms2":      s["peak_to_peak"],
            # Percentiles
            "p50_ms2":               s["p50"],
            "p90_ms2":               s["p90"],
            "p95_ms2":               s["p95"],
            "p99_ms2":               s["p99"],
            # Threshold
            "pct_above_warn":        s["pct_above_warn"],
            "pct_above_critical":    s["pct_above_crit"],
            "frames_over_threshold": s["frames_over"],
            "frames_over_warn":      s["frames_warn"],
            "max_consecutive_frames":s["max_consecutive"],
            "warn_threshold_ms2":    THRESHOLDS["vibe_warn_ms2"],
            "crit_threshold_ms2":    THRESHOLDS["vibe_max_ms2"],
            # FFT
            "fft_dominant_freq_hz":  s["fft_dom_freq"] if s["fft_dom_freq"] else "N/A",
            "fft_dominant_amplitude":s["fft_dom_amp"]  if s["fft_dom_amp"]  else "N/A",
            # Health
            "health_score_0_100":    s["health_score"],
            "health_label":          s["health_label"],
            # Status
            "status":                s["status"],
            "diagnosis":             s["diagnosis"],
        })
    for clip, c in stats.get("clips", {}).items():
        rows.append({
            "log_file":              log_name,
            "generated":             ts,
            "axis":                  clip,
            "samples":               stats["overall"].get("samples", 0),
            "mean_ms2":              "N/A",
            "max_ms2":               c["total"],
            "min_ms2":               "N/A",
            "std_ms2":               "N/A",
            "rms_ms2":               "N/A",
            "rolling_rms_max_ms2":   "N/A",
            "peak_to_peak_ms2":      "N/A",
            "p50_ms2":               "N/A",
            "p90_ms2":               "N/A",
            "p95_ms2":               "N/A",
            "p99_ms2":               "N/A",
            "pct_above_warn":        "N/A",
            "pct_above_critical":    "N/A",
            "frames_over_threshold": c["total"],
            "frames_over_warn":      "N/A",
            "max_consecutive_frames":"N/A",
            "warn_threshold_ms2":    THRESHOLDS["vibe_clip_warn"],
            "crit_threshold_ms2":    THRESHOLDS["vibe_clip_max"],
            "fft_dominant_freq_hz":  "N/A",
            "fft_dominant_amplitude":"N/A",
            "health_score_0_100":    "N/A",
            "health_label":          "N/A",
            "status":                c["status"],
            "diagnosis":             f"IMU clip count: {c['total']}  burst_max={c['burst_max']}",
        })
    return rows


# ─────────────────────────────────────────────────────────────────────────────
# GENERAL ANALYSIS ENGINE
# ─────────────────────────────────────────────────────────────────────────────

def load_csv_file(csv_files: dict, msg_type: str):
    path = csv_files.get(msg_type)
    if not path or not Path(path).exists():
        return None
    try:
        return pd.read_csv(path)
    except Exception:
        return None

def safe_col(df, *candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def mk(category, parameter, status, max_value, threshold, anomaly_count, interpretation):
    return dict(category=category, parameter=parameter, status=status,
                max_value=max_value, threshold=threshold,
                anomaly_count=anomaly_count, interpretation=interpretation)

def sev(condition: bool, critical: bool = False) -> str:
    return "[OK]" if not condition else ("[CRITICAL]" if critical else "[WARNING]")


def analyze_attitude(csv_files):
    events = []
    df = load_csv_file(csv_files, "ATT")
    if df is None: return events
    T = THRESHOLDS
    rc = safe_col(df,"Roll","roll"); pc = safe_col(df,"Pitch","pitch")
    if rc:
        mx = df[rc].abs().max(); over = int((df[rc].abs()>T["roll_max_deg"]).sum())
        events.append(mk("Attitude","Roll Angle",sev(over>0,over>50),
            f"{mx:.1f}°",f"±{T['roll_max_deg']}°",over,
            "Severe roll — flip / loss of control" if over>0 else "Normal"))
        rr = df[rc].diff().abs(); orr = int((rr>T["roll_rate_max_dps"]).sum())
        events.append(mk("Attitude","Roll Angular Rate",sev(orr>0),
            f"{rr.max():.1f} °/sample",f"<{T['roll_rate_max_dps']} °/s",orr,
            "Sudden attitude flip" if orr>0 else "Normal"))
    if pc:
        mx = df[pc].abs().max(); over = int((df[pc].abs()>T["pitch_max_deg"]).sum())
        events.append(mk("Attitude","Pitch Angle",sev(over>0,over>50),
            f"{mx:.1f}°",f"±{T['pitch_max_deg']}°",over,
            "Severe pitch — nose-dive or stall" if over>0 else "Normal"))
    return events


def analyze_vibration(csv_files):
    """Main vibration analyzer — uses full stats engine."""
    df = load_csv_file(csv_files, "VIBE")
    if df is None:
        return []
    stats = compute_vibration_stats(df)
    return vibration_stats_to_events(stats)


def analyze_gps(csv_files):
    events = []
    df = load_csv_file(csv_files,"GPS")
    if df is None: return events
    T = THRESHOLDS
    hdop  = safe_col(df,"HDop","hdop","HDOP")
    nsats = safe_col(df,"NSats","nsats","NumSats","Num_Sats")
    stat  = safe_col(df,"Status","status","GpsStatus")
    if hdop:
        bad = int((df[hdop]>T["gps_hdop_max"]).sum())
        events.append(mk("GPS","GPS HDOP",sev(bad>10),
            f"max={df[hdop].max():.2f}",f"<{T['gps_hdop_max']}",bad,
            "Poor GPS accuracy" if bad>10 else "Normal"))
    if nsats:
        low = int((df[nsats]<T["gps_nsats_min"]).sum())
        events.append(mk("GPS","GPS Satellite Count",sev(low>0,low>50),
            f"min={int(df[nsats].min())} sats",f">={T['gps_nsats_min']} sats",low,
            "Loss of GPS lock" if low>0 else "Normal"))
    if stat:
        nf = int((df[stat]<3).sum())
        events.append(mk("GPS","GPS 3D Fix Status",sev(nf>0,nf>20),
            f"{nf} frames without 3D fix","Fix≥3D",nf,
            "GPS lost 3D fix" if nf>0 else "Normal"))
    return events


def analyze_battery(csv_files):
    events = []
    df = load_csv_file(csv_files,"BAT") or load_csv_file(csv_files,"CURR")
    if df is None: return events
    T = THRESHOLDS
    vc = safe_col(df,"Volt","volt","VoltR","voltage","Vcc")
    cc = safe_col(df,"Curr","curr","current","Current")
    if vc:
        mn = df[vc].min(); lv = int((df[vc]<T["voltage_min_v"]).sum())
        events.append(mk("Battery","Battery Voltage",sev(lv>0,lv>50),
            f"min={mn:.2f} V",f">={T['voltage_min_v']} V",lv,
            "Low-voltage failsafe" if lv>0 else "Normal"))
        if len(df)>10:
            drops=df[vc].diff(); worst=drops.min()
            ndrop=int((drops<-T["voltage_drop_rate"]).sum())
            events.append(mk("Battery","Voltage Drop Rate",
                sev(worst<-T["voltage_drop_rate"],True),
                f"worst={worst:.3f} V/sample",f">-{T['voltage_drop_rate']} V/sample",ndrop,
                "Sudden voltage collapse" if worst<-T["voltage_drop_rate"] else "Normal"))
    if cc:
        mx=df[cc].max(); oc=int((df[cc]>T["current_max_a"]).sum())
        events.append(mk("Battery","Battery Current",sev(oc>0),
            f"max={mx:.1f} A",f"<={T['current_max_a']} A",oc,
            "Over-current" if oc>0 else "Normal"))
    return events


def analyze_baro(csv_files):
    events = []
    df = load_csv_file(csv_files,"BARO")
    if df is None: return events
    T = THRESHOLDS
    ac = safe_col(df,"Alt","alt","Altitude","altitude")
    if ac is None: return events
    diffs=df[ac].diff(); drops=int((diffs<-T["alt_drop_m"]).sum())
    events.append(mk("Barometer","Altitude Sudden Drop",sev(drops>0,drops>5),
        f"max_alt={df[ac].max():.1f} m  worst={diffs.min():.2f} m/sample",
        f"drop<{T['alt_drop_m']} m/sample",drops,
        "Rapid altitude loss — crash / motor failure" if drops>0 else "Normal"))
    return events


def analyze_ekf(csv_files):
    events = []; T = THRESHOLDS
    for msg in ("XKF4","NKF4"):
        df = load_csv_file(csv_files,msg)
        if df is None: continue
        for cn in ("SV","SP","SH","SM","SVT"):
            col = safe_col(df,cn)
            if col is None: continue
            mv=df[col].max(); over=int((df[col]>T["ekf_variance_max"]).sum())
            events.append(mk("EKF",f"EKF Variance {cn} ({msg})",
                sev(over>0,over>100),f"max={mv:.4f}",f"<={T['ekf_variance_max']}",over,
                "EKF diverging — attitude unreliable" if over>0 else "Normal"))
        break
    return events


def analyze_rc(csv_files):
    events = []
    df = load_csv_file(csv_files,"RCIN")
    if df is None: return events
    T = THRESHOLDS
    tc = safe_col(df,"C3","c3","CH3","Throttle","throttle")
    if tc is None: return events
    lost=int((df[tc]<900).sum()); pct=lost/len(df)*100 if len(df)>0 else 0
    events.append(mk("RC Signal","Throttle RC Input",sev(pct>T["rc_lost_pct"],True),
        f"{pct:.1f}% frames < 900 µs ({lost} frames)",
        f"<{T['rc_lost_pct']}% frames lost",lost,
        "RC signal loss / failsafe" if pct>T["rc_lost_pct"] else "Normal"))
    return events


def analyze_motors(csv_files):
    events = []
    df = load_csv_file(csv_files,"RCOU")
    if df is None: return events
    T = THRESHOLDS; motors={}
    for i in range(1,9):
        col = safe_col(df,f"C{i}",f"c{i}",f"Chan{i}",f"M{i}")
        if col: motors[f"Motor{i}"]=df[col]
        if len(motors)==4: break
    if len(motors)<2: return events
    means={k:float(v.mean()) for k,v in motors.items()}
    hi,lo=max(means.values()),min(means.values())
    imbal=(hi-lo)/hi*100 if hi>0 else 0
    events.append(mk("Motors","Motor Output Imbalance",
        sev(imbal>T["motor_imbalance_pct"],True),
        f"{imbal:.1f}%  ({lo:.0f}–{hi:.0f} µs)",f"<{T['motor_imbalance_pct']}%",
        int(imbal>T["motor_imbalance_pct"]),
        "Motor imbalance — ESC failure suspected" if imbal>T["motor_imbalance_pct"] else "Normal"))
    for name,series in motors.items():
        lc=int((series<T["motor_min_us"]).sum())
        if lc>T["motor_min_frames"]:
            events.append(mk("Motors",f"{name} Near Cutout","[CRITICAL]",
                f"{lc} frames < {T['motor_min_us']} µs",
                f"<{T['motor_min_frames']} such frames",lc,
                f"{name} near minimum — possible ESC cutout"))
    return events


def analyze_errors(csv_files):
    SUBSYS = {
        2: ("Radio/RC","[CRITICAL]","Radio failsafe — RC signal lost"),
        6: ("Battery","[CRITICAL]","Battery failsafe — low voltage"),
        7: ("GPS","[CRITICAL]","GPS failsafe — signal lost"),
        12:("Crash Check","[CRITICAL]","CRASH DETECTED by autopilot"),
        13:("Flip","[CRITICAL]","Flip detected — uncontrolled rotation"),
        16:("EKF/DCM","[CRITICAL]","EKF check FAILED — attitude unreliable"),
        17:("Barometer","[CRITICAL]","Barometer failure — altitude hold lost"),
        3: ("Compass","[WARNING]","Compass inconsistency — mag interference"),
        8: ("GCS","[WARNING]","Ground-station link lost"),
        18:("CPU","[WARNING]","CPU overloaded"),
    }
    events = []
    df = load_csv_file(csv_files,"ERR")
    if df is None: return events
    sc=safe_col(df,"Subsys","subsys","SubSystem","subsystem")
    ec=safe_col(df,"ECode","ecode","ErrorCode","error_code")
    if sc is None: return events
    for _,row in df.iterrows():
        sid=int(row.get(sc,0)); ecode=int(row.get(ec,0)) if ec else 0
        if ecode==0: continue
        if sid in SUBSYS:
            sname,status,interp=SUBSYS[sid]
        else:
            sname,status,interp=(f"Subsystem_{sid}","[WARNING]",
                                  f"Error ECode={ecode} subsystem {sid}")
        events.append(mk("Error Codes",f"ERR — {sname}",status,
            f"ECode={ecode}","ECode=0",1,interp))
    return events


def analyze_events(csv_files):
    EV_MAP={
        10:("Armed","[INFO]","Vehicle armed"),
        11:("Disarmed","[INFO]","Vehicle disarmed"),
        15:("Auto Armed","[INFO]","Auto-arm successful"),
        16:("Takeoff","[INFO]","Takeoff initiated"),
        18:("Land Complete","[INFO]","Landing complete"),
        25:("CRASH","[CRITICAL]","CRASH EVENT LOGGED by autopilot!"),
        28:("Land Maybe","[INFO]","Possible landing detected"),
    }
    events=[]
    df=load_csv_file(csv_files,"EV")
    if df is None: return events
    ic=safe_col(df,"Id","id","event_id","EventId")
    if ic is None: return events
    for _,row in df.iterrows():
        eid=int(row.get(ic,0))
        if eid not in EV_MAP: continue
        name,status,interp=EV_MAP[eid]
        events.append(mk("Events",f"Flight Event — {name}",status,
            f"EventID={eid}","N/A",1 if eid==25 else 0,interp))
    return events


def run_all_analyzers(csv_files: dict) -> list:
    all_events = []
    for fn in (analyze_attitude,analyze_vibration,analyze_gps,
               analyze_battery,analyze_baro,analyze_ekf,
               analyze_rc,analyze_motors,analyze_errors,analyze_events):
        try: all_events.extend(fn(csv_files))
        except Exception as exc: print(f"  [WARN] {fn.__name__}: {exc}")
    all_events.sort(key=lambda e: SEVERITY_RANK.get(e["status"],4))
    return all_events


def determine_verdict(events: list):
    criticals=[e for e in events if e["status"]=="[CRITICAL]"]
    warnings =[e for e in events if e["status"]=="[WARNING]"]
    evidence =[e for e in criticals if any(
        kw in e["interpretation"].lower() for kw in CRASH_KEYWORDS)]
    if evidence:           verdict="CRASH LIKELY — Critical anomalies matching crash patterns"
    elif len(criticals)>=2:verdict="PROBABLE INCIDENT — Multiple critical parameters exceeded"
    elif criticals:        verdict="POSSIBLE INCIDENT — Single critical anomaly found"
    elif warnings:         verdict="CAUTION — Warnings detected, review recommended"
    else:                  verdict="NO CRASH INDICATORS — Flight appears nominal"
    return verdict, evidence


def build_text_report(events, log_name, verdict, evidence, vib_stats=None):
    ts=datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    SEP="="*78; DASH="-"*78
    nc=sum(1 for e in events if e["status"]=="[CRITICAL]")
    nw=sum(1 for e in events if e["status"]=="[WARNING]")
    no=sum(1 for e in events if e["status"]=="[OK]")
    lines=[SEP,"  ARDUPILOT CRASH ANALYSIS REPORT",
        f"  Log File   : {log_name}",f"  Generated  : {ts}",
        f"  Checks: {len(events)}  |  CRITICAL: {nc}  |  WARNING: {nw}  |  OK: {no}",
        SEP,"",f"  VERDICT  :  {verdict}",""]
    if evidence:
        lines+=[DASH,"  KEY CRASH EVIDENCE",DASH]
        for e in evidence:
            lines+=[f"  ⚠  [{e['category']}]  {e['parameter']}",
                    f"     {e['interpretation']}",
                    f"     Value: {e['max_value']}   Threshold: {e['threshold']}",""]
    # Vibration section
    if vib_stats and vib_stats.get("has_data"):
        lines+=[DASH,"  VIBRATION ANALYSIS — ALL AXES",DASH]
        ov=vib_stats["overall"]
        lines.append(f"  Overall Status    : {ov['status']}")
        lines.append(f"  Combined RMS      : {ov['combined_rms']:.3f} m/s²")
        lines.append(f"  Total IMU Clips   : {ov['total_clips']}")
        lines.append(f"  Duration (est.)   : {ov['duration_est']}")
        lines.append(f"  Samples           : {ov['samples']:,}")
        lines.append("")
        lines.append(f"  {'Axis':<8} {'Mean':>8} {'Max':>8} {'Min':>8} {'RMS':>8} {'Std':>8} {'Pk-Pk':>8} {'%>Warn':>8} {'%>Crit':>8} {'Status'}")
        lines.append(f"  {'-'*8} {'-'*8} {'-'*8} {'-'*8} {'-'*8} {'-'*8} {'-'*8} {'-'*8} {'-'*8} {'-'*10}")
        for axis,s in vib_stats["axes"].items():
            lines.append(
                f"  {axis:<8} {s['mean']:>8.3f} {s['max']:>8.3f} {s['min']:>8.3f} "
                f"{s['rms']:>8.3f} {s['std']:>8.3f} {s['peak_to_peak']:>8.3f} "
                f"{s['pct_above_warn']:>8.1f}% {s['pct_above_crit']:>8.1f}% {s['status']}")
        lines.append("")
        lines.append(f"  {'Clip Chan':<10} {'Total Clips':>12} {'Rate/sample':>14} {'Status'}")
        lines.append(f"  {'-'*10} {'-'*12} {'-'*14} {'-'*10}")
        for clip,c in vib_stats["clips"].items():
            lines.append(f"  {clip:<10} {c['total']:>12} {c['rate']:>14.4f} {c['status']}")
        lines.append("")
        for axis,s in vib_stats["axes"].items():
            lines.append(f"  {axis} Diagnosis: {s['diagnosis']}")
        lines.append("")
    # Full parameter table
    lines+=[DASH,"  DETAILED PARAMETER ANALYSIS  (OK rows suppressed)",DASH,
        f"  {'CATEGORY':<14} {'PARAMETER':<32} {'STATUS':<12} DETAILS",
        f"  {'-'*14} {'-'*32} {'-'*12} {'-'*40}"]
    prev_cat=None
    for e in events:
        if e["status"]=="[OK]": continue
        if e["category"]!=prev_cat:
            lines.append(f"\n  ── {e['category']}")
            prev_cat=e["category"]
        lines.append(
            f"  {e['category']:<14} {e['parameter']:<32} {e['status']:<12} "
            f"{e['max_value'][:30]}  →  {e['interpretation'][:55]}")
    lines+=["",SEP,"","  THRESHOLD REFERENCE",DASH,
        f"  {'Parameter':<38}  {'Threshold':<22}  Crash Relevance",
        f"  {'-'*38}  {'-'*22}  {'-'*30}"]
    for p,t,r in [
        ("Roll / Pitch angle","±45°","Flip / loss of control"),
        ("Vibration — Warning zone",f">{THRESHOLDS['vibe_warn_ms2']} m/s²","Caution — check balance"),
        ("Vibration — Critical zone",f">{THRESHOLDS['vibe_max_ms2']} m/s²","Motor damage / resonance"),
        ("IMU clip count (warning)",f">{THRESHOLDS['vibe_clip_warn']} clips","Elevated clipping"),
        ("IMU clip count (critical)",f">{THRESHOLDS['vibe_clip_max']} clips","Accelerometer saturation"),
        ("GPS HDOP","<2.0","Poor positional accuracy"),
        ("GPS satellites",">=6","Risk of GPS lock loss"),
        ("Battery voltage",">=10.5 V","Low-voltage failsafe"),
        ("Altitude drop","<5 m/sample","Freefall / crash"),
        ("EKF variance","<=1.0","Attitude estimate invalid"),
        ("RC throttle signal","<5% frames lost","RC failsafe"),
        ("Motor PWM imbalance","<30%","Motor / ESC failure"),
    ]:
        lines.append(f"  {p:<38}  {t:<22}  {r}")
    lines+=["",SEP,""]
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# BIN → CSV
# ─────────────────────────────────────────────────────────────────────────────

def bin_to_csv(bin_path: str, output_mgr: OutputManager,
               progress_cb=None, log_cb=None) -> dict:
    def log(msg):
        if log_cb: log_cb(msg)
        print(msg)
    bin_path = Path(bin_path)
    log(f"Opening: {bin_path.name}")
    if not PYMAVLINK_OK:
        log("[ERROR] pymavlink not installed"); return {}
    mlog = mavutil.mavlink_connection(str(bin_path), dialect="ardupilotmega")
    data: dict[str, list] = defaultdict(list)
    total_read = 0
    log("Reading messages…")
    while True:
        try:
            msg = mlog.recv_match(blocking=False)
            if msg is None: break
            mtype = msg.get_type()
            if mtype in SKIP_MSG_TYPES: continue
            row = msg.to_dict(); row.pop("mavpackettype", None)
            data[mtype].append(row); total_read += 1
            if total_read % 5000 == 0:
                log(f"  Read {total_read:,} messages…")
                if progress_cb: progress_cb(min(50, total_read // 1000))
        except Exception: continue
    log(f"Total: {total_read:,}  Types: {len(data)}")
    if progress_cb: progress_cb(55)
    csv_files: dict[str, str] = {}
    types_sorted = sorted(data.keys()); n = len(types_sorted)
    for idx, mtype in enumerate(types_sorted):
        rows = data[mtype]
        if not rows: continue
        df = pd.DataFrame(rows)
        path = output_mgr.save_raw_csv(mtype, df)
        csv_files[mtype] = str(path)
        if progress_cb: progress_cb(55 + int(40*idx/max(n,1)))
    log(f"Saved {len(csv_files)} CSV files → {output_mgr.csv_dir}")
    if progress_cb: progress_cb(95)
    return csv_files


# ─────────────────────────────────────────────────────────────────────────────
# MAIN GUI
# ─────────────────────────────────────────────────────────────────────────────

class DroneLogAnalyzer:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title(f"{APP_TITLE}  v{APP_VERSION}")
        self.root.geometry("1300x820")
        self.root.minsize(1050, 680)
        self.root.configure(bg=BG_DARK)

        self.bin_path_var   = tk.StringVar(value="No file selected")
        self.output_dir_var = tk.StringVar(value=str(Path(OUTPUT_ROOT).resolve()))
        self.status_var     = tk.StringVar(value="Ready — select a .bin log file to begin")
        self.progress_var   = tk.DoubleVar(value=0)

        self.csv_files:   dict = {}
        self.events:      list = []
        self.verdict:     str  = ""
        self.evidence:    list = []
        self.vib_stats:   dict = {}
        self.output_mgr        = None
        self._worker_thread    = None

        self._build_ui()
        self.root.mainloop()

    # ── UI ───────────────────────────────────────────────────────────────────

    def _build_ui(self):
        self._build_header()
        self._build_toolbar()
        self._build_progress()
        self._build_notebook()
        self._build_statusbar()

    def _build_header(self):
        hdr = tk.Frame(self.root, bg=BG_MID, height=58)
        hdr.pack(fill="x"); hdr.pack_propagate(False)
        tk.Label(hdr, text="✈  DroneLogAnalyzer",
                 font=("Segoe UI",18,"bold"), bg=BG_MID, fg=ACCENT_TEAL).pack(side="left",padx=18,pady=8)
        tk.Label(hdr, text=f"v{APP_VERSION}  |  ArduPilot Crash & Vibration Inspector",
                 font=("Segoe UI",10), bg=BG_MID, fg=FG_GREY).pack(side="left",pady=8)

    def _build_toolbar(self):
        bar = tk.Frame(self.root, bg=BG_LIGHT, pady=8)
        bar.pack(fill="x")
        tk.Label(bar, text="Log File:", bg=BG_LIGHT, fg=FG_WHITE,
                 font=("Segoe UI",9,"bold")).pack(side="left",padx=(12,4))
        tk.Label(bar, textvariable=self.bin_path_var,
                 bg=BG_LIGHT, fg=ACCENT_BLUE, font=("Segoe UI",9),
                 width=40, anchor="w").pack(side="left")
        bs = dict(font=("Segoe UI",9,"bold"),relief="flat",padx=10,pady=4,cursor="hand2")
        tk.Button(bar, text="📂  Browse .bin", bg=ACCENT_BLUE, fg="white",
                  command=self._browse_bin, **bs).pack(side="left",padx=6)
        tk.Button(bar, text="▶  Run Analysis", bg=ACCENT_TEAL, fg=BG_DARK,
                  command=self._run_analysis, **bs).pack(side="left",padx=4)
        tk.Button(bar, text="🗂  Open Output", bg=BG_MID, fg=FG_WHITE,
                  command=self._open_output_folder, **bs).pack(side="left",padx=4)
        tk.Button(bar, text="🔄  Reset", bg="#555570", fg=FG_WHITE,
                  command=self._reset, **bs).pack(side="left",padx=4)
        tk.Label(bar, text="Out:", bg=BG_LIGHT, fg=FG_GREY,
                 font=("Segoe UI",9)).pack(side="left",padx=(16,4))
        tk.Label(bar, textvariable=self.output_dir_var,
                 bg=BG_LIGHT, fg=FG_GREY, font=("Segoe UI",8),
                 width=28, anchor="w").pack(side="left")

    def _build_progress(self):
        pf = tk.Frame(self.root, bg=BG_DARK)
        pf.pack(fill="x", padx=10, pady=(4,0))
        self.progress_bar = ttk.Progressbar(pf, variable=self.progress_var,
            maximum=100, mode="determinate", length=400)
        self.progress_bar.pack(side="left", fill="x", expand=True)
        s = ttk.Style(); s.theme_use("default")
        s.configure("TProgressbar", troughcolor=BG_LIGHT,
                    background=ACCENT_TEAL, thickness=8)

    def _build_notebook(self):
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill="both", expand=True, padx=10, pady=6)
        s = ttk.Style()
        s.configure("TNotebook", background=BG_DARK, borderwidth=0)
        s.configure("TNotebook.Tab", background=BG_MID, foreground=FG_GREY,
                    padding=[14,6], font=("Segoe UI",9,"bold"))
        s.map("TNotebook.Tab",
              background=[("selected",BG_LIGHT)],
              foreground=[("selected",FG_WHITE)])
        self._build_overview_tab()
        self._build_vibration_tab()      # ← NEW dedicated vibration tab
        self._build_analysis_tab()
        self._build_charts_tab()
        self._build_report_tab()
        self._build_files_tab()
        self._build_log_tab()

    def _build_statusbar(self):
        sb = tk.Frame(self.root, bg=BG_MID, height=26)
        sb.pack(fill="x", side="bottom"); sb.pack_propagate(False)
        tk.Label(sb, textvariable=self.status_var,
                 bg=BG_MID, fg=FG_GREY, font=("Segoe UI",9), anchor="w").pack(side="left",padx=10)

    # ── Tab: Overview ─────────────────────────────────────────────────────────

    def _build_overview_tab(self):
        frame = tk.Frame(self.notebook, bg=BG_DARK)
        self.notebook.add(frame, text="📋  Overview")

        self.verdict_frame = tk.Frame(frame, bg=BG_MID, pady=16)
        self.verdict_frame.pack(fill="x", padx=10, pady=(10,4))
        self.verdict_icon = tk.Label(self.verdict_frame, text="⬤",
                                     font=("Segoe UI",28), bg=BG_MID, fg=FG_GREY)
        self.verdict_icon.pack(side="left", padx=(20,8))
        vt = tk.Frame(self.verdict_frame, bg=BG_MID); vt.pack(side="left")
        self.verdict_title = tk.Label(vt, text="No file loaded",
                                      font=("Segoe UI",14,"bold"), bg=BG_MID, fg=FG_WHITE)
        self.verdict_title.pack(anchor="w")
        self.verdict_sub = tk.Label(vt, text="Load a .bin file and run analysis",
                                    font=("Segoe UI",10), bg=BG_MID, fg=FG_GREY)
        self.verdict_sub.pack(anchor="w")

        stats_row = tk.Frame(frame, bg=BG_DARK); stats_row.pack(fill="x",padx=10,pady=4)
        self.stat_cards: dict[str, tk.Label] = {}
        for label, key in [("CRITICAL","critical"),("WARNING","warning"),
                            ("OK","ok"),("TOTAL","total"),("MSG TYPES","types")]:
            card = tk.Frame(stats_row, bg=BG_LIGHT, padx=18, pady=10)
            card.pack(side="left", padx=4, fill="y")
            vl = tk.Label(card, text="—", font=("Segoe UI",22,"bold"),
                          bg=BG_LIGHT, fg=FG_WHITE); vl.pack()
            tk.Label(card, text=label, font=("Segoe UI",8),
                     bg=BG_LIGHT, fg=FG_GREY).pack()
            self.stat_cards[key] = vl

        mid = tk.Frame(frame, bg=BG_DARK); mid.pack(fill="both",expand=True,padx=10,pady=4)
        tk.Label(mid, text="Message Types Extracted",
                 font=("Segoe UI",10,"bold"), bg=BG_DARK, fg=FG_WHITE).pack(anchor="w",pady=(4,2))
        tf = tk.Frame(mid, bg=BG_DARK); tf.pack(fill="both",expand=True)
        cols=("type","rows","important")
        self.msg_tree = ttk.Treeview(tf, columns=cols, show="headings", height=12)
        s = ttk.Style()
        s.configure("Treeview", background=BG_LIGHT, foreground=FG_WHITE,
                    fieldbackground=BG_LIGHT, rowheight=24, font=("Segoe UI",9))
        s.configure("Treeview.Heading", background=BG_MID, foreground=FG_WHITE,
                    font=("Segoe UI",9,"bold"))
        s.map("Treeview", background=[("selected",ACCENT_BLUE)])
        self.msg_tree.heading("type",      text="Message Type")
        self.msg_tree.heading("rows",      text="Rows")
        self.msg_tree.heading("important", text="Category")
        self.msg_tree.column("type",      width=160, anchor="w")
        self.msg_tree.column("rows",      width=100, anchor="e")
        self.msg_tree.column("important", width=180, anchor="w")
        vsb = ttk.Scrollbar(tf, orient="vertical", command=self.msg_tree.yview)
        self.msg_tree.configure(yscrollcommand=vsb.set)
        self.msg_tree.pack(side="left",fill="both",expand=True)
        vsb.pack(side="right",fill="y")

    # ── Tab: Vibration  (DEDICATED, COMPREHENSIVE) ────────────────────────────

    def _build_vibration_tab(self):
        frame = tk.Frame(self.notebook, bg=BG_DARK)
        self.notebook.add(frame, text="📳  Vibration")

        # ── Top: status cards for each axis ──
        top = tk.Frame(frame, bg=BG_DARK); top.pack(fill="x",padx=10,pady=(10,4))
        tk.Label(top, text="Vibration Analysis — All Axes",
                 font=("Segoe UI",12,"bold"), bg=BG_DARK, fg=FG_WHITE).pack(anchor="w")
        tk.Label(top, text="Source: VIBE message  |  Units: m/s²  |  IMU clipping count",
                 font=("Segoe UI",9), bg=BG_DARK, fg=FG_GREY).pack(anchor="w")

        # Status card row
        card_row = tk.Frame(frame, bg=BG_DARK); card_row.pack(fill="x",padx=10,pady=4)
        self.vib_axis_cards: dict = {}
        for axis, color in [("VibeX",VIBE_X_CLR),("VibeY",VIBE_Y_CLR),
                             ("VibeZ",VIBE_Z_CLR),("IMU Clips","#CC88FF"),
                             ("Overall",ACCENT_TEAL)]:
            card = tk.Frame(card_row, bg=BG_LIGHT, padx=16, pady=10, relief="flat", bd=1)
            card.pack(side="left", padx=5)
            tk.Label(card, text=axis, font=("Segoe UI",9,"bold"),
                     bg=BG_LIGHT, fg=color).pack()
            val_lbl = tk.Label(card, text="—", font=("Segoe UI",16,"bold"),
                               bg=BG_LIGHT, fg=FG_WHITE); val_lbl.pack()
            st_lbl  = tk.Label(card, text="Max m/s²", font=("Segoe UI",8),
                               bg=BG_LIGHT, fg=FG_GREY); st_lbl.pack()
            badge   = tk.Label(card, text="—", font=("Segoe UI",8,"bold"),
                               bg=BG_LIGHT, fg=FG_GREY); badge.pack()
            self.vib_axis_cards[axis] = {"val":val_lbl,"st":st_lbl,"badge":badge,"card":card}

        # Notebook within vibration tab for sub-views
        vib_nb = ttk.Notebook(frame)
        vib_nb.pack(fill="both", expand=True, padx=10, pady=4)

        # Sub-tab 1: Stats table
        self._build_vib_stats_subtab(vib_nb)
        # Sub-tab 2: Per-axis time-series chart
        self._build_vib_chart_subtab(vib_nb)
        # Sub-tab 3: IMU Clipping
        self._build_vib_clip_subtab(vib_nb)
        # Sub-tab 4: Comparative bar chart
        self._build_vib_compare_subtab(vib_nb)
        # Sub-tab 5: FFT Frequency Analysis
        self._build_vib_fft_subtab(vib_nb)

    def _build_vib_stats_subtab(self, parent_nb):
        frame = tk.Frame(parent_nb, bg=BG_DARK)
        parent_nb.add(frame, text="📊  Statistics Table")

        desc = tk.Frame(frame, bg=BG_MID, pady=6)
        desc.pack(fill="x", padx=8, pady=(6,2))
        tk.Label(desc, text="Per-Axis Full Statistical Summary",
                 font=("Segoe UI",10,"bold"), bg=BG_MID, fg=FG_WHITE).pack(side="left",padx=10)
        tk.Label(desc,
                 text=f"Warn: >{THRESHOLDS['vibe_warn_ms2']} m/s²   Critical: >{THRESHOLDS['vibe_max_ms2']} m/s²   Clips warn: >{THRESHOLDS['vibe_clip_warn']}   Clips crit: >{THRESHOLDS['vibe_clip_max']}",
                 font=("Segoe UI",8), bg=BG_MID, fg=FG_GREY).pack(side="right",padx=10)

        # ── Basic stats tree ───────────────────────────────────────────────
        tk.Label(frame, text="Basic Statistics",
                 font=("Segoe UI",9,"bold"), bg=BG_DARK, fg=ACCENT_TEAL).pack(anchor="w",padx=10,pady=(6,1))
        cols1 = ("axis","samples","mean","max","min","rms","std","peak_to_peak",
                 "frames_over","max_run","status","diagnosis")
        tf1 = tk.Frame(frame, bg=BG_DARK); tf1.pack(fill="x", padx=8, pady=(0,4))
        self.vib_stats_tree = ttk.Treeview(tf1, columns=cols1, show="headings", height=5)
        h1 = {
            "axis":        ("Axis",           70,  "w"),
            "samples":     ("Samples",        80,  "e"),
            "mean":        ("Mean m/s²",      80,  "e"),
            "max":         ("Max m/s²",       80,  "e"),
            "min":         ("Min m/s²",       75,  "e"),
            "rms":         ("RMS m/s²",       80,  "e"),
            "std":         ("Std Dev",        72,  "e"),
            "peak_to_peak":("Pk-Pk m/s²",    80,  "e"),
            "frames_over": ("Frames>Crit",    90,  "e"),
            "max_run":     ("Max Run",        70,  "e"),
            "status":      ("Status",         90,  "center"),
            "diagnosis":   ("Diagnosis",     280,  "w"),
        }
        for col,(hd,w,anc) in h1.items():
            self.vib_stats_tree.heading(col,text=hd)
            self.vib_stats_tree.column(col,width=w,anchor=anc,minwidth=50)
        vsb1=ttk.Scrollbar(tf1,orient="vertical",command=self.vib_stats_tree.yview)
        hsb1=ttk.Scrollbar(tf1,orient="horizontal",command=self.vib_stats_tree.xview)
        self.vib_stats_tree.configure(yscrollcommand=vsb1.set,xscrollcommand=hsb1.set)
        self.vib_stats_tree.pack(side="left",fill="x",expand=True)
        vsb1.pack(side="right",fill="y"); hsb1.pack(side="bottom",fill="x")

        # ── Percentiles tree ───────────────────────────────────────────────
        tk.Label(frame, text="Percentiles  (P50 = Median)",
                 font=("Segoe UI",9,"bold"), bg=BG_DARK, fg=ACCENT_TEAL).pack(anchor="w",padx=10,pady=(8,1))
        cols2 = ("axis","p50","p90","p95","p99","pct_warn","pct_crit","status")
        tf2 = tk.Frame(frame, bg=BG_DARK); tf2.pack(fill="x", padx=8, pady=(0,4))
        self.vib_pct_tree = ttk.Treeview(tf2, columns=cols2, show="headings", height=4)
        h2 = {
            "axis":     ("Axis",       70, "w"),
            "p50":      ("P50 (med)",  90, "e"),
            "p90":      ("P90 m/s²",  90, "e"),
            "p95":      ("P95 m/s²",  90, "e"),
            "p99":      ("P99 m/s²",  90, "e"),
            "pct_warn": ("% > Warn",  80, "e"),
            "pct_crit": ("% > Crit",  80, "e"),
            "status":   ("Status",    90, "center"),
        }
        for col,(hd,w,anc) in h2.items():
            self.vib_pct_tree.heading(col,text=hd)
            self.vib_pct_tree.column(col,width=w,anchor=anc,minwidth=50)
        vsb2=ttk.Scrollbar(tf2,orient="vertical",command=self.vib_pct_tree.yview)
        self.vib_pct_tree.configure(yscrollcommand=vsb2.set)
        self.vib_pct_tree.pack(side="left",fill="x",expand=True)
        vsb2.pack(side="right",fill="y")

        # ── FFT + Health tree ──────────────────────────────────────────────
        tk.Label(frame, text="FFT Dominant Frequency  +  Health Score",
                 font=("Segoe UI",9,"bold"), bg=BG_DARK, fg=ACCENT_TEAL).pack(anchor="w",padx=10,pady=(8,1))
        cols3 = ("axis","dom_freq","dom_amp","health_score","health_label","rolling_rms_max","status")
        tf3 = tk.Frame(frame, bg=BG_DARK); tf3.pack(fill="x", padx=8, pady=(0,4))
        self.vib_fft_tree = ttk.Treeview(tf3, columns=cols3, show="headings", height=4)
        h3 = {
            "axis":           ("Axis",           70,  "w"),
            "dom_freq":       ("Dom. Freq Hz",   110,  "e"),
            "dom_amp":        ("FFT Amplitude",  110,  "e"),
            "health_score":   ("Health 0-100",   100,  "e"),
            "health_label":   ("Health Rating",  100,  "center"),
            "rolling_rms_max":("Roll-RMS Max",   100,  "e"),
            "status":         ("Status",          90,  "center"),
        }
        for col,(hd,w,anc) in h3.items():
            self.vib_fft_tree.heading(col,text=hd)
            self.vib_fft_tree.column(col,width=w,anchor=anc,minwidth=50)
        vsb3=ttk.Scrollbar(tf3,orient="vertical",command=self.vib_fft_tree.yview)
        self.vib_fft_tree.configure(yscrollcommand=vsb3.set)
        self.vib_fft_tree.pack(side="left",fill="x",expand=True)
        vsb3.pack(side="right",fill="y")

        # tag colours shared across all three trees
        for tree in (self.vib_stats_tree, self.vib_pct_tree, self.vib_fft_tree):
            tree.tag_configure("CRITICAL", foreground="#FF6666")
            tree.tag_configure("WARNING",  foreground="#FFAA44")
            tree.tag_configure("OK",       foreground="#66CC66")
            tree.tag_configure("INFO",     foreground="#66AAFF")
            tree.tag_configure("GOOD",     foreground="#66CC66")
            tree.tag_configure("FAIR",     foreground="#FFDD44")
            tree.tag_configure("POOR",     foreground="#FF9900")

        # Colour legend
        leg = tk.Frame(frame, bg=BG_DARK); leg.pack(fill="x",padx=8,pady=4)
        for color, label in [(VIBE_X_CLR,"VibeX"),(VIBE_Y_CLR,"VibeY"),
                             (VIBE_Z_CLR,"VibeZ"),(VIBE_THRESH,"Critical threshold"),
                             (VIBE_WARN, "Warning threshold")]:
            tk.Label(leg,text="■",fg=color,bg=BG_DARK,font=("Segoe UI",12)).pack(side="left",padx=(8,2))
            tk.Label(leg,text=label,fg=FG_GREY,bg=BG_DARK,font=("Segoe UI",8)).pack(side="left",padx=(0,8))

    def _build_vib_chart_subtab(self, parent_nb):
        frame = tk.Frame(parent_nb, bg=BG_DARK)
        parent_nb.add(frame, text="📈  Time-Series Charts")

        ctrl = tk.Frame(frame, bg=BG_DARK); ctrl.pack(fill="x",padx=8,pady=4)
        tk.Label(ctrl, text="Show axes:",
                 bg=BG_DARK, fg=FG_WHITE, font=("Segoe UI",9,"bold")).pack(side="left")
        self.vib_show_x = tk.BooleanVar(value=True)
        self.vib_show_y = tk.BooleanVar(value=True)
        self.vib_show_z = tk.BooleanVar(value=True)
        self.vib_show_combined = tk.BooleanVar(value=True)
        for var, label, color in [(self.vib_show_x,"VibeX",VIBE_X_CLR),
                                   (self.vib_show_y,"VibeY",VIBE_Y_CLR),
                                   (self.vib_show_z,"VibeZ",VIBE_Z_CLR),
                                   (self.vib_show_combined,"Combined",ACCENT_TEAL)]:
            tk.Checkbutton(ctrl, text=label, variable=var,
                           bg=BG_DARK, fg=color, selectcolor=BG_MID,
                           activebackground=BG_DARK, font=("Segoe UI",9,"bold"),
                           command=self._redraw_vib_chart).pack(side="left",padx=6)
        tk.Button(ctrl, text="🔄 Refresh", bg=ACCENT_BLUE, fg="white",
                  font=("Segoe UI",9), relief="flat",
                  command=self._redraw_vib_chart).pack(side="left",padx=8)

        self.vib_chart_frame = tk.Frame(frame, bg=BG_DARK)
        self.vib_chart_frame.pack(fill="both", expand=True, padx=8, pady=4)
        tk.Label(self.vib_chart_frame, text="Run analysis to view vibration charts",
                 bg=BG_DARK, fg=FG_GREY, font=("Segoe UI",12)).pack(expand=True)

    def _build_vib_clip_subtab(self, parent_nb):
        frame = tk.Frame(parent_nb, bg=BG_DARK)
        parent_nb.add(frame, text="⚡  IMU Clipping")

        tk.Label(frame,
                 text="IMU Clipping — counts how many times the accelerometer saturated (hit hardware limit)\n"
                      "High clip counts = attitude data is INVALID during those frames → crash risk",
                 font=("Segoe UI",9), bg=BG_DARK, fg=FG_GREY,
                 justify="left").pack(anchor="w",padx=12,pady=(8,4))

        cols=("channel","total_clips","clip_rate","status","meaning")
        tf = tk.Frame(frame, bg=BG_DARK); tf.pack(fill="x",padx=10,pady=4)
        self.vib_clip_tree = ttk.Treeview(tf, columns=cols, show="headings", height=5)
        self.vib_clip_tree.heading("channel",    text="IMU Channel")
        self.vib_clip_tree.heading("total_clips",text="Total Clips")
        self.vib_clip_tree.heading("clip_rate",  text="Rate (clips/sample)")
        self.vib_clip_tree.heading("status",     text="Status")
        self.vib_clip_tree.heading("meaning",    text="Interpretation")
        self.vib_clip_tree.column("channel",     width=110, anchor="w")
        self.vib_clip_tree.column("total_clips", width=100, anchor="e")
        self.vib_clip_tree.column("clip_rate",   width=150, anchor="e")
        self.vib_clip_tree.column("status",      width=100, anchor="center")
        self.vib_clip_tree.column("meaning",     width=400, anchor="w")
        self.vib_clip_tree.pack(fill="x")
        self.vib_clip_tree.tag_configure("CRITICAL", foreground="#FF6666")
        self.vib_clip_tree.tag_configure("WARNING",  foreground="#FFAA44")
        self.vib_clip_tree.tag_configure("OK",       foreground="#66CC66")

        # Clip chart
        self.vib_clip_chart_frame = tk.Frame(frame, bg=BG_DARK)
        self.vib_clip_chart_frame.pack(fill="both", expand=True, padx=10, pady=4)
        tk.Label(self.vib_clip_chart_frame, text="Run analysis to view clipping chart",
                 bg=BG_DARK, fg=FG_GREY, font=("Segoe UI",11)).pack(expand=True)

    def _build_vib_compare_subtab(self, parent_nb):
        frame = tk.Frame(parent_nb, bg=BG_DARK)
        parent_nb.add(frame, text="🔢  Axis Comparison")

        tk.Label(frame,
                 text="Side-by-side comparison of all vibration axes — Max, Mean, RMS, Std Dev",
                 font=("Segoe UI",9), bg=BG_DARK, fg=FG_GREY).pack(anchor="w",padx=12,pady=(8,2))

        self.vib_compare_frame = tk.Frame(frame, bg=BG_DARK)
        self.vib_compare_frame.pack(fill="both", expand=True, padx=10, pady=4)
        tk.Label(self.vib_compare_frame, text="Run analysis to view axis comparison",
                 bg=BG_DARK, fg=FG_GREY, font=("Segoe UI",12)).pack(expand=True)

    def _build_vib_fft_subtab(self, parent_nb):
        """FFT frequency spectrum sub-tab."""
        frame = tk.Frame(parent_nb, bg=BG_DARK)
        parent_nb.add(frame, text="〰  FFT Frequency")

        tk.Label(frame,
                 text="FFT Frequency Spectrum — dominant frequencies reveal motor RPM resonance and prop harmonics\n"
                      "Requires scipy (pip install scipy). Assumed sample rate: 400 Hz.",
                 font=("Segoe UI",9), bg=BG_DARK, fg=FG_GREY,
                 justify="left").pack(anchor="w",padx=12,pady=(8,2))

        ctrl = tk.Frame(frame, bg=BG_DARK); ctrl.pack(fill="x",padx=8,pady=2)
        tk.Label(ctrl, text="Axis:", bg=BG_DARK, fg=FG_WHITE,
                 font=("Segoe UI",9,"bold")).pack(side="left")
        self.fft_axis_var = tk.StringVar(value="VibeX")
        for axis, color in [("VibeX",VIBE_X_CLR),("VibeY",VIBE_Y_CLR),("VibeZ",VIBE_Z_CLR)]:
            tk.Radiobutton(ctrl, text=axis, variable=self.fft_axis_var, value=axis,
                           bg=BG_DARK, fg=color, selectcolor=BG_MID,
                           activebackground=BG_DARK, font=("Segoe UI",9,"bold"),
                           command=self._redraw_vib_fft_chart).pack(side="left",padx=6)
        tk.Button(ctrl, text="🔄 Refresh", bg=ACCENT_BLUE, fg="white",
                  font=("Segoe UI",9), relief="flat",
                  command=self._redraw_vib_fft_chart).pack(side="left",padx=8)

        self.vib_fft_chart_frame = tk.Frame(frame, bg=BG_DARK)
        self.vib_fft_chart_frame.pack(fill="both", expand=True, padx=8, pady=4)
        tk.Label(self.vib_fft_chart_frame, text="Run analysis to view FFT spectrum",
                 bg=BG_DARK, fg=FG_GREY, font=("Segoe UI",12)).pack(expand=True)

    def _redraw_vib_fft_chart(self):
        """Draw FFT frequency spectrum for the selected axis."""
        if not self.vib_stats or not self.vib_stats.get("has_data") or not MATPLOTLIB_OK:
            return
        axis = self.fft_axis_var.get()
        s    = self.vib_stats["axes"].get(axis)
        if s is None:
            return

        for w in self.vib_fft_chart_frame.winfo_children():
            w.destroy()

        fig = Figure(figsize=(11, 4.5), facecolor=BG_DARK)
        color_map = {"VibeX":VIBE_X_CLR,"VibeY":VIBE_Y_CLR,"VibeZ":VIBE_Z_CLR}
        color = color_map.get(axis, ACCENT_BLUE)

        if s.get("fft_freqs") and s.get("fft_amplitudes"):
            # Full spectrum plot
            ax = fig.add_subplot(111)
            ax.set_facecolor(BG_MID)
            freqs = s["fft_freqs"]
            amps  = s["fft_amplitudes"]
            ax.bar(freqs, amps, width=(freqs[1]-freqs[0]) if len(freqs)>1 else 1,
                   color=color, alpha=0.8, edgecolor="none")
            # Mark dominant frequency
            if s.get("fft_dom_freq"):
                ax.axvline(x=s["fft_dom_freq"], color=VIBE_THRESH,
                           linestyle="--", linewidth=1.5,
                           label=f"Dominant: {s['fft_dom_freq']} Hz")
                ax.text(s["fft_dom_freq"]+0.5,
                        max(amps)*0.85,
                        f"{s['fft_dom_freq']} Hz",
                        color=VIBE_THRESH, fontsize=9)
            ax.set_title(f"FFT Spectrum — {axis}   (dominant={s.get('fft_dom_freq','N/A')} Hz)",
                         color=FG_WHITE, fontsize=10, pad=8)
            ax.set_xlabel("Frequency (Hz)", color=FG_GREY, fontsize=8)
            ax.set_ylabel("Amplitude", color=FG_GREY, fontsize=8)
            ax.tick_params(colors=FG_GREY, labelsize=8)
            ax.spines[:].set_color(BG_LIGHT)
            ax.grid(True, color=BG_LIGHT, linewidth=0.5, alpha=0.7)
            ax.legend(facecolor=BG_MID, edgecolor=BG_LIGHT, labelcolor=FG_WHITE, fontsize=9)
            # Annotation box
            info = (f"Max: {s['max']:.3f} m/s²\n"
                    f"RMS: {s['rms']:.3f} m/s²\n"
                    f"Health: {s['health_score']}/100 ({s['health_label']})\n"
                    f"Dom. Freq: {s.get('fft_dom_freq','N/A')} Hz")
            ax.text(0.99, 0.97, info, transform=ax.transAxes,
                    fontsize=8, color=FG_WHITE, va="top", ha="right",
                    bbox=dict(boxstyle="round,pad=0.4", facecolor=BG_DARK, alpha=0.8,
                              edgecolor=BG_LIGHT))
        else:
            # scipy not installed — show raw time-domain with annotation
            df = load_csv_file(self.csv_files, "VIBE")
            ax = fig.add_subplot(111)
            ax.set_facecolor(BG_MID)
            if df is not None:
                col = safe_col(df, axis, axis.lower())
                if col:
                    data = df[col].values
                    ax.plot(data, color=color, lw=0.7, label=axis)
                    ax.axhline(y=THRESHOLDS["vibe_max_ms2"], color=VIBE_THRESH,
                               linestyle="--", lw=1.2, label="Critical")
                    ax.axhline(y=THRESHOLDS["vibe_warn_ms2"], color=VIBE_WARN,
                               linestyle=":", lw=1, label="Warning")
            ax.text(0.5, 0.95,
                    "Install scipy for FFT spectrum:\n  pip install scipy",
                    transform=ax.transAxes, ha="center", va="top",
                    color=VIBE_WARN, fontsize=10,
                    bbox=dict(boxstyle="round", facecolor=BG_DARK, alpha=0.9, edgecolor=VIBE_WARN))
            ax.set_title(f"{axis} — Time Domain (scipy not installed for FFT)",
                         color=FG_WHITE, fontsize=10)
            ax.tick_params(colors=FG_GREY, labelsize=8)
            ax.spines[:].set_color(BG_LIGHT)
            ax.grid(True, color=BG_LIGHT, linewidth=0.5, alpha=0.7)
            ax.legend(facecolor=BG_MID, edgecolor=BG_LIGHT, labelcolor=FG_WHITE, fontsize=8)

        fig.tight_layout(pad=2)
        canvas = FigureCanvasTkAgg(fig, master=self.vib_fft_chart_frame)
        canvas.draw(); canvas.get_tk_widget().pack(fill="both", expand=True)

    # ── Tab: Analysis ─────────────────────────────────────────────────────────

    def _build_analysis_tab(self):
        frame = tk.Frame(self.notebook, bg=BG_DARK)
        self.notebook.add(frame, text="🔍  Analysis")
        tk.Label(frame, text="Full Crash Parameter Analysis",
                 font=("Segoe UI",11,"bold"), bg=BG_DARK, fg=FG_WHITE).pack(anchor="w",padx=10,pady=(10,2))
        fbar = tk.Frame(frame, bg=BG_DARK); fbar.pack(fill="x",padx=10,pady=2)
        tk.Label(fbar, text="Filter:", bg=BG_DARK, fg=FG_GREY,
                 font=("Segoe UI",9)).pack(side="left")
        self.filter_var = tk.StringVar(value="ALL")
        for val in ("ALL","CRITICAL","WARNING","INFO","OK"):
            clr = SEVERITY_COLOR.get(f"[{val}]",FG_GREY) if val!="ALL" else FG_WHITE
            tk.Radiobutton(fbar, text=val, variable=self.filter_var, value=val,
                           bg=BG_DARK, fg=clr, selectcolor=BG_MID,
                           activebackground=BG_DARK, font=("Segoe UI",9,"bold"),
                           command=self._apply_filter).pack(side="left",padx=6)
        at_frame = tk.Frame(frame, bg=BG_DARK); at_frame.pack(fill="both",expand=True,padx=10,pady=4)
        cols=("category","parameter","status","max_value","threshold","anomaly_count","interpretation")
        self.analysis_tree = ttk.Treeview(at_frame, columns=cols, show="headings", height=20)
        for col,(hd,w,anc) in {
            "category":      ("Category",      110,"w"),
            "parameter":     ("Parameter",     200,"w"),
            "status":        ("Status",         90,"center"),
            "max_value":     ("Observed Value",160,"w"),
            "threshold":     ("Threshold",     120,"w"),
            "anomaly_count": ("Anomalies",      80,"e"),
            "interpretation":("Interpretation",300,"w"),
        }.items():
            self.analysis_tree.heading(col, text=hd)
            self.analysis_tree.column(col, width=w, anchor=anc, minwidth=60)
        vsb2=ttk.Scrollbar(at_frame,orient="vertical",command=self.analysis_tree.yview)
        hsb2=ttk.Scrollbar(at_frame,orient="horizontal",command=self.analysis_tree.xview)
        self.analysis_tree.configure(yscrollcommand=vsb2.set,xscrollcommand=hsb2.set)
        self.analysis_tree.pack(side="left",fill="both",expand=True)
        vsb2.pack(side="right",fill="y"); hsb2.pack(side="bottom",fill="x")
        self.analysis_tree.tag_configure("CRITICAL",foreground="#FF6666")
        self.analysis_tree.tag_configure("WARNING", foreground="#FFAA44")
        self.analysis_tree.tag_configure("INFO",    foreground="#66AAFF")
        self.analysis_tree.tag_configure("OK",      foreground="#66CC66")

    # ── Tab: Charts ───────────────────────────────────────────────────────────

    def _build_charts_tab(self):
        frame = tk.Frame(self.notebook, bg=BG_DARK)
        self.notebook.add(frame, text="📊  Charts")
        ctrl = tk.Frame(frame, bg=BG_DARK); ctrl.pack(fill="x",padx=10,pady=6)
        tk.Label(ctrl, text="Plot:", bg=BG_DARK, fg=FG_WHITE,
                 font=("Segoe UI",9,"bold")).pack(side="left")
        self.chart_choice = tk.StringVar(value="Attitude (Roll/Pitch)")
        choices=["Attitude (Roll/Pitch)","Vibration — All Axes",
                 "Vibration — Separate Axes","GPS (HDOP & Satellites)",
                 "Battery Voltage","Battery Current","Altitude (Baro)",
                 "Motor Outputs","Severity Summary"]
        ttk.OptionMenu(ctrl, self.chart_choice, choices[0], *choices,
                       command=lambda _: self._draw_chart()).pack(side="left",padx=8)
        tk.Button(ctrl, text="🔄 Refresh", bg=ACCENT_BLUE, fg="white",
                  font=("Segoe UI",9), relief="flat",
                  command=self._draw_chart).pack(side="left",padx=4)
        self.chart_frame = tk.Frame(frame, bg=BG_DARK)
        self.chart_frame.pack(fill="both",expand=True,padx=10,pady=4)
        tk.Label(self.chart_frame, text="Run analysis to generate charts",
                 font=("Segoe UI",13), bg=BG_DARK, fg=FG_GREY).pack(expand=True)

    # ── Tab: Report ───────────────────────────────────────────────────────────

    def _build_report_tab(self):
        frame = tk.Frame(self.notebook, bg=BG_DARK)
        self.notebook.add(frame, text="📄  Report")
        ctrl = tk.Frame(frame, bg=BG_DARK); ctrl.pack(fill="x",padx=10,pady=6)
        tk.Button(ctrl, text="💾  Save TXT", bg=ACCENT_BLUE, fg="white",
                  font=("Segoe UI",9,"bold"), relief="flat",
                  command=self._save_report_txt).pack(side="left",padx=4)
        tk.Button(ctrl, text="📋  Copy", bg=BG_MID, fg=FG_WHITE,
                  font=("Segoe UI",9), relief="flat",
                  command=self._copy_report).pack(side="left",padx=4)
        self.report_text = scrolledtext.ScrolledText(
            frame, wrap="none", bg="#0D0D1A", fg=FG_WHITE,
            font=("Consolas",9), insertbackground=FG_WHITE,
            selectbackground=ACCENT_BLUE)
        self.report_text.pack(fill="both",expand=True,padx=10,pady=4)
        self.report_text.insert("1.0","Run analysis to generate the report…")
        self.report_text.configure(state="disabled")

    # ── Tab: Files ────────────────────────────────────────────────────────────

    def _build_files_tab(self):
        frame = tk.Frame(self.notebook, bg=BG_DARK)
        self.notebook.add(frame, text="🗂  Output Files")
        tk.Label(frame, text="Files saved in output folder",
                 font=("Segoe UI",10,"bold"), bg=BG_DARK, fg=FG_WHITE).pack(anchor="w",padx=10,pady=(10,2))
        cols=("file","size_kb","description","saved_at")
        ff = tk.Frame(frame, bg=BG_DARK); ff.pack(fill="both",expand=True,padx=10,pady=4)
        self.files_tree = ttk.Treeview(ff, columns=cols, show="headings", height=20)
        self.files_tree.heading("file",        text="File Name")
        self.files_tree.heading("size_kb",     text="Size (KB)")
        self.files_tree.heading("description", text="Description")
        self.files_tree.heading("saved_at",    text="Saved At")
        self.files_tree.column("file",        width=200, anchor="w")
        self.files_tree.column("size_kb",     width=90,  anchor="e")
        self.files_tree.column("description", width=420, anchor="w")
        self.files_tree.column("saved_at",    width=90,  anchor="center")
        vsb3=ttk.Scrollbar(ff,orient="vertical",command=self.files_tree.yview)
        self.files_tree.configure(yscrollcommand=vsb3.set)
        self.files_tree.pack(side="left",fill="both",expand=True)
        vsb3.pack(side="right",fill="y")
        self.files_footer = tk.Label(frame, text="", bg=BG_DARK, fg=FG_GREY,
                                     font=("Segoe UI",9))
        self.files_footer.pack(anchor="w",padx=10,pady=4)
        tk.Button(frame, text="📂  Open Output Folder",
                  bg=ACCENT_BLUE, fg="white", font=("Segoe UI",9,"bold"), relief="flat",
                  command=self._open_output_folder).pack(anchor="w",padx=10,pady=4)

    # ── Tab: Log ──────────────────────────────────────────────────────────────

    def _build_log_tab(self):
        frame = tk.Frame(self.notebook, bg=BG_DARK)
        self.notebook.add(frame, text="📟  Console Log")
        ctrl = tk.Frame(frame, bg=BG_DARK); ctrl.pack(fill="x",padx=10,pady=4)
        tk.Button(ctrl, text="🗑  Clear", bg=BG_MID, fg=FG_WHITE,
                  font=("Segoe UI",9), relief="flat",
                  command=self._clear_log).pack(side="left")
        self.log_text = scrolledtext.ScrolledText(
            frame, wrap="word", bg="#0D0D1A", fg="#00FF88",
            font=("Consolas",9), insertbackground="#00FF88")
        self.log_text.pack(fill="both",expand=True,padx=10,pady=4)

    # ─────────────────────────────────────────────────────────────────────────
    # ACTIONS
    # ─────────────────────────────────────────────────────────────────────────

    def _browse_bin(self):
        path = filedialog.askopenfilename(
            title="Select ArduPilot .bin log file",
            filetypes=[("ArduPilot Log","*.bin *.BIN *.ulg"),("All files","*.*")])
        if path:
            self.bin_path_var.set(path)
            self._log(f"Selected: {path}")
            self._set_status(f"File loaded: {Path(path).name}")

    def _run_analysis(self):
        bp = self.bin_path_var.get()
        if bp == "No file selected" or not Path(bp).exists():
            messagebox.showwarning("No File","Please select a .bin log file first.")
            return
        if self._worker_thread and self._worker_thread.is_alive():
            messagebox.showinfo("Busy","Analysis is already running.")
            return
        self._worker_thread = threading.Thread(
            target=self._analysis_worker, args=(bp,), daemon=True)
        self._worker_thread.start()

    def _analysis_worker(self, bin_path: str):
        try:
            self._set_status("Starting analysis…"); self._set_progress(0)
            self._log("="*60)
            self._log(f"ANALYSIS STARTED: {Path(bin_path).name}")
            self._log(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            self._log("="*60)

            log_stem = Path(bin_path).stem
            self.output_mgr = OutputManager(OUTPUT_ROOT, log_stem)
            self.root.after(0,lambda:self.output_dir_var.set(str(self.output_mgr.base.resolve())))
            self._log(f"Output folder: {self.output_mgr.base.resolve()}")
            self._set_progress(5)

            # Step 1: BIN → CSV
            self._set_status("Step 1/3 — Converting BIN → CSV…")
            self._log("\n── STEP 1: BIN → CSV ─────────────────────────")
            self.csv_files = bin_to_csv(bin_path, self.output_mgr,
                                        progress_cb=self._set_progress,
                                        log_cb=self._log)
            self._set_progress(75)
            self.root.after(0, self._populate_overview)

            # Step 2: Vibration deep analysis
            self._set_status("Step 2/3 — Vibration analysis…")
            self._log("\n── STEP 2: VIBRATION ANALYSIS ───────────────")
            df_vibe = load_csv_file(self.csv_files, "VIBE")
            if df_vibe is not None:
                self.vib_stats = compute_vibration_stats(df_vibe)
                vib_rows = build_vibration_csv_rows(
                    self.vib_stats, Path(bin_path).name)
                self.output_mgr.save_vibration_report_csv(vib_rows)
                ov = self.vib_stats.get("overall", {})
                self._log(f"  Vibration samples : {ov.get('samples',0):,}")
                self._log(f"  Combined RMS      : {ov.get('combined_rms',0):.3f} m/s²")
                self._log(f"  Total IMU Clips   : {ov.get('total_clips',0)}")
                self._log(f"  Overall Status    : {ov.get('status','N/A')}")
                for axis, s in self.vib_stats.get("axes",{}).items():
                    self._log(f"  {axis}: max={s['max']:.2f}  rms={s['rms']:.2f}  "
                              f"mean={s['mean']:.2f}  std={s['std']:.2f}  "
                              f"frames_over={s['frames_over']}  status={s['status']}")
            else:
                self.vib_stats = {}
                self._log("  [WARN] No VIBE data found in this log")
            self._set_progress(82)

            # Step 3: Full crash analysis
            self._set_status("Step 2/3 — Crash analysis…")
            self._log("\n── STEP 3: CRASH ANALYSIS ────────────────────")
            self.events = run_all_analyzers(self.csv_files)
            self.verdict, self.evidence = determine_verdict(self.events)
            self._set_progress(88)
            self._log(f"  Parameters checked: {len(self.events)}")
            self._log(f"  Verdict: {self.verdict}")

            # Step 4: Save outputs
            self._set_status("Step 3/3 — Saving files…")
            self._log("\n── STEP 4: SAVING ALL OUTPUTS ────────────────")
            self._save_all_outputs()
            self._set_progress(95)

            # Update all UI tabs
            self.root.after(0, self._populate_analysis_tab)
            self.root.after(0, self._populate_report_tab)
            self.root.after(0, self._populate_files_tab)
            self.root.after(0, self._populate_vibration_tab)
            self.root.after(0, self._draw_chart)
            self.root.after(0, self._update_verdict_banner)
            self.root.after(0, self._update_stat_cards)

            self._set_progress(100)
            self._set_status(f"Complete — {self.verdict}")
            self._log("\n"+"="*60)
            self._log("ANALYSIS COMPLETE"); self._log(f"VERDICT: {self.verdict}")
            self._log("="*60)

        except Exception as exc:
            err = traceback.format_exc()
            self._log(f"\n[ERROR] {exc}\n{err}")
            self._set_status(f"Error: {exc}")
            self.root.after(0, lambda: messagebox.showerror("Error", str(exc)))

    def _save_all_outputs(self):
        if not self.output_mgr: return
        nc=sum(1 for e in self.events if e["status"]=="[CRITICAL]")
        nw=sum(1 for e in self.events if e["status"]=="[WARNING]")
        no=sum(1 for e in self.events if e["status"]=="[OK]")
        self.output_mgr.save_crash_report_csv(self.events)
        payload={"log_file":Path(self.bin_path_var.get()).name,
                 "generated":datetime.now().isoformat(),
                 "verdict":self.verdict,
                 "statistics":{"total_checks":len(self.events),"critical":nc,"warning":nw,"ok":no},
                 "crash_evidence":self.evidence,"events":self.events,
                 "vibration_stats":self.vib_stats,"thresholds_used":THRESHOLDS}
        self.output_mgr.save_crash_report_json(payload)
        text=build_text_report(self.events,Path(self.bin_path_var.get()).name,
                               self.verdict,self.evidence,self.vib_stats)
        self.output_mgr.save_crash_report_txt(text)
        summary={"log_file":Path(self.bin_path_var.get()).name,
                 "generated":datetime.now().isoformat(),"verdict":self.verdict,
                 "critical_count":nc,"warning_count":nw,"ok_count":no,
                 "vibration_overall_status":self.vib_stats.get("overall",{}).get("status","N/A"),
                 "vibration_combined_rms":self.vib_stats.get("overall",{}).get("combined_rms",0),
                 "critical_categories":sorted({e["category"] for e in self.events
                                               if e["status"]=="[CRITICAL]"}),
                 "csv_message_types":sorted(self.csv_files.keys()),
                 "output_folder":str(self.output_mgr.base.resolve())}
        self.output_mgr.save_summary_json(summary)
        self._log(f"All outputs → {self.output_mgr.base.resolve()}")

    # ─────────────────────────────────────────────────────────────────────────
    # VIBRATION TAB POPULATION
    # ─────────────────────────────────────────────────────────────────────────

    def _populate_vibration_tab(self):
        """Fill all vibration sub-tabs with data."""
        if not self.vib_stats or not self.vib_stats.get("has_data"):
            self._log("  [INFO] No VIBE data to display in vibration tab")
            return

        self._populate_vib_axis_cards()
        self._populate_vib_stats_tree()
        self._populate_vib_clip_tree()
        self._redraw_vib_chart()
        self._redraw_vib_compare()
        self._redraw_vib_fft_chart()

    def _populate_vib_axis_cards(self):
        axes_map = {
            "VibeX": self.vib_stats["axes"].get("VibeX"),
            "VibeY": self.vib_stats["axes"].get("VibeY"),
            "VibeZ": self.vib_stats["axes"].get("VibeZ"),
        }
        color_map = {"VibeX":VIBE_X_CLR,"VibeY":VIBE_Y_CLR,"VibeZ":VIBE_Z_CLR}
        for axis, s in axes_map.items():
            c = self.vib_axis_cards.get(axis)
            if c is None or s is None: continue
            clr = SEVERITY_COLOR.get(s["status"], FG_WHITE)
            c["val"].configure(text=f"{s['max']:.2f}", fg=clr)
            c["st"].configure(text=f"Max m/s²  RMS={s['rms']:.2f}")
            c["badge"].configure(text=s["status"], fg=clr)

        # Clips card
        ov = self.vib_stats.get("overall",{})
        cc = self.vib_axis_cards.get("IMU Clips")
        if cc and ov:
            tc   = ov.get("total_clips",0)
            clr  = SEVERITY_COLOR.get(ov.get("status",SEVERITY_COLOR["[OK]"]),"#CC88FF")
            cc["val"].configure(text=str(tc), fg=clr)
            cc["st"].configure(text="Total Clips")
            cc["badge"].configure(text="[CRITICAL]" if tc>THRESHOLDS["vibe_clip_max"]
                                  else "[WARNING]" if tc>THRESHOLDS["vibe_clip_warn"]
                                  else "[OK]", fg=clr)

        # Overall card
        oc = self.vib_axis_cards.get("Overall")
        if oc and ov:
            clr = SEVERITY_COLOR.get(ov.get("status","[OK]"), ACCENT_TEAL)
            oc["val"].configure(text=f"{ov.get('combined_rms',0):.3f}", fg=clr)
            oc["st"].configure(text="Combined RMS m/s²")
            oc["badge"].configure(text=ov.get("status","—"), fg=clr)

    def _populate_vib_stats_tree(self):
        for item in self.vib_stats_tree.get_children():
            self.vib_stats_tree.delete(item)
        for item in self.vib_pct_tree.get_children():
            self.vib_pct_tree.delete(item)
        for item in self.vib_fft_tree.get_children():
            self.vib_fft_tree.delete(item)

        T = THRESHOLDS
        for axis, s in self.vib_stats["axes"].items():
            tag = s["status"].strip("[]")
            # Basic stats
            self.vib_stats_tree.insert("","end", tags=(tag,), values=(
                axis, f"{s['samples']:,}",
                f"{s['mean']:.3f}", f"{s['max']:.3f}", f"{s['min']:.3f}",
                f"{s['rms']:.3f}", f"{s['std']:.3f}", f"{s['peak_to_peak']:.3f}",
                s["frames_over"], s["max_consecutive"],
                s["status"], s["diagnosis"]))
            # Percentiles
            pct_tag = "CRITICAL" if s["pct_above_crit"]>5 else "WARNING" if s["pct_above_warn"]>10 else "OK"
            self.vib_pct_tree.insert("","end", tags=(pct_tag,), values=(
                axis,
                f"{s['p50']:.3f}", f"{s['p90']:.3f}",
                f"{s['p95']:.3f}", f"{s['p99']:.3f}",
                f"{s['pct_above_warn']:.1f}%", f"{s['pct_above_crit']:.1f}%",
                s["status"]))
            # FFT + Health
            freq_str = f"{s['fft_dom_freq']} Hz" if s.get("fft_dom_freq") else "N/A (scipy not installed)"
            amp_str  = f"{s['fft_dom_amp']:.4f}"  if s.get("fft_dom_amp")  else "N/A"
            hl_tag   = s["health_label"]
            self.vib_fft_tree.insert("","end", tags=(hl_tag,), values=(
                axis, freq_str, amp_str,
                f"{s['health_score']}/100",
                s["health_label"],
                f"{s['rolling_rms_max']:.3f} m/s²",
                s["status"]))

        # Clip rows in basic stats tree
        self.vib_stats_tree.insert("","end", values=("──────",)+("")*11)
        for clip, c in self.vib_stats["clips"].items():
            tag = c["status"].strip("[]")
            meaning = ("IMU fully saturated" if c["total"]>T["vibe_clip_max"]
                       else "Moderate clipping" if c["total"]>T["vibe_clip_warn"]
                       else "No clipping")
            self.vib_stats_tree.insert("","end", tags=(tag,), values=(
                clip, self.vib_stats["overall"].get("samples",0),
                "N/A", c["total"], "N/A", "N/A", "N/A", "N/A",
                c["total"], c["burst_max"], c["status"], meaning))

    def _populate_vib_clip_tree(self):
        for item in self.vib_clip_tree.get_children():
            self.vib_clip_tree.delete(item)
        T = THRESHOLDS
        for clip, c in self.vib_stats["clips"].items():
            tag = c["status"].strip("[]")
            meaning = ("⚠ CRITICAL: IMU fully saturated — attitude data invalid" if c["total"]>T["vibe_clip_max"]
                       else "⚠ WARNING: Elevated clipping — check vibration dampening" if c["total"]>T["vibe_clip_warn"]
                       else "✓ Normal — minimal clipping")
            self.vib_clip_tree.insert("","end", tags=(tag,),
                values=(clip, c["total"], f"{c['rate']:.6f}", c["status"], meaning))

        # Draw clip bar chart
        if MATPLOTLIB_OK:
            for w in self.vib_clip_chart_frame.winfo_children():
                w.destroy()
            fig = Figure(figsize=(10,3), facecolor=BG_DARK)
            ax  = fig.add_subplot(111)
            ax.set_facecolor(BG_MID)
            clips_data = self.vib_stats["clips"]
            labels = list(clips_data.keys())
            vals   = [clips_data[k]["total"] for k in labels]
            colors = [SEVERITY_COLOR.get(clips_data[k]["status"],"#CC88FF") for k in labels]
            bars   = ax.bar(labels, vals, color=colors, edgecolor=BG_DARK, width=0.5)
            ax.bar_label(bars, padding=3, color=FG_WHITE, fontsize=10, fontweight="bold")
            ax.axhline(y=T["vibe_clip_max"], color=VIBE_THRESH,
                       linestyle="--", linewidth=1.5, label=f"Critical ({T['vibe_clip_max']})")
            ax.axhline(y=T["vibe_clip_warn"], color=VIBE_WARN,
                       linestyle=":", linewidth=1.2, label=f"Warning ({T['vibe_clip_warn']})")
            ax.legend(facecolor=BG_MID,edgecolor=BG_LIGHT,labelcolor=FG_WHITE,fontsize=8)
            ax.tick_params(colors=FG_GREY,labelsize=9)
            ax.spines[:].set_color(BG_LIGHT)
            ax.set_title("IMU Clip Counts per Channel",color=FG_WHITE,fontsize=10)
            ax.set_ylabel("Total Clips",color=FG_GREY,fontsize=8)
            ax.set_ylim(0, max(max(vals)+5, T["vibe_clip_max"]+10))
            ax.grid(True,color=BG_LIGHT,linewidth=0.5,alpha=0.7)
            fig.tight_layout(pad=1.5)
            canvas = FigureCanvasTkAgg(fig, master=self.vib_clip_chart_frame)
            canvas.draw(); canvas.get_tk_widget().pack(fill="both",expand=True)

    def _redraw_vib_chart(self):
        """Draw per-axis time-series vibration chart with subplots."""
        if not self.vib_stats or not self.vib_stats.get("has_data") or not MATPLOTLIB_OK:
            return
        df = load_csv_file(self.csv_files, "VIBE")
        if df is None: return

        for w in self.vib_chart_frame.winfo_children():
            w.destroy()

        show = {
            "VibeX": self.vib_show_x.get(),
            "VibeY": self.vib_show_y.get(),
            "VibeZ": self.vib_show_z.get(),
        }
        show_combined = self.vib_show_combined.get()
        axes_to_show  = [a for a, v in show.items() if v]
        n_plots = len(axes_to_show) + (1 if show_combined else 0)
        if n_plots == 0:
            return

        fig = Figure(figsize=(11, max(3, n_plots * 2.2)), facecolor=BG_DARK)
        gs  = gridspec.GridSpec(n_plots, 1, figure=fig, hspace=0.45)
        T   = THRESHOLDS
        x   = np.arange(len(df))
        clr_map = {"VibeX": VIBE_X_CLR, "VibeY": VIBE_Y_CLR, "VibeZ": VIBE_Z_CLR}

        plot_idx = 0
        for axis in ("VibeX", "VibeY", "VibeZ"):
            if not show.get(axis): continue
            col = safe_col(df, axis, axis.lower())
            if col is None: continue
            s   = self.vib_stats["axes"].get(axis,{})
            ax  = fig.add_subplot(gs[plot_idx]); plot_idx += 1
            ax.set_facecolor(BG_MID)
            data = df[col].values
            ax.fill_between(x, data, alpha=0.2, color=clr_map[axis])
            ax.plot(x, data, color=clr_map[axis], linewidth=0.7, label=axis)
            ax.axhline(y=T["vibe_max_ms2"], color=VIBE_THRESH,
                       linestyle="--", linewidth=1, alpha=0.8, label=f"Critical ({T['vibe_max_ms2']})")
            ax.axhline(y=T["vibe_warn_ms2"], color=VIBE_WARN,
                       linestyle=":", linewidth=1, alpha=0.7, label=f"Warning ({T['vibe_warn_ms2']})")
            # Shade regions above threshold red
            ax.fill_between(x, data, T["vibe_max_ms2"],
                            where=np.array(data) > T["vibe_max_ms2"],
                            alpha=0.4, color=VIBE_THRESH, label="Over threshold")
            stat_txt = f"max={s.get('max',0):.2f}  rms={s.get('rms',0):.2f}  frames_over={s.get('frames_over',0)}  {s.get('status','')}"
            ax.set_title(f"{axis}  —  {stat_txt}", color=FG_WHITE, fontsize=8, pad=4)
            ax.set_ylabel("m/s²", color=FG_GREY, fontsize=7)
            ax.tick_params(colors=FG_GREY, labelsize=7)
            ax.spines[:].set_color(BG_LIGHT)
            ax.grid(True, color=BG_LIGHT, linewidth=0.4, alpha=0.6)
            ax.legend(loc="upper right", facecolor=BG_MID, edgecolor=BG_LIGHT,
                      labelcolor=FG_WHITE, fontsize=7, ncol=4)

        # Combined subplot
        if show_combined:
            ax = fig.add_subplot(gs[plot_idx]); plot_idx += 1
            ax.set_facecolor(BG_MID)
            for axis, color in [("VibeX",VIBE_X_CLR),("VibeY",VIBE_Y_CLR),("VibeZ",VIBE_Z_CLR)]:
                col = safe_col(df, axis, axis.lower())
                if col: ax.plot(x, df[col].values, color=color, linewidth=0.7, alpha=0.85, label=axis)
            ax.axhline(y=T["vibe_max_ms2"], color=VIBE_THRESH,
                       linestyle="--", linewidth=1.2, label="Critical threshold")
            ax.axhline(y=T["vibe_warn_ms2"], color=VIBE_WARN,
                       linestyle=":", linewidth=1, label="Warning threshold")
            ax.set_title("Combined — All Axes", color=FG_WHITE, fontsize=8, pad=4)
            ax.set_ylabel("m/s²", color=FG_GREY, fontsize=7)
            ax.set_xlabel("Sample Index", color=FG_GREY, fontsize=7)
            ax.tick_params(colors=FG_GREY, labelsize=7)
            ax.spines[:].set_color(BG_LIGHT)
            ax.grid(True, color=BG_LIGHT, linewidth=0.4, alpha=0.6)
            ax.legend(loc="upper right", facecolor=BG_MID, edgecolor=BG_LIGHT,
                      labelcolor=FG_WHITE, fontsize=7, ncol=4)

        canvas = FigureCanvasTkAgg(fig, master=self.vib_chart_frame)
        canvas.draw(); canvas.get_tk_widget().pack(fill="both",expand=True)

    def _redraw_vib_compare(self):
        """Draw side-by-side grouped bar chart comparing axes."""
        if not self.vib_stats or not self.vib_stats.get("has_data") or not MATPLOTLIB_OK:
            return
        for w in self.vib_compare_frame.winfo_children():
            w.destroy()

        axes   = list(self.vib_stats["axes"].keys())
        if not axes: return

        metrics = ["mean","max","rms","std","peak_to_peak"]
        labels  = ["Mean","Max","RMS","Std Dev","Pk-Pk"]
        n_m     = len(metrics)
        n_axes  = len(axes)
        x       = np.arange(n_m)
        width   = 0.25
        colors  = [VIBE_X_CLR, VIBE_Y_CLR, VIBE_Z_CLR]

        fig = Figure(figsize=(11, 4.5), facecolor=BG_DARK)
        ax  = fig.add_subplot(111)
        ax.set_facecolor(BG_MID)

        for i, (axis, color) in enumerate(zip(axes, colors)):
            s    = self.vib_stats["axes"][axis]
            vals = [s[m] for m in metrics]
            bars = ax.bar(x + i*width, vals, width, label=axis,
                          color=color, edgecolor=BG_DARK, alpha=0.85)
            ax.bar_label(bars, fmt="%.2f", padding=2,
                         color=color, fontsize=7, fontweight="bold")

        # Threshold lines
        ax.axhline(y=THRESHOLDS["vibe_max_ms2"], color=VIBE_THRESH,
                   linestyle="--", linewidth=1.2, label=f"Critical ({THRESHOLDS['vibe_max_ms2']} m/s²)")
        ax.axhline(y=THRESHOLDS["vibe_warn_ms2"], color=VIBE_WARN,
                   linestyle=":", linewidth=1, label=f"Warning ({THRESHOLDS['vibe_warn_ms2']} m/s²)")

        ax.set_xticks(x + width*(n_axes-1)/2)
        ax.set_xticklabels(labels, color=FG_WHITE, fontsize=9)
        ax.set_ylabel("m/s²", color=FG_GREY, fontsize=9)
        ax.set_title("Vibration Axis Comparison — All Metrics",
                     color=FG_WHITE, fontsize=11, pad=8)
        ax.tick_params(colors=FG_GREY, labelsize=8)
        ax.spines[:].set_color(BG_LIGHT)
        ax.grid(True, axis="y", color=BG_LIGHT, linewidth=0.5, alpha=0.7)
        ax.legend(facecolor=BG_MID, edgecolor=BG_LIGHT,
                  labelcolor=FG_WHITE, fontsize=8, loc="upper right")
        fig.tight_layout(pad=2)
        canvas = FigureCanvasTkAgg(fig, master=self.vib_compare_frame)
        canvas.draw(); canvas.get_tk_widget().pack(fill="both",expand=True)

    # ─────────────────────────────────────────────────────────────────────────
    # OTHER TAB POPULATION
    # ─────────────────────────────────────────────────────────────────────────

    def _populate_overview(self):
        for item in self.msg_tree.get_children():
            self.msg_tree.delete(item)
        for mtype in sorted(self.csv_files.keys()):
            path = self.csv_files[mtype]
            try:
                rows = sum(1 for _ in open(path, encoding="utf-8")) - 1
            except Exception:
                rows = 0
            cat = "Important" if mtype in IMPORTANT_MSG_TYPES else "Other"
            self.msg_tree.insert("","end",
                values=(mtype,f"{rows:,}",cat),
                tags=("imp" if cat=="Important" else "oth",))
        self.msg_tree.tag_configure("imp", foreground=ACCENT_TEAL)
        self.msg_tree.tag_configure("oth", foreground=FG_GREY)

    def _update_stat_cards(self):
        nc=sum(1 for e in self.events if e["status"]=="[CRITICAL]")
        nw=sum(1 for e in self.events if e["status"]=="[WARNING]")
        no=sum(1 for e in self.events if e["status"]=="[OK]")
        self.stat_cards["critical"].configure(text=str(nc),
            fg=SEVERITY_COLOR["[CRITICAL]"] if nc else "#44BB44")
        self.stat_cards["warning"].configure(text=str(nw),
            fg=SEVERITY_COLOR["[WARNING]"] if nw else FG_WHITE)
        self.stat_cards["ok"].configure(text=str(no),fg="#44BB44")
        self.stat_cards["total"].configure(text=str(len(self.events)),fg=FG_WHITE)
        self.stat_cards["types"].configure(text=str(len(self.csv_files)),fg=FG_WHITE)

    def _update_verdict_banner(self):
        v = self.verdict
        if "CRASH LIKELY" in v:   color,icon=SEVERITY_COLOR["[CRITICAL]"],"💥"
        elif "PROBABLE" in v:     color,icon="#FF7722","⚠️"
        elif "POSSIBLE" in v or "CAUTION" in v: color,icon=SEVERITY_COLOR["[WARNING]"],"⚠️"
        else:                     color,icon=SEVERITY_COLOR["[OK]"],"✅"
        self.verdict_icon.configure(text=icon,fg=color)
        self.verdict_title.configure(text=v,fg=color)
        nc=sum(1 for e in self.events if e["status"]=="[CRITICAL]")
        nw=sum(1 for e in self.events if e["status"]=="[WARNING]")
        vib_ov=self.vib_stats.get("overall",{})
        vib_txt=f"  •  Vib RMS={vib_ov.get('combined_rms',0):.3f} m/s² [{vib_ov.get('status','')}]" if vib_ov else ""
        self.verdict_sub.configure(
            text=f"{nc} critical  •  {nw} warnings  •  {len(self.csv_files)} msg types"
                 f"{vib_txt}  •  {Path(self.bin_path_var.get()).name}")

    def _populate_analysis_tab(self):
        for item in self.analysis_tree.get_children():
            self.analysis_tree.delete(item)
        for e in self.events:
            tag=e["status"].strip("[]")
            self.analysis_tree.insert("","end", tags=(tag,), values=(
                e["category"],e["parameter"],e["status"],
                e["max_value"],e["threshold"],
                e["anomaly_count"],e["interpretation"]))

    def _apply_filter(self):
        filt=self.filter_var.get()
        for item in self.analysis_tree.get_children():
            self.analysis_tree.delete(item)
        for e in self.events:
            tag=e["status"].strip("[]")
            if filt!="ALL" and tag!=filt: continue
            self.analysis_tree.insert("","end", tags=(tag,), values=(
                e["category"],e["parameter"],e["status"],
                e["max_value"],e["threshold"],
                e["anomaly_count"],e["interpretation"]))

    def _populate_report_tab(self):
        text=build_text_report(self.events,Path(self.bin_path_var.get()).name,
                               self.verdict,self.evidence,self.vib_stats)
        self.report_text.configure(state="normal")
        self.report_text.delete("1.0","end")
        self.report_text.insert("1.0",text)
        self.report_text.configure(state="disabled")

    def _populate_files_tab(self):
        for item in self.files_tree.get_children():
            self.files_tree.delete(item)
        if not self.output_mgr: return
        manifest=self.output_mgr.get_manifest(); total_kb=0
        for rec in manifest:
            self.files_tree.insert("","end", values=(
                rec["file"],f"{rec['size_kb']:.1f}",
                rec["description"],rec["saved_at"]))
            total_kb+=rec["size_kb"]
        self.files_footer.configure(
            text=f"  {len(manifest)} files  |  {total_kb:.1f} KB total  |  "
                 f"Folder: {self.output_mgr.base.resolve()}")

    # ─────────────────────────────────────────────────────────────────────────
    # GENERAL CHARTS
    # ─────────────────────────────────────────────────────────────────────────

    def _draw_chart(self):
        if not self.csv_files or not MATPLOTLIB_OK: return
        for w in self.chart_frame.winfo_children(): w.destroy()
        choice=self.chart_choice.get()
        fig=Figure(figsize=(11,5),facecolor=BG_DARK)
        try:
            if choice=="Attitude (Roll/Pitch)":          self._chart_attitude(fig)
            elif choice=="Vibration — All Axes":         self._chart_vib_all(fig)
            elif choice=="Vibration — Separate Axes":    self._chart_vib_separate(fig)
            elif choice=="GPS (HDOP & Satellites)":      self._chart_gps(fig)
            elif choice=="Battery Voltage":              self._chart_batt_volt(fig)
            elif choice=="Battery Current":              self._chart_batt_curr(fig)
            elif choice=="Altitude (Baro)":              self._chart_altitude(fig)
            elif choice=="Motor Outputs":                self._chart_motors(fig)
            elif choice=="Severity Summary":             self._chart_severity(fig)
        except Exception as exc:
            ax=fig.add_subplot(111); ax.set_facecolor(BG_DARK)
            ax.text(0.5,0.5,f"Chart error:\n{exc}",ha="center",va="center",
                    color="red",transform=ax.transAxes)
        canvas=FigureCanvasTkAgg(fig,master=self.chart_frame)
        canvas.draw(); canvas.get_tk_widget().pack(fill="both",expand=True)

    def _sax(self, ax, title="", xlabel="", ylabel=""):
        ax.set_facecolor(BG_MID); ax.tick_params(colors=FG_GREY,labelsize=8)
        ax.spines[:].set_color(BG_LIGHT)
        if title: ax.set_title(title,color=FG_WHITE,fontsize=10,pad=8)
        if xlabel: ax.set_xlabel(xlabel,color=FG_GREY,fontsize=8)
        if ylabel: ax.set_ylabel(ylabel,color=FG_GREY,fontsize=8)
        ax.grid(True,color=BG_LIGHT,linewidth=0.5,alpha=0.7)

    def _chart_attitude(self, fig):
        df=load_csv_file(self.csv_files,"ATT")
        if df is None: raise ValueError("ATT data not available")
        ax=fig.add_subplot(111); x=range(len(df))
        rc=safe_col(df,"Roll","roll"); pc=safe_col(df,"Pitch","pitch")
        if rc: ax.plot(x,df[rc],color=ACCENT_BLUE,lw=0.8,label="Roll")
        if pc: ax.plot(x,df[pc],color=ACCENT_TEAL,lw=0.8,label="Pitch")
        ax.axhline(y=45,color=VIBE_THRESH,ls="--",lw=1,alpha=0.6)
        ax.axhline(y=-45,color=VIBE_THRESH,ls="--",lw=1,alpha=0.6,label="±45° limit")
        ax.legend(facecolor=BG_MID,edgecolor=BG_LIGHT,labelcolor=FG_WHITE,fontsize=8)
        self._sax(ax,"Attitude — Roll & Pitch","Sample","Degrees")

    def _chart_vib_all(self, fig):
        df=load_csv_file(self.csv_files,"VIBE")
        if df is None: raise ValueError("VIBE data not available")
        ax=fig.add_subplot(111); x=range(len(df))
        for axis,c in [("VibeX",VIBE_X_CLR),("VibeY",VIBE_Y_CLR),("VibeZ",VIBE_Z_CLR)]:
            col=safe_col(df,axis,axis.lower())
            if col: ax.plot(x,df[col],color=c,lw=0.8,label=axis)
        ax.axhline(y=THRESHOLDS["vibe_max_ms2"],color=VIBE_THRESH,ls="--",lw=1.2,label="Critical")
        ax.axhline(y=THRESHOLDS["vibe_warn_ms2"],color=VIBE_WARN,ls=":",lw=1,label="Warning")
        ax.legend(facecolor=BG_MID,edgecolor=BG_LIGHT,labelcolor=FG_WHITE,fontsize=8)
        self._sax(ax,"Vibration — All Axes Combined","Sample","m/s²")

    def _chart_vib_separate(self, fig):
        df=load_csv_file(self.csv_files,"VIBE")
        if df is None: raise ValueError("VIBE data not available")
        axes_data=[("VibeX",VIBE_X_CLR),("VibeY",VIBE_Y_CLR),("VibeZ",VIBE_Z_CLR)]
        T=THRESHOLDS; x=np.arange(len(df))
        for i,(axis,color) in enumerate(axes_data):
            col=safe_col(df,axis,axis.lower())
            if col is None: continue
            ax=fig.add_subplot(1,3,i+1)
            data=df[col].values
            ax.fill_between(x,data,alpha=0.2,color=color)
            ax.plot(x,data,color=color,lw=0.7,label=axis)
            ax.fill_between(x,data,T["vibe_max_ms2"],
                            where=data>T["vibe_max_ms2"],alpha=0.4,color=VIBE_THRESH)
            ax.axhline(y=T["vibe_max_ms2"],color=VIBE_THRESH,ls="--",lw=1.2,alpha=0.9)
            ax.axhline(y=T["vibe_warn_ms2"],color=VIBE_WARN,ls=":",lw=1,alpha=0.8)
            s=self.vib_stats.get("axes",{}).get(axis,{})
            ax.set_title(f"{axis}\nmax={s.get('max',0):.2f}  rms={s.get('rms',0):.2f}  {s.get('status','')}",
                         color=FG_WHITE,fontsize=8)
            self._sax(ax,"","Sample","m/s²")
        fig.tight_layout(pad=2)

    def _chart_gps(self, fig):
        df=load_csv_file(self.csv_files,"GPS")
        if df is None: raise ValueError("GPS data not available")
        ax1=fig.add_subplot(121); ax2=fig.add_subplot(122); x=range(len(df))
        hd=safe_col(df,"HDop","hdop","HDOP"); ns=safe_col(df,"NSats","nsats","NumSats","Num_Sats")
        if hd: ax1.plot(x,df[hd],color=ACCENT_BLUE,lw=0.8); ax1.axhline(y=2,color=VIBE_THRESH,ls="--",lw=1)
        if ns: ax2.plot(x,df[ns],color=ACCENT_TEAL,lw=0.8); ax2.axhline(y=6,color=VIBE_THRESH,ls="--",lw=1)
        self._sax(ax1,"GPS HDOP","Sample","HDOP"); self._sax(ax2,"GPS Satellites","Sample","#Sats")
        fig.tight_layout(pad=2)

    def _chart_batt_volt(self, fig):
        df=load_csv_file(self.csv_files,"BAT") or load_csv_file(self.csv_files,"CURR")
        if df is None: raise ValueError("Battery data not available")
        vc=safe_col(df,"Volt","volt","VoltR","voltage","Vcc")
        if vc is None: raise ValueError("Voltage column not found")
        ax=fig.add_subplot(111)
        ax.plot(range(len(df)),df[vc],color=ACCENT_TEAL,lw=0.8,label="Voltage")
        ax.axhline(y=THRESHOLDS["voltage_min_v"],color=VIBE_THRESH,ls="--",lw=1,label="Min")
        ax.legend(facecolor=BG_MID,edgecolor=BG_LIGHT,labelcolor=FG_WHITE,fontsize=8)
        self._sax(ax,"Battery Voltage","Sample","Volts")

    def _chart_batt_curr(self, fig):
        df=load_csv_file(self.csv_files,"BAT") or load_csv_file(self.csv_files,"CURR")
        if df is None: raise ValueError("Battery data not available")
        cc=safe_col(df,"Curr","curr","current","Current")
        if cc is None: raise ValueError("Current column not found")
        ax=fig.add_subplot(111)
        ax.plot(range(len(df)),df[cc],color="#FF9900",lw=0.8,label="Current")
        ax.axhline(y=THRESHOLDS["current_max_a"],color=VIBE_THRESH,ls="--",lw=1,label="Max")
        ax.legend(facecolor=BG_MID,edgecolor=BG_LIGHT,labelcolor=FG_WHITE,fontsize=8)
        self._sax(ax,"Battery Current","Sample","Amps")

    def _chart_altitude(self, fig):
        df=load_csv_file(self.csv_files,"BARO")
        if df is None: raise ValueError("BARO data not available")
        ac=safe_col(df,"Alt","alt","Altitude","altitude")
        if ac is None: raise ValueError("Altitude column not found")
        ax=fig.add_subplot(111); x=range(len(df))
        ax.fill_between(x,df[ac],alpha=0.25,color=ACCENT_BLUE)
        ax.plot(x,df[ac],color=ACCENT_BLUE,lw=0.8,label="Altitude")
        ax.legend(facecolor=BG_MID,edgecolor=BG_LIGHT,labelcolor=FG_WHITE,fontsize=8)
        self._sax(ax,"Barometric Altitude","Sample","Metres")

    def _chart_motors(self, fig):
        df=load_csv_file(self.csv_files,"RCOU")
        if df is None: raise ValueError("RCOU data not available")
        ax=fig.add_subplot(111); clrs=[ACCENT_BLUE,ACCENT_TEAL,"#FF9900",VIBE_THRESH]; found=0
        for i in range(1,9):
            col=safe_col(df,f"C{i}",f"c{i}",f"Chan{i}",f"M{i}")
            if col and found<4:
                ax.plot(range(len(df)),df[col],color=clrs[found],lw=0.8,label=f"Motor {i}"); found+=1
        if found==0: raise ValueError("No motor output columns found")
        ax.legend(facecolor=BG_MID,edgecolor=BG_LIGHT,labelcolor=FG_WHITE,fontsize=8)
        self._sax(ax,"Motor PWM Outputs","Sample","µs")

    def _chart_severity(self, fig):
        if not self.events: raise ValueError("No analysis events")
        cats={}
        for e in self.events:
            if e["status"] in ("[CRITICAL]","[WARNING]"):
                cats[e["category"]]=cats.get(e["category"],0)+1
        if not cats: raise ValueError("No critical/warning events")
        ax=fig.add_subplot(111)
        labels=list(cats.keys()); vals=[cats[l] for l in labels]
        colors=[SEVERITY_COLOR["[CRITICAL]"] if
                any(e["category"]==l and e["status"]=="[CRITICAL]" for e in self.events)
                else SEVERITY_COLOR["[WARNING]"] for l in labels]
        bars=ax.barh(labels,vals,color=colors,edgecolor=BG_DARK)
        ax.bar_label(bars,padding=4,color=FG_WHITE,fontsize=9)
        self._sax(ax,"Issues by Category","Count","")
        ax.set_xlim(0,max(vals)+2)

    # ─────────────────────────────────────────────────────────────────────────
    # UTILITIES
    # ─────────────────────────────────────────────────────────────────────────

    def _log(self, msg):
        def _a():
            self.log_text.configure(state="normal")
            self.log_text.insert("end",msg+"\n")
            self.log_text.see("end")
            self.log_text.configure(state="disabled")
        self.root.after(0,_a)

    def _set_status(self, msg): self.root.after(0,lambda:self.status_var.set(msg))
    def _set_progress(self, pct): self.root.after(0,lambda:self.progress_var.set(min(100,pct)))

    def _clear_log(self):
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0","end")
        self.log_text.configure(state="disabled")

    def _save_report_txt(self):
        if not self.events: messagebox.showinfo("No Data","Run analysis first."); return
        path=filedialog.asksaveasfilename(defaultextension=".txt",
            filetypes=[("Text","*.txt"),("All","*.*")],initialfile="crash_report.txt")
        if path:
            Path(path).write_text(build_text_report(
                self.events,Path(self.bin_path_var.get()).name,
                self.verdict,self.evidence,self.vib_stats),encoding="utf-8")
            messagebox.showinfo("Saved",f"Report saved:\n{path}")

    def _copy_report(self):
        if not self.events: return
        self.root.clipboard_clear()
        self.root.clipboard_append(build_text_report(
            self.events,Path(self.bin_path_var.get()).name,
            self.verdict,self.evidence,self.vib_stats))
        messagebox.showinfo("Copied","Report copied to clipboard.")

    def _open_output_folder(self):
        folder=self.output_mgr.base if self.output_mgr else Path(OUTPUT_ROOT)
        folder.mkdir(parents=True,exist_ok=True)
        if sys.platform=="win32": os.startfile(str(folder))
        elif sys.platform=="darwin": os.system(f'open "{folder}"')
        else: os.system(f'xdg-open "{folder}"')

    def _reset(self):
        self.csv_files={}; self.events=[]; self.verdict=""
        self.evidence=[]; self.vib_stats={}; self.output_mgr=None
        self.bin_path_var.set("No file selected"); self.progress_var.set(0)
        self._set_status("Ready — select a .bin log file to begin")
        self.verdict_title.configure(text="No file loaded",fg=FG_WHITE)
        self.verdict_sub.configure(text="Load a .bin file and run analysis")
        self.verdict_icon.configure(text="⬤",fg=FG_GREY)
        for k in self.stat_cards: self.stat_cards[k].configure(text="—",fg=FG_WHITE)
        for t in (self.msg_tree,self.analysis_tree,self.files_tree,
                  self.vib_stats_tree,self.vib_clip_tree,
                  self.vib_pct_tree,self.vib_fft_tree):
            for item in t.get_children(): t.delete(item)
        for k,c in self.vib_axis_cards.items():
            c["val"].configure(text="—",fg=FG_WHITE)
            c["badge"].configure(text="—",fg=FG_GREY)
        self.report_text.configure(state="normal")
        self.report_text.delete("1.0","end")
        self.report_text.insert("1.0","Run analysis to generate the report…")
        self.report_text.configure(state="disabled")
        for cf in (self.chart_frame,self.vib_chart_frame,
                   self.vib_clip_chart_frame,self.vib_compare_frame,
                   self.vib_fft_chart_frame):
            for w in cf.winfo_children(): w.destroy()
            tk.Label(cf,text="Run analysis",bg=BG_DARK,fg=FG_GREY,
                     font=("Segoe UI",11)).pack(expand=True)
        self._clear_log()


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app = DroneLogAnalyzer()
