"""
=============================================================================
  ArduPilot .BIN Log  →  CSV Converter  +  Crash Analyzer
  Based on : https://github.com/ajaydcm/CSV_Conversion

  OUTPUT FOLDER STRUCTURE  (auto-created on every run)
  ─────────────────────────────────────────────────────
  output/
  └── <log_name>/
      ├── csv/
      │   ├── ATT.csv          Attitude  (Roll, Pitch, Yaw)
      │   ├── GPS.csv          GPS position & quality
      │   ├── IMU.csv          Accelerometer & gyroscope
      │   ├── BARO.csv         Barometer / altitude
      │   ├── VIBE.csv         Vibration levels
      │   ├── RCIN.csv         RC input channels
      │   ├── RCOU.csv         Motor PWM outputs
      │   ├── BAT.csv          Battery voltage & current
      │   ├── ERR.csv          ArduPilot error codes
      │   ├── EV.csv           Flight events (arm/crash/land)
      │   └── …                All other message types
      ├── crash_report.csv     Full analysis table (one row per parameter)
      ├── crash_report.json    Structured JSON  (verdict + all events)
      ├── crash_report.txt     Human-readable report + threshold reference
      └── summary.json         Quick-look : verdict, counts, critical categories
=============================================================================
"""

# ─────────────────────────────────────────────────────────────────────────────
# IMPORTS
# ─────────────────────────────────────────────────────────────────────────────
import os
import sys
import csv
import json
import shutil
import textwrap
import argparse
import warnings
from pathlib import Path
from datetime import datetime
from collections import defaultdict

warnings.filterwarnings("ignore")


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 0  —  DEPENDENCY CHECK
# ─────────────────────────────────────────────────────────────────────────────

def check_dependencies():
    missing = []
    for pkg in ("pymavlink", "pandas", "numpy"):
        try:
            __import__(pkg)
        except ImportError:
            missing.append(pkg)
    if missing:
        print(f"\n[ERROR] Missing Python packages: {', '.join(missing)}")
        print(f"        Fix:  pip install {' '.join(missing)}\n")
        sys.exit(1)


check_dependencies()

import pandas as pd
import numpy as np
from pymavlink import mavutil


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 1  —  OUTPUT MANAGER
#   Single class responsible for creating the folder tree and saving every file.
#   Every save is printed to console with size + full path.
# ─────────────────────────────────────────────────────────────────────────────

class OutputManager:
    """
    Creates and manages the output folder structure:

        <root>/
        └── <log_stem>/
            ├── csv/         <- raw telemetry CSVs
            ├── crash_report.csv
            ├── crash_report.json
            ├── crash_report.txt
            └── summary.json
    """

    def __init__(self, root: str, log_stem: str):
        self.root     = Path(root)
        self.log_stem = log_stem
        self.base     = self.root / log_stem   # e.g.  output/flight_001
        self.csv_dir  = self.base / "csv"      # e.g.  output/flight_001/csv

        self._manifest: list[dict] = []        # track every saved file

        # Create folders and announce them
        self._make_dir(self.root)
        self._make_dir(self.base)
        self._make_dir(self.csv_dir)

    # ── private ─────────────────────────────────────────────────────────────

    def _make_dir(self, path: Path):
        existed = path.exists()
        path.mkdir(parents=True, exist_ok=True)
        status = "EXISTS" if existed else "CREATED"
        print(f"  [DIR  {status}]  {path.resolve()}")

    def _record(self, path: Path, description: str):
        size_kb = path.stat().st_size / 1024
        entry = {
            "file":        path.name,
            "folder":      str(path.parent.resolve()),
            "full_path":   str(path.resolve()),
            "size_kb":     round(size_kb, 2),
            "description": description,
            "saved_at":    datetime.now().strftime("%H:%M:%S"),
        }
        self._manifest.append(entry)
        print(f"  [SAVED]  {path.resolve()}"
              f"  |  {size_kb:.1f} KB  |  {description}")

    # ── public save methods ──────────────────────────────────────────────────

    def save_raw_csv(self, msg_type: str, df: pd.DataFrame) -> Path:
        """Write one message-type DataFrame to  csv/<MSG_TYPE>.csv"""
        path = self.csv_dir / f"{msg_type}.csv"
        df.to_csv(path, index=False)
        self._record(path, f"Telemetry — {msg_type}  ({len(df):,} rows, "
                           f"{len(df.columns)} columns)")
        return path

    def save_crash_report_csv(self, events: list) -> Path:
        """Write the analysis event table to  crash_report.csv"""
        path = self.base / "crash_report.csv"
        fieldnames = [
            "category", "parameter", "status",
            "max_value", "threshold", "anomaly_count", "interpretation",
        ]
        with open(path, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(events)
        n_crit = sum(1 for e in events if e["status"] == "[CRITICAL]")
        n_warn = sum(1 for e in events if e["status"] == "[WARNING]")
        self._record(path, f"Crash analysis — {len(events)} checks  "
                           f"({n_crit} CRITICAL, {n_warn} WARNING)")
        return path

    def save_crash_report_json(self, payload: dict) -> Path:
        """Write the full structured payload to  crash_report.json"""
        path = self.base / "crash_report.json"
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(payload, fh, indent=2)
        self._record(path, "Full JSON — verdict + all events + thresholds")
        return path

    def save_crash_report_txt(self, text: str) -> Path:
        """Write the human-readable report to  crash_report.txt"""
        path = self.base / "crash_report.txt"
        with open(path, "w", encoding="utf-8") as fh:
            fh.write(text)
        self._record(path, "Human-readable report + threshold reference table")
        return path

    def save_summary_json(self, summary: dict) -> Path:
        """Write the quick-look summary to  summary.json"""
        path = self.base / "summary.json"
        with open(path, "w", encoding="utf-8") as fh:
            json.dump(summary, fh, indent=2)
        self._record(path, "Quick-look — verdict, counts, critical categories")
        return path

    def print_manifest(self):
        """Print a formatted manifest of every file that was saved."""
        SEP  = "═" * 72
        DASH = "─" * 72
        csv_recs   = [r for r in self._manifest if r["folder"] == str(self.csv_dir.resolve())]
        other_recs = [r for r in self._manifest if r["folder"] != str(self.csv_dir.resolve())]

        print(f"\n{SEP}")
        print(f"  OUTPUT FOLDER MANIFEST")
        print(f"  Root : {self.base.resolve()}")
        print(f"{SEP}")

        print(f"\n  📂  csv/   ({len(csv_recs)} raw telemetry files)")
        print(f"  {DASH}")
        for r in csv_recs:
            print(f"    {r['file']:<26}  {r['size_kb']:>8.1f} KB   {r['description']}")

        print(f"\n  📄  Analysis Reports  ({len(other_recs)} files)")
        print(f"  {DASH}")
        for r in other_recs:
            print(f"    {r['file']:<26}  {r['size_kb']:>8.1f} KB   {r['description']}")

        total_kb = sum(r["size_kb"] for r in self._manifest)
        print(f"\n  {'─'*40}")
        print(f"  Total files    : {len(self._manifest)}")
        print(f"  Total size     : {total_kb:.1f} KB")
        print(f"  Output folder  : {self.base.resolve()}")
        print(f"{SEP}\n")


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 2  —  BIN → CSV CONVERSION
#   Logic mirrors: https://github.com/ajaydcm/CSV_Conversion
# ─────────────────────────────────────────────────────────────────────────────

IMPORTANT_MSG_TYPES = {
    "ATT", "CTUN", "GPS", "IMU", "RCIN", "RCOU", "BARO", "CURR",
    "MODE", "ERR",  "EV",  "MSG", "PM",   "VIBE", "XKF1", "XKF4",
    "NKF1", "NKF4", "POWR", "AHR2", "MAG", "BAT", "MOTB", "TERR",
}
SKIP_MSG_TYPES = {"BAD_DATA", "FMT", "FMTU", "UNIT", "MULT", "PARM"}


def bin_to_csv(bin_path: str, output_mgr: OutputManager) -> dict:
    """
    Read every message from an ArduPilot .bin log and save each message type
    as a CSV file using OutputManager.save_raw_csv().

    Returns:
        { msg_type : csv_filepath }
    """
    bin_path = Path(bin_path)

    print(f"\n{'─'*60}")
    print(f"  STEP 1 — BIN → CSV CONVERSION")
    print(f"  Log file : {bin_path.resolve()}")
    print(f"  CSV dir  : {output_mgr.csv_dir.resolve()}")
    print(f"{'─'*60}\n")

    print(f"  [INFO] Opening ArduPilot log …")
    mlog = mavutil.mavlink_connection(str(bin_path), dialect="ardupilotmega")

    data: dict[str, list] = defaultdict(list)
    total_read = 0
    skipped    = 0

    print(f"  [INFO] Reading all messages (may take a moment for large logs) …")
    while True:
        try:
            msg = mlog.recv_match(blocking=False)
            if msg is None:
                break
            mtype = msg.get_type()
            if mtype in SKIP_MSG_TYPES:
                skipped += 1
                continue
            row = msg.to_dict()
            row.pop("mavpackettype", None)
            data[mtype].append(row)
            total_read += 1
        except Exception:
            continue

    print(f"  [INFO] Messages read    : {total_read:>10,}")
    print(f"  [INFO] Messages skipped : {skipped:>10,}  (format / meta)")
    print(f"  [INFO] Unique types     : {len(data):>10,}")
    print(f"\n  [INFO] Writing CSV files …\n")

    csv_files: dict[str, str] = {}
    important_found = []
    other_found     = []

    for mtype in sorted(data.keys()):
        rows = data[mtype]
        if not rows:
            continue
        df   = pd.DataFrame(rows)
        path = output_mgr.save_raw_csv(mtype, df)
        csv_files[mtype] = str(path)
        if mtype in IMPORTANT_MSG_TYPES:
            important_found.append(mtype)
        else:
            other_found.append(mtype)

    print(f"\n  [OK] Important types : {', '.join(sorted(important_found))}")
    if other_found:
        print(f"  [OK] Other types     : {', '.join(sorted(other_found))}")
    print(f"\n  [OK] {len(csv_files)} CSV files written.")

    return csv_files


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 3  —  CRASH ANALYSIS ENGINE
# ─────────────────────────────────────────────────────────────────────────────

THRESHOLDS = {
    # Attitude
    "roll_max_deg":          45.0,
    "pitch_max_deg":         45.0,
    "roll_rate_max_dps":     200.0,
    "pitch_rate_max_dps":    200.0,
    # Vibration
    "vibe_max_ms2":          30.0,
    "vibe_clip_max":         100,
    # GPS
    "gps_hdop_max":          2.0,
    "gps_nsats_min":         6,
    # Battery
    "voltage_min_v":         10.5,
    "current_max_a":         80.0,
    "voltage_drop_rate":     0.5,
    # Barometer
    "alt_drop_m":            5.0,
    # EKF
    "ekf_variance_max":      1.0,
    # RC
    "rc_lost_pct":           5.0,
    # Motors
    "motor_imbalance_pct":   30.0,
    "motor_min_us":          1050,
    "motor_min_frames":      20,
}

# ── Small utilities ──────────────────────────────────────────────────────────

def load_csv(csv_files: dict, msg_type: str):
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
    return {
        "category":       category,
        "parameter":      parameter,
        "status":         status,
        "max_value":      max_value,
        "threshold":      threshold,
        "anomaly_count":  anomaly_count,
        "interpretation": interpretation,
    }


def sev(condition: bool, critical: bool = False) -> str:
    if not condition:
        return "[OK]"
    return "[CRITICAL]" if critical else "[WARNING]"


# ── Individual analyzers ─────────────────────────────────────────────────────

def analyze_attitude(csv_files):
    events = []
    df = load_csv(csv_files, "ATT")
    if df is None:
        return events
    T = THRESHOLDS
    rc = safe_col(df, "Roll",  "roll")
    pc = safe_col(df, "Pitch", "pitch")

    if rc:
        mx   = df[rc].abs().max()
        over = int((df[rc].abs() > T["roll_max_deg"]).sum())
        events.append(mk("Attitude", "Roll Angle",
                         sev(over > 0, over > 50),
                         f"{mx:.1f}°", f"±{T['roll_max_deg']}°", over,
                         "Severe roll — flip / loss of control" if over > 0 else "Normal"))
        rr   = df[rc].diff().abs()
        mrr  = rr.max()
        orr  = int((rr > T["roll_rate_max_dps"]).sum())
        events.append(mk("Attitude", "Roll Angular Rate",
                         sev(orr > 0),
                         f"{mrr:.1f} °/sample", f"<{T['roll_rate_max_dps']} °/s", orr,
                         "Sudden attitude flip" if orr > 0 else "Normal"))

    if pc:
        mx   = df[pc].abs().max()
        over = int((df[pc].abs() > T["pitch_max_deg"]).sum())
        events.append(mk("Attitude", "Pitch Angle",
                         sev(over > 0, over > 50),
                         f"{mx:.1f}°", f"±{T['pitch_max_deg']}°", over,
                         "Severe pitch — nose-dive or stall" if over > 0 else "Normal"))

    return events


def analyze_vibration(csv_files):
    events = []
    df = load_csv(csv_files, "VIBE")
    if df is None:
        return events
    T = THRESHOLDS
    for axis in ("VibeX", "VibeY", "VibeZ"):
        col = safe_col(df, axis, axis.lower())
        if col is None:
            continue
        mv   = df[col].max()
        over = int((df[col] > T["vibe_max_ms2"]).sum())
        events.append(mk("Vibration", f"Vibration {axis}",
                         sev(over > 0, mv > 60),
                         f"{mv:.2f} m/s²", f"<{T['vibe_max_ms2']} m/s²", over,
                         "High vibration — motor damage / loose props" if over > 0 else "Normal"))

    for clip in ("Clip0", "Clip1", "Clip2"):
        col = safe_col(df, clip, clip.lower())
        if col is None:
            continue
        total = int(df[col].max()) if not df[col].isnull().all() else 0
        events.append(mk("Vibration", f"IMU Clipping ({clip})",
                         sev(total > T["vibe_clip_max"], True),
                         str(total), f"<{T['vibe_clip_max']} clips", total,
                         "IMU saturated — invalid attitude data" if total > T["vibe_clip_max"]
                         else "Normal"))
    return events


def analyze_gps(csv_files):
    events = []
    df = load_csv(csv_files, "GPS")
    if df is None:
        return events
    T = THRESHOLDS
    hdop  = safe_col(df, "HDop",  "hdop",  "HDOP")
    nsats = safe_col(df, "NSats", "nsats", "NumSats", "Num_Sats")
    stat  = safe_col(df, "Status","status","GpsStatus")

    if hdop:
        bad = int((df[hdop] > T["gps_hdop_max"]).sum())
        mx  = df[hdop].max()
        events.append(mk("GPS", "GPS HDOP",
                         sev(bad > 10),
                         f"max={mx:.2f}", f"<{T['gps_hdop_max']}", bad,
                         "Poor GPS accuracy — position jumps likely" if bad > 10 else "Normal"))

    if nsats:
        low  = int((df[nsats] < T["gps_nsats_min"]).sum())
        mins = int(df[nsats].min())
        events.append(mk("GPS", "GPS Satellite Count",
                         sev(low > 0, low > 50),
                         f"min={mins} sats", f">={T['gps_nsats_min']} sats", low,
                         "Loss of GPS lock — RTL / position hold may fail" if low > 0
                         else "Normal"))

    if stat:
        nf = int((df[stat] < 3).sum())
        events.append(mk("GPS", "GPS 3D Fix Status",
                         sev(nf > 0, nf > 20),
                         f"{nf} frames without 3D fix", "Fix≥3D (status≥3)", nf,
                         "GPS lost 3D fix during flight" if nf > 0 else "Normal"))
    return events


def analyze_battery(csv_files):
    events = []
    df = load_csv(csv_files, "BAT") or load_csv(csv_files, "CURR")
    if df is None:
        return events
    T    = THRESHOLDS
    vc   = safe_col(df, "Volt", "volt", "VoltR", "voltage", "Vcc")
    cc   = safe_col(df, "Curr", "curr", "current", "Current")

    if vc:
        mn  = df[vc].min()
        lv  = int((df[vc] < T["voltage_min_v"]).sum())
        events.append(mk("Battery", "Battery Voltage",
                         sev(lv > 0, lv > 50),
                         f"min={mn:.2f} V", f">={T['voltage_min_v']} V", lv,
                         "Low-voltage failsafe — battery sag / depletion" if lv > 0
                         else "Normal"))
        if len(df) > 10:
            drops  = df[vc].diff()
            worst  = drops.min()
            ndrop  = int((drops < -T["voltage_drop_rate"]).sum())
            events.append(mk("Battery", "Voltage Drop Rate",
                             sev(worst < -T["voltage_drop_rate"], True),
                             f"worst={worst:.3f} V/sample",
                             f">-{T['voltage_drop_rate']} V/sample", ndrop,
                             "Sudden voltage collapse — short circuit / battery failure"
                             if worst < -T["voltage_drop_rate"] else "Normal"))

    if cc:
        mx  = df[cc].max()
        oc  = int((df[cc] > T["current_max_a"]).sum())
        events.append(mk("Battery", "Battery Current",
                         sev(oc > 0),
                         f"max={mx:.1f} A", f"<={T['current_max_a']} A", oc,
                         "Over-current — motors overloaded" if oc > 0 else "Normal"))
    return events


def analyze_baro(csv_files):
    events = []
    df = load_csv(csv_files, "BARO")
    if df is None:
        return events
    T   = THRESHOLDS
    ac  = safe_col(df, "Alt", "alt", "Altitude", "altitude")
    if ac is None:
        return events
    diffs  = df[ac].diff()
    drops  = int((diffs < -T["alt_drop_m"]).sum())
    worst  = diffs.min()
    maxalt = df[ac].max()
    events.append(mk("Barometer", "Altitude Sudden Drop",
                     sev(drops > 0, drops > 5),
                     f"max_alt={maxalt:.1f} m  worst={worst:.2f} m/sample",
                     f"drop<{T['alt_drop_m']} m/sample", drops,
                     "Rapid altitude loss — crash / motor failure / sensor fault"
                     if drops > 0 else "Normal"))
    return events


def analyze_ekf(csv_files):
    events = []
    T = THRESHOLDS
    for msg in ("XKF4", "NKF4"):
        df = load_csv(csv_files, msg)
        if df is None:
            continue
        for col_name in ("SV", "SP", "SH", "SM", "SVT"):
            col = safe_col(df, col_name)
            if col is None:
                continue
            mv   = df[col].max()
            over = int((df[col] > T["ekf_variance_max"]).sum())
            events.append(mk("EKF", f"EKF Variance {col_name} ({msg})",
                             sev(over > 0, over > 100),
                             f"max={mv:.4f}", f"<={T['ekf_variance_max']}", over,
                             "EKF diverging — attitude / position unreliable"
                             if over > 0 else "Normal"))
        break
    return events


def analyze_rc(csv_files):
    events = []
    df = load_csv(csv_files, "RCIN")
    if df is None:
        return events
    T   = THRESHOLDS
    tc  = safe_col(df, "C3", "c3", "CH3", "Throttle", "throttle")
    if tc is None:
        return events
    lost = int((df[tc] < 900).sum())
    pct  = lost / len(df) * 100 if len(df) > 0 else 0
    events.append(mk("RC Signal", "Throttle RC Input",
                     sev(pct > T["rc_lost_pct"], True),
                     f"{pct:.1f}% frames < 900 µs  ({lost} frames)",
                     f"<{T['rc_lost_pct']}% frames lost", lost,
                     "RC signal loss / failsafe triggered"
                     if pct > T["rc_lost_pct"] else "Normal"))
    return events


def analyze_motors(csv_files):
    events = []
    df = load_csv(csv_files, "RCOU")
    if df is None:
        return events
    T      = THRESHOLDS
    motors = {}
    for i in range(1, 9):
        col = safe_col(df, f"C{i}", f"c{i}", f"Chan{i}", f"M{i}")
        if col:
            motors[f"Motor{i}"] = df[col]
        if len(motors) == 4:
            break
    if len(motors) < 2:
        return events

    means     = {k: float(v.mean()) for k, v in motors.items()}
    hi, lo    = max(means.values()), min(means.values())
    imbal     = (hi - lo) / hi * 100 if hi > 0 else 0
    events.append(mk("Motors", "Motor Output Imbalance",
                     sev(imbal > T["motor_imbalance_pct"], True),
                     f"{imbal:.1f}%  ({lo:.0f}–{hi:.0f} µs)",
                     f"<{T['motor_imbalance_pct']}%",
                     int(imbal > T["motor_imbalance_pct"]),
                     "Motor imbalance — motor / ESC failure suspected"
                     if imbal > T["motor_imbalance_pct"] else "Normal"))

    for name, series in motors.items():
        lc = int((series < T["motor_min_us"]).sum())
        if lc > T["motor_min_frames"]:
            events.append(mk("Motors", f"{name} Near Cutout",
                             "[CRITICAL]",
                             f"{lc} frames < {T['motor_min_us']} µs",
                             f"<{T['motor_min_frames']} such frames", lc,
                             f"{name} commanded near minimum — possible ESC cutout"))
    return events


def analyze_errors(csv_files):
    SUBSYS = {
        2:  ("Radio / RC",       "[CRITICAL]",
             "Radio / RC failsafe — transmitter signal lost"),
        6:  ("Failsafe Battery", "[CRITICAL]",
             "Battery failsafe — low voltage / capacity"),
        7:  ("Failsafe GPS",     "[CRITICAL]",
             "GPS failsafe — signal lost or too poor to navigate"),
        12: ("Crash Check",      "[CRITICAL]",
             "CRASH DETECTED — sudden attitude change triggered crash check"),
        13: ("Flip Detected",    "[CRITICAL]",
             "Flip detected — uncontrolled attitude rotation"),
        16: ("EKF / DCM Check",  "[CRITICAL]",
             "EKF / DCM check FAILED — attitude estimate unreliable"),
        17: ("Barometer",        "[CRITICAL]",
             "Barometer failure — altitude hold lost"),
        3:  ("Compass",          "[WARNING]",
             "Compass inconsistency — magnetic interference"),
        8:  ("Failsafe GCS",     "[WARNING]",
             "GCS (ground-station) link lost"),
        18: ("CPU",              "[WARNING]",
             "CPU overloaded — possible timing issues"),
    }
    events = []
    df = load_csv(csv_files, "ERR")
    if df is None:
        return events
    sc = safe_col(df, "Subsys", "subsys", "SubSystem", "subsystem")
    ec = safe_col(df, "ECode",  "ecode",  "ErrorCode",  "error_code")
    if sc is None:
        return events

    for _, row in df.iterrows():
        sid   = int(row.get(sc, 0))
        ecode = int(row.get(ec, 0)) if ec else 0
        if ecode == 0:
            continue
        if sid in SUBSYS:
            sname, status, interp = SUBSYS[sid]
        else:
            sname, status, interp = (f"Subsystem_{sid}", "[WARNING]",
                                     f"Error ECode={ecode} on subsystem {sid}")
        events.append(mk("Error Codes", f"ERR — {sname}",
                         status, f"ECode={ecode}", "ECode=0 (no error)", 1, interp))
    return events


def analyze_events(csv_files):
    EV_MAP = {
        10: ("Armed",         "[INFO]",     "Vehicle armed"),
        11: ("Disarmed",      "[INFO]",     "Vehicle disarmed"),
        15: ("Auto Armed",    "[INFO]",     "Auto-arm successful"),
        16: ("Takeoff",       "[INFO]",     "Takeoff initiated"),
        18: ("Land Complete", "[INFO]",     "Landing complete"),
        25: ("CRASH",         "[CRITICAL]", "CRASH EVENT LOGGED by autopilot!"),
        28: ("Land Maybe",    "[INFO]",     "Possible landing detected"),
    }
    events = []
    df = load_csv(csv_files, "EV")
    if df is None:
        return events
    ic = safe_col(df, "Id", "id", "event_id", "EventId")
    if ic is None:
        return events
    for _, row in df.iterrows():
        eid = int(row.get(ic, 0))
        if eid not in EV_MAP:
            continue
        name, status, interp = EV_MAP[eid]
        events.append(mk("Events", f"Flight Event — {name}",
                         status, f"EventID={eid}", "N/A",
                         1 if eid == 25 else 0, interp))
    return events


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 4  —  REPORT BUILDER
# ─────────────────────────────────────────────────────────────────────────────

SEVERITY_RANK  = {"[CRITICAL]": 0, "[WARNING]": 1, "[INFO]": 2, "[OK]": 3}
CRASH_KEYWORDS = [
    "crash", "flip", "loss of control", "altitude loss", "freefall",
    "motor failure", "ekf", "voltage collapse", "rc signal loss", "cutout",
    "failsafe", "nose-dive",
]


def run_all_analyzers(csv_files: dict) -> list:
    all_events = []
    for fn in (analyze_attitude, analyze_vibration, analyze_gps,
               analyze_battery, analyze_baro, analyze_ekf,
               analyze_rc, analyze_motors, analyze_errors, analyze_events):
        try:
            all_events.extend(fn(csv_files))
        except Exception as exc:
            print(f"  [WARN] {fn.__name__} failed: {exc}")
    all_events.sort(key=lambda e: SEVERITY_RANK.get(e["status"], 4))
    return all_events


def determine_verdict(events: list):
    criticals = [e for e in events if e["status"] == "[CRITICAL]"]
    warnings  = [e for e in events if e["status"] == "[WARNING]"]
    evidence  = [e for e in criticals if any(
        kw in e["interpretation"].lower() for kw in CRASH_KEYWORDS)]

    if evidence:
        verdict = "CRASH LIKELY — Critical anomalies matching crash patterns"
    elif len(criticals) >= 2:
        verdict = "PROBABLE INCIDENT — Multiple critical parameters exceeded"
    elif criticals:
        verdict = "POSSIBLE INCIDENT — Single critical anomaly found"
    elif warnings:
        verdict = "CAUTION — Warnings detected, review recommended"
    else:
        verdict = "NO CRASH INDICATORS — Flight appears nominal"

    return verdict, evidence


def build_text_report(events: list, log_name: str, verdict: str, evidence: list) -> str:
    ts   = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    SEP  = "=" * 78
    DASH = "-" * 78
    n_crit = sum(1 for e in events if e["status"] == "[CRITICAL]")
    n_warn = sum(1 for e in events if e["status"] == "[WARNING]")
    n_ok   = sum(1 for e in events if e["status"] == "[OK]")

    lines = [
        SEP,
        "  ARDUPILOT CRASH ANALYSIS REPORT",
        f"  Log File   : {log_name}",
        f"  Generated  : {ts}",
        f"  Total checks: {len(events)}  |  CRITICAL: {n_crit}  |  WARNING: {n_warn}  |  OK: {n_ok}",
        SEP,
        "",
        f"  VERDICT  :  {verdict}",
        "",
    ]

    if evidence:
        lines += [DASH, "  KEY CRASH EVIDENCE", DASH]
        for e in evidence:
            lines += [
                f"  ⚠  [{e['category']}]  {e['parameter']}",
                f"     Interpretation : {e['interpretation']}",
                f"     Observed value : {e['max_value']}",
                f"     Threshold      : {e['threshold']}",
                f"     Anomaly count  : {e['anomaly_count']}",
                "",
            ]

    lines += [
        DASH,
        f"  DETAILED PARAMETER ANALYSIS  (OK rows suppressed)",
        DASH,
        f"  {'CATEGORY':<15}  {'PARAMETER':<34}  {'STATUS':<12}  {'VALUE / THRESHOLD':<30}  INTERPRETATION",
        f"  {'─'*15}  {'─'*34}  {'─'*12}  {'─'*30}  {'─'*40}",
    ]

    prev_cat = None
    for e in events:
        if e["status"] == "[OK]":
            continue
        if e["category"] != prev_cat:
            lines.append("")
            lines.append(f"  ── {e['category']}")
            prev_cat = e["category"]
        val   = f"{e['max_value'][:28]}  (thr: {e['threshold'][:18]})"
        lines.append(
            f"  {e['category']:<15}  {e['parameter']:<34}  {e['status']:<12}  "
            f"{val:<50}  {e['interpretation'][:65]}"
        )

    lines += ["", SEP, ""]

    # ── Threshold reference table ─────────────────────────────────────────
    lines += [
        "  THRESHOLD REFERENCE TABLE",
        DASH,
        f"  {'Parameter':<42}  {'Safe Threshold':<24}  Crash Relevance",
        f"  {'─'*42}  {'─'*24}  {'─'*35}",
    ]
    ref = [
        ("Roll / Pitch angle",              "±45°",               "Flip / loss of control"),
        ("Roll / Pitch angular rate",        "<200 °/s",           "Sudden attitude flip"),
        ("Vibration VibeX / Y / Z",         "<30 m/s²",           "Motor damage / resonance"),
        ("IMU clip count per axis",         "<100 clips",         "Accelerometer saturation"),
        ("GPS HDOP",                        "<2.0",               "Poor positional accuracy"),
        ("GPS satellite count",             "≥6 satellites",      "Risk of GPS lock loss"),
        ("Battery pack voltage",            "≥10.5 V",            "Low-voltage failsafe"),
        ("Voltage drop rate",               "<0.5 V/sample",      "Battery collapse / short circuit"),
        ("Battery current draw",            "≤80 A",              "Motor / ESC overload"),
        ("Barometer altitude drop",         "<5 m/sample",        "Freefall / crash event"),
        ("EKF innovation variance",         "≤1.0",               "Attitude estimate invalid"),
        ("RC throttle signal loss",         "<5% frames < 900µs", "RC failsafe / link dropout"),
        ("Motor PWM imbalance",             "<30% between motors","Motor / ESC failure"),
        ("Motor near-cutout PWM frames",    "<20 frames < 1050µs","Motor / ESC cutout"),
    ]
    for param, thresh, reason in ref:
        lines.append(f"  {param:<42}  {thresh:<24}  {reason}")

    lines += ["", SEP, ""]
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 5  —  SAVE ALL OUTPUTS  (orchestrator)
# ─────────────────────────────────────────────────────────────────────────────

def save_all_outputs(
    output_mgr: OutputManager,
    events:     list,
    csv_files:  dict,
    log_name:   str,
    verdict:    str,
    evidence:   list,
):
    """
    Save every output file via OutputManager — every save is logged to console.

    Files created:
      1.  csv/<MSG>.csv           (already written in bin_to_csv)
      2.  crash_report.csv        analysis table
      3.  crash_report.json       structured JSON
      4.  crash_report.txt        human-readable report + threshold table
      5.  summary.json            quick-look verdict + stats
    """
    print(f"\n{'─'*60}")
    print(f"  STEP 3 — SAVING ALL OUTPUT FILES")
    print(f"  Destination : {output_mgr.base.resolve()}")
    print(f"{'─'*60}\n")

    # Ensure any raw CSVs not yet in csv/ are copied there
    for mtype, src in csv_files.items():
        dst = output_mgr.csv_dir / f"{mtype}.csv"
        if not dst.exists():
            shutil.copy2(src, dst)

    n_crit = sum(1 for e in events if e["status"] == "[CRITICAL]")
    n_warn = sum(1 for e in events if e["status"] == "[WARNING]")
    n_ok   = sum(1 for e in events if e["status"] == "[OK]")

    # ── crash_report.csv ───────────────────────────────────────────────────
    output_mgr.save_crash_report_csv(events)

    # ── crash_report.json ──────────────────────────────────────────────────
    payload = {
        "log_file":   log_name,
        "generated":  datetime.now().isoformat(),
        "verdict":    verdict,
        "statistics": {
            "total_checks": len(events),
            "critical":     n_crit,
            "warning":      n_warn,
            "ok":           n_ok,
        },
        "crash_evidence":   evidence,
        "events":           events,
        "thresholds_used":  THRESHOLDS,
    }
    output_mgr.save_crash_report_json(payload)

    # ── crash_report.txt ───────────────────────────────────────────────────
    text = build_text_report(events, log_name, verdict, evidence)
    output_mgr.save_crash_report_txt(text)

    # ── summary.json ───────────────────────────────────────────────────────
    summary = {
        "log_file":            log_name,
        "generated":           datetime.now().isoformat(),
        "verdict":             verdict,
        "critical_count":      n_crit,
        "warning_count":       n_warn,
        "ok_count":            n_ok,
        "critical_categories": sorted({e["category"] for e in events
                                       if e["status"] == "[CRITICAL]"}),
        "csv_message_types":   sorted(csv_files.keys()),
        "output_folder":       str(output_mgr.base.resolve()),
    }
    output_mgr.save_summary_json(summary)

    # ── Print full manifest ─────────────────────────────────────────────────
    output_mgr.print_manifest()


# ─────────────────────────────────────────────────────────────────────────────
# SECTION 6  —  MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        prog="crash_analyzer.py",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        description=textwrap.dedent("""\
            ArduPilot .BIN → CSV Converter + Crash Analyzer
            Based on: https://github.com/ajaydcm/CSV_Conversion

            Automatically creates output folder:

              output/
              └── <log_name>/
                  ├── csv/                ← one CSV per telemetry message type
                  ├── crash_report.csv    ← full analysis table
                  ├── crash_report.json   ← structured JSON
                  ├── crash_report.txt    ← human-readable report
                  └── summary.json        ← quick-look verdict + stats
        """),
    )
    parser.add_argument("bin_file",
                        help="Path to the ArduPilot .bin log file")
    parser.add_argument("-o", "--output_dir", default="output", metavar="DIR",
                        help="Root output directory  (default: ./output)")
    parser.add_argument("--skip-convert", action="store_true",
                        help="Skip BIN→CSV step if CSVs already exist")
    args = parser.parse_args()

    bin_file = Path(args.bin_file)
    if not bin_file.exists():
        print(f"\n[ERROR] File not found: {bin_file.resolve()}\n")
        sys.exit(1)

    ts_start = datetime.now()
    SEP = "═" * 60
    print(f"\n{SEP}")
    print(f"  ArduPilot .BIN Crash Analyzer")
    print(f"  Log     : {bin_file.resolve()}")
    print(f"  Output  : {Path(args.output_dir).resolve() / bin_file.stem}")
    print(f"  Started : {ts_start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"{SEP}")

    # ── Create output folder structure ─────────────────────────────────────
    print(f"\n  Creating output folder structure …\n")
    output_mgr = OutputManager(root=args.output_dir, log_stem=bin_file.stem)

    # ── STEP 1 : BIN → CSV ─────────────────────────────────────────────────
    csv_dir = output_mgr.csv_dir
    if args.skip_convert and any(csv_dir.glob("*.csv")):
        print(f"\n  [INFO] --skip-convert: reusing CSVs from {csv_dir}")
        csv_files = {p.stem: str(p) for p in csv_dir.glob("*.csv")}
        print(f"  [INFO] {len(csv_files)} existing CSV files loaded")
    else:
        csv_files = bin_to_csv(str(bin_file), output_mgr)

    if not csv_files:
        print("\n[ERROR] No telemetry data could be extracted.\n")
        sys.exit(1)

    # ── STEP 2 : Crash analysis ─────────────────────────────────────────────
    print(f"\n{'─'*60}")
    print(f"  STEP 2 — CRASH ANALYSIS")
    print(f"{'─'*60}\n")
    events  = run_all_analyzers(csv_files)
    verdict, evidence = determine_verdict(events)

    n_crit = sum(1 for e in events if e["status"] == "[CRITICAL]")
    n_warn = sum(1 for e in events if e["status"] == "[WARNING]")
    print(f"  Parameters checked  : {len(events)}")
    print(f"  CRITICAL findings   : {n_crit}")
    print(f"  WARNING  findings   : {n_warn}")

    # Print full report to console
    print(build_text_report(events, bin_file.name, verdict, evidence))

    # ── STEP 3 : Save all outputs ───────────────────────────────────────────
    save_all_outputs(output_mgr, events, csv_files,
                     bin_file.name, verdict, evidence)

    elapsed = (datetime.now() - ts_start).total_seconds()
    print(f"  Completed in {elapsed:.1f} s")
    print(f"  VERDICT : {verdict}\n")

    sys.exit(1 if "CRASH LIKELY" in verdict else 0)


if __name__ == "__main__":
    main()
