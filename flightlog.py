"""
=============================================================================
  DroneLogAnalyzer  —  ArduPilot .BIN Log GUI Analyzer
  Based on : https://github.com/ajaydcm/CSV_Conversion

  Features:
    • Load .bin or .ulg ArduPilot / PX4 log files
    • Convert to CSV (one file per message type)
    • Crash analysis across 10 parameter categories
    • Full GUI with tabbed interface (Overview, Charts, Report, Files)
    • Auto-saves all output to  output/<log_name>/
=============================================================================
"""

# ─────────────────────────────────────────────────────────────────────────────
# STANDARD LIBRARY IMPORTS
# ─────────────────────────────────────────────────────────────────────────────
import os
import sys
import csv
import json
import shutil
import threading
import warnings
import traceback
from pathlib import Path
from datetime import datetime
from collections import defaultdict

warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────────────────────
# TKINTER IMPORTS
# ─────────────────────────────────────────────────────────────────────────────
import tkinter as tk
from tkinter import ttk, filedialog, messagebox, scrolledtext

# ─────────────────────────────────────────────────────────────────────────────
# THIRD-PARTY IMPORTS  (checked at runtime)
# ─────────────────────────────────────────────────────────────────────────────
def _check_deps():
    missing = []
    for pkg in ("pymavlink", "pandas", "numpy", "matplotlib"):
        try:
            __import__(pkg)
        except ImportError:
            missing.append(pkg)
    if missing:
        messagebox.showerror(
            "Missing Dependencies",
            f"Please install missing packages:\n\npip install {' '.join(missing)}"
        )
        sys.exit(1)

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
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
    from matplotlib.figure import Figure
    MATPLOTLIB_OK = True
except ImportError:
    MATPLOTLIB_OK = False


# ─────────────────────────────────────────────────────────────────────────────
# CONSTANTS & THRESHOLDS
# ─────────────────────────────────────────────────────────────────────────────

APP_TITLE   = "DroneLogAnalyzer — ArduPilot Crash Inspector"
APP_VERSION = "2.0"
OUTPUT_ROOT = "output"

THRESHOLDS = {
    "roll_max_deg":        45.0,
    "pitch_max_deg":       45.0,
    "roll_rate_max_dps":   200.0,
    "pitch_rate_max_dps":  200.0,
    "vibe_max_ms2":        30.0,
    "vibe_clip_max":       100,
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
    "ATT", "CTUN", "GPS", "IMU", "RCIN", "RCOU", "BARO", "CURR",
    "MODE", "ERR",  "EV",  "MSG", "PM",   "VIBE", "XKF1", "XKF4",
    "NKF1", "NKF4", "POWR", "AHR2", "MAG", "BAT", "MOTB", "TERR",
}
SKIP_MSG_TYPES = {"BAD_DATA", "FMT", "FMTU", "UNIT", "MULT", "PARM"}

SEVERITY_RANK  = {"[CRITICAL]": 0, "[WARNING]": 1, "[INFO]": 2, "[OK]": 3}
SEVERITY_COLOR = {
    "[CRITICAL]": "#FF4444",
    "[WARNING]":  "#FF9900",
    "[INFO]":     "#3399FF",
    "[OK]":       "#44BB44",
}
CRASH_KEYWORDS = [
    "crash", "flip", "loss of control", "altitude loss", "freefall",
    "motor failure", "ekf", "voltage collapse", "rc signal loss",
    "cutout", "failsafe", "nose-dive",
]

# GUI color palette
BG_DARK     = "#1E1E2E"
BG_MID      = "#2A2A3E"
BG_LIGHT    = "#313145"
FG_WHITE    = "#EEEEF5"
FG_GREY     = "#AAAACC"
ACCENT_BLUE = "#5B8DEF"
ACCENT_TEAL = "#3EC9A7"


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
            "file":        path.name,
            "folder":      str(path.parent.resolve()),
            "full_path":   str(path.resolve()),
            "size_kb":     round(size_kb, 2),
            "description": description,
            "saved_at":    datetime.now().strftime("%H:%M:%S"),
        })

    def save_raw_csv(self, msg_type: str, df: pd.DataFrame) -> Path:
        path = self.csv_dir / f"{msg_type}.csv"
        df.to_csv(path, index=False)
        self._record(path, f"Telemetry — {msg_type} ({len(df):,} rows)")
        return path

    def save_crash_report_csv(self, events: list) -> Path:
        path = self.base / "crash_report.csv"
        fieldnames = ["category","parameter","status","max_value",
                      "threshold","anomaly_count","interpretation"]
        with open(path, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=fieldnames)
            w.writeheader()
            w.writerows(events)
        self._record(path, f"Crash analysis — {len(events)} checks")
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
# ANALYSIS ENGINE
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
    if not condition:
        return "[OK]"
    return "[CRITICAL]" if critical else "[WARNING]"


def analyze_attitude(csv_files):
    events = []
    df = load_csv_file(csv_files, "ATT")
    if df is None:
        return events
    T = THRESHOLDS
    rc = safe_col(df, "Roll", "roll")
    pc = safe_col(df, "Pitch", "pitch")
    if rc:
        mx   = df[rc].abs().max()
        over = int((df[rc].abs() > T["roll_max_deg"]).sum())
        events.append(mk("Attitude","Roll Angle",sev(over>0,over>50),
            f"{mx:.1f}°",f"±{T['roll_max_deg']}°",over,
            "Severe roll — flip / loss of control" if over>0 else "Normal"))
        rr  = df[rc].diff().abs()
        orr = int((rr > T["roll_rate_max_dps"]).sum())
        events.append(mk("Attitude","Roll Angular Rate",sev(orr>0),
            f"{rr.max():.1f} °/sample",f"<{T['roll_rate_max_dps']} °/s",orr,
            "Sudden attitude flip" if orr>0 else "Normal"))
    if pc:
        mx   = df[pc].abs().max()
        over = int((df[pc].abs() > T["pitch_max_deg"]).sum())
        events.append(mk("Attitude","Pitch Angle",sev(over>0,over>50),
            f"{mx:.1f}°",f"±{T['pitch_max_deg']}°",over,
            "Severe pitch — nose-dive or stall" if over>0 else "Normal"))
    return events


def analyze_vibration(csv_files):
    events = []
    df = load_csv_file(csv_files, "VIBE")
    if df is None:
        return events
    T = THRESHOLDS
    for axis in ("VibeX","VibeY","VibeZ"):
        col = safe_col(df, axis, axis.lower())
        if col is None:
            continue
        mv   = df[col].max()
        over = int((df[col] > T["vibe_max_ms2"]).sum())
        events.append(mk("Vibration",f"Vibration {axis}",sev(over>0,mv>60),
            f"{mv:.2f} m/s²",f"<{T['vibe_max_ms2']} m/s²",over,
            "High vibration — motor damage / loose props" if over>0 else "Normal"))
    for clip in ("Clip0","Clip1","Clip2"):
        col = safe_col(df, clip, clip.lower())
        if col is None:
            continue
        total = int(df[col].max()) if not df[col].isnull().all() else 0
        events.append(mk("Vibration",f"IMU Clipping ({clip})",
            sev(total>T["vibe_clip_max"],True),str(total),
            f"<{T['vibe_clip_max']} clips",total,
            "IMU saturated — invalid attitude data" if total>T["vibe_clip_max"] else "Normal"))
    return events


def analyze_gps(csv_files):
    events = []
    df = load_csv_file(csv_files, "GPS")
    if df is None:
        return events
    T = THRESHOLDS
    hdop  = safe_col(df,"HDop","hdop","HDOP")
    nsats = safe_col(df,"NSats","nsats","NumSats","Num_Sats")
    stat  = safe_col(df,"Status","status","GpsStatus")
    if hdop:
        bad = int((df[hdop] > T["gps_hdop_max"]).sum())
        events.append(mk("GPS","GPS HDOP",sev(bad>10),
            f"max={df[hdop].max():.2f}",f"<{T['gps_hdop_max']}",bad,
            "Poor GPS accuracy — position jumps likely" if bad>10 else "Normal"))
    if nsats:
        low = int((df[nsats] < T["gps_nsats_min"]).sum())
        events.append(mk("GPS","GPS Satellite Count",sev(low>0,low>50),
            f"min={int(df[nsats].min())} sats",f">={T['gps_nsats_min']} sats",low,
            "Loss of GPS lock" if low>0 else "Normal"))
    if stat:
        nf = int((df[stat] < 3).sum())
        events.append(mk("GPS","GPS 3D Fix Status",sev(nf>0,nf>20),
            f"{nf} frames without 3D fix","Fix≥3D",nf,
            "GPS lost 3D fix during flight" if nf>0 else "Normal"))
    return events


def analyze_battery(csv_files):
    events = []
    df = load_csv_file(csv_files,"BAT") or load_csv_file(csv_files,"CURR")
    if df is None:
        return events
    T  = THRESHOLDS
    vc = safe_col(df,"Volt","volt","VoltR","voltage","Vcc")
    cc = safe_col(df,"Curr","curr","current","Current")
    if vc:
        mn = df[vc].min()
        lv = int((df[vc] < T["voltage_min_v"]).sum())
        events.append(mk("Battery","Battery Voltage",sev(lv>0,lv>50),
            f"min={mn:.2f} V",f">={T['voltage_min_v']} V",lv,
            "Low-voltage failsafe — battery sag" if lv>0 else "Normal"))
        if len(df) > 10:
            drops = df[vc].diff()
            worst = drops.min()
            ndrop = int((drops < -T["voltage_drop_rate"]).sum())
            events.append(mk("Battery","Voltage Drop Rate",
                sev(worst < -T["voltage_drop_rate"],True),
                f"worst={worst:.3f} V/sample",
                f">-{T['voltage_drop_rate']} V/sample",ndrop,
                "Sudden voltage collapse" if worst < -T["voltage_drop_rate"] else "Normal"))
    if cc:
        mx = df[cc].max()
        oc = int((df[cc] > T["current_max_a"]).sum())
        events.append(mk("Battery","Battery Current",sev(oc>0),
            f"max={mx:.1f} A",f"<={T['current_max_a']} A",oc,
            "Over-current — motors overloaded" if oc>0 else "Normal"))
    return events


def analyze_baro(csv_files):
    events = []
    df = load_csv_file(csv_files,"BARO")
    if df is None:
        return events
    T  = THRESHOLDS
    ac = safe_col(df,"Alt","alt","Altitude","altitude")
    if ac is None:
        return events
    diffs  = df[ac].diff()
    drops  = int((diffs < -T["alt_drop_m"]).sum())
    worst  = diffs.min()
    maxalt = df[ac].max()
    events.append(mk("Barometer","Altitude Sudden Drop",sev(drops>0,drops>5),
        f"max_alt={maxalt:.1f} m  worst={worst:.2f} m/sample",
        f"drop<{T['alt_drop_m']} m/sample",drops,
        "Rapid altitude loss — crash / motor failure" if drops>0 else "Normal"))
    return events


def analyze_ekf(csv_files):
    events = []
    T = THRESHOLDS
    for msg in ("XKF4","NKF4"):
        df = load_csv_file(csv_files, msg)
        if df is None:
            continue
        for col_name in ("SV","SP","SH","SM","SVT"):
            col = safe_col(df, col_name)
            if col is None:
                continue
            mv   = df[col].max()
            over = int((df[col] > T["ekf_variance_max"]).sum())
            events.append(mk("EKF",f"EKF Variance {col_name} ({msg})",
                sev(over>0,over>100),f"max={mv:.4f}",
                f"<={T['ekf_variance_max']}",over,
                "EKF diverging — attitude unreliable" if over>0 else "Normal"))
        break
    return events


def analyze_rc(csv_files):
    events = []
    df = load_csv_file(csv_files,"RCIN")
    if df is None:
        return events
    T  = THRESHOLDS
    tc = safe_col(df,"C3","c3","CH3","Throttle","throttle")
    if tc is None:
        return events
    lost = int((df[tc] < 900).sum())
    pct  = lost / len(df) * 100 if len(df) > 0 else 0
    events.append(mk("RC Signal","Throttle RC Input",sev(pct>T["rc_lost_pct"],True),
        f"{pct:.1f}% frames < 900 µs ({lost} frames)",
        f"<{T['rc_lost_pct']}% frames lost",lost,
        "RC signal loss / failsafe triggered" if pct>T["rc_lost_pct"] else "Normal"))
    return events


def analyze_motors(csv_files):
    events = []
    df = load_csv_file(csv_files,"RCOU")
    if df is None:
        return events
    T      = THRESHOLDS
    motors = {}
    for i in range(1, 9):
        col = safe_col(df,f"C{i}",f"c{i}",f"Chan{i}",f"M{i}")
        if col:
            motors[f"Motor{i}"] = df[col]
        if len(motors) == 4:
            break
    if len(motors) < 2:
        return events
    means = {k: float(v.mean()) for k, v in motors.items()}
    hi, lo = max(means.values()), min(means.values())
    imbal  = (hi - lo) / hi * 100 if hi > 0 else 0
    events.append(mk("Motors","Motor Output Imbalance",
        sev(imbal>T["motor_imbalance_pct"],True),
        f"{imbal:.1f}%  ({lo:.0f}–{hi:.0f} µs)",
        f"<{T['motor_imbalance_pct']}%",
        int(imbal>T["motor_imbalance_pct"]),
        "Motor imbalance — ESC failure suspected" if imbal>T["motor_imbalance_pct"] else "Normal"))
    for name, series in motors.items():
        lc = int((series < T["motor_min_us"]).sum())
        if lc > T["motor_min_frames"]:
            events.append(mk("Motors",f"{name} Near Cutout","[CRITICAL]",
                f"{lc} frames < {T['motor_min_us']} µs",
                f"<{T['motor_min_frames']} such frames",lc,
                f"{name} near minimum — possible ESC cutout"))
    return events


def analyze_errors(csv_files):
    SUBSYS = {
        2:  ("Radio / RC",      "[CRITICAL]","Radio failsafe — RC signal lost"),
        6:  ("Battery",         "[CRITICAL]","Battery failsafe — low voltage"),
        7:  ("GPS",             "[CRITICAL]","GPS failsafe — signal lost"),
        12: ("Crash Check",     "[CRITICAL]","CRASH DETECTED by autopilot"),
        13: ("Flip",            "[CRITICAL]","Flip detected — uncontrolled rotation"),
        16: ("EKF / DCM",       "[CRITICAL]","EKF check FAILED — attitude unreliable"),
        17: ("Barometer",       "[CRITICAL]","Barometer failure — altitude hold lost"),
        3:  ("Compass",         "[WARNING]", "Compass inconsistency — mag interference"),
        8:  ("GCS",             "[WARNING]", "Ground-station link lost"),
        18: ("CPU",             "[WARNING]", "CPU overloaded"),
    }
    events = []
    df = load_csv_file(csv_files,"ERR")
    if df is None:
        return events
    sc = safe_col(df,"Subsys","subsys","SubSystem","subsystem")
    ec = safe_col(df,"ECode","ecode","ErrorCode","error_code")
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
            sname, status, interp = (f"Subsystem_{sid}","[WARNING]",
                                     f"Error ECode={ecode} subsystem {sid}")
        events.append(mk("Error Codes",f"ERR — {sname}",status,
            f"ECode={ecode}","ECode=0",1,interp))
    return events


def analyze_events(csv_files):
    EV_MAP = {
        10: ("Armed",        "[INFO]",     "Vehicle armed"),
        11: ("Disarmed",     "[INFO]",     "Vehicle disarmed"),
        15: ("Auto Armed",   "[INFO]",     "Auto-arm successful"),
        16: ("Takeoff",      "[INFO]",     "Takeoff initiated"),
        18: ("Land Complete","[INFO]",     "Landing complete"),
        25: ("CRASH",        "[CRITICAL]", "CRASH EVENT LOGGED by autopilot!"),
        28: ("Land Maybe",   "[INFO]",     "Possible landing detected"),
    }
    events = []
    df = load_csv_file(csv_files,"EV")
    if df is None:
        return events
    ic = safe_col(df,"Id","id","event_id","EventId")
    if ic is None:
        return events
    for _, row in df.iterrows():
        eid = int(row.get(ic, 0))
        if eid not in EV_MAP:
            continue
        name, status, interp = EV_MAP[eid]
        events.append(mk("Events",f"Flight Event — {name}",status,
            f"EventID={eid}","N/A",1 if eid==25 else 0,interp))
    return events


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


def build_text_report(events, log_name, verdict, evidence):
    ts    = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    SEP   = "=" * 78
    DASH  = "-" * 78
    nc    = sum(1 for e in events if e["status"]=="[CRITICAL]")
    nw    = sum(1 for e in events if e["status"]=="[WARNING]")
    no    = sum(1 for e in events if e["status"]=="[OK]")
    lines = [
        SEP,
        "  ARDUPILOT CRASH ANALYSIS REPORT",
        f"  Log File   : {log_name}",
        f"  Generated  : {ts}",
        f"  Checks: {len(events)}  |  CRITICAL: {nc}  |  WARNING: {nw}  |  OK: {no}",
        SEP,"",
        f"  VERDICT  :  {verdict}","",
    ]
    if evidence:
        lines += [DASH,"  KEY CRASH EVIDENCE",DASH]
        for e in evidence:
            lines += [
                f"  ⚠  [{e['category']}]  {e['parameter']}",
                f"     {e['interpretation']}",
                f"     Value: {e['max_value']}   Threshold: {e['threshold']}","",
            ]
    lines += [DASH,"  DETAILED ANALYSIS",DASH,
        f"  {'CATEGORY':<14} {'PARAMETER':<32} {'STATUS':<12} DETAILS",
        f"  {'-'*14} {'-'*32} {'-'*12} {'-'*40}"]
    prev_cat = None
    for e in events:
        if e["status"] == "[OK]":
            continue
        if e["category"] != prev_cat:
            lines.append(f"\n  ── {e['category']}")
            prev_cat = e["category"]
        lines.append(
            f"  {e['category']:<14} {e['parameter']:<32} {e['status']:<12} "
            f"{e['max_value'][:30]}  →  {e['interpretation'][:55]}")
    lines += ["",SEP,""]
    lines += ["  THRESHOLD REFERENCE",DASH,
        f"  {'Parameter':<38}  {'Threshold':<22}  Crash Relevance",
        f"  {'-'*38}  {'-'*22}  {'-'*30}"]
    for p,t,r in [
        ("Roll / Pitch angle",         "±45°",               "Flip / loss of control"),
        ("Roll / Pitch angular rate",  "<200 °/s",           "Sudden attitude flip"),
        ("Vibration VibeX/Y/Z",        "<30 m/s²",           "Motor damage / resonance"),
        ("IMU clip count",             "<100 clips",         "Accelerometer saturation"),
        ("GPS HDOP",                   "<2.0",               "Poor positional accuracy"),
        ("GPS satellite count",        ">=6 satellites",     "Risk of GPS lock loss"),
        ("Battery voltage",            ">=10.5 V",           "Low-voltage failsafe"),
        ("Voltage drop rate",          "<0.5 V/sample",      "Battery collapse / short"),
        ("Battery current",            "<=80 A",             "Motor / ESC overload"),
        ("Barometer altitude drop",    "<5 m/sample",        "Freefall / crash event"),
        ("EKF innovation variance",    "<=1.0",              "Attitude estimate invalid"),
        ("RC throttle signal",         "<5% frames lost",    "RC failsafe / link loss"),
        ("Motor PWM imbalance",        "<30%",               "Motor / ESC failure"),
    ]:
        lines.append(f"  {p:<38}  {t:<22}  {r}")
    lines += ["",SEP,""]
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# BIN → CSV CONVERSION
# ─────────────────────────────────────────────────────────────────────────────

def bin_to_csv(bin_path: str, output_mgr: OutputManager,
               progress_cb=None, log_cb=None) -> dict:
    """Convert .bin log to CSVs. progress_cb(pct), log_cb(msg)."""
    def log(msg):
        if log_cb:
            log_cb(msg)
        print(msg)

    bin_path = Path(bin_path)
    log(f"Opening: {bin_path.name}")

    if not PYMAVLINK_OK:
        log("[ERROR] pymavlink not installed — cannot read .bin files")
        return {}

    mlog = mavutil.mavlink_connection(str(bin_path), dialect="ardupilotmega")
    data: dict[str, list] = defaultdict(list)
    total_read = 0

    log("Reading messages…")
    while True:
        try:
            msg = mlog.recv_match(blocking=False)
            if msg is None:
                break
            mtype = msg.get_type()
            if mtype in SKIP_MSG_TYPES:
                continue
            row = msg.to_dict()
            row.pop("mavpackettype", None)
            data[mtype].append(row)
            total_read += 1
            if total_read % 5000 == 0:
                log(f"  Read {total_read:,} messages…")
                if progress_cb:
                    progress_cb(min(50, total_read // 1000))
        except Exception:
            continue

    log(f"Total messages: {total_read:,}  |  Types: {len(data)}")
    if progress_cb:
        progress_cb(55)

    csv_files: dict[str, str] = {}
    types_sorted = sorted(data.keys())
    n = len(types_sorted)
    for idx, mtype in enumerate(types_sorted):
        rows = data[mtype]
        if not rows:
            continue
        df   = pd.DataFrame(rows)
        path = output_mgr.save_raw_csv(mtype, df)
        csv_files[mtype] = str(path)
        if progress_cb:
            progress_cb(55 + int(40 * idx / max(n, 1)))

    log(f"Saved {len(csv_files)} CSV files  →  {output_mgr.csv_dir}")
    if progress_cb:
        progress_cb(95)
    return csv_files


# ─────────────────────────────────────────────────────────────────────────────
# MAIN GUI APPLICATION
# ─────────────────────────────────────────────────────────────────────────────

class DroneLogAnalyzer:
    def __init__(self):
        self.root = tk.Tk()
        self.root.title(f"{APP_TITLE}  v{APP_VERSION}")
        self.root.geometry("1280x800")
        self.root.minsize(1000, 650)
        self.root.configure(bg=BG_DARK)

        # State
        self.bin_path_var   = tk.StringVar(value="No file selected")
        self.output_dir_var = tk.StringVar(value=str(Path(OUTPUT_ROOT).resolve()))
        self.status_var     = tk.StringVar(value="Ready — select a .bin log file to begin")
        self.progress_var   = tk.DoubleVar(value=0)

        self.csv_files: dict  = {}
        self.events:    list  = []
        self.verdict:   str   = ""
        self.evidence:  list  = []
        self.output_mgr       = None
        self._worker_thread   = None

        self._build_ui()
        self.root.mainloop()

    # ─────────────────────────────────────────────────────────────────────
    # UI CONSTRUCTION
    # ─────────────────────────────────────────────────────────────────────

    def _build_ui(self):
        self._build_header()
        self._build_toolbar()
        self._build_progress()
        self._build_notebook()
        self._build_statusbar()

    def _build_header(self):
        hdr = tk.Frame(self.root, bg=BG_MID, height=60)
        hdr.pack(fill="x", side="top")
        hdr.pack_propagate(False)

        tk.Label(hdr, text="✈  DroneLogAnalyzer",
                 font=("Segoe UI", 18, "bold"),
                 bg=BG_MID, fg=ACCENT_TEAL).pack(side="left", padx=18, pady=10)
        tk.Label(hdr, text=f"v{APP_VERSION}  |  ArduPilot Crash Inspector",
                 font=("Segoe UI", 10),
                 bg=BG_MID, fg=FG_GREY).pack(side="left", pady=10)

    def _build_toolbar(self):
        bar = tk.Frame(self.root, bg=BG_LIGHT, pady=8)
        bar.pack(fill="x", side="top")

        # File selection
        tk.Label(bar, text="Log File:", bg=BG_LIGHT, fg=FG_WHITE,
                 font=("Segoe UI", 9, "bold")).pack(side="left", padx=(12,4))
        tk.Label(bar, textvariable=self.bin_path_var,
                 bg=BG_LIGHT, fg=ACCENT_BLUE,
                 font=("Segoe UI", 9), width=42, anchor="w").pack(side="left")

        btn_style = dict(font=("Segoe UI", 9, "bold"), relief="flat",
                         padx=10, pady=4, cursor="hand2")

        tk.Button(bar, text="📂  Browse .bin",
                  bg=ACCENT_BLUE, fg="white",
                  command=self._browse_bin,
                  **btn_style).pack(side="left", padx=6)

        tk.Button(bar, text="▶  Run Analysis",
                  bg=ACCENT_TEAL, fg=BG_DARK,
                  command=self._run_analysis,
                  **btn_style).pack(side="left", padx=4)

        tk.Button(bar, text="🗂  Output Folder",
                  bg=BG_MID, fg=FG_WHITE,
                  command=self._open_output_folder,
                  **btn_style).pack(side="left", padx=4)

        tk.Button(bar, text="🔄  Reset",
                  bg="#555570", fg=FG_WHITE,
                  command=self._reset,
                  **btn_style).pack(side="left", padx=4)

        # Output dir label
        tk.Label(bar, text="Output:", bg=BG_LIGHT, fg=FG_GREY,
                 font=("Segoe UI", 9)).pack(side="left", padx=(16,4))
        tk.Label(bar, textvariable=self.output_dir_var,
                 bg=BG_LIGHT, fg=FG_GREY,
                 font=("Segoe UI", 8), width=30, anchor="w").pack(side="left")

    def _build_progress(self):
        pf = tk.Frame(self.root, bg=BG_DARK)
        pf.pack(fill="x", side="top", padx=10, pady=(4, 0))
        self.progress_bar = ttk.Progressbar(
            pf, variable=self.progress_var,
            maximum=100, mode="determinate", length=400)
        self.progress_bar.pack(side="left", fill="x", expand=True)

        style = ttk.Style()
        style.theme_use("default")
        style.configure("TProgressbar", troughcolor=BG_LIGHT,
                        background=ACCENT_TEAL, thickness=8)

    def _build_notebook(self):
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill="both", expand=True, padx=10, pady=6)

        style = ttk.Style()
        style.configure("TNotebook",        background=BG_DARK, borderwidth=0)
        style.configure("TNotebook.Tab",    background=BG_MID,  foreground=FG_GREY,
                        padding=[14, 6],    font=("Segoe UI", 9, "bold"))
        style.map("TNotebook.Tab",
                  background=[("selected", BG_LIGHT)],
                  foreground=[("selected", FG_WHITE)])

        self._build_overview_tab()
        self._build_analysis_tab()
        self._build_charts_tab()
        self._build_report_tab()
        self._build_files_tab()
        self._build_log_tab()

    def _build_statusbar(self):
        sb = tk.Frame(self.root, bg=BG_MID, height=26)
        sb.pack(fill="x", side="bottom")
        sb.pack_propagate(False)
        tk.Label(sb, textvariable=self.status_var,
                 bg=BG_MID, fg=FG_GREY,
                 font=("Segoe UI", 9), anchor="w").pack(side="left", padx=10)

    # ── Tab: Overview ──────────────────────────────────────────────────────

    def _build_overview_tab(self):
        frame = tk.Frame(self.notebook, bg=BG_DARK)
        self.notebook.add(frame, text="📋  Overview")

        # Top — verdict banner
        self.verdict_frame = tk.Frame(frame, bg=BG_MID, pady=16)
        self.verdict_frame.pack(fill="x", padx=10, pady=(10, 4))

        self.verdict_icon  = tk.Label(self.verdict_frame, text="⬤",
                                      font=("Segoe UI", 28), bg=BG_MID, fg=FG_GREY)
        self.verdict_icon.pack(side="left", padx=(20, 8))

        vt = tk.Frame(self.verdict_frame, bg=BG_MID)
        vt.pack(side="left")
        self.verdict_title = tk.Label(vt, text="No file loaded",
                                      font=("Segoe UI", 14, "bold"),
                                      bg=BG_MID, fg=FG_WHITE)
        self.verdict_title.pack(anchor="w")
        self.verdict_sub   = tk.Label(vt, text="Load a .bin file and run analysis",
                                      font=("Segoe UI", 10),
                                      bg=BG_MID, fg=FG_GREY)
        self.verdict_sub.pack(anchor="w")

        # Stats row
        stats_row = tk.Frame(frame, bg=BG_DARK)
        stats_row.pack(fill="x", padx=10, pady=4)
        self.stat_cards: dict[str, tk.Label] = {}
        for label, key in [("CRITICAL","critical"),("WARNING","warning"),
                            ("OK","ok"),("TOTAL","total"),("MSG TYPES","types")]:
            card = tk.Frame(stats_row, bg=BG_LIGHT, padx=18, pady=10)
            card.pack(side="left", padx=4, fill="y")
            val_lbl = tk.Label(card, text="—", font=("Segoe UI", 22, "bold"),
                               bg=BG_LIGHT, fg=FG_WHITE)
            val_lbl.pack()
            tk.Label(card, text=label, font=("Segoe UI", 8),
                     bg=BG_LIGHT, fg=FG_GREY).pack()
            self.stat_cards[key] = val_lbl

        # Message types tree
        mid = tk.Frame(frame, bg=BG_DARK)
        mid.pack(fill="both", expand=True, padx=10, pady=4)

        tk.Label(mid, text="Message Types Extracted",
                 font=("Segoe UI", 10, "bold"),
                 bg=BG_DARK, fg=FG_WHITE).pack(anchor="w", pady=(4,2))

        tree_frame = tk.Frame(mid, bg=BG_DARK)
        tree_frame.pack(fill="both", expand=True)

        cols = ("type", "rows", "important")
        self.msg_tree = ttk.Treeview(tree_frame, columns=cols,
                                     show="headings", height=12)

        style = ttk.Style()
        style.configure("Treeview",
                        background=BG_LIGHT, foreground=FG_WHITE,
                        fieldbackground=BG_LIGHT, rowheight=24,
                        font=("Segoe UI", 9))
        style.configure("Treeview.Heading",
                        background=BG_MID, foreground=FG_WHITE,
                        font=("Segoe UI", 9, "bold"))
        style.map("Treeview", background=[("selected", ACCENT_BLUE)])

        self.msg_tree.heading("type",      text="Message Type")
        self.msg_tree.heading("rows",      text="Rows")
        self.msg_tree.heading("important", text="Category")

        # ✅ FIX: use compass anchors ("e" not "right", "w" not "left")
        self.msg_tree.column("type",      width=160, anchor="w")
        self.msg_tree.column("rows",      width=100, anchor="e")
        self.msg_tree.column("important", width=180, anchor="w")

        vsb = ttk.Scrollbar(tree_frame, orient="vertical",
                            command=self.msg_tree.yview)
        self.msg_tree.configure(yscrollcommand=vsb.set)
        self.msg_tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

    # ── Tab: Analysis ──────────────────────────────────────────────────────

    def _build_analysis_tab(self):
        frame = tk.Frame(self.notebook, bg=BG_DARK)
        self.notebook.add(frame, text="🔍  Analysis")

        tk.Label(frame, text="Crash Parameter Analysis",
                 font=("Segoe UI", 11, "bold"),
                 bg=BG_DARK, fg=FG_WHITE).pack(anchor="w", padx=10, pady=(10,2))

        # Filter bar
        fbar = tk.Frame(frame, bg=BG_DARK)
        fbar.pack(fill="x", padx=10, pady=2)
        tk.Label(fbar, text="Filter:",
                 bg=BG_DARK, fg=FG_GREY,
                 font=("Segoe UI", 9)).pack(side="left")
        self.filter_var = tk.StringVar(value="ALL")
        for val in ("ALL","CRITICAL","WARNING","INFO","OK"):
            clr = SEVERITY_COLOR.get(f"[{val}]", FG_GREY) if val != "ALL" else FG_WHITE
            tk.Radiobutton(fbar, text=val, variable=self.filter_var,
                           value=val, bg=BG_DARK, fg=clr,
                           selectcolor=BG_MID, activebackground=BG_DARK,
                           font=("Segoe UI", 9, "bold"),
                           command=self._apply_filter).pack(side="left", padx=6)

        # Analysis tree
        at_frame = tk.Frame(frame, bg=BG_DARK)
        at_frame.pack(fill="both", expand=True, padx=10, pady=4)

        cols = ("category","parameter","status","max_value","threshold","anomaly_count","interpretation")
        self.analysis_tree = ttk.Treeview(at_frame, columns=cols,
                                          show="headings", height=20)

        headings = {
            "category":      ("Category",      110, "w"),
            "parameter":     ("Parameter",     200, "w"),
            "status":        ("Status",         90, "center"),
            "max_value":     ("Observed Value",160, "w"),
            "threshold":     ("Threshold",     120, "w"),
            "anomaly_count": ("Anomalies",      80, "e"),
            "interpretation":("Interpretation",300, "w"),
        }
        for col, (heading, width, anchor) in headings.items():
            self.analysis_tree.heading(col, text=heading)
            # ✅ FIX: all anchors use compass values
            self.analysis_tree.column(col, width=width, anchor=anchor, minwidth=60)

        vsb2 = ttk.Scrollbar(at_frame, orient="vertical",
                             command=self.analysis_tree.yview)
        hsb2 = ttk.Scrollbar(at_frame, orient="horizontal",
                             command=self.analysis_tree.xview)
        self.analysis_tree.configure(yscrollcommand=vsb2.set,
                                     xscrollcommand=hsb2.set)

        self.analysis_tree.pack(side="left", fill="both", expand=True)
        vsb2.pack(side="right", fill="y")
        hsb2.pack(side="bottom", fill="x")

        # Tag colors for rows
        self.analysis_tree.tag_configure("CRITICAL", foreground="#FF6666")
        self.analysis_tree.tag_configure("WARNING",  foreground="#FFAA44")
        self.analysis_tree.tag_configure("INFO",     foreground="#66AAFF")
        self.analysis_tree.tag_configure("OK",       foreground="#66CC66")

    # ── Tab: Charts ────────────────────────────────────────────────────────

    def _build_charts_tab(self):
        frame = tk.Frame(self.notebook, bg=BG_DARK)
        self.notebook.add(frame, text="📊  Charts")

        ctrl = tk.Frame(frame, bg=BG_DARK)
        ctrl.pack(fill="x", padx=10, pady=6)

        tk.Label(ctrl, text="Plot parameter:",
                 bg=BG_DARK, fg=FG_WHITE,
                 font=("Segoe UI", 9, "bold")).pack(side="left")

        self.chart_choice = tk.StringVar(value="Attitude (Roll/Pitch)")
        choices = [
            "Attitude (Roll/Pitch)",
            "Vibration",
            "GPS (HDOP & Satellites)",
            "Battery Voltage",
            "Battery Current",
            "Altitude (Baro)",
            "Motor Outputs",
            "Severity Summary",
        ]
        om = ttk.OptionMenu(ctrl, self.chart_choice, choices[0], *choices,
                            command=lambda _: self._draw_chart())
        om.pack(side="left", padx=8)

        tk.Button(ctrl, text="🔄 Refresh",
                  bg=ACCENT_BLUE, fg="white",
                  font=("Segoe UI", 9), relief="flat",
                  command=self._draw_chart).pack(side="left", padx=4)

        self.chart_frame = tk.Frame(frame, bg=BG_DARK)
        self.chart_frame.pack(fill="both", expand=True, padx=10, pady=4)

        self.chart_placeholder = tk.Label(
            self.chart_frame,
            text="Run analysis to generate charts",
            font=("Segoe UI", 13), bg=BG_DARK, fg=FG_GREY)
        self.chart_placeholder.pack(expand=True)

    # ── Tab: Report ────────────────────────────────────────────────────────

    def _build_report_tab(self):
        frame = tk.Frame(self.notebook, bg=BG_DARK)
        self.notebook.add(frame, text="📄  Report")

        ctrl = tk.Frame(frame, bg=BG_DARK)
        ctrl.pack(fill="x", padx=10, pady=6)

        tk.Button(ctrl, text="💾  Save Report TXT",
                  bg=ACCENT_BLUE, fg="white",
                  font=("Segoe UI", 9, "bold"), relief="flat",
                  command=self._save_report_txt).pack(side="left", padx=4)
        tk.Button(ctrl, text="📋  Copy to Clipboard",
                  bg=BG_MID, fg=FG_WHITE,
                  font=("Segoe UI", 9), relief="flat",
                  command=self._copy_report).pack(side="left", padx=4)

        self.report_text = scrolledtext.ScrolledText(
            frame, wrap="none",
            bg="#0D0D1A", fg=FG_WHITE,
            font=("Consolas", 9),
            insertbackground=FG_WHITE,
            selectbackground=ACCENT_BLUE)
        self.report_text.pack(fill="both", expand=True, padx=10, pady=4)
        self.report_text.insert("1.0", "Run analysis to generate the report…")
        self.report_text.configure(state="disabled")

    # ── Tab: Files ─────────────────────────────────────────────────────────

    def _build_files_tab(self):
        frame = tk.Frame(self.notebook, bg=BG_DARK)
        self.notebook.add(frame, text="🗂  Output Files")

        tk.Label(frame, text="Files saved in output folder",
                 font=("Segoe UI", 10, "bold"),
                 bg=BG_DARK, fg=FG_WHITE).pack(anchor="w", padx=10, pady=(10,2))

        cols = ("file","size_kb","description","saved_at")
        files_frame = tk.Frame(frame, bg=BG_DARK)
        files_frame.pack(fill="both", expand=True, padx=10, pady=4)

        self.files_tree = ttk.Treeview(files_frame, columns=cols,
                                       show="headings", height=20)
        self.files_tree.heading("file",        text="File Name")
        self.files_tree.heading("size_kb",     text="Size (KB)")
        self.files_tree.heading("description", text="Description")
        self.files_tree.heading("saved_at",    text="Saved At")

        # ✅ FIX: compass anchors
        self.files_tree.column("file",        width=200, anchor="w")
        self.files_tree.column("size_kb",     width=90,  anchor="e")
        self.files_tree.column("description", width=420, anchor="w")
        self.files_tree.column("saved_at",    width=90,  anchor="center")

        vsb3 = ttk.Scrollbar(files_frame, orient="vertical",
                             command=self.files_tree.yview)
        self.files_tree.configure(yscrollcommand=vsb3.set)
        self.files_tree.pack(side="left", fill="both", expand=True)
        vsb3.pack(side="right", fill="y")

        # Footer
        self.files_footer = tk.Label(frame, text="",
                                     bg=BG_DARK, fg=FG_GREY,
                                     font=("Segoe UI", 9))
        self.files_footer.pack(anchor="w", padx=10, pady=4)

        tk.Button(frame, text="📂  Open Output Folder",
                  bg=ACCENT_BLUE, fg="white",
                  font=("Segoe UI", 9, "bold"), relief="flat",
                  command=self._open_output_folder).pack(anchor="w", padx=10, pady=4)

    # ── Tab: Log ───────────────────────────────────────────────────────────

    def _build_log_tab(self):
        frame = tk.Frame(self.notebook, bg=BG_DARK)
        self.notebook.add(frame, text="📟  Console Log")

        ctrl = tk.Frame(frame, bg=BG_DARK)
        ctrl.pack(fill="x", padx=10, pady=4)
        tk.Button(ctrl, text="🗑  Clear",
                  bg=BG_MID, fg=FG_WHITE,
                  font=("Segoe UI", 9), relief="flat",
                  command=self._clear_log).pack(side="left")

        self.log_text = scrolledtext.ScrolledText(
            frame, wrap="word",
            bg="#0D0D1A", fg="#00FF88",
            font=("Consolas", 9),
            insertbackground="#00FF88")
        self.log_text.pack(fill="both", expand=True, padx=10, pady=4)

    # ─────────────────────────────────────────────────────────────────────
    # ACTIONS
    # ─────────────────────────────────────────────────────────────────────

    def _browse_bin(self):
        path = filedialog.askopenfilename(
            title="Select ArduPilot .bin log file",
            filetypes=[("ArduPilot Log", "*.bin *.BIN *.ulg"),
                       ("All files", "*.*")])
        if path:
            self.bin_path_var.set(path)
            self._log(f"Selected: {path}")
            self._set_status(f"File loaded: {Path(path).name}")

    def _run_analysis(self):
        bin_path = self.bin_path_var.get()
        if bin_path == "No file selected" or not Path(bin_path).exists():
            messagebox.showwarning("No File", "Please select a .bin log file first.")
            return
        if self._worker_thread and self._worker_thread.is_alive():
            messagebox.showinfo("Busy", "Analysis is already running.")
            return
        self._worker_thread = threading.Thread(
            target=self._analysis_worker, args=(bin_path,), daemon=True)
        self._worker_thread.start()

    def _analysis_worker(self, bin_path: str):
        try:
            self._set_status("Starting analysis…")
            self._set_progress(0)
            self._log("=" * 60)
            self._log(f"ANALYSIS STARTED: {Path(bin_path).name}")
            self._log(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            self._log("=" * 60)

            # Create output manager
            log_stem = Path(bin_path).stem
            self.output_mgr = OutputManager(OUTPUT_ROOT, log_stem)
            self.root.after(0, lambda: self.output_dir_var.set(
                str(self.output_mgr.base.resolve())))

            self._log(f"\nOutput folder: {self.output_mgr.base.resolve()}")
            self._set_progress(5)

            # Step 1: Convert BIN → CSV
            self._set_status("Step 1/3 — Converting BIN to CSV…")
            self._log("\n── STEP 1: BIN → CSV ─────────────────────────")
            self.csv_files = bin_to_csv(
                bin_path, self.output_mgr,
                progress_cb=self._set_progress,
                log_cb=self._log)
            self._set_progress(80)

            # Populate overview tab
            self.root.after(0, self._populate_overview)

            # Step 2: Analyze
            self._set_status("Step 2/3 — Running crash analysis…")
            self._log("\n── STEP 2: CRASH ANALYSIS ────────────────────")
            self.events  = run_all_analyzers(self.csv_files)
            self.verdict, self.evidence = determine_verdict(self.events)
            self._set_progress(88)
            self._log(f"Parameters checked: {len(self.events)}")
            self._log(f"Verdict: {self.verdict}")

            # Step 3: Save outputs
            self._set_status("Step 3/3 — Saving output files…")
            self._log("\n── STEP 3: SAVING OUTPUTS ────────────────────")
            self._save_all_outputs()
            self._set_progress(95)

            # Update all tabs
            self.root.after(0, self._populate_analysis_tab)
            self.root.after(0, self._populate_report_tab)
            self.root.after(0, self._populate_files_tab)
            self.root.after(0, self._draw_chart)
            self.root.after(0, self._update_verdict_banner)
            self.root.after(0, self._update_stat_cards)

            self._set_progress(100)
            self._set_status(f"Complete — {self.verdict}")
            self._log("\n" + "=" * 60)
            self._log("ANALYSIS COMPLETE")
            self._log(f"VERDICT: {self.verdict}")
            self._log("=" * 60)

        except Exception as exc:
            err = traceback.format_exc()
            self._log(f"\n[ERROR] {exc}\n{err}")
            self._set_status(f"Error: {exc}")
            self.root.after(0, lambda: messagebox.showerror("Error", str(exc)))

    def _save_all_outputs(self):
        if not self.output_mgr:
            return
        n_crit = sum(1 for e in self.events if e["status"] == "[CRITICAL]")
        n_warn = sum(1 for e in self.events if e["status"] == "[WARNING]")
        n_ok   = sum(1 for e in self.events if e["status"] == "[OK]")

        self.output_mgr.save_crash_report_csv(self.events)
        payload = {
            "log_file":  Path(self.bin_path_var.get()).name,
            "generated": datetime.now().isoformat(),
            "verdict":   self.verdict,
            "statistics":{"total_checks":len(self.events),"critical":n_crit,
                          "warning":n_warn,"ok":n_ok},
            "crash_evidence": self.evidence,
            "events":    self.events,
            "thresholds_used": THRESHOLDS,
        }
        self.output_mgr.save_crash_report_json(payload)
        text = build_text_report(self.events, Path(self.bin_path_var.get()).name,
                                 self.verdict, self.evidence)
        self.output_mgr.save_crash_report_txt(text)
        summary = {
            "log_file":            Path(self.bin_path_var.get()).name,
            "generated":           datetime.now().isoformat(),
            "verdict":             self.verdict,
            "critical_count":      n_crit,
            "warning_count":       n_warn,
            "ok_count":            n_ok,
            "critical_categories": sorted({e["category"] for e in self.events
                                           if e["status"]=="[CRITICAL]"}),
            "csv_message_types":   sorted(self.csv_files.keys()),
            "output_folder":       str(self.output_mgr.base.resolve()),
        }
        self.output_mgr.save_summary_json(summary)
        self._log(f"All output files saved → {self.output_mgr.base.resolve()}")

    # ─────────────────────────────────────────────────────────────────────
    # TAB POPULATION
    # ─────────────────────────────────────────────────────────────────────

    def _populate_overview(self):
        """Fill the message-type tree in the Overview tab."""
        for item in self.msg_tree.get_children():
            self.msg_tree.delete(item)
        for mtype in sorted(self.csv_files.keys()):
            path = self.csv_files[mtype]
            try:
                df   = pd.read_csv(path, nrows=1)
                rows = sum(1 for _ in open(path, encoding="utf-8")) - 1
            except Exception:
                rows = 0
            cat = "Important" if mtype in IMPORTANT_MSG_TYPES else "Other"
            tag = "imp" if cat == "Important" else "oth"
            self.msg_tree.insert("", "end",
                values=(mtype, f"{rows:,}", cat), tags=(tag,))
        self.msg_tree.tag_configure("imp", foreground=ACCENT_TEAL)
        self.msg_tree.tag_configure("oth", foreground=FG_GREY)

    def _update_stat_cards(self):
        n_crit  = sum(1 for e in self.events if e["status"] == "[CRITICAL]")
        n_warn  = sum(1 for e in self.events if e["status"] == "[WARNING]")
        n_ok    = sum(1 for e in self.events if e["status"] == "[OK]")
        self.stat_cards["critical"].configure(text=str(n_crit),
            fg=SEVERITY_COLOR["[CRITICAL]"] if n_crit else "#44BB44")
        self.stat_cards["warning"].configure(text=str(n_warn),
            fg=SEVERITY_COLOR["[WARNING]"] if n_warn else FG_WHITE)
        self.stat_cards["ok"].configure(text=str(n_ok), fg="#44BB44")
        self.stat_cards["total"].configure(text=str(len(self.events)), fg=FG_WHITE)
        self.stat_cards["types"].configure(text=str(len(self.csv_files)), fg=FG_WHITE)

    def _update_verdict_banner(self):
        v = self.verdict
        if "CRASH LIKELY" in v:
            color, icon = SEVERITY_COLOR["[CRITICAL]"], "💥"
        elif "PROBABLE" in v:
            color, icon = "#FF7722", "⚠️"
        elif "POSSIBLE" in v or "CAUTION" in v:
            color, icon = SEVERITY_COLOR["[WARNING]"], "⚠️"
        else:
            color, icon = SEVERITY_COLOR["[OK]"], "✅"
        self.verdict_icon.configure(text=icon, fg=color)
        self.verdict_title.configure(text=v, fg=color)
        n_crit = sum(1 for e in self.events if e["status"]=="[CRITICAL]")
        n_warn = sum(1 for e in self.events if e["status"]=="[WARNING]")
        self.verdict_sub.configure(
            text=f"{n_crit} critical  •  {n_warn} warnings  •  "
                 f"{len(self.csv_files)} message types  •  "
                 f"{Path(self.bin_path_var.get()).name}")

    def _populate_analysis_tab(self):
        for item in self.analysis_tree.get_children():
            self.analysis_tree.delete(item)
        for e in self.events:
            tag = e["status"].strip("[]")
            self.analysis_tree.insert("", "end", values=(
                e["category"], e["parameter"], e["status"],
                e["max_value"], e["threshold"],
                e["anomaly_count"], e["interpretation"]
            ), tags=(tag,))

    def _apply_filter(self):
        filt = self.filter_var.get()
        for item in self.analysis_tree.get_children():
            self.analysis_tree.delete(item)
        for e in self.events:
            tag = e["status"].strip("[]")
            if filt != "ALL" and tag != filt:
                continue
            self.analysis_tree.insert("", "end", values=(
                e["category"], e["parameter"], e["status"],
                e["max_value"], e["threshold"],
                e["anomaly_count"], e["interpretation"]
            ), tags=(tag,))

    def _populate_report_tab(self):
        text = build_text_report(
            self.events, Path(self.bin_path_var.get()).name,
            self.verdict, self.evidence)
        self.report_text.configure(state="normal")
        self.report_text.delete("1.0", "end")
        self.report_text.insert("1.0", text)
        self.report_text.configure(state="disabled")

    def _populate_files_tab(self):
        for item in self.files_tree.get_children():
            self.files_tree.delete(item)
        if not self.output_mgr:
            return
        manifest = self.output_mgr.get_manifest()
        total_kb = 0
        for rec in manifest:
            self.files_tree.insert("", "end", values=(
                rec["file"], f"{rec['size_kb']:.1f}",
                rec["description"], rec["saved_at"]))
            total_kb += rec["size_kb"]
        self.files_footer.configure(
            text=f"  {len(manifest)} files  |  {total_kb:.1f} KB total  |  "
                 f"Folder: {self.output_mgr.base.resolve()}")

    # ─────────────────────────────────────────────────────────────────────
    # CHARTS
    # ─────────────────────────────────────────────────────────────────────

    def _draw_chart(self):
        if not self.csv_files or not MATPLOTLIB_OK:
            return

        # Destroy previous chart
        for w in self.chart_frame.winfo_children():
            w.destroy()

        choice = self.chart_choice.get()
        fig = Figure(figsize=(11, 5), facecolor=BG_DARK)

        try:
            if choice == "Attitude (Roll/Pitch)":
                self._chart_attitude(fig)
            elif choice == "Vibration":
                self._chart_vibration(fig)
            elif choice == "GPS (HDOP & Satellites)":
                self._chart_gps(fig)
            elif choice == "Battery Voltage":
                self._chart_battery_volt(fig)
            elif choice == "Battery Current":
                self._chart_battery_curr(fig)
            elif choice == "Altitude (Baro)":
                self._chart_altitude(fig)
            elif choice == "Motor Outputs":
                self._chart_motors(fig)
            elif choice == "Severity Summary":
                self._chart_severity_summary(fig)
        except Exception as exc:
            ax = fig.add_subplot(111)
            ax.set_facecolor(BG_DARK)
            ax.text(0.5, 0.5, f"Chart error:\n{exc}",
                    ha="center", va="center",
                    color="red", transform=ax.transAxes)

        canvas = FigureCanvasTkAgg(fig, master=self.chart_frame)
        canvas.draw()
        canvas.get_tk_widget().pack(fill="both", expand=True)

    def _style_ax(self, ax, title="", xlabel="", ylabel=""):
        ax.set_facecolor(BG_MID)
        ax.tick_params(colors=FG_GREY, labelsize=8)
        ax.spines[:].set_color(BG_LIGHT)
        if title:
            ax.set_title(title, color=FG_WHITE, fontsize=10, pad=8)
        if xlabel:
            ax.set_xlabel(xlabel, color=FG_GREY, fontsize=8)
        if ylabel:
            ax.set_ylabel(ylabel, color=FG_GREY, fontsize=8)
        ax.grid(True, color=BG_LIGHT, linewidth=0.5, alpha=0.7)

    def _chart_attitude(self, fig):
        df = load_csv_file(self.csv_files, "ATT")
        if df is None:
            raise ValueError("ATT data not available")
        ax = fig.add_subplot(111)
        rc = safe_col(df,"Roll","roll")
        pc = safe_col(df,"Pitch","pitch")
        x  = range(len(df))
        if rc:
            ax.plot(x, df[rc], color="#5B8DEF", linewidth=0.8, label="Roll")
            ax.axhline(y=45,  color="#FF4444", linestyle="--", linewidth=1, alpha=0.6)
            ax.axhline(y=-45, color="#FF4444", linestyle="--", linewidth=1, alpha=0.6)
        if pc:
            ax.plot(x, df[pc], color=ACCENT_TEAL, linewidth=0.8, label="Pitch")
        ax.legend(facecolor=BG_MID, edgecolor=BG_LIGHT, labelcolor=FG_WHITE, fontsize=8)
        self._style_ax(ax, "Attitude — Roll & Pitch", "Sample", "Degrees")

    def _chart_vibration(self, fig):
        df = load_csv_file(self.csv_files, "VIBE")
        if df is None:
            raise ValueError("VIBE data not available")
        ax  = fig.add_subplot(111)
        x   = range(len(df))
        clr = [ACCENT_BLUE, ACCENT_TEAL, "#FF9900"]
        for axis, c in zip(("VibeX","VibeY","VibeZ"), clr):
            col = safe_col(df, axis, axis.lower())
            if col:
                ax.plot(x, df[col], color=c, linewidth=0.8, label=axis)
        ax.axhline(y=THRESHOLDS["vibe_max_ms2"], color="#FF4444",
                   linestyle="--", linewidth=1, alpha=0.7, label="Threshold")
        ax.legend(facecolor=BG_MID, edgecolor=BG_LIGHT, labelcolor=FG_WHITE, fontsize=8)
        self._style_ax(ax, "Vibration Levels", "Sample", "m/s²")

    def _chart_gps(self, fig):
        df = load_csv_file(self.csv_files, "GPS")
        if df is None:
            raise ValueError("GPS data not available")
        ax1 = fig.add_subplot(121)
        ax2 = fig.add_subplot(122)
        x   = range(len(df))
        hdop  = safe_col(df,"HDop","hdop","HDOP")
        nsats = safe_col(df,"NSats","nsats","NumSats","Num_Sats")
        if hdop:
            ax1.plot(x, df[hdop], color=ACCENT_BLUE, linewidth=0.8)
            ax1.axhline(y=2.0, color="#FF4444", linestyle="--", linewidth=1)
        if nsats:
            ax2.plot(x, df[nsats], color=ACCENT_TEAL, linewidth=0.8)
            ax2.axhline(y=6, color="#FF4444", linestyle="--", linewidth=1)
        self._style_ax(ax1, "GPS HDOP", "Sample", "HDOP")
        self._style_ax(ax2, "GPS Satellites", "Sample", "# Sats")
        fig.tight_layout(pad=2)

    def _chart_battery_volt(self, fig):
        df = load_csv_file(self.csv_files,"BAT") or load_csv_file(self.csv_files,"CURR")
        if df is None:
            raise ValueError("Battery data not available")
        vc = safe_col(df,"Volt","volt","VoltR","voltage","Vcc")
        if vc is None:
            raise ValueError("Voltage column not found")
        ax = fig.add_subplot(111)
        ax.plot(range(len(df)), df[vc], color=ACCENT_TEAL, linewidth=0.8, label="Voltage")
        ax.axhline(y=THRESHOLDS["voltage_min_v"], color="#FF4444",
                   linestyle="--", linewidth=1, label="Min Threshold")
        ax.legend(facecolor=BG_MID, edgecolor=BG_LIGHT, labelcolor=FG_WHITE, fontsize=8)
        self._style_ax(ax, "Battery Voltage", "Sample", "Volts")

    def _chart_battery_curr(self, fig):
        df = load_csv_file(self.csv_files,"BAT") or load_csv_file(self.csv_files,"CURR")
        if df is None:
            raise ValueError("Battery data not available")
        cc = safe_col(df,"Curr","curr","current","Current")
        if cc is None:
            raise ValueError("Current column not found")
        ax = fig.add_subplot(111)
        ax.plot(range(len(df)), df[cc], color="#FF9900", linewidth=0.8, label="Current")
        ax.axhline(y=THRESHOLDS["current_max_a"], color="#FF4444",
                   linestyle="--", linewidth=1, label="Max Threshold")
        ax.legend(facecolor=BG_MID, edgecolor=BG_LIGHT, labelcolor=FG_WHITE, fontsize=8)
        self._style_ax(ax, "Battery Current", "Sample", "Amps")

    def _chart_altitude(self, fig):
        df = load_csv_file(self.csv_files,"BARO")
        if df is None:
            raise ValueError("BARO data not available")
        ac = safe_col(df,"Alt","alt","Altitude","altitude")
        if ac is None:
            raise ValueError("Altitude column not found")
        ax = fig.add_subplot(111)
        ax.fill_between(range(len(df)), df[ac], alpha=0.3, color=ACCENT_BLUE)
        ax.plot(range(len(df)), df[ac], color=ACCENT_BLUE, linewidth=0.8, label="Altitude")
        ax.legend(facecolor=BG_MID, edgecolor=BG_LIGHT, labelcolor=FG_WHITE, fontsize=8)
        self._style_ax(ax, "Barometric Altitude", "Sample", "Metres")

    def _chart_motors(self, fig):
        df = load_csv_file(self.csv_files,"RCOU")
        if df is None:
            raise ValueError("RCOU data not available")
        ax     = fig.add_subplot(111)
        colors = [ACCENT_BLUE, ACCENT_TEAL, "#FF9900", "#FF4444"]
        found  = 0
        for i in range(1, 9):
            col = safe_col(df, f"C{i}", f"c{i}", f"Chan{i}", f"M{i}")
            if col and found < 4:
                ax.plot(range(len(df)), df[col],
                        color=colors[found], linewidth=0.8, label=f"Motor {i}")
                found += 1
        if found == 0:
            raise ValueError("No motor output columns found")
        ax.legend(facecolor=BG_MID, edgecolor=BG_LIGHT, labelcolor=FG_WHITE, fontsize=8)
        self._style_ax(ax, "Motor PWM Outputs", "Sample", "µs")

    def _chart_severity_summary(self, fig):
        if not self.events:
            raise ValueError("No analysis events yet")
        cats    = {}
        for e in self.events:
            if e["status"] in ("[CRITICAL]","[WARNING]"):
                cats[e["category"]] = cats.get(e["category"], 0) + 1
        if not cats:
            raise ValueError("No critical/warning events to display")
        ax     = fig.add_subplot(111)
        labels = list(cats.keys())
        vals   = [cats[l] for l in labels]
        colors = [SEVERITY_COLOR["[CRITICAL]"] if
                  any(e["category"]==l and e["status"]=="[CRITICAL]"
                      for e in self.events) else SEVERITY_COLOR["[WARNING]"]
                  for l in labels]
        bars = ax.barh(labels, vals, color=colors, edgecolor=BG_DARK)
        ax.bar_label(bars, padding=4, color=FG_WHITE, fontsize=9)
        self._style_ax(ax, "Issues by Category", "Count", "")
        ax.set_xlim(0, max(vals) + 2)

    # ─────────────────────────────────────────────────────────────────────
    # UTILITIES
    # ─────────────────────────────────────────────────────────────────────

    def _log(self, msg: str):
        def _append():
            self.log_text.configure(state="normal")
            self.log_text.insert("end", msg + "\n")
            self.log_text.see("end")
            self.log_text.configure(state="disabled")
        self.root.after(0, _append)

    def _set_status(self, msg: str):
        self.root.after(0, lambda: self.status_var.set(msg))

    def _set_progress(self, pct: float):
        self.root.after(0, lambda: self.progress_var.set(min(100, pct)))

    def _clear_log(self):
        self.log_text.configure(state="normal")
        self.log_text.delete("1.0", "end")
        self.log_text.configure(state="disabled")

    def _save_report_txt(self):
        if not self.events:
            messagebox.showinfo("No Data", "Run analysis first.")
            return
        path = filedialog.asksaveasfilename(
            defaultextension=".txt",
            filetypes=[("Text files","*.txt"),("All files","*.*")],
            initialfile="crash_report.txt")
        if path:
            text = build_text_report(
                self.events, Path(self.bin_path_var.get()).name,
                self.verdict, self.evidence)
            Path(path).write_text(text, encoding="utf-8")
            messagebox.showinfo("Saved", f"Report saved:\n{path}")

    def _copy_report(self):
        if not self.events:
            return
        text = build_text_report(
            self.events, Path(self.bin_path_var.get()).name,
            self.verdict, self.evidence)
        self.root.clipboard_clear()
        self.root.clipboard_append(text)
        messagebox.showinfo("Copied", "Report copied to clipboard.")

    def _open_output_folder(self):
        folder = self.output_mgr.base if self.output_mgr else Path(OUTPUT_ROOT)
        folder.mkdir(parents=True, exist_ok=True)
        if sys.platform == "win32":
            os.startfile(str(folder))
        elif sys.platform == "darwin":
            os.system(f'open "{folder}"')
        else:
            os.system(f'xdg-open "{folder}"')

    def _reset(self):
        self.csv_files = {}
        self.events    = []
        self.verdict   = ""
        self.evidence  = []
        self.output_mgr = None
        self.bin_path_var.set("No file selected")
        self.progress_var.set(0)
        self._set_status("Ready — select a .bin log file to begin")
        self.verdict_title.configure(text="No file loaded", fg=FG_WHITE)
        self.verdict_sub.configure(text="Load a .bin file and run analysis")
        self.verdict_icon.configure(text="⬤", fg=FG_GREY)
        for k in self.stat_cards:
            self.stat_cards[k].configure(text="—", fg=FG_WHITE)
        for item in self.msg_tree.get_children():
            self.msg_tree.delete(item)
        for item in self.analysis_tree.get_children():
            self.analysis_tree.delete(item)
        for item in self.files_tree.get_children():
            self.files_tree.delete(item)
        self.report_text.configure(state="normal")
        self.report_text.delete("1.0","end")
        self.report_text.insert("1.0","Run analysis to generate the report…")
        self.report_text.configure(state="disabled")
        for w in self.chart_frame.winfo_children():
            w.destroy()
        self.chart_placeholder = tk.Label(
            self.chart_frame,
            text="Run analysis to generate charts",
            font=("Segoe UI", 13), bg=BG_DARK, fg=FG_GREY)
        self.chart_placeholder.pack(expand=True)
        self._clear_log()


# ─────────────────────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app = DroneLogAnalyzer()
