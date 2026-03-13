
import os
import subprocess
import sys
import tkinter as tk
from tkinter import filedialog, messagebox

import pandas as pd
import numpy as np
from pyulog import ULog
from pymavlink import mavutil


# -----------------------------
# GUI INITIALIZATION
# -----------------------------
root = tk.Tk()
root.withdraw()

log_file = filedialog.askopenfilename(
    title="Select Log File (.bin or .ulg)",
    filetypes=[("BIN or ULog files", "*.bin *.ulg"), ("All files", "*.*")]
)

if not log_file:
    sys.exit(0)

ext = os.path.splitext(log_file)[1].lower()

parent_folder = filedialog.askdirectory(title="Select Folder to Save Output")
if not parent_folder:
    sys.exit(0)

basename = os.path.basename(log_file)
name = os.path.splitext(basename)[0]

output_dir = os.path.join(parent_folder, name)
csv_dir = os.path.join(output_dir, "csv")

os.makedirs(csv_dir, exist_ok=True)

print("Selected log:", log_file)
print("Output folder:", output_dir)


# -----------------------------
# BIN → CSV CONVERSION
# -----------------------------
def convert_bin():

    print("Processing BIN log")

    mav = mavutil.mavlink_connection(log_file, dialect="ardupilotmega")
    msg_types = set()

    while True:
        msg = mav.recv_match()
        if msg is None:
            break
        msg_types.add(msg.get_type())

    print("Message types:", len(msg_types))

    for mtype in sorted(msg_types):

        csv_file = os.path.join(csv_dir, f"{mtype}.csv")

        cmd = ["mavlogdump.py", log_file, "--types", mtype, "--format", "csv"]

        try:
            with open(csv_file, "w") as f:
                subprocess.run(cmd, stdout=f, stderr=subprocess.PIPE, check=True)

            print("Saved", mtype)

        except Exception as e:
            print("Failed:", mtype, e)


# -----------------------------
# ULOG → CSV CONVERSION
# -----------------------------
def convert_ulog():

    print("Processing ULog")

    ulog = ULog(log_file)

    for data in ulog.data_list:

        topic = data.name
        msg_id = data.msg_id
        field_data = data.data

        if not field_data:
            continue

        df = pd.DataFrame(field_data)

        safe = topic.replace("/", "_")

        csv_path = os.path.join(csv_dir, f"{safe}_{msg_id}.csv")

        df.to_csv(csv_path, index=False)

        print("Saved topic:", topic)


# -----------------------------
# FIRST CRASH ANALYSIS
# -----------------------------
def analyze_crash_basic():

    events = []

    print("Running basic crash analysis...")

    bat_file = os.path.join(csv_dir, "BAT.csv")
    if os.path.exists(bat_file):

        df = pd.read_csv(bat_file)

        for col in df.columns:

            if "Volt" in col:

                drops = df[col].diff().fillna(0)

                if (drops < -0.5).sum() > 5:
                    events.append("Battery voltage dropped rapidly")

    alt_file = os.path.join(csv_dir, "CTUN.csv")
    if os.path.exists(alt_file):

        df = pd.read_csv(alt_file)

        for col in df.columns:

            if "Alt" in col:

                diffs = df[col].diff().fillna(0)

                if (diffs < -3).sum() > 5:
                    events.append("Rapid altitude drop detected")

    motor_file = os.path.join(csv_dir, "RCOU.csv")
    if os.path.exists(motor_file):

        df = pd.read_csv(motor_file)

        motors = [c for c in df.columns if "C" in c]

        if len(motors) >= 2:

            means = df[motors].mean()

            hi = means.max()
            lo = means.min()

            if hi > 0:

                imbalance = (hi - lo) / hi * 100

                if imbalance > 40:
                    events.append("Motor imbalance detected")

    if len(events) == 0:
        verdict = "NO CRASH DETECTED"
    else:
        verdict = "CRASH LIKELY"

    report_file = os.path.join(output_dir, "crash_report.txt")

    with open(report_file, "w") as f:

        f.write("Drone Crash Analysis\n\n")
        f.write("Verdict: " + verdict + "\n\n")

        for e in events:
            f.write("- " + e + "\n")

    print("Basic analysis complete")



# -----------------------------
# ADVANCED CSV ANALYSIS
# -----------------------------
def analyze_csv_advanced():

    csv_folder = csv_dir
    output_folder = os.path.join(output_dir, "analysis_output")

    os.makedirs(output_folder, exist_ok=True)

    report_txt = os.path.join(output_folder, "crash_analysis.txt")
    report_csv = os.path.join(output_folder, "flight_summary.csv")

    summary = []
    crash_reasons = []

    print("\nRunning full flight analysis...\n")

    # ----------------------------
    # ALTITUDE ANALYSIS
    # ----------------------------
    ctun = os.path.join(csv_folder, "CTUN.csv")

    if os.path.exists(ctun):

        df = pd.read_csv(ctun)

        if "Alt" in df.columns:

            alt_min = df["Alt"].min()
            alt_max = df["Alt"].max()

            alt_drop = df["Alt"].diff()
            rapid_drop = (alt_drop < -8).sum()

            summary.append(["Altitude_Min", alt_min])
            summary.append(["Altitude_Max", alt_max])

            if rapid_drop > 2:
                crash_reasons.append("Severe altitude drop before crash")

    # ----------------------------
    # ATTITUDE ANALYSIS
    # ----------------------------
    att = os.path.join(csv_folder, "ATT.csv")

    if os.path.exists(att):

        df = pd.read_csv(att)

        if "Roll" in df.columns:

            roll_max = df["Roll"].abs().max()
            summary.append(["Max_Roll", roll_max])

            if roll_max > 85:
                crash_reasons.append("Extreme roll angle (possible flip)")

        if "Pitch" in df.columns:

            pitch_max = df["Pitch"].abs().max()
            summary.append(["Max_Pitch", pitch_max])

            if pitch_max > 85:
                crash_reasons.append("Extreme pitch angle (loss of control)")

    # ----------------------------
    # MOTOR ANALYSIS
    # ----------------------------
    rcou = os.path.join(csv_folder, "RCOU.csv")

    if os.path.exists(rcou):

        df = pd.read_csv(rcou)

        motor_cols = [c for c in df.columns if "C" in c]

        if len(motor_cols) > 0:

            motor_mean = df[motor_cols].mean()
            min_motor = motor_mean.min()

            summary.append(["Motor_Min_Output", min_motor])

            if min_motor < 900:
                crash_reasons.append("Motor output dropped (possible motor failure)")

    # ----------------------------
    # BATTERY ANALYSIS
    # ----------------------------
    bat = os.path.join(csv_folder, "BAT.csv")

    if os.path.exists(bat):

        df = pd.read_csv(bat)

        for col in df.columns:

            if "Volt" in col:

                min_v = df[col].min()
                summary.append(["Min_Battery_Voltage", min_v])

                drop = df[col].diff()

                if (drop < -1.5).sum() > 2:
                    crash_reasons.append("Battery voltage collapse")

    # ----------------------------
    # VIBRATION ANALYSIS
    # ----------------------------
    vibe = os.path.join(csv_folder, "VIBE.csv")

    if os.path.exists(vibe):

        df = pd.read_csv(vibe)

        if "VibeX" in df.columns:

            vib_avg = df["VibeX"].mean()
            summary.append(["Avg_Vibration", vib_avg])

            if vib_avg > 60:
                crash_reasons.append("High vibration levels detected")

    # ----------------------------
    # FINAL RESULT
    # ----------------------------
    if len(crash_reasons) == 0:
        verdict = "No crash indicators found"
    else:
        verdict = "Crash likely detected"

    summary.append(["Verdict", verdict])

    df_summary = pd.DataFrame(summary, columns=["Parameter", "Value"])
    df_summary.to_csv(report_csv, index=False)

    with open(report_txt, "w") as f:

        f.write("Drone Flight Crash Analysis\n\n")
        f.write("Verdict: " + verdict + "\n\n")
        f.write("Detected Reasons:\n")

        for r in crash_reasons:
            f.write("- " + r + "\n")

    print("Analysis completed")
    print("Summary saved:", report_csv)

# -----------------------------
# MAIN EXECUTION
# -----------------------------
if ext == ".bin":
    convert_bin()

elif ext == ".ulg":
    convert_ulog()

else:
    messagebox.showerror("Error", "Unsupported file format")
    sys.exit(1)


analyze_crash_basic()

analyze_csv_advanced()


messagebox.showinfo(
    "Done",
    f"Processing complete.\nOutput folder:\n{output_dir}"
)

