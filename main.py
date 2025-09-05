#!/usr/bin/env python3
import netifaces as ni
from reportlab.lib.units import mm
from reportlab.lib import colors
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.lib.pagesizes import A4
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer, PageBreak, Image
from matplotlib.backends.backend_qtagg import FigureCanvasQTAgg as FigureCanvas
import matplotlib.pyplot as plt
import sys
import os
import csv
import time
import platform
import subprocess
import datetime as dt
import io
import json
import re
from typing import Optional, Tuple, List, Dict

from PySide6.QtCore import Qt, QThread, QObject, Signal, QTimer
from PySide6.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout, QPushButton,
    QLineEdit, QLabel, QFileDialog, QSpinBox, QTableWidget, QTableWidgetItem,
    QMessageBox, QGroupBox, QFormLayout, QCheckBox
)

import pandas as pd
import matplotlib
matplotlib.use("QtAgg")


CONFIG_PATH = os.path.join(os.path.abspath(
    os.path.dirname(__file__)), "config.json")

TRANSIENT_ERR_PATTERNS = [
    r"sendto:\s*Cannot allocate memory",
    r"No buffer space available",
    r"Message too long",
    r"Resource temporarily unavailable",
]
TRANSIENT_ERR_REGEX = re.compile(
    "|".join(TRANSIENT_ERR_PATTERNS), re.IGNORECASE)


def now_iso():
    return dt.datetime.now().replace(microsecond=0).isoformat()


def day_str(d=None):
    x = d or dt.datetime.now()
    return x.strftime("%Y-%m-%d")


def ensure_dir(p):
    os.makedirs(p, exist_ok=True)


def ensure_csv_header(path):
    exists = os.path.exists(path)
    if not exists or os.path.getsize(path) == 0:
        ensure_dir(os.path.dirname(os.path.abspath(path)))
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["timestamp", "target", "success", "rtt_ms", "error"])


def write_csv_row(path, row):
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(row)


def load_config() -> Dict:
    cfg = {
        "auto_start": False,
        "include_gateway": True,
        "targets": ["1.1.1.1", "8.8.8.8", "9.9.9.9"],
        "interval_sec": 2,
        "timeout_ms": 1000,
        "base_dir": os.path.abspath("net_out"),
    }
    try:
        if os.path.exists(CONFIG_PATH):
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    cfg.update({k: data.get(k, v) for k, v in cfg.items()})
    except Exception:
        pass
    if not isinstance(cfg.get("targets"), list) or not cfg["targets"]:
        cfg["targets"] = ["1.1.1.1", "8.8.8.8", "9.9.9.9"]
    cfg["interval_sec"] = int(max(1, int(cfg.get("interval_sec", 2))))
    cfg["timeout_ms"] = int(max(100, int(cfg.get("timeout_ms", 1000))))
    if not cfg.get("base_dir"):
        cfg["base_dir"] = os.path.abspath("net_out")
    return cfg


def save_config(cfg: Dict):
    try:
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2, ensure_ascii=False)
    except Exception as e:
        print(f"Failed to save config: {e}", file=sys.stderr)


def parse_ping_output(returncode: int, stdout: str, stderr: str, os_name: str) -> Tuple[bool, Optional[float], Optional[str]]:
    if returncode != 0:
        err = (stderr or "").strip() or (stdout or "").strip()
        return False, None, (err[:200] if err else "ping failed")
    out = (stdout or "").strip()
    rtt_ms = None
    try:
        if os_name == "Windows":
            for line in out.splitlines():
                line = line.strip()
                if "time=" in line and "ms" in line.lower():
                    seg = line.split("time=")[1]
                    num = ""
                    for ch in seg:
                        if ch.isdigit() or ch == ".":
                            num += ch
                        else:
                            break
                    if num:
                        rtt_ms = float(num)
                        break
        else:
            for line in out.splitlines():
                line = line.strip()
                if "time=" in line and " ms" in line:
                    seg = line.split("time=")[1]
                    num = seg.split(" ")[0].replace("ms", "")
                    rtt_ms = float(num)
                    break
    except Exception as e:
        return True, None, f"parse_rtt_error: {e}"
    return True, rtt_ms, None


def ping_once(target: str, timeout_ms: int = 1000) -> Tuple[bool, Optional[float], Optional[str]]:
    os_name = platform.system()
    if os_name == "Windows":
        cmd = ["ping", "-n", "1", "-w", str(timeout_ms), target]
    elif os_name == "Darwin":
        cmd = ["ping", "-c", "1", target]
    else:
        timeout_s = max(1, int(round(timeout_ms / 1000)))
        cmd = ["ping", "-c", "1", "-W", str(timeout_s), target]
    try:
        proc = subprocess.run(
            cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, check=False, stdin=subprocess.DEVNULL
        )
        return parse_ping_output(proc.returncode, proc.stdout, proc.stderr, os_name)
    except FileNotFoundError:
        return False, None, "ping_not_found"
    except Exception as e:
        return False, None, str(e)


def is_transient_icmp_error(err: Optional[str]) -> bool:
    if not err:
        return False
    return bool(TRANSIENT_ERR_REGEX.search(err))


def get_default_gateway() -> Optional[str]:
    try:
        gws = ni.gateways()
        gw = gws.get('default', {}).get(ni.AF_INET, [None])[0]
        return gw
    except Exception:
        return None


class MonitorWorker(QObject):
    new_record = Signal(dict)
    status = Signal(str)
    stopped = Signal()

    def __init__(self):
        super().__init__()
        self._running = False
        self.targets: List[str] = []
        self.interval = 2.0
        self.timeout_ms = 1000
        self.base_dir = "net_out"
        self.current_day = day_str()
        self.csv_path = ""
        self._consecutive_sendto_errors = 0

    def configure(self, targets: List[str], interval: float, timeout_ms: int, base_dir: str):
        self.targets = targets
        self.interval = max(1.0, float(interval))
        self.timeout_ms = max(100, int(timeout_ms))
        self.base_dir = base_dir
        self.current_day = day_str()
        day_dir = os.path.join(self.base_dir, self.current_day)
        ensure_dir(day_dir)
        self.csv_path = os.path.join(day_dir, "net_log.csv")
        ensure_csv_header(self.csv_path)

    def start(self):
        self._running = True
        while self._running:
            now = dt.datetime.now()
            today = day_str(now)
            if today != self.current_day:
                self.current_day = today
                day_dir = os.path.join(self.base_dir, self.current_day)
                ensure_dir(day_dir)
                self.csv_path = os.path.join(day_dir, "net_log.csv")
                ensure_csv_header(self.csv_path)
            ts = now_iso()
            for t in self.targets:
                if not self._running:
                    break
                ok, rtt, err = ping_once(t, timeout_ms=self.timeout_ms)
                if ((not ok) and is_transient_icmp_error(err)) or (err and err.endswith("network timeout")):
                    time.sleep(0.3)
                    ok2, rtt2, err2 = ping_once(t, timeout_ms=self.timeout_ms)
                    if ok2:
                        ok, rtt, err = ok2, rtt2, err2
                    else:
                        self._consecutive_sendto_errors += 1
                        err = "Network Timeout"
                else:
                    self._consecutive_sendto_errors = 0
                row = {"timestamp": ts, "target": t, "success": 1 if ok else 0, "rtt_ms": (
                    f"{rtt:.3f}" if rtt is not None else ""), "error": (err or "")}
                write_csv_row(self.csv_path, [
                              row["timestamp"], row["target"], row["success"], row["rtt_ms"], row["error"]])
                self.new_record.emit(row)
                time.sleep(0.05)
            self.status.emit(f"{ts} running")
            if self._consecutive_sendto_errors >= 3:
                time.sleep(min(2.0, self.interval))
                self._consecutive_sendto_errors = 0
            slept = 0.0
            while self._running and slept < self.interval:
                time.sleep(0.1)
                slept += 0.1
        self.stopped.emit()

    def stop(self):
        self._running = False


class MplCanvas(FigureCanvas):
    def __init__(self):
        self.fig, self.ax = plt.subplots(
            figsize=(10, 4), constrained_layout=True)
        super().__init__(self.fig)

    def clear(self):
        self.ax.clear()
        self.draw()


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Network Monitor")
        self.cfg = load_config()
        self.worker = MonitorWorker()
        self.thread = QThread()
        self.worker.moveToThread(self.thread)
        self.thread.started.connect(self.worker.start)
        self.worker.new_record.connect(self.on_new_record)
        self.worker.status.connect(self.on_status)
        self.worker.stopped.connect(self.on_worker_stopped)
        self.records: List[Dict] = []
        self.records_limit = 20000
        self.ui_timer = QTimer(self)
        self.ui_timer.setInterval(2000)
        self.ui_timer.timeout.connect(self.refresh_views)
        self.hour_timer = QTimer(self)
        self.hour_timer.setSingleShot(True)
        self.hour_timer.timeout.connect(self.hourly_report)
        wrap = QWidget()
        root = QVBoxLayout(wrap)
        ctl_group = QGroupBox("Settings")
        ctl_form = QFormLayout(ctl_group)
        self.le_targets = QLineEdit(", ".join(self.cfg["targets"]))
        self.cb_include_gw = QCheckBox("Include default gateway")
        self.cb_include_gw.setChecked(bool(self.cfg["include_gateway"]))
        self.cb_autostart = QCheckBox("Auto start on launch")
        self.cb_autostart.setChecked(bool(self.cfg["auto_start"]))
        self.lbl_gw = QLabel("Gateway: -")
        self.spin_interval = QSpinBox()
        self.spin_interval.setRange(1, 3600)
        self.spin_interval.setValue(int(self.cfg["interval_sec"]))
        self.spin_timeout = QSpinBox()
        self.spin_timeout.setRange(100, 10000)
        self.spin_timeout.setSingleStep(100)
        self.spin_timeout.setValue(int(self.cfg["timeout_ms"]))
        self.le_outdir = QLineEdit(self.cfg["base_dir"])
        btn_browse = QPushButton("Choose folder…")

        def pick_dir():
            d = QFileDialog.getExistingDirectory(
                self, "Select output folder", self.le_outdir.text() or os.getcwd())
            if d:
                self.le_outdir.setText(d)
        btn_browse.clicked.connect(pick_dir)
        ctl_form.addRow("Targets (comma-separated)", self.le_targets)
        ctl_form.addRow(self.cb_include_gw)
        ctl_form.addRow(self.cb_autostart)
        ctl_form.addRow(self.lbl_gw)
        ctl_form.addRow("Interval (sec)", self.spin_interval)
        ctl_form.addRow("Timeout (ms)", self.spin_timeout)
        h = QHBoxLayout()
        h.addWidget(self.le_outdir, 1)
        h.addWidget(btn_browse)
        row = QWidget()
        row.setLayout(h)
        ctl_form.addRow("Base folder", row)
        btns = QHBoxLayout()
        self.btn_start = QPushButton("Start")
        self.btn_stop = QPushButton("Stop")
        self.btn_export_now = QPushButton("Export PDF")
        self.btn_save_cfg = QPushButton("Save Settings")
        btns.addWidget(self.btn_start)
        btns.addWidget(self.btn_stop)
        btns.addWidget(self.btn_export_now)
        btns.addWidget(self.btn_save_cfg)
        ctl_form.addRow(btns)
        root.addWidget(ctl_group)
        self.lbl_status = QLabel("Status: -")
        root.addWidget(self.lbl_status)
        table_group = QGroupBox("Summary per target")
        table_layout = QVBoxLayout(table_group)
        self.table = QTableWidget(0, 9)
        self.table.setHorizontalHeaderLabels(
            ["target", "count", "success", "loss_%", "avg_rtt_ms", "max_rtt_ms", "min_rtt_ms", "first_ts", "last_ts"])
        self.table.horizontalHeader().setStretchLastSection(True)
        table_layout.addWidget(self.table)
        root.addWidget(table_group, 2)
        charts_group = QGroupBox("Live charts")
        charts_layout = QVBoxLayout(charts_group)
        self.canvas_rtt = MplCanvas()
        self.canvas_loss = MplCanvas()
        charts_layout.addWidget(self.canvas_rtt, 3)
        charts_layout.addWidget(self.canvas_loss, 2)
        root.addWidget(charts_group, 4)
        self.setCentralWidget(wrap)
        self.btn_start.clicked.connect(self.on_start)
        self.btn_stop.clicked.connect(self.on_stop)
        self.btn_export_now.clicked.connect(self.hourly_report)
        self.btn_save_cfg.clicked.connect(self.on_save_settings)
        self.resize(1200, 860)
        self.refresh_gateway_label()
        if self.cb_autostart.isChecked():
            QTimer.singleShot(300, self.on_start)

    def on_save_settings(self):
        targets = [t.strip()
                   for t in self.le_targets.text().split(",") if t.strip()]
        cfg = {
            "auto_start": self.cb_autostart.isChecked(),
            "include_gateway": self.cb_include_gw.isChecked(),
            "targets": targets if targets else ["1.1.1.1", "8.8.8.8", "9.9.9.9"],
            "interval_sec": int(self.spin_interval.value()),
            "timeout_ms": int(self.spin_timeout.value()),
            "base_dir": self.le_outdir.text().strip() or os.path.abspath("net_out"),
        }
        save_config(cfg)
        self.cfg = cfg

    def closeEvent(self, e):
        self.on_save_settings()
        super().closeEvent(e)

    def refresh_gateway_label(self):
        gw = get_default_gateway()
        self.lbl_gw.setText(f"Gateway: {gw if gw else '-'}")

    def on_start(self):
        if self.thread.isRunning():
            QMessageBox.information(self, "Running", "Already monitoring.")
            return
        targets = [t.strip()
                   for t in self.le_targets.text().split(",") if t.strip()]
        gw = get_default_gateway() if self.cb_include_gw.isChecked() else None
        if gw and gw not in targets:
            targets = [gw] + targets
        if not targets:
            QMessageBox.warning(self, "Error", "Please provide targets.")
            return
        base_dir = self.le_outdir.text().strip() or os.path.abspath("net_out")
        interval = float(self.spin_interval.value())
        timeout_ms = int(self.spin_timeout.value())
        self.worker.configure(targets, interval, timeout_ms, base_dir)
        self.on_save_settings()
        self.records.clear()
        self.thread.start()
        self.ui_timer.start()
        self.schedule_next_hour()
        self.lbl_status.setText("Status: started")

    def on_stop(self):
        if self.thread.isRunning():
            self.worker.stop()
            self.ui_timer.stop()
            self.hour_timer.stop()
            self.lbl_status.setText("Status: stopping…")

    def on_worker_stopped(self):
        self.thread.quit()
        self.thread.wait()
        self.lbl_status.setText("Status: stopped")

    def on_new_record(self, row: Dict):
        self.records.append(row)
        if len(self.records) > self.records_limit:
            self.records = self.records[-self.records_limit:]

    def on_status(self, text: str):
        self.lbl_status.setText(f"Status: {text}")

    def refresh_views(self):
        if not self.records:
            return
        df = pd.DataFrame(self.records)
        df["success"] = pd.to_numeric(
            df["success"], errors="coerce").fillna(0).astype(int)
        df["rtt_ms"] = pd.to_numeric(df["rtt_ms"], errors="coerce")
        df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
        grp = df.groupby("target")
        summary = pd.DataFrame({
            "count": grp["success"].count(),
            "success": grp["success"].sum(),
            "loss_%": (1 - grp["success"].mean()) * 100.0,
            "avg_rtt_ms": grp["rtt_ms"].mean(),
            "max_rtt_ms": grp["rtt_ms"].max(),
            "min_rtt_ms": grp["rtt_ms"].min(),
            "first_ts": grp["timestamp"].min(),
            "last_ts": grp["timestamp"].max(),
        }).reset_index().sort_values("loss_%", ascending=False)
        self.update_table(summary)
        self.plot_rtt(df)
        self.plot_loss(summary)

    def update_table(self, summary: pd.DataFrame):
        self.table.setRowCount(len(summary))
        for i, r in summary.reset_index(drop=True).iterrows():
            vals = [
                r["target"],
                int(r["count"]) if pd.notna(r["count"]) else 0,
                int(r["success"]) if pd.notna(r["success"]) else 0,
                round(float(r["loss_%"]), 2) if pd.notna(r["loss_%"]) else 0.0,
                round(float(r["avg_rtt_ms"]), 2) if pd.notna(
                    r["avg_rtt_ms"]) else 0.0,
                round(float(r["max_rtt_ms"]), 2) if pd.notna(
                    r["max_rtt_ms"]) else 0.0,
                round(float(r["min_rtt_ms"]), 2) if pd.notna(
                    r["min_rtt_ms"]) else 0.0,
                (pd.Timestamp(r["first_ts"]).strftime(
                    "%Y-%m-%d %H:%M:%S") if pd.notna(r["first_ts"]) else ""),
                (pd.Timestamp(r["last_ts"]).strftime(
                    "%Y-%m-%d %H:%M:%S") if pd.notna(r["last_ts"]) else ""),
            ]
            for j, v in enumerate(vals):
                item = QTableWidgetItem(str(v))
                if j in (1, 2, 3, 4, 5, 6):
                    item.setTextAlignment(Qt.AlignRight | Qt.AlignVCenter)
                self.table.setItem(i, j, item)
        self.table.resizeColumnsToContents()

    def plot_rtt(self, df: pd.DataFrame):
        ax = self.canvas_rtt.ax
        ax.clear()
        ok = df[(df["success"] == 1) & df["rtt_ms"].notna()]
        if ok.empty:
            ax.set_title("RTT Over Time")
            ax.set_xlabel("time")
            ax.set_ylabel("ms")
            self.canvas_rtt.draw()
            return
        for t, g in ok.groupby("target"):
            g2 = g.sort_values("timestamp").set_index("timestamp")
            y = g2["rtt_ms"].rolling(60, min_periods=1).mean()
            ax.plot(y.index, y.values, label=f"{t} (roll=60)")
        ax.set_title("RTT Over Time")
        ax.set_xlabel("Time")
        ax.set_ylabel("RTT (ms)")
        ax.legend(loc="best")
        self.canvas_rtt.draw()

    def plot_loss(self, summary: pd.DataFrame):
        ax = self.canvas_loss.ax
        ax.clear()
        if summary.empty:
            ax.set_title("Loss by Target")
            ax.set_xlabel("target")
            ax.set_ylabel("loss %")
            self.canvas_loss.draw()
            return
        ax.bar(summary["target"], summary["loss_%"])
        ax.set_title("Packet Loss by Target")
        ax.set_xlabel("Target")
        ax.set_ylabel("Loss (%)")
        for tick in ax.get_xticklabels():
            tick.set_rotation(20)
        self.canvas_loss.draw()

    def schedule_next_hour(self):
        now = dt.datetime.now()
        nxt = (now.replace(minute=0, second=0,
               microsecond=0) + dt.timedelta(hours=1))
        ms = int((nxt - now).total_seconds() * 1000)
        self.hour_timer.start(max(ms, 1000))

    def hourly_report(self):
        try:
            base_dir = self.le_outdir.text().strip() or os.path.abspath("net_out")
            current_day = day_str()
            day_dir = os.path.join(base_dir, current_day)
            csv_path = os.path.join(day_dir, "net_log.csv")
            if not os.path.exists(csv_path):
                QMessageBox.information(
                    self, "No data", f"Not found: {csv_path}")
                self.schedule_next_hour()
                return
            df = pd.read_csv(csv_path)
            if df.empty:
                self.schedule_next_hour()
                return
            df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")
            df = df[df["timestamp"].notna()]
            df["success"] = pd.to_numeric(
                df["success"], errors="coerce").fillna(0).astype(int)
            df["rtt_ms"] = pd.to_numeric(df["rtt_ms"], errors="coerce")

            grp = df.groupby("target")
            summary = pd.DataFrame({
                "Count": grp["success"].count(),
                "Success": grp["success"].sum(),
                "Loss_%": (1 - grp["success"].mean()) * 100.0,
                "Avg_RTT_ms": grp["rtt_ms"].mean(),
                "Max_RTT_ms": grp["rtt_ms"].max(),
                "Min_RTT_ms": grp["rtt_ms"].min(),
                "First_TS": grp["timestamp"].min(),
                "Last_TS": grp["timestamp"].max(),
            }).reset_index().sort_values("Loss_%", ascending=False)

            def _not_transient(s):
                try:
                    return not bool(TRANSIENT_ERR_REGEX.search(str(s)))
                except Exception:
                    return True
            losses = df[(df["success"] == 0) & df["error"].apply(_not_transient)].sort_values(
                "timestamp")[["timestamp", "target", "error"]]

            ts = dt.datetime.now().strftime("%Y%m%d_%H00")
            pdf_path = os.path.join(day_dir, f"report_{ts}.pdf")

            # Existing charts
            rtt_png = self.build_matplotlib_rtt_png(df)
            lossbar_png = self.build_matplotlib_lossbar_png(summary)

            # NEW: loss by time charts (bucket 5m)
            loss_timeline_png = self.build_loss_timeline_png(df, freq='5min')
            loss_top_periods_png = self.build_loss_top_periods_png(df, freq='5min', top_n=20)

            self.render_pdf_report(
                pdf_path, summary, df,
                rtt_png, lossbar_png,
                loss_timeline_png, loss_top_periods_png,
                losses
            )
            self.lbl_status.setText(f"Status: PDF saved {pdf_path}")
        except Exception as e:
            QMessageBox.warning(self, "Error", str(e))
        finally:
            self.schedule_next_hour()

    def build_matplotlib_rtt_png(self, df: pd.DataFrame) -> bytes:
        buf = io.BytesIO()
        fig, ax = plt.subplots(figsize=(11, 5), constrained_layout=True)
        ok = df[(df["success"] == 1) & df["rtt_ms"].notna()]
        if not ok.empty:
            for t, g in ok.groupby("target"):
                g2 = g.sort_values("timestamp").set_index("timestamp")
                y = g2["rtt_ms"].rolling(60, min_periods=1).mean()
                ax.plot(y.index, y.values, label=f"{t} (roll=60)")
            ax.set_title("RTT Over Time")
            ax.set_xlabel("Time")
            ax.set_ylabel("RTT (ms)")
            ax.legend(loc="best")
        fig.savefig(buf, format="png", dpi=150)
        plt.close(fig)
        buf.seek(0)
        return buf.read()

    def build_matplotlib_lossbar_png(self, summary: pd.DataFrame) -> bytes:
        buf = io.BytesIO()
        fig, ax = plt.subplots(figsize=(11, 5), constrained_layout=True)
        if not summary.empty:
            ax.bar(summary["target"], summary["Loss_%"])
            ax.set_title("Packet Loss by Target")
            ax.set_xlabel("Target")
            ax.set_ylabel("Loss (%)")
            for tick in ax.get_xticklabels():
                tick.set_rotation(20)
        fig.savefig(buf, format="png", dpi=150)
        plt.close(fig)
        buf.seek(0)
        return buf.read()

    # ===== NEW helpers for Loss by Time =====
    def _loss_by_bucket(self, df: pd.DataFrame, freq: str = '5min') -> pd.DataFrame:
        """
        Compute loss percentage per time bucket across all targets.
        Returns DataFrame with columns: bucket (datetime), Loss_% (float), Count (int)
        """
        if df.empty:
            return pd.DataFrame(columns=["bucket", "Loss_%", "Count"])
        x = df.copy()
        x["bucket"] = x["timestamp"].dt.floor(freq)
        byb = x.groupby("bucket")["success"].agg(["mean", "count"]).reset_index()
        byb["Loss_%"] = (1 - byb["mean"]) * 100.0
        byb.rename(columns={"count": "Count"}, inplace=True)
        return byb[["bucket", "Loss_%", "Count"]].sort_values("bucket")

    def build_loss_timeline_png(self, df: pd.DataFrame, freq: str = '5min') -> bytes:
        """Timeline (ordered by time) of Loss % per bucket."""
        buf = io.BytesIO()
        fig, ax = plt.subplots(figsize=(11, 5), constrained_layout=True)
        byb = self._loss_by_bucket(df, freq=freq)
        if not byb.empty:
            ax.plot(byb["bucket"], byb["Loss_%"])
            ax.set_title(f"Loss % over Time ({freq} buckets)")
            ax.set_xlabel("Time")
            ax.set_ylabel("Loss (%)")
        fig.autofmt_xdate()
        fig.savefig(buf, format="png", dpi=150)
        plt.close(fig)
        buf.seek(0)
        return buf.read()

    def build_loss_top_periods_png(self, df: pd.DataFrame, freq: str = '5min', top_n: int = 20) -> bytes:
        """Top-N periods with highest Loss %, shown as a horizontal bar chart."""
        buf = io.BytesIO()
        fig, ax = plt.subplots(figsize=(11, 7), constrained_layout=True)
        byb = self._loss_by_bucket(df, freq=freq)
        if not byb.empty:
            top = byb.sort_values("Loss_%", ascending=False).head(top_n).copy()
            # Show worst at top
            top = top.sort_values("Loss_%", ascending=True)
            labels = top["bucket"].dt.strftime("%Y-%m-%d %H:%M")
            ax.barh(labels, top["Loss_%"])
            ax.set_title(f"Top {len(top)} Lossy Periods ({freq} buckets)")
            ax.set_xlabel("Loss (%)")
            ax.set_ylabel("Period start")
        fig.savefig(buf, format="png", dpi=150)
        plt.close(fig)
        buf.seek(0)
        return buf.read()
    # ===== END NEW helpers =====

    def render_pdf_report(self, path: str, summary: pd.DataFrame, df: pd.DataFrame,
                          rtt_png: bytes, lossbar_png: bytes,
                          loss_timeline_png: bytes, loss_top_periods_png: bytes,
                          losses_df: pd.DataFrame):
        doc = SimpleDocTemplate(path, pagesize=A4, leftMargin=18*mm,
                                rightMargin=18*mm, topMargin=14*mm, bottomMargin=14*mm)
        styles = getSampleStyleSheet()
        title = styles['Title']
        subtitle = styles['Heading2']
        normal = styles['Normal']
        elems = []
        elems.append(Paragraph("Network Connectivity Report", title))
        elems.append(Paragraph(dt.datetime.now().strftime(
            "Generated on %Y-%m-%d %H:%M:%S"), normal))
        tgts = ", ".join(sorted(df["target"].astype(str).unique()))
        elems.append(Paragraph(f"Targets: {tgts}", normal))
        elems.append(Spacer(1, 8))
        table_data = [["Target", "Count", "Success", "Loss %",
                       "Avg RTT (ms)", "Max RTT (ms)", "Min RTT (ms)", "First TS", "Last TS"]]
        for _, r in summary.iterrows():
            table_data.append([
                r["target"],
                int(r["Count"]) if pd.notna(r["Count"]) else 0,
                int(r["Success"]) if pd.notna(r["Success"]) else 0,
                f"{round(float(r['Loss_%']), 2) if pd.notna(r['Loss_%']) else 0.0}",
                f"{round(float(r['Avg_RTT_ms']), 2) if pd.notna(r['Avg_RTT_ms']) else 0.0}",
                f"{round(float(r['Max_RTT_ms']), 2) if pd.notna(r['Max_RTT_ms']) else 0.0}",
                f"{round(float(r['Min_RTT_ms']), 2) if pd.notna(r['Min_RTT_ms']) else 0.0}",
                (pd.Timestamp(r["First_TS"]).strftime(
                    "%Y-%m-%d %H:%M:%S") if pd.notna(r["First_TS"]) else ""),
                (pd.Timestamp(r["Last_TS"]).strftime(
                    "%Y-%m-%d %H:%M:%S") if pd.notna(r["Last_TS"]) else ""),
            ])
        t = Table(table_data, hAlign='LEFT', repeatRows=1)
        t.setStyle(TableStyle([
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#f1f3f5")),
            ('GRID', (0, 0), (-1, -1), 0.3, colors.HexColor("#cbd5e1")),
            ('ALIGN', (1, 1), (-1, -1), 'RIGHT'),
            ('ALIGN', (0, 0), (0, -1), 'LEFT'),
            ('ROWBACKGROUNDS', (0, 1), (-1, -1),
             [colors.white, colors.HexColor("#fafafa")]),
            ('FONTSIZE', (0, 0), (-1, -1), 9),
            ('TOPPADDING', (0, 0), (-1, -1), 4),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 4),
        ]))
        elems.append(t)
        elems.append(Spacer(1, 10))
        elems.append(Paragraph("Charts", subtitle))

        if rtt_png:
            elems.append(Paragraph("RTT Over Time", styles["Heading3"]))
            img1 = Image(io.BytesIO(rtt_png))
            img1._restrictSize(170*mm, 90*mm)
            elems.append(img1)

        elems.append(Spacer(1, 6))
        if lossbar_png:
            elems.append(Paragraph("Packet Loss by Target", styles["Heading3"]))
            img2 = Image(io.BytesIO(lossbar_png))
            img2._restrictSize(170*mm, 90*mm)
            elems.append(img2)

        # NEW: insert loss-by-time charts
        elems.append(Spacer(1, 6))
        if loss_timeline_png:
            elems.append(Paragraph("Loss % over Time (5-min buckets)", styles["Heading3"]))
            img3 = Image(io.BytesIO(loss_timeline_png))
            img3._restrictSize(170*mm, 90*mm)
            elems.append(img3)

        elems.append(Spacer(1, 6))
        if loss_top_periods_png:
            elems.append(Paragraph("Top Lossy Periods (5-min buckets)", styles["Heading3"]))
            img4 = Image(io.BytesIO(loss_top_periods_png))
            img4._restrictSize(170*mm, 110*mm)
            elems.append(img4)

        elems.append(PageBreak())
        elems.append(Paragraph("Loss Events (failed pings only)", subtitle))
        if losses_df is not None and not losses_df.empty:
            show = losses_df.copy()
            show["Time"] = show["timestamp"].dt.strftime("%Y-%m-%d %H:%M:%S")
            odata = [["Time", "Target", "Error"]]
            for _, r in show[["Time", "target", "error"]].iterrows():
                odata.append(
                    [r["Time"], r["target"], str(r["error"] or "")[:120]])
            ot = Table(odata, hAlign='LEFT', repeatRows=1,
                       colWidths=[55*mm, 40*mm, 80*mm])
            ot.setStyle(TableStyle([
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor("#ffe3e3")),
                ('GRID', (0, 0), (-1, -1), 0.3, colors.HexColor("#e03131")),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('FONTSIZE', (0, 0), (-1, -1), 8.5),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1),
                 [colors.white, colors.HexColor("#fff5f5")]),
                ('TOPPADDING', (0, 0), (-1, -1), 3),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 3),
            ]))
            elems.append(ot)
        else:
            elems.append(
                Paragraph("No loss events detected in the current period.", styles['Normal']))
        doc.build(elems)


def main():
    app = QApplication(sys.argv)
    w = MainWindow()
    w.show()
    ret = app.exec()
    if w.thread.isRunning():
        w.worker.stop()
        w.thread.quit()
        w.thread.wait()
    sys.exit(ret)


if __name__ == "__main__":
    main()
