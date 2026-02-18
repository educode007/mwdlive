try:
    import eventlet

    eventlet.monkey_patch()
except Exception:
    pass

import argparse
import csv
import io
import json
import os
import signal
import sqlite3
import sys
import threading
import time
import traceback
import urllib.error
import urllib.request
from typing import Any

from flask import Flask, jsonify, render_template, request, Response
from flask_socketio import SocketIO

from serial_reader import SerialReader, list_serial_ports, parse_serial_config
from wits_parser import parse_wits_value_line


app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

NO_WEB = False

latest_lock = threading.Lock()
latest_values: dict[str, float] = {}

decoder_lock = threading.Lock()
start_time = time.monotonic()
last_tfa_update = 0
pump_up_start_time: float | None = None
pump_down_start_time: float | None = None
seen_0121_count = 0
did_initial_0121_reset = False
last_pump_reset_seq_logged: int | None = None
decoder_state: dict[str, Any] = {
    "title": "Compass Rose - Live Mode",
    "network_id": "618410",
    "pressure_psi": None,
    "dec_deg": 6.81,
    "dao_deg": 0.0,
    "pump_on": False,
    "pump_down_seconds": 0,
    "uptime_seconds": 0,
    "shk1": None,
    "vib1": None,
    "magf": None,
    "inc": None,
    "azm": None,
    "bat2_on": None,
    "center_label": "gTFA",
    "center_value": None,
    "center_counter_seconds": 0,
    "tfa_tick": 0,
    "reset_seq": 0,
    "render_publish_ok": None,
    "render_publish_ts": None,
    "render_publish_error": None,
    "grav": None,
    "dipa": None,
    "temp": None,
}

ingest_lock = threading.Lock()
last_ingest: dict[str, Any] | None = None

plotter_lock = threading.Lock()
last_plotter_snapshot: dict[str, Any] | None = None

serial_lock = threading.Lock()
serial_reader: SerialReader | None = None
serial_config_snapshot: dict[str, Any] = {
    "serial_port": None,
    "baudrate": 9600,
    "bytesize": 8,
    "parity": "N",
    "stopbits": "1",
    "timeout": 1.0,
    "pressure_code": "0121",
    "inc_code": "0713",
    "azm_code": "0715",
    "mtf_code": "0716",
    "gtf_code": "0717",
    "magf_code": "0732",
    "dipa_code": "0746",
    "temp_code": "0751",
    "grav_code": "0747",
    "shk1_code": "0736",
    "vib1_code": "0737",
    "bat2_code": "0735",
    "pump_on_threshold": 300.0,
    "ingest_url": "",
    "ingest_key": "",
    "ingest_interval_seconds": 5.0,
}


def _cfg_num(key: str, default: float) -> float:
    with serial_lock:
        v = serial_config_snapshot.get(key)
    try:
        out = float(v)
        if not (out == out):
            return default
        return out
    except Exception:
        return default


def _cfg_str(key: str, default: str) -> str:
    with serial_lock:
        v = serial_config_snapshot.get(key)
    if v is None:
        return default
    s = str(v).strip()
    return s if s else default


def _cfg_float(key: str, default: float) -> float:
    with serial_lock:
        v = serial_config_snapshot.get(key)
    try:
        out = float(v)
        if not (out == out):
            return default
        return out
    except Exception:
        return default


def _latest_value(code: str) -> float | None:
    with latest_lock:
        v = latest_values.get(code)
    if isinstance(v, (int, float)):
        return float(v)
    return None


def _update_decoder_from_latest() -> dict[str, Any] | None:
    global pump_up_start_time, pump_down_start_time

    pressure_code = _cfg_str("pressure_code", "0121")
    inc_code = _cfg_str("inc_code", "0713")
    azm_code = _cfg_str("azm_code", "0715")
    mtf_code = _cfg_str("mtf_code", "0716")
    gtf_code = _cfg_str("gtf_code", "0717")
    shk1_code = _cfg_str("shk1_code", "0736")
    vib1_code = _cfg_str("vib1_code", "0737")
    grav_code = _cfg_str("grav_code", "0747")
    magf_code = _cfg_str("magf_code", "0732")
    dipa_code = _cfg_str("dipa_code", "0746")
    temp_code = _cfg_str("temp_code", "0751")
    bat2_code = _cfg_str("bat2_code", "0735")
    pump_threshold = _cfg_float("pump_on_threshold", 300.0)

    changed = False
    now = time.monotonic()

    pressure = _latest_value(pressure_code)
    inc = _latest_value(inc_code)
    azm = _latest_value(azm_code)
    shk1 = _latest_value(shk1_code)
    vib1 = _latest_value(vib1_code)
    grav = _latest_value(grav_code)
    magf = _latest_value(magf_code)
    dipa = _latest_value(dipa_code)
    temp = _latest_value(temp_code)
    bat2_raw = _latest_value(bat2_code)

    did_pump_reset = False
    snapshot_after_reset: dict[str, Any] | None = None

    with decoder_lock:
        if pressure is not None:
            if decoder_state.get("pressure_psi") != pressure:
                decoder_state["pressure_psi"] = pressure
                changed = True

            is_pump_on = float(pressure) >= float(pump_threshold)
            was_pump_on = bool(decoder_state.get("pump_on"))
            if is_pump_on and not was_pump_on:
                decoder_state["pump_on"] = True
                pump_up_start_time = now
                pump_down_start_time = None
                decoder_state["pump_down_seconds"] = 0
                decoder_state["uptime_seconds"] = 0
                decoder_state["inc"] = None
                decoder_state["azm"] = None
                decoder_state["center_value"] = None
                decoder_state["shk1"] = None
                decoder_state["vib1"] = None
                decoder_state["grav"] = None
                decoder_state["magf"] = None
                decoder_state["dipa"] = None
                decoder_state["temp"] = None
                decoder_state["tfa_tick"] = 0
                try:
                    decoder_state["reset_seq"] = int(decoder_state.get("reset_seq", 0)) + 1
                except Exception:
                    decoder_state["reset_seq"] = 1
                changed = True

                did_pump_reset = True
                snapshot_after_reset = dict(decoder_state)
            elif (not is_pump_on) and was_pump_on:
                decoder_state["pump_on"] = False
                pump_down_start_time = now
                changed = True

        # If we just detected Pump Off -> Pump On, we intentionally keep bullets empty
        # until new serial data arrives. Do NOT repopulate from previous latest_values.
        if not did_pump_reset:
            if inc is not None and decoder_state.get("inc") != inc:
                decoder_state["inc"] = inc
                changed = True
            if azm is not None and decoder_state.get("azm") != azm:
                decoder_state["azm"] = azm
                changed = True

            if shk1 is not None and decoder_state.get("shk1") != shk1:
                decoder_state["shk1"] = shk1
                changed = True
            if vib1 is not None and decoder_state.get("vib1") != vib1:
                decoder_state["vib1"] = vib1
                changed = True
            if grav is not None and decoder_state.get("grav") != grav:
                decoder_state["grav"] = grav
                changed = True
            if magf is not None and decoder_state.get("magf") != magf:
                decoder_state["magf"] = magf
                changed = True
            if dipa is not None and decoder_state.get("dipa") != dipa:
                decoder_state["dipa"] = dipa
                changed = True
            if temp is not None and decoder_state.get("temp") != temp:
                decoder_state["temp"] = temp
                changed = True

            if bat2_raw is not None:
                bat2_on = bool(float(bat2_raw) >= 0.5)
                if decoder_state.get("bat2_on") != bat2_on:
                    decoder_state["bat2_on"] = bat2_on
                    changed = True

            inc_for_center = decoder_state.get("inc")
            use_gtf = isinstance(inc_for_center, (int, float)) and float(inc_for_center) >= 3.0
            center_code = gtf_code if use_gtf else mtf_code
            center_value = _latest_value(center_code)
            if center_value is not None and decoder_state.get("center_value") != center_value:
                decoder_state["center_value"] = center_value
                changed = True

        snapshot = dict(decoder_state)

    if did_pump_reset and snapshot_after_reset is not None:
        with latest_lock:
            latest_values.clear()
        return snapshot_after_reset

    return snapshot if changed else None


def choose_serial_port_interactive() -> str:
    ports = list_serial_ports()
    if not ports:
        print("No se encontraron puertos seriales.")
        sys.exit(1)

    print("Puertos disponibles:")
    for idx, port in enumerate(ports, start=1):
        print(f"{idx}) {port}")

    while True:
        choice = input("Elegí el puerto (número o nombre, ej: 1 o COM3): ").strip()
        if not choice:
            continue

        if choice.isdigit():
            sel = int(choice)
            if 1 <= sel <= len(ports):
                return ports[sel - 1]
        else:
            for port in ports:
                if port.lower() == choice.lower():
                    return port

        print("Selección inválida.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="MWD Monitor: lectura de puerto serial WITSML y emisión por WebSocket."
    )
    parser.add_argument(
        "--serial-port",
        dest="serial_port",
        help="Puerto serial (ej: COM3).",
    )
    parser.add_argument("--baudrate", type=int, default=9600)
    parser.add_argument("--bytesize", type=int, choices=[5, 6, 7, 8], default=8)
    parser.add_argument(
        "--parity",
        type=str,
        choices=["N", "E", "O", "M", "S", "n", "e", "o", "m", "s"],
        default="N",
    )
    parser.add_argument("--stopbits", type=str, choices=["1", "1.5", "2"], default="1")
    parser.add_argument("--timeout", type=float, default=1.0)
    parser.add_argument("--list-ports", action="store_true")
    parser.add_argument("--web-host", default="0.0.0.0")
    parser.add_argument("--web-port", type=int, default=5000)
    parser.add_argument("--no-web", action="store_true")
    parser.add_argument("--no-serial", action="store_true")
    return parser


@app.route("/")
def index() -> str:
    return render_template("index.html")


@app.route("/plotter")
def plotter():
    return render_template(
        "plotter.html",
        desktop_ingest_url=_cfg_str("ingest_url", ""),
        desktop_ingest_key=_cfg_str("ingest_key", ""),
    )


@app.route("/plotter/viewer")
def plotter_viewer():
    return render_template(
        "plotter.html",
        desktop_ingest_url="",
        desktop_ingest_key="",
        viewer_mode="1",
    )


@app.route("/anticollision")
def anticollision():
    return render_template("anticollision.html")


@app.route("/api/plotter/publish", methods=["POST"])
def api_plotter_publish():
    global last_plotter_snapshot

    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return ("Bad Request", 400)

    plotter_payload = _extract_plotter_payload(data)
    with plotter_lock:
        last_plotter_snapshot = plotter_payload
    _save_plotter_state(plotter_payload)

    url = _cfg_str("ingest_url", "").strip()
    key = _cfg_str("ingest_key", "").strip()
    if not url or not key:
        return jsonify({"ok": False, "error": "ingest_url/ingest_key no configurados"}), 400

    url = url.rstrip("/")
    if not url.lower().endswith("/api/ingest"):
        url = url + "/api/ingest"

    ok, err = _publish_to_render(url, key, plotter_payload)
    return jsonify({"ok": bool(ok), "error": err})


def _user_data_dir() -> str:
    root = os.environ.get("MWDMONITOR_USERDATA")
    if root:
        return root
    return os.getcwd()


def _serial_config_path() -> str:
    return os.path.join(_user_data_dir(), "serial_config.json")


def _plotter_state_path() -> str:
    return os.path.join(_user_data_dir(), "plotter_state.json")


def _load_serial_config() -> None:
    global serial_config_snapshot
    path = _serial_config_path()
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if isinstance(raw, dict):
            merged = dict(serial_config_snapshot)
            merged.update(raw)
            serial_config_snapshot = merged
    except FileNotFoundError:
        return
    except Exception:
        return


def _save_serial_config() -> None:
    path = _serial_config_path()
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(serial_config_snapshot, f, ensure_ascii=False, indent=2)
    except Exception:
        return


def _normalize_plotter_rows(rows: Any) -> list[dict[str, float]]:
    if not isinstance(rows, list):
        return []
    out: list[dict[str, float]] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        try:
            md = float(r.get("md"))
            inc = float(r.get("inc"))
            azm = float(r.get("azm"))
        except Exception:
            continue
        if not (md == md and inc == inc and azm == azm):
            continue
        out.append({"md": md, "inc": inc, "azm": azm})
    out.sort(key=lambda x: x["md"])
    return out


def _extract_plotter_payload(data: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {
        "well_id": str(data.get("well_id") or "default"),
        "source": str(data.get("source") or "desktop_plotter"),
        "ts": time.time(),
        "surveys": {
            "real": [],
            "proposal": [],
        },
    }

    try:
        ts = float(data.get("ts"))
        if ts == ts:
            out["ts"] = ts
    except Exception:
        pass

    try:
        seq = int(data.get("seq"))
        out["seq"] = seq
    except Exception:
        pass

    try:
        vsp = float(data.get("vsp"))
        if vsp == vsp:
            out["vsp"] = vsp
    except Exception:
        pass

    surveys = data.get("surveys")
    if isinstance(surveys, dict):
        out["surveys"] = {
            "real": _normalize_plotter_rows(surveys.get("real")),
            "proposal": _normalize_plotter_rows(surveys.get("proposal")),
        }
    return out


def _load_plotter_state() -> None:
    global last_plotter_snapshot

    path = _plotter_state_path()
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        if not isinstance(raw, dict):
            return
        payload = _extract_plotter_payload(raw)
        with plotter_lock:
            last_plotter_snapshot = payload
    except FileNotFoundError:
        return
    except Exception:
        return


def _save_plotter_state(payload: dict[str, Any]) -> None:
    path = _plotter_state_path()
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except Exception:
        return


def stop_serial_reader() -> None:
    global serial_reader
    with serial_lock:
        reader = serial_reader
        serial_reader = None
    if reader is None:
        return
    try:
        reader.stop()
        reader.join(timeout=2)
    except Exception:
        pass


def start_serial_reader_from_snapshot() -> None:
    global serial_reader
    with serial_lock:
        cfg = dict(serial_config_snapshot)

    port = cfg.get("serial_port")
    if not port:
        return

    try:
        tmp = argparse.Namespace(
            serial_port=str(port),
            baudrate=int(cfg.get("baudrate") or 9600),
            bytesize=int(cfg.get("bytesize") or 8),
            parity=str(cfg.get("parity") or "N"),
            stopbits=str(cfg.get("stopbits") or "1"),
            timeout=float(cfg.get("timeout") if cfg.get("timeout") is not None else 1.0),
        )
        serial_cfg = parse_serial_config(tmp)
    except Exception as exc:
        print(f"Error en configuración serial guardada: {exc}")
        return

    reader = SerialReader(serial_cfg, on_line=handle_line)
    reader.start()
    with serial_lock:
        serial_reader = reader


def restart_serial_reader() -> None:
    stop_serial_reader()
    start_serial_reader_from_snapshot()


@app.route("/api/serial/ports", methods=["GET"])
def api_serial_ports():
    try:
        ports = list_serial_ports()
    except Exception:
        ports = []
    return jsonify({"ports": ports})


@app.route("/api/config/serial", methods=["GET", "POST"])
def api_config_serial():
    global serial_config_snapshot
    if request.method == "GET":
        with serial_lock:
            snap = dict(serial_config_snapshot)
        return jsonify(snap)

    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return ("Bad Request", 400)

    next_snap = dict(serial_config_snapshot)
    for key in [
        "serial_port",
        "baudrate",
        "bytesize",
        "parity",
        "stopbits",
        "timeout",
        "pressure_code",
        "inc_code",
        "azm_code",
        "mtf_code",
        "gtf_code",
        "magf_code",
        "dipa_code",
        "temp_code",
        "grav_code",
        "shk1_code",
        "vib1_code",
        "bat2_code",
        "pump_on_threshold",
        "ingest_url",
        "ingest_key",
        "ingest_interval_seconds",
    ]:
        if key in data:
            next_snap[key] = data.get(key)

    port = next_snap.get("serial_port")
    if port is not None and str(port).strip() == "":
        next_snap["serial_port"] = None

    serial_config_snapshot = next_snap
    _save_serial_config()
    restart_serial_reader()
    return jsonify({"ok": True})


def _get_ingest_settings() -> tuple[str, str, float]:
    url = _cfg_str("ingest_url", "").strip()
    key = _cfg_str("ingest_key", "").strip()
    interval = _cfg_num("ingest_interval_seconds", 5.0)
    interval = max(1.0, min(60.0, interval))
    return url, key, interval


def _build_publish_payload() -> dict[str, Any]:
    with decoder_lock:
        snap = dict(decoder_state)
    with latest_lock:
        wits = dict(latest_values)
    with plotter_lock:
        plotter = dict(last_plotter_snapshot) if isinstance(last_plotter_snapshot, dict) else None

    snap["ts"] = time.time()
    snap["wits"] = wits

    if plotter:
        if "well_id" in plotter:
            snap["well_id"] = plotter.get("well_id")
        if "source" in plotter:
            snap["source"] = plotter.get("source")
        if "seq" in plotter:
            snap["seq"] = plotter.get("seq")
        if "vsp" in plotter:
            snap["vsp"] = plotter.get("vsp")
        surveys = plotter.get("surveys")
        if isinstance(surveys, dict):
            snap["surveys"] = surveys

    return snap


def _publish_to_render(url: str, key: str, payload: dict[str, Any]) -> tuple[bool, str | None]:
    if not url:
        return False, "ingest_url vacío"
    if not key:
        return False, "ingest_key vacío"

    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST")
    req.add_header("Content-Type", "application/json")
    req.add_header("Authorization", f"Bearer {key}")

    try:
        with urllib.request.urlopen(req, timeout=8) as resp:
            status = int(getattr(resp, "status", 200))
            if 200 <= status < 300:
                return True, None
            return False, f"HTTP {status}"
    except urllib.error.HTTPError as exc:
        return False, f"HTTP {exc.code}"
    except Exception as exc:
        return False, str(exc)


def render_publisher_background_task() -> None:
    while True:
        url, key, interval = _get_ingest_settings()
        if not url or not key:
            with decoder_lock:
                decoder_state["render_publish_ok"] = False
                decoder_state["render_publish_ts"] = None
                decoder_state["render_publish_error"] = "OFF (configurar Render URL/Key)"
                snapshot = dict(decoder_state)
            if not NO_WEB:
                socketio.emit("decoder_state", snapshot)
            time.sleep(interval)
            continue

        payload = _build_publish_payload()
        ok, err = _publish_to_render(url, key, payload)
        with decoder_lock:
            decoder_state["render_publish_ok"] = bool(ok)
            decoder_state["render_publish_ts"] = time.time()
            decoder_state["render_publish_error"] = None if ok else err
            snapshot = dict(decoder_state)
        if not NO_WEB:
            socketio.emit("decoder_state", snapshot)

        time.sleep(interval)


def _pg_dsn() -> str | None:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        return None
    return dsn


def _pg_connect():
    dsn = _pg_dsn()
    if not dsn:
        return None
    try:
        import psycopg2
    except Exception:
        return None
    return psycopg2.connect(dsn)


def _pg_ensure_schema() -> None:
    con = _pg_connect()
    if con is None:
        return
    try:
        with con:
            with con.cursor() as cur:
                cur.execute(
                    "CREATE TABLE IF NOT EXISTS ingest_snapshots ("
                    "  id BIGSERIAL PRIMARY KEY,"
                    "  ts DOUBLE PRECISION NOT NULL,"
                    "  payload JSONB NOT NULL"
                    ");"
                )
                cur.execute("CREATE INDEX IF NOT EXISTS idx_ingest_ts ON ingest_snapshots(ts);")
                cur.execute(
                    "CREATE TABLE IF NOT EXISTS incazm_log ("
                    "  id BIGSERIAL PRIMARY KEY,"
                    "  ts DOUBLE PRECISION NOT NULL,"
                    "  name TEXT NOT NULL,"
                    "  value DOUBLE PRECISION NOT NULL"
                    ");"
                )
                cur.execute("CREATE INDEX IF NOT EXISTS idx_incazm_ts ON incazm_log(ts);")
    finally:
        con.close()


def _pg_add_incazm(ts_epoch: float, name: str, value: float) -> None:
    try:
        _pg_ensure_schema()
        con = _pg_connect()
        if con is None:
            return
        try:
            with con:
                with con.cursor() as cur:
                    cur.execute(
                        "INSERT INTO incazm_log(ts, name, value) VALUES (%s, %s, %s);",
                        (float(ts_epoch), str(name), float(value)),
                    )
        finally:
            con.close()
    except Exception:
        pass


def _pg_read_incazm(limit: int = 20000) -> list[tuple[float, str, float]]:
    limit = max(1, min(20000, int(limit)))
    try:
        _pg_ensure_schema()
        con = _pg_connect()
        if con is None:
            return []
        try:
            with con.cursor() as cur:
                cur.execute(
                    "SELECT ts, name, value FROM incazm_log ORDER BY ts ASC LIMIT %s;",
                    (limit,),
                )
                rows = cur.fetchall()
        finally:
            con.close()
        out: list[tuple[float, str, float]] = []
        for r in rows or []:
            try:
                out.append((float(r[0]), str(r[1]), float(r[2])))
            except Exception:
                continue
        return out
    except Exception:
        return []


def _db_path() -> str:
    override = os.environ.get("MWDMONITOR_DB")
    if override:
        return override
    user_dir = os.environ.get("MWDMONITOR_USERDATA")
    if user_dir:
        try:
            os.makedirs(user_dir, exist_ok=True)
        except Exception:
            pass
        return os.path.join(user_dir, "mwdmonitor.db")
    return os.path.join(os.getcwd(), "mwdmonitor.db")


def _db_connect() -> sqlite3.Connection:
    con = sqlite3.connect(_db_path(), timeout=10)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute(
        "CREATE TABLE IF NOT EXISTS ingest_snapshots ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  ts REAL NOT NULL,"
        "  payload TEXT NOT NULL"
        ");"
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_ingest_ts ON ingest_snapshots(ts);")
    con.execute(
        "CREATE TABLE IF NOT EXISTS incazm_log ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  ts REAL NOT NULL,"
        "  name TEXT NOT NULL,"
        "  value REAL NOT NULL"
        ");"
    )
    con.execute("CREATE INDEX IF NOT EXISTS idx_incazm_ts ON incazm_log(ts);")
    return con


def _db_add_incazm(ts_epoch: float, name: str, value: float) -> None:
    try:
        con = _db_connect()
        try:
            con.execute(
                "INSERT INTO incazm_log(ts, name, value) VALUES (?, ?, ?);",
                (float(ts_epoch), str(name), float(value)),
            )
            con.commit()
        finally:
            con.close()
    except Exception:
        pass


def _db_read_incazm(limit: int = 20000) -> list[tuple[float, str, float]]:
    limit = max(1, min(20000, int(limit)))
    try:
        con = _db_connect()
        try:
            rows = con.execute(
                "SELECT ts, name, value FROM incazm_log ORDER BY ts ASC LIMIT ?;",
                (limit,),
            ).fetchall()
        finally:
            con.close()
        out: list[tuple[float, str, float]] = []
        for r in rows or []:
            try:
                out.append((float(r[0]), str(r[1]), float(r[2])))
            except Exception:
                continue
        return out
    except Exception:
        return []


@app.route("/api/incazm/log", methods=["GET"])
def api_incazm_log():
    limit_raw = request.args.get("limit", "20000")
    try:
        limit = int(limit_raw)
    except Exception:
        limit = 20000
    rows = _pg_read_incazm(limit=limit) if _pg_dsn() else _db_read_incazm(limit=limit)
    items = []
    for ts, name, value in rows:
        items.append({"ts": ts, "name": name, "value": value})
    return jsonify({"items": items})


@app.route("/api/incazm/export.csv", methods=["GET"])
def api_incazm_export_csv():
    rows = _pg_read_incazm(limit=20000) if _pg_dsn() else _db_read_incazm(limit=20000)
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["ts", "name", "value"])
    for ts, name, value in rows:
        w.writerow([ts, name, value])
    data = buf.getvalue().encode("utf-8")
    return Response(
        data,
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=inc_azm_log.csv"},
    )


def _retention_seconds() -> int:
    return int(os.environ.get("INGEST_RETENTION_SECONDS", str(48 * 3600)))


def _auth_ingest() -> bool:
    expected = os.environ.get("INGEST_API_KEY")
    if not expected:
        return False
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return False
    token = auth.removeprefix("Bearer ").strip()
    return token == expected


@app.route("/api/build", methods=["GET"])
def api_build():
    branch = os.environ.get("RENDER_GIT_BRANCH") or "unknown"
    commit = os.environ.get("RENDER_GIT_COMMIT") or "unknown"
    service = os.environ.get("RENDER_SERVICE_NAME") or ""
    return jsonify(
        {
            "branch": branch,
            "commit": commit,
            "service": service,
        }
    )


@app.route("/api/state", methods=["GET"])
def api_state():
    with ingest_lock:
        snap = last_ingest
    if snap is not None:
        return jsonify(snap)

    if _pg_dsn():
        try:
            _pg_ensure_schema()
            con = _pg_connect()
            if con is None:
                return jsonify({})
            try:
                with con.cursor() as cur:
                    cur.execute("SELECT payload FROM ingest_snapshots ORDER BY ts DESC LIMIT 1;")
                    row = cur.fetchone()
                if not row:
                    return jsonify({})
                return jsonify(row[0])
            finally:
                con.close()
        except Exception:
            return jsonify({})

    try:
        con = _db_connect()
        try:
            row = con.execute("SELECT payload FROM ingest_snapshots ORDER BY ts DESC LIMIT 1;").fetchone()
        finally:
            con.close()
        if not row:
            return jsonify({})
        return jsonify(json.loads(row[0]))
    except Exception:
        return jsonify({})


@app.route("/api/history", methods=["GET"])
def api_history():
    hours_raw = request.args.get("hours", "48")
    try:
        hours = float(hours_raw)
    except ValueError:
        hours = 48.0
    hours = max(0.0, min(48.0, hours))
    cutoff = time.time() - hours * 3600.0

    if _pg_dsn():
        try:
            _pg_ensure_schema()
            con = _pg_connect()
            if con is None:
                return jsonify({"items": []})
            try:
                with con.cursor() as cur:
                    cur.execute(
                        "SELECT ts, payload FROM ingest_snapshots WHERE ts >= %s ORDER BY ts ASC;",
                        (cutoff,),
                    )
                    rows = cur.fetchall()
                out = [{"ts": float(ts), "payload": payload} for (ts, payload) in rows]
                return jsonify({"items": out})
            finally:
                con.close()
        except Exception:
            return jsonify({"items": []})

    con = _db_connect()
    try:
        rows = con.execute(
            "SELECT ts, payload FROM ingest_snapshots WHERE ts >= ? ORDER BY ts ASC;",
            (cutoff,),
        ).fetchall()
    finally:
        con.close()

    out = []
    for ts, payload in rows:
        try:
            out.append({"ts": ts, "payload": json.loads(payload)})
        except Exception:
            continue
    return jsonify({"items": out})


@app.route("/api/ingest", methods=["POST"])
def api_ingest():
    global last_ingest

    if not _auth_ingest():
        return ("Unauthorized", 401)

    payload = request.get_json(silent=True)
    if not isinstance(payload, dict):
        return ("Bad Request", 400)

    now = time.time()
    try:
        ts = payload.get("ts")
        if isinstance(ts, (int, float)):
            ts_epoch = float(ts)
        else:
            ts_epoch = now
    except Exception:
        ts_epoch = now

    try:
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    except Exception:
        return ("Bad Request", 400)

    with ingest_lock:
        last_ingest = payload

    if _pg_dsn():
        try:
            _pg_ensure_schema()
            con = _pg_connect()
            if con is not None:
                try:
                    with con:
                        with con.cursor() as cur:
                            cur.execute(
                                "INSERT INTO ingest_snapshots(ts, payload) VALUES (%s, %s::jsonb);",
                                (ts_epoch, encoded),
                            )
                            cutoff = now - _retention_seconds()
                            cur.execute("DELETE FROM ingest_snapshots WHERE ts < %s;", (cutoff,))
                finally:
                    con.close()
        except Exception:
            pass
    else:
        con = _db_connect()
        try:
            con.execute(
                "INSERT INTO ingest_snapshots(ts, payload) VALUES (?, ?);",
                (ts_epoch, encoded),
            )
            cutoff = now - _retention_seconds()
            con.execute("DELETE FROM ingest_snapshots WHERE ts < ?;", (cutoff,))
            con.commit()
        finally:
            con.close()

    socketio.emit("state_update", payload)
    socketio.emit("decoder_state", payload)
    try:
        w = payload.get("wits") if isinstance(payload, dict) else None
        if isinstance(w, dict):
            socketio.emit(
                "wits_values",
                {
                    "0108": w.get("0108"),
                    "0110": w.get("0110"),
                },
            )
    except Exception:
        pass
    return jsonify({"ok": True})


@socketio.on("connect")
def on_connect() -> None:
    with ingest_lock:
        ing = last_ingest

    if isinstance(ing, dict):
        w = ing.get("wits")
        if isinstance(w, dict):
            payload = {
                "0108": w.get("0108"),
                "0110": w.get("0110"),
            }
            if not NO_WEB:
                socketio.emit("wits_values", payload)

        if not NO_WEB:
            socketio.emit("decoder_state", ing)
        return

    with latest_lock:
        payload = {
            "0108": latest_values.get("0108"),
            "0110": latest_values.get("0110"),
        }
    if not NO_WEB:
        socketio.emit("wits_values", payload)

    with decoder_lock:
        snap = dict(decoder_state)
    if not NO_WEB:
        socketio.emit("decoder_state", snap)


def decoder_background_task() -> None:
    global last_tfa_update, pump_down_start_time, last_pump_reset_seq_logged
    while True:
        time.sleep(1)
        did_log = False
        log_rows: list[dict[str, Any]] = []
        with decoder_lock:
            uptime = int(time.monotonic() - start_time)
            now = time.monotonic()
            if decoder_state.get("pump_on"):
                if pump_up_start_time is not None:
                    decoder_state["uptime_seconds"] = int(now - pump_up_start_time)
                decoder_state["pump_down_seconds"] = 0
            else:
                if pump_down_start_time is not None:
                    decoder_state["pump_down_seconds"] = int(now - pump_down_start_time)

            decoder_state["center_counter_seconds"] = uptime % 3600
            inc = decoder_state.get("inc")
            if isinstance(inc, (int, float)) and inc < 3:
                decoder_state["center_label"] = "mTFA"
            else:
                decoder_state["center_label"] = "gTFA"
            if decoder_state.get("pump_on") and uptime - last_tfa_update >= 5:
                decoder_state["tfa_tick"] = int(decoder_state.get("tfa_tick", 0)) + 1
                last_tfa_update = uptime
            elif not decoder_state.get("pump_on"):
                last_tfa_update = uptime

            # Inc/Azm log once per pump cycle: ~60s after OFF->ON reset.
            rs = decoder_state.get("reset_seq")
            try:
                rs_i = int(rs) if rs is not None else 0
            except Exception:
                rs_i = 0

            if (
                decoder_state.get("pump_on")
                and pump_up_start_time is not None
                and rs_i > 0
                and last_pump_reset_seq_logged != rs_i
                and (now - pump_up_start_time) >= 60
            ):
                inc = decoder_state.get("inc")
                azm = decoder_state.get("azm")
                if isinstance(inc, (int, float)) and isinstance(azm, (int, float)):
                    ts_epoch = time.time()
                    log_rows = [
                        {"ts": ts_epoch, "name": "Inc", "value": float(inc)},
                        {"ts": ts_epoch, "name": "Azm", "value": float(azm)},
                    ]
                    did_log = True
                    last_pump_reset_seq_logged = rs_i
            snapshot = dict(decoder_state)

        if did_log:
            for r in log_rows:
                if _pg_dsn():
                    _pg_add_incazm(float(r["ts"]), str(r["name"]), float(r["value"]))
                else:
                    _db_add_incazm(float(r["ts"]), str(r["name"]), float(r["value"]))
            if not NO_WEB:
                socketio.emit("incazm_log_append", {"items": log_rows})
        if not NO_WEB:
            socketio.emit("decoder_state", snapshot)


def handle_line(line: str) -> None:
    global pump_up_start_time, pump_down_start_time, seen_0121_count, did_initial_0121_reset

    parsed = parse_wits_value_line(line)
    if parsed is None:
        print(line, end="", flush=True)
    else:
        print(f"{parsed.code} {parsed.name}: {parsed.value}", flush=True)
        with latest_lock:
            latest_values[parsed.code] = parsed.value
            payload = {
                "0108": latest_values.get("0108"),
                "0110": latest_values.get("0110"),
            }
        if not NO_WEB:
            socketio.emit("wits_values", payload)

        snapshot = _update_decoder_from_latest()
        if snapshot is not None and not NO_WEB:
            socketio.emit("decoder_state", snapshot)
    if not NO_WEB:
        socketio.emit("witsml_data", {"data": line})


def main() -> None:
    try:
        # Ensure logs appear immediately on platforms like Render
        if hasattr(sys.stdout, "reconfigure"):
            sys.stdout.reconfigure(line_buffering=True)
        if hasattr(sys.stderr, "reconfigure"):
            sys.stderr.reconfigure(line_buffering=True)
    except Exception:
        pass

    global NO_WEB
    args = build_parser().parse_args()

    _load_serial_config()
    _load_plotter_state()

    render_port = os.environ.get("PORT")
    if render_port:
        try:
            args.web_port = int(render_port)
        except Exception:
            pass

    if args.list_ports:
        ports = list_serial_ports()
        if not ports:
            print("No se encontraron puertos seriales.")
        else:
            print("Puertos disponibles:")
            for port in ports:
                print(f"- {port}")
        return

    if not args.no_serial and not args.serial_port:
        with serial_lock:
            saved_port = serial_config_snapshot.get("serial_port")
            saved_baud = serial_config_snapshot.get("baudrate")
            saved_bytesize = serial_config_snapshot.get("bytesize")
            saved_parity = serial_config_snapshot.get("parity")
            saved_stopbits = serial_config_snapshot.get("stopbits")
            saved_timeout = serial_config_snapshot.get("timeout")

        if saved_port:
            args.serial_port = str(saved_port)
            if saved_baud is not None:
                try:
                    args.baudrate = int(saved_baud)
                except Exception:
                    pass
            if saved_bytesize is not None:
                try:
                    args.bytesize = int(saved_bytesize)
                except Exception:
                    pass
            if saved_parity is not None:
                args.parity = str(saved_parity)
            if saved_stopbits is not None:
                args.stopbits = str(saved_stopbits)
            if saved_timeout is not None:
                try:
                    args.timeout = float(saved_timeout)
                except Exception:
                    pass
        else:
            try:
                is_tty = bool(getattr(sys.stdin, "isatty", lambda: False)())
            except Exception:
                is_tty = False

            if is_tty:
                args.serial_port = choose_serial_port_interactive()
            else:
                args.no_serial = True

    NO_WEB = bool(args.no_web)

    reader = None
    if not args.no_serial:
        try:
            serial_config = parse_serial_config(args)
        except ValueError as exc:
            print(f"Error en parámetros del puerto: {exc}")
            sys.exit(1)

        with serial_lock:
            serial_config_snapshot["serial_port"] = args.serial_port
            serial_config_snapshot["baudrate"] = args.baudrate
            serial_config_snapshot["bytesize"] = args.bytesize
            serial_config_snapshot["parity"] = args.parity
            serial_config_snapshot["stopbits"] = args.stopbits
            serial_config_snapshot["timeout"] = args.timeout
        _save_serial_config()

        reader = SerialReader(serial_config, on_line=handle_line)
        reader.start()
        with serial_lock:
            global serial_reader
            serial_reader = reader

    def shutdown(*_: object) -> None:
        if reader is not None:
            reader.stop()
            reader.join(timeout=2)
        sys.exit(0)

    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    if args.no_web:
        print("Leyendo datos del puerto (Ctrl+C para salir)...")
        try:
            while reader is not None and reader.is_alive():
                time.sleep(0.5)
        except KeyboardInterrupt:
            shutdown()
    else:
        socketio.start_background_task(decoder_background_task)
        socketio.start_background_task(render_publisher_background_task)
        print(f"Servidor web iniciado en http://{args.web_host}:{args.web_port}", flush=True)
        socketio.run(app, host=args.web_host, port=args.web_port)


if __name__ == "__main__":
    try:
        main()
    except Exception:
        traceback.print_exc()
        try:
            sys.stdout.flush()
            sys.stderr.flush()
        except Exception:
            pass
        raise
