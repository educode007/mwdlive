import argparse
import json
import os
import signal
import sqlite3
import sys
import threading
import time
from typing import Any

from flask import Flask, jsonify, render_template, request
from flask_socketio import SocketIO

from serial_reader import SerialReader, list_serial_ports, parse_serial_config
from wits_parser import parse_wits_value_line


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


def main() -> None:
    args = build_parser().parse_args()

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
        args.serial_port = choose_serial_port_interactive()

    app = Flask(__name__)
    socketio = SocketIO(app, cors_allowed_origins="*")

    latest_lock = threading.Lock()
    latest_values: dict[str, float] = {}

    decoder_lock = threading.Lock()
    start_time = time.monotonic()
    last_tfa_update = 0
    pump_up_start_time: float | None = None
    pump_down_start_time: float | None = None
    seen_0121_count = 0
    did_initial_0121_reset = False
    decoder_state: dict[str, Any] = {
        "title": "Compass Rose - Live Mode",
        "network_id": "618410",
        "pressure_psi": 1983,
        "dec_deg": 6.81,
        "dao_deg": 0.0,
        "pump_on": False,
        "pump_down_seconds": 0,
        "uptime_seconds": 0,
        "shk1": 0.39,
        "vib1": 0.00,
        "magf": 0.22,
        "inc": 88.11,
        "azm": 167.83,
        "bat2_on": True,
        "center_label": "gTFA",
        "center_value": None,
        "center_counter_seconds": 7,
        "tfa_tick": 0,
        "grav": None,
        "dipa": None,
        "temp": None,
    }

    @app.route("/")
    def index() -> str:
        return render_template("index.html")

    @app.route("/plotter")
    def plotter() -> str:
        return render_template(
            "plotter.html",
            desktop_ingest_url=os.environ.get("DESKTOP_INGEST_URL", ""),
            desktop_ingest_key=os.environ.get("DESKTOP_INGEST_API_KEY", ""),
        )

    ingest_lock = threading.Lock()
    last_ingest: dict[str, Any] | None = None

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
        finally:
            con.close()

    def _db_path() -> str:
        return os.environ.get("MWDMONITOR_DB", os.path.join(os.getcwd(), "mwdmonitor.db"))

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
        return con

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
                row = con.execute(
                    "SELECT payload FROM ingest_snapshots ORDER BY ts DESC LIMIT 1;"
                ).fetchone()
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
        nonlocal last_ingest
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
        return jsonify({"ok": True})

    @socketio.on("connect")
    def on_connect() -> None:
        with latest_lock:
            payload = {
                "0108": latest_values.get("0108"),
                "0110": latest_values.get("0110"),
            }
        socketio.emit("wits_values", payload)

        with decoder_lock:
            socketio.emit("decoder_state", decoder_state)

    def decoder_background_task() -> None:
        nonlocal last_tfa_update, pump_down_start_time
        while True:
            time.sleep(1)
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
                if uptime - last_tfa_update >= 5:
                    decoder_state["tfa_tick"] = int(decoder_state.get("tfa_tick", 0)) + 1
                    last_tfa_update = uptime
                snapshot = dict(decoder_state)
            socketio.emit("decoder_state", snapshot)

    def handle_line(line: str) -> None:
        nonlocal pump_up_start_time, pump_down_start_time, seen_0121_count, did_initial_0121_reset
        parsed = parse_wits_value_line(line)
        if parsed is None:
            print(line, end="", flush=True)
        else:
            print(f"{parsed.code} {parsed.name}: {parsed.value}")
            if parsed.code in {"0108", "0110"}:
                with latest_lock:
                    latest_values[parsed.code] = parsed.value
                    payload = {
                        "0108": latest_values.get("0108"),
                        "0110": latest_values.get("0110"),
                    }

                if not args.no_web:
                    socketio.emit("wits_values", payload)

            # Mapear Inc/Azm desde códigos reales
            if parsed.code in {"0713", "0715"}:
                with decoder_lock:
                    if parsed.code == "0713":
                        decoder_state["inc"] = parsed.value
                    elif parsed.code == "0715":
                        decoder_state["azm"] = parsed.value

                    snapshot = dict(decoder_state)

                if not args.no_web:
                    socketio.emit("decoder_state", snapshot)

            if parsed.code == "0121":
                with decoder_lock:
                    now = time.monotonic()
                    seen_0121_count += 1
                    decoder_state["pressure_psi"] = parsed.value

                    is_pump_on = isinstance(parsed.value, (int, float)) and float(parsed.value) >= 300
                    was_pump_on = bool(decoder_state.get("pump_on"))
                    if is_pump_on and not was_pump_on:
                        decoder_state["pump_on"] = True
                        pump_up_start_time = now
                        pump_down_start_time = None
                        decoder_state["pump_down_seconds"] = 0
                        decoder_state["uptime_seconds"] = 0
                    elif (not is_pump_on) and was_pump_on:
                        decoder_state["pump_on"] = False
                        pump_down_start_time = now

                    if not decoder_state.get("pump_on") and pump_down_start_time is not None:
                        decoder_state["pump_down_seconds"] = int(now - pump_down_start_time)

                    if (not did_initial_0121_reset) and seen_0121_count >= 3:
                        did_initial_0121_reset = True
                        decoder_state["shk1"] = None
                        decoder_state["vib1"] = None
                        decoder_state["grav"] = None
                        decoder_state["magf"] = None
                        decoder_state["dipa"] = None
                        decoder_state["temp"] = None
                        decoder_state["inc"] = None
                        decoder_state["azm"] = None
                        decoder_state["center_value"] = None

                    snapshot = dict(decoder_state)

                if not args.no_web:
                    socketio.emit("decoder_state", snapshot)

            if parsed.code in {"0716", "0717", "0736", "0737", "0747", "0732", "0746", "0751"}:
                with decoder_lock:
                    if parsed.code == "0736":
                        decoder_state["shk1"] = parsed.value
                    elif parsed.code == "0737":
                        decoder_state["vib1"] = parsed.value
                    elif parsed.code == "0747":
                        decoder_state["grav"] = parsed.value
                    elif parsed.code == "0732":
                        decoder_state["magf"] = parsed.value
                    elif parsed.code == "0746":
                        decoder_state["dipa"] = parsed.value
                    elif parsed.code == "0751":
                        decoder_state["temp"] = parsed.value
                    elif parsed.code in {"0716", "0717"}:
                        inc = decoder_state.get("inc")
                        use_0717 = isinstance(inc, (int, float)) and inc >= 3
                        if (use_0717 and parsed.code == "0717") or ((not use_0717) and parsed.code == "0716"):
                            decoder_state["center_value"] = parsed.value

                    snapshot = dict(decoder_state)

                if not args.no_web:
                    socketio.emit("decoder_state", snapshot)
        if not args.no_web:
            socketio.emit("witsml_data", {"data": line})

    reader = None
    if not args.no_serial:
        try:
            serial_config = parse_serial_config(args)
        except ValueError as exc:
            print(f"Error en parámetros del puerto: {exc}")
            sys.exit(1)

        reader = SerialReader(serial_config, on_line=handle_line)
        reader.start()

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
        print(f"Servidor web iniciado en http://{args.web_host}:{args.web_port}")
        socketio.run(app, host=args.web_host, port=args.web_port)


if __name__ == "__main__":
    main()
