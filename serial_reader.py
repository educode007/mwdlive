from __future__ import annotations

from dataclasses import dataclass
import threading
from typing import Callable, Iterable, Optional

import serial
from serial.tools import list_ports as serial_list_ports


@dataclass
class SerialConfig:
    port: str
    baudrate: int = 9600
    bytesize: int = serial.EIGHTBITS
    parity: str = serial.PARITY_NONE
    stopbits: float = serial.STOPBITS_ONE
    timeout: float = 1.0


def list_serial_ports() -> list[str]:
    return [port.device for port in serial_list_ports.comports()]


def parse_serial_config(args: object) -> SerialConfig:
    bytesize_map = {
        5: serial.FIVEBITS,
        6: serial.SIXBITS,
        7: serial.SEVENBITS,
        8: serial.EIGHTBITS,
    }
    parity_map = {
        "N": serial.PARITY_NONE,
        "E": serial.PARITY_EVEN,
        "O": serial.PARITY_ODD,
        "M": serial.PARITY_MARK,
        "S": serial.PARITY_SPACE,
    }
    stopbits_map = {
        "1": serial.STOPBITS_ONE,
        "1.5": serial.STOPBITS_ONE_POINT_FIVE,
        "2": serial.STOPBITS_TWO,
    }

    if args.baudrate <= 0:
        raise ValueError("baudrate debe ser mayor que 0")
    if args.timeout < 0:
        raise ValueError("timeout no puede ser negativo")
    if args.bytesize not in bytesize_map:
        raise ValueError("bytesize inválido (use 5, 6, 7 u 8)")

    parity_key = str(args.parity).upper()
    if parity_key not in parity_map:
        raise ValueError("parity inválido (use N, E, O, M o S)")
    if str(args.stopbits) not in stopbits_map:
        raise ValueError("stopbits inválido (use 1, 1.5 o 2)")

    return SerialConfig(
        port=args.serial_port,
        baudrate=args.baudrate,
        bytesize=bytesize_map[args.bytesize],
        parity=parity_map[parity_key],
        stopbits=stopbits_map[str(args.stopbits)],
        timeout=args.timeout,
    )


class SerialReader(threading.Thread):
    def __init__(
        self,
        config: SerialConfig,
        on_line: Optional[Callable[[str], None]] = None,
    ) -> None:
        super().__init__(daemon=True)
        self.config = config
        self.on_line = on_line
        self._stop_event = threading.Event()

    def stop(self) -> None:
        self._stop_event.set()

    def run(self) -> None:
        try:
            serial_port = serial.Serial(
                port=self.config.port,
                baudrate=self.config.baudrate,
                bytesize=self.config.bytesize,
                parity=self.config.parity,
                stopbits=self.config.stopbits,
                timeout=self.config.timeout,
            )
        except serial.SerialException as exc:
            print(f"Error abriendo el puerto {self.config.port}: {exc}")
            return

        with serial_port:
            while not self._stop_event.is_set():
                try:
                    data = serial_port.readline()
                except serial.SerialException as exc:
                    print(f"Error leyendo del puerto: {exc}")
                    break

                if not data:
                    continue

                line = data.decode("ascii", errors="replace")
                if self.on_line:
                    self.on_line(line)
