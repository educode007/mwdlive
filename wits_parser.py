from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Optional, Union


@dataclass(frozen=True)
class WitsValue:
    code: str
    value: Union[float, bool]
    name: str


_CODE_VALUE_RE = re.compile(r"^(?P<code>\d{4})(?P<value>[+-]?(?:\d+(?:\.\d*)?|\.\d+))\s*$")
_CODE_BOOL_RE = re.compile(r"^(?P<code>\d{4})(?P<value>true|false)\s*$", re.IGNORECASE)


DEFAULT_CODE_MAP: dict[str, str] = {
    "0121": "Pressure",
    "0108": "Hole Depth",
    "0110": "Bit Depth",
    "0713": "Inc",
    "0715": "Azm",
    "0716": "mTFA",
    "0717": "gTFA",
    "0736": "SHK1",
    "0737": "VIB1",
    "0747": "Grav",
    "0732": "MagF",
    "0746": "DipA",
    "0751": "Temp",
}


def parse_wits_value_line(line: str, code_map: Optional[dict[str, str]] = None) -> Optional[WitsValue]:
    s = line.strip()
    if not s:
        return None

    if s in {"!!", "&&"}:
        return None

    m = _CODE_VALUE_RE.match(s)
    if m:
        code = m.group("code")
        value: Union[float, bool] = float(m.group("value"))
    else:
        m2 = _CODE_BOOL_RE.match(s)
        if not m2:
            return None
        code = m2.group("code")
        value = m2.group("value").strip().lower() == "true"

    cmap = code_map or DEFAULT_CODE_MAP
    name = cmap.get(code, "Unknown")
    return WitsValue(code=code, value=value, name=name)
