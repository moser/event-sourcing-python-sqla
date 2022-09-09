import base64 as _base64
import datetime as _dt
import decimal as _decimal
import enum as _enum
import json as _json
import uuid as _uuid
from typing import Optional, Union


StrictlyJsonable = Optional[
    Union[
        dict,  # dict[Union[str, int, float], "StrictlyJsonable"],
        list,  # list["StrictlyJsonable"],
        str,
        int,
        float,
        bool,
        None,
    ]
]
# The commented types are not possbile with mypy at the moment. (And variance
# would be wrong, too)
Jsonable = Optional[
    Union[
        dict,  # dict[Union[str, int, float], "Jsonable"],
        list,  # list["Jsonable"],
        str,
        int,
        float,
        bool,
        bytes,
        _dt.date,
        _dt.datetime,
        _decimal.Decimal,
    ]
]
Dejsonable = Optional[
    Union[
        dict,  # dict[Union[str, int, float], "Dejsonable"],
        list,  # list["Dejsonable"]
        str,
        int,
        bool,
        None,
        bytes,
        _dt.date,
        _dt.datetime,
        _decimal.Decimal,
    ]
]

SENTO_MARKER = "$SENTO"
SENTO_TYPES = {
    bytes: ("BYTES", lambda val: _base64.b64encode(val).decode(), _base64.b64decode),
    _uuid.UUID: ("UUID", str, _uuid.UUID),
}
SENTO_DECODER_BY_MARKER = {
    marker: decoder for marker, _, decoder in SENTO_TYPES.values()
}


def make_jsonable(obj: Jsonable) -> StrictlyJsonable:
    if isinstance(obj, list):
        return [make_jsonable(elem) for elem in obj]
    if isinstance(obj, dict):
        return {key: make_jsonable(value) for key, value in obj.items()}
    # if isinstance(obj, _decimal.Decimal):
    #    return str(obj)
    # if isinstance(obj, _dt.datetime):
    #    return obj.isoformat()
    # if isinstance(obj, _dt.date):
    #    return obj.isoformat()
    if type(obj) in SENTO_TYPES:
        marker, encoder, _ = SENTO_TYPES[type(obj)]
        encoded = encoder(obj)
        return f"{SENTO_MARKER}-{marker}:{encoded}"
    if obj is None or isinstance(obj, (str, int, float, bool)):
        return obj
    raise NotImplementedError(f"Cannot make object of type {type(obj)} jsonable")


class CustomEncoder(_json.JSONEncoder):
    def default(self, obj):
        try:
            return make_jsonable(obj)
        except NotImplementedError:
            pass
        return super().default(obj)


def dump_json(obj: Jsonable) -> str:
    return _json.dumps(obj, cls=CustomEncoder)


def parse_json(obj: str) -> Dejsonable:
    """Parses JSON without converting numbers to float."""
    return _parse_sento_types(_json.loads(obj, parse_float=str))


def _parse_sento_types(obj: Dejsonable) -> Dejsonable:
    if isinstance(obj, list):
        return [_parse_sento_types(val) for val in obj]
    if isinstance(obj, dict):
        return {key: _parse_sento_types(val) for key, val in obj.items()}
    if isinstance(obj, str):
        if obj.startswith(SENTO_MARKER):
            marker, encoded = obj.split(":", 1)
            marker = marker[len(SENTO_MARKER) + 1 :]
            decoder = SENTO_DECODER_BY_MARKER[marker]
            return decoder(encoded)
    return obj
