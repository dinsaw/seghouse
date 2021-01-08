from .data_type import DataType

BASE_STRUCTURE = {
    "message_id": DataType.STRING,
    "anonymous_id": DataType.STRING,
    "received_at": DataType.DATETIME,
    "timestamp": DataType.DATETIME,
    "ip": DataType.STRING,
}

EVENT_SPECIFIC = {
    "original_event": DataType.STRING,
    "event": DataType.STRING,
}

TRACKS = dict(BASE_STRUCTURE)
TRACKS.update(EVENT_SPECIFIC)
TRACKS_ALLOWED_FIELD_PREFIXES = (
    "context_",
    "traits_",
    "geoip_",
)

IDENTITIES = dict(BASE_STRUCTURE)

PAGES = dict(BASE_STRUCTURE)

SCREENS = dict(BASE_STRUCTURE)

USERS = dict(BASE_STRUCTURE)
USER_SPECIFIC = {"user_id": DataType.STRING, "ver": DataType.INT64}
USERS.update(USER_SPECIFIC)
