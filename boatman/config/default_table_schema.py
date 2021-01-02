from dataclasses import dataclass, field
from datetime import datetime


BASE_EVENT_SCHEMA = {
    "message_id": str,
    "anonymous_id": str,
    "sent_at": datetime,
    "received_at": datetime,
    "timestamp": datetime,
    "original_timestamp": datetime,
    "event_text": str,
    "event": str,
    "ip": str,
}

TRACKS_SCHEMA = {}.update(BASE_EVENT_SCHEMA)
IDENTIFIES_SCHEMA = {}.update(BASE_EVENT_SCHEMA)
PAGES_SCHEMA = {}.update(BASE_EVENT_SCHEMA)
SCREENS_SCHEMA = {}.update(BASE_EVENT_SCHEMA)
