from __future__ import annotations

import logging
from enum import IntEnum, StrEnum
from typing import Final

LOGGER = logging.getLogger(__package__)

# Configuration, options, defaults
CONF_ENTITY_PREFIX: Final = "entity_prefix"
CONF_HOSTNAME: Final = "hostname"

DEFAULT_ENTITY_PREFIX: Final = "RCT Power Storage"
DEFAULT_PORT: Final = 8899


class ConfScanInterval(StrEnum):
    REALTIME = "realtime_scan_interval"
    FREQUENT = "frequent_scan_interval"
    INFREQUENT = "infrequent_scan_interval"
    STATIC = "static_scan_interval"


class ScanIntervalDefault(IntEnum):
    # 20 s is safe for the inverter's network module even in a pessimistic
    # socket model; lower it in the options dialog once the link is proven
    REALTIME = 20
    FREQUENT = 30
    INFREQUENT = 60 * 3
    STATIC = 60 * 60


# a realtime value may be reused for this many intervals after a failed read,
# but at least REALTIME_STALE_TOLERANCE_MIN seconds: two realtime reads can be
# several seconds further apart than the interval while a healthy batch of
# another tier holds the connection (measured up to 8 s); after that the
# realtime entities become unavailable until a read succeeds
REALTIME_STALE_TOLERANCE_INTERVALS: Final = 5
REALTIME_STALE_TOLERANCE_MIN: Final = 12
