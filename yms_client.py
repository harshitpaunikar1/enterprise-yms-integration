"""
YMS (Yard Management System) API client for enterprise integration.
Handles authentication, dock status polling, trailer position updates, and event subscriptions.
"""
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry
    REQUESTS_AVAILABLE = True
except ImportError:
    REQUESTS_AVAILABLE = False
    logger.warning("requests not installed. YMS client will run in stub mode.")


@dataclass
class YMSConfig:
    base_url: str = "http://yms.internal/api/v2"
    api_key: str = ""
    timeout_s: int = 10
    max_retries: int = 3
    retry_backoff: float = 0.5
    yard_id: str = "YARD_001"


@dataclass
class DockStatus:
    dock_id: str
    status: str          # available, occupied, blocked, maintenance
    trailer_id: Optional[str]
    last_updated: float
    lane: str
    capacity_tons: float


@dataclass
class TrailerPosition:
    trailer_id: str
    dock_id: Optional[str]
    zone: str
    timestamp: float
    latitude: Optional[float] = None
    longitude: Optional[float] = None


@dataclass
class YMSEvent:
    event_id: str
    event_type: str      # dock_change, trailer_arrival, trailer_departure, alert
    payload: Dict[str, Any]
    received_at: float = field(default_factory=time.time)


class YMSClient:
    """
    REST API client for a yard management system.
    Supports dock status polling, trailer assignment, and event-driven webhook callbacks.
    Operates in stub mode when requests is not available or base_url is unreachable.
    """

    def __init__(self, config: YMSConfig):
        self.config = config
        self._session = None
        self._event_handlers: Dict[str, List[Callable]] = {}
        self._stub_mode = not REQUESTS_AVAILABLE

    def _build_session(self):
        if not REQUESTS_AVAILABLE:
            return None
        session = requests.Session()
        retry = Retry(
            total=self.config.max_retries,
            backoff_factor=self.config.retry_backoff,
            status_forcelist=[500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.headers.update({
            "Authorization": f"Bearer {self.config.api_key}",
            "Content-Type": "application/json",
            "X-Yard-ID": self.config.yard_id,
        })
        return session

    def connect(self) -> bool:
        if self._stub_mode:
            logger.info("YMS client in stub mode (requests unavailable).")
            return True
        self._session = self._build_session()
        try:
            resp = self._session.get(
                f"{self.config.base_url}/health",
                timeout=self.config.timeout_s,
            )
            resp.raise_for_status()
            logger.info("Connected to YMS at %s", self.config.base_url)
            return True
        except Exception as exc:
            logger.warning("YMS connection failed: %s. Using stub mode.", exc)
            self._stub_mode = True
            return False

    def _stub_dock_statuses(self) -> List[DockStatus]:
        return [
            DockStatus(dock_id=f"D{i:03d}", status="available" if i % 3 != 0 else "occupied",
                       trailer_id=f"T{i:03d}" if i % 3 == 0 else None,
                       last_updated=time.time(), lane=f"Lane-{i % 4 + 1}",
                       capacity_tons=float(20 + i % 10))
            for i in range(1, 11)
        ]

    def get_dock_statuses(self) -> List[DockStatus]:
        """Fetch current status for all docks in the configured yard."""
        if self._stub_mode:
            return self._stub_dock_statuses()
        try:
            resp = self._session.get(
                f"{self.config.base_url}/yards/{self.config.yard_id}/docks",
                timeout=self.config.timeout_s,
            )
            resp.raise_for_status()
            data = resp.json()
            return [
                DockStatus(
                    dock_id=d["dock_id"],
                    status=d["status"],
                    trailer_id=d.get("trailer_id"),
                    last_updated=d.get("last_updated", time.time()),
                    lane=d.get("lane", ""),
                    capacity_tons=d.get("capacity_tons", 20.0),
                )
                for d in data.get("docks", [])
            ]
        except Exception as exc:
            logger.error("get_dock_statuses failed: %s", exc)
            return []

    def assign_trailer_to_dock(self, trailer_id: str, dock_id: str) -> bool:
        """Request the YMS to assign a trailer to a specific dock."""
        if self._stub_mode:
            logger.info("[STUB] Assigning trailer %s to dock %s", trailer_id, dock_id)
            return True
        try:
            payload = {"trailer_id": trailer_id, "dock_id": dock_id, "yard_id": self.config.yard_id}
            resp = self._session.post(
                f"{self.config.base_url}/assignments",
                json=payload,
                timeout=self.config.timeout_s,
            )
            resp.raise_for_status()
            return True
        except Exception as exc:
            logger.error("assign_trailer_to_dock failed: %s", exc)
            return False

    def update_trailer_position(self, position: TrailerPosition) -> bool:
        """Push a trailer's current position update to the YMS."""
        if self._stub_mode:
            logger.info("[STUB] Position update: trailer %s at zone %s", position.trailer_id, position.zone)
            return True
        try:
            payload = {
                "trailer_id": position.trailer_id,
                "dock_id": position.dock_id,
                "zone": position.zone,
                "timestamp": position.timestamp,
            }
            if position.latitude is not None:
                payload["latitude"] = position.latitude
                payload["longitude"] = position.longitude
            resp = self._session.put(
                f"{self.config.base_url}/trailers/{position.trailer_id}/position",
                json=payload,
                timeout=self.config.timeout_s,
            )
            resp.raise_for_status()
            return True
        except Exception as exc:
            logger.error("update_trailer_position failed: %s", exc)
            return False

    def poll_events(self, since_timestamp: Optional[float] = None) -> List[YMSEvent]:
        """Poll for new yard events since a given timestamp."""
        if self._stub_mode:
            return [
                YMSEvent(event_id="E001", event_type="dock_change",
                         payload={"dock_id": "D001", "new_status": "occupied", "trailer_id": "T101"}),
                YMSEvent(event_id="E002", event_type="trailer_arrival",
                         payload={"trailer_id": "T202", "zone": "Zone-A"}),
            ]
        params = {}
        if since_timestamp:
            params["since"] = int(since_timestamp)
        try:
            resp = self._session.get(
                f"{self.config.base_url}/yards/{self.config.yard_id}/events",
                params=params,
                timeout=self.config.timeout_s,
            )
            resp.raise_for_status()
            data = resp.json()
            return [
                YMSEvent(event_id=e["event_id"], event_type=e["event_type"],
                         payload=e.get("payload", {}))
                for e in data.get("events", [])
            ]
        except Exception as exc:
            logger.error("poll_events failed: %s", exc)
            return []

    def on_event(self, event_type: str, handler: Callable[[YMSEvent], None]) -> None:
        """Register a callback for a specific event type."""
        self._event_handlers.setdefault(event_type, []).append(handler)

    def dispatch_events(self, events: List[YMSEvent]) -> None:
        """Route polled events to registered handlers."""
        for event in events:
            handlers = self._event_handlers.get(event.event_type, [])
            for handler in handlers:
                try:
                    handler(event)
                except Exception as exc:
                    logger.error("Event handler failed for %s: %s", event.event_type, exc)


if __name__ == "__main__":
    config = YMSConfig(
        base_url="http://yms.internal/api/v2",
        api_key="demo_key",
        yard_id="YARD_001",
    )
    client = YMSClient(config)
    client.connect()

    docks = client.get_dock_statuses()
    print(f"Docks retrieved: {len(docks)}")
    for dock in docks[:3]:
        print(f"  {dock.dock_id}: {dock.status} | trailer={dock.trailer_id}")

    success = client.assign_trailer_to_dock("T101", "D002")
    print(f"Assignment result: {success}")

    client.on_event("dock_change", lambda e: print(f"  DOCK EVENT: {e.payload}"))
    events = client.poll_events()
    print(f"\nEvents polled: {len(events)}")
    client.dispatch_events(events)
