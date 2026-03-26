"""Helpers for scanning public event sources into control-center records."""

from __future__ import annotations

import asyncio
import ipaddress
import json
import re
import socket
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from email.utils import parsedate_to_datetime
from html import unescape
from typing import Any
from urllib.parse import parse_qsl, unquote, urlencode, urljoin, urlparse, urlunparse
from xml.etree import ElementTree

import httpx
from dateutil import parser as date_parser
from zoneinfo import ZoneInfo

USER_AGENT = (
    "MissionControlEventsBot/1.0 "
    "(https://mission.echoheelixmissioncontrol.com; admin@echoheelixmissioncontrol.com)"
)
EVENT_TIMEZONE = ZoneInfo("Australia/Melbourne")
SCRIPT_JSON_LD_RE = re.compile(
    r"<script[^>]+type=[\"']application/ld\+json[\"'][^>]*>(?P<body>.*?)</script>",
    re.IGNORECASE | re.DOTALL,
)
ANCHOR_RE = re.compile(
    r"<a\b[^>]*href=[\"'](?P<href>[^\"']+)[\"'][^>]*>(?P<label>.*?)</a>",
    re.IGNORECASE | re.DOTALL,
)
TAG_RE = re.compile(r"<[^>]+>")
DATE_ONLY_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
WHITESPACE_RE = re.compile(r"\s+")
EVENT_LINK_HINTS = (
    "/event",
    "/events",
    "/whatson",
    "/e/",
    "eventbrite",
    "humanitix",
    "meetup.com",
    "allevents.in",
    "concreteplayground",
    "whatson.melbourne",
    "eventfinda",
)
EVENT_TEXT_HINTS = (
    "event",
    "festival",
    "summit",
    "workshop",
    "meetup",
    "conference",
    "expo",
    "show",
    "market",
    "launch",
    "networking",
)
EVENT_SCHEMA_TYPES = {
    "BusinessEvent",
    "ChildrensEvent",
    "ComedyEvent",
    "EducationEvent",
    "Event",
    "EventSeries",
    "ExhibitionEvent",
    "Festival",
    "FoodEvent",
    "LiteraryEvent",
    "MusicEvent",
    "SaleEvent",
    "ScreeningEvent",
    "SocialEvent",
    "SportsEvent",
    "TheaterEvent",
    "VisualArtsEvent",
}
META_TAG_RE = re.compile(
    r"<meta[^>]+(?:property|name)=[\"'](?P<name>[^\"']+)[\"'][^>]+content=[\"'](?P<content>[^\"']+)[\"'][^>]*>",
    re.IGNORECASE,
)
IMG_RE = re.compile(
    r"<img[^>]+src=[\"'](?P<src>[^\"']+)[\"'][^>]*>",
    re.IGNORECASE,
)
TITLE_RE = re.compile(r"<title>(?P<title>.*?)</title>", re.IGNORECASE | re.DOTALL)
NEXT_DATA_RE = re.compile(
    r'<script[^>]+id=["\']__NEXT_DATA__["\'][^>]*>(?P<body>.*?)</script>',
    re.IGNORECASE | re.DOTALL,
)
EVENTFINDA_CARD_RE = re.compile(
    r'<div class="d-flex align-items-stretch[^"]*">.*?'
    r'<a[^>]+href=["\'](?P<card_href>[^"\']+)["\'][^>]*>.*?'
    r'(?:<img[^>]+src=["\'](?P<image>[^"\']+)["\'][^>]*>)?.*?</a>.*?'
    r'<div class="card-body">.*?'
    r'<h2 class="card-title[^"]*"><a href=["\'](?P<href>[^"\']+)["\'][^>]*>(?P<title>.*?)</a></h2>.*?'
    r'(?:<p class="card-text meta-location[^"]*">(?P<location>.*?)</p>)?.*?'
    r'(?:<p class="card-text meta-date">(?P<date>.*?)</p>)?',
    re.IGNORECASE | re.DOTALL,
)
WELLINGTONNZ_CARD_RE = re.compile(
    r'<a href=["\'](?P<href>/visit/events/[^"\']+)["\'][^>]*class=["\'][^"\']*featured-item[^"\']*["\'][^>]*>.*?'
    r'(?:<img[^>]+src=["\'](?P<image>[^"\']+)["\'][^>]*>)?.*?'
    r'<h2 class="featured-item__title"[^>]*>(?P<title>.*?)</h2>.*?'
    r'<p class="featured-item__summary[^"]*"[^>]*>.*?'
    r'(?:<span class="featured-item__text featured-item__text--date"[^>]*>(?P<date>.*?)</span>)?.*?'
    r'(?:<span class="featured-item__text featured-item__text--info"[^>]*>(?P<venue>.*?)</span>)?',
    re.IGNORECASE | re.DOTALL,
)
EVENTBRITE_EVENT_LINK_RE = re.compile(
    r'href=["\'](?P<href>https?://(?:www\.)?eventbrite\.(?:com\.au|co\.nz)/e/[^"\']+)["\']',
    re.IGNORECASE,
)
WHATSON_MELBOURNE_LINK_RE = re.compile(
    r'href=["\'](?P<href>(?:/whatson/Pages/Event\.aspx\?[^"\']+|/things-to-do/[^"\']+))["\']',
    re.IGNORECASE,
)
CONCRETE_PLAYGROUND_LINK_RE = re.compile(
    r'href=["\'](?P<href>https?://concreteplayground\.com/[^"\']+/event/[^"\']+)["\']',
    re.IGNORECASE,
)
MONTH_NAME_RE = r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|Aug(?:ust)?|Sep(?:t(?:ember)?)?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
DATE_RANGE_RE = re.compile(
    rf"(?P<start_day>\d{{1,2}})\s*(?:-|–|—|to)\s*(?P<end_day>\d{{1,2}})\s+(?P<month>{MONTH_NAME_RE})\s+(?P<year>\d{{4}})",
    re.IGNORECASE,
)
FULL_DATE_RE = re.compile(
    rf"(?P<day>\d{{1,2}})\s+(?P<month>{MONTH_NAME_RE})(?:\s+(?P<year>\d{{4}}))?",
    re.IGNORECASE,
)

SOURCE_REASON_KEYS = ("off_source", "missing_date", "parse_failed", "network_error")
TRACKING_QUERY_PARAMS = frozenset(
    {
        "aff",
        "fbclid",
        "gclid",
        "mc_cid",
        "mc_eid",
        "ref",
        "ref_ctx_id",
        "referrer",
        "searchid",
        "source",
        "source_context",
    }
)
SUPPORTED_EVENT_SOURCE_HOSTS = frozenset(
    {
        "allevents.in",
        "calendar.google.com",
        "concreteplayground.com",
        "eventbrite.co.nz",
        "eventbrite.com.au",
        "eventfinda.co.nz",
        "humanitix.com",
        "meetup.com",
        "whatson.melbourne.vic.gov.au",
        "wellingtonnz.com",
    }
)
MAX_FETCH_REDIRECTS = 5


class UnsafeEventSourceError(httpx.InvalidURL):
    """Raised when a source or redirect target is outside the scanner safety policy."""


@dataclass(slots=True)
class EventCandidate:
    title: str
    event_url: str
    source_url: str
    source_name: str
    summary: str | None = None
    start_at: str | None = None
    end_at: str | None = None
    venue: str | None = None
    address: str | None = None
    city: str | None = None
    country: str | None = None
    organizer: str | None = None
    group_name: str | None = None
    price: str | None = None
    currency: str | None = None
    is_free: bool | None = None
    image_url: str | None = None
    event_type: str | None = None
    status: str | None = None
    cancelled: bool | None = None
    online_or_hybrid: str | None = None
    attendee_count: int | None = None
    review_count: int | None = None
    ticket_url: str | None = None
    timezone: str | None = None


@dataclass(slots=True)
class EventScanResult:
    imported: int
    skipped: int
    week_start: str
    week_end: str
    events: list[EventCandidate]
    diagnostics: list["SourceDiagnostic"] = field(default_factory=list)
    skipped_reasons: dict[str, int] = field(default_factory=dict)

    @property
    def created(self) -> int:
        return self.imported


@dataclass(slots=True)
class SourceDiagnostic:
    source_url: str
    source_name: str
    scanned_candidates: int = 0
    imported: int = 0
    skipped: int = 0
    failure_reasons: dict[str, int] = field(default_factory=dict)

    def increment(self, reason: str, amount: int = 1) -> None:
        if amount <= 0:
            return
        self.skipped += amount
        self.failure_reasons[reason] = self.failure_reasons.get(reason, 0) + amount


@dataclass(slots=True)
class SourceScanResult:
    candidates: list[EventCandidate]
    diagnostic: SourceDiagnostic


@dataclass(slots=True)
class GeocodeResult:
    lat: float
    lon: float
    display_name: str | None = None


def _clean_text(value: object) -> str | None:
    if value is None:
        return None
    text = unescape(str(value))
    text = TAG_RE.sub(" ", text)
    text = WHITESPACE_RE.sub(" ", text).strip()
    return text or None


def _clean_url(value: object, *, base_url: str | None = None) -> str | None:
    if isinstance(value, list):
        for item in value:
            cleaned = _clean_url(item, base_url=base_url)
            if cleaned:
                return cleaned
        return None
    if isinstance(value, dict):
        for key in ("url", "src", "href", "contentUrl"):
            if key in value:
                cleaned = _clean_url(value.get(key), base_url=base_url)
                if cleaned:
                    return cleaned
        return None
    raw = _clean_text(value)
    if not raw:
        return None
    url_match = re.search(r"https?://[^\s'\",\\\]]+|https?:/[^\s'\",\\\]]+", raw)
    if url_match:
        raw = url_match.group(0)
    if raw.startswith("https:/") and not raw.startswith("https://"):
        raw = raw.replace("https:/", "https://", 1)
    if raw.startswith("http:/") and not raw.startswith("http://"):
        raw = raw.replace("http:/", "http://", 1)
    if raw.startswith(("mailto:", "javascript:", "#")):
        return None
    candidate = urljoin(base_url or "", raw)
    parsed = urlparse(candidate)
    if parsed.scheme not in {"http", "https"}:
        return None
    query = urlencode(
        [
            (key, val)
            for key, val in parse_qsl(parsed.query, keep_blank_values=True)
            if not key.lower().startswith("utm_") and key.lower() not in TRACKING_QUERY_PARAMS
        ],
        doseq=True,
    )
    return urlunparse(
        (parsed.scheme, parsed.netloc.lower(), parsed.path.rstrip("/"), "", query, "")
    )


def _source_name_for_url(url: str) -> str:
    host = (urlparse(url).hostname or "").lower()
    host = host.removeprefix("www.")
    return host or "source"


def _normalized_host(url: str) -> str:
    return (urlparse(url).hostname or "").lower().removeprefix("www.")


def _host_matches_supported_source(host: str) -> bool:
    return any(
        host == allowed_host or host.endswith(f".{allowed_host}")
        for allowed_host in SUPPORTED_EVENT_SOURCE_HOSTS
    )


def _is_public_ip_address(address: ipaddress.IPv4Address | ipaddress.IPv6Address) -> bool:
    return (
        address.is_global
        and not address.is_loopback
        and not address.is_link_local
        and not address.is_multicast
        and not address.is_private
        and not address.is_reserved
        and not address.is_unspecified
    )


def _ensure_public_addresses(
    host: str,
    addresses: set[ipaddress.IPv4Address | ipaddress.IPv6Address],
) -> None:
    if not addresses:
        raise UnsafeEventSourceError(f"Could not resolve event source host: {host}")
    blocked = [str(address) for address in addresses if not _is_public_ip_address(address)]
    if blocked:
        raise UnsafeEventSourceError(
            f"Unsafe event source host {host} resolves to blocked address(es): {', '.join(sorted(blocked))}"
        )


async def _resolve_ip_addresses(host: str) -> set[ipaddress.IPv4Address | ipaddress.IPv6Address]:
    stripped_host = host.strip().lower().rstrip(".")
    if not stripped_host or stripped_host == "localhost":
        raise UnsafeEventSourceError(f"Unsafe event source host: {host}")
    try:
        return {ipaddress.ip_address(stripped_host)}
    except ValueError:
        pass

    loop = asyncio.get_running_loop()
    try:
        addr_infos = await loop.getaddrinfo(
            stripped_host,
            None,
            family=socket.AF_UNSPEC,
            type=socket.SOCK_STREAM,
        )
    except socket.gaierror as exc:
        raise UnsafeEventSourceError(f"Could not resolve event source host: {host}") from exc

    addresses: set[ipaddress.IPv4Address | ipaddress.IPv6Address] = set()
    for _, _, _, _, sockaddr in addr_infos:
        if not sockaddr:
            continue
        try:
            addresses.add(ipaddress.ip_address(sockaddr[0]))
        except ValueError:
            continue
    _ensure_public_addresses(host, addresses)
    return addresses


def _validate_supported_source_url(url: str, *, allow_calendar_ics: bool = False) -> str:
    cleaned = _clean_url(url)
    if not cleaned:
        raise UnsafeEventSourceError(f"Invalid event source URL: {url}")
    parsed = urlparse(cleaned)
    host = _normalized_host(cleaned)
    if parsed.scheme not in {"http", "https"}:
        raise UnsafeEventSourceError(f"Unsupported event source scheme: {url}")
    if parsed.username or parsed.password:
        raise UnsafeEventSourceError(f"Event source URLs cannot include credentials: {url}")
    if parsed.port not in (None, 80, 443):
        raise UnsafeEventSourceError(f"Event source URLs must use the default web port: {url}")
    if not host or not _host_matches_supported_source(host) or _adapter_key_for_url(cleaned) == "generic":
        raise UnsafeEventSourceError(
            "Unsupported event source host. Only approved public event domains are allowed."
        )
    if _adapter_key_for_url(cleaned) == "google_calendar":
        params = dict(parse_qsl(parsed.query))
        if not params.get("src"):
            if allow_calendar_ics and parsed.path.startswith("/calendar/ical/"):
                return cleaned
            raise UnsafeEventSourceError(
                "Google Calendar event sources must use a public calendar embed URL with a src parameter."
            )
    return cleaned


async def validate_event_source_urls(sources: list[str]) -> list[str]:
    cleaned_sources: list[str] = []
    for source in sources:
        cleaned = _validate_supported_source_url(source)
        resolved = await _resolve_ip_addresses(_normalized_host(cleaned))
        _ensure_public_addresses(_normalized_host(cleaned), resolved)
        cleaned_sources.append(cleaned)
    return cleaned_sources


async def _assert_safe_fetch_url(url: str) -> str:
    cleaned = _validate_supported_source_url(url, allow_calendar_ics=True)
    resolved = await _resolve_ip_addresses(_normalized_host(cleaned))
    _ensure_public_addresses(_normalized_host(cleaned), resolved)
    return cleaned


def _adapter_key_for_url(url: str) -> str:
    host = _normalized_host(url)
    if host.endswith("eventfinda.co.nz"):
        return "eventfinda"
    if host.endswith("wellingtonnz.com"):
        return "wellingtonnz"
    if host.endswith("whatson.melbourne.vic.gov.au"):
        return "whatson_melbourne"
    if host.endswith("eventbrite.com.au") or host.endswith("eventbrite.co.nz"):
        return "eventbrite"
    if host.endswith("humanitix.com"):
        return "humanitix"
    if host.endswith("concreteplayground.com"):
        return "concreteplayground"
    if host.endswith("meetup.com"):
        return "meetup"
    if host.endswith("calendar.google.com"):
        return "google_calendar"
    return "generic"


def _allowed_hosts_for_source(source_url: str) -> set[str]:
    source_host = _normalized_host(source_url)
    adapter_key = _adapter_key_for_url(source_url)
    if adapter_key == "eventbrite":
        return {source_host}
    if adapter_key == "humanitix":
        return {
            source_host,
            "events.humanitix.com",
            "humanitix.com",
        }
    return {source_host}


def _is_allowed_event_url(event_url: str, *, source_url: str) -> bool:
    candidate_host = _normalized_host(event_url)
    if not candidate_host:
        return False
    if _adapter_key_for_url(source_url) == "google_calendar":
        return True
    allowed_hosts = _allowed_hosts_for_source(source_url)
    if candidate_host in allowed_hosts:
        return True
    adapter_key = _adapter_key_for_url(source_url)
    if adapter_key == "eventfinda" and candidate_host.endswith("eventfinda.co.nz"):
        return True
    if adapter_key == "wellingtonnz" and candidate_host.endswith("wellingtonnz.com"):
        return True
    if adapter_key == "whatson_melbourne" and candidate_host.endswith("whatson.melbourne.vic.gov.au"):
        return True
    if adapter_key == "meetup" and candidate_host.endswith("meetup.com"):
        return True
    if adapter_key == "concreteplayground" and candidate_host.endswith("concreteplayground.com"):
        return True
    return False


def _string_at(data: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = data.get(key)
        if isinstance(value, str):
            cleaned = _clean_text(value)
            if cleaned:
                return cleaned
    return None


def _normalize_datetime(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, list):
        for item in value:
            normalized = _normalize_datetime(item)
            if normalized:
                return normalized
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        text = _clean_text(value)
        if not text:
            return None
        text = text.replace("&ndash;", "–").replace("&mdash;", "—")
        if DATE_ONLY_RE.fullmatch(text):
            return text
        range_match = DATE_RANGE_RE.search(text)
        if range_match:
            try:
                start_text = (
                    f"{range_match.group('start_day')} "
                    f"{range_match.group('month')} "
                    f"{range_match.group('year')}"
                )
                dt = date_parser.parse(start_text, dayfirst=True)
                return dt.date().isoformat()
            except (ValueError, TypeError, OverflowError):
                pass
        full_match = FULL_DATE_RE.search(text)
        if full_match:
            try:
                year = full_match.group("year") or str(datetime.now(EVENT_TIMEZONE).year)
                dt = date_parser.parse(
                    f"{full_match.group('day')} {full_match.group('month')} {year}",
                    dayfirst=True,
                )
                return dt.date().isoformat()
            except (ValueError, TypeError, OverflowError):
                pass
        try:
            dt = date_parser.parse(text)
        except (ValueError, TypeError, OverflowError):
            try:
                dt = parsedate_to_datetime(text)
            except (TypeError, ValueError, IndexError):
                return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=EVENT_TIMEZONE)
    return dt.isoformat()


def _coerce_week_start(value: str | None) -> date:
    if value:
        parsed = _normalize_datetime(value)
        if parsed:
            parsed_date: date
            if DATE_ONLY_RE.fullmatch(parsed):
                parsed_date = date.fromisoformat(parsed)
            else:
                parsed_date = datetime.fromisoformat(parsed).date()
            return parsed_date - timedelta(days=parsed_date.weekday())
    today = datetime.now(EVENT_TIMEZONE).date()
    return today - timedelta(days=today.weekday())


def _event_date(value: str | None) -> date | None:
    if not value:
        return None
    if DATE_ONLY_RE.fullmatch(value):
        return date.fromisoformat(value)
    try:
        return datetime.fromisoformat(value).date()
    except ValueError:
        return None


def _within_week(candidate: EventCandidate, *, week_start: date, week_end: date) -> bool:
    event_day = _event_date(candidate.start_at)
    end_day = _event_date(candidate.end_at)
    if event_day is None and end_day is None:
        return False
    if event_day is not None and end_day is not None:
        return event_day <= week_end and end_day >= week_start
    if event_day is not None:
        return week_start <= event_day <= week_end
    return end_day is not None and week_start <= end_day <= week_end


def _iter_json_ld_nodes(payload: Any) -> list[dict[str, Any]]:
    nodes: list[dict[str, Any]] = []
    if isinstance(payload, dict):
        nodes.append(payload)
        graph = payload.get("@graph")
        if isinstance(graph, list):
            for item in graph:
                nodes.extend(_iter_json_ld_nodes(item))
        item_list = payload.get("itemListElement")
        if isinstance(item_list, list):
            for item in item_list:
                if isinstance(item, dict) and "item" in item:
                    nodes.extend(_iter_json_ld_nodes(item["item"]))
                else:
                    nodes.extend(_iter_json_ld_nodes(item))
    elif isinstance(payload, list):
        for item in payload:
            nodes.extend(_iter_json_ld_nodes(item))
    return nodes


def _is_event_type(value: object) -> bool:
    if isinstance(value, str):
        lowered = value.lower()
        return value in EVENT_SCHEMA_TYPES or lowered.endswith("event")
    if isinstance(value, list):
        return any(_is_event_type(item) for item in value)
    return False


def _build_node_index(nodes: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    index: dict[str, dict[str, Any]] = {}
    for node in nodes:
        node_id = _clean_text(node.get("@id"))
        if node_id:
            index[node_id] = node
    return index


def _resolve_node_ref(value: object, *, node_index: dict[str, dict[str, Any]]) -> object:
    if isinstance(value, dict):
        ref_id = _clean_text(value.get("@id"))
        if ref_id and ref_id in node_index:
            return node_index[ref_id]
        return value
    if isinstance(value, list):
        return [_resolve_node_ref(item, node_index=node_index) for item in value]
    return value


def _extract_location_bits(location: object) -> dict[str, str | None]:
    if not isinstance(location, dict):
        return {"venue": _clean_text(location), "address": None, "city": None, "country": None}
    address = location.get("address")
    venue = _string_at(location, "name")
    if isinstance(address, dict):
        address_line = _string_at(
            address,
            "streetAddress",
            "addressLocality",
            "name",
        )
        city = _string_at(address, "addressLocality")
        country = _string_at(address, "addressCountry")
        address_parts = [
            _string_at(address, "streetAddress"),
            _string_at(address, "addressLocality"),
            _string_at(address, "addressRegion"),
            _string_at(address, "postalCode"),
            country,
        ]
        merged_address = ", ".join([part for part in address_parts if part])
        return {
            "venue": venue,
            "address": merged_address or address_line,
            "city": city,
            "country": country,
        }
    return {
        "venue": venue,
        "address": _clean_text(address),
        "city": None,
        "country": None,
    }


def _location_bits_from_text(value: str | None) -> dict[str, str | None]:
    cleaned = _clean_text(value)
    if not cleaned:
        return {"venue": None, "address": None, "city": None, "country": None}
    parts = [part.strip() for part in cleaned.split(",") if part.strip()]
    venue = parts[0] if parts else cleaned
    city = None
    country = None
    if len(parts) >= 2:
        city = parts[-1]
    if len(parts) >= 3:
        country = parts[-1]
        city = parts[-2]
    return {
        "venue": venue or None,
        "address": cleaned,
        "city": city or None,
        "country": country or None,
    }


def _format_price(
    *,
    minimum: object | None = None,
    maximum: object | None = None,
    currency: str | None = None,
) -> tuple[str | None, bool]:
    def _as_float(value: object | None) -> float | None:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        cleaned = _clean_text(value)
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except (TypeError, ValueError):
            return None

    low = _as_float(minimum)
    high = _as_float(maximum)
    if low is None and high is None:
        return None, False
    if low is not None and high is not None and abs(low - high) > 0.009:
        amount = f"{low:.2f}-{high:.2f}"
        free = low == 0 and high == 0
    else:
        resolved = low if low is not None else high
        if resolved is None:
            return None, False
        amount = f"{resolved:.2f}"
        free = resolved == 0
    return (f"{currency} {amount}".strip() if currency else amount), free


def _extract_integer(value: object) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value) if value.is_integer() else int(round(value))
    cleaned = _clean_text(value)
    if not cleaned:
        return None
    match = re.search(r"(\d[\d,]*)", cleaned)
    if not match:
        return None
    try:
        return int(match.group(1).replace(",", ""))
    except ValueError:
        return None


def _normalize_event_status(value: object) -> tuple[str | None, bool | None]:
    cleaned = _clean_text(value)
    if not cleaned:
        return None, None
    lowered = cleaned.lower()
    lowered = lowered.rsplit("/", 1)[-1]
    normalized = lowered.replace("-", " ").replace("_", " ").strip()
    if "cancel" in normalized:
        return "cancelled", True
    if "postpon" in normalized:
        return "postponed", False
    if "resched" in normalized:
        return "rescheduled", False
    if "sold out" in normalized or "soldout" in normalized or "sales ended" in normalized:
        return "sold_out", False
    if "login" in normalized or "sign in" in normalized or "join meetup" in normalized:
        return "requires_login", False
    if normalized in {"eventscheduled", "scheduled", "published", "available"}:
        return "scheduled", False
    return normalized.replace(" ", "_"), False


def _derive_event_type_label(value: object) -> str | None:
    values = value if isinstance(value, list) else [value]
    cleaned_values = [
        cleaned
        for item in values
        if isinstance(item, str)
        if (cleaned := _clean_text(item))
    ]
    for cleaned in cleaned_values:
        if cleaned != "Event":
            return cleaned
    return cleaned_values[0] if cleaned_values else None


def _extract_timezone(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.tzname() if value.tzinfo else None
    cleaned = _clean_text(value)
    if not cleaned:
        return None
    try:
        dt = date_parser.parse(cleaned)
    except (ValueError, TypeError, OverflowError):
        return None
    return dt.tzname() if dt.tzinfo else None


def _extract_online_mode(*, attendance_mode: object, location: object, fallback_text: str | None = None) -> str | None:
    attendance = _clean_text(attendance_mode)
    if attendance:
        lowered = attendance.lower()
        if "mixed" in lowered or "hybrid" in lowered:
            return "hybrid"
        if "online" in lowered:
            return "online"
        if "offline" in lowered or "physical" in lowered:
            return "in_person"
    if isinstance(location, dict):
        if _clean_text(location.get("@type")) == "VirtualLocation":
            return "online"
        name = _clean_text(location.get("name"))
        if name and "hybrid" in name.lower():
            return "hybrid"
        if name and "online" in name.lower():
            return "online"
    location_text = _clean_text(location)
    if location_text:
        lowered = location_text.lower()
        if "hybrid" in lowered:
            return "hybrid"
        if "online" in lowered or "virtual" in lowered:
            return "online"
    fallback = _clean_text(fallback_text)
    if fallback:
        lowered = fallback.lower()
        if "hybrid event" in lowered:
            return "hybrid"
        if "online event" in lowered or "virtual event" in lowered:
            return "online"
    return None


def _extract_offer_bits(offers: object) -> dict[str, str | bool | None]:
    offer = offers[0] if isinstance(offers, list) and offers else offers
    if not isinstance(offer, dict):
        return {
            "price": None,
            "currency": None,
            "is_free": None,
            "ticket_url": None,
            "status": None,
        }
    raw_price = offer.get("price")
    price = _clean_text(raw_price)
    currency = _string_at(offer, "priceCurrency")
    free = False
    if isinstance(raw_price, (int, float)):
        free = float(raw_price) == 0
    elif isinstance(raw_price, str):
        free = raw_price.strip().lower() in {"0", "0.0", "free"}
    if not free and isinstance(price, str):
        free = price.lower() == "free"
    availability_status, _ = _normalize_event_status(
        offer.get("availability") or offer.get("availabilityStarts")
    )
    return {
        "price": price,
        "currency": currency,
        "is_free": free,
        "ticket_url": _clean_url(offer.get("url")),
        "status": availability_status,
    }


def _event_candidate_from_json_ld(
    node: dict[str, Any],
    *,
    source_url: str,
    page_url: str,
    node_index: dict[str, dict[str, Any]],
) -> EventCandidate | None:
    title = _string_at(node, "name", "title")
    if not title:
        return None
    resolved_location = _resolve_node_ref(node.get("location"), node_index=node_index)
    location_bits = _extract_location_bits(resolved_location)
    offer_bits = _extract_offer_bits(_resolve_node_ref(node.get("offers"), node_index=node_index))
    event_url = _clean_url(node.get("url"), base_url=page_url) or page_url
    organizer = None
    group_name = None
    organizer_data = node.get("organizer")
    if isinstance(organizer_data, dict):
        organizer = _string_at(organizer_data, "name")
    else:
        organizer = _clean_text(organizer_data)
    group_name = organizer
    image = node.get("image")
    image_url = None
    if isinstance(image, list):
        image_url = _clean_url(image[0], base_url=page_url) if image else None
    else:
        image_url = _clean_url(image, base_url=page_url)
    status, cancelled = _normalize_event_status(node.get("eventStatus") or node.get("status"))
    aggregate_rating = (
        _resolve_node_ref(node.get("aggregateRating"), node_index=node_index)
        if isinstance(node.get("aggregateRating"), (dict, list))
        else node.get("aggregateRating")
    )
    review_count = None
    if isinstance(aggregate_rating, dict):
        review_count = _extract_integer(
            aggregate_rating.get("reviewCount") or aggregate_rating.get("ratingCount")
        )
    attendance_mode = _extract_online_mode(
        attendance_mode=node.get("eventAttendanceMode"),
        location=resolved_location,
        fallback_text=title,
    )
    return EventCandidate(
        title=title,
        summary=_string_at(node, "description"),
        start_at=_normalize_datetime(node.get("startDate") or node.get("start_date")),
        end_at=_normalize_datetime(node.get("endDate") or node.get("end_date")),
        venue=location_bits["venue"],
        address=location_bits["address"],
        city=location_bits["city"],
        country=location_bits["country"],
        organizer=organizer,
        group_name=group_name,
        price=offer_bits["price"] if isinstance(offer_bits["price"], str) else None,
        currency=offer_bits["currency"] if isinstance(offer_bits["currency"], str) else None,
        is_free=bool(offer_bits["is_free"]) if offer_bits["is_free"] is not None else None,
        event_url=event_url,
        source_url=source_url,
        source_name=_source_name_for_url(source_url),
        image_url=image_url,
        event_type=_derive_event_type_label(node.get("@type")),
        status=status,
        cancelled=cancelled,
        online_or_hybrid=attendance_mode,
        attendee_count=_extract_integer(
            node.get("maximumAttendeeCapacity")
            or node.get("remainingAttendeeCapacity")
            or node.get("attendeeCount")
        ),
        review_count=review_count,
        ticket_url=offer_bits["ticket_url"] if isinstance(offer_bits["ticket_url"], str) else None,
        timezone=_extract_timezone(node.get("startDate") or node.get("endDate")),
    )


def extract_events_from_html(html: str, *, source_url: str, page_url: str) -> list[EventCandidate]:
    candidates: list[EventCandidate] = []
    for match in SCRIPT_JSON_LD_RE.finditer(html):
        body = _clean_text(match.group("body"))
        if not body:
            continue
        try:
            payload = json.loads(body)
        except json.JSONDecodeError:
            continue
        nodes = _iter_json_ld_nodes(payload)
        node_index = _build_node_index(nodes)
        for node in nodes:
            if not _is_event_type(node.get("@type")):
                continue
            candidate = _event_candidate_from_json_ld(
                node,
                source_url=source_url,
                page_url=page_url,
                node_index=node_index,
            )
            if candidate is not None:
                candidates.append(candidate)
    return candidates


def _extract_h1_text(html: str) -> str | None:
    match = re.search(r"<h1[^>]*>(?P<title>.*?)</h1>", html, re.IGNORECASE | re.DOTALL)
    return _clean_text(match.group("title")) if match else None


def _extract_section_window(html: str, *labels: str, limit: int = 1400) -> str | None:
    lowered = html.lower()
    for label in labels:
        index = lowered.find(label.lower())
        if index >= 0:
            return _clean_text(html[index : index + limit])
    return None


def _extract_time_datetime_value(html: str) -> str | None:
    match = re.search(r'<time[^>]+datetime=["\'](?P<value>[^"\']+)["\']', html, re.IGNORECASE)
    return _clean_text(match.group("value")) if match else None


def _extract_count_from_text(text: str | None, *patterns: str) -> int | None:
    if not text:
        return None
    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return _extract_integer(match.group(1))
    return None


def _first_present(*values: str | None) -> str | None:
    for value in values:
        if value:
            return value
    return None


def _merge_status(
    primary_status: str | None,
    primary_cancelled: bool | None,
    fallback_status: str | None,
    fallback_cancelled: bool | None,
) -> tuple[str | None, bool | None]:
    return (
        primary_status if primary_status is not None else fallback_status,
        primary_cancelled if primary_cancelled is not None else fallback_cancelled,
    )


def _eventbrite_detail_candidate_from_html(
    html: str,
    *,
    source_url: str,
    page_url: str,
) -> EventCandidate | None:
    schema_candidates = extract_events_from_html(html, source_url=source_url, page_url=page_url)
    base = next(
        (item for item in schema_candidates if item.event_url == _clean_url(page_url)),
        schema_candidates[0] if schema_candidates else None,
    )
    page_text = _clean_text(html) or ""
    title = _first_present(
        base.title if base else None,
        _extract_h1_text(html),
        _extract_meta_value(html, "og:title", "twitter:title"),
    )
    if not title:
        return None
    section_summary = _first_present(
        _extract_section_window(html, "About this event"),
        _extract_section_window(html, "Good to know"),
        _extract_section_window(html, "Agenda"),
    )
    location_text = _extract_section_window(html, "Location", "Where")
    location_bits = _location_bits_from_text(location_text)
    organizer = _first_present(
        base.organizer if base else None,
        _clean_text(
            re.search(r"\bBy\s+([A-Z0-9][^\n<]{2,80})", page_text, re.IGNORECASE).group(1)
            if re.search(r"\bBy\s+([A-Z0-9][^\n<]{2,80})", page_text, re.IGNORECASE)
            else None
        ),
    )
    fallback_status, fallback_cancelled = _normalize_event_status(
        "sold out"
        if re.search(r"\b(Sold out|Sales ended)\b", page_text, re.IGNORECASE)
        else "scheduled"
        if re.search(r"\bCheck availability\b", page_text, re.IGNORECASE)
        else None
    )
    status, cancelled = _merge_status(
        base.status if base else None,
        base.cancelled if base else None,
        fallback_status,
        fallback_cancelled,
    )
    online_or_hybrid = _first_present(
        base.online_or_hybrid if base else None,
        _extract_online_mode(
            attendance_mode=None,
            location=location_text,
            fallback_text=page_text,
        ),
    )
    attendee_count = _first_present(
        str(base.attendee_count) if base and base.attendee_count is not None else None,
        str(
            _extract_count_from_text(
                page_text,
                r"(\d[\d,]*)\s+(?:attendees?|people going|going)",
                r"(\d[\d,]*)\s+(?:spots|seats)\s+left",
            )
        )
        if _extract_count_from_text(
            page_text,
            r"(\d[\d,]*)\s+(?:attendees?|people going|going)",
            r"(\d[\d,]*)\s+(?:spots|seats)\s+left",
        )
        is not None
        else None,
    )
    review_count = _first_present(
        str(base.review_count) if base and base.review_count is not None else None,
        str(_extract_count_from_text(page_text, r"(\d[\d,]*)\s+reviews?"))
        if _extract_count_from_text(page_text, r"(\d[\d,]*)\s+reviews?") is not None
        else None,
    )
    return EventCandidate(
        title=title,
        event_url=_clean_url(page_url) or page_url,
        source_url=source_url,
        source_name=_source_name_for_url(source_url),
        summary=_first_present(
            base.summary if base else None,
            _extract_meta_value(html, "description", "og:description", "twitter:description"),
            section_summary,
        ),
        start_at=_first_present(base.start_at if base else None, _normalize_datetime(_extract_time_datetime_value(html))),
        end_at=base.end_at if base else None,
        venue=_first_present(base.venue if base else None, location_bits["venue"]),
        address=_first_present(base.address if base else None, location_bits["address"]),
        city=_first_present(base.city if base else None, location_bits["city"]),
        country=_first_present(
            base.country if base else None,
            "Australia" if ".com.au" in source_url else "New Zealand",
        ),
        organizer=organizer,
        group_name=_first_present(base.group_name if base else None, organizer),
        price=base.price if base else None,
        currency=base.currency if base else None,
        is_free=base.is_free if base else None,
        image_url=_first_present(
            base.image_url if base else None,
            _clean_url(
                _extract_meta_value(html, "og:image", "twitter:image", "twitter:image:src"),
                base_url=page_url,
            ),
        ),
        event_type=_first_present(base.event_type if base else None, "Event"),
        status=status,
        cancelled=cancelled,
        online_or_hybrid=online_or_hybrid,
        attendee_count=int(attendee_count) if attendee_count else (base.attendee_count if base else None),
        review_count=int(review_count) if review_count else (base.review_count if base else None),
        ticket_url=_first_present(base.ticket_url if base else None, _clean_url(page_url)),
        timezone=_first_present(
            base.timezone if base else None,
            _extract_timezone(_extract_time_datetime_value(html)),
        ),
    )


def _meetup_detail_candidate_from_html(
    html: str,
    *,
    source_url: str,
    page_url: str,
) -> EventCandidate | None:
    schema_candidates = extract_events_from_html(html, source_url=source_url, page_url=page_url)
    base = next(
        (item for item in schema_candidates if item.event_url == _clean_url(page_url)),
        schema_candidates[0] if schema_candidates else None,
    )
    page_text = _clean_text(html) or ""
    title = _first_present(
        base.title if base else None,
        _extract_h1_text(html),
        _extract_meta_value(html, "og:title", "twitter:title"),
    )
    if not title:
        return None
    details_text = _first_present(
        _extract_section_window(html, "Details"),
        _extract_section_window(html, "About"),
        _extract_meta_value(html, "description", "og:description", "twitter:description"),
    )
    location_text = _first_present(
        _extract_section_window(html, "Location"),
        _extract_section_window(html, "Online event"),
    )
    location_bits = _location_bits_from_text(location_text)
    hosted_match = re.search(
        r"\b(?:Hosted by|Host(?:ed)?|Group)\s+(.{2,90}?)(?=\s+\d+\s+(?:attendees?|reviews?)|\s+Join Meetup|$)",
        page_text,
        re.IGNORECASE,
    )
    group_name = _first_present(
        base.group_name if base else None,
        _clean_text(hosted_match.group(1)) if hosted_match else None,
        base.organizer if base else None,
    )
    fallback_status, fallback_cancelled = _normalize_event_status(
        "requires login"
        if re.search(r"\b(Log in|Join Meetup|Sign in)\b", page_text, re.IGNORECASE)
        else "cancelled"
        if re.search(r"\bCancelled\b", page_text, re.IGNORECASE)
        else None
    )
    status, cancelled = _merge_status(
        base.status if base else None,
        base.cancelled if base else None,
        fallback_status,
        fallback_cancelled,
    )
    attendee_count = _extract_count_from_text(
        page_text,
        r"(\d[\d,]*)\s+(?:attendees?|going)",
        r"(\d[\d,]*)\s+people\s+going",
    )
    review_count = _extract_count_from_text(
        page_text,
        r"(\d[\d,]*)\s+reviews?",
        r"rated\s+(\d[\d,]*)",
    )
    return EventCandidate(
        title=title,
        event_url=_clean_url(page_url) or page_url,
        source_url=source_url,
        source_name=_source_name_for_url(source_url),
        summary=_first_present(base.summary if base else None, details_text),
        start_at=_first_present(base.start_at if base else None, _normalize_datetime(_extract_time_datetime_value(html))),
        end_at=base.end_at if base else None,
        venue=_first_present(base.venue if base else None, location_bits["venue"]),
        address=_first_present(base.address if base else None, location_bits["address"]),
        city=_first_present(base.city if base else None, location_bits["city"]),
        country=_first_present(base.country if base else None, location_bits["country"]),
        organizer=_first_present(base.organizer if base else None, group_name),
        group_name=group_name,
        price=base.price if base else None,
        currency=base.currency if base else None,
        is_free=base.is_free if base else None,
        image_url=_first_present(
            base.image_url if base else None,
            _clean_url(
                _extract_meta_value(html, "og:image", "twitter:image", "twitter:image:src"),
                base_url=page_url,
            ),
        ),
        event_type=_first_present(base.event_type if base else None, "Meetup"),
        status=status,
        cancelled=cancelled,
        online_or_hybrid=_first_present(
            base.online_or_hybrid if base else None,
            _extract_online_mode(attendance_mode=None, location=location_text, fallback_text=page_text),
        ),
        attendee_count=attendee_count if attendee_count is not None else (base.attendee_count if base else None),
        review_count=review_count if review_count is not None else (base.review_count if base else None),
        ticket_url=_first_present(base.ticket_url if base else None, _clean_url(page_url)),
        timezone=_first_present(
            base.timezone if base else None,
            _extract_timezone(_extract_time_datetime_value(html)),
        ),
    )


def _eventfinda_candidates_from_html(html: str, *, source_url: str) -> list[EventCandidate]:
    candidates: list[EventCandidate] = []
    block_re = re.compile(
        r'<div class="d-flex align-items-stretch[^"]*">(?P<body>.*?)<div class="card-body">(?P<card>.*?)</div>\s*</div>',
        re.IGNORECASE | re.DOTALL,
    )
    title_re = re.compile(
        r'<h2 class="card-title[^"]*"><a href=["\'](?P<href>[^"\']+)["\'][^>]*>(?P<title>.*?)</a></h2>',
        re.IGNORECASE | re.DOTALL,
    )
    location_re = re.compile(
        r'<p class="card-text meta-location[^"]*">(?P<location>.*?)</p>',
        re.IGNORECASE | re.DOTALL,
    )
    date_re = re.compile(
        r'<p class="card-text meta-date">(?P<date>.*?)</p>',
        re.IGNORECASE | re.DOTALL,
    )
    image_re = re.compile(r'<img[^>]+src=["\'](?P<src>[^"\']+)["\']', re.IGNORECASE)
    for match in block_re.finditer(html):
        card = match.group("card")
        title_match = title_re.search(card)
        if not title_match:
            continue
        title = _clean_text(title_match.group("title"))
        href = _clean_url(title_match.group("href"), base_url=source_url)
        if not title or not href:
            continue
        location_match = location_re.search(card)
        date_match = date_re.search(card)
        image_match = image_re.search(match.group("body"))
        location_bits = _location_bits_from_text(
            location_match.group("location") if location_match else None
        )
        candidates.append(
            EventCandidate(
                title=title,
                event_url=href,
                source_url=source_url,
                source_name=_source_name_for_url(source_url),
                start_at=_normalize_datetime(date_match.group("date") if date_match else None),
                venue=location_bits["venue"],
                address=location_bits["address"],
                city=location_bits["city"],
                country=location_bits["country"],
                image_url=_clean_url(image_match.group("src"), base_url=source_url)
                if image_match
                else None,
            )
        )
    return candidates


def _wellingtonnz_candidates_from_html(html: str, *, source_url: str) -> list[EventCandidate]:
    candidates: list[EventCandidate] = []
    block_re = re.compile(
        r'<a href=["\'](?P<href>/visit/events/[^"\']+)["\'][^>]*class=["\'][^"\']*featured-item[^"\']*["\'][^>]*>(?P<body>.*?)</a>',
        re.IGNORECASE | re.DOTALL,
    )
    title_re = re.compile(
        r'<h2 class="featured-item__title"[^>]*>(?P<title>.*?)</h2>',
        re.IGNORECASE | re.DOTALL,
    )
    date_re = re.compile(
        r'<span class="featured-item__text featured-item__text--date"[^>]*>(?P<date>.*?)</span>',
        re.IGNORECASE | re.DOTALL,
    )
    venue_re = re.compile(
        r'<span class="featured-item__text featured-item__text--info"[^>]*>(?P<venue>.*?)</span>',
        re.IGNORECASE | re.DOTALL,
    )
    image_re = re.compile(r'<img[^>]+src=["\'](?P<src>[^"\']+)["\']', re.IGNORECASE)
    for match in block_re.finditer(html):
        title_match = title_re.search(match.group("body"))
        title = _clean_text(title_match.group("title")) if title_match else None
        href = _clean_url(match.group("href"), base_url=source_url)
        if not title or not href:
            continue
        date_match = date_re.search(match.group("body"))
        venue_match = venue_re.search(match.group("body"))
        image_match = image_re.search(match.group("body"))
        venue = _clean_text(venue_match.group("venue")) if venue_match else None
        candidates.append(
            EventCandidate(
                title=title,
                event_url=href,
                source_url=source_url,
                source_name=_source_name_for_url(source_url),
                start_at=_normalize_datetime(date_match.group("date") if date_match else None),
                venue=venue,
                city="Wellington" if venue else None,
                country="New Zealand" if venue else None,
                image_url=_clean_url(image_match.group("src"), base_url=source_url)
                if image_match
                else None,
            )
        )
    return candidates


def _whatson_melbourne_candidates_from_html(html: str, *, source_url: str) -> list[EventCandidate]:
    candidates: list[EventCandidate] = []
    block_re = re.compile(
        r'<div data-track-index=&quot;\d+&quot;[^>]*data-listing-type=&quot;event&quot;[^>]*class="page-preview[^"]*"[^>]*>(?P<body>.*?)</div>\s*</div>',
        re.IGNORECASE | re.DOTALL,
    )
    href_re = re.compile(
        r'<a class="main-link"[^>]*href=["\'](?P<href>[^"\']+)["\']',
        re.IGNORECASE | re.DOTALL,
    )
    title_re = re.compile(r'<h3 class="title">(?P<title>.*?)</h3>', re.IGNORECASE | re.DOTALL)
    date_re = re.compile(
        r'<span class="from-to-date[^"]*">(?P<date>.*?)</span>',
        re.IGNORECASE | re.DOTALL,
    )
    time_datetime_re = re.compile(
        r'<time[^>]+datetime=["\'](?P<date>[^"\']+)["\']',
        re.IGNORECASE | re.DOTALL,
    )
    summary_re = re.compile(r'<p class="summary">(?P<summary>.*?)</p>', re.IGNORECASE | re.DOTALL)
    image_re = re.compile(r'<img[^>]+src=["\'](?P<src>[^"\']+)["\']', re.IGNORECASE)
    for match in block_re.finditer(html):
        body = match.group("body")
        href_match = href_re.search(body)
        title_match = title_re.search(body)
        if not href_match or not title_match:
            continue
        href = _clean_url(href_match.group("href"), base_url=source_url)
        title = _clean_text(title_match.group("title"))
        if not href or not title:
            continue
        date_match = date_re.search(body)
        time_datetime_match = time_datetime_re.search(body)
        summary_match = summary_re.search(body)
        image_match = image_re.search(body)
        date_text = _clean_text(date_match.group("date")) if date_match else None
        normalized_date = _normalize_datetime(
            time_datetime_match.group("date")
            if time_datetime_match
            else (date_match.group("date") if date_match else None)
        )
        is_until_event = isinstance(date_text, str) and "until" in date_text.lower()
        candidates.append(
            EventCandidate(
                title=title,
                event_url=href,
                source_url=source_url,
                source_name=_source_name_for_url(source_url),
                start_at=None if is_until_event else normalized_date,
                end_at=normalized_date if is_until_event else None,
                summary=_clean_text(summary_match.group("summary")) if summary_match else None,
                city="Melbourne",
                country="Australia",
                image_url=_clean_url(image_match.group("src"), base_url=source_url)
                if image_match
                else None,
            )
        )
    return candidates


def _is_whatson_event_path(path: str) -> bool:
    normalized = (path or "").strip().rstrip("/")
    if not normalized:
        return False
    if normalized.lower().startswith("/whatson/pages/event.aspx"):
        return True
    prefix = "/things-to-do/"
    if not normalized.startswith(prefix):
        return False
    slug = normalized[len(prefix) :]
    if not slug or "/" in slug:
        return False
    generic_slugs = {
        "free",
        "family-and-kids",
        "entertainment",
        "attractions-and-sights",
        "major-events",
        "exhibitions",
        "festivals",
        "aboriginal-melbourne",
        "walks",
        "whats-on-today",
        "this-weekend",
        "whats-on-this-week-in-melbourne",
        "whats-on-march",
    }
    return slug not in generic_slugs


def _eventbrite_candidates_from_html(html: str, *, source_url: str) -> list[EventCandidate]:
    candidates: list[EventCandidate] = []
    block_re = re.compile(
        r'<section class="discover-vertical-event-card">(?P<body>.*?)</section>\s*</div>',
        re.IGNORECASE | re.DOTALL,
    )
    href_re = re.compile(
        r'<a href=["\'](?P<href>https?://(?:www\.)?eventbrite\.(?:com\.au|co\.nz)/[^"\']+)["\']',
        re.IGNORECASE,
    )
    title_re = re.compile(r"<h3[^>]*>(?P<title>.*?)</h3>", re.IGNORECASE | re.DOTALL)
    date_re = re.compile(
        r'<p[^>]*Typography_body-md-bold[^>]*>(?P<date>.*?)</p>',
        re.IGNORECASE | re.DOTALL,
    )
    venue_re = re.compile(
        r'<p[^>]*Typography_body-md__487rx[^>]*>(?P<venue>.*?)</p>',
        re.IGNORECASE | re.DOTALL,
    )
    image_re = re.compile(r'<img[^>]+src=["\'](?P<src>[^"\']+)["\']', re.IGNORECASE)
    for match in block_re.finditer(html):
        body = match.group("body")
        href_match = href_re.search(body)
        title_match = title_re.search(body)
        if not href_match or not title_match:
            continue
        href = _clean_url(href_match.group("href"), base_url=source_url)
        if not href or not _is_allowed_event_url(href, source_url=source_url):
            continue
        title = _clean_text(title_match.group("title"))
        if not title:
            continue
        venue_text = _clean_text(venue_re.search(body).group("venue")) if venue_re.search(body) else None
        location_bits = _location_bits_from_text(venue_text)
        candidates.append(
            EventCandidate(
                title=title,
                event_url=href,
                source_url=source_url,
                source_name=_source_name_for_url(source_url),
                start_at=_normalize_datetime(date_re.search(body).group("date")) if date_re.search(body) else None,
                venue=location_bits["venue"],
                address=location_bits["address"],
                city=location_bits["city"],
                country="Australia" if ".com.au" in source_url else "New Zealand",
                image_url=_clean_url(image_re.search(body).group("src"), base_url=source_url)
                if image_re.search(body)
                else None,
            )
        )
    return candidates


def _humanitix_candidates_from_html(html: str, *, source_url: str) -> list[EventCandidate]:
    match = NEXT_DATA_RE.search(html)
    if not match:
        return []
    body = _clean_text(match.group("body"))
    if not body:
        return []
    try:
        payload = json.loads(body)
    except json.JSONDecodeError:
        return []
    entries = (
        payload.get("props", {})
        .get("pageProps", {})
        .get("featuredCarouselEntries", {})
        .get("verticalEvents", [])
    )
    if not isinstance(entries, list):
        return []
    candidates: list[EventCandidate] = []
    for entry in entries[:25]:
        if not isinstance(entry, dict):
            continue
        title = _clean_text(entry.get("name"))
        event_url = _clean_url(
            f"{_clean_url(entry.get('hostname')) or 'https://events.humanitix.com'}/{str(entry.get('slug') or '').lstrip('/')}"
        )
        if not title or not event_url:
            continue
        date_payload = entry.get("date") if isinstance(entry.get("date"), dict) else {}
        location = entry.get("eventLocation") if isinstance(entry.get("eventLocation"), dict) else {}
        pricing = entry.get("pricing") if isinstance(entry.get("pricing"), dict) else {}
        address = _clean_text(location.get("address"))
        venue = _clean_text(location.get("venueName"))
        city = None
        country = None
        components = location.get("addressComponents")
        if isinstance(components, list):
            for component in components:
                if not isinstance(component, dict):
                    continue
                types = component.get("types")
                if not isinstance(types, list):
                    continue
                if "locality" in types and not city:
                    city = _clean_text(component.get("short_name") or component.get("long_name"))
                if "country" in types and not country:
                    country = _clean_text(component.get("long_name"))
        currency = "AUD" if str(entry.get("location") or "").upper() == "AU" else "NZD"
        price, is_free = _format_price(
            minimum=pricing.get("minimumPrice"),
            maximum=pricing.get("maximumPrice"),
            currency=currency,
        )
        image_url = None
        banner = entry.get("bannerImage")
        if isinstance(banner, dict):
            handle = _clean_text(banner.get("handle"))
            if handle:
                image_url = f"https://images.humanitix.com/{handle}"
        candidates.append(
            EventCandidate(
                title=title,
                event_url=event_url,
                source_url=source_url,
                source_name=_source_name_for_url(source_url),
                start_at=_normalize_datetime(date_payload.get("startDate")),
                end_at=_normalize_datetime(date_payload.get("endDate")),
                venue=venue,
                address=address,
                city=city,
                country=country,
                organizer=_clean_text((entry.get("organiser") or {}).get("name"))
                if isinstance(entry.get("organiser"), dict)
                else None,
                price=price,
                currency=currency,
                is_free=is_free,
                image_url=image_url,
            )
        )
    return candidates


def _source_specific_candidates_from_html(
    html: str,
    *,
    source_url: str,
    page_url: str,
) -> list[EventCandidate]:
    adapter_key = _adapter_key_for_url(source_url)
    if adapter_key == "eventbrite":
        if page_url != source_url:
            detailed = _eventbrite_detail_candidate_from_html(
                html,
                source_url=source_url,
                page_url=page_url,
            )
            return [detailed] if detailed else []
        adapter_candidates = _eventbrite_candidates_from_html(html, source_url=source_url)
        return adapter_candidates or extract_events_from_html(
            html,
            source_url=source_url,
            page_url=page_url,
        )
    if adapter_key == "eventfinda":
        adapter_candidates = _eventfinda_candidates_from_html(html, source_url=source_url)
        return adapter_candidates or extract_events_from_html(
            html,
            source_url=source_url,
            page_url=page_url,
        )
    if adapter_key == "wellingtonnz":
        adapter_candidates = _wellingtonnz_candidates_from_html(html, source_url=source_url)
        return adapter_candidates or extract_events_from_html(
            html,
            source_url=source_url,
            page_url=page_url,
        )
    if adapter_key == "whatson_melbourne":
        if page_url == source_url:
            return _whatson_melbourne_candidates_from_html(html, source_url=source_url)
        return extract_events_from_html(html, source_url=source_url, page_url=page_url)
    if adapter_key == "humanitix":
        adapter_candidates = _humanitix_candidates_from_html(html, source_url=source_url)
        return adapter_candidates or extract_events_from_html(
            html,
            source_url=source_url,
            page_url=page_url,
        )
    if adapter_key == "meetup":
        if page_url != source_url:
            detailed = _meetup_detail_candidate_from_html(
                html,
                source_url=source_url,
                page_url=page_url,
            )
            return [detailed] if detailed else []
        return extract_events_from_html(html, source_url=source_url, page_url=page_url)
    if adapter_key == "concreteplayground":
        if page_url == source_url:
            return []
        return extract_events_from_html(html, source_url=source_url, page_url=page_url)
    return extract_events_from_html(html, source_url=source_url, page_url=page_url)


def _extract_meta_value(html: str, *names: str) -> str | None:
    wanted = {name.lower() for name in names}
    for match in META_TAG_RE.finditer(html):
        name = _clean_text(match.group("name"))
        if not name or name.lower() not in wanted:
            continue
        content = _clean_text(match.group("content"))
        if content:
            return content
    return None


def _extract_first_image(html: str, *, page_url: str) -> str | None:
    for match in IMG_RE.finditer(html):
        src = _clean_url(match.group("src"), base_url=page_url)
        if not src:
            continue
        lowered = src.lower()
        if any(blocked in lowered for blocked in ("favicon", "apple-touch-icon", "shielded-logo")):
            continue
        return src
    return None


def _candidate_from_meta(html: str, *, source_url: str, page_url: str) -> EventCandidate | None:
    title = _first_present(
        _extract_h1_text(html),
        _extract_meta_value(html, "og:title", "twitter:title", "title"),
    )
    if not title:
        match = TITLE_RE.search(html)
        title = _clean_text(match.group("title")) if match else None
    if not title:
        return None
    page_text = _clean_text(html) or ""
    status, cancelled = _normalize_event_status(
        "sold out"
        if re.search(r"\b(Sold out|Sales ended)\b", page_text, re.IGNORECASE)
        else "cancelled"
        if re.search(r"\bCancelled\b", page_text, re.IGNORECASE)
        else "requires login"
        if re.search(r"\b(Log in|Sign in|Join Meetup)\b", page_text, re.IGNORECASE)
        else None
    )
    return EventCandidate(
        title=title,
        event_url=page_url,
        source_url=source_url,
        source_name=_source_name_for_url(source_url),
        summary=_extract_meta_value(html, "description", "og:description", "twitter:description"),
        image_url=_clean_url(
            _extract_meta_value(html, "og:image", "twitter:image", "twitter:image:src"),
            base_url=page_url,
        )
        or _extract_first_image(html, page_url=page_url),
        status=status,
        cancelled=cancelled,
        online_or_hybrid=_extract_online_mode(
            attendance_mode=None,
            location=_extract_section_window(html, "Location", "Where", "Online event"),
            fallback_text=page_text,
        ),
        timezone=_extract_timezone(_extract_time_datetime_value(html)),
        ticket_url=_clean_url(page_url),
    )


def _prefer_text(primary: str | None, secondary: str | None) -> str | None:
    if primary and secondary:
        return primary if len(primary) >= len(secondary) else secondary
    return secondary or primary


def _merge_candidates(base: EventCandidate, detailed: EventCandidate | None) -> EventCandidate:
    if detailed is None:
        return base
    return EventCandidate(
        title=detailed.title or base.title,
        event_url=detailed.event_url or base.event_url,
        source_url=base.source_url,
        source_name=base.source_name,
        summary=_prefer_text(base.summary, detailed.summary),
        start_at=detailed.start_at or base.start_at,
        end_at=detailed.end_at or base.end_at,
        venue=detailed.venue or base.venue,
        address=detailed.address or base.address,
        city=detailed.city or base.city,
        country=detailed.country or base.country,
        organizer=detailed.organizer or base.organizer,
        group_name=detailed.group_name or base.group_name,
        price=detailed.price or base.price,
        currency=detailed.currency or base.currency,
        is_free=detailed.is_free if detailed.is_free is not None else base.is_free,
        image_url=detailed.image_url or base.image_url,
        event_type=detailed.event_type or base.event_type,
        status=detailed.status or base.status,
        cancelled=detailed.cancelled if detailed.cancelled is not None else base.cancelled,
        online_or_hybrid=detailed.online_or_hybrid or base.online_or_hybrid,
        attendee_count=detailed.attendee_count if detailed.attendee_count is not None else base.attendee_count,
        review_count=detailed.review_count if detailed.review_count is not None else base.review_count,
        ticket_url=detailed.ticket_url or base.ticket_url,
        timezone=detailed.timezone or base.timezone,
    )


def _candidate_needs_enrichment(candidate: EventCandidate) -> bool:
    return not all(
        [
            candidate.start_at,
            candidate.venue or candidate.address,
            candidate.image_url,
            candidate.summary,
        ]
    )


async def _enrich_candidate_from_page(
    client: httpx.AsyncClient,
    *,
    candidate: EventCandidate,
) -> EventCandidate:
    if not candidate.event_url or candidate.event_url == candidate.source_url:
        return candidate
    try:
        _, page = await _fetch_text(client, candidate.event_url)
    except httpx.HTTPError:
        return candidate
    page_candidates = _source_specific_candidates_from_html(
        page,
        source_url=candidate.source_url,
        page_url=candidate.event_url,
    )
    detailed: EventCandidate | None = None
    if page_candidates:
        detailed = next(
            (
                item
                for item in page_candidates
                if item.event_url == candidate.event_url
                or item.title.strip().lower() == candidate.title.strip().lower()
            ),
            page_candidates[0],
        )
    else:
        detailed = _candidate_from_meta(
            page,
            source_url=candidate.source_url,
            page_url=candidate.event_url,
        )
    return _merge_candidates(candidate, detailed)


def _source_specific_link_candidates(html: str, *, source_url: str) -> list[EventCandidate]:
    adapter_key = _adapter_key_for_url(source_url)
    candidates: list[EventCandidate] = []
    seen: set[str] = set()
    if adapter_key == "eventbrite":
        for match in EVENTBRITE_EVENT_LINK_RE.finditer(html):
            href = _clean_url(match.group("href"), base_url=source_url)
            if not href or href in seen:
                continue
            seen.add(href)
            candidates.append(
                EventCandidate(
                    title=_clean_text(urlparse(href).path.rsplit("/", 1)[-1].replace("-", " ")) or "Eventbrite event",
                    event_url=href,
                    source_url=source_url,
                    source_name=_source_name_for_url(source_url),
                )
            )
        return candidates
    if adapter_key == "whatson_melbourne":
        for match in WHATSON_MELBOURNE_LINK_RE.finditer(html):
            href = _clean_url(match.group("href"), base_url=source_url)
            if not href or href in seen:
                continue
            if not _is_whatson_event_path(urlparse(href).path):
                continue
            seen.add(href)
            candidates.append(
                EventCandidate(
                    title=_clean_text(urlparse(href).path.rsplit("/", 1)[-1].replace("-", " "))
                    or _clean_text(urlparse(href).query.replace("+", " "))
                    or "Melbourne event",
                    event_url=href,
                    source_url=source_url,
                    source_name=_source_name_for_url(source_url),
                )
            )
        return candidates
    if adapter_key == "concreteplayground":
        for match in CONCRETE_PLAYGROUND_LINK_RE.finditer(html):
            href = _clean_url(match.group("href"), base_url=source_url)
            if not href or href in seen:
                continue
            seen.add(href)
            candidates.append(
                EventCandidate(
                    title=_clean_text(urlparse(href).path.rsplit("/", 1)[-1].replace("-", " "))
                    or "Concrete Playground event",
                    event_url=href,
                    source_url=source_url,
                    source_name=_source_name_for_url(source_url),
                )
            )
        return candidates
    return []


def _extract_links(html: str, *, page_url: str) -> list[str]:
    ranked: list[tuple[int, str]] = []
    seen: set[str] = set()
    for match in ANCHOR_RE.finditer(html):
        href = _clean_url(match.group("href"), base_url=page_url)
        label = _clean_text(match.group("label")) or ""
        if not href or href in seen:
            continue
        combined = f"{href} {label}".lower()
        if any(
            blocked in combined
            for blocked in ("login", "signup", "signin", "register", "account", "privacy", "terms")
        ):
            continue
        score = 0
        if any(hint in href.lower() for hint in EVENT_LINK_HINTS):
            score += 3
        if any(hint in label.lower() for hint in EVENT_TEXT_HINTS):
            score += 2
        if score <= 0:
            continue
        seen.add(href)
        ranked.append((score, href))
    ranked.sort(key=lambda item: (-item[0], item[1]))
    return [href for _, href in ranked[:10]]


def _google_calendar_ics_url(source_url: str) -> str | None:
    if _adapter_key_for_url(source_url) != "google_calendar":
        return None
    params = dict(parse_qsl(urlparse(source_url).query))
    calendar_id = params.get("src")
    if not calendar_id:
        return None
    return f"https://calendar.google.com/calendar/ical/{unquote(calendar_id)}/public/basic.ics"


def _unfold_ical_lines(feed_text: str) -> list[str]:
    lines: list[str] = []
    for raw_line in feed_text.replace("\r\n", "\n").split("\n"):
        if raw_line.startswith((" ", "\t")) and lines:
            lines[-1] += raw_line[1:]
        else:
            lines.append(raw_line)
    return lines


def _parse_ical_datetime(value: str, *, tzid: str | None = None) -> str | None:
    raw = value.strip()
    if not raw:
        return None
    try:
        if len(raw) == 8 and raw.isdigit():
            return date.fromisoformat(f"{raw[0:4]}-{raw[4:6]}-{raw[6:8]}").isoformat()
        if raw.endswith("Z"):
            return datetime.strptime(raw, "%Y%m%dT%H%M%SZ").replace(tzinfo=ZoneInfo("UTC")).isoformat()
        zone = ZoneInfo(tzid) if tzid else EVENT_TIMEZONE
        return datetime.strptime(raw, "%Y%m%dT%H%M%S").replace(tzinfo=zone).isoformat()
    except (ValueError, TypeError):
        return None


def _extract_first_url(value: str | None) -> str | None:
    if not value:
        return None
    match = re.search(r'https?://[^\s<>"\']+', value)
    if not match:
        return None
    return match.group(0).rstrip(").,")


def _extract_events_from_ical(feed_text: str, *, source_url: str) -> list[EventCandidate]:
    lines = _unfold_ical_lines(feed_text)
    calendar_timezone: str | None = None
    current: dict[str, tuple[str | None, str]] | None = None
    candidates: list[EventCandidate] = []

    for line in lines:
        if line.startswith("X-WR-TIMEZONE:"):
            calendar_timezone = line.split(":", 1)[1].strip() or None
            continue
        if line == "BEGIN:VEVENT":
            current = {}
            continue
        if line == "END:VEVENT":
            if current:
                title = _clean_text(current.get("SUMMARY", (None, ""))[1])
                if title:
                    description_raw = current.get("DESCRIPTION", (None, ""))[1]
                    location_raw = current.get("LOCATION", (None, ""))[1]
                    external_url = (
                        _clean_url(current.get("URL", (None, ""))[1], base_url=source_url)
                        or _clean_url(_extract_first_url(location_raw), base_url=source_url)
                        or _clean_url(_extract_first_url(description_raw), base_url=source_url)
                        or source_url
                    )
                    treat_location_as_link = _extract_first_url(location_raw) is not None
                    location_bits = _location_bits_from_text(_clean_text(location_raw))
                    dtstart_tzid, dtstart_raw = current.get("DTSTART", (calendar_timezone, ""))
                    dtend_tzid, dtend_raw = current.get("DTEND", (calendar_timezone, ""))
                    candidates.append(
                        EventCandidate(
                            title=title,
                            event_url=external_url,
                            source_url=source_url,
                            source_name=_source_name_for_url(source_url),
                            summary=_clean_text(description_raw),
                            start_at=_parse_ical_datetime(dtstart_raw, tzid=dtstart_tzid or calendar_timezone),
                            end_at=_parse_ical_datetime(dtend_raw, tzid=dtend_tzid or calendar_timezone),
                            venue=None if treat_location_as_link else location_bits["venue"],
                            address=None if treat_location_as_link else location_bits["address"],
                            city=None if treat_location_as_link else location_bits["city"],
                            country=None if treat_location_as_link else location_bits["country"],
                        )
                    )
            current = None
            continue
        if current is None or ":" not in line:
            continue
        key_part, raw_value = line.split(":", 1)
        key_name, *params = key_part.split(";")
        tzid = None
        for param in params:
            if param.startswith("TZID="):
                tzid = param.split("=", 1)[1]
                break
        current[key_name.upper()] = (tzid, raw_value.replace("\\n", "\n"))
    return candidates


def extract_events_from_feed(feed_text: str, *, source_url: str) -> list[EventCandidate]:
    if feed_text.lstrip().startswith("BEGIN:VCALENDAR"):
        return _extract_events_from_ical(feed_text, source_url=source_url)
    try:
        root = ElementTree.fromstring(feed_text)
    except ElementTree.ParseError:
        return []
    items = root.findall(".//item") or root.findall(".//{http://www.w3.org/2005/Atom}entry")
    candidates: list[EventCandidate] = []
    for item in items[:25]:
        title = _clean_text(
            item.findtext("title") or item.findtext("{http://www.w3.org/2005/Atom}title")
        )
        if not title:
            continue
        link = _clean_text(item.findtext("link"))
        if link is None:
            atom_link = item.find("{http://www.w3.org/2005/Atom}link")
            if atom_link is not None:
                link = atom_link.attrib.get("href")
        description = _clean_text(
            item.findtext("description")
            or item.findtext("summary")
            or item.findtext("{http://www.w3.org/2005/Atom}summary"),
        )
        event_url = _clean_url(link, base_url=source_url) or source_url
        candidates.append(
            EventCandidate(
                title=title,
                event_url=event_url,
                source_url=source_url,
                source_name=_source_name_for_url(source_url),
                summary=description,
            ),
        )
    return candidates


def _candidate_key(candidate: EventCandidate) -> str:
    if candidate.event_url:
        return f"url:{candidate.event_url}"
    title = (candidate.title or "").strip().lower()
    when = candidate.start_at or ""
    venue = (candidate.venue or "").strip().lower()
    return f"title:{title}|when:{when}|venue:{venue}"


def record_key_from_payload(*, title: str, data: dict[str, Any]) -> str:
    event_url = _clean_url(data.get("event_url") or data.get("eventUrl"))
    if event_url:
        return f"url:{event_url}"
    when = _clean_text(data.get("start_at") or data.get("startAt")) or ""
    venue = _clean_text(data.get("venue")) or ""
    return f"title:{title.strip().lower()}|when:{when}|venue:{venue.strip().lower()}"


async def _fetch_text(client: httpx.AsyncClient, url: str) -> tuple[str, str]:
    current_url = _google_calendar_ics_url(url) or url
    for _ in range(MAX_FETCH_REDIRECTS + 1):
        current_url = await _assert_safe_fetch_url(current_url)
        response = await client.get(current_url, follow_redirects=False)
        if response.is_redirect:
            location = response.headers.get("location")
            if not location:
                response.raise_for_status()
            next_url = _clean_url(location, base_url=str(response.request.url))
            if not next_url:
                raise UnsafeEventSourceError(
                    f"Unsafe redirect target while fetching event source: {location}"
                )
            current_url = next_url
            continue
        response.raise_for_status()
        content_type = response.headers.get("content-type", "").lower()
        return content_type, response.text
    raise httpx.TooManyRedirects("Too many redirects while fetching event source.")


async def _scan_one_source(
    client: httpx.AsyncClient,
    *,
    source_url: str,
    week_start: date,
    week_end: date,
) -> SourceScanResult:
    diagnostic = SourceDiagnostic(source_url=source_url, source_name=_source_name_for_url(source_url))
    try:
        content_type, body = await _fetch_text(client, source_url)
    except httpx.HTTPError:
        diagnostic.increment("network_error")
        return SourceScanResult(candidates=[], diagnostic=diagnostic)

    raw_candidates: list[EventCandidate] = []
    is_feed = (
        "xml" in content_type
        or "calendar" in content_type
        or body.lstrip().startswith(("<?xml", "<rss", "<feed", "BEGIN:VCALENDAR"))
    )
    if is_feed:
        raw_candidates.extend(extract_events_from_feed(body, source_url=source_url))
    else:
        raw_candidates.extend(
            _source_specific_candidates_from_html(body, source_url=source_url, page_url=source_url)
        )
        if _adapter_key_for_url(source_url) == "whatson_melbourne":
            raw_candidates.extend(_source_specific_link_candidates(body, source_url=source_url))
        if not raw_candidates:
            raw_candidates.extend(_source_specific_link_candidates(body, source_url=source_url))
        if not raw_candidates:
            linked_urls = _extract_links(body, page_url=source_url)
            raw_candidates.extend(
                [
                    EventCandidate(
                        title=_clean_text(urlparse(url).path.rsplit("/", 1)[-1].replace("-", " "))
                        or "Event",
                        event_url=url,
                        source_url=source_url,
                        source_name=_source_name_for_url(source_url),
                    )
                    for url in linked_urls
                ]
            )

    diagnostic.scanned_candidates = len(raw_candidates)
    if not raw_candidates:
        diagnostic.increment("parse_failed")
        return SourceScanResult(candidates=[], diagnostic=diagnostic)

    valid_candidates: list[EventCandidate] = []
    for candidate in raw_candidates:
        if not candidate.event_url or not _is_allowed_event_url(candidate.event_url, source_url=source_url):
            diagnostic.increment("off_source")
            continue
        valid_candidates.append(candidate)

    semaphore = asyncio.Semaphore(4)

    async def _resolve_candidate(item: EventCandidate) -> EventCandidate | None:
        async with semaphore:
            if is_feed and (item.start_at or item.end_at):
                if not _within_week(item, week_start=week_start, week_end=week_end):
                    diagnostic.skipped += 1
                    return None
            needs_enrichment = _candidate_needs_enrichment(item)
            resolved = (
                await _enrich_candidate_from_page(client, candidate=item)
                if needs_enrichment
                else item
            )
            if not resolved.start_at:
                diagnostic.increment("missing_date")
                return None
            if not _within_week(resolved, week_start=week_start, week_end=week_end):
                diagnostic.skipped += 1
                return None
            return resolved

    resolved_items = await asyncio.gather(
        *[_resolve_candidate(item) for item in valid_candidates],
        return_exceptions=True,
    )
    deduped: list[EventCandidate] = []
    seen: set[str] = set()
    for item in resolved_items:
        if isinstance(item, Exception):
            diagnostic.increment("network_error")
            continue
        if item is None:
            continue
        key = _candidate_key(item)
        if key in seen:
            diagnostic.skipped += 1
            continue
        seen.add(key)
        deduped.append(item)
    diagnostic.imported = len(deduped)
    return SourceScanResult(candidates=deduped, diagnostic=diagnostic)


async def scan_event_sources(
    *, sources: list[str], week_start_value: str | None
) -> EventScanResult:
    week_start = _coerce_week_start(week_start_value)
    week_end = week_start + timedelta(days=6)
    cleaned_sources = await validate_event_source_urls(sources)
    if not cleaned_sources:
        return EventScanResult(
            imported=0,
            skipped=0,
            week_start=week_start.isoformat(),
            week_end=week_end.isoformat(),
            events=[],
            diagnostics=[],
            skipped_reasons={},
        )
    async with httpx.AsyncClient(
        headers={"User-Agent": USER_AGENT},
        timeout=httpx.Timeout(20.0, connect=8.0),
    ) as client:
        nested = await asyncio.gather(
            *[
                _scan_one_source(
                    client,
                    source_url=source_url,
                    week_start=week_start,
                    week_end=week_end,
                )
                for source_url in cleaned_sources
            ],
            return_exceptions=True,
        )
    candidates: list[EventCandidate] = []
    diagnostics: list[SourceDiagnostic] = []
    skipped = 0
    skipped_reasons: dict[str, int] = {}
    for item in nested:
        if isinstance(item, Exception):
            skipped += 1
            continue
        candidates.extend(item.candidates)
        diagnostics.append(item.diagnostic)
        skipped += item.diagnostic.skipped
        for reason, count in item.diagnostic.failure_reasons.items():
            skipped_reasons[reason] = skipped_reasons.get(reason, 0) + count
    deduped: list[EventCandidate] = []
    seen: set[str] = set()
    for candidate in sorted(
        candidates, key=lambda item: (item.start_at or "9999", item.title.lower())
    ):
        key = _candidate_key(candidate)
        if key in seen:
            skipped += 1
            skipped_reasons["duplicates"] = skipped_reasons.get("duplicates", 0) + 1
            continue
        seen.add(key)
        deduped.append(candidate)
    return EventScanResult(
        imported=len(deduped),
        skipped=skipped,
        week_start=week_start.isoformat(),
        week_end=week_end.isoformat(),
        events=deduped,
        diagnostics=diagnostics,
        skipped_reasons=skipped_reasons,
    )


async def geocode_query(query: str) -> GeocodeResult | None:
    cleaned = _clean_text(query)
    if not cleaned:
        return None
    async with httpx.AsyncClient(
        headers={"User-Agent": USER_AGENT},
        timeout=httpx.Timeout(12.0, connect=5.0),
    ) as client:
        response = await client.get(
            "https://nominatim.openstreetmap.org/search",
            params={"format": "jsonv2", "limit": 1, "q": cleaned},
        )
        response.raise_for_status()
        payload = response.json()
    if not isinstance(payload, list) or not payload:
        return None
    first = payload[0]
    try:
        lat = float(first["lat"])
        lon = float(first["lon"])
    except (KeyError, TypeError, ValueError):
        return None
    return GeocodeResult(
        lat=lat,
        lon=lon,
        display_name=_clean_text(first.get("display_name")),
    )
