# ruff: noqa: INP001
"""Regression tests for control-center event scanning helpers and routes."""

from __future__ import annotations

from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest

from app.api import control_center as cc
from app.schemas.control_center import EventScanRequest
from app.services import event_scanning


def test_extract_events_from_html_reads_json_ld_event() -> None:
    html = """
    <html>
      <body>
        <script type="application/ld+json">
          {
            "@context": "https://schema.org",
            "@type": "Event",
            "name": "Melbourne Builder Meetup",
            "startDate": "2026-03-23T18:30:00+11:00",
            "endDate": "2026-03-23T21:00:00+11:00",
            "url": "https://example.com/events/melbourne-builder-meetup",
            "description": "A meetup for local founders and builders.",
            "location": {
              "@type": "Place",
              "name": "Startup House",
              "address": {
                "@type": "PostalAddress",
                "streetAddress": "123 Collins St",
                "addressLocality": "Melbourne",
                "addressCountry": "Australia"
              }
            },
            "offers": {
              "@type": "Offer",
              "price": "0",
              "priceCurrency": "AUD"
            }
          }
        </script>
      </body>
    </html>
    """

    events = event_scanning.extract_events_from_html(
        html,
        source_url="https://example.com/events",
        page_url="https://example.com/events",
    )

    assert len(events) == 1
    assert events[0].title == "Melbourne Builder Meetup"
    assert events[0].venue == "Startup House"
    assert events[0].city == "Melbourne"
    assert events[0].country == "Australia"
    assert events[0].is_free is True
    assert events[0].event_url == "https://example.com/events/melbourne-builder-meetup"


def test_extract_events_from_html_resolves_schema_refs_and_subtypes() -> None:
    html = """
    <html>
      <head>
        <script type="application/ld+json">
          [
            {
              "@context": "https://schema.org",
              "@type": "Place",
              "@id": "https://example.com/venues/startup-house",
              "name": "Startup House",
              "address": {
                "@type": "PostalAddress",
                "streetAddress": "123 Collins St",
                "addressLocality": "Melbourne",
                "addressCountry": "Australia"
              }
            },
            {
              "@context": "https://schema.org",
              "@type": "Offer",
              "@id": "offer-1",
              "price": "35.00",
              "priceCurrency": "AUD"
            },
            {
              "@context": "https://schema.org",
              "@type": "Festival",
              "name": "Melbourne Makers Festival",
              "url": "https://example.com/events/melbourne-makers-festival",
              "startDate": "2026-03-21T10:00:00+11:00",
              "endDate": "2026-03-21T17:00:00+11:00",
              "image": "https://cdn.example.com/events/festival.png",
              "location": { "@id": "https://example.com/venues/startup-house" },
              "offers": [{ "@id": "offer-1" }]
            }
          ]
        </script>
      </head>
    </html>
    """

    events = event_scanning.extract_events_from_html(
        html,
        source_url="https://example.com/feed.xml",
        page_url="https://example.com/events/melbourne-makers-festival",
    )

    assert len(events) == 1
    assert events[0].title == "Melbourne Makers Festival"
    assert events[0].venue == "Startup House"
    assert events[0].city == "Melbourne"
    assert events[0].price == "35.00"
    assert events[0].currency == "AUD"
    assert events[0].image_url == "https://cdn.example.com/events/festival.png"


def test_extract_events_from_html_reads_richer_schema_fields() -> None:
    html = """
    <html>
      <body>
        <script type="application/ld+json">
          {
            "@context": "https://schema.org",
            "@type": ["Event", "BusinessEvent"],
            "name": "Operators Breakfast",
            "startDate": "2026-03-27T08:00:00+11:00",
            "eventStatus": "https://schema.org/EventScheduled",
            "eventAttendanceMode": "https://schema.org/MixedEventAttendanceMode",
            "maximumAttendeeCapacity": 180,
            "aggregateRating": {
              "@type": "AggregateRating",
              "reviewCount": 42
            },
            "offers": {
              "@type": "Offer",
              "price": "25",
              "priceCurrency": "AUD",
              "url": "https://tickets.example.com/operators-breakfast"
            }
          }
        </script>
      </body>
    </html>
    """

    events = event_scanning.extract_events_from_html(
        html,
        source_url="https://example.com/events",
        page_url="https://example.com/events/operators-breakfast",
    )

    assert len(events) == 1
    assert events[0].event_type == "BusinessEvent"
    assert events[0].status == "scheduled"
    assert events[0].online_or_hybrid == "hybrid"
    assert events[0].attendee_count == 180
    assert events[0].review_count == 42
    assert events[0].ticket_url == "https://tickets.example.com/operators-breakfast"


def test_clean_url_extracts_first_url_from_serialized_image_list() -> None:
    raw = (
        "['https:/cdn.example.com/content/uploads/hero-1920x1080.jpg', "
        "'https:/cdn.example.com/content/uploads/hero-1440x1440.jpg']"
    )

    cleaned = event_scanning._clean_url(raw, base_url="https://example.com")

    assert cleaned == "https://cdn.example.com/content/uploads/hero-1920x1080.jpg"


def test_coerce_week_start_normalizes_to_monday() -> None:
    normalized = event_scanning._coerce_week_start("2026-03-26")

    assert normalized.isoformat() == "2026-03-23"


def test_within_week_rejects_events_without_start_date() -> None:
    candidate = event_scanning.EventCandidate(
        title="Date TBC mixer",
        event_url="https://example.com/events/date-tbc",
        source_url="https://example.com/events",
        source_name="example.com",
        start_at=None,
    )

    assert (
        event_scanning._within_week(
            candidate,
            week_start=event_scanning.date(2026, 3, 23),
            week_end=event_scanning.date(2026, 3, 29),
        )
        is False
    )


def test_within_week_includes_ongoing_events_that_overlap_selected_week() -> None:
    candidate = event_scanning.EventCandidate(
        title="Festival Week",
        event_url="https://example.com/events/festival-week",
        source_url="https://example.com/events",
        source_name="example.com",
        start_at="2026-03-19",
        end_at="2026-03-23",
    )

    assert (
        event_scanning._within_week(
            candidate,
            week_start=event_scanning.date(2026, 3, 23),
            week_end=event_scanning.date(2026, 3, 29),
        )
        is True
    )


def test_within_week_includes_multi_day_events_spanning_entire_window() -> None:
    candidate = event_scanning.EventCandidate(
        title="Long Festival",
        event_url="https://example.com/events/long-festival",
        source_url="https://example.com/events",
        source_name="example.com",
        start_at="2026-03-21",
        end_at="2026-04-02",
    )

    assert (
        event_scanning._within_week(
            candidate,
            week_start=event_scanning.date(2026, 3, 23),
            week_end=event_scanning.date(2026, 3, 29),
        )
        is True
    )


def test_eventfinda_adapter_extracts_cards() -> None:
    html = """
    <div class="d-flex align-items-stretch col-12 col-md-6 col-xl-4">
      <a href="/2026/best-in-class/wellington">
        <img src="https://cdn.eventfinda.co.nz/best-in-class.jpg" />
      </a>
      <div class="card-body">
        <h2 class="card-title p-summary p-name"><a href="/2026/best-in-class/wellington" class="url summary">Best In Class</a></h2>
        <p class="card-text meta-location location vcard p-location h-adr"><span class="p-locality">The Fringe Bar,&nbsp;Wellington</span></p>
        <p class="card-text meta-date">Fri 27 Mar 2026 7:00pm</p>
      </div>
    </div>
    """

    events = event_scanning._source_specific_candidates_from_html(
        html,
        source_url="https://www.eventfinda.co.nz/whatson/events/wellington",
        page_url="https://www.eventfinda.co.nz/whatson/events/wellington",
    )

    assert len(events) == 1
    assert events[0].title == "Best In Class"
    assert events[0].venue == "The Fringe Bar"
    assert events[0].city == "Wellington"
    assert events[0].image_url == "https://cdn.eventfinda.co.nz/best-in-class.jpg"


def test_wellingtonnz_adapter_extracts_featured_cards() -> None:
    html = """
    <a href="/visit/events/faultline-ultra" class="featured-item featured-item--event highlighted-item">
      <article>
        <img src="https://wellingtonnz.bynder.com/faultline.jpg" />
        <h2 class="featured-item__title">Faultline Ultra</h2>
        <p class="featured-item__summary--hidden featured-item__summary">
          <span class="featured-item__text featured-item__text--date">25 April 2026</span>
          <span class="featured-item__text featured-item__text--info">Wellington Region</span>
        </p>
      </article>
    </a>
    """

    events = event_scanning._source_specific_candidates_from_html(
        html,
        source_url="https://www.wellingtonnz.com/visit/events",
        page_url="https://www.wellingtonnz.com/visit/events",
    )

    assert len(events) == 1
    assert events[0].title == "Faultline Ultra"
    assert events[0].venue == "Wellington Region"
    assert events[0].country == "New Zealand"
    assert events[0].image_url == "https://wellingtonnz.bynder.com/faultline.jpg"


def test_humanitix_adapter_extracts_next_data_events() -> None:
    html = """
    <script id="__NEXT_DATA__" type="application/json">
      {"props":{"pageProps":{"featuredCarouselEntries":{"verticalEvents":[
        {
          "hostname":"https://events.humanitix.com/",
          "slug":"exec-marketing-meetup-2026",
          "name":"Executive Marketing Meetup 2026",
          "date":{"startDate":"Thu Mar 26 2026 05:00:00 GMT+0000 (Coordinated Universal Time)","endDate":"Thu Mar 26 2026 08:00:00 GMT+0000 (Coordinated Universal Time)"},
          "bannerImage":{"handle":"mU98AZVRPyhgvVwImF3g"},
          "eventLocation":{
            "address":"Level 2/293-297 Lygon St, Carlton VIC 3053, Australia",
            "venueName":"Johnny's Green Room",
            "addressComponents":[
              {"long_name":"Carlton","short_name":"Carlton","types":["locality","political"]},
              {"long_name":"Australia","short_name":"AU","types":["country","political"]}
            ]
          },
          "pricing":{"maximumPrice":50,"minimumPrice":50},
          "location":"AU",
          "organiser":{"name":"Creative Natives"}
        }
      ]}}}}
    </script>
    """

    events = event_scanning._source_specific_candidates_from_html(
        html,
        source_url="https://events.humanitix.com/",
        page_url="https://events.humanitix.com/",
    )

    assert len(events) == 1
    assert events[0].title == "Executive Marketing Meetup 2026"
    assert events[0].venue == "Johnny's Green Room"
    assert events[0].city == "Carlton"
    assert events[0].price == "AUD 50.00"
    assert events[0].organizer == "Creative Natives"


def test_extract_events_from_feed_does_not_use_publish_date_as_event_date() -> None:
    feed = """
    <rss><channel><item>
      <title>Sample Event</title>
      <link>https://example.com/events/sample</link>
      <description>From the feed.</description>
      <pubDate>Mon, 23 Mar 2026 08:00:00 GMT</pubDate>
    </item></channel></rss>
    """

    events = event_scanning.extract_events_from_feed(feed, source_url="https://example.com/feed.xml")

    assert len(events) == 1
    assert events[0].start_at is None


def test_normalize_datetime_supports_fuzzy_ranges_and_yearless_dates() -> None:
    assert event_scanning._normalize_datetime("25 – 26 April 2026") == "2026-04-25"
    assert event_scanning._normalize_datetime("Until 23 Mar") == f"{event_scanning.datetime.now(event_scanning.EVENT_TIMEZONE).year}-03-23"


def test_eventbrite_source_rejects_cross_region_urls() -> None:
    assert (
        event_scanning._is_allowed_event_url(
            "https://www.eventbrite.de/e/off-region-ticket",
            source_url="https://www.eventbrite.com.au/d/australia--melbourne/events/",
        )
        is False
    )
    assert (
        event_scanning._is_allowed_event_url(
            "https://www.eventbrite.com.au/e/melbourne-ticket",
            source_url="https://www.eventbrite.com.au/d/australia--melbourne/events/",
        )
        is True
    )


@pytest.mark.asyncio
async def test_validate_event_source_urls_rejects_unapproved_hosts() -> None:
    with pytest.raises(event_scanning.UnsafeEventSourceError):
        await event_scanning.validate_event_source_urls(["https://example.com/events"])


@pytest.mark.asyncio
async def test_validate_event_source_urls_rejects_private_resolution_for_supported_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_resolve(host: str):
        del host
        return {event_scanning.ipaddress.ip_address("127.0.0.1")}

    monkeypatch.setattr(event_scanning, "_resolve_ip_addresses", _fake_resolve)

    with pytest.raises(event_scanning.UnsafeEventSourceError):
        await event_scanning.validate_event_source_urls(
            ["https://www.eventbrite.com.au/d/australia--melbourne/events/"]
        )


@pytest.mark.asyncio
async def test_validate_event_source_urls_accepts_supported_public_hosts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_resolve(host: str):
        del host
        return {event_scanning.ipaddress.ip_address("93.184.216.34")}

    monkeypatch.setattr(event_scanning, "_resolve_ip_addresses", _fake_resolve)

    result = await event_scanning.validate_event_source_urls(
        ["https://www.meetup.com/find/?location=au--melbourne&source=EVENTS"]
    )

    assert result == ["https://www.meetup.com/find?location=au--melbourne"]


def test_whatson_melbourne_adapter_tracks_until_dates_as_end_dates() -> None:
    html = """
    <div data-track-index=&quot;1&quot; data-listing-type=&quot;event&quot; class="page-preview fill-height preview-type-list-square">
      <a class="main-link" href="/things-to-do/melbourne-women-in-film-festival">
        <img src="/images/mwff.jpg" />
      </a>
      <span class="from-to-date"><span class="from-to-date-prefix">Until </span><time datetime="2026-03-23">23 Mar</time></span>
      <h3 class="title">Melbourne Women in Film Festival</h3>
      <p class="summary">Festival week.</p>
    </div></div>
    """

    events = event_scanning._source_specific_candidates_from_html(
        html,
        source_url="https://whatson.melbourne.vic.gov.au/",
        page_url="https://whatson.melbourne.vic.gov.au/",
    )

    assert len(events) == 1
    assert events[0].start_at is None
    assert events[0].end_at == "2026-03-23"
    assert events[0].title == "Melbourne Women in Film Festival"


def test_whatson_melbourne_link_candidates_include_detail_pages_not_category_pages() -> None:
    html = """
    <a href="/things-to-do/melbourne-food-and-wine-festival">Melbourne Food and Wine Festival</a>
    <a href="/things-to-do/melbourne-international-comedy-festival">Melbourne Comedy Festival</a>
    <a href="/things-to-do/this-weekend">This weekend</a>
    <a href="/things-to-do/free">Free things to do</a>
    """

    events = event_scanning._source_specific_link_candidates(
        html,
        source_url="https://whatson.melbourne.vic.gov.au/",
    )

    assert [event.event_url for event in events] == [
        "https://whatson.melbourne.vic.gov.au/things-to-do/melbourne-food-and-wine-festival",
        "https://whatson.melbourne.vic.gov.au/things-to-do/melbourne-international-comedy-festival",
    ]


def test_eventbrite_adapter_extracts_local_cards() -> None:
    html = """
    <section class="discover-vertical-event-card">
      <a href="https://www.eventbrite.com.au/e/back-to-the-90s-melbourne-tickets-1979849481094?aff=ebdssbneighborhoodbrowse" class="event-card-link">
        <img src="https://img.evbuc.com/back-to-the-90s.jpg" />
      </a>
      <section class="event-card-details">
        <a href="https://www.eventbrite.com.au/e/back-to-the-90s-melbourne-tickets-1979849481094?aff=ebdssbneighborhoodbrowse" class="event-card-link">
          <h3>Back To The 90's - Melbourne</h3>
        </a>
        <p class="Typography_root__487rx Typography_body-md-bold__487rx">Sat, Mar 28, 7:30 PM</p>
        <p class="Typography_root__487rx Typography_body-md__487rx">233 Lonsdale St</p>
      </section>
    </section></div>
    """

    events = event_scanning._source_specific_candidates_from_html(
        html,
        source_url="https://www.eventbrite.com.au/d/australia--melbourne/events/",
        page_url="https://www.eventbrite.com.au/d/australia--melbourne/events/",
    )

    assert len(events) == 1
    assert events[0].title == "Back To The 90's - Melbourne"
    assert events[0].event_url.startswith("https://www.eventbrite.com.au/e/back-to-the-90s-melbourne")
    assert events[0].start_at is not None


def test_eventbrite_detail_page_extracts_richer_metadata() -> None:
    html = """
    <html>
      <head>
        <meta property="og:description" content="Deep-dive training day for operators." />
      </head>
      <body>
        <h1>Technology &amp; Digital Tools</h1>
        <script type="application/ld+json">
          {
            "@context": "https://schema.org",
            "@type": "EducationEvent",
            "name": "Technology & Digital Tools",
            "startDate": "2026-03-27T09:00:00+11:00",
            "location": {
              "@type": "Place",
              "name": "Melbourne Connect",
              "address": {
                "@type": "PostalAddress",
                "streetAddress": "700 Swanston St",
                "addressLocality": "Melbourne",
                "addressCountry": "Australia"
              }
            },
            "organizer": { "@type": "Organization", "name": "Future Labs" },
            "offers": {
              "@type": "Offer",
              "price": "49",
              "priceCurrency": "AUD",
              "url": "https://www.eventbrite.com.au/e/technology-digital-tools-tickets-123"
            }
          }
        </script>
        <div>By Future Labs</div>
        <section><h2>Location</h2><p>Melbourne Connect, 700 Swanston St, Melbourne</p></section>
        <section><h2>Agenda</h2><p>Hands-on sessions and Q&A.</p></section>
        <div>56 reviews</div>
        <button>Check availability</button>
      </body>
    </html>
    """

    events = event_scanning._source_specific_candidates_from_html(
        html,
        source_url="https://www.eventbrite.com.au/d/australia--melbourne/events/",
        page_url="https://www.eventbrite.com.au/e/technology-digital-tools-tickets-123",
    )

    assert len(events) == 1
    assert events[0].organizer == "Future Labs"
    assert events[0].ticket_url == "https://www.eventbrite.com.au/e/technology-digital-tools-tickets-123"
    assert events[0].status == "scheduled"
    assert events[0].review_count == 56
    assert events[0].event_type == "EducationEvent"
    assert events[0].venue == "Melbourne Connect"


def test_allevents_page_extracts_event_schema_items() -> None:
    html = """
    <html>
      <head>
        <script type="application/ld+json">
          {
            "@context":"https://schema.org",
            "@type":"Event",
            "name":"Opening Night Comedy Allstars Supershow",
            "startDate":"2026-03-28T20:00:00+11:00",
            "url":"https://allevents.in/melbourne/opening-night-comedy-allstars-supershow/10000123456789",
            "location":{"@type":"Place","name":"Comedy Republic","address":{"@type":"PostalAddress","addressLocality":"Melbourne","addressCountry":"Australia"}},
            "image":"https://cdn2.allevents.in/comedy.jpg"
          }
        </script>
      </head>
    </html>
    """

    events = event_scanning._source_specific_candidates_from_html(
        html,
        source_url="https://allevents.in/melbourne",
        page_url="https://allevents.in/melbourne",
    )

    assert len(events) == 1
    assert events[0].title == "Opening Night Comedy Allstars Supershow"
    assert events[0].city == "Melbourne"


def test_meetup_page_extracts_event_schema_items() -> None:
    html = """
    <html>
      <head>
        <script type="application/ld+json">
          {
            "@context":"https://schema.org",
            "@type":"Event",
            "name":"Melbourne AWS User Group #157 - March 2026",
            "startDate":"2026-03-25T18:00:00+11:00",
            "url":"https://www.meetup.com/aws-aus/events/313703852/",
            "location":{"@type":"Place","name":"AWS Melbourne","address":{"@type":"PostalAddress","addressLocality":"Melbourne","addressCountry":"Australia"}}
          }
        </script>
      </head>
    </html>
    """

    events = event_scanning._source_specific_candidates_from_html(
        html,
        source_url="https://www.meetup.com/find/?location=au--melbourne&source=EVENTS",
        page_url="https://www.meetup.com/find/?location=au--melbourne&source=EVENTS",
    )

    assert len(events) == 1
    assert events[0].title == "Melbourne AWS User Group #157 - March 2026"
    assert events[0].event_url == "https://www.meetup.com/aws-aus/events/313703852"


def test_meetup_detail_page_extracts_group_counts_and_login_state() -> None:
    html = """
    <html>
      <head>
        <meta property="og:description" content="Builders night with demos and networking." />
      </head>
      <body>
        <h1>Melbourne AI Builders Night</h1>
        <script type="application/ld+json">
          {
            "@context":"https://schema.org",
            "@type":"Event",
            "name":"Melbourne AI Builders Night",
            "startDate":"2026-03-26T18:30:00+11:00",
            "url":"https://www.meetup.com/ai-builders/events/313703852/",
            "location":{"@type":"VirtualLocation","name":"Online event"}
          }
        </script>
        <section><h2>Details</h2><p>Shipping stories, demos, and intros.</p></section>
        <div>Hosted by Melbourne AI Builders</div>
        <div>143 attendees</div>
        <div>21 reviews</div>
        <button>Join Meetup</button>
      </body>
    </html>
    """

    events = event_scanning._source_specific_candidates_from_html(
        html,
        source_url="https://www.meetup.com/find/?location=au--melbourne&source=EVENTS",
        page_url="https://www.meetup.com/ai-builders/events/313703852/",
    )

    assert len(events) == 1
    assert events[0].group_name == "Melbourne AI Builders"
    assert events[0].organizer == "Melbourne AI Builders"
    assert events[0].online_or_hybrid == "online"
    assert events[0].attendee_count == 143
    assert events[0].review_count == 21
    assert events[0].status == "requires_login"


def test_concreteplayground_link_candidates_extract_event_urls() -> None:
    html = """
    <a href="https://concreteplayground.com/melbourne/event/melbourne-food-and-wine-festival">Melbourne Food and Wine Festival</a>
    """

    events = event_scanning._source_specific_link_candidates(
        html,
        source_url="https://concreteplayground.com/melbourne/events",
    )

    assert len(events) == 1
    assert events[0].event_url == "https://concreteplayground.com/melbourne/event/melbourne-food-and-wine-festival"


def test_extract_events_from_google_calendar_ical() -> None:
    feed = """
BEGIN:VCALENDAR
X-WR-TIMEZONE:Australia/Melbourne
BEGIN:VEVENT
DTSTART:20260325T073000Z
DTEND:20260325T093000Z
DESCRIPTION:Join us for a builder meetup.\\nhttps://www.meetup.com/aws-aus/events/313703852/
LOCATION:https://www.meetup.com/aws-aus/events/313703852/
SUMMARY:Melbourne AWS User Group #157 - March 2026
END:VEVENT
END:VCALENDAR
    """.strip()

    events = event_scanning.extract_events_from_feed(
        feed,
        source_url="https://calendar.google.com/calendar/u/0/embed?src=test@group.calendar.google.com&ctz=Australia/Melbourne",
    )

    assert len(events) == 1
    assert events[0].title == "Melbourne AWS User Group #157 - March 2026"
    assert events[0].event_url == "https://www.meetup.com/aws-aus/events/313703852"
    assert events[0].start_at is not None


def test_google_calendar_embed_converts_to_public_ics_url() -> None:
    url = event_scanning._google_calendar_ics_url(
        "https://calendar.google.com/calendar/u/0/embed?src=741714b060754779a29f37566919b7921ec1133990e4c4021d013e72204f38f9%40group.calendar.google.com&ctz=Australia/Melbourne"
    )

    assert (
        url
        == "https://calendar.google.com/calendar/ical/741714b060754779a29f37566919b7921ec1133990e4c4021d013e72204f38f9@group.calendar.google.com/public/basic.ics"
    )


@pytest.mark.asyncio
async def test_fetch_text_rejects_redirect_to_private_supported_host(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_resolve(host: str):
        if host == "eventbrite.com.au":
            return {event_scanning.ipaddress.ip_address("93.184.216.34")}
        if host == "meetup.com":
            return {event_scanning.ipaddress.ip_address("127.0.0.1")}
        raise AssertionError(host)

    monkeypatch.setattr(event_scanning, "_resolve_ip_addresses", _fake_resolve)

    class _Client:
        async def get(self, url: str, *, follow_redirects: bool):
            assert follow_redirects is False
            return httpx.Response(
                302,
                headers={"location": "https://www.meetup.com/private-target"},
                request=httpx.Request("GET", url),
            )

    with pytest.raises(event_scanning.UnsafeEventSourceError):
        await event_scanning._fetch_text(
            _Client(), "https://www.eventbrite.com.au/d/australia--melbourne/events/"
        )


@pytest.mark.asyncio
async def test_scan_one_source_filters_feed_items_using_detail_page_date(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    feed = """
    <rss><channel><item>
      <title>Feed Event</title>
      <link>https://example.com/events/feed-event</link>
      <pubDate>Mon, 23 Mar 2026 08:00:00 GMT</pubDate>
    </item></channel></rss>
    """
    detail_html = """
    <script type="application/ld+json">
      {
        "@context": "https://schema.org",
        "@type": "Event",
        "name": "Feed Event",
        "url": "https://example.com/events/feed-event",
        "startDate": "2026-04-04T19:00:00+11:00"
      }
    </script>
    """

    async def _fake_fetch_text(client, url):
        del client
        if url.endswith("feed.xml"):
            return "application/rss+xml", feed
        return "text/html", detail_html

    monkeypatch.setattr(event_scanning, "_fetch_text", _fake_fetch_text)

    result = await event_scanning._scan_one_source(
        None,  # type: ignore[arg-type]
        source_url="https://example.com/feed.xml",
        week_start=event_scanning.date(2026, 3, 23),
        week_end=event_scanning.date(2026, 3, 29),
    )

    assert result.candidates == []
    assert result.diagnostic.scanned_candidates == 1
    assert result.diagnostic.imported == 0


@pytest.mark.asyncio
async def test_scan_one_source_reports_missing_dates(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    html = '<a href="https://example.com/events/date-tbc">Date TBC</a>'
    detail_html = """
    <html>
      <head><title>Date TBC</title></head>
      <body><p>No date yet.</p></body>
    </html>
    """

    async def _fake_fetch_text(client, url):
        del client
        if url == "https://example.com/events":
            return "text/html", html
        return "text/html", detail_html

    monkeypatch.setattr(event_scanning, "_fetch_text", _fake_fetch_text)

    result = await event_scanning._scan_one_source(
        None,  # type: ignore[arg-type]
        source_url="https://example.com/events",
        week_start=event_scanning.date(2026, 3, 23),
        week_end=event_scanning.date(2026, 3, 29),
    )

    assert result.candidates == []
    assert result.diagnostic.failure_reasons["missing_date"] == 1


@pytest.mark.asyncio
async def test_scan_events_week_creates_only_new_records(monkeypatch: pytest.MonkeyPatch) -> None:
    org_id = uuid4()
    existing_event_url = "https://example.com/events/already-known"
    existing_record = SimpleNamespace(
        title="Known Event",
        summary="",
        data={
            "event_url": existing_event_url,
            "start_at": "2026-03-24T19:00:00+11:00",
            "venue": "Town Hall",
        },
    )
    existing_record.id = uuid4()
    existing_record.data = {
        "event_url": existing_event_url,
        "start_at": "2026-03-24T19:00:00+11:00",
        "venue": "Town Hall",
    }
    existing_record.summary = ""
    existing_record.title = "Known Event"
    existing_record.updated_at = None
    existing_record.stage = "discovered"

    refreshed_candidate = event_scanning.EventCandidate(
        title="Known Event",
        event_url=existing_event_url,
        source_url="https://example.com/events",
        source_name="example.com",
        summary="Refreshed details from the live page.",
        start_at="2026-03-24T19:00:00+11:00",
        venue="Town Hall",
        image_url="https://cdn.example.com/known-event.png",
        attendee_count=88,
        ticket_url="https://example.com/tickets/known-event",
    )
    new_candidate = event_scanning.EventCandidate(
        title="Fresh Event",
        event_url="https://example.com/events/fresh",
        source_url="https://example.com/events",
        source_name="example.com",
        summary="A brand new event.",
        start_at="2026-03-25T19:00:00+11:00",
        venue="Laneway Club",
        group_name="Founder House",
        status="scheduled",
    )

    async def _fake_scan_event_sources(*, sources, week_start_value):
        del sources, week_start_value
        return event_scanning.EventScanResult(
            imported=2,
            skipped=0,
            week_start="2026-03-23",
            week_end="2026-03-29",
            events=[refreshed_candidate, new_candidate],
        )

    created_payloads: list[dict[str, object]] = []
    patched_payloads: list[dict[str, object]] = []

    async def _fake_create(session, model, **kwargs):
        del session, model
        created_payloads.append(kwargs)
        return SimpleNamespace(id=uuid4(), **kwargs)

    async def _fake_patch(session, target, payload):
        del session, target
        patched_payloads.append(payload)
        return existing_record

    class _ExecResult:
        def __iter__(self):
            yield existing_record

    class _Session:
        async def exec(self, statement):
            del statement
            return _ExecResult()

    monkeypatch.setattr(cc, "scan_event_sources", _fake_scan_event_sources)
    monkeypatch.setattr(cc.crud, "create", _fake_create)
    monkeypatch.setattr(cc.crud, "patch", _fake_patch)

    ctx = SimpleNamespace(organization=SimpleNamespace(id=org_id))
    response = await cc.scan_events_week(
        payload=EventScanRequest(
            module_id="events",
            module_slug="events",
            module_title="Events",
            sources=["https://example.com/events"],
            week_start="2026-03-23",
        ),
        session=_Session(),
        ctx=ctx,
    )

    assert response.imported == 1
    assert response.created == 1
    assert response.skipped_duplicates == 1
    assert len(created_payloads) == 1
    assert len(patched_payloads) == 1
    assert patched_payloads[0]["data"]["image_url"] == "https://cdn.example.com/known-event.png"
    assert patched_payloads[0]["data"]["attendee_count"] == 88
    assert created_payloads[0]["title"] == "Fresh Event"
    assert created_payloads[0]["module_id"] == "events"
    assert created_payloads[0]["data"]["group_name"] == "Founder House"


@pytest.mark.asyncio
async def test_scan_events_week_rejects_unsafe_sources() -> None:
    ctx = SimpleNamespace(organization=SimpleNamespace(id=uuid4()))

    class _Session:
        async def exec(self, statement):
            del statement
            raise AssertionError("session.exec should not run for rejected sources")

    with pytest.raises(cc.HTTPException) as exc_info:
        await cc.scan_events_week(
            payload=EventScanRequest(
                module_id="events",
                module_slug="events",
                module_title="Events",
                sources=["http://169.254.169.254/latest/meta-data/"],
                week_start="2026-03-23",
            ),
            session=_Session(),
            ctx=ctx,
        )

    assert exc_info.value.status_code == 400
    assert "Unsupported event source host" in str(exc_info.value.detail)


@pytest.mark.asyncio
async def test_geocode_events_location_returns_not_found_when_lookup_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def _fake_geocode_query(query: str):
        del query
        return None

    monkeypatch.setattr(cc, "geocode_query", _fake_geocode_query)

    ctx = SimpleNamespace(organization=SimpleNamespace(id=uuid4()))
    response = await cc.geocode_events_location(query="Melbourne", ctx=ctx)

    assert response.ok is False
    assert response.lat is None
    assert response.lon is None
