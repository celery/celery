from datetime import datetime, timedelta, timezone

import pytest

from celery.exceptions import CPendingDeprecationWarning
from celery.utils.iso8601 import parse_iso8601


def test_parse_iso8601_utc():
    dt = parse_iso8601("2023-10-26T10:30:00Z")
    assert dt == datetime(2023, 10, 26, 10, 30, 0, tzinfo=timezone.utc)


def test_parse_iso8601_positive_offset():
    dt = parse_iso8601("2023-10-26T10:30:00+05:30")
    expected_tz = timezone(timedelta(hours=5, minutes=30))
    assert dt == datetime(2023, 10, 26, 10, 30, 0, tzinfo=expected_tz)


def test_parse_iso8601_negative_offset():
    dt = parse_iso8601("2023-10-26T10:30:00-08:00")
    expected_tz = timezone(timedelta(hours=-8))
    assert dt == datetime(2023, 10, 26, 10, 30, 0, tzinfo=expected_tz)


def test_parse_iso8601_with_microseconds():
    dt = parse_iso8601("2023-10-26T10:30:00.123456Z")
    assert dt == datetime(2023, 10, 26, 10, 30, 0, 123456, tzinfo=timezone.utc)


def test_parse_iso8601_date_only():
    dt = parse_iso8601("2023-10-26")
    assert dt == datetime(2023, 10, 26, 0, 0, 0)  # Expects naive datetime


def test_parse_iso8601_date_hour_minute_only():
    # The regex uses '.' as a separator, often 'T' is used.
    # Let's test with 'T' as it's common in ISO8601.
    dt = parse_iso8601("2023-10-26T10:30")
    assert dt == datetime(2023, 10, 26, 10, 30, 0)  # Expects naive datetime


def test_parse_iso8601_invalid_string():
    with pytest.raises(ValueError, match="unable to parse date string"):
        parse_iso8601("invalid-date-string")


def test_parse_iso8601_malformed_strings():
    # These strings match the regex but have invalid date/time component values
    invalid_component_strings = [
        "2023-13-01T00:00:00Z",  # Invalid month
        "2023-12-32T00:00:00Z",  # Invalid day
        "2023-12-01T25:00:00Z",  # Invalid hour
        "2023-12-01T00:60:00Z",  # Invalid minute
        "2023-12-01T00:00:60Z",  # Invalid second
    ]
    for s in invalid_component_strings:
        # For these, the error comes from datetime constructor
        with pytest.raises(ValueError):
            parse_iso8601(s)

    # This string has a timezone format that is ignored by the parser, resulting in a naive datetime
    ignored_tz_string = "2023-10-26T10:30:00+05:AA"
    dt_ignored_tz = parse_iso8601(ignored_tz_string)
    assert dt_ignored_tz == datetime(2023, 10, 26, 10, 30, 0)
    assert dt_ignored_tz.tzinfo is None

    # This string does not match the main ISO8601_REGEX pattern correctly, leading to None groups
    unparseable_string = "20231026T103000Z"
    with pytest.raises(TypeError):  # Expects TypeError due to int(None)
        parse_iso8601(unparseable_string)


def test_parse_iso8601_deprecation_warning():
    with pytest.warns(CPendingDeprecationWarning, match="parse_iso8601 is scheduled for deprecation"):
        parse_iso8601("2023-10-26T10:30:00Z")
