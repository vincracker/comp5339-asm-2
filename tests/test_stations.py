import pytest

from src.generation.stations import max_kw, site_type, to_station


@pytest.mark.parametrize("sa4, expected", [
    ("Sydney - Blacktown", "urban"),
    ("Central Coast", "urban"),
    ("Newcastle and Lake Macquarie", "urban"),
    ("Illawarra", "urban"),
    ("Riverina", "regional"),
    ("Hunter Valley exc Newcastle", "regional"),
    (None, "regional"),
])
def test_site_type(sa4, expected):
    assert site_type(sa4) == expected


def test_max_kw_uses_rating_when_present():
    assert max_kw(150.0, "150 kW") == 150.0


def test_max_kw_parses_highest_figure_from_mixed_text():
    assert max_kw(None, "2x350kW & 2x175kW") == 350.0


def test_max_kw_without_figure_raises():
    with pytest.raises(ValueError):
        max_kw(None, "unknown")


def test_to_station_falls_back_to_address_for_name():
    row = ("chr_x", None, "1 Bay St, Sydney, 2037", "Sydney - Inner West", 4, 50.0, "50 kW", -33.9, 151.2)
    st = to_station(row)
    assert st == {"station_id": "chr_x", "name": "1 Bay St, Sydney, 2037", "lat": -33.9, "lon": 151.2,
                  "site_type": "urban", "connectors": 4, "max_kw": 50.0}


def test_to_station_rejects_missing_plugs():
    with pytest.raises(ValueError):
        to_station(("chr_x", "n", "a", "Riverina", None, 50.0, "50 kW", -35.0, 147.0))
