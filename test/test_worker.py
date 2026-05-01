import os
import sys
import json
import importlib

import jobs
import worker

os.environ["REDIS_IP"] = "127.0.0.1"

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

# Reload modules so they read the Redis IP
importlib.reload(jobs)
importlib.reload(worker)

def test_get_selected_planets():
    """Test selected planet range"""
    worker.rd.flushdb()

    worker.rd.set("Kepler-1 b", json.dumps({
        "planet_name": "Kepler-1 b",
        "disc_year": 2020,
        "planet_rad": 1.0,
        "planet_mass": 2.0,
        "distance": 10.0
    }))
    worker.rd.set("Kepler-2 b", json.dumps({
        "planet_name": "Kepler-2 b",
        "disc_year": 2021,
        "planet_rad": 2.0,
        "planet_mass": 4.0,
        "distance": 20.0
    }))

    result = worker.get_selected_planets(0, 1)

    assert len(result) == 2
    assert result[0]["planet_name"] == "Kepler-1 b"
    assert result[1]["planet_name"] == "Kepler-2 b"

    worker.rd.flushdb()

def test_discoveries_per_year_plot():
    """Test discoveries per year plot returns PNG bytes"""
    planets = [
        {"disc_year": 2020},
        {"disc_year": 2020},
        {"disc_year": 2021}
    ]

    result = worker.plot_discoveries_per_year(planets)

    assert isinstance(result, bytes) == True
    assert result[:8] == b"\x89PNG\r\n\x1a\n"

def test_mass_distribution_plot():
    """Test mass distribution plot returns PNG bytes"""
    planets = [
        {"planet_mass": 1.0},
        {"planet_mass": 2.0},
        {"planet_mass": 3.0}
    ]

    result = worker.plot_mass_distribution(planets)

    assert isinstance(result, bytes) == True
    assert result[:8] == b"\x89PNG\r\n\x1a\n"

def test_radius_vs_distance_plot():
    """Test radius vs distance plot returns PNG bytes"""
    planets = [
        {"planet_rad": 1.0, "distance": 10.0},
        {"planet_rad": 2.0, "distance": 20.0},
        {"planet_rad": 3.0, "distance": 30.0}
    ]

    result = worker.plot_radius_vs_distance(planets)

    assert isinstance(result, bytes) == True
    assert result[:8] == b"\x89PNG\r\n\x1a\n"

def test_analyze_planets():
    """Test analyze planets returns PNG bytes"""
    worker.rd.flushdb()

    worker.rd.set("Kepler-1 b", json.dumps({
        "planet_name": "Kepler-1 b",
        "disc_year": 2020,
        "planet_rad": 1.0,
        "planet_mass": 2.0,
        "distance": 10.0
    }))
    worker.rd.set("Kepler-2 b", json.dumps({
        "planet_name": "Kepler-2 b",
        "disc_year": 2021,
        "planet_rad": 2.0,
        "planet_mass": 4.0,
        "distance": 20.0
    }))

    result = worker.analyze_planets("discoveries_per_year", 0, 1)

    assert isinstance(result, bytes) == True
    assert result[:8] == b"\x89PNG\r\n\x1a\n"

    worker.rd.flushdb()