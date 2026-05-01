import json
import os
import logging
import io

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

from jobs import q, rd, save_result, start_job, update_job_status, get_job_by_id, JobStatus

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)

@q.worker
def do_work(jid: str) -> None:
    """
    Worker that processes exoplanet analytics provided a jobid

    Args:
        jobid: String of job ID
    """
    try:
        start_job(jid)

        job = get_job_by_id(jid)
        image_bytes = analyze_planets(job.plot_type, job.start, job.end)

        save_result(jid, image_bytes)
        update_job_status(jid, JobStatus.SUCCESS)
        logger.info("Completed job %s", jid)

    except Exception as e:
        logger.exception("Worker failed on job %s: %s", jid, e)
        update_job_status(jid, JobStatus.ERROR)


def get_selected_planets(start: int | None = None, end: int | None = None) -> list[dict]:
    """
    Get selected exoplanets from Redis

    Args:
        start: Starting planet index
        end: Ending planet index

    Returns:
        list[dict]: List of planet data
    """
    planet_ids = sorted(rd.keys())

    if not planet_ids:
        raise ValueError("No NASA exoplanet data loaded")

    if start is None:
        start = 0

    if end is None:
        end = len(planet_ids) - 1

    if start < 0 or end < 0 or start > end:
        raise ValueError("Require 0 <= start <= end")

    if start >= len(planet_ids) or end >= len(planet_ids):
        raise ValueError("Requested index range exceeded")

    selected_ids = planet_ids[start:end + 1]
    selected_planets = []

    for pid in selected_ids:
        planet_data = rd.get(pid)
        if planet_data is not None:
            selected_planets.append(json.loads(planet_data))

    return selected_planets


def figure_to_bytes(fig) -> bytes:
    """
    Convert matplotlib figure to PNG bytes

    Args:
        fig: Matplotlib figure

    Returns:
        bytes: PNG image bytes
    """
    buffer = io.BytesIO()
    fig.savefig(buffer, format="png", bbox_inches="tight")
    buffer.seek(0)
    image_bytes = buffer.read()
    plt.close(fig)
    return image_bytes


def plot_discoveries_per_year(planets: list[dict]) -> bytes:
    """
    Plot number of exoplanet discoveries per year

    Args:
        planets: List of planet data

    Returns:
        bytes: PNG image bytes
    """
    discoveries_per_year: dict[int, int] = {}

    for planet in planets:
        discovery_year = planet.get("disc_year")
        if discovery_year:
            year = int(discovery_year)
            discoveries_per_year[year] = discoveries_per_year.get(year, 0) + 1

    years = sorted(discoveries_per_year.keys())
    counts = [discoveries_per_year[year] for year in years]

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(years, counts)
    ax.set_xlabel("Discovery Year")
    ax.set_ylabel("Number of Planets")
    ax.set_title("Exoplanet Discoveries Per Year")
    ax.tick_params(axis="x", rotation=45)

    return figure_to_bytes(fig)


def plot_mass_distribution(planets: list[dict]) -> bytes:
    """
    Plot planet mass distribution

    Args:
        planets: List of planet data

    Returns:
        bytes: PNG image bytes
    """
    masses = []

    for planet in planets:
        mass = planet.get("planet_mass")
        if mass is not None and mass < 5000:
            masses.append(mass)

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.hist(masses, bins=50)
    ax.set_xlabel("Planet Mass (Earth Masses)")
    ax.set_ylabel("Count")
    ax.set_title("Planet Mass Distribution")

    return figure_to_bytes(fig)


def plot_radius_vs_distance(planets: list[dict]) -> bytes:
    """
    Plot planet radius against system distance

    Args:
        planets: List of planet data

    Returns:
        bytes: PNG image bytes
    """
    radii = []
    distances = []

    for planet in planets:
        radius = planet.get("planet_rad")
        distance = planet.get("distance")

        if radius is not None and distance is not None:
            if radius < 50 and distance < 10000:
                radii.append(radius)
                distances.append(distance)

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.scatter(distances, radii, alpha=0.5, s=10)
    ax.set_xlabel("Distance (parsecs)")
    ax.set_ylabel("Planet Radius (Earth Radii)")
    ax.set_title("Planet Radius vs Distance")

    return figure_to_bytes(fig)


def analyze_planets(plot_type: str, start: int | None = None, end: int | None = None) -> bytes:
    """
    Analyze NASA exoplanet data and generate a plot

    Args:
        plot_type: Type of plot to generate
        start: Starting planet index
        end: Ending planet index

    Returns:
        bytes: PNG image bytes
    """
    planets = get_selected_planets(start, end)

    if plot_type == "discoveries_per_year":
        return plot_discoveries_per_year(planets)

    if plot_type == "mass_distribution":
        return plot_mass_distribution(planets)

    if plot_type == "radius_vs_distance":
        return plot_radius_vs_distance(planets)

    raise ValueError("Unsupported plot type")


if __name__ == "__main__":
    do_work()