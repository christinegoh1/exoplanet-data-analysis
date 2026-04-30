import csv
import io
import json
import redis
import requests
import os
import logging

from fastapi.responses import StreamingResponse
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from jobs import add_job, get_job_by_id, get_job_ids, get_result

app = FastAPI()

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)

class Exoplanet(BaseModel):
    """
    Class for NASA exoplanet records

    Args:
        BaseModel: Pydantic model
    """
    planet_name: str
    hostname: str | None = None          # Optional bc dataset is sparse so not every cell has data in it
    num_stars: int | None = None
    num_planets: int | None = None
    disc_year: int | None = None
    orbital_period: float | None = None
    planet_rad: float | None = None
    planet_mass: float | None = None
    orb_eccentricity: float | None = None
    distance: float | None = None

def get_redis_client():
    """
    Create Redis client

    Returns:
        redis.Redis: Redis Client
    """
    redis_ip = os.environ.get("REDIS_IP", "redis-db")
    return redis.Redis(host=redis_ip, port=6379, db=0, decode_responses=True)

def parse_int(value):
    """
    Convert empty strings to None and valid values to integers

    Args:
        value: Value from NASA csv file

    Returns:
        int: Integer value or None
    """
    if value is None or value == "":
        return None
    return int(float(value))

def parse_float(value):
    """
    Convert empty strings to None and valid values to floats

    Args:
        value: Value from NASA csv

    Returns:
        float: Float value or None
    """
    if value is None or value == "":
        return None
    return float(value)

@app.post("/data")
def load_data() -> str:
    """
    Load NASA exoplanet data into Redis database

    Returns:
        str: Message indicating data loaded successfully
    """
    rd = get_redis_client()

    query = """
    select pl_name as planet_name,
           hostname,
           sy_snum as num_stars,
           sy_pnum as num_planets,
           disc_year,
           pl_orbper as orbital_period,
           pl_rade as planet_rad,
           pl_bmasse as planet_mass,
           pl_orbeccen as orb_eccentricity,
           sy_dist as distance
    from pscomppars
    """

    url = "https://exoplanetarchive.ipac.caltech.edu/TAP/sync"
    response = requests.get(
        url,
        params={"query": query, "format": "csv"},
        timeout=120
    )
    response.raise_for_status()

    data = csv.DictReader(io.StringIO(response.text))

    for planet in data:
        planet_model = Exoplanet(
            planet_name=planet["planet_name"],
            hostname=planet.get("hostname"),
            num_stars=parse_int(planet.get("num_stars")),
            num_planets=parse_int(planet.get("num_planets")),
            disc_year=parse_int(planet.get("disc_year")),
            orbital_period=parse_float(planet.get("orbital_period")),
            planet_rad=parse_float(planet.get("planet_rad")),
            planet_mass=parse_float(planet.get("planet_mass")),
            orb_eccentricity=parse_float(planet.get("orb_eccentricity")),
            distance=parse_float(planet.get("distance"))
        )

        rd.set(planet_model.planet_name, planet_model.model_dump_json())

    logger.info("Loaded NASA exoplanet data into Redis successfully")
    return "Loaded NASA exoplanet data into Redis successfully"

@app.get("/data")
def get_data() -> list[dict]:
    """
    Get all data out of Redis

    Returns:
        list: JSON list containing all planets
    """
    rd = get_redis_client()
    planets = []

    for key in rd.keys():
        planet_data = rd.get(key)
        if planet_data is not None:
            planets.append(json.loads(planet_data))

    logger.info("Returned all NASA exoplanet data")
    return planets

@app.delete("/data")
def delete_data() -> str:
    """
    Delete all data from Redis

    Returns:
        str: Message indicating deletion was successful
    """
    rd = get_redis_client()
    rd.flushdb()
    logger.warning("Deleted all NASA exoplanet data from Redis")
    return "Data deleted successfully"


@app.get("/planets")
def get_planets() -> list[str]:
    """
    Return a JSON-formatted list with the planet name for each planet

    Returns:
        list[str]: JSON list of planet names
    """
    rd = get_redis_client()
    logger.info("Returned list of planet names")
    return list(rd.keys())

@app.get("/planets/{planet_name}")
def get_planet(planet_name: str) -> dict:
    """
    Return all data associated with a given planet name

    Args:
        planet_name: Unique planet name for each exoplanet

    Returns:
        dict: All planet data associated with requested planet name
    """
    rd = get_redis_client()
    planet_data = rd.get(planet_name)

    if planet_data is None:
        logger.warning("Invalid planet name: %s", planet_name)
        raise HTTPException(status_code=404, detail="Planet name not found")

    return json.loads(planet_data)

@app.get("/planets/discovery-year/{year}")
def get_planets_by_year(year: int) -> list[dict]:
    """
    Return all planets discovered in a given year

    Args:
        year: Discovery year

    Returns:
        list[dict]: List of planets discovered in requested year
    """
    rd = get_redis_client()
    planets = []

    for key in rd.keys():
        planet_data = rd.get(key)
        if planet_data is not None:
            planet = json.loads(planet_data)
            if planet.get("disc_year") == year:
                planets.append(planet)

    logger.info("Returned planets discovered in year %s", year)
    return planets

@app.get("/planets/host/{hostname}")
def get_planets_by_host(hostname: str) -> list[dict]:
    """
    Return all planets associated with a given host star

    Args:
        hostname: Host star name

    Returns:
        list[dict]: List of planets around requested host star
    """
    rd = get_redis_client()
    planets = []

    for key in rd.keys():
        planet_data = rd.get(key)
        if planet_data is not None:
            planet = json.loads(planet_data)
            if planet.get("hostname") == hostname:
                planets.append(planet)

    logger.info("Returned planets for host star %s", hostname)
    return planets

@app.get("/planets/distance/{min_distance}/{max_distance}")
def get_planets_by_distance(min_distance: float, max_distance: float) -> list[dict]:
    """
    Return all planets within a distance range

    Args:
        min_distance: Minimum system distance
        max_distance: Maximum system distance

    Returns:
        list[dict]: List of planets in requested distance range
    """
    rd = get_redis_client()
    planets = []

    if min_distance < 0 or max_distance < 0 or min_distance > max_distance:
        raise HTTPException(
            status_code=400,
            detail="Require 0 <= min_distance <= max_distance"
        )

    for key in rd.keys():
        planet_data = rd.get(key)
        if planet_data is not None:
            planet = json.loads(planet_data)
            distance = planet.get("distance")

            if distance is not None and min_distance <= distance <= max_distance:
                planets.append(planet)

    logger.info("Returned planets within distance range")
    return planets

@app.post("/jobs")
def create_job(job: dict) -> dict:
    """
    Creates a new job

    Args:
        plot_type: string representing plot type
    
    Returns:
        dict: job message and information
    """
    if "plot_type" not in job:
        raise HTTPException(
            status_code=400,
            detail="Job must include 'plot_type'"
        )

    if not isinstance(job["plot_type"], str):
        raise HTTPException(
            status_code=400,
            detail="'plot_type' must be a string"
        )

    valid_plot_types = [
        "discoveries_per_year",
        "mass_distribution",
        "radius_vs_distance"
    ]

    if job["plot_type"] not in valid_plot_types:
        raise HTTPException(
            status_code=400,
            detail=f"'plot_type' must be one of {valid_plot_types}"
        )

    start = job.get("start")
    end = job.get("end")

    if start is not None and end is not None:
        if not isinstance(start, int) or not isinstance(end, int):
            raise HTTPException(
                status_code=400,
                detail="'start' and 'end' must be integers"
            )

        if start < 0 or end < 0 or start > end:
            raise HTTPException(
                status_code=400,
                detail="Require 0 <= start <= end"
            )

    new_job = add_job(job["plot_type"], start, end)
    logger.info("Created job %s", new_job.jid)

    return {
        "message": "Job created successfully",
        "job": new_job.model_dump(mode="json")
    }

@app.get("/jobs")
def get_jobs() -> list[str]:
    """
    Return a list of all job ids

    Returns: list of job ids
    """
    logger.info("Returned all job ids")
    return get_job_ids()

@app.get("/jobs/{jobid}")
def get_job(jobid: str) -> dict:
    """
    Returns job information for a provided job id

    Args:
        jobid: String of job id

    Returns:
        dict: Job information for job id
    """
    try:
        job = get_job_by_id(jobid)
        return job.model_dump(mode="json")
    except KeyError:
        logger.warning("Invalid job id: %s", jobid)
        raise HTTPException(status_code=404, detail="Job id not found")

@app.get("/results/{jobid}")
def get_job_result(jobid: str):
    """
    Returns result image for a provided job id

    Args:
        jobid: String of job id

    Returns:
        StreamingResponse: PNG image for job id
    """
    try:
        job = get_job_by_id(jobid)
    except KeyError:
        logger.warning("Invalid result job id: %s", jobid)
        raise HTTPException(status_code=404, detail="Job id not found")

    if job.status != "FINISHED -- SUCCESS":
        raise HTTPException(
            status_code=400,
            detail=f"Job not complete. Current status: {job.status}"
        )

    try:
        image_bytes = get_result(jobid)
    except KeyError:
        logger.error("Result not found for job %s", jobid)
        raise HTTPException(status_code=404, detail="Result not found")

    return StreamingResponse(io.BytesIO(image_bytes), media_type="image/png")

@app.get("/help")
def help() -> dict:
    """
    Return all API routes and descriptions
    """
    return {
        "routes": {
            "GET /help": "Return all API routes and descriptions",
            "POST /data": "Load NASA exoplanet data into Redis",
            "GET /data": "Return all exoplanet data",
            "DELETE /data": "Delete all exoplanet data",
            "GET /planets": "Return list of planet names",
            "GET /planets/{planet_name}": "Return one planet by planet name",
            "GET /planets/discovery-year/{year}": "Return planets discovered in a given year",
            "GET /planets/host/{hostname}": "Return planets orbiting a given host star",
            "GET /planets/distance/{min_distance}/{max_distance}": "Return planets within a distance range",
            "POST /jobs": "Create an analysis job",
            "GET /jobs": "Return all job ids",
            "GET /jobs/{jobid}": "Return job information",
            "GET /results/{jobid}": "Download job result image"
        }
    }