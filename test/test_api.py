import requests

response_help = requests.get("http://127.0.0.1:8000/help")
response_data = requests.get("http://127.0.0.1:8000/data")
response_planets = requests.get("http://127.0.0.1:8000/planets")
response_jobs = requests.get("http://127.0.0.1:8000/jobs")
response_bad_planet = requests.get("http://127.0.0.1:8000/planets/BADPLANET")
response_bad_result = requests.get("http://127.0.0.1:8000/results/BADJOBID")

def test_help() -> None:
    """Test /help GET route returns a dictionary"""
    assert response_help.status_code == 200
    assert isinstance(response_help.json(), dict) == True

def test_get_data() -> None:
    """Test /data GET route returns a list of planet data"""
    assert response_data.status_code == 200
    assert isinstance(response_data.json(), list) == True

def test_planets() -> None:
    """Test /planets GET route returns a list of planet names"""
    assert response_planets.status_code == 200
    assert isinstance(response_planets.json(), list) == True

def test_jobs() -> None:
    """Test /jobs GET route returns a list of job IDs"""
    assert response_jobs.status_code == 200
    assert isinstance(response_jobs.json(), list) == True

def test_bad_planet_id() -> None:
    """Test if a bad planet name returns a 404 error"""
    assert response_bad_planet.status_code == 404
    assert isinstance(response_bad_planet.json(), dict) == True

def test_bad_job_id() -> None:
    """Test if a bad job id returns a 404 error"""
    assert response_bad_result.status_code == 404
    assert isinstance(response_bad_result.json(), dict) == True

def test_post_job() -> None:
    """Test /jobs POST route creates a new job"""
    response = requests.post(
        "http://127.0.0.1:8000/jobs",
        json={"plot_type": "discoveries_per_year"}
    )
    assert response.status_code == 200
    assert isinstance(response.json(), dict) == True

def test_job_id() -> None:
    """Test /jobs/{jobid} returns a job"""
    create_response = requests.post(
        "http://127.0.0.1:8000/jobs",
        json={"plot_type": "mass_distribution"}
    )
    jid = create_response.json()["job"]["jid"]

    response = requests.get(f"http://127.0.0.1:8000/jobs/{jid}")
    assert response.status_code == 200
    assert isinstance(response.json(), dict) == True

def test_bad_plot_type() -> None:
    """Test if a bad plot type returns a 400 error"""
    response = requests.post(
        "http://127.0.0.1:8000/jobs",
        json={"plot_type": "bad_plot"}
    )
    assert response.status_code == 400
    assert isinstance(response.json(), dict) == True