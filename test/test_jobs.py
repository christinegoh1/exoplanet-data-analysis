import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

from jobs import JobStatus, _instantiate_job

def test_instantiate_job():
    """Test that job object is created correctly"""
    job = _instantiate_job("job67", JobStatus.QUEUED, "discoveries_per_year", 1, 6)

    assert job.jid == "job67"
    assert job.status == JobStatus.QUEUED
    assert job.plot_type == "discoveries_per_year"
    assert job.start == 1
    assert job.end == 6
    assert job.start_time is None
    assert job.end_time is None