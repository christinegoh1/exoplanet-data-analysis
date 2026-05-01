from datetime import datetime
import json
import uuid
import redis
import typing
import os
import logging
from hotqueue import HotQueue
from enum import Enum
from pydantic import BaseModel

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(level=LOG_LEVEL)
logger = logging.getLogger(__name__)

_redis_ip = os.environ.get("REDIS_IP", "redis-db")
_redis_port = 6379

rd = redis.Redis(host=_redis_ip, port=_redis_port, db=0)
q = HotQueue("queue", host=_redis_ip, port=_redis_port, db=1)
jdb = redis.Redis(host=_redis_ip, port=_redis_port, db=2)
rdb = redis.Redis(host=_redis_ip, port=_redis_port, db=3)


class JobStatus(str, Enum):
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    ERROR = "FINISHED -- ERROR"
    SUCCESS = "FINISHED -- SUCCESS"

class Job(BaseModel):
    jid: str
    status: JobStatus
    plot_type: str
    start: int | None = None
    end: int | None = None
    start_time: typing.Optional[datetime] = None
    end_time: typing.Optional[datetime] = None

def _generate_jid() -> str:
    """
    Generate a pseudo-random identifier for a job.
    """
    jid = str(uuid.uuid4())
    logger.debug("Generated job id %s", jid)
    return jid

def _instantiate_job(jid: str, status: JobStatus, plot_type: str, start: int | None = None, end: int | None = None) -> Job:
    """
    Create the job object description as a python dictionary. Requires the job id,
    status, plot_type, start and end parameters.
    """
    return Job(jid=jid, status=status, plot_type=plot_type, start=start, end=end)

def _save_job(jid: str, job: Job) -> bool:
    """Save a job object in the Redis database."""
    jdb.set(jid, json.dumps(job.model_dump(mode="json")))
    logger.info("Saved job %s", jid)
    return True

def _queue_job(jid: str) -> bool:
    """Add a job to the redis queue."""
    q.put(jid)
    logger.info("Queued job %s", jid)
    return True

def get_job_by_id(jid: str) -> Job:
    """Return job object given jid"""
    raw_data = jdb.get(jid)
    if raw_data is None:
        raise KeyError(f"Job {jid} not found")
    return Job(**json.loads(raw_data))

def get_job_ids() -> list[str]:
    """
    Return all existing job ids.
    """
    return list(jdb.keys())

def add_job(plot_type: str, start: int | None = None, end: int | None = None) -> Job:
    """Add a job to the redis database and queue."""
    jid = _generate_jid()
    job = _instantiate_job(jid, JobStatus.QUEUED, plot_type, start, end)
    _save_job(jid, job)
    _queue_job(jid)
    return job

def start_job(jid: str) -> bool:
    """Called by worker when starting a new job. Updates the job's status and start time."""
    job = get_job_by_id(jid)
    job.status = JobStatus.RUNNING
    job.start_time = datetime.now()
    logger.info("Started job %s", jid)
    return _save_job(jid=jid, job=job)

def update_job_status(jid: str, status: JobStatus) -> bool:
    """Update the status of job with job id `jid` to status `status`."""
    job = get_job_by_id(jid)
    if job:
        job.status = status
        if job.status == JobStatus.ERROR or job.status == JobStatus.SUCCESS:
            job.end_time = datetime.now()
        logger.info("Updated job %s to status %s", jid, status)
        return _save_job(jid, job)
    else:
        raise Exception()

def save_result(jid: str, image_bytes: bytes) -> bool:
    """
    Save results for a completed job in Redis.
    """
    rdb.set(jid, image_bytes)
    logger.info("Saved result for job %s", jid)
    return True

def get_result(jid: str) -> bytes:
    """
    Return result object given jid
    """
    raw_data = rdb.get(jid)
    if raw_data is None:
        raise KeyError(f"Result for job {jid} not found")
    return raw_data