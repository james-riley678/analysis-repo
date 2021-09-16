from typing import List, Set, Dict, Tuple, Optional

import os
import sys
import datetime
import time
import json
import sys
import logging
import lib.utils.utils as utils
from lib.utils.InitialiseConfiguration import InitialiseConfiguration

from lib.jobs.BaseJob import BaseJob

from helpers.config import config, schedulers
from helpers.logger import logger

from apscheduler.schedulers.background import BackgroundScheduler
from importlib import import_module
import threading
import traceback


def jobExec(jobName: str) -> None:
    try:
        Job: BaseJob = BaseJob()
        Job.start(jobName)  # Starting Job
        logger.info("Job successfully completed, with no errors")
    except BaseException:
        logger.error("Job finished, however errors occurred within the job")
        returnError = traceback.format_exc()
        logger.error(returnError)
        print(returnError)


def runJob(jobName: str) -> None:
    try:
        # Do not want app waiting for job to finish
        threading.Thread(target=jobExec, args=[jobName]).start()
    except Exception as error:
        logger.error("Error with running thread {}: {}".format(jobName, error))


def initialiseScheules(scheduler) -> BackgroundScheduler:
    # Looping through each job from config file and creating new trigger
    for job in schedulers.keys():
        if schedulers[job]["jobEnabled"]:
            jobTriggers: List[dict] = schedulers[job]["triggers"]
            for number, jobTrigger in enumerate(jobTriggers, start=1):
                scheduler.add_job(
                    runJob,
                    id=f"{job} {number}",
                    args=[job],
                    trigger="cron",
                    second=jobTrigger["second"],  # second (0-59)
                    minute=jobTrigger["minute"],  # minute (0-59)
                    hour=jobTrigger["hour"],  # hour (0-23)
                    day=jobTrigger["day"],  # day of the month (1-31)
                    month=jobTrigger["month"],  # month (1-12) # day of week (0-6 / mon-sun)
                    day_of_week=jobTrigger["dayOfWeek"]
                )
    return scheduler


class Error(Exception):
    pass


if __name__ == "__main__":
    logger.info('Starting...')
    args: list = sys.argv
    envVariables: os._Environ = os.environ
    pythonEnv: str = str(os.getenv("PYTHON_ENV"))
    pythonConfigDir: str = str(os.getenv("PYTHON_CONFIG_DIR"))
    pythonConfigDir = pythonConfigDir.rstrip('/')
    logger.info(
        f'Please go to {pythonConfigDir}/{pythonEnv}.json to see the logs for all jobs')
    if len(args) == 1:
        logger.info("Initialising BackgroundScheduler")

        Scheduler: BackgroundScheduler = BackgroundScheduler()
        Scheduler.start()  # Starting apscheduler
        Scheduler = initialiseScheules(Scheduler)

        logger.info("BackgroundScheduler running")
        logger.info(f'Schedules: {Scheduler.get_jobs()}')

        waitTime: int = 60
        while True:
            time.sleep(waitTime)
    else:
        logger.info("Test Run, {} job has been executed".format(args[1]))
        runJob(args[1])  # Test running
