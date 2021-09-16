# MyPy for Static Typing
from typing import List, Set, Dict, Tuple, Optional, Any, Iterable

# Custom Modules
from helpers.config import config, schedulers
from helpers.logger import logger
from lib.jobs.Baseline import Baseline

# PyPi Modules
import datetime

class BaseJob:    
    def __init__(self):
        # Global Variables        
        self.runningJob: str = 'BaseJob'
        self.runtime: datetime.datetime = datetime.datetime.now()
        self.allJobs: Dict[Any, Any] = dict(
            EnergyBaseline = Baseline
        )

    def start(self, jobName: str) -> None:
        if jobName not in schedulers.keys(): raise Error(f'Cannot find "{jobName}" in {schedulers}, please make sure the naming is correct')
        self.runningJob = jobName
        try: 
            self.allJobs[jobName]()
        except Exception as error:
            logger.error(f'Failed at {self.runningJob} job')
            raise

class Error(Exception):
    pass