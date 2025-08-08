from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route

from celery import Celery
from celery.app.control import Inspect
from celery.signals import before_task_publish, worker_shutdown

import logging
import time

import random

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

async def add_task(request: Request) -> Response:
    task_id = background.apply_async(queue="one_by_one")
    logging.info("Task id: %s", task_id)
    response = JSONResponse({"task_id": str(task_id)})
    return response

async def ping(request: Request) -> Response:
    response = JSONResponse(inspection.ping())
    return response

async def heartbeat(request: Request) -> Response:
    response = JSONResponse(inspection.heartbeat())
    return response

async def query_task(request: Request) -> Response:
    response = JSONResponse(inspection.query_task())
    return response

async def stats(request: Request) -> Response:
    response = JSONResponse(inspection.stats())
    return response

async def active_queues(request: Request) -> Response:
    response = JSONResponse(inspection.active_queues())
    return response

async def cancel_consumer(request: Request) -> Response:
    response = JSONResponse(celery_app.control.cancel_consumer("one_by_one"))
    return response





app = Starlette(debug=True, routes=[
    Route('/add_task', add_task),
    Route('/ping', ping),
    Route('/heartbeat', heartbeat),
    Route('/stats', stats),
    Route('/query_task', query_task),
    Route('/cancel_consumer', cancel_consumer),
    Route('/active_queues', active_queues)
])

celery_app = Celery()
celery_app.config_from_object({
    "broker_url": 'amqp://user:password@rabbitmq:5672/myhost',
    "worker_prefetch_multiplier": 1,
})

class CeleryWorkersController:
    def __init__(self, celery_app: Celery):
        self.active_workers = set()
        self.inactive_workers = set()
        self.current_worker = None
        self.inspection_object = Inspect(app=celery_app)
        self.control_object = celery_app.control

        self.update_worker_status()

    def update_worker_status(self):
        ping_result = self.inspection_object.ping()
        if ping_result:
            _active_workers = set(ping_result.keys())
            _inactive_workers = self.active_workers.difference(_active_workers)
            self.active_workers = _active_workers
            _resumed_workers = self.inactive_workers.intersection(_active_workers)
            self.inactive_workers.difference_update(_active_workers)

            self.inactive_workers.update(_inactive_workers)
            logger.debug(f"Неактивные: {self.inactive_workers}, Активные: {self.active_workers}")
            if len(_resumed_workers):
                logger.info(f"Вернулись в работу: {_resumed_workers}")
        else:
            logger.error("ERROR PING ")

    def deassign_queue(self) -> None:
        self.current_worker = None
        self.control_object.cancel_consumer("one_by_one")

    def assign_queue(self) -> bool:
        if self.current_worker:
            logger.warning("Worker already assigned")
            return False
        self.current_worker = self.select_worker()
        logger.info(self.current_worker)
        if not self.current_worker:
            logger.critical("Can not assign worker - no available workers")
            return False
        self.control_object.add_consumer("one_by_one", destination=[self.current_worker])
        return True

    def select_worker(self):
        if not len(self.active_workers):
            return None
        return random.sample(list(self.active_workers), 1)[0]

    def run(self) -> bool:
        self.update_worker_status()
        if self.current_worker and self.current_worker in self.active_workers:
            return True
        else:
            self.deassign_queue()
            if not self.assign_queue():
                return False
            return True




controller = CeleryWorkersController(celery_app)

inspection = Inspect(app=celery_app)

@celery_app.task()
def background() -> str:
    print("start...")
    time.sleep(10)
    return "Hello from background task..."



@before_task_publish.connect
def task_publish_handler(*args, **kwargs):
    controller.run()

@worker_shutdown.connect
def re_elect_worker(*args, **kwargs):
    print("WORKER IS DEAD")
    controller.run()