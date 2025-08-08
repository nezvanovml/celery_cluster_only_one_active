from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.routing import Route

from celery import Celery
from celery.result import AsyncResult
from celery.app.control import Inspect
from starlette.requests import Request
from starlette.responses import Response
import logging
import time







async def add_task(request: Request) -> Response:
    task_id = background.apply_async(queue="one_by_one")
    logging.info("Task id: %s", task_id)
    response = JSONResponse({"task_id": str(task_id)})
    return response

async def ping(request: Request) -> Response:
    response = JSONResponse(celery_app.control.ping())
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


async def cancel_consumer(request: Request) -> Response:
    response = JSONResponse(celery_app.control.cancel_consumer("one_by_one"))
    return response





app = Starlette(debug=True, routes=[
    Route('/add_task', add_task),
    Route('/ping', ping),
    Route('/heartbeat', heartbeat),
    Route('/stats', stats),
    Route('/query_task', query_task),
    Route('/cancel_consumer', cancel_consumer)
])

celery_app = Celery()
celery_app.config_from_object({
    "broker_url": 'amqp://user:password@rabbitmq:5672/myhost',
    "worker_prefetch_multiplier": 1,
    # "task_acks_late":
    # "broker_transport_options": {"confirm_publish": True},
    # "task_default_queue_type": 'quorum'
})

inspection = Inspect(app=celery_app)

@celery_app.task()
def background() -> str:
    print("start...")
    time.sleep(10)
    return "Hello from background task..."