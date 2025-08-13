from starlette.applications import Starlette
from starlette.responses import JSONResponse
from starlette.requests import Request
from starlette.responses import Response
from starlette.routing import Route
from starlette.templating import Jinja2Templates

from celery import Celery
from celery.app.control import Inspect
from celery.signals import before_task_publish, worker_shutdown

import logging
import time

import random

TARGET_QUEUE_NAME = "one_by_one"

templates = Jinja2Templates(directory='app/templates')

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

async def homepage(request):
    if request.query_params.get("run_task"):
        task_id = background.apply_async(queue=TARGET_QUEUE_NAME, args=[1, 2, 3], kwargs={'param': {"key": "value"}})
        logging.info("Task id: %s", task_id)

    if worker := request.query_params.get("set_active"):
        logging.info(f"Setting as active: {worker}")
        controller.assign_queue(worker)


    res = controller.report()
    logger.warning(res)
    return templates.TemplateResponse(request, 'index.html', {'context': res})

async def active_queues(request: Request) -> Response:
    response = JSONResponse(inspection.active_queues())
    return response

# общая статистика по воркеру
async def stats(request: Request) -> Response:
    response = JSONResponse(inspection.stats())
    return response

# Получение выполняемых в текущий момент задач
async def active(request: Request) -> Response:
    response = JSONResponse(inspection.active())
    return response




app = Starlette(debug=True, routes=[
    Route('/active_queues', active_queues),
    Route('/', homepage),
    Route('/stats', stats),
    Route('/report', report),
    Route('/scheduled', scheduled),
    Route('/reserved', reserved),
    Route('/registered', registered),
    Route('/active', active)

])

celery_app = Celery()
celery_app.config_from_object({
    "broker_url": 'sentinel://sentinel-host-1.local:26380/1;sentinel://sentinel-host-2.local:26381/1;sentinel://sentinel-host-3.local:26382/1',
    "result_backend": 'sentinel://sentinel-host-1.local:26380/2;sentinel://sentinel-host-2.local:26381/2;sentinel://sentinel-host-3.local:26382/2',
    "broker_transport_options": {'master_name': "celery"},
    "result_backend_transport_options": {'master_name': "celery"},
    "worker_prefetch_multiplier": 1,
})

class CeleryWorkersController:
    def __init__(self, celery_app: Celery):
        self.online_workers = set()
        self.offline_workers = set()
        self.active_workers = set()
        self.inspection_object = Inspect(app=celery_app)
        self.control_object = celery_app.control

        self.update_worker_status()

    def update_worker_status(self):
        result = self.inspection_object.active_queues()
        if result:
            _online_workers = set()
            self.active_workers.clear()
            for _worker, _queues in result.items():
                _online_workers.add(_worker)
                for _queue in _queues:
                    if _queue.get("name") != TARGET_QUEUE_NAME:
                        continue

                    self.active_workers.add(_worker)

            _offline_workers = self.online_workers.difference(_online_workers)
            self.online_workers = _online_workers
            _resumed_workers = self.offline_workers.intersection(_online_workers)
            self.offline_workers.difference_update(_online_workers)
            self.offline_workers.update(_offline_workers)
            logger.warning(f"Онлайн: {_online_workers}, оффлайн: {_offline_workers}")
            if len(_resumed_workers):
                logger.warning(f"Вернулись в работу: {_resumed_workers}")

            # self.active_workers = self.active_workers.difference(self.offline_workers)

            match len(self.active_workers):
                case 0:
                    logger.warning("Нет назначенных воркеров.")
                case 1:
                    pass
                case _:
                    logger.warning("Количество воркеров больше 1.")

        else:
            logger.error("Can not receive queues.")
            self.offline_workers.update(self.online_workers)
            self.online_workers.clear()
            self.active_workers.clear()

    def deassign_queue(self) -> None:
        self.active_workers.clear()
        self.control_object.cancel_consumer(TARGET_QUEUE_NAME)

    def assign_queue(self, target_worker=None) -> bool:
        if not target_worker:
            target_worker = self.select_random_worker()
            if not target_worker:
                logger.error(f"Не удалось выбрать воркер")
                return
        else:
            if not target_worker in self.online_workers:
                logger.error(f"Невозможно назначить активным данный воркер: {target_worker}")
                return False

        print(target_worker)
        self.deassign_queue()
        self.active_workers.add(target_worker)
        logger.warning(f"Назначаем воркера: {self.active_workers}")
        self.control_object.add_consumer(TARGET_QUEUE_NAME, destination=list(self.active_workers))
        return True


    def select_random_worker(self):
        if not len(self.online_workers):
            return None
        return random.sample(list(self.online_workers), 1)[0]

    def run(self) -> bool:
        self.update_worker_status()
        if len(self.active_workers) and self.active_workers.issubset(self.online_workers):
            return True
        else:
            self.assign_queue()
            return True

    def report(self) -> list:
        res = []
        self.update_worker_status()
        for worker in self.online_workers.union(self.offline_workers):
            res.append({"name": worker, "is_active": True if worker in self.active_workers else False, "is_online": True if worker in self.online_workers else False})
        return sorted(res, key=lambda d: d['name'])



controller = CeleryWorkersController(celery_app)

inspection = Inspect(app=celery_app)

@celery_app.task()
def background(*args, **kwargs) -> str:
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