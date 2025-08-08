@celery_app.task()
def background() -> str:
    time.sleep(5)
    return "Hello from background task..."