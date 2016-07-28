from .conf import app, DEFAULT_QUEUE, lock_redis

@app.task
def concatenate(x, y):
    return x + y

@app.task
def echo(msg):
    return msg

@app.task
def ping():
    echo.delay('PING')


