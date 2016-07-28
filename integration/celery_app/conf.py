from celery import Celery
from kombu import Queue, Exchange
import redis

from datetime import timedelta
from celery.schedules import crontab
from .base_task import BaseTask

def get_redis_info():
    return '127.0.0.1', 6379

def get_redis_str():
    host, port = get_redis_info()
    return 'redis://%s:%s/1' % (host, str(port))


def broker_redis():
    host, port = get_redis_info()
    return redis.StrictRedis(host=host, port=port, db=1)

def lock_redis():
    host, port = get_redis_info()
    return redis.StrictRedis(host=host, port=port, db=2)

DEFAULT_QUEUE = 'celery'

class CeleryConfig(object):
    BROKER_TRANSPORT_OPTIONS = {'visibility_timeout': 12 * 3600}
    CELERY_QUEUES = [
        Queue(DEFAULT_QUEUE, Exchange(DEFAULT_QUEUE), routing_key=DEFAULT_QUEUE),
    ]

    @property
    def BROKER_URL(self):
        return get_redis_str()

    @property
    def CELERY_RESULT_BACKEND(self):
        return self.BROKER_URL

    CELERY_TASK_RESULT_EXPIRES = timedelta(minutes=5)

    CELERYD_TASK_SOFT_TIME_LIMIT = 300
    CELERYD_TASK_TIME_LIMIT = 330

    CELERY_TASK_SERIALIZER = 'pickle'
    CELERY_ACCEPT_CONTENT = ['pickle', 'json']

    CELERY_SEND_TASK_SENT_EVENT = True
    CELERY_TIMEZONE = 'UTC'
    CELERYD_HIJACK_ROOT_LOGGER = False

    @property
    def CELERYBEAT_SCHEDULE(self):
        return {
            'ping': {
                'task': 'celery_app.simple.ping',
                'schedule': timedelta(seconds=10)
            }
        }

    CELERY_IMPORTS = (
        'celery_app.simple'
    )


app = Celery('celery_app', task_cls=BaseTask, config_source=CeleryConfig())
