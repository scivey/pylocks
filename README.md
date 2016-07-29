# pylocks

redis-backed locks and leases, aimed mostly at celery tasks

## basic examples
```python
import redis
from pylocks.blocking import BlockingRedisLockFactory
from pylocks.errors import LockAlreadyHeld

person_lock_factory = BlockingRedisLockFactory(
    prefix='people',
    ttl=300,
    arity=1,
    root_prefix='example'
)

def main():
    lock = person_lock_factory.build(redis_conn=redis.StrictRedis(
        host='127.0.0.1', port=6379, db=2
    ))
    joe_lease = lock.acquire('joe')
    with joe_lease.releasing():
        last_err = None
        try:
            lock.acquire('joe')
        except LockAlreadyHeld as err:
            last_err = err
        assert isinstance(last_err, LockAlreadyHeld)
        last_err = None

        bill_lease = lock.acquire('bill')
        bill_lease.check_if_held()
        bill_lease.release()
        try:
            bill_lease.check_if_held()
        except LockNotHeld as err:
            last_err = err
        assert isinstance(last_err, LockNotHeld)

        assert lock.is_held('joe')
    assert not lock.is_held('joe')

if __name__ == '__main__':
    main()
```


## better examples


In this example, a celery task is used to increment a given redis key by a given amount.  We're going to pretend that Redis doesn't have a built-in atomic increment, and that this requires two separate network calls for the get and set.  There are better alternatives to locking for this particular case -- it's just an example.

In order to prevent workers from interfering with each other, we're going to protect each key with its own lock in Redis.

### naive approach

In the first approach, we try to acquire the lock once the task actually begins execution.  If the lock is already held, we just fail.


```python
import redis
from pylocks.blocking import BlockingRedisLockFactory
from pylocks.errors import LockNotOwned, LockAlreadyHeld
from some_app.workers import celery_app
import logging

logger = logging.getLogger(__name__)

def lock_redis():
    return redis.StrictRedis(host='127.0.0.1', port=6379, db=0)

def people_redis():
    return redis.StrictRedis(host='127.0.0.1', port=6379, db=1)

person_lock = BlockingRedisLockFactory(
    prefix='person',
    ttl=60,
    arity=1,
    root_prefix='locks_example'
)

@celery_app.task
def add_to_person(person_id, amount):
    try:
        handle = person_lock.acquire(person_id)
    except LockAlreadyHeld as err:
        logger.error("lock on person %s was already held", person_id, exc_info=True)
        raise
    with handle.releasing():
        r = people_redis()
        person_key = 'person:%s' % person_id
        val = r.get(person_key) or 0
        r.set(person_key, val + amount)

```


### lease-based approach


In the more sophisticated approach, we try to acquire the lock prior to enqueuing a task for that person.  If acquiring the lock fails, we don't enqueue a task.  Otherwise, we enqueue the task and *also pass it a lease ID*.

The task then uses this id to confirm that it still owns the lock before it does anything to the shared state.

Without a unique lease ID, the task would only be able to tell whether the lock was held by *anyone* at execution start.  By comparing lease IDs, it can detect the case where its own lease has expired and another process has acquired the lock.


```python
import redis
from pylocks.blocking import RedisLockFactory, SingleLockHandle
from pylocks.errors import LockNotOwned, LockAlreadyHeld
from some_app.workers import celery_app
import logging

logger = logging.getLogger(__name__)

def lock_redis():
    return redis.StrictRedis(host='127.0.0.1', port=6379, db=0)

def people_redis():
    return redis.StrictRedis(host='127.0.0.1', port=6379, db=1)

person_lock = RedisLockFactory(
    prefix='person',
    ttl=60,
    arity=1,
    root_prefix='locks_example'
)

@celery_app.task
def add_to_person(person_id, lease_id, amount):
    try:
        handle = person_lock.get_handle(person_id, lease_id)
    except LockNotHeld as err:
        logger.error("lease %s on person %s expired", lease_id, person_id, exc_info=True)
        raise

    with handle.releasing():
        r = people_redis()
        person_key = 'person:%s' % person_id
        val = r.get(person_key) or 0
        r.set(person_key, val + amount)


def enqueue_add_person_task(person_id, amount):
    try:
        handle = person_lock.acquire(person_id)
    except LockAlreadyHeld as err:
        logger.error("couldn't get lock")
        raise
    add_to_person.apply_async([person_id, handle.id, amount])

```


