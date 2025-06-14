import datetime
from datetime import timedelta
import json
import logging
import signal
import time
from django.conf import settings
from django.core.management.base import BaseCommand
from django.db import InterfaceError
import threading

import psycopg
import redis

from core.utils import insert_requests

logger = logging.getLogger(__name__)
from clickhouse_connect.driver import httputil
pool_mgr = httputil.get_pool_manager(maxsize=16, num_pools=12)

list_lock = threading.Lock()
queue_name = settings.REQUESTS_QUEUE
batch_size = settings.REQUESTS_BULK_BATCH_SIZE
killed = False

def verbose_log(log):
    if settings.REQUEST_WRITER_VERBOSE_LOG:
        logger.info(log)

def sigterm_handler(signum, frame):
    global killed
    logger.info('Received SIGTERM')
    killed = True

def sigint_handler(signum, frame):
    global killed
    logger.info('Received SIGINT')
    killed = True

CONN_DATA = settings.DATABASES['request_writer']

class Command(BaseCommand):
    help = 'Starts a server to broadcast logs.'

    def handle(self, *args, **options):

        threads = []
        for i in range(settings.REQUEST_WRITER_THREADS):
        # for i in range(1):
            thread = threading.Thread(target=self.thread_func, args=(i,))
            thread.start()
            logger.info(f'Thread {i} started')
            threads.append(thread)
            
        signal.signal(signal.SIGTERM, sigterm_handler)
        signal.signal(signal.SIGINT, sigint_handler)
        
        for thread in threads:
            thread.join()

    def thread_func(self, i):
        redis_client = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=0, charset="utf-8", decode_responses=True)
        connection = psycopg.connect(f"dbname={CONN_DATA['NAME']} user={CONN_DATA['USER']} password={CONN_DATA['PASSWORD']} host={CONN_DATA['HOST']} port={CONN_DATA['PORT']}")
        cursor = connection.cursor()
        
        # TODO: Add a transaction logic here
        while True:
            if killed:
                logger.info(f'Thread {i} killed. Closing connection')
                redis_client.close()
                cursor.close()
                break
            pipe = redis_client.pipeline()
            pipe.lrange(queue_name, 0, batch_size - 1)
            pipe.ltrim(queue_name, batch_size, -1)
            batch, _ = pipe.execute()
            batch = [json.loads(item) for item in batch]  # Decoding bytes to strings
            if not batch:
                time.sleep(0.5)
                continue
            try:
                insert_start = time.time()
                insert_requests(batch, cursor, connection)
                verbose_log(f'Batch inserted {len(batch)} requests in {time.time() - insert_start} seconds')
            except psycopg.OperationalError as e:
                if 'the connection is lost' in str(e):
                    logger.warn(f'Error batch inserting requests: {e}')
                    connection = psycopg.connect(f"dbname={CONN_DATA['NAME']} user={CONN_DATA['USER']} password={CONN_DATA['PASSWORD']} host={CONN_DATA['HOST']} port={CONN_DATA['PORT']}")
                    cursor = connection.cursor()
                    try:
                        insert_requests(batch, cursor)
                    except Exception as e:
                        logger.error(f'Error batch inserting requests: {type(e)}, {e}')
            except Exception as e:
                logger.error(f'Error batch inserting requests: {type(e)} {e}')
                # TODO: Add one by one processing

            