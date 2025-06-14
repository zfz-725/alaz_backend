import datetime
from datetime import timedelta
import logging
import time
from django.conf import settings
from django.core.management.base import BaseCommand
import threading

import redis

from accounts.utils import find_team_owner_if_exists
from core.utils import broadcast_logs, format_log_date

from core.clickhouse_utils import get_all_logs_time_interval, get_clickhouse_client
from core.models import Cluster
logger = logging.getLogger(__name__)
from clickhouse_connect.driver import httputil
pool_mgr = httputil.get_pool_manager(maxsize=16, num_pools=12)

list_lock = threading.Lock()

def verbose_log(log):
    if settings.LOG_BROADCASTER_VERBOSE_LOG:
        logger.info(log)

# TODO: Handle shutdowns like log listener

class Command(BaseCommand):
    help = 'Starts a server to broadcast logs.'

    def handle(self, *args, **options):
        # Periodically fetch logs from the db, group them, send them to the websockets
        
        verbose_log(f'Log broadcast lag: {settings.LOG_BROADCAST_LAG_SECONDS}')

        threads = []
        for i in range(settings.LOG_BROADCAST_THREADS):
        # for i in range(1):
            thread = threading.Thread(target=self.thread_func, args=(i,))
            thread.start()
            logger.info(f'Thread {i} started')
            threads.append(thread)
            
        for thread in threads:
            thread.join()
        
    def thread_func(self, i):
        redis_client = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=0, charset="utf-8", decode_responses=True)
        clickhouse_client = get_clickhouse_client(pool_mgr=pool_mgr)
        sleep_seconds = settings.LOG_BROADCASTER_SLEEP_SECONDS
        # pubsub = redis_client.pubsub()
        # pubsub.psubscribe(f'__keyspace@0__:{settings.LOG_QUEUE_KEY}')
        
        # logger.info(f'{i} - Subscribed to {settings.LOG_QUEUE_KEY}')

        # logger.info(f'{i} - Fetched channels')
        while True:
            channels = redis_client.hgetall(settings.LOG_CHANNELS_KEY)
            verbose_log(f'{i} - Fetched channels: {channels}')
            for channel_key, _ in channels.items():
            # for _ in pubsub.listen():
                # verbose_log(f'{i} - Woke up from the listen')
                # group_name = message['data']
                # end_time = datetime.datetime.now(datetime.UTC) + timedelta(days=365)
                end_time = datetime.datetime.now(datetime.UTC) - timedelta(seconds=settings.LOG_BROADCAST_LAG_SECONDS)
                end_timestamp = int(datetime.datetime.timestamp(end_time) * 1_000_000)

                # Process until the queue is empty
                # TODO: A batching could be added here
                # while True:
                    # group_name = redis_client.lpop(settings.LOG_QUEUE_KEY)

                    # if group_name is None:
                        # break
                    # verbose_log(f'{i} - Group name: {group_name}')

                    # channels = redis_client.hgetall(settings.LOG_CHANNELS_KEY)

                    # channels_found = []
                    # # verbose_log(f'{i} - Will iterate channels: {channels}')
                    # for channel, _ in channels.items():
                    #     channel_group_name = '_'.join(channel.split('_')[0:3])
                    #     channel_unique_id = '_'.join(channel.split('_')[0:4])
                    #     # logger.info(f'{i} - Channel group name: {channel_group_name}')
                    #     if channel_group_name == group_name:
                    #         channels_found.append(channel_unique_id)

                    # verbose_log(f'{i} - Channels found: {channels_found} for group_name: {group_name}')

                    # logger.info(f'{i} - Will continue normally')
                    # for channel in channels_found:
                
                try:
                    pipe = redis_client.pipeline()
                    pipe.watch(f'log_concurrency_{channel_key}')
                    # TODO: Get the current queue, process it whole
                    start_time = pipe.hget(settings.LOG_CHANNELS_KEY, channel_key)
                    status = pipe.get(f'log_concurrency_{channel_key}')
                    if status in ['Locked', None]:
                        # verbose_log(f'{i} Channel {group_name} is locked or deleted with status: {status}. Skipping...')
                        continue
                    verbose_log(f'{i} Locking channel {channel_key}')
                    pipe.multi()
                    pipe.set(f'log_concurrency_{channel_key}', 'Locked')
                    start_timestamp = int(start_time)
                    start_date = datetime.datetime.fromtimestamp(start_timestamp / 1_000_000, datetime.UTC)
                    # verbose_log(f"{i} Fetching logs from {start_date} to {end_time} for channel {channel}")
                    # last_log_timestamp = logs[-1][4] if logs else None
                    last_log_timestamp = end_timestamp
                    verbose_log(f'{i} Trying setting last_log_timestamp: {last_log_timestamp} for channel {channel_key}')
                    pipe.hset(settings.LOG_CHANNELS_KEY, channel_key, last_log_timestamp)
                    # pipe.hset(settings.LOG_CHANNELS_KEY, channel, end_timestamp)
                    pipe.execute()
                    # logger.info(f'Result: {result}')
                    verbose_log(f'{i} Successfully locked channel {channel_key} for {start_date} to {end_time}')
                    verbose_log(f'{i} Successfully set last_log_timestamp: {last_log_timestamp} for channel {channel_key}. Its value in redis is: {redis_client.hget(settings.LOG_CHANNELS_KEY, channel_key)}')
                except redis.WatchError:
                    verbose_log(f'{i} Channel {channel_key} is locked. Skipping...')
                    continue
                except Exception as e:
                    logger.error(f'{i} Error locking channel {channel_key}: {e}')
                    continue
                # finally:
                    # redis_client.unwatch()

                verbose_log(f'{i} - Entered the lock')
                pod_uid, container_name, container_num = channel_key.split('_')[0:3]
                logs = get_all_logs_time_interval(pod_uid, container_name, container_num, end_timestamp, clickhouse_client, start_timestamp)

                grouped_logs = []
                verbose_log(f'{i} Will iterate logs')
                for log in logs:
                    timestamp = log[0]
                    body = log[1]

                    # last_timestamp = str(timestamp)

                    # Timestamp comes in microseconds
                    log_date = datetime.datetime.fromtimestamp(timestamp / 1_000_000, datetime.UTC)
                    log_date_str = format_log_date(log_date)
                        
                    grouped_logs.append({'timestamp': log_date_str, 'log': body})
                
                channel_name_mapping = redis_client.hget(settings.LOG_CHANNELS_MAPPING, channel_key)
                # Check if channel exists, if not, stop setting it
                if grouped_logs != []:
                    verbose_log(f'{i} Broadcasting {grouped_logs} logs for channel {channel_key}.')
                    broadcast_logs(channel_name_mapping, grouped_logs)
                    verbose_log(f'{i} Broadcasted logs for channel {channel_key}')

                verbose_log(f'{i} Unlocking channel {channel_key}')

                channel_concurrency = redis_client.get(f'log_concurrency_{channel_key}')
                if channel_concurrency == 'Locked': # It can be None if the connection is closed
                    verbose_log(f'{i} Channel {channel_key} concurrency is locked. Unlocking...')
                    redis_client.set(f'log_concurrency_{channel_key}', 'Free')
                
                time.sleep(sleep_seconds)