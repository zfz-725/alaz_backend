import datetime
import json
import logging
import re
import os
import signal
import ssl
import time
import traceback
import uuid
from django.conf import settings
from django.core.management.base import BaseCommand
import socket
import threading
import tempfile
from django.db import connections

from django.db import connection
import redis
from accounts.utils import find_team_owner_if_exists
from core.utils import format_log_date
from clickhouse_connect.driver import httputil

from core.clickhouse_utils import get_clickhouse_client, insert_logs, preprocess_log
from core.models import Cluster, Container, Pod
logger = logging.getLogger(__name__)
# TODO: Use something other than _ for separating
metadata_pattern = r'^\*\*AlazLogs_[a-zA-Z0-9-]+_[a-zA -Z0-9-]+_[a-zA-Z0-9-]+_[a-zA-Z0-9-]+_[0-9]+\*\*$'
dirty_metadata_pattern = r'^\*\*AlazLogs_[a-zA-Z0-9-]+_[a-zA -Z0-9-]+_[a-zA-Z0-9-]+_[a-zA-Z0-9-]+_[0-9]+\*\*$'
redis_client = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=0, charset="utf-8", decode_responses=True)
pool_mgr = httputil.get_pool_manager(maxsize=16, num_pools=12)

list_lock = threading.Lock()
thread_lock = threading.Lock()

class LoggedException(Exception):
    pass

def trim_exception():
    exception = '\n'.join(traceback.format_exc().split('\n')[-7:])
    return exception

class CustomSSLSocket:
    def __init__(self, server_socket, ssl_context=None):
        self.server_socket = server_socket
        self.ssl_context = ssl_context

    def accept(self):
        newsock, addr = self.server_socket.accept()
        if not self.ssl_context:
            return newsock, addr
        try:
            ssl_sock = self.ssl_context.wrap_socket(newsock,
                                                    do_handshake_on_connect=True,
                                                    server_side=True)
            return ssl_sock, addr
        except ssl.SSLError as e:
            newsock.close()
            logger.warn(f"SSL error: {e}, from address: {addr}")
            raise LoggedException(f"SSL error: {e}, from address: {addr}")

def verbose_log(log):
    if settings.LOG_LISTENER_VERBOSE_LOG:
        logger.info(log)

killed = False

def sigterm_handler(signum, frame):
    global killed
    logger.info('Received SIGTERM')
    killed = True

def sigint_handler(signum, frame):
    global killed
    logger.info('Received SIGINT')
    killed = True

class Command(BaseCommand):
    help = 'Starts a TCP server to listen to logs sent by Alaz.'

    def add_arguments(self, parser):
        parser.add_argument('port', type=int, help='Port number')

    def log_connection_count(self):
        while True:
            if killed:
                logger.info('Killed, exiting connection counter')
                break
            verbose_log(f'Number of connections: {len(self.threads)}')
            time.sleep(10)

    def log_flusher(self):
        clickhouse_client = get_clickhouse_client(pool_mgr=pool_mgr)
        while True:
            if killed:
                clickhouse_client.close()
                logger.info('Killed, exiting flusher')
                break
            time.sleep(settings.LOG_FLUSH_PERIOD_SECONDS)
            with list_lock:
                self.flush_logs(clickhouse_client)

    def handle(self, *args, **options):
        port = options['port']
        self.threads = {}
        self.logs = []
        self.log_resources = set()

        ssl_private_key = os.getenv('CERT_PRIVATE_KEY_LOGS')
        ssl_ca_certificate = os.getenv('CERT_CA_CERTIFICATE_LOGS')

        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(('0.0.0.0', port))

        if ssl_private_key and ssl_ca_certificate:
            # Write the private key and certificate to temporary files
            with tempfile.NamedTemporaryFile(delete=False) as cert_file, \
                 tempfile.NamedTemporaryFile(delete=False) as key_file:
                cert_file.write(ssl_ca_certificate.encode())
                key_file.write(ssl_private_key.encode())
                cert_path = cert_file.name
                key_path = key_file.name

            context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            context.load_cert_chain(certfile=cert_path, keyfile=key_path)
            logger.info(self.style.SUCCESS(f'Starting TCP server with SSL on port {port}'))

            # Remove temporary files after use
            os.remove(cert_path)
            os.remove(key_path)
            # server = context.wrap_socket(server, server_side=True)
        else:
            logger.info(self.style.SUCCESS(f'Starting TCP server without SSL on port {port}'))
            context = None

        server.listen()
        server.settimeout(5.0)  # Set a timeout of 5 seconds

        custom_ssl_server = CustomSSLSocket(server, context)
        
        # Set up the signal handler
        signal.signal(signal.SIGTERM, sigterm_handler)
        signal.signal(signal.SIGINT, sigint_handler)
        connection_counter = threading.Thread(target=self.log_connection_count)
        log_flusher = threading.Thread(target=self.log_flusher)
        connection_counter.start()
        log_flusher.start()
        try:
            while True:
                if killed:
                    logger.info('Killed, exiting the main loop')
                    break
                try:
                    client, address = custom_ssl_server.accept()
                    with thread_lock:
                        if len(self.threads) >= settings.MAX_LOG_LISTENER_THREADS:
                            logger.error(f'Maximum number of threads reached: {len(self.threads)}')
                            client.close()
                            continue
                    
                    logger.info(f'Connected to {address}')
                except LoggedException as e:
                    continue
                except ssl.SSLError as e:
                    # TODO: Change this to logger.error
                    logger.warn(f'An SSL error occurred: {e}\n{traceback.format_exc()}')
                    continue
                except TimeoutError as e:
                    continue
                except socket.error as e:
                    # TODO: Change this to logger.error
                    logger.warn(f'Socket error occurred: {e}\n{traceback.format_exc()}')
                    continue
                except Exception as e:
                    logger.error(f'Unexpected error: {e}\n{traceback.format_exc()}')
                    continue
                # verbose_log(self.style.SUCCESS(f'Connected to {address}'))
                thread_id = uuid.uuid4()
                thread = threading.Thread(target=self.handle_client, args=(client, address, thread_id))
                with thread_lock:
                    self.threads[thread_id] = thread
                # thread.daemon = True
                thread.start()
        finally:
            connection_counter.join()
            log_flusher.join()
            with thread_lock:
                thread_ids = self.threads.keys()
                for thread_id in thread_ids:
                    if thread_id in self.threads:
                        thread = self.threads[thread_id]
                        logger.info(f'Joining thread {thread_id}')
                        output = thread.join()
                        logger.info(f'Joined thread {thread_id}: {output}')
            server.close()
            logger.info('Server closed')
            redis_client.close()
            pool_mgr.clear()

    def extract_log_prefix(self, data):
        parts = data.split(' ')  # Split the data by spaces
        # Assuming the format is consistent, the first four parts include the timestamp, "stdout", and "P/F"
        if len(parts) >= 3:
            log_prefix = ' '.join(parts[:3]).strip()  # Rejoin to get the prefix
            log = ' '.join(parts[3:]).strip()  # The rest of the log message
            partial = log_prefix[-1] == 'P'  # Check if the log is partial
        else:
            log_prefix = ''  # Fallback in case the data doesn't match the expected format
            log = data.strip()  # The rest of the log message
            partial = False  # Assume the log is not partial
        return log_prefix, log, partial
    
    def process_log(self, log_dict):
        verbose_log(f'Processing log: {log_dict}')
        with list_lock:
            self.accumulate_log(
                log_dict['metadata'], 
                log_dict['partial'], 
                log_dict['log'], 
                log_dict['monitoring_id'], 
                log_dict['cluster_id'], 
                log_dict['timestamp'])

            log_resource_key = f'{log_dict["metadata"]["pod_uid"]}_{log_dict["metadata"]["container_name"]}_{log_dict["metadata"]["container_num"]}'
            if log_resource_key not in self.log_resources:
                self.log_resources.add(log_resource_key)
        
            # broadcast_logs(
            #     log_dict['monitoring_id'], 
            #     log_dict['metadata']['pod_uid'], 
            #     log_dict['metadata']['container_name'], 
            #     log_dict['metadata']['container_num'], 
            #     [
            #         {'log': log_dict['log'], 
            #         'timestamp': log_dict['date_str']
            #         }
            #     ]
            # )

            # now = datetime.datetime.now(datetime.UTC)
            # logger.info(f'Now: {now}, Last flush time: {self.last_flush_time}')
            # if now - self.last_flush_time > datetime.timedelta(seconds=settings.LOG_FLUSH_PERIOD_SECONDS):
            # # if False:
            #     # logger.info(f'Flushing logs after {(now - self.last_flush_time).total_seconds()} seconds')
            #     # verbose_log(f'Flushing logs at {now}')
            #     self.flush_logs(clickhouse_client)
            #     self.last_flush_time = now
            # # self.flush_logs()

    def get_metadata(self, data):
        vals = data.split('_')
        if vals[1] in [None, ''] or vals[2] in [None, ''] or vals[3] in [None, ''] or vals[4] in [None, ''] or vals[5] in [None, '']:
            raise Exception(f'Error getting metadata from data: {data}')

        metadata = {
            'namespace': vals[1],
            'pod_name': vals[2],
            'pod_uid': vals[3],
            'container_name': vals[4],
            'container_num': vals[5][:-2]
        }
        # verbose_log(f'Got metadata {json.dumps(metadata)} from data: {data}')
        return metadata

    def get_cluster_from_pod(self, pod_uid, container_name, namespace, container_num):
        # logger.info(f'Getting cluster from pod: {pod_uid}')
        int_container_num = int(container_num)
        exception = None
        pods_key = settings.PODS_REDIS_KEY
        clusters_key = settings.CLUSTERS_REDIS_KEY
        containers_key = settings.CONTAINERS_REDIS_KEY

        try:
            for _ in range(5):
                try:
                    # Fetch pod data from Redis
                    pod_data = redis_client.hget(pods_key, pod_uid)
                    if pod_data is None:
                        logger.warn(f'Pod with UID {pod_uid} not found in Redis')
                        return None, None

                    pod_data = json.loads(pod_data)

                    cluster_id = pod_data['cluster']
                    
                    # Fetch cluster data from Redis
                    cluster_data = redis_client.hget(clusters_key, cluster_id)
                    if cluster_data is None:
                        logger.warn(f'Cluster with ID {cluster_id} not found in Redis')
                        return None, None
                    else:
                        cluster_data = json.loads(cluster_data)

                    container_data = redis_client.hget(containers_key, f'{cluster_id}_{pod_uid}_{container_name}_{namespace}')

                    if container_data is None:
                        logger.warn(f'Container with name {container_name} not found in Redis')
                        return None, None

                    container_data = json.loads(container_data)
                    container_data['updated'] = True
                    container_data['has_logs'] = True
                    container_nums = container_data['container_nums']
                    if int_container_num not in container_nums:
                        container_nums.append(int_container_num)
                        container_data['container_nums'] = container_nums
                    redis_client.hset(containers_key, f'{cluster_id}_{pod_uid}_{container_name}_{namespace}', json.dumps(container_data))

                    return str(cluster_id), str(cluster_data['monitoring_id'])
                except Exception as e:
                    exception = e
                    time.sleep(0.2)
                    continue
        finally:
            # pass
        #     # Ensure the connection is closed when done
            connection.close()

        logger.error(f'Error getting cluster from pod: {exception}')
        return None, None

    def accumulate_log(self, metadata, partial, log, monitoring_id, cluster_id, date_str):
        verbose_log(f'Accumulating log: {log}')
        self.logs.append((cluster_id, monitoring_id, metadata, partial, date_str, log))

        # print(self.logs)

    def flush_logs(self, clickhouse_client):
        preprocessed_logs = []
        start_time = datetime.datetime.now(datetime.UTC)
        for cluster_id, monitoring_id, metadata, partial, date_str, log in self.logs:
            try:
                # if cluster_id is None or monitoring_id is None:
                #     raise Exception('Cluster or monitoring ID is None')
                preprocessed_log = preprocess_log(metadata, date_str, log, partial, monitoring_id, cluster_id)
                preprocessed_logs.append(preprocessed_log)
                if cluster_id:
                    bytes = len(json.dumps(preprocessed_log))
                    redis_client.hincrby(settings.LOGS_SIZE_KEY, cluster_id, bytes)
            except Exception as e:
                exception = trim_exception()
                logger.error(f'Error flushing log: {exception} \npod: {metadata['pod_uid']}\ncluster_id: {cluster_id}\nmonitoring_id: {monitoring_id}\ndate_str: {date_str}\nlog: {log}')
                continue
            
        verbose_log(f'Preprocessed {len(preprocessed_logs)} logs in {datetime.datetime.now(datetime.UTC) - start_time}')
        start_time = datetime.datetime.now(datetime.UTC)
        if preprocessed_logs != []:
            insert_logs(preprocessed_logs, clickhouse_client)
            # verbose_log(f'Flushed logs: {preprocessed_logs}')
            # logger.info(f'Flushed {list(map(lambda x: (x[4], x[-1]), preprocessed_logs))} logs in {datetime.datetime.now(datetime.UTC) - start_time}')
            # logger.info(f'Flushed {len(preprocessed_logs)} logs in {datetime.datetime.now(datetime.UTC) - start_time}')

        self.logs = []
        group_names = self.get_open_channels()
        verbose_log(f'Group names: {group_names}')
        for resource in self.log_resources:
            verbose_log(f'Checking resource: {resource}')
            if resource not in group_names:
                continue

            redis_client.rpush(settings.LOG_QUEUE_KEY, resource)
            # publish_res = redis_client.publish(settings.LOG_NOTIFICATION_KEY, resource)
            verbose_log(f'Published to {settings.LOG_QUEUE_KEY}: {resource}')

        self.log_resources = set()
        
        verbose_log(f'Flushed {len(preprocessed_logs)} logs in {datetime.datetime.now(datetime.UTC) - start_time}')

    def get_open_channels(self):
        channels = redis_client.hgetall(settings.LOG_CHANNELS_KEY)
        group_names = set()
        for channel, _ in channels.items():
            channel_group_name = '_'.join(channel.split('_')[0:3])
            group_names.add(channel_group_name)
            
        return group_names

    def handle_client(self, connection, address, thread_id):
        try:
            verbose_log(self.style.SUCCESS(f'Connected to {address}'))
            metadata = None  # Metadata for the log
            monitoring_id = None  # Monitoring ID for the log
            cluster_id = None
            will_log = True
            self.last_flush_time = datetime.datetime.now(datetime.UTC)
            self.address = address
            # clickhouse_client = get_clickhouse_client(pool_mgr=pool_mgr)

            iteration_count = 0
            cluster = None
            previous_log = None
            empty_log_came = False
            connection.settimeout(5.0)  # No timeout for the accepted socket

            while True:
                if killed:
                    logger.info('Killed, exiting connection handler')
                    break
                try:
                    iteration_count += 1

                    # if settings.ANTEON_ENV != 'onprem' and iteration_count % 1 == 0:
                    if settings.ANTEON_ENV != 'onprem' and iteration_count % 50 == 0:
                        if cluster and find_team_owner_if_exists(cluster.user).active:
                            logger.warn(f'User with ID {cluster.user_id} is not active')
                            # Exit
                            break
                            
                    data = None
                    try:
                        data = socket.SocketIO(connection, 'r').readline()
                    except TimeoutError as e:
                        continue
                    except ssl.SSLError as e:
                        logger.warn(f'An SSL error occurred from address {address}: {e}')
                    except Exception as e:
                        logger.error(f'Error reading from client: {e}\n{traceback.format_exc()}')
                    if not data:
                        logger.warn(f'Socket is closed')
                        break  # Exit the loop if no data is received, indicating the client closed the connection
                    data = data.decode('utf-8', errors='ignore')
                    verbose_log(f'Data: {json.dumps(data)} for pod: {metadata['pod_uid'] if metadata else None}')
                    # continue
                    if data[-1] == '\n':
                        data = data[:-1]

                    # if will_log:
                    # print(f'Data: {data}')
                    # continue

                    # Determine the type of the message, it can be
                    # 1- Metadata
                    # 2- Partial log
                    # 3- Full log

                    if data == '':
                        empty_log_came = True
                        if previous_log:
                            self.process_log(previous_log)
                            previous_log = None
                        if will_log:
                            verbose_log('Empty log. Skipping')
                        continue

                    # verbose_log(f'Checking if the data matches the metadata pattern: {metadata_pattern}')
                    if re.match(metadata_pattern, data):
                        if will_log:
                            verbose_log('Metadata came')
                        # Metadata
                        if previous_log:
                            if not empty_log_came:
                                # Write the partial previous log to redis
                                buffered_log = {
                                    'metadata': previous_log['metadata'],
                                    'partial': previous_log['partial'],
                                    'log': previous_log['log'],
                                    'monitoring_id': previous_log['monitoring_id'],
                                    'cluster_id': previous_log['cluster_id'],
                                    'timestamp': previous_log['timestamp'],
                                    'date_str': previous_log['date_str'],
                                    'data': previous_log['data']
                                }
                                redis_client.set(redis_key, json.dumps(buffered_log))
                                logger.info(f'Saving partial log to redis as new metadata came: {buffered_log}')
                            else:
                                self.process_log(previous_log)

                        metadata_start = data.find('*')
                        data = data[metadata_start:]
                        try:
                            metadata = self.get_metadata(data.strip())
                        except Exception as e:
                            logger.error(f'Error getting metadata for log {data.strip()}: {e}')
                            metadata = None
                        previous_log = None
                        empty_log_came = False
                        
                        cluster_id, monitoring_id = self.get_cluster_from_pod(metadata['pod_uid'], metadata['container_name'], metadata['namespace'], metadata['container_num'])
                        # if monitoring_id is None or cluster_id is None:
                        #     logger.error('Could not found cluster')
                        #     break
                        # with list_lock:
                            # self.flush_logs(clickhouse_client)
                        # verbose_log(f'Metadata received: {metadata}')
                    else:
                        # Log
                        if not metadata:
                            logger.warn(f'Metadata is not received before the log: {data}')
                            continue

                        if will_log:
                            verbose_log('Metadata exists')
                        redis_key = f'{metadata["pod_uid"]}_{metadata["container_name"]}_{metadata["container_num"]}'
                        
                        if redis_client.exists(redis_key):
                            previous_log = json.loads(redis_client.get(redis_key))
                            redis_client.delete(redis_key)
                            # print(previous_log)
                            if previous_log:
                                data = previous_log['data'] + data
                                logger.info(f'Concatenated log: {data}')
                                # if will_log:
                                    # verbose_log(f'Concatenated log: {data}')
                            previous_log = None

                        if will_log:
                            verbose_log(f'Not metadata, will try to parse it')
                        try:
                            log_prefix, log, partial = self.extract_log_prefix(data)
                        # if not log_prefix:
                            # continue
                            timestamp = log_prefix.split(' ')[0]
                            date_obj = datetime.datetime.fromisoformat(timestamp)
                            date_str = format_log_date(date_obj)

                        except Exception as e:
                            try:
                                data_splits = data.split(' ')
                                timestamp = data_splits[0]
                                date_obj = datetime.datetime.fromisoformat(timestamp)
                                date_str = format_log_date(date_obj)
                            except Exception as e:
                                # TODO: Change this to logger.error
                                logger.warn(f'Could not extract timestamp from log: {data}')
                                continue

                        if previous_log:
                            self.process_log(previous_log)
                            previous_log = None

                        # if cluster_id is None or monitoring_id is None:
                        #     logger.warn(f'Cluster or monitoring ID is None for log: {data}')
                        #     continue 

                        previous_log = {
                            'metadata': metadata,
                            'partial': partial,
                            'log': log,
                            'monitoring_id': monitoring_id,
                            'cluster_id': cluster_id,
                            'timestamp': timestamp,
                            'date_str': date_str,
                            'data': data
                        }
                        
                        # if self.log_count > settings.LOG_BATCH_SIZE:
                        # if self.log_count > 25:
                        # if self.log_count > 0:
                        #     with list_lock:
                        #         self.flush_logs()
                except Exception as e:
                    if previous_log:
                        previous_log = None
                    # Get the last 6 lines of the traceback
                    exception = '\n'.join(traceback.format_exc().split('\n')[-7:])
                    logger.error(f'Error occurred in the log listener loop. \n{exception} \nLog type: {(type(data))}. \nLog: {data}')
                    continue
        finally:
            logger.info('Closing connection')
            
            if previous_log:
                self.process_log(previous_log)

            with list_lock:
                if len(self.logs) > 0:
                    clickhouse_client = get_clickhouse_client(pool_mgr=pool_mgr)
                    self.flush_logs(clickhouse_client)
                    clickhouse_client.close()

            try:
                connection.sendall(b'X')
                wait_closing = True
            except (BrokenPipeError, ConnectionResetError, ssl.SSLEOFError, ssl.SSLZeroReturnError) as e:
                wait_closing = False
                
            if wait_closing:
                while True:
                    try:
                        data = socket.SocketIO(connection, 'r').readline()
                        if not data:
                            break
                    except Exception as e:
                        logger.info(f'Error reading from client while closing: {e}')
                        time.sleep(0.5)
                        continue
                    
            connection.close()
            logger.warn(f'Connection closed with {self.address}')

            if not killed:
                with thread_lock:
                    del self.threads[thread_id]

        return
