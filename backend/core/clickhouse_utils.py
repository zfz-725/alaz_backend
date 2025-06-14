from datetime import datetime, timedelta, UTC
import json
import clickhouse_connect
from django.conf import settings
from django.utils import timezone
import logging

logger = logging.getLogger(__name__)

def verbose_log(log):
    if settings.LOG_BROADCASTER_VERBOSE_LOG:
        logger.info(log)

class Initializer:
    def __init__(self):
        self.client = get_clickhouse_client()

    def create_databases(self):
        self.client.command('CREATE DATABASE IF NOT EXISTS alaz')

    def create_tables(self):
        # TODO: Look for an index for monitoring ids
        self.client.command('''
            drop table if exists alaz.logs
        ''')

        self.client.command('''
            CREATE TABLE alaz.logs
            (
                `Timestamp` DateTime64(9) CODEC(Delta(8), ZSTD(1)),
                `TimestampDate` Date MATERIALIZED toDate(toUnixTimestamp(Timestamp)),
                `TimestampTime` DateTime MATERIALIZED toDateTime(toUnixTimestamp(Timestamp)),
                `TraceId` String CODEC(ZSTD(1)),
                `SpanId` String CODEC(ZSTD(1)),
                `TraceFlags` UInt8,
                `SeverityText` LowCardinality(String) CODEC(ZSTD(1)),
                `SeverityNumber` UInt8,
                `ServiceName` LowCardinality(String) CODEC(ZSTD(1)),
                `Body` String CODEC(ZSTD(1)),
                `ResourceSchemaUrl` LowCardinality(String) CODEC(ZSTD(1)),
                `ResourceAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
                `ScopeSchemaUrl` LowCardinality(String) CODEC(ZSTD(1)),
                `ScopeName` String CODEC(ZSTD(1)),
                `ScopeVersion` LowCardinality(String) CODEC(ZSTD(1)),
                `ScopeAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
                `LogAttributes` Map(LowCardinality(String), String) CODEC(ZSTD(1)),
                `BodyLength` UInt64 MATERIALIZED length(Body),
                `Processed` UInt8 DEFAULT 0,
                INDEX idx_trace_id TraceId TYPE bloom_filter(0.001) GRANULARITY 1,
                INDEX idx_res_attr_key mapKeys(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
                INDEX idx_res_attr_value mapValues(ResourceAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
                INDEX idx_scope_attr_key mapKeys(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
                INDEX idx_scope_attr_value mapValues(ScopeAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
                INDEX idx_log_attr_key mapKeys(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
                INDEX idx_log_attr_value mapValues(LogAttributes) TYPE bloom_filter(0.01) GRANULARITY 1,
                INDEX idx_body Body TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 1
            )
            ENGINE = MergeTree
            PARTITION BY toYYYYMM(TimestampDate)
            ORDER BY (Timestamp, ResourceAttributes['pod_uid'], ResourceAttributes['container_name'], LogAttributes['container_id'])
            TTL TimestampTime + toIntervalDay(14)
            SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
        ''')
        
        self.client.command('''
            drop table if exists alaz.log_resources_last_ts
        ''')
        
        self.client.command('''
            CREATE TABLE IF NOT EXISTS alaz.log_resources_last_ts
            (
                monitoring_id String,
                namespace String,
                pod_uid String,
                container_name String,
                container_id String,
                max_timestamp DateTime64(9)
            )
            ENGINE = MergeTree()
            ORDER BY (monitoring_id, namespace, pod_uid, container_name, container_id, max_timestamp);
        ''')

    def create_materialized_views(self):
        pass
        self.client.command('''
            drop view if exists alaz.log_resources_last_ts_mv
        ''')

        self.client.command('''
            CREATE MATERIALIZED VIEW IF NOT EXISTS alaz.log_resources_last_ts_mv
            TO alaz.log_resources_last_ts AS
            SELECT
                ResourceAttributes['monitoring_id'] as monitoring_id,
                ResourceAttributes['namespace'] as namespace,
                ResourceAttributes['pod_uid'] AS pod_uid,
                ResourceAttributes['container_name'] AS container_name,
                LogAttributes['container_id'] AS container_id,
                max(Timestamp) AS max_timestamp
            FROM alaz.logs
            GROUP BY
                monitoring_id,
                namespace,
                pod_uid,
                container_name,
                container_id                            
        ''')
        
            
    # def create_tcpdump_tables(self):
    #     if settings.TCPDUMP_ENABLED:
    #         self.client.command('''
    #                         drop table if exists alaz.tcpdump
    #                         ''')
            
    #         self.client.command('''
    #             CREATE TABLE IF NOT EXISTS alaz.tcpdump
    #             (
    #                 `timestamp` UInt64 CODEC(DoubleDelta, LZ4),
    #                 `monitoring_id` LowCardinality(String) CODEC(ZSTD(1)),
    #                 `cluster_id` LowCardinality(String) CODEC(ZSTD(1)),
    #                 `resource_type` String CODEC(ZSTD(1)),
    #                 `resource_id` String CODEC(ZSTD(1)),
    #                 `from_ip` String CODEC(ZSTD(1)),
    #                 `to_ip` String CODEC(ZSTD(1)),
    #                 `from_port` UInt16 CODEC(LZ4),
    #                 `to_port` UInt16 CODEC(LZ4),
    #                 `payload` String CODEC(ZSTD(2)),
    #                 INDEX body_idx payload TYPE tokenbf_v1(10240, 3, 0) GRANULARITY 4,
    #                 INDEX idx_query timestamp TYPE minmax GRANULARITY 1
    #             )
    #             ENGINE = MergeTree
    #             PARTITION BY toDate(timestamp / 60000000)
    #             ORDER BY timestamp
    #             TTL toDateTime(timestamp / 1000000) + toIntervalSecond(1209600)
    #             SETTINGS index_granularity = 8192, ttl_only_drop_parts = 1
    #                             ''')

# def return_dict_key_vals(log):
#     """Iteratively gets keys and values from a nested dictionary with prefixed keys.
    
#     Args:
#         log (dict): The nested dictionary.
        
#     Returns:
#         list of tuples: A list of (key, value) pairs where keys are prefixed with their parent keys.
#     """
#     if type(log) != dict:
#         return []
#     stack = [("", log)]  # Initialize with a tuple of (parent key path, current dict)
#     items = []

#     while stack:
#         parent_key, current_dict = stack.pop()
#         for key, value in current_dict.items():
#             # Construct the new key with parent key path.
#             new_key = f"{parent_key}.{key}" if parent_key else key
#             if isinstance(value, dict):
#                 # If the value is a dict, add it to the stack for further processing.
#                 stack.append((new_key, value))
#             else:
#                 # If it's not a dict, add the key and value to the items list.
#                 items.append((new_key, value))

#     return items

# def preprocess_log(metadata, date_str, log, partial, monitoring_id, cluster_id):
#     # date_str is either of format 2024-03-19T15:19:02.033149512+00:00
#     # or format 2024-03-19T15:19:34.854225103Z
    
#     # Convert the date string to a timestamp
#     # print(f'preprocess_log: date_str: {date_str}')
#     date_obj = datetime.fromisoformat(date_str)
#     timestamp = int(date_obj.timestamp() * 1000000)
#     # timestamp = int(date_obj.timestamp())
    
#     # timestamp = int(now.timestamp())
#     body = log
#     try:
#         log = json.loads(log)
#     except Exception:
#         pass
#     # key_vals = return_dict_key_vals(log)

#     if monitoring_id is None:
#         logger.warn(f'Monitoring id is None. Writing it as empty')
#         monitoring_id = ''
#     if cluster_id is None:
#         logger.warn(f'Cluster id is None. Writing it as empty')
#         cluster_id = ''

#     namespace = metadata['namespace']
#     pod_name = metadata['pod_name']
#     pod_uid = metadata['pod_uid']
#     container_name = metadata['container_name']
#     container_num = int(metadata['container_num'])

#     # resources_string_key = []
#     # resources_string_value = []

#     # attributes_string_key = []
#     # attributes_string_value = []
#     # attributes_int64_key = []
#     # attributes_int64_value = []
#     # attributes_float64_key = []
#     # attributes_float64_value = []
#     # attributes_bool_key = []
#     # attributes_bool_value = []
#     # attributes_array_key = []
#     # attributes_array_value = []
    
#     # for key, val in key_vals:
#     #     if isinstance(val, str):
#     #         attributes_string_key.append(key)
#     #         attributes_string_value.append(val)
#     #     elif isinstance(val, int):
#     #         attributes_int64_key.append(key)
#     #         attributes_int64_value.append(val)
#     #     elif isinstance(val, float):
#     #         attributes_float64_key.append(key)
#     #         attributes_float64_value.append(val)
#     #     elif isinstance(val, bool):
#     #         attributes_bool_key.append(key)
#     #         attributes_bool_value.append(val)
#     #     elif isinstance(val, list):
#     #         attributes_array_key.append(key)
#     #         attributes_array_value.append(json.dumps(val))

#     processed_log = (monitoring_id,
#                      cluster_id,
#                      namespace,
#                      pod_name,
#                      pod_uid,
#                      container_name,
#                      container_num,
#                      timestamp,
#                      partial,
#                      body,
#                     #  "1",
#                     #  resources_string_key,
#                     #  resources_string_value,
#                     #  attributes_string_key,
#                     #  attributes_string_value,
#                     #  attributes_int64_key,
#                     #  attributes_int64_value,
#                     #  attributes_float64_key,
#                     #  attributes_float64_value,
#                     #  attributes_bool_key,
#                     #  attributes_bool_value,
#                     #  attributes_array_key,
#                     #  attributes_array_value
#                      )

#     return processed_log

def get_clickhouse_client(pool_mgr=None):
    host = settings.CLICKHOUSE_HOST
    port = settings.CLICKHOUSE_PORT
    user = settings.CLICKHOUSE_USER
    password = settings.CLICKHOUSE_PASSWORD
    if pool_mgr:
        clickhouse_connect.get_client(host=host, port=port, username=user, password=password, pool_mgr=pool_mgr)
    return clickhouse_connect.get_client(host=host, port=port, username=user, password=password)

# def insert_logs(logs, client):
#     # client = get_clickhouse_client()
#     table = 'alaz.logs'
#     # verbose_log(logs)
#     # print(logs)
#     try:
#         client.insert(table, logs, 
#                     ['monitoring_id', 
#                     'cluster_id',
#                     'namespace',
#                     'pod_name',
#                     'pod_uid',
#                     'container_name',
#                     'container_num',
#                     'timestamp', 
#                     'partial',
#                     'body'])
#                     #    'resources_string_key', 
#                     #    'resources_string_value', 
#                     #    'attributes_string_key', 
#                     #    'attributes_string_value', 
#                     #    'attributes_int64_key', 
#                     #    'attributes_int64_value', 
#                     #    'attributes_float64_key', 
#                     #    'attributes_float64_value', 
#                     #    'attributes_bool_key', 
#                     #    'attributes_bool_value',
#                     #    'attributes_array_key',
#                     #    'attributes_array_value'
#                     #    ]
#                     #   )
#     except Exception:
#         for log in logs:
#             try:
#                 client.insert(table, [log], 
#                     ['monitoring_id', 
#                     'cluster_id',
#                     'namespace',
#                     'pod_name',
#                     'pod_uid',
#                     'container_name',
#                     'container_num',
#                     'timestamp', 
#                     'partial',
#                     'body'])
#                     #    'resources_string_key', 
#                     #    'resources_string_value', 
#                     #    'attributes_string_key', 
#                     #    'attributes_string_value', 
#                     #    'attributes_int64_key', 
#                     #    'attributes_int64_value', 
#                     #    'attributes_float64_key', 
#                     #    'attributes_float64_value', 
#                     #    'attributes_bool_key', 
#                     #    'attributes_bool_value',
#                     #    'attributes_array_key',
#                     #    'attributes_array_value'
#                     #    ]
#                     #   )
#             except Exception as e:
#                 logger.error(f'Error inserting log: {log} with error: {e}')
#     # print(f'Inserted {len(logs)} logs with pod_uids: {[log[4] for log in logs]}')
#     # print(f'Inserted logs {logs}')

# def insert_tcpdump(tcpdump, client):
#     table = 'alaz.tcpdump'
#     try:
#         client.insert(table, tcpdump, 
#                     ['timestamp', 
#                     'monitoring_id',
#                     'cluster_id',
#                     'resource_type',
#                     'resource_id',
#                     'from_ip',
#                     'to_ip',
#                     'from_port',
#                     'to_port',
#                     'payload'])
#     except Exception:
#         for data in tcpdump:
#             try:
#                 client.insert(table, [data], 
#                     ['timestamp', 
#                     'monitoring_id',
#                     'cluster_id',
#                     'resource_type',
#                     'resource_id',
#                     'from_ip',
#                     'to_ip',
#                     'from_port',
#                     'to_port',
#                     'payload'])
#             except Exception as e:
#                 logger.error(f'Error inserting tcpdump: {data} with error: {e}')
#     # print(f'Inserted {len(tcpdump)} logs with pod_uids: {[log[4] for log in tcpdump]}')
#     # print(f'Inserted logs {tcpdump}')

def get_latest_logs(count, monitoring_id, pod_uid, container_name, container_num, client, end_timestamp):
    # INITIAL LOGS FUNC
    # client = get_clickhouse_client()
    # end_time_with_lag = datetime.now(UTC) - timedelta(seconds=settings.LOG_BROADCAST_LAG_SECONDS)
    # end_timestamp = int(datetime.timestamp(end_time_with_lag) * 1_000_000)
    query = f"SELECT toUnixTimestamp64Micro(Timestamp), Body \
FROM alaz.logs \
WHERE ResourceAttributes['pod_uid'] = '{pod_uid}' AND \
ResourceAttributes['container_name'] = '{container_name}' AND \
LogAttributes['container_id'] = '{container_num}' AND \
Timestamp < toDateTime({end_timestamp} / 1000000) \
ORDER BY Timestamp DESC LIMIT {count}"
    
    logger.info(f'get_latest_logs: Query: {query}')
    logs = client.query(query)
    # rows = map(lambda x: x[0], reversed(logs.result_rows))
    rows = reversed(logs.result_rows)
    return list(rows)

def get_logs_from_start_time(count, monitoring_id, pod_uid, container_name, container_num, start_timestamp, client, end_timestamp):
    # INITIAL LOGS FUNC
    # client = get_clickhouse_client()
    # end_time_with_lag = datetime.now(UTC) - timedelta(seconds=settings.LOG_BROADCAST_LAG_SECONDS)
    # end_timestamp = int(datetime.timestamp(end_time_with_lag) * 1_000_000)
    
    query = f"SELECT toUnixTimestamp64Micro(Timestamp), Body \
FROM alaz.logs \
WHERE ResourceAttributes['pod_uid'] = '{pod_uid}' AND \
ResourceAttributes['container_name'] = '{container_name}' AND \
LogAttributes['container_id'] = '{container_num}' AND \
Timestamp > toDateTime({start_timestamp} / 1000000) and Timestamp <= toDateTime({end_timestamp} / 1000000) \
ORDER BY Timestamp DESC LIMIT {count}"

    logger.info(f'get_logs_from_start_time: Query: {query}')
    logs = client.query(query)
    # rows = map(lambda x: x[0], reversed(logs.result_rows))
    rows = reversed(logs.result_rows)
    return list(rows)

def get_all_logs(monitoring_id, pod_uid, container_name, container_num):
    # DOWNLOAD FUNC
    client = get_clickhouse_client()
    end_time_with_lag = datetime.now(UTC) - timedelta(seconds=settings.LOG_BROADCAST_LAG_SECONDS)
    end_timestamp = datetime.timestamp(end_time_with_lag)

    query = f"SELECT LogAttributes['partial'], Body \
FROM alaz.logs \
WHERE ResourceAttributes['pod_uid'] = '{pod_uid}' AND \
ResourceAttributes['container_name'] = '{container_name}' AND \
LogAttributes['container_id'] = '{container_num}' AND \
Timestamp <= toDateTime({end_timestamp}) \
ORDER BY Timestamp ASC"

    logger.info(f'get_all_logs: Query: {query}')
    logs = client.query(query)
    rows = logs.result_rows
    # rows = map(lambda x: x[0], reversed(logs.result_rows))
    return list(rows)

def get_all_logs_time_interval(pod_uid, container_name, container_num, end_time, client, start_time=None):
    # BROADCASTER FUNC
    # client = get_clickhouse_client()
    if start_time:
        query = f"SELECT toUnixTimestamp64Micro(Timestamp), Body \
        FROM alaz.logs \
        WHERE Timestamp > toDateTime({start_time} / 1000000) AND \
        Timestamp <= toDateTime({end_time} / 1000000) AND \
        ResourceAttributes['pod_uid'] = '{pod_uid}' AND \
        ResourceAttributes['container_name'] = '{container_name}' AND \
        LogAttributes['container_id'] = '{container_num}' \
        ORDER BY Timestamp ASC"
    else:
        query = f"SELECT toUnixTimestamp64Micro(Timestamp), Body \
        FROM alaz.logs \
        WHERE Timestamp <= toDateTime({end_time} / 1000000) AND \
        ResourceAttributes['pod_uid'] = '{pod_uid}' AND \
        ResourceAttributes['container_name'] = '{container_name}' AND \
        LogAttributes['container_id'] = '{container_num}' \
        ORDER BY Timestamp ASC"

    logger.info(f'get_all_logs_time_interval: Query: {query}')
    
    logs = client.query(query)
    rows = logs.result_rows
    return rows

def mark_logs_as_processing(client):
    
    mark_for_process_query = """
    ALTER TABLE alaz.logs
    UPDATE Processed = 1
    WHERE Processed = 0
    """
    
    client.query(mark_for_process_query)

def get_log_storage_usage(client):
    calculate_usage_query = """
    SELECT
        ResourceAttributes['monitoring_id'],
        sum(BodyLength) as total_size_bytes
    FROM alaz.logs
    WHERE Processed = 1
    GROUP BY ResourceAttributes['monitoring_id']
    """
    
    rows = client.query(calculate_usage_query).result_rows
    
    return rows

def mark_logs_as_processed(client):
    mark_as_processed_query = """
    ALTER TABLE alaz.logs
    UPDATE Processed = 2
    WHERE Processed = 1
    """
    client.query(mark_as_processed_query)

# def get_log_sources(monitoring_id, namespace):
#     client = get_clickhouse_client()
#     # Return the following:
#     # {'pod_name': {'pod_uid': uid, 'containers': [{'name': name, 'num': num}]}}
#     query = f"SELECT pod_name, pod_uid, container_name, container_num \
# FROM alaz.logs \
# WHERE monitoring_id = '{monitoring_id}' AND \
#     namespace = '{namespace}' \
# GROUP BY pod_name, pod_uid, container_name, container_num"
#     rows = client.query(query).result_rows
#     resources = {}
#     for row in rows:
#         pod_name = row[0]
#         pod_uid = row[1]
#         container_name = row[2]
#         container_num = row[3]
#         if pod_name not in resources:
#             resources[pod_name] = {'pod_uid': pod_uid, 'containers': []}
#         resources[pod_name]['containers'].append({'name': container_name, 'num': container_num})
#     return resources

def get_log_namespaces(monitoring_id):
    client = get_clickhouse_client()
    query = f"SELECT DISTINCT ResourceAttributes['namespace'] \
FROM alaz.logs \
WHERE ResourceAttributes['monitoring_id'] = '{monitoring_id}'"

    logger.info(f'get_log_namespaces: Query: {query}')
    rows = client.query(query).result_rows
    return [row[0] for row in rows]


def get_last_2_container_ids(monitoring_id, namespace):
    # Get the largest 2 container nums of each (monitoring_id, namespace, pod_uid, container_name) group
    # The numbers may not always add up by one. So fetch the second largest value
    client = get_clickhouse_client()

    query = """SELECT
    pod_uid,
    container_name,
    groupUniqArray(container_id) AS container_ids
FROM
(
    SELECT
        pod_uid,
        container_name,
        container_id,
        max_timestamp,
        row_number() OVER (PARTITION BY pod_uid, container_name ORDER BY max_timestamp DESC) AS rn
    FROM alaz.log_resources_last_ts
    WHERE monitoring_id = 'dcd27696-589a-4b9c-908b-b4bf086772e1'
      AND namespace = 'anteon-staging'
)
WHERE rn <= 2
GROUP BY
    pod_uid,
    container_name
"""

    query = query.format(monitoring_id=monitoring_id, namespace=namespace)
    logger.info(f'get_last_2_container_ids: Query: {query}')
    rows = client.query(query).result_rows
    return rows