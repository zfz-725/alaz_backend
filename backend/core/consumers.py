from datetime import datetime, UTC, timedelta
import logging
import uuid
from channels.generic.websocket import AsyncWebsocketConsumer
import json
from asgiref.sync import sync_to_async
from django.conf import settings
import jwt
import redis
from core.utils import format_log_date, verbose_log
from core.clickhouse_utils import get_clickhouse_client, get_latest_logs, get_logs_from_start_time
from channels.exceptions import StopConsumer
redis_client = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=0, charset="utf-8", decode_responses=True)

from core.models import Cluster

logger = logging.getLogger(__name__)

class LogConsumer(AsyncWebsocketConsumer):

    def check_cluster_exists(self, monitoring_id):
        return Cluster.objects.filter(monitoring_id=monitoring_id).exists()

    async def connect(self):
        verbose_log(f"Connecting to websocket")
        self.monitoring_id = self.scope['url_route']['kwargs']['monitoring_id']
        self.pod_uid = self.scope['url_route']['kwargs']['pod_uid']
        self.container_name = self.scope['url_route']['kwargs']['container_name']
        # self.container_id = self.scope['url_route']['kwargs']['container_id']
        self.container_num = self.scope['url_route']['kwargs']['container_num']

        self.start_time = self.scope['url_route']['kwargs'].get('start_time', None)  # In ms
        
        token = self.scope['url_route']['kwargs']['token']
        websocket_secret_key = settings.WEBSOCKET_SECRET_KEY
        credentials = jwt.decode(token, websocket_secret_key, algorithms=["HS256"])

        username = settings.ALAZ_BACKEND_USERNAME
        password = settings.ALAZ_BACKEND_PASSWORD

        if 'username' not in credentials or 'password' not in credentials or credentials['username'] != username or credentials['password'] != password:
            logger.warn(f"Invalid token: {token}")
            await self.close()
            return

        
        # Validate monitoring id
        verbose_log(f'Will validate monitoring_id: {self.monitoring_id}')
        cluster_exists = await sync_to_async(self.check_cluster_exists, thread_sensitive=True)(self.monitoring_id) 

        if not cluster_exists:
            logger.warn(f"Cluster with monitoring_id: {self.monitoring_id} does not exist")
            return

        verbose_log(f'Will accept connection')
        await self.accept()
        verbose_log(f'Accepted connection')

        # Send the latest logs
        initial_log_count = settings.INITIAL_LOG_COUNT
        clickhouse_client = get_clickhouse_client()
        last_timestamp = int((datetime.now(UTC) - timedelta(seconds=settings.LOG_BROADCAST_LAG_SECONDS)).timestamp() * 1_000_000)
        if self.start_time:
            start_timestamp = float(self.start_time) * 1000
            logs = await sync_to_async(get_logs_from_start_time, thread_sensitive=True)(initial_log_count, self.monitoring_id, self.pod_uid, self.container_name, self.container_num, start_timestamp, clickhouse_client, last_timestamp)
            pass
        else:
            logs = await sync_to_async(get_latest_logs, thread_sensitive=True)(initial_log_count, self.monitoring_id, self.pod_uid, self.container_name, self.container_num, clickhouse_client, last_timestamp)

        # logger.info(f'Initial logs: \n{logs}')
        verbose_log(f"Sending {len(logs)} initial logs")
        # print(f"Sending {len(logs)} initial logs")
        # logs = map(lambda log: log[0], logs)
        for log in logs:
            # last_timestamp = max(last_timestamp, log[0])
            await self.send_initial_log_message(log)

        last_timestamp = str(last_timestamp)

        verbose_log(f'Sent initial logs')

        self.group_name = f'{self.pod_uid}_{self.container_name}_{self.container_num}_{uuid.uuid4()}'
        verbose_log(f'Will set group name: {self.group_name} as Free and its last timestamp as {last_timestamp}')
        self.generated_channel_name = str(uuid.uuid4())
        redis_client.hset(settings.LOG_CHANNELS_MAPPING, self.group_name, self.generated_channel_name) # This is to keep track of the group name
        # We generate a random uuid as len(group_name) >= 100 violates django constraints
        await self.channel_layer.group_add(
            self.generated_channel_name,
            self.channel_name
        )
        redis_client.hset(settings.LOG_CHANNELS_KEY, self.group_name, last_timestamp)
        redis_client.set(f'log_concurrency_{self.group_name}', 'Free')
        verbose_log(f'Added to group {self.group_name}')

    async def disconnect(self, close_code):
        # Leave room group
        verbose_log(f"Disconnecting from websocket")
        verbose_log(f'Will disconnect from group {self.group_name} and delete it and its concurrency key')
        redis_client.hdel(settings.LOG_CHANNELS_KEY, self.group_name)
        redis_client.delete(f'log_concurrency_{self.group_name}')
        redis_client.hdel(settings.LOG_CHANNELS_MAPPING, self.group_name)
        await self.channel_layer.group_discard(
            self.group_name,
            self.channel_name
        )
        verbose_log(f'Deleted group {self.group_name} and its concurrency key')
        raise StopConsumer

    async def receive(self, text_data):
        # Handle incoming messages from WebSocket
        # For example, you might want to broadcast messages to the group
        pass

    async def send_initial_log_message(self, log):
        timestamp = log[0]
        message = log[1]

        # Turn timestamp to format
        # 2024-02-28T10:53:17.355087080+00:00
        # First turn it into a date object
        
        date_obj = datetime.fromtimestamp(timestamp / 1000000)
        date_str = format_log_date(date_obj)
        # date_str = date_obj.strftime('%Y-%m-%dT%H:%M:%S.%f%z')
        
        await self.send(text_data=json.dumps(
            {
                "type": "send.initial.log.message",  # Matches the method in LogConsumer
                'timestamp': date_str,
                "message": message,
                }))

    async def send_log_message(self, message):
        await self.send(text_data=json.dumps(message))