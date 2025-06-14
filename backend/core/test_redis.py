
import os
import sys
import threading
import django
sys.path.append('/workspace/backend')
sys.path.append('/workspaces/alaz_backend')
sys.path.append('/workspaces/alaz_backend/backend')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "backend.settings")
django.setup()
from django.conf import settings
import redis

print_lock = threading.Lock()

def thread_func(thread_num):
    redis_client = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=0, charset="utf-8", decode_responses=True)

    while True:
        val = redis_client.lpop('test')
        if val is None:
            break
        with print_lock:
            print(f'{thread_num} - {val}')

redis_client = redis.Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, db=0, charset="utf-8", decode_responses=True)
        
for i in range(20):
    redis_client.rpush('test', i)

for i in range(5):
    threading.Thread(target=thread_func, args=(i, )).start()