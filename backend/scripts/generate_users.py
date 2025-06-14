
import os
import sys
import time

import django

sys.path.append('/workspace/backend')
sys.path.append('/workspaces/alaz-backend/backend')
sys.path.append('/workspaces/alaz_backend/backend')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "backend.settings")
django.setup()

from core.requester import MyBackendRequester
from django.contrib.auth import get_user_model
from django.conf import settings

ROOT_EMAIL = settings.ROOT_EMAIL
ROOT_PASSWORD = settings.ROOT_PASSWORD

if not ROOT_EMAIL:
    print("ERR: You must set ROOT_EMAIL environment variable")
if not ROOT_PASSWORD:
    print("ERR: You must set ROOT_PASSWORD environment variable")

user_model = get_user_model()

backend_requester = MyBackendRequester()

last_error = None
root_id = None
fetched = False
for _ in range(20):
    try:
        accounts_summary = backend_requester.get_accounts_summary()
        fetched = True
        break
    except Exception as e:
        last_error = e
        time.sleep(5)
    
if not fetched:
    print(f"ERR: Failed to fetch accounts summary: {last_error}")
    exit()
    
users = accounts_summary['users']
root_id = None
for user_id, user in users.items():
    if user['email'] == settings.ROOT_EMAIL:
        root_id = user_id

if root_id is None:
    print('ERR: Root user does not exist in the backend')
    exit()

# Create root user
user = user_model.objects.filter(email=ROOT_EMAIL)[0:1]
if not user:
    user = user_model.objects.create_superuser(id=root_id, email=ROOT_EMAIL, password=ROOT_PASSWORD, name='root')
    print("Created root user.")
    print(f"Email: {ROOT_EMAIL}")
    print(f"Password: {ROOT_PASSWORD}")
