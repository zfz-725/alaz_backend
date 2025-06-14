import json
import os
import sys
import time

import django
from django.conf import settings

sys.path.append('/workspace/backend')
sys.path.append('/workspaces/alaz-backend/backend')
sys.path.append('/workspaces/alaz_backend/backend')
os.environ.setdefault("DJANGO_SETTINGS_MODULE", "backend.settings")
django.setup()

from core.requester import SlackRequester
from core.models import Setting

slack_requester = SlackRequester()
access_token = settings.SLACK_BLOAT_NOTIFIER_TOKEN

channels = slack_requester.get_channels(access_token)['channels']
# List the channels for the user to pick
print("Select a channel to post the message:")
# print(json.dumps(channels))
# print()
for i, channel in enumerate(channels):
    print(f"{i+1}: {channel['name']}")
channel_index = int(input("Enter the channel number: ")) - 1
channel_id = channels[channel_index]['id']

slack_requester.set_channel(access_token, channel_id)

try:
    setting = Setting.objects.get(name='default')
except Setting.DoesNotExist:
    print(f"Default setting not found. Please create it and set the bloat_notifier_channel as {channels[channel_index]['name']} (the channel you just selected)")
    sys.exit(1)

setting.bloat_notifier_channel = channels[channel_index]['name']
setting.save()
print(f"Channel set to {channels[channel_index]['name']} successfully")