# Generated by Django 5.0.7 on 2024-07-22 14:18

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('core', '0009_setting_alert_messages_and_more'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='request',
            name='date_created',
        ),
        migrations.RemoveField(
            model_name='request',
            name='date_updated',
        ),
    ]
