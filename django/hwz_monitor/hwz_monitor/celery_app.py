from __future__ import absolute_import, unicode_literals
import os
from celery import Celery

# setting the Django settings module.
os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'hwz_monitor.settings')
# use mongdodb as backend and redis as broker
app = Celery('hwz_monitor', \
    backend='mongodb://localhost:27017/celery_backend', \
    broker='redis://localhost:6379/0')

app.config_from_object('django.conf:settings', namespace='CELERY')

# Looks up for task modules in Django applications and loads them
app.autodiscover_tasks()
