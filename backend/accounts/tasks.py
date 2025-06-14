from os import sync
from celery import shared_task
from celery.utils.log import get_task_logger
from django.conf import settings
from accounts.models import User

from accounts.utils import sync_users_and_teams
from core.requester import MyBackendRequester
from django.utils import timezone

logger = get_task_logger(__name__)

def verbose_log(log):
    if settings.CELERY_VERBOSE_LOG:
        logger.info(log)

@shared_task
def update_users_teams():
    verbose_log('Updating users and teams')
    start_time = timezone.now()
    my_backend_requester = MyBackendRequester()
    summary = my_backend_requester.get_accounts_summary()
    verbose_log(f'Fetched accounts summary in {timezone.now() - start_time}')

    sync_users_and_teams(summary)



@shared_task
def check_enterprise_selfhosted():
    if settings.ANTEON_ENV != 'onprem':
        return

    root_user_qs = User.objects.filter(email=settings.ROOT_EMAIL)
    if not root_user_qs.exists():
        logger.error('Root user does not exist')
        return
    root_user = root_user_qs.first()
    if root_user.selfhosted_enterprise:
        return
    
    my_backend_requester = MyBackendRequester()
    selfhosted_enterprise_status = my_backend_requester.get_selfhosted_enterprise_status()
    if not selfhosted_enterprise_status:
        logger.error('Could not get selfhosted enterprise status')
        return
    root_user.selfhosted_enterprise = selfhosted_enterprise_status.get('enterprise_unlocked', False)
    root_user.save()