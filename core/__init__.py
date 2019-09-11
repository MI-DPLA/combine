# use custom Core configuration
default_app_config = 'core.apps.CoreConfig'

# pylint: disable=wrong-import-position
# This will make sure the app is always imported when
# Django starts so that shared_task will use this app.
from .celery import celery_app

__all__ = ('celery_app',)
