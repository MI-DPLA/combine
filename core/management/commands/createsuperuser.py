import logging
from django.core.management.base import BaseCommand, CommandError
from django.contrib.auth.models import User
logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Ensure superuser is created, if they do not exist'

    def handle(self, *args, **options):
        logger.debug('Checking for superuser existence...')
        if not User.objects.filter(username='combine', email='root@none.com', is_superuser=True):
            logger.debug('Superuser does not exist. Creating...')
            User.objects.create_superuser('combine', 'root@none.com', 'combine')
        self.stdout.write(self.style.SUCCESS('Superuser found and/or created'))
