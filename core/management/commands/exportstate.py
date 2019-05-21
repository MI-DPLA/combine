# generic imports
import datetime
import logging

# django
from django.core.management.base import BaseCommand, CommandError

# import core
from core.models import *

# Get an instance of a logger
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    '''
    Manage command to trigger the state export of:
            - all Organizations (and downstream Jobs);
            - all Configuration Scenarios
    '''

    help = 'Using State Export/Import, export state of all Jobs and Configuration Scenarios'

    def add_arguments(self, parser):
        # add optional organization ids to skip
        parser.add_argument(
            '--skip_json',
            dest='skip_json',
            help='JSON for objects to skip',
            type=str
        )

    def handle(self, *args, **options):
        logger.debug('Exporting state of all Jobs and Configuration Scenarios')

        # init StateIO instance - sio
        sio = StateIO(
            name="Full State Export - %s" % datetime.datetime.now().strftime('%b. %d, %Y, %-I:%M:%S %p')
        )

        # run export
        sio.export_all(skip_dict=json.loads(options['skip_json']))

        # return
        self.stdout.write(self.style.SUCCESS('Export complete.'))
