# generic imports
import logging

# django
from django.core.management.base import BaseCommand, CommandError

# import core
from core.models import *

# Get an instance of a logger
logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Remove orphaned Records from Mongo that have no associated Job'

    def handle(self, *args, **options):
        # removing records with no job_id field
        no_job_ids = Record.objects(mongoengine.Q(job_id__exists=False))
        logger.debug('removing %s records without job_id field' %
                     no_job_ids.count())
        no_job_ids.delete()

        # remove records where Job no longer exists
        # get list of Jobs
        job_ids = list(Job.objects.values_list('id', flat=True))

        # get Records that have job_ids that do not match
        delete_results = mc_handle.combine.record.delete_many(
            {'job_id': {'$nin': job_ids}})
        logger.debug('removed %s records where Job does not exist' %
                     delete_results.deleted_count)

        # return
        self.stdout.write(self.style.SUCCESS(
            'Orphaned Records removed from MongoDB'))
