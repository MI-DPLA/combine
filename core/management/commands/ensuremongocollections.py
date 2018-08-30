
# generic imports
import logging

# django
from django.core.management.base import BaseCommand, CommandError

# import core
from core.models import *

# Get an instance of a logger
logger = logging.getLogger(__name__)

class Command(BaseCommand):

	help = 'Ensure Mongo collections are created, and have proper indices'

	def handle(self, *args, **options):		

		# Record model
		logger.debug('ensuring indices for record collection')
		Record.ensure_indexes()

		# RecordValidation model
		logger.debug('ensuring indices for record_validation collection')
		RecordValidation.ensure_indexes()

		# IndexMappingFailure model
		logger.debug('ensuring indices for index_mapping_failures collection')
		IndexMappingFailure.ensure_indexes()

		# return
		self.stdout.write(self.style.SUCCESS('Mongo collections and indices verified and/or created'))