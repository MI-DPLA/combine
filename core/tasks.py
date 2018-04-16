from background_task import background

# Get an instance of a logger
import logging
logger = logging.getLogger(__name__)

from core import models

'''
This file provides background tasks that are performed with Django-Background-Tasks
'''

@background(schedule=1)
def job_delete(job_id):

	'''
	Background task to perform job deletions
	'''
	
	try:
		# get job
		job = models.Job.objects.get(pk=job_id)
		logger.debug('retrieved Job ID %s, deleting' % job_id)
		
		# delete
		return job.delete()

	except:
		logger.debug('could not retrieve Job ID %s, aborting' % job_id)
		return False


@background(schedule=1)
def download_and_index_bulk_data(dbdd_id):

	'''
	Background task driver to manage downloading and indexing of bulk data

	Args:
		dbdd_id (int): ID of DPLABulkDataDownload (dbdd) instance
	'''

	# init bulk download instance
	dbdd = models.DPLABulkDataDownload.objects.get(pk=dbdd_id)

	# init data client with filepath
	dbdc = models.DPLABulkDataClient()

	# download data
	logger.debug('downloading %s' % dbdd.s3_key)
	dbdd.status = 'downloading'
	dbdd.save()
	download_results = dbdc.download_bulk_data(dbdd.s3_key, dbdd.filepath)	

	# index data
	logger.debug('indexing %s' % dbdd.filepath)
	dbdd.status = 'indexing'
	dbdd.save()
	es_index = dbdc.index_to_es(dbdd.filepath)	

	# update and return
	dbdd.es_index = es_index
	dbdd.status = 'finished'
	dbdd.save()

