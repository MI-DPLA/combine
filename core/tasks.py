from background_task import background

# Get an instance of a logger
import logging
logger = logging.getLogger(__name__)

from core.models import Job

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
		job = Job.objects.get(pk=job_id)
		logger.debug('retrieved Job ID %s, deleting' % job_id)
		
		# delete
		return job.delete()

	except:
		logger.debug('could not retrieve Job ID %s, aborting' % job_id)
		return False	