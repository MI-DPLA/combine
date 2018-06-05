
# generic imports
import django
from lxml import etree
import os
import sys

# check for registered apps signifying readiness, if not, run django.setup() to run as standalone
if not hasattr(django, 'apps'):
	os.environ['DJANGO_SETTINGS_MODULE'] = 'combine.settings'
	sys.path.append('/opt/combine')
	django.setup()	

# import django settings
from django.conf import settings
from django.db import connection

from core.models import Job

def get_job_db_bounds(job):

	'''
	Method to determine lower and upper bounds for job IDs, for more efficient MySQL retrieval
	'''	

	records = job.get_records()
	records = records.order_by('id')
	start_id = records.first().id
	end_id = records.last().id

	return {
		'lowerBound':start_id,
		'upperBound':end_id
	}


def get_job_as_df(spark, job_id):

	# get job
	input_job = Job.objects.get(pk=int(job_id))

	bounds = get_job_db_bounds(input_job)
	sqldf = spark.read.jdbc(
			settings.COMBINE_DATABASE['jdbc_url'],
			'core_record',
			properties=settings.COMBINE_DATABASE,
			column='id',
			lowerBound=bounds['lowerBound'],
			upperBound=bounds['upperBound'],
			numPartitions=settings.JDBC_NUMPARTITIONS
		)
	records = sqldf.filter(sqldf.job_id == int(job_id))
	return records









