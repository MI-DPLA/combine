
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

from core.models import Job, PublishedRecords


def get_job_db_bounds(record_qs):

	'''
	Method to determine lower and upper bounds for job IDs, for more efficient MySQL retrieval

	Args:
		record_qs(django.models.QuerySet): queryset of records for finding bounds
	'''
	
	record_qs = record_qs.order_by('id')
	start_id = record_qs.first().id
	end_id = record_qs.last().id

	return {
		'lowerBound':start_id,
		'upperBound':end_id
	}


def get_job_as_df(spark, job_id, published=False):

	'''
	Convenience method to retrieve set of records as Spark DataFrame
	'''

	# return published
	if published:
		pr = PublishedRecords()
		record_qs = pr.records

	# if not published
	else:
		# get job
		record_qs = Job.objects.get(pk=int(job_id)).get_records()

	bounds = get_job_db_bounds(record_qs)
	sqldf = spark.read.jdbc(
			settings.COMBINE_DATABASE['jdbc_url'],
			'core_record',
			properties=settings.COMBINE_DATABASE,
			column='id',
			lowerBound=bounds['lowerBound'],
			upperBound=bounds['upperBound'],
			numPartitions=settings.JDBC_NUMPARTITIONS
		)
	if published:
		records = sqldf.filter(sqldf.published == True)
	else:
		records = sqldf.filter(sqldf.job_id == int(job_id))
	return records









