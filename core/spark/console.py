
# generic imports
import django
from lxml import etree
import math
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


def export_records_as_xml(spark, base_path, job_dict, records_per_file):

	'''
	Function to export multiple Jobs, with folder hierarchy for each Job

	Args:
		base_path (str): base location for folder structure
		job_dict (dict): dictionary of directory name --> list of Job ids
			- e.g. single job: {'j29':[29]}			
			- e.g. published records: {'foo':[2,42], 'bar':[3]}
				- in this case, a union will be performed for all Jobs within a single key			
		records_per_file (int): number of XML records per file
	'''

	# clean base path
	base_path = "file:///%s" % base_path.lstrip('file://').rstrip('/')

	# loop through keys and export
	for folder_name, job_ids in job_dict.items():

		# handle single job_id
		if len(job_ids) == 1:

			# get Job records as df
			rdd_to_write = get_job_as_df(spark, job_ids[0]).select('document').rdd

		# handle multiple jobs
		else:

			rdds = [ get_job_as_df(spark, job_id).select('document').rdd for job_id in job_ids ]
			rdd_to_write = spark.sparkContext.union(rdds)

		# write rdd to disk
		rdd_to_write.repartition(math.ceil(rdd_to_write.count()/int(records_per_file)))\
		.map(lambda row: row.document.replace('<?xml version=\"1.0\" encoding=\"UTF-8\"?>',''))\
		.saveAsTextFile('%s/%s' % (base_path, folder_name))










