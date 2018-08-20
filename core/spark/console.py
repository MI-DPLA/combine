
# generic imports
import django
import json
from lxml import etree
import math
import os
from pyspark.sql.types import StringType, StructField, StructType, BooleanType, ArrayType, IntegerType
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
from core.es import es_handle


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



############################################################################
# Convenience Functions
############################################################################

def get_job_as_df(spark, job_id, remove_id=False):

	'''
	Convenience method to retrieve set of records as Spark DataFrame
	'''

	pipeline = json.dumps({'$match': {'job_id': job_id}})
	mdf = spark.read.format("com.mongodb.spark.sql.DefaultSource")\
	.option("uri","mongodb://127.0.0.1")\
	.option("database","combine")\
	.option("collection","record")\
	.option("partitioner","MongoSamplePartitioner")\
	.option("spark.mongodb.input.partitionerOptions.partitionSizeMB",settings.MONGO_READ_PARTITION_SIZE_MB)\
	.option("pipeline",pipeline).load()

	# if remove ID
	if remove_id:
		mdf = mdf.select([ c for c in mdf.columns if c != '_id' ])

	return mdf


def get_job_es(spark, job_id=None, indices=None, es_query=None, field_exclude=None, as_rdd=False):

	'''
	Convenience method to retrieve ElasticSearch indices as DataFrame
	'''

	# handle indices
	if job_id:
		es_indexes = 'j%s' % job_id
	elif indices:
		es_indexes = ','.join(indices)

	# prep conf
	conf = {
		"es.resource":"%s/record" % es_indexes,
		"es.output.json":"true",
		"es.input.max.docs.per.partition":"10000"
	}

	# handle es_query
	if es_query:
		conf['es.query'] = es_query

	# handle field exclusion
	if field_exclude:
		conf['es.read.field.exclude'] = field_exclude

	# get es index as RDD
	es_rdd = spark.sparkContext.newAPIHadoopRDD(
		inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
		keyClass="org.apache.hadoop.io.NullWritable",
		valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
		conf=conf)

	# return rdd
	if as_rdd:
		return es_rdd

	# read json
	es_df = spark.read.json(es_rdd.map(lambda row: row[1]))
	
	# return
	return es_df


def get_sql_job_as_df(spark, job_id, remove_id=False):

	sqldf = spark.read.jdbc(settings.COMBINE_DATABASE['jdbc_url'],'core_record',properties=settings.COMBINE_DATABASE)
	sqldf = sqldf.filter(sqldf['job_id'] == job_id)

	# if remove ID
	if remove_id:
		sqldf = sqldf.select([ c for c in sqldf.columns if c != 'id' ])

	return sqldf


def copy_sql_to_mongo(spark, job_id):

	# get sql job
	sdf = get_sql_job_as_df(spark, job_id, remove_id=True)

	# repartition	
	sdf = sdf.rdd.repartition(200).toDF(schema=sdf.schema)

	# insert
	sdf.write.format("com.mongodb.spark.sql.DefaultSource")\
	.mode("append")\
	.option("uri","mongodb://127.0.0.1")\
	.option("database","combine")\
	.option("collection", "record").save()


def copy_sql_to_mongo_adv(spark, job_id, lowerBound, upperBound, numPartitions):

	sqldf = spark.read.jdbc(
			settings.COMBINE_DATABASE['jdbc_url'],
			'core_record',
			properties=settings.COMBINE_DATABASE,
			column='id',
			lowerBound=lowerBound,
			upperBound=upperBound,
			numPartitions=numPartitions
		)
	db_records = sqldf.filter(sqldf.job_id == int(job_id))

	db_records.write.format("com.mongodb.spark.sql.DefaultSource")\
	.mode("append")\
	.option("uri","mongodb://127.0.0.1")\
	.option("database","combine")\
	.option("collection", "record").save()





