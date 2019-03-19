
# generic imports
import django
import json
from lxml import etree
import math
import os
import sys

# pyspark imports
from pyspark.sql.functions import udf, regexp_replace, lit
from pyspark.sql.types import StringType, StructField, StructType, BooleanType, ArrayType, IntegerType

# check for registered apps signifying readiness, if not, run django.setup() to run as standalone
if not hasattr(django, 'apps'):
	os.environ['DJANGO_SETTINGS_MODULE'] = 'combine.settings'
	sys.path.append('/opt/combine')
	django.setup()

# import django settings
from django.conf import settings
from django.db import connection

# import from core
from core.models import Job, PublishedRecords, CombineBackgroundTask
from core.es import es_handle

# import XML2kvp from uploaded instance
from xml2kvp import XML2kvp


############################################################################
# Background Tasks
############################################################################


def export_records_as_xml(spark, ct_id):

	'''
	Function to export multiple Jobs, with folder hierarchy for each Job

	Notes:
		- exports to s3 as parquet
			- with limited columns, can benefit from parquest's compression

	Args:
		ct_id (int): CombineBackgroundTask id
	'''

	# hydrate CombineBackgroundTask
	ct = CombineBackgroundTask.objects.get(pk=int(ct_id))

	# clean base path
	output_path = "file:///%s" % ct.task_params['output_path'].lstrip('file://').rstrip('/')

	# write DataFrame to S3
	if ct.task_params.get('s3_export', False) and ct.task_params.get('s3_export_type', None) == 'spark_df':

		# dynamically set credentials
		spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", settings.AWS_ACCESS_KEY_ID)
		spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", settings.AWS_SECRET_ACCESS_KEY)

		# determine column subset
		col_subset = ['*']

		# loop through keys and export
		rdds = []
		for folder_name, job_ids in ct.task_params['job_dict'].items():

			# handle single job_id
			if len(job_ids) == 1:
				rdds.extend([get_job_as_df(spark, job_ids[0]).select(col_subset).rdd])

			# handle multiple jobs
			else:
				rdds.extend([ get_job_as_df(spark, job_id).select(col_subset).rdd for job_id in job_ids ])

		# get union of all RDDs to write
		rdd_to_write = spark.sparkContext.union(rdds)

		# repartition
		rdd_to_write = rdd_to_write.repartition(math.ceil(rdd_to_write.count() / settings.TARGET_RECORDS_PER_PARTITION))

		# convert to DataFrame and write to s3 as parquet
		rdd_to_write.toDF().write.mode('overwrite').parquet('s3a://%s/%s' % (ct.task_params['s3_bucket'], ct.task_params['s3_key']))

	# write to disk
	else:

		# determine column subset
		col_subset = ['document']

		# loop through keys and export
		for folder_name, job_ids in ct.task_params['job_dict'].items():

			# handle single job_id
			if len(job_ids) == 1:

				# get Job records as df
				rdd_to_write = get_job_as_df(spark, job_ids[0]).select(col_subset).rdd

			# handle multiple jobs
			else:

				rdds = [ get_job_as_df(spark, job_id).select(col_subset).rdd for job_id in job_ids ]
				rdd_to_write = spark.sparkContext.union(rdds)

			# repartition, wrap in XML dec, and write
			rdd_to_write.repartition(math.ceil(rdd_to_write.count()/int(ct.task_params['records_per_file'])))\
			.map(lambda row: row.document.replace('<?xml version=\"1.0\" encoding=\"UTF-8\"?>',''))\
			.saveAsTextFile('%s/%s' % (output_path, folder_name))


def generate_validation_report(spark, output_path, task_params):

	job_id = task_params['job_id']
	validation_scenarios = [ int(vs_id) for vs_id in task_params['validation_scenarios']]

	# get job validations, limiting by selected validation scenarios
	pipeline = json.dumps({'$match': {'job_id': job_id, 'validation_scenario_id':{'$in':validation_scenarios}}})
	rvdf = spark.read.format("com.mongodb.spark.sql.DefaultSource")\
	.option("uri","mongodb://%s" % settings.MONGO_HOST)\
	.option("database","combine")\
	.option("collection","record_validation")\
	.option("partitioner","MongoSamplePartitioner")\
	.option("spark.mongodb.input.partitionerOptions.partitionSizeMB",settings.MONGO_READ_PARTITION_SIZE_MB)\
	.option("pipeline",pipeline).load()

	# get job as df
	records_df = get_job_as_df(spark, job_id)

	# merge on validation failures
	mdf = rvdf.alias('rvdf').join(records_df.alias('records_df'), rvdf['record_id'] == records_df['_id'])

	# select subset of fields for export, and rename
	mdf = mdf.select(
			'records_df._id.oid',
			'records_df.record_id',
			'rvdf.validation_scenario_id',
			'rvdf.validation_scenario_name',
			'rvdf.results_payload',
			'rvdf.fail_count'
		)

	# if mapped fields requested, query ES and join
	if len(task_params['mapped_field_include']) > 0:

		# get mapped fields
		mapped_fields = task_params['mapped_field_include']

		# get mapped fields as df
		if 'db_id' not in mapped_fields:
			mapped_fields.append('db_id')
		es_df = get_job_es(spark, job_id=job_id).select(mapped_fields)

		# join
		mdf = mdf.alias('mdf').join(es_df.alias('es_df'), mdf['oid'] == es_df['db_id'])

	# cleanup columns
	mdf = mdf.select([c for c in mdf.columns if c != 'db_id']).withColumnRenamed('oid','db_id')

	# write to output dir
	if task_params['report_format'] == 'csv':
		mdf.write.format('com.databricks.spark.csv').option("delimiter", ",").save('file://%s' % output_path)
	if task_params['report_format'] == 'tsv':
		mdf.write.format('com.databricks.spark.csv').option("delimiter", "\t").save('file://%s' % output_path)
	if task_params['report_format'] == 'json':
		mdf.write.format('json').save('file://%s' % output_path)


def export_records_as_tabular_data(spark, ct_id):


	'''
	Function to export multiple Jobs, with folder hierarchy for each Job

	Notes:
		- writes to s3 as JSONLines to avoid column names which contain characters
		that parquet will not accept
			- much less efficient storage-wise, but flexible for the field/column variety
			that tabular data has

	Args:
		ct_id (int): CombineBackgroundTask id

	Expecting from CombineBackgroundTask:
		output_path (str): base location for folder structure
		job_dict (dict): dictionary of directory name --> list of Job ids
			- e.g. single job: {'j29':[29]}
			- e.g. published records: {'foo':[2,42], 'bar':[3]}
				- in this case, a union will be performed for all Jobs within a single key
		records_per_file (int): number of XML records per file
		fm_export_config_json (str): JSON of configurations to be used
		tabular_data_export_type (str): 'json' or 'csv'
	'''

	# hydrate CombineBackgroundTask
	ct = CombineBackgroundTask.objects.get(pk=int(ct_id))

	# reconstitute fm_export_config_json
	fm_config = json.loads(ct.task_params['fm_export_config_json'])

	# clean base path
	output_path = "file:///%s" % ct.task_params['output_path'].lstrip('file://').rstrip('/')

	# write DataFrame to S3
	if ct.task_params.get('s3_export', False) and ct.task_params.get('s3_export_type', None) == 'spark_df':

		# dynamically set credentials
		spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", settings.AWS_ACCESS_KEY_ID)
		spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", settings.AWS_SECRET_ACCESS_KEY)

		# determine column subset
		col_subset = ['*']

		# loop through keys and export
		rdds = []
		for folder_name, job_ids in ct.task_params['job_dict'].items():

			# handle single job_id
			if len(job_ids) == 1:
				rdds.extend([get_job_as_df(spark, job_ids[0]).select(['document','combine_id','record_id']).rdd])

			# handle multiple jobs
			else:
				rdds.extend([ get_job_as_df(spark, job_id).select(['document','combine_id','record_id']).rdd for job_id in job_ids ])

		# union all
		batch_rdd = spark.sparkContext.union(rdds)

		# convert rdd
		kvp_batch_rdd = _convert_xml_to_kvp(batch_rdd, fm_config)

		# repartition to records per file
		kvp_batch_rdd = kvp_batch_rdd.repartition(math.ceil(kvp_batch_rdd.count() / settings.TARGET_RECORDS_PER_PARTITION))

		# convert to dataframe
		kvp_batch_df = spark.read.json(kvp_batch_rdd)

		# write to bucket as jsonl
		kvp_batch_df.write.mode('overwrite').json('s3a://%s/%s' % (ct.task_params['s3_bucket'], ct.task_params['s3_key']))

	# write to disk
	else:

		# loop through potential output folders
		for folder_name, job_ids in ct.task_params['job_dict'].items():

			# handle single job_id
			if len(job_ids) == 1:

				# get Job records as df
				batch_rdd = get_job_as_df(spark, job_ids[0]).select(['document','combine_id','record_id']).rdd

			# handle multiple jobs
			else:

				rdds = [ get_job_as_df(spark, job_id).select(['document','combine_id','record_id']).rdd for job_id in job_ids ]
				batch_rdd = spark.sparkContext.union(rdds)

			# convert rdd
			kvp_batch_rdd = _convert_xml_to_kvp(batch_rdd, fm_config)

			# repartition to records per file
			kvp_batch_rdd = kvp_batch_rdd.repartition(math.ceil(kvp_batch_rdd.count()/int(ct.task_params['records_per_file'])))

			# handle json
			if ct.task_params['tabular_data_export_type'] == 'json':
				_write_tabular_json(spark, kvp_batch_rdd, output_path, folder_name, fm_config)

			# handle csv
			if ct.task_params['tabular_data_export_type'] == 'csv':
				_write_tabular_csv(spark, kvp_batch_rdd, output_path, folder_name, fm_config)


def _convert_xml_to_kvp(batch_rdd, fm_config):

	'''
	Sub-Function to convert RDD of XML to KVP

	Args:
		batch_rdd (RDD): RDD containing batch of Records rows
		fm_config (dict): Dictionary of XML2kvp configurations to use for kvp_to_xml()

	Returns
		kvp_batch_rdd (RDD): RDD of JSONlines
	'''

	def kvp_writer_udf(row, fm_config):

		'''
		Converts XML to kvpjson, for testing okay?
		'''

		# get handler, that includes defaults
		xml2kvp_defaults = XML2kvp(**fm_config)

		# convert XML to kvp
		xml2kvp_handler = XML2kvp.xml_to_kvp(row.document, return_handler=True, handler=xml2kvp_defaults)

		# loop through and convert lists/tuples to multivalue_delim
		for k,v in xml2kvp_handler.kvp_dict.items():
			if type(v) in [list,tuple]:
				xml2kvp_handler.kvp_dict[k] = xml2kvp_handler.multivalue_delim.join(v)

		# mixin other row attributes to kvp_dict
		xml2kvp_handler.kvp_dict.update({
			'record_id':row.record_id,
			'combine_id':row.combine_id
		})

		# return JSON line
		return json.dumps(xml2kvp_handler.kvp_dict)

	# run UDF
	return batch_rdd.map(lambda row: kvp_writer_udf(row, fm_config))


def _write_tabular_json(spark, kvp_batch_rdd, base_path, folder_name, fm_config):

	# write JSON lines
	kvp_batch_rdd.saveAsTextFile('%s/%s' % (base_path, folder_name))


def _write_tabular_csv(spark, kvp_batch_rdd, base_path, folder_name, fm_config):

	# read rdd to DataFrame
	kvp_batch_df = spark.read.json(kvp_batch_rdd)

	# load XML2kvp instance
	xml2kvp_defaults = XML2kvp(**fm_config)

	# write to CSV
	kvp_batch_df.write.csv('%s/%s' % (base_path, folder_name), header=True)


def _write_rdd_to_s3(
	spark,
	rdd,
	bucket,
	key,
	access_key=settings.AWS_ACCESS_KEY_ID,
	secret_key=settings.AWS_SECRET_ACCESS_KEY):

	'''
	Function to write RDD to S3

	Args:
		rdd (RDD): RDD to write to S3
		bucket (str): bucket string to write to
		key (str): key/path to write to in S3 bucket
		access_key (str): default to settings, override with access key
		secret_key (str): default to settings, override with secret key
	'''

	# dynamically set s3 credentials
	spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", access_key)
	spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", secret_key)

	# write rdd to S3
	rdd.saveAsTextFile('s3a://%s/%s' % (bucket, key.lstrip('/')))



############################################################################
# Convenience Functions
############################################################################

def get_job_as_df(spark, job_id, remove_id=False):

	'''
	Convenience method to retrieve set of records as Spark DataFrame
	'''

	pipeline = json.dumps({'$match': {'job_id': job_id}})
	mdf = spark.read.format("com.mongodb.spark.sql.DefaultSource")\
	.option("uri","mongodb://%s" % settings.MONGO_HOST)\
	.option("database","combine")\
	.option("collection","record")\
	.option("partitioner","MongoSamplePartitioner")\
	.option("spark.mongodb.input.partitionerOptions.partitionSizeMB",settings.MONGO_READ_PARTITION_SIZE_MB)\
	.option("pipeline",pipeline).load()

	# if remove ID
	if remove_id:
		mdf = mdf.select([ c for c in mdf.columns if c != '_id' ])

	return mdf


def get_job_es(spark,
	job_id=None,
	indices=None,
	doc_type='record',
	es_query=None,
	field_include=None,
	field_exclude=None,
	as_rdd=False):

	'''
	Convenience method to retrieve mapped fields from ElasticSearch

	Args:

		job_id (int): job to retrieve
		indices (list): list of index strings to retrieve from
		doc_type (str): defaults to 'record', but configurable (e.g. 'item')
		es_query (str): JSON string of ES query
		field_include (str): comma seperated list of fields to include in response
		field_exclude (str): comma seperated list of fields to exclude in response
		as_rdd (boolean): boolean to return as RDD, or False to convert to DF
	'''

	# handle indices
	if job_id:
		es_indexes = 'j%s' % job_id
	elif indices:
		es_indexes = ','.join(indices)

	# prep conf
	conf = {
		"es.resource":"%s/%s" % (es_indexes,doc_type),
		"es.output.json":"true",
		"es.input.max.docs.per.partition":"10000",
		"es.nodes":"%s:9200" % settings.ES_HOST,
	}

	# handle es_query
	if es_query:
		conf['es.query'] = es_query

	# handle field exclusion
	if field_exclude:
		conf['es.read.field.exclude'] = field_exclude

	# handle field inclusion
	if field_include:
		conf['es.read.field.include'] = field_exclude

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
	.option("uri","mongodb://%s" % settings.MONGO_HOST)\
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
	.option("uri","mongodb://%s" % settings.MONGO_HOST)\
	.option("database","combine")\
	.option("collection", "record").save()





