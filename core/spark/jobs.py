# imports
import ast
import django
from lxml import etree
import os
import sys

# load elasticsearch spark code
from es import ESIndex, MODSMapper

# import Row from pyspark
from pyspark.sql import Row
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import udf

# init django settings file to retrieve settings
os.environ['DJANGO_SETTINGS_MODULE'] = 'combine.settings'
sys.path.append('/opt/combine')
django.setup()
from django.conf import settings

# import select models from Core
from core.models import CombineJob, Job



class HarvestSpark(object):

	'''
	Spark code for harvesting records
	'''

	@staticmethod
	def spark_function(spark, **kwargs):

		'''
		Harvest records, select non-null, and write to avro files

		Args:
			spark (pyspark.sql.session.SparkSession): provided by pyspark context
			kwargs:
				job_id (int): Job ID
				endpoint (str): OAI endpoint
				verb (str): OAI verb used
				metadataPrefix (str): metadataPrefix for OAI harvest
				scope_type (str): [setList, whiteList, blackList, harvestAllSets], used by DPLA Ingestion3
				scope_value (str): value for scope_type
				index_mapper (str): class name from core.spark.es, extending BaseMapper

		Returns:
			None:
			- harvests OAI records and writes to disk as avro files
			- indexes records into DB
			- map / flatten records and indexes to ES
		'''

		# get job
		job = Job.objects.get(pk=int(kwargs['job_id']))

		df = spark.read.format("dpla.ingestion3.harvesters.oai")\
		.option("endpoint", kwargs['endpoint'])\
		.option("verb", kwargs['verb'])\
		.option("metadataPrefix", kwargs['metadataPrefix'])\
		.option(kwargs['scope_type'], kwargs['scope_value'])\
		.load()

		# select records with content and write to avro
		records = df.select("record.*").where("record is not null")

		# add blank error column
		error = udf(lambda id: '', StringType())
		records = records.withColumn('error', error(records.id))

		# write them to avro files
		records.write.format("com.databricks.spark.avro").save(job.job_output)

		# reload from avro for future operations
		avro_records = spark.read.format('com.databricks.spark.avro').load(job.job_output)

		# index records to db
		index_records_to_db(
			job=job,
			publish_set_id=job.record_group.publish_set_id,
			records=avro_records
		)

		# finally, index to ElasticSearch
		if settings.INDEX_TO_ES:
			ESIndex.index_job_to_es_spark(
				spark,
				job=job,
				records_df=avro_records,
				index_mapper=kwargs['index_mapper']
			)



class TransformSpark(object):

	'''
	Spark code for Transform jobs
	'''

	@staticmethod
	def spark_function(spark, **kwargs):

		'''
		Harvest records, select non-null, and write to avro files

		Args:
			spark (pyspark.sql.session.SparkSession): provided by pyspark context
			kwargs:
				job_id (int): Job ID
				job_input (str): location of avro files on disk
				transform_filepath (str): location of XSL file used for transformation
				index_mapper (str): class name from core.spark.es, extending BaseMapper

		Returns:
			None
			- transforms records via XSL, writes new records to avro files on disk
			- indexes records into DB
			- map / flatten records and indexes to ES
		'''

		# get job
		job = Job.objects.get(pk=int(kwargs['job_id']))

		# read output from input_job
		df = spark.read.format('com.databricks.spark.avro').load(kwargs['job_input'])

		# define function for transformation
		def transform_xml(record_id, xml, xslt):

			# attempt transformation and save out put to 'document'
			try:
				# xslt_root = etree.fromstring(xslt)
				xslt_root = etree.parse(xslt)
				transform = etree.XSLT(xslt_root)
				xml_root = etree.fromstring(xml)
				mods_root = xml_root.find('{http://www.openarchives.org/OAI/2.0/}metadata/{http://www.loc.gov/mods/v3}mods')
				result_tree = transform(mods_root)
				result = etree.tostring(result_tree)
				return Row(
					id=record_id,
					document=result.decode('utf-8'),
					error=''
				)

			# catch transformation exception and save exception to 'error'
			except Exception as e:
				return Row(
					id=record_id,
					document='',
					error=str(e)
				)

		# transform via rdd.map
		transformed = df.rdd.map(lambda row: transform_xml(row.id, row.document, kwargs['transform_filepath']))

		# write them to avro files
		transformed.toDF().write.format("com.databricks.spark.avro").save(job.job_output)

		# index records to db
		index_records_to_db(
			job=job,
			publish_set_id=job.record_group.publish_set_id,
			records=transformed.toDF()
		)

		# finally, index to ElasticSearch
		if settings.INDEX_TO_ES:
			ESIndex.index_job_to_es_spark(
				spark,
				job=job,
				records_df=transformed.toDF(),
				index_mapper=kwargs['index_mapper']
			)



class MergeSpark(object):

	'''
	Spark code for Merging records from previously run jobs
	Note: Merge jobs merge only successful documents from an input job, not the errors
	'''

	@staticmethod
	def spark_function(spark, sc, **kwargs):

		'''
		Harvest records, select non-null, and write to avro files

		Args:
			spark (pyspark.sql.session.SparkSession): provided by pyspark context
			kwargs:
				job_id (int): Job ID
				job_inputs (list): list of locations of avro files on disk
				index_mapper (str): class name from core.spark.es, extending BaseMapper

		Returns:
			None
			- merges records from previous jobs, writes new aggregated records to avro files on disk
			- indexes records into DB
			- map / flatten records and indexes to ES
		'''

		# get job
		job = Job.objects.get(pk=int(kwargs['job_id']))

		# rehydrate list of input jobs
		input_jobs = ast.literal_eval(kwargs['job_inputs'])

		# get list of RDDs from input jobs
		input_jobs_dfs = []		
		for input_job in input_jobs:
			job_df = spark.read.format('com.databricks.spark.avro').load(input_job)
			job_df = job_df[job_df['document'] != '']
			input_jobs_dfs.append(job_df)

		# create aggregate rdd of frames
		agg_rdd = sc.union([ df.rdd for df in input_jobs_dfs ])
		agg_df = spark.createDataFrame(agg_rdd, schema=input_jobs_dfs[0].schema)

		# write agg to new avro files
		agg_df.write.format("com.databricks.spark.avro").save(job.job_output)

		# index records to db
		index_records_to_db(
			job=job,
			publish_set_id=job.record_group.publish_set_id,
			records=agg_df
		)

		# finally, index to ElasticSearch
		if settings.INDEX_TO_ES:
			ESIndex.index_job_to_es_spark(
				spark,
				job=job,
				records_df=agg_df,
				index_mapper=kwargs['index_mapper']
			)



class PublishSpark(object):

	@staticmethod
	def spark_function(spark, **kwargs):

		'''
		Publish records in Combine, prepares for OAI server output

		Args:
			spark (pyspark.sql.session.SparkSession): provided by pyspark context
			kwargs:
				job_id (int): Job ID
				job_input (str): location of avro files on disk

		Returns:
			None
			- creates symlinks from input job to new avro file symlinks on disk
			- indexes records into DB
			- map / flatten records and indexes to ES
			- map / flatten records and indexes to ES under /published index
		'''

		# get job
		job = Job.objects.get(pk=int(kwargs['job_id']))

		# read output from input_job
		df = spark.read.format('com.databricks.spark.avro').load(kwargs['job_input'])
		
		# get rows with document content
		docs = df[df['document'] != '']

		# rewrite with publish_set_id from RecordGroup
		publish_set_id = job.record_group.publish_set_id
		set_id = udf(lambda row_id: publish_set_id, StringType())
		docs = docs.withColumn('setIds', set_id(docs.id))
		docs.write.format("com.databricks.spark.avro").save(job.job_output)

		# write symlinks
		# confirm directory exists
		published_dir = '%s/published' % (settings.BINARY_STORAGE.split('file://')[-1].rstrip('/'))
		if not os.path.exists(published_dir):
			os.mkdir(published_dir)

		# get avro files
		job_output_dir = job.job_output.split('file://')[-1]
		avros = [f for f in os.listdir(job_output_dir) if f.endswith('.avro')]
		for avro in avros:
			os.symlink(os.path.join(job_output_dir, avro), os.path.join(published_dir, avro))

		# index records to db
		index_records_to_db(
			job=job,
			publish_set_id=job.record_group.publish_set_id,
			records=docs
		)

		# finally, index to ElasticSearch
		if settings.INDEX_TO_ES:
			ESIndex.index_job_to_es_spark(
				spark,
				job=job,
				records_df=docs,
				index_mapper=kwargs['index_mapper']
			)

			# index to ES /published
			ESIndex.index_published_job(
				job_id = job.id,
				publish_set_id=job.record_group.publish_set_id
			)



def index_records_to_db(job=None, publish_set_id=None, records=None):

	'''
	Function to index records to DB.
	Additionally, generates and writes oai_id column to DB.

	Args:
		job (core.models.Job): Job for records
		publish_set_id (str): core.models.RecordGroup.published_set_id, used to build OAI identifier
		records (pyspark.sql.DataFrame): records as pyspark DataFrame 
	'''

	# add job_id as column
	job_id = job.id
	job_id_udf = udf(lambda id: job_id, IntegerType())
	records = records.withColumn('job_id', job_id_udf(records.id))

	# add oai_id column
	oai_id_udf = udf(lambda id: '%s%s:%s' % (settings.COMBINE_OAI_IDENTIFIER, publish_set_id, id), StringType())
	records = records.withColumn('oai_id', oai_id_udf(records.id))

	# add unique column
	unique_udf = udf(lambda id: 1, IntegerType())
	records = records.withColumn('unique', unique_udf(records.id))

	# write records to DB
	records.withColumn('record_id', records.id)\
	.select(['record_id', 'job_id', 'oai_id', 'document', 'error', 'unique'])\
	.write.jdbc(settings.COMBINE_DATABASE['jdbc_url'],
		'core_record',
		properties=settings.COMBINE_DATABASE,
		mode='append')

