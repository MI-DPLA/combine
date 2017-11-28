# imports
import ast
import datetime
import django
from lxml import etree
import os
import sys

# pyjxslt
import pyjxslt

# load elasticsearch spark code
try:
	from es import ESIndex, MODSMapper
except:
	from core.spark.es import ESIndex, MODSMapper

# import Row from pyspark
from pyspark.sql import Row
from pyspark.sql.types import StringType, StructField, StructType, BooleanType, ArrayType, IntegerType
import pyspark.sql.functions as pyspark_sql_functions
from pyspark.sql.functions import udf
from pyspark.sql.window import Window

# init django settings file to retrieve settings
os.environ['DJANGO_SETTINGS_MODULE'] = 'combine.settings'
sys.path.append('/opt/combine')
django.setup()
from django.conf import settings

# import select models from Core
from core.models import CombineJob, Job, JobTrack, Transformation


class CombineRecordSchema(object):

	'''
	Class to organize Combine specific spark dataframe schemas
	'''

	def __init__(self):

		# schema for Combine records
		self.schema = StructType([
				StructField('record_id', StringType(), True),
				StructField('oai_id', StringType(), True),
				StructField('document', StringType(), True),
				StructField('error', StringType(), True),
				StructField('unique', BooleanType(), False),
				StructField('job_id', IntegerType(), False),
				StructField('oai_set', StringType(), True)
			]
		)

		# fields
		self.field_names = [f.name for f in self.schema.fields if f.name != 'id']




class HarvestSpark(object):

	'''
	Spark code for harvesting records
	'''

	@staticmethod
	def spark_function(spark, **kwargs):

		'''
		Harvest records via OAI.

		As a harvest type job, unlike other jobs, this introduces various fields to the Record for the first time:
			- record_id 
			- job_id
			- oai_id
			- oai_set
			- publish_set_id
			- unique (TBD)

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

		# start job_track instance, marking job start
		job_track = JobTrack(
			job_id = job.id
		)
		job_track.save()

		df = spark.read.format("dpla.ingestion3.harvesters.oai")\
		.option("endpoint", kwargs['endpoint'])\
		.option("verb", kwargs['verb'])\
		.option("metadataPrefix", kwargs['metadataPrefix'])\
		.option(kwargs['scope_type'], kwargs['scope_value'])\
		.load()

		# select records with content
		records = df.select("record.*").where("record is not null")

		# repartition
		records = records.repartition(settings.JDBC_NUMPARTITIONS)

		# attempt to find and select <metadata> element from OAI record, else filter out
		def find_metadata(document):
			if type(document) == str:
				xml_root = etree.fromstring(document)
				m_root = xml_root.find('{http://www.openarchives.org/OAI/2.0/}metadata')
				if m_root is not None:
					# expecting only one child to <metadata> element
					m_children = m_root.getchildren()
					if len(m_children) == 1:
						m_child = m_children[0]
						m_string = etree.tostring(m_child).decode('utf-8')
						return m_string
				else:
					return 'none'
			else:
				return 'none'

		metadata_udf = udf(lambda col_val: find_metadata(col_val), StringType())
		records = records.select(*[metadata_udf(col).alias('document') if col == 'document' else col for col in records.columns])
		records = records.filter(records.document != 'none')

		# copy 'id' from OAI harvest to 'record_id' column
		records = records.withColumn('record_id', records.id)

		# add job_id as column
		job_id = job.id
		job_id_udf = udf(lambda id: job_id, IntegerType())
		records = records.withColumn('job_id', job_id_udf(records.id))

		# add oai_id column
		publish_set_id = job.record_group.publish_set_id
		oai_id_udf = udf(lambda id: '%s%s:%s' % (
			settings.COMBINE_OAI_IDENTIFIER,
			publish_set_id,
			id), StringType())
		records = records.withColumn('oai_id', oai_id_udf(records.id))

		# add oai_set
		records = records.withColumn('oai_set', records.setIds[0])

		# add blank error column
		error = udf(lambda id: '', StringType())
		records = records.withColumn('error', error(records.id))

		# index records to db
		save_records(
			spark=spark,
			kwargs=kwargs,
			job=job,
			records_df=records
		)

		# finally, update finish_timestamp of job_track instance
		job_track.finish_timestamp = datetime.datetime.now()
		job_track.save()
		


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

		# start job_track instance, marking job start
		job_track = JobTrack(
			job_id = job.id
		)
		job_track.save()

		# read output from input job, filtering by job_id, grabbing Combine Record schema fields
		sqldf = spark.read.jdbc(
				settings.COMBINE_DATABASE['jdbc_url'],
				'core_record',
				properties=settings.COMBINE_DATABASE
			)
		records = sqldf.filter(sqldf.job_id == int(kwargs['input_job_id']))

		# get transformation
		transformation = Transformation.objects.get(pk=int(kwargs['transformation_id']))

		# if xslt type transformation
		if transformation.transformation_type == 'xslt':

			# define udf function for transformation
			def transform_xml(job_id, row, xslt_string):

				# attempt transformation and save out put to 'document'
				try:
					
					# transform with pyjxslt gateway
					gw = pyjxslt.Gateway(6767)
					gw.add_transform('xslt_transform', xslt_string)
					result = gw.transform('xslt_transform', row.document)
					gw.drop_transform('xslt_transform')

					# set trans_result tuple
					trans_result = (result, '')

				# catch transformation exception and save exception to 'error'
				except Exception as e:
					# set trans_result tuple
					trans_result = ('', str(e))

				# return Row
				return Row(
						record_id = row.record_id,
						oai_id = row.oai_id,
						document = trans_result[0],
						error = trans_result[1],
						job_id = int(job_id),
						oai_set = row.oai_set
					)

			# open XSLT transformation, pass to map as string
			with open(transformation.filepath,'r') as f:
				xslt_string = f.read()

			# transform via rdd.map
			job_id = job.id			
			records_trans = records.repartition(settings.SPARK_REPARTITION).rdd.map(lambda row: transform_xml(job_id, row, xslt_string))

		# back to DataFrame
		records_trans = records_trans.toDF()

		# index records to db
		save_records(
			spark=spark,
			kwargs=kwargs,
			job=job,
			records_df=records_trans
		)

		# finally, update finish_timestamp of job_track instance
		job_track.finish_timestamp = datetime.datetime.now()
		job_track.save()



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

		# start job_track instance, marking job start
		job_track = JobTrack(
			job_id = job.id
		)
		job_track.save()

		# rehydrate list of input jobs
		input_jobs_ids = ast.literal_eval(kwargs['input_jobs_ids'])

		# get list of RDDs from input jobs
		sqldf = spark.read.jdbc(
				settings.COMBINE_DATABASE['jdbc_url'],
				'core_record',
				properties=settings.COMBINE_DATABASE
			)		
		input_jobs_dfs = []		
		for input_job_id in input_jobs_ids:

			# db
			job_df = sqldf.filter(sqldf.job_id == int(input_job_id))
			input_jobs_dfs.append(job_df)

		# create aggregate rdd of frames
		agg_rdd = sc.union([ df.rdd for df in input_jobs_dfs ])
		agg_df = spark.createDataFrame(agg_rdd, schema=input_jobs_dfs[0].schema)

		# update job column, overwriting job_id from input jobs in merge
		job_id = job.id
		job_id_udf = udf(lambda record_id: job_id, IntegerType())
		agg_df = agg_df.withColumn('job_id', job_id_udf(agg_df.record_id))

		# index records to db
		save_records(
			spark=spark,
			kwargs=kwargs,
			job=job,
			records_df=agg_df
		)

		# finally, update finish_timestamp of job_track instance
		job_track.finish_timestamp = datetime.datetime.now()
		job_track.save()



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

		# start job_track instance, marking job start
		job_track = JobTrack(
			job_id = job.id
		)
		job_track.save()

		# read output from input job, filtering by job_id, grabbing Combine Record schema fields
		sqldf = spark.read.jdbc(
				settings.COMBINE_DATABASE['jdbc_url'],
				'core_record',
				properties=settings.COMBINE_DATABASE
			)
		records = sqldf.filter(sqldf.job_id == int(kwargs['input_job_id']))
		# records = records.select(CombineRecordSchema().field_names)
		
		# get rows with document content
		records = records[records['document'] != '']

		# update job column, overwriting job_id from input jobs in merge
		job_id = job.id
		job_id_udf = udf(lambda record_id: job_id, IntegerType())
		records = records.withColumn('job_id', job_id_udf(records.record_id))

		############################################################################################################
		# Predates data model rework, unknown if needed, keeping commented out for now...
		############################################################################################################
		# rewrite with publish_set_id from RecordGroup
		# publish_set_id = job.record_group.publish_set_id
		# set_id = udf(lambda row_id: publish_set_id, StringType())
		# records = records.withColumn('setIds', set_id(records.id))
		############################################################################################################

		# write job output to avro
		records.select(CombineRecordSchema().field_names).write.format("com.databricks.spark.avro").save(job.job_output)

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
		save_records(
			spark=spark,
			kwargs=kwargs,
			job=job,
			records_df=records,
			write_avro=False
		)

		# index to ES /published
		ESIndex.index_published_job(
			job_id = job.id,
			publish_set_id=job.record_group.publish_set_id
		)

		# finally, update finish_timestamp of job_track instance
		job_track.finish_timestamp = datetime.datetime.now()
		job_track.save()



def save_records(spark=None, kwargs=None, job=None, records_df=None, write_avro=True):

	'''
	Function to index records to DB.
	Additionally, generates and writes oai_id column to DB.

	Args:
		spark (pyspark.sql.session.SparkSession): spark instance from static job methods
		kwargs (dict): dictionary of args sent to Job spark method
		job (core.models.Job): Job instance		
		records_df (pyspark.sql.DataFrame): records as pyspark DataFrame
		write_avro (bool): boolean to write avro files to disk after DB indexing 

	Returns:
		None
			- determines if record_id unique among records DataFrame
			- selects only columns that match CombineRecordSchema
			- writes to DB, writes to avro files
	'''

	# check uniqueness (overwrites if column already exists)
	records_df = records_df.withColumn("unique", (
		pyspark_sql_functions.count('record_id')\
		.over(Window.partitionBy('record_id')) == 1)\
		.cast('integer'))

	# ensure columns to avro and DB
	records_df_combine_cols = records_df.select(CombineRecordSchema().field_names)

	# write records to DB
	records_df_combine_cols.write.jdbc(
		settings.COMBINE_DATABASE['jdbc_url'],
		'core_record',
		properties=settings.COMBINE_DATABASE,
		mode='append')

	# read rows from DB for indexing to ES and writing avro
	sqldf = spark.read.jdbc(
			settings.COMBINE_DATABASE['jdbc_url'],
			'core_record',
			properties=settings.COMBINE_DATABASE
		)
	job_id = job.id
	db_records = sqldf.filter(sqldf.job_id == job_id)

	# index to ElasticSearch
	if settings.INDEX_TO_ES:
		ESIndex.index_job_to_es_spark(
			spark,
			job=job,
			records_df=db_records,
			index_mapper=kwargs['index_mapper']
		)

	# write avro, coalescing default 200 partitions to 4 for output
	if write_avro:
		db_records.coalesce(4).write.format("com.databricks.spark.avro").save(job.job_output)



	

