# imports
import ast
import datetime
import django
from elasticsearch import Elasticsearch
import hashlib
import json
from lxml import etree
import os
import re
import requests
import shutil
import sys
import time
from types import ModuleType
import uuid

# pyjxslt
import pyjxslt

# import from core.spark
try:
	from es import ESIndex
	from utils import PythonUDFRecord, refresh_django_db_connection	
	from record_validation import ValidationScenarioSpark
except:
	from core.spark.es import ESIndex
	from core.spark.utils import PythonUDFRecord, refresh_django_db_connection
	from core.spark.record_validation import ValidationScenarioSpark

# import Row from pyspark
from pyspark.sql import Row
from pyspark.sql.types import StringType, StructField, StructType, BooleanType, ArrayType, IntegerType
import pyspark.sql.functions as pyspark_sql_functions
from pyspark.sql.functions import udf, regexp_replace
from pyspark.sql.window import Window

# check for registered apps signifying readiness, if not, run django.setup() to run as standalone
if not hasattr(django, 'apps'):
	os.environ['DJANGO_SETTINGS_MODULE'] = 'combine.settings'
	sys.path.append('/opt/combine')
	django.setup()	

# import django settings
from django.conf import settings
from django.db import connection

# import select models from Core
from core.models import CombineJob, Job, JobTrack, Transformation, PublishedRecords, RecordIdentifierTransformationScenario, RecordValidation



####################################################################
# Custom Exceptions 											   #
####################################################################

class AmbiguousIdentifier(Exception):
	pass


####################################################################
# Dataframe Schemas 											   #
####################################################################

class CombineRecordSchema(object):

	'''
	Class to organize Combine record spark dataframe schemas
	'''

	def __init__(self):

		# schema for Combine records
		self.schema = StructType([
				StructField('combine_id', StringType(), True),
				StructField('record_id', StringType(), True),
				StructField('document', StringType(), True),
				StructField('error', StringType(), True),
				StructField('unique', BooleanType(), False),
				StructField('job_id', IntegerType(), False),
				StructField('oai_set', StringType(), True),
				StructField('success', BooleanType(), False)				
			]
		)

		# fields
		self.field_names = [f.name for f in self.schema.fields if f.name != 'id']



####################################################################
# Spark Jobs           											   #
####################################################################

class HarvestOAISpark(object):

	'''
	Spark code for harvesting OAI records
	'''

	@staticmethod
	def spark_function(spark, **kwargs):

		'''
		Harvest records via OAI.

		As a harvest type job, unlike other jobs, this introduces various fields to the Record for the first time:
			- record_id 
			- job_id			
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
				validation_scenarios (list): list of Validadtion Scenario IDs

		Returns:
			None:
			- harvests OAI records and writes to disk as avro files
			- indexes records into DB
			- map / flatten records and indexes to ES
		'''

		# refresh Django DB Connection
		refresh_django_db_connection()

		# get job
		job = Job.objects.get(pk=int(kwargs['job_id']))

		# start job_track instance, marking job start
		job_track = JobTrack(
			job_id = job.id
		)
		job_track.save()

		# harvest OAI records via Ingestion3
		df = spark.read.format("dpla.ingestion3.harvesters.oai")\
		.option("endpoint", kwargs['endpoint'])\
		.option("verb", kwargs['verb'])\
		.option("metadataPrefix", kwargs['metadataPrefix'])\
		.option(kwargs['scope_type'], kwargs['scope_value'])\
		.load()

		# select records with content
		records = df.select("record.*").where("record is not null")

		# repartition
		records = records.repartition(settings.SPARK_REPARTITION)

		# attempt to find and select <metadata> element from OAI record, else filter out
		def find_metadata_udf(document):
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

		metadata_udf = udf(lambda col_val: find_metadata_udf(col_val), StringType())
		records = records.select(*[metadata_udf(col).alias('document') if col == 'document' else col for col in records.columns])
		records = records.filter(records.document != 'none')

		# establish 'success' column, setting all success for Harvest
		records = records.withColumn('success', pyspark_sql_functions.lit(1))

		# copy 'id' from OAI harvest to 'record_id' column
		records = records.withColumn('record_id', records.id)

		# add job_id as column
		job_id = job.id
		job_id_udf = udf(lambda id: job_id, IntegerType())
		records = records.withColumn('job_id', job_id_udf(records.id))

		# add oai_set
		records = records.withColumn('oai_set', records.setIds[0])

		# add blank error column
		error = udf(lambda id: '', StringType())
		records = records.withColumn('error', error(records.id))

		# run record identifier transformations (rits) if present
		records = run_rits(records, kwargs.get('rits', False))

		# index records to DB and index to ElasticSearch
		db_records = save_records(
			spark=spark,
			kwargs=kwargs,
			job=job,
			records_df=records,
			assign_combine_id=True
		)

		# run record validation scnearios if requested, using db_records from save_records() output
		vs = ValidationScenarioSpark(
			spark=spark,
			job=job,
			records_df=db_records,
			validation_scenarios = ast.literal_eval(kwargs['validation_scenarios'])
		)
		vs.run_record_validation_scenarios()

		# finally, update finish_timestamp of job_track instance
		job_track.finish_timestamp = datetime.datetime.now()
		job_track.save()
		


class HarvestStaticXMLSpark(object):

	'''
	Spark code for harvesting static xml records
	'''

	@staticmethod
	def spark_function(spark, **kwargs):

		'''
		Harvest static XML records provided by user.

		Expected input structure:
			/foo/bar <-- self.static_payload
				baz1.xml <-- record at self.xpath_query within file
				baz2.xml
				baz3.xml

		As a harvest type job, unlike other jobs, this introduces various fields to the Record for the first time:
			- record_id 
			- job_id
			- oai_set
			- publish_set_id
			- unique (TBD)

		Args:
			spark (pyspark.sql.session.SparkSession): provided by pyspark context
			kwargs:
				job_id (int): Job ID
				static_payload (str): path of static payload on disk
				# TODO: add other kwargs here from static job
				index_mapper (str): class name from core.spark.es, extending BaseMapper
				validation_scenarios (list): list of Validadtion Scenario IDs

		Returns:
			None:
			- opens and parses static files from payload
			- indexes records into DB
			- map / flatten records and indexes to ES
		'''

		# refresh Django DB Connection
		refresh_django_db_connection()

		# get job
		job = Job.objects.get(pk=int(kwargs['job_id']))

		# start job_track instance, marking job start
		job_track = JobTrack(
			job_id = job.id
		)
		job_track.save()

		# read directory of static files
		static_rdd = spark.sparkContext.wholeTextFiles(
				'file://%s' % kwargs['static_payload'],
				minPartitions=settings.SPARK_REPARTITION
			)


		# parse namespaces
		def get_namespaces(xml_node):
			nsmap = {}
			for ns in xml_node.xpath('//namespace::*'):
				if ns[0]:
					nsmap[ns[0]] = ns[1]
			return nsmap


		def get_metadata_udf(job_id, row, kwargs):

			# get doc string
			doc_string = row[1]

			try:

				# parse with lxml
				xml_root = etree.fromstring(doc_string.encode('utf-8'))

				# get namespaces
				nsmap = get_namespaces(xml_root)

				# get metadata root
				if kwargs['xpath_document_root'] != '':
					meta_root = xml_root.xpath(kwargs['xpath_document_root'], namespaces=nsmap)
				else:
					meta_root = xml_root.xpath('/*', namespaces=nsmap)
				if len(meta_root) == 1:
					meta_root = meta_root[0]
				elif len(meta_root) > 1:
					raise Exception('multiple elements found for metadata root xpath: %s' % kwargs['xpath_document_root'])
				elif len(meta_root) == 0:
					raise Exception('no elements found for metadata root xpath: %s' % kwargs['xpath_document_root'])

				# get unique identifier
				if kwargs['xpath_record_id'] != '':
					record_id = meta_root.xpath(kwargs['xpath_record_id'], namespaces=nsmap)
					if len(record_id) == 1:
						record_id = record_id[0].text
					elif len(meta_root) > 1:
						raise AmbiguousIdentifier('multiple elements found for identifier xpath: %s' % kwargs['xpath_record_id'])
					elif len(meta_root) == 0:
						raise AmbiguousIdentifier('no elements found for identifier xpath: %s' % kwargs['xpath_record_id'])
				else:
					record_id = hashlib.md5(doc_string.encode('utf-8')).hexdigest()

				# return success Row
				return Row(
					record_id = record_id,
					document = etree.tostring(meta_root).decode('utf-8'),
					error = '',
					job_id = int(job_id),
					oai_set = '',
					success = 1
				)

			# catch missing or ambiguous identifiers
			except AmbiguousIdentifier as e:

				# hash record string to produce a unique id
				record_id = hashlib.md5(doc_string.encode('utf-8')).hexdigest()

				# return error Row
				return Row(
					record_id = record_id,
					document = etree.tostring(meta_root).decode('utf-8'),
					error = str(e),
					job_id = int(job_id),
					oai_set = '',
					success = 0
				)

			# handle all other exceptions
			except Exception as e:

				# hash record string to produce a unique id
				record_id = hashlib.md5(doc_string.encode('utf-8')).hexdigest()

				# return error Row
				return Row(
					record_id = record_id,
					document = '',
					error = str(e),
					job_id = int(job_id),
					oai_set = '',
					success = 0
				)


		# map with get_metadata_udf 
		job_id = job.id		
		records = static_rdd.map(lambda row: get_metadata_udf(job_id, row, kwargs))

		# run record identifier transformations (rits) if present
		records = run_rits(records, kwargs.get('rits', False))

		# index records to DB and index to ElasticSearch
		db_records = save_records(
			spark=spark,
			kwargs=kwargs,
			job=job,
			records_df=records.toDF(),
			assign_combine_id=True
		)

		# run record validation scnearios if requested, using db_records from save_records() output
		vs = ValidationScenarioSpark(
			spark=spark,
			job=job,
			records_df=db_records,
			validation_scenarios = ast.literal_eval(kwargs['validation_scenarios'])
		)
		vs.run_record_validation_scenarios()

		# remove temporary payload directory if static job was upload based, not location on disk
		if kwargs['static_type'] == 'upload':
			shutil.rmtree(kwargs['static_payload'])

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
		Transform records based on Transformation Scenario.

		Args:
			spark (pyspark.sql.session.SparkSession): provided by pyspark context
			kwargs:
				job_id (int): Job ID
				job_input (str): location of avro files on disk
				transformation_id (str): id of Transformation Scenario
				index_mapper (str): class name from core.spark.es, extending BaseMapper
				validation_scenarios (list): list of Validadtion Scenario IDs

		Returns:
			None
			- transforms records via XSL, writes new records to avro files on disk
			- indexes records into DB
			- map / flatten records and indexes to ES
		'''

		# refresh Django DB Connection
		refresh_django_db_connection()

		# get job
		job = Job.objects.get(pk=int(kwargs['job_id']))

		# start job_track instance, marking job start
		job_track = JobTrack(
			job_id = job.id
		)
		job_track.save()

		# read output from input job, filtering by job_id, grabbing Combine Record schema fields
		input_job = Job.objects.get(pk=int(kwargs['input_job_id']))
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
		records = sqldf.filter(sqldf.job_id == int(kwargs['input_job_id']))

		# repartition
		records = records.repartition(settings.SPARK_REPARTITION)

		# get transformation
		transformation = Transformation.objects.get(pk=int(kwargs['transformation_id']))

		# if xslt type transformation
		if transformation.transformation_type == 'xslt':
			records_trans = TransformSpark.transform_xslt(spark, kwargs, job, transformation, records)

		# if python type transformation
		if transformation.transformation_type == 'python':
			records_trans = TransformSpark.transform_python(spark, kwargs, job, transformation, records)

		# convert back to DataFrame
		records_trans = records_trans.toDF()

		# run record identifier transformations (rits) if present
		records = run_rits(records, kwargs.get('rits', False))

		# index records to DB and index to ElasticSearch
		db_records = save_records(
			spark=spark,
			kwargs=kwargs,
			job=job,
			records_df=records_trans
		)

		# run record validation scnearios if requested, using db_records from save_records() output
		vs = ValidationScenarioSpark(
			spark=spark,
			job=job,
			records_df=db_records,
			validation_scenarios = ast.literal_eval(kwargs['validation_scenarios'])
		)
		vs.run_record_validation_scenarios()

		# finally, update finish_timestamp of job_track instance
		job_track.finish_timestamp = datetime.datetime.now()
		job_track.save()


	@staticmethod
	def transform_xslt(spark, kwargs, job, transformation, records):

		'''		
		Method to transform records with XSLT, using pyjxslt server

		Args:
			spark (pyspark.sql.session.SparkSession): provided by pyspark context
			kwargs (dict): kwargs from parent job
			job: job from parent job
			transformation: Transformation Scenario from parent job
			records (pyspark.sql.DataFrame): DataFrame of records pre-transformation

		Return:
			records_trans (rdd): transformed records as RDD
		'''

		# define udf function for transformation
		def transform_xml_udf(job_id, row, xslt_string):

			# attempt transformation and save out put to 'document'
			try:
				
				# transform with pyjxslt gateway
				gw = pyjxslt.Gateway(6767)
				gw.add_transform('xslt_transform', xslt_string)
				result = gw.transform('xslt_transform', row.document)
				gw.drop_transform('xslt_transform')

				# set trans_result tuple
				trans_result = (result, '', 1)

			# catch transformation exception and save exception to 'error'
			except Exception as e:
				# set trans_result tuple
				trans_result = ('', str(e), 0)

			# return Row
			return Row(
					combine_id = row.combine_id,
					record_id = row.record_id,
					document = trans_result[0],
					error = trans_result[1],
					job_id = int(job_id),
					oai_set = row.oai_set,
					success = trans_result[2]
				)

		# get XSLT transformation as string		
		xslt_string = transformation.payload

		# transform via rdd.map and return
		job_id = job.id			
		records_trans = records.rdd.map(lambda row: transform_xml_udf(job_id, row, xslt_string))
		return records_trans


	@staticmethod
	def transform_python(spark, kwargs, job, transformation, records):

		'''
		Transform records via python code snippet.

		Required:
			- a function named `python_record_transformation(record)` in transformation.payload python code

		Args:
			spark (pyspark.sql.session.SparkSession): provided by pyspark context
			kwargs (dict): kwargs from parent job
			job: job from parent job
			transformation: Transformation Scenario from parent job
			records (pyspark.sql.DataFrame): DataFrame of records pre-transformation

		Return:
			records_trans (rdd): transformed records as RDD
		'''

		# define udf function for python transformation
		def transform_xml_udf(job_id, row, python_code):
			
			try:
				# prepare row as parsed document with PythonUDFRecord class
				prtb = PythonUDFRecord(row)

				# get python function from Transformation Scenario
				temp_pyts = ModuleType('temp_pyts')
				exec(python_code, temp_pyts.__dict__)

				# run transformation
				trans_result = temp_pyts.python_record_transformation(prtb)

				# convert any possible byte responses to string
				if type(trans_result[0]) == bytes:
					trans_result[0] = trans_result[0].decode('utf-8')
				if type(trans_result[1]) == bytes:
					trans_result[1] = trans_result[1].decode('utf-8')

			except Exception as e:
				# set trans_result tuple
				trans_result = ('', str(e), 0)

			# return Row
			return Row(
				combine_id = row.combine_id,
				record_id = row.record_id,
				document = trans_result[0],
				error = trans_result[1],
				job_id = int(job_id),
				oai_set = row.oai_set,
				success = trans_result[2]
			)

		# transform via rdd.map and return
		job_id = job.id			
		transformation_payload = transformation.payload
		records_trans = records.rdd.map(lambda row: transform_xml_udf(job_id, row, transformation_payload))
		return records_trans



class MergeSpark(object):

	'''
	Spark code for running Merge type jobs.  Also used for duplciation, analysis, and others.
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
				validation_scenarios (list): list of Validadtion Scenario IDs

		Returns:
			None
			- merges records from previous jobs, writes new aggregated records to avro files on disk
			- indexes records into DB
			- map / flatten records and indexes to ES
		'''

		# refresh Django DB Connection
		refresh_django_db_connection()

		# get job
		job = Job.objects.get(pk=int(kwargs['job_id']))

		# start job_track instance, marking job start
		job_track = JobTrack(
			job_id = job.id
		)
		job_track.save()

		# rehydrate list of input jobs
		input_jobs_ids = ast.literal_eval(kwargs['input_jobs_ids'])

		# get total range of id's from input jobs to help partition jdbc reader
		records_ids = []
		for input_job_id in input_jobs_ids:
			input_job_temp = Job.objects.get(pk=int(input_job_id))
			records = input_job_temp.get_records().order_by('id')
			start_id = records.first().id
			end_id = records.last().id
			records_ids += [start_id, end_id]
		records_ids.sort()		

		# get list of RDDs from input jobs
		sqldf = spark.read.jdbc(
				settings.COMBINE_DATABASE['jdbc_url'],
				'core_record',
				properties=settings.COMBINE_DATABASE,
				column='id',
				lowerBound=records_ids[0],
				upperBound=records_ids[-1],
				numPartitions=settings.JDBC_NUMPARTITIONS
			)
		input_jobs_dfs = []		
		for input_job_id in input_jobs_ids:

			# db
			job_df = sqldf.filter(sqldf.job_id == int(input_job_id))
			input_jobs_dfs.append(job_df)

		# create aggregate rdd of frames
		agg_rdd = sc.union([ df.rdd for df in input_jobs_dfs ])
		agg_df = spark.createDataFrame(agg_rdd, schema=input_jobs_dfs[0].schema)

		# repartition
		agg_df = agg_df.repartition(settings.SPARK_REPARTITION)

		# filter based on record validity
		agg_df = record_validity_valve(spark, agg_df, kwargs)

		# update job column, overwriting job_id from input jobs in merge
		job_id = job.id
		job_id_udf = udf(lambda record_id: job_id, IntegerType())
		agg_df = agg_df.withColumn('job_id', job_id_udf(agg_df.record_id))

		# run record identifier transformations (rits) if present
		agg_df = run_rits(agg_df, kwargs.get('rits', False))

		# if Analysis Job, do not write avro
		if job.job_type == 'AnalysisJob':
			write_avro = False

		# index records to DB and index to ElasticSearch
		db_records = save_records(
			spark=spark,
			kwargs=kwargs,
			job=job,
			records_df=agg_df			
		)

		# run record validation scnearios if requested, using db_records from save_records() output
		vs = ValidationScenarioSpark(
			spark=spark,
			job=job,
			records_df=db_records,
			validation_scenarios = ast.literal_eval(kwargs['validation_scenarios'])
		)
		vs.run_record_validation_scenarios()

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
			- copies records in DB from input job to new published job
			- copies documents in ES from input to new published job index
		'''

		# refresh Django DB Connection
		refresh_django_db_connection()

		# get job
		job = Job.objects.get(pk=int(kwargs['job_id']))

		# start job_track instance, marking job start
		job_track = JobTrack(
			job_id = job.id
		)
		job_track.save()

		# read output from input job, filtering by job_id, grabbing Combine Record schema fields
		input_job = Job.objects.get(pk=int(kwargs['input_job_id']))
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
		records = sqldf.filter(sqldf.job_id == int(kwargs['input_job_id']))

		# repartition
		records = records.repartition(settings.SPARK_REPARTITION)

		# get rows with document content
		records = records[records['document'] != '']

		# update job column, overwriting job_id from input jobs in merge
		job_id = job.id
		job_id_udf = udf(lambda record_id: job_id, IntegerType())
		records = records.withColumn('job_id', job_id_udf(records.record_id))

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

		# index records to DB and index to ElasticSearch
		db_records = save_records(
			spark=spark,
			kwargs=kwargs,
			job=job,
			records_df=records,
			write_avro=False,
			index_records=False
		)

		# copy index from input job to new Publish job
		index_to_job_index = ESIndex.copy_es_index(
			source_index = 'j%s' % input_job.id,
			target_index = 'j%s' % job.id,
			wait_for_completion=False
		)

		# copy index from new Publish Job to /published index
		# NOTE: because back to back reindexes, and problems with timeouts on requests,
		# wait on task from previous reindex
		es_handle_temp = Elasticsearch(hosts=[settings.ES_HOST])
		retry = 1
		while retry <= 100:

			# get task
			task = es_handle_temp.tasks.get(index_to_job_index['task'])

			# if task complete, index job index to published index
			if task['completed']:
				index_to_published_index = ESIndex.copy_es_index(
					source_index = 'j%s' % job.id,
					target_index = 'published',
					wait_for_completion = False,
					add_copied_from = job_id # do not use Job instance here, only pass string
				)
				break # break from retry loop

			else:
				print("indexing to /published, waiting on task node %s, retry: %s/10" % (task['task']['node'], retry))

				# bump retries, sleep, and continue
				retry += 1
				time.sleep(3)
				continue

		# get PublishedRecords handle
		pr = PublishedRecords()

		# set records from job as published		
		pr.set_published_field(job_id)

		# update uniqueness of all published records
		pr.update_published_uniqueness()

		# finally, update finish_timestamp of job_track instance
		job_track.finish_timestamp = datetime.datetime.now()
		job_track.save()



####################################################################
# Utility Functions 											   #
####################################################################

def save_records(spark=None, kwargs=None, job=None, records_df=None, write_avro=settings.WRITE_AVRO, index_records=True, assign_combine_id=False):

	'''
	Function to index records to DB and trigger indexing to ElasticSearch (ES)		

	Args:
		spark (pyspark.sql.session.SparkSession): spark instance from static job methods
		kwargs (dict): dictionary of args sent to Job spark method
		job (core.models.Job): Job instance		
		records_df (pyspark.sql.DataFrame): records as pyspark DataFrame
		write_avro (bool): boolean to write avro files to disk after DB indexing
		index_records (bool): boolean to index to ES
		assign_combine_id (bool): if True, establish `combine_id` column and populate with UUID

	Returns:
		None
			- determines if record_id unique among records DataFrame
			- selects only columns that match CombineRecordSchema
			- writes to DB, writes to avro files
	'''

	# assign combine ID
	if assign_combine_id:
		combine_id_udf = udf(lambda record_id: str(uuid.uuid4()), StringType())
		records_df = records_df.withColumn('combine_id', combine_id_udf(records_df.record_id))

	# check uniqueness (overwrites if column already exists)
	'''
	What is the performance hit of this uniqueness check?
	'''
	records_df = records_df.withColumn("unique", (
		pyspark_sql_functions.count('record_id')\
		.over(Window.partitionBy('record_id')) == 1)\
		.cast('integer'))

	# ensure columns to avro and DB
	records_df_combine_cols = records_df.select(CombineRecordSchema().field_names)

	# write avro, coalescing for output
	if write_avro:
		records_df_combine_cols.coalesce(settings.SPARK_REPARTITION)\
		.write.format("com.databricks.spark.avro").save(job.job_output)

	# write records to DB
	records_df_combine_cols.write.jdbc(
		settings.COMBINE_DATABASE['jdbc_url'],
		'core_record',
		properties=settings.COMBINE_DATABASE,
		mode='append')

	# read rows from DB for indexing to ES and writing avro
	bounds = get_job_db_bounds(job)
	sqldf = spark.read.jdbc(
			settings.COMBINE_DATABASE['jdbc_url'],
			'core_record',
			properties=settings.COMBINE_DATABASE,
			column='id',
			lowerBound=bounds['lowerBound'],
			upperBound=bounds['upperBound'],
			numPartitions=settings.SPARK_REPARTITION
		)
	job_id = job.id
	db_records = sqldf.filter(sqldf.job_id == job_id).filter(sqldf.success == 1)

	# index to ElasticSearch
	if index_records and settings.INDEX_TO_ES:
		ESIndex.index_job_to_es_spark(
			spark,
			job=job,
			records_df=db_records,
			index_mapper=kwargs['index_mapper']
		)

	# return db_records for later use
	return db_records


def get_job_db_bounds(job):

	'''
	Function to determine lower and upper bounds for job IDs, for more efficient MySQL retrieval
	'''	

	records = job.get_records()
	records = records.order_by('id')
	start_id = records.first().id
	end_id = records.last().id

	return {
		'lowerBound':start_id,
		'upperBound':end_id
	}


def run_rits(records_df, rits_id):

	'''
	Function to run Record Identifier Transformation Scenarios (rits) if present.

	RITS can be of three types:
		1) 'regex' - Java Regular Expressions
		2) 'python' - Python Code Snippets
		3) 'xpath' - XPath expression

	Each are handled differently, but all strive to return a dataframe (records_df),
	with the `record_id` column modified.

	Args:
		records_df (pyspark.sql.DataFrame): records as pyspark DataFrame
		rits_id (string|int): DB identifier of pre-configured RITS

	Returns:
		
	'''
	
	# if rits id provided
	if rits_id and rits_id != None:

		rits = RecordIdentifierTransformationScenario.objects.get(pk=int(rits_id))

		# handle regex
		if rits.transformation_type == 'regex':

				# define udf function for python transformation
			def regex_record_id_trans_udf(row, match, replace, trans_target):
				
				try:
					
					# use python's re module to perform regex
					if trans_target == 'record_id':
						trans_result = re.sub(match, replace, row.record_id)
					if trans_target == 'document':
						trans_result = re.sub(match, replace, row.document)

					# run transformation
					success = 1
					error = row.error

				except Exception as e:
					trans_result = str(e)
					error = 'record_id transformation failure'
					success = 0

				# return Row
				return Row(
					combine_id = row.combine_id,
					record_id = trans_result,
					document = row.document,
					error = error,
					job_id = row.job_id,
					oai_set = row.oai_set,
					success = success
				)

			# transform via rdd.map and return			
			match = rits.regex_match_payload
			replace = rits.regex_replace_payload
			trans_target = rits.transformation_target
			records_rdd = records_df.rdd.map(lambda row: regex_record_id_trans_udf(row, match, replace, trans_target))
			records_df = records_rdd.toDF()

		# handle python
		if rits.transformation_type == 'python':	

			# define udf function for python transformation
			def python_record_id_trans_udf(row, python_code, trans_target):
				
				try:
					# get python function from Transformation Scenario
					temp_mod = ModuleType('temp_mod')
					exec(python_code, temp_mod.__dict__)

					# establish python udf record
					if trans_target == 'record_id':
						pyudfr = PythonUDFRecord(None, non_row_input = True, record_id = row.record_id)
					if trans_target == 'document':
						pyudfr = PythonUDFRecord(None, non_row_input = True, document = row.document)

					# run transformation
					trans_result = temp_mod.transform_identifier(pyudfr)
					success = 1
					error = row.error

				except Exception as e:
					trans_result = str(e)
					error = 'record_id transformation failure'
					success = 0

				# return Row
				return Row(
					combine_id = row.combine_id,
					record_id = trans_result,
					document = row.document,
					error = error,
					job_id = row.job_id,
					oai_set = row.oai_set,
					success = success
				)

			# transform via rdd.map and return			
			python_code = rits.python_payload
			trans_target = rits.transformation_target
			records_rdd = records_df.rdd.map(lambda row: python_record_id_trans_udf(row, python_code, trans_target))
			records_df = records_rdd.toDF()

		# handle xpath
		if rits.transformation_type == 'xpath':

			'''
			Currently XPath RITS are handled via python and etree,
			but might be worth investigating if this could be performed
			via pyjxslt to support XPath 2.0
			'''
			
			# define udf function for xpath expression
			def xpath_record_id_trans_udf(row, xpath):				

				# establish python udf record, forcing 'document' type trans for XPath				
				pyudfr = PythonUDFRecord(None, non_row_input = True, document = row.document)

				# run xpath and retrieve value
				xpath_query = pyudfr.xml.xpath(xpath, namespaces=pyudfr.nsmap)
				if len(xpath_query) == 1:
					trans_result = xpath_query[0].text
					success = 1
					error = row.error
				elif len(xpath_query) == 0:
					trans_result = 'xpath expression found nothing'
					success = 0
					error = 'record_id transformation failure'
				else:
					trans_result = 'more than one node found for XPath query'
					success = 0
					error = 'record_id transformation failure'

				# return Row
				return Row(
					combine_id = row.combine_id,
					record_id = trans_result,
					document = row.document,
					error = error,
					job_id = row.job_id,
					oai_set = row.oai_set,
					success = success
				)

			# transform via rdd.map and return			
			xpath = rits.xpath_payload			
			records_rdd = records_df.rdd.map(lambda row: xpath_record_id_trans_udf(row, xpath))
			records_df = records_rdd.toDF()
		
		# return
		return records_df

	# else return dataframe untouched
	else:
		return records_df


def record_validity_valve(spark, records_df, kwargs):

	'''
	Helper function to include all, valid, or invalid records only 

	Args:
		spark (pyspark.sql.session.SparkSession): provided by pyspark context
		records_df (pyspark.sql.DataFrame): DataFrame of records pre validity filtering
		kwargs (dict): kwargs

	Returns:
		(pyspark.sql.DataFrame): DataFrame of records post validity filtering
	'''

	# if all, return untouched
	if kwargs['input_validity_valve'] == 'all':
		filtered_df = records_df

	# else, returning valid or invalid
	else:

		# prepare record validations as dataframe, selecting only distinct record_ids
		rvs = RecordValidation.objects.all().order_by('id')
		rv_df = spark.read.jdbc(
				settings.COMBINE_DATABASE['jdbc_url'],
				'core_recordvalidation',
				properties=settings.COMBINE_DATABASE,
				column='id',
				lowerBound=rvs.first().id,
				upperBound=rvs.last().id,
				numPartitions=settings.JDBC_NUMPARTITIONS
			).select('record_id').distinct()

		# return valid records		
		if kwargs['input_validity_valve'] == 'valid':
			filtered_df = records_df.join(rv_df, records_df.id == rv_df.record_id, 'leftanti').select(records_df['*'])

		# return invalid records		
		if kwargs['input_validity_valve'] == 'invalid':
			filtered_df = records_df.join(rv_df, records_df.id == rv_df.record_id, 'leftsemi').select(records_df['*'])

	# return
	return filtered_df

