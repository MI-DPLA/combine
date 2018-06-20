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
import textwrap
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
from pyspark.sql.functions import udf, regexp_replace, lit, crc32
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
from core.models import CombineJob, Job, JobTrack, Transformation, PublishedRecords, RecordIdentifierTransformationScenario, RecordValidation, DPLABulkDataDownload



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
				StructField('success', BooleanType(), False),
				StructField('fingerprint', IntegerType(), False),
				StructField('transformed', BooleanType(), False)				
			]
		)

		# fields
		self.field_names = [f.name for f in self.schema.fields if f.name != 'id']



####################################################################
# Spark Jobs 		 											   #
####################################################################

class CombineSparkJob(object):

	'''
	Base class for Combine Spark Jobs.
	Provides some usuable components for jobs.
	'''


	def __init__(self, spark, **kwargs):

		self.spark = spark

		self.kwargs = kwargs

		# init logging support
		spark.sparkContext.setLogLevel('INFO')
		log4jLogger = spark.sparkContext._jvm.org.apache.log4j
		self.logger = log4jLogger.LogManager.getLogger(__name__)


	def init_job(self):

		# refresh Django DB Connection
		refresh_django_db_connection()

		# get job
		self.job = Job.objects.get(pk=int(self.kwargs['job_id']))

		# start job_track instance, marking job start
		self.job_track = JobTrack(
			job_id = self.job.id
		)
		self.job_track.save()


	def close_job(self):

		# finally, update finish_timestamp of job_track instance
		self.job_track.finish_timestamp = datetime.datetime.now()
		self.job_track.save()


	def save_records(self, records_df=None, write_avro=settings.WRITE_AVRO, index_records=settings.INDEX_TO_ES, assign_combine_id=False):

		'''
		Method to index records to DB and trigger indexing to ElasticSearch (ES)		

		Args:				
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

		# run record identifier transformation scenario if provided
		records_df = self.run_rits(records_df)

		# check uniqueness (overwrites if column already exists)	
		records_df = records_df.withColumn("unique", (
			pyspark_sql_functions.count('record_id')\
			.over(Window.partitionBy('record_id')) == 1)\
			.cast('integer'))

		# ensure columns to avro and DB
		records_df_combine_cols = records_df.select(CombineRecordSchema().field_names)

		# write avro, coalescing for output
		if write_avro:
			records_df_combine_cols.coalesce(settings.SPARK_REPARTITION)\
			.write.format("com.databricks.spark.avro").save(self.job.job_output)

		# write records to DB
		records_df_combine_cols.write.jdbc(
			settings.COMBINE_DATABASE['jdbc_url'],
			'core_record',
			properties=settings.COMBINE_DATABASE,
			mode='append')

		# check if anything written to DB to continue, else abort
		if len(self.job.get_records()) > 0:

			# read rows from DB for indexing to ES
			bounds = self.get_job_db_bounds(self.job)
			sqldf = self.spark.read.jdbc(
					settings.COMBINE_DATABASE['jdbc_url'],
					'core_record',
					properties=settings.COMBINE_DATABASE,
					column='id',
					lowerBound=bounds['lowerBound'],
					upperBound=bounds['upperBound'],
					numPartitions=settings.SPARK_REPARTITION
				)
			job_id = self.job.id
			db_records = sqldf.filter(sqldf.job_id == job_id).filter(sqldf.success == 1)

			# index to ElasticSearch
			if index_records and settings.INDEX_TO_ES:
				ESIndex.index_job_to_es_spark(
					self.spark,
					job=self.job,
					records_df=db_records,
					index_mapper=self.kwargs['index_mapper'],
					include_attributes=self.kwargs['include_attributes']
				)

			# run Validation Scenarios
			if 'validation_scenarios' in self.kwargs.keys():
				vs = ValidationScenarioSpark(
					spark=self.spark,
					job=self.job,
					records_df=db_records,
					validation_scenarios = ast.literal_eval(self.kwargs['validation_scenarios'])
				)
				vs.run_record_validation_scenarios()

			# update `valid` column for Records based on results of ValidationScenarios
			cursor = connection.cursor()
			query_results = cursor.execute("UPDATE core_record AS r LEFT OUTER JOIN core_recordvalidation AS rv ON r.id = rv.record_id SET r.valid = (SELECT IF(rv.id,0,1)) WHERE r.job_id = %s" % self.job.id)

			# run comparison against bulk data if provided
			db_records = self.bulk_data_compare(db_records)

			# return db_records DataFrame
			return db_records

		else:		
			raise Exception("Nothing written to disk for Job: %s" % self.job.name)


	def get_job_db_bounds(self, job):

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


	def record_validity_valve(self, records_df):

		'''
		Method to include all, valid, or invalid records only for downstream jobs

		Args:
			spark (pyspark.sql.session.SparkSession): provided by pyspark context
			records_df (pyspark.sql.DataFrame): DataFrame of records pre validity filtering
			kwargs (dict): kwargs

		Returns:
			(pyspark.sql.DataFrame): DataFrame of records post validity filtering
		'''

		# if all, return untouched
		if self.kwargs['input_validity_valve'] == 'all':
			filtered_df = records_df

		# else, filter to valid or invalid records
		else:
			# return valid records		
			if self.kwargs['input_validity_valve'] == 'valid':
				filtered_df = records_df.filter(records_df.valid == 1)

			# return invalid records		
			if self.kwargs['input_validity_valve'] == 'invalid':
				filtered_df = records_df.filter(records_df.valid == 0)

		# return
		return filtered_df


	def run_rits(self, records_df):

		'''
		Method to run Record Identifier Transformation Scenarios (rits) if present.

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

		# get rits ID from kwargs
		rits_id = self.kwargs.get('rits', False)
		
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


	def bulk_data_compare(self, db_records):

		'''
		Method to compare against bulk data if provided
		'''

		self.logger.info('running bulk data compare')

		# get dbdd instance
		# get dbdd ID from kwargs
		dbdd_id = self.kwargs.get('dbdd', False)

		# if rits id provided
		if dbdd_id and dbdd_id != None:

			self.logger.info('DBDD id provided, retrieving and running...')

			# get dbdd instance
			dbdd = DPLABulkDataDownload.objects.get(pk=int(dbdd_id))
			self.logger.info('DBDD retrieved: %s @ ES index %s' % (dbdd.s3_key, dbdd.es_index))

			# get bulk data as DF from ES
			dpla_rdd = self.spark.sparkContext.newAPIHadoopRDD(
				inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
				keyClass="org.apache.hadoop.io.NullWritable",
				valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
				conf={ "es.resource" : "%s/item" % dbdd.es_index }
			)
			dpla_df = dpla_rdd.toDF()			

			# check if records from job are in bulk data DF
			in_dpla = db_records.join(dpla_df, db_records['record_id'] == dpla_df['_1'], 'leftsemi').select(db_records['id'])

			# prepare matches DF
			dbdd_id = dbdd.id
			matches_to_write = in_dpla.withColumn('record_id', in_dpla['id']).withColumn('match', lit(1)).withColumn('dbdd_id', lit(dbdd_id)).select('match', 'record_id', 'dbdd_id')
			
			# write to DPLABulkDataMatch table
			matches_to_write.write.jdbc(
				settings.COMBINE_DATABASE['jdbc_url'],
				'core_dplabulkdatamatch',
				properties=settings.COMBINE_DATABASE,
				mode='append'
			)

			# return
			return db_records

		# else, return untouched
		else:
			return db_records


	def fingerprint_records(self, df):

		'''
		Method to generate a crc32 hash "fingerprint" for each Record
		'''

		# fingerprint Record document
		df = df.withColumn('fingerprint', crc32(df.document))
		return df



class HarvestOAISpark(CombineSparkJob):

	'''
	Spark code for harvesting OAI records
	'''
	
	def spark_function(self):

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

		# init job
		self.init_job()

		# harvest OAI records via Ingestion3
		df = self.spark.read.format("dpla.ingestion3.harvesters.oai")\
		.option("endpoint", self.kwargs['endpoint'])\
		.option("verb", self.kwargs['verb'])\
		.option("metadataPrefix", self.kwargs['metadataPrefix'])\
		.option(self.kwargs['scope_type'], self.kwargs['scope_value'])\
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
		job_id = self.job.id
		job_id_udf = udf(lambda id: job_id, IntegerType())
		records = records.withColumn('job_id', job_id_udf(records.id))

		# add oai_set
		records = records.withColumn('oai_set', records.setIds[0])

		# add blank error column
		error = udf(lambda id: '', StringType())
		records = records.withColumn('error', error(records.id))

		# fingerprint records and set transformed
		records = self.fingerprint_records(records)
		records = records.withColumn('transformed', pyspark_sql_functions.lit(1))

		# index records to DB and index to ElasticSearch
		self.save_records(			
			records_df=records,
			assign_combine_id=True
		)
		
		# close job
		self.close_job()



class HarvestStaticXMLSpark(CombineSparkJob):

	'''
	Spark code for harvesting static xml records
	'''

	def spark_function(self):

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

		# init job
		self.init_job()

		# use Spark-XML's XmlInputFormat to stream globbed files, parsing with user provided `document_element_root`
		static_rdd = self.spark.sparkContext.newAPIHadoopFile(
			'file://%s/**' % self.kwargs['static_payload'].rstrip('/'),
			'com.databricks.spark.xml.XmlInputFormat',
			'org.apache.hadoop.io.LongWritable',
			'org.apache.hadoop.io.Text',
			conf = {
				'xmlinput.start':'<%s>' % self.kwargs['document_element_root'],
				'xmlinput.end':'</%s>' % self.kwargs['document_element_root'],
				'xmlinput.encoding': 'utf-8'
			}
		)

		
		# parse namespaces
		def get_namespaces(xml_node):
			nsmap = {}
			for ns in xml_node.xpath('//namespace::*'):
				if ns[0]:
					nsmap[ns[0]] = ns[1]
			return nsmap


		def parse_records_udf(job_id, row, kwargs):

			# get doc string
			doc_string = row[1]

			# if optional (additional) namespace declaration provided, use
			if kwargs['additional_namespace_decs']:
				doc_string = re.sub(
					ns_regex,
					r'<%s %s>' % (kwargs['document_element_root'], kwargs['additional_namespace_decs']),
					doc_string
				)	

			try:

				# parse with lxml
				try:
					xml_root = etree.fromstring(doc_string.encode('utf-8'))
				except Exception as e:
					raise Exception('Could not parse record XML: %s' % str(e))

				# get namespaces
				nsmap = get_namespaces(xml_root)

				# get unique identifier
				if kwargs['xpath_record_id'] != '':
					record_id = xml_root.xpath(kwargs['xpath_record_id'], namespaces=nsmap)
					if len(record_id) == 1:
						record_id = record_id[0].text
					elif len(xml_root) > 1:
						raise AmbiguousIdentifier('multiple elements found for identifier xpath: %s' % kwargs['xpath_record_id'])
					elif len(xml_root) == 0:
						raise AmbiguousIdentifier('no elements found for identifier xpath: %s' % kwargs['xpath_record_id'])
				else:
					record_id = hashlib.md5(doc_string.encode('utf-8')).hexdigest()

				# return success Row
				return Row(
					record_id = record_id,
					document = etree.tostring(xml_root).decode('utf-8'),
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
					document = etree.tostring(xml_root).decode('utf-8'),
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

		# map with parse_records_udf 
		job_id = self.job.id
		kwargs = self.kwargs
		ns_regex = re.compile(r'<%s(.?|.+?)>' % self.kwargs['document_element_root'])
		records = static_rdd.map(lambda row: parse_records_udf(job_id, row, kwargs))

		# convert back to DF
		records = records.toDF()

		# fingerprint records and set transformed
		records = self.fingerprint_records(records)
		records = records.withColumn('transformed', pyspark_sql_functions.lit(1))

		# index records to DB and index to ElasticSearch
		self.save_records(			
			records_df=records,
			assign_combine_id=True
		)

		# remove temporary payload directory if static job was upload based, not location on disk
		if self.kwargs['static_type'] == 'upload':
			shutil.rmtree(self.kwargs['static_payload'])

		# close job
		self.close_job()



class TransformSpark(CombineSparkJob):

	'''
	Spark code for Transform jobs
	'''
	
	def spark_function(self):

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

		# init job
		self.init_job()

		# read output from input job, filtering by job_id, grabbing Combine Record schema fields
		input_job = Job.objects.get(pk=int(self.kwargs['input_job_id']))
		bounds = self.get_job_db_bounds(input_job)
		sqldf = self.spark.read.jdbc(
				settings.COMBINE_DATABASE['jdbc_url'],
				'core_record',
				properties=settings.COMBINE_DATABASE,
				column='id',
				lowerBound=bounds['lowerBound'],
				upperBound=bounds['upperBound'],
				numPartitions=settings.JDBC_NUMPARTITIONS
			)
		records = sqldf.filter(sqldf.job_id == int(self.kwargs['input_job_id']))

		# fork as input_records
		input_records = records

		# filter based on record validity
		records = self.record_validity_valve(records)

		# repartition
		records = records.repartition(settings.SPARK_REPARTITION)

		# get transformation
		transformation = Transformation.objects.get(pk=int(self.kwargs['transformation_id']))

		# if xslt type transformation
		if transformation.transformation_type == 'xslt':
			records_trans = self.transform_xslt(transformation, records)

		# if python type transformation
		if transformation.transformation_type == 'python':
			records_trans = self.transform_python(transformation, records)

		# if OpenRefine type transformation
		if transformation.transformation_type == 'openrefine':
			records_trans = self.transform_openrefineactions(transformation, records)

		# convert back to DataFrame
		records_trans = records_trans.toDF()

		# fingerprint Record document
		records_trans = self.fingerprint_records(records_trans)

		# write `transformed` column based on new fingerprint
		records_trans = records_trans.alias("records_trans").join(input_records.alias("input_records"), input_records.fingerprint == records_trans.fingerprint, 'left').select(*['records_trans.%s' % c for c in records_trans.columns if c not in ['transformed']], pyspark_sql_functions.when(pyspark_sql_functions.isnull(pyspark_sql_functions.col('input_records.fingerprint')), pyspark_sql_functions.lit(True)).otherwise(pyspark_sql_functions.lit(False)).alias('transformed'))


		# index records to DB and index to ElasticSearch
		self.save_records(			
			records_df=records_trans
		)

		# close job
		self.close_job()


	def transform_xslt(self, transformation, records):

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

		def transform_xslt_pt_udf(pt):

			# transform with pyjxslt gateway
			gw = pyjxslt.Gateway(6767)
			gw.add_transform('xslt_transform', xslt_string)

			# loop through rows in partition
			for row in pt:
				
				try:
					result = gw.transform('xslt_transform', row.document)
					# attempt XML parse to confirm well-formedness
					# error will bubble up in try/except
					valid_xml = etree.fromstring(result.encode('utf-8'))

					# set trans_result tuple
					trans_result = (result, '', 1)

				# catch transformation exception and save exception to 'error'
				except Exception as e:
					# set trans_result tuple
					trans_result = ('', str(e), 0)

				# yield each Row in mapPartition
				yield Row(
					combine_id = row.combine_id,
					record_id = row.record_id,
					document = trans_result[0],
					error = trans_result[1],
					job_id = int(job_id),
					oai_set = row.oai_set,
					success = trans_result[2],
					fingerprint = row.fingerprint,
					transformed = row.transformed
				)

			# drop transform
			gw.drop_transform('xslt_transform')

		# get XSLT transformation as string		
		xslt_string = transformation.payload

		# transform via rdd.map and return
		job_id = self.job.id

		# perform transformations a la mapPartitions
		records_trans = records.rdd.mapPartitions(transform_xslt_pt_udf)
		return records_trans


	def transform_python(self, transformation, records):

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
		def transform_python_pt_udf(pt):

			# get python function from Transformation Scenario
			temp_pyts = ModuleType('temp_pyts')
			exec(python_code, temp_pyts.__dict__)

			for row in pt:

				try:

					# prepare row as parsed document with PythonUDFRecord class
					prtb = PythonUDFRecord(row)

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
				yield Row(
					combine_id = row.combine_id,
					record_id = row.record_id,
					document = trans_result[0],
					error = trans_result[1],
					job_id = int(job_id),
					oai_set = row.oai_set,
					success = trans_result[2],
					fingerprint = row.fingerprint,
					transformed = row.transformed
				)

		# transform via rdd.mapPartitions and return
		job_id = self.job.id			
		python_code = transformation.payload
		records_trans = records.rdd.mapPartitions(transform_python_pt_udf)
		return records_trans


	def transform_openrefineactions(self, transformation, records):

		'''
		Transform records per OpenRefine Actions JSON		

		Args:
			spark (pyspark.sql.session.SparkSession): provided by pyspark context
			kwargs (dict): kwargs from parent job
			job: job from parent job
			transformation: Transformation Scenario from parent job
			records (pyspark.sql.DataFrame): DataFrame of records pre-transformation

		Return:
			records_trans (rdd): transformed records as RDD
		'''

		def _field_name_to_xpath(field_name):

			# for each column, reconstitue columnName --> XPath				
			field_parts = field_name.split('_')[1:] # skip root element

			# loop through pieces and build xpath
			on_attrib = False
			xpath = '/' # begin with single slash, will get appended to

			for part in field_parts:

				# if not attribute, assume node hop
				if not part.startswith('@'):

					# handle closing attrib if present
					if on_attrib:
						xpath += ']/'

					# close previous element
					else:
						xpath += '/'
				
					# replace pipe with colon for prefix
					part = part.replace('|',':')

					# append to xpath string
					xpath += '%s' % part

				# if attribute, assume part of previous element and build
				else:

					# handle attribute
					attrib, value = part.split('=')

					# if not on_attrib, open xpath for attribute inclusion
					if not on_attrib:
						xpath += "[%s='%s'" % (attrib, value)

					# else, currently in attribute write block, continue
					else:
						xpath += " and %s='%s'" % (attrib, value)

					# set on_attrib flag for followup
					on_attrib = True

			# return 
			return xpath


		# define udf function for python transformation	
		def transform_openrefine_pt_udf(pt):

			# parse OpenRefine actions JSON
			or_actions = json.loads(or_actions_json)

			# loop through rows
			for row in pt:

				try:

					# prepare row as parsed document with PythonUDFRecord class
					prtb = PythonUDFRecord(row)

					# loop through actions
					for event in or_actions:

						# handle mass edits
						if event['op'] == 'core/mass-edit':

							# for each column, reconstitue columnName --> XPath	
							xpath = _field_name_to_xpath(event['columnName'])
							
							# find elements for potential edits
							eles = prtb.xml.xpath(xpath, namespaces=prtb.nsmap)

							# loop through elements
							for ele in eles:				

								# loop through edits
								for edit in event['edits']:

									# check if element text in from, change
									if ele.text in edit['from']:
										ele.text = edit['to']

						# handle jython
						if event['op'] == 'core/text-transform' and event['expression'].startswith('jython:'):					

							# fire up temp module
							temp_pyts = ModuleType('temp_pyts')

							# parse code
							code = event['expression'].split('jython:')[1]

							# wrap in function and write to temp module
							code = 'def temp_func(value):\n%s' % textwrap.indent(code, prefix='    ')					
							exec(code, temp_pyts.__dict__)

							# get xpath (unique to action, can't pre learn)
							xpath = _field_name_to_xpath(event['columnName'])
							
							# find elements for potential edits
							eles = prtb.xml.xpath(xpath, namespaces=prtb.nsmap)

							# loop through elements
							for ele in eles:
								ele.text = temp_pyts.temp_func(ele.text)

					# re-serialize as trans_result
					trans_result = (etree.tostring(prtb.xml).decode('utf-8'), '', 1)

				except Exception as e:
					# set trans_result tuple
					trans_result = ('', str(e), 0)

				# return Row
				yield Row(
					combine_id = row.combine_id,
					record_id = row.record_id,
					document = trans_result[0],
					error = trans_result[1],
					job_id = int(job_id),
					oai_set = row.oai_set,
					success = trans_result[2],
					fingerprint = row.fingerprint,
					transformed = row.transformed
				)


		# transform via rdd.mapPartitions and return
		job_id = self.job.id			
		or_actions_json = transformation.payload
		records_trans = records.rdd.mapPartitions(transform_openrefine_pt_udf)
		return records_trans



class MergeSpark(CombineSparkJob):

	'''
	Spark code for running Merge type jobs.  Also used for duplciation, analysis, and others.
	Note: Merge jobs merge only successful documents from an input job, not the errors
	'''

	def spark_function(self):

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

		# init job
		self.init_job()

		# rehydrate list of input jobs
		input_jobs_ids = ast.literal_eval(self.kwargs['input_jobs_ids'])

		# get total range of id's from input jobs to help partition jdbc reader
		records_ids = []
		for input_job_id in input_jobs_ids:
			input_job_temp = Job.objects.get(pk=int(input_job_id))

			# if job has records, continue
			if len(input_job_temp.get_records()) > 0:
				records = input_job_temp.get_records().order_by('id')
				start_id = records.first().id
				end_id = records.last().id
				records_ids += [start_id, end_id]
			else:
				print("Job %s had no records, skipping" % input_job_temp.name)

		records_ids.sort()		

		# get list of RDDs from input jobs
		sqldf = self.spark.read.jdbc(
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
		agg_rdd = self.spark.sparkContext.union([ df.rdd for df in input_jobs_dfs ])
		agg_df = self.spark.createDataFrame(agg_rdd, schema=input_jobs_dfs[0].schema)

		# filter based on record validity
		agg_df = self.record_validity_valve(agg_df)

		# repartition
		agg_df = agg_df.repartition(settings.SPARK_REPARTITION)

		# update job column, overwriting job_id from input jobs in merge
		job_id = self.job.id
		job_id_udf = udf(lambda record_id: job_id, IntegerType())
		agg_df = agg_df.withColumn('job_id', job_id_udf(agg_df.record_id))

		# set transformed column to False		
		agg_df = agg_df.withColumn('transformed', pyspark_sql_functions.lit(0))

		# if Analysis Job, do not write avro
		if self.job.job_type == 'AnalysisJob':
			write_avro = False
		else:
			write_avro = settings.WRITE_AVRO

		# index records to DB and index to ElasticSearch
		self.save_records(			
			records_df=agg_df,
			write_avro=write_avro
		)

		# close job
		self.close_job()



class PublishSpark(CombineSparkJob):

	'''
	Spark code for running Publish type jobs.	
	'''
	
	def spark_function(self):

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

		# init job
		self.init_job()

		# read output from input job, filtering by job_id, grabbing Combine Record schema fields
		input_job = Job.objects.get(pk=int(self.kwargs['input_job_id']))
		bounds = self.get_job_db_bounds(input_job)
		sqldf = self.spark.read.jdbc(
				settings.COMBINE_DATABASE['jdbc_url'],
				'core_record',
				properties=settings.COMBINE_DATABASE,
				column='id',
				lowerBound=bounds['lowerBound'],
				upperBound=bounds['upperBound'],
				numPartitions=settings.JDBC_NUMPARTITIONS
			)
		records = sqldf.filter(sqldf.job_id == int(self.kwargs['input_job_id']))

		# repartition
		records = records.repartition(settings.SPARK_REPARTITION)

		# get rows with document content
		records = records[records['document'] != '']

		# update job column, overwriting job_id from input jobs in merge
		job_id = self.job.id
		job_id_udf = udf(lambda record_id: job_id, IntegerType())
		records = records.withColumn('job_id', job_id_udf(records.record_id))

		# set transformed column to False		
		records = records.withColumn('transformed', pyspark_sql_functions.lit(0))

		# write job output to avro
		records.select(CombineRecordSchema().field_names).write.format("com.databricks.spark.avro").save(self.job.job_output)

		# confirm directory exists
		published_dir = '%s/published' % (settings.BINARY_STORAGE.split('file://')[-1].rstrip('/'))
		if not os.path.exists(published_dir):
			os.mkdir(published_dir)

		# get avro files
		job_output_dir = self.job.job_output.split('file://')[-1]
		avros = [f for f in os.listdir(job_output_dir) if f.endswith('.avro')]
		for avro in avros:
			os.symlink(os.path.join(job_output_dir, avro), os.path.join(published_dir, avro))

		# index records to DB and index to ElasticSearch
		self.save_records(			
			records_df=records,
			write_avro=False,
			index_records=False
		)

		# copy index from input job to new Publish job
		index_to_job_index = ESIndex.copy_es_index(
			source_index = 'j%s' % input_job.id,
			target_index = 'j%s' % self.job.id,
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
					source_index = 'j%s' % self.job.id,
					target_index = 'published',
					wait_for_completion = False,
					add_copied_from = job_id # do not use Job instance here, only pass string
				)
				break # break from retry loop

			else:
				print("indexing to /published, waiting on task node %s, retry: %s/100" % (task['task']['node'], retry))

				# bump retries, sleep, and continue
				retry += 1
				time.sleep(3)
				continue

		# get PublishedRecords handle
		pr = PublishedRecords()

		# set records from job as published		
		job_id = self.job.id
		pr.set_published_field(job_id)

		# update uniqueness of all published records
		pr.update_published_uniqueness()

		# close job
		self.close_job()



####################################################################
# Combine Spark Patches											   #
####################################################################


class CombineSparkPatch(object):

	'''
	Base class for Combine Spark Patches.
		- these are considered categorically "secondary" to the main
		CombineSparkJobs above, but may be just as long running
	'''


	def __init__(self, spark, **kwargs):

		self.spark = spark

		self.kwargs = kwargs

		# init logging support
		spark.sparkContext.setLogLevel('INFO')
		log4jLogger = spark.sparkContext._jvm.org.apache.log4j
		self.logger = log4jLogger.LogManager.getLogger(__name__)


	def get_job_db_bounds(self, job):

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



class ReindexSparkPatch(CombineSparkPatch):

	'''
	Class to handle Job re-indexing

	Args:
		kwargs(dict):
			- job_id (int): ID of Job to reindex
	'''

	def spark_function(self):

		# get job and set to self
		self.job = Job.objects.get(pk=int(self.kwargs['job_id']))

		# get records from job as DF
		bounds = self.get_job_db_bounds(self.job)
		sqldf = self.spark.read.jdbc(
				settings.COMBINE_DATABASE['jdbc_url'],
				'core_record',
				properties=settings.COMBINE_DATABASE,
				column='id',
				lowerBound=bounds['lowerBound'],
				upperBound=bounds['upperBound'],
				numPartitions=settings.JDBC_NUMPARTITIONS
			)
		db_records = sqldf.filter(sqldf.job_id == int(self.kwargs['job_id']))

		# reindex
		ESIndex.index_job_to_es_spark(
			self.spark,
			job=self.job,
			records_df=db_records,
			index_mapper=self.kwargs['index_mapper'],
			include_attributes=self.kwargs['include_attributes']
		)



class RunNewValidationsSpark(CombineSparkPatch):

	'''
	Class to handle Job re-indexing

	Args:
		kwargs(dict):
			- job_id (int): ID of Job to reindex
	'''

	def spark_function(self):

		# get job and set to self
		self.job = Job.objects.get(pk=int(self.kwargs['job_id']))

		# get records from job as DF
		bounds = self.get_job_db_bounds(self.job)
		sqldf = self.spark.read.jdbc(
				settings.COMBINE_DATABASE['jdbc_url'],
				'core_record',
				properties=settings.COMBINE_DATABASE,
				column='id',
				lowerBound=bounds['lowerBound'],
				upperBound=bounds['upperBound'],
				numPartitions=settings.JDBC_NUMPARTITIONS
			)
		db_records = sqldf.filter(sqldf.job_id == int(self.kwargs['job_id']))

		# run Validation Scenarios
		if 'validation_scenarios' in self.kwargs.keys():
			vs = ValidationScenarioSpark(
				spark=self.spark,
				job=self.job,
				records_df=db_records,
				validation_scenarios = ast.literal_eval(self.kwargs['validation_scenarios'])
			)
			vs.run_record_validation_scenarios()

		# update `valid` column for Records based on results of ValidationScenarios
		cursor = connection.cursor()
		query_results = cursor.execute("UPDATE core_record AS r LEFT OUTER JOIN core_recordvalidation AS rv ON r.id = rv.record_id SET r.valid = (SELECT IF(rv.id,0,1)) WHERE r.job_id = %s" % self.job.id)








