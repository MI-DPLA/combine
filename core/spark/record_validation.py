# imports
import django
from functools import reduce
import hashlib
from inspect import isfunction, signature
import json
from lxml import etree, isoschematron
import os
import shutil
import sys
from types import ModuleType

# import Row from pyspark
from pyspark import StorageLevel
from pyspark.sql import Row
from pyspark.sql.types import StringType, IntegerType
import pyspark.sql.functions as pyspark_sql_functions
from pyspark.sql.functions import udf

# import from core.spark
try:
	from utils import PythonUDFRecord, refresh_django_db_connection
except:
	from core.spark.utils import PythonUDFRecord, refresh_django_db_connection

# init django settings file to retrieve settings
os.environ['DJANGO_SETTINGS_MODULE'] = 'combine.settings'
sys.path.append('/opt/combine')
django.setup()

from django.conf import settings
from django.db import connection

# import select models from Core
from core.models import Job, ValidationScenario



####################################################################
# Record Validation												   #
####################################################################

class ValidationScenarioSpark(object):

	'''
	Class to organize methods and attributes used for running validation scenarios
	'''

	def __init__(self,
		spark=None,
		job=None,
		records_df=None,
		validation_scenarios=None):

		'''
		Args:
			spark (pyspark.sql.session.SparkSession): spark instance from static job methods
			job (core.models.Job): Job instance
			records_df (pyspark.sql.DataFrame): records as pyspark DataFrame
			validation_scenarios (list): list of ValidationScenario job ids as integers
		'''

		self.spark = spark
		self.job = job
		self.records_df = records_df
		self.validation_scenarios = validation_scenarios

		# init logging support
		spark.sparkContext.setLogLevel('INFO')
		log4jLogger = spark.sparkContext._jvm.org.apache.log4j
		self.logger = log4jLogger.LogManager.getLogger(__name__)


	def run_record_validation_scenarios(self):

		'''
		Function to run validation scenarios
		Results are written to RecordValidation table, one result, per record, per failed validation test.

		Validation tests may be of type:
			- 'sch': Schematron based validation, performed with lxml etree
			- 'python': custom python code snippets

		Args:
			None

		Returns:
			None
				- writes validation fails to RecordValidation table
		'''

		# refresh Django DB Connection
		refresh_django_db_connection()

		# loop through validation scenarios and fire validation type specific method
		failure_rdds = []
		for vs_id in self.validation_scenarios:

			# get validation scenario
			vs = ValidationScenario.objects.get(pk=int(vs_id))
			vs_id = vs.id
			vs_name = vs.name
			vs_filepath = vs.filepath

			# schematron based validation scenario
			if vs.validation_type == 'sch':
				validation_fails_rdd = self._sch_validation(vs, vs_id, vs_name, vs_filepath)

			# python based validation scenario
			elif vs.validation_type == 'python':
				validation_fails_rdd = self._python_validation(vs, vs_id, vs_name, vs_filepath)

			# ElasticSearch DSL query based validation scenario
			elif vs.validation_type == 'es_query':
				validation_fails_rdd = self._es_query_validation(vs, vs_id, vs_name, vs_filepath)

			# XML Schema (XSD) based validation scenario
			elif vs.validation_type == 'xsd':
				validation_fails_rdd = self._xsd_validation(vs, vs_id, vs_name, vs_filepath)

			# if results, append
			if validation_fails_rdd and not validation_fails_rdd.isEmpty():
				failure_rdds.append(validation_fails_rdd)

		# if rdds, union and write
		if len(failure_rdds) > 0:

			# merge rdds
			failures_union_rdd = self.spark.sparkContext.union(failure_rdds)
			failures_df = failures_union_rdd.toDF()

			# write failures
			failures_df.write.format("com.mongodb.spark.sql.DefaultSource")\
			.mode("append")\
			.option("uri","mongodb://127.0.0.1")\
			.option("database","combine")\
			.option("collection", "record_validation").save()

			# update validity for Job
			self.update_job_record_validity()


	def _sch_validation(self, vs, vs_id, vs_name, vs_filepath):

		self.logger.info('running schematron validation: %s' % vs.name)

		def validate_schematron_pt_udf(pt):

			# parse schematron
			sct_doc = etree.parse(vs_filepath)
			validator = isoschematron.Schematron(sct_doc, store_report=True)

			for row in pt:

				try:

					# get document xml
					record_xml = etree.fromstring(row.document.encode('utf-8'))

					# validate
					is_valid = validator.validate(record_xml)

					# if not valid, prepare Row
					if not is_valid:

						# prepare results_dict
						results_dict = {
							'fail_count':0,
							'failed':[]
						}

						# get failed
						report_root = validator.validation_report.getroot()
						fails = report_root.findall('svrl:failed-assert', namespaces=report_root.nsmap)

						# log fail_count
						results_dict['fail_count'] = len(fails)

						# loop through fails and add to dictionary
						for fail in fails:
							fail_text_elem = fail.find('svrl:text', namespaces=fail.nsmap)
							results_dict['failed'].append(fail_text_elem.text)

						yield Row(
							record_id=row._id,
							record_identifier=row.record_id,
							job_id=row.job_id,
							validation_scenario_id=int(vs_id),
							validation_scenario_name=vs_name,
							valid=False,
							results_payload=json.dumps(results_dict),
							fail_count=results_dict['fail_count']
						)

				except Exception as e:

					results_dict = {
						'fail_count':0,
						'failed':[]
					}
					results_dict['fail_count'] += 1
					results_dict['failed'].append("Schematron validation exception: %s" % (str(e)))

					yield Row(
							record_id=row._id,
							record_identifier=row.record_id,
							job_id=row.job_id,
							validation_scenario_id=int(vs_id),
							validation_scenario_name=vs_name,
							valid=False,
							results_payload=json.dumps(results_dict),
							fail_count=results_dict['fail_count']
						)

		# run pt_udf map
		validation_fails_rdd = self.records_df.rdd.mapPartitions(validate_schematron_pt_udf).filter(lambda row: row is not None)

		# return
		return validation_fails_rdd


	def _python_validation(self, vs, vs_id, vs_name, vs_filepath):

		self.logger.info('running python validation: %s' % vs.name)

		def validate_python_udf(vs_id, vs_name, pyvs_funcs, row):

			'''
			Loop through test functions and aggregate in fail_dict to return with Row

			Args:
				vs_id (int): integer of validation scenario
				pyvs_funcs (list): list of functions imported from user created python validation scenario payload
				row ():
			'''

			# prepare row as parsed document with PythonUDFRecord class
			prvb = PythonUDFRecord(row)

			# prepare results_dict
			results_dict = {
				'fail_count':0,
				'failed':[]
			}

			# loop through functions
			for func in pyvs_funcs:

				# get name as string
				func_name = func.__name__

				# get func test message
				func_signature = signature(func)
				t_msg = func_signature.parameters['test_message'].default

				# attempt to run user-defined validation function
				try:

					# run test
					test_result = func(prvb)

					# if fail, append
					if test_result != True:

						# bump fail count
						results_dict['fail_count'] += 1

						# if custom message override provided, use
						if test_result != False:
							results_dict['failed'].append(test_result)

						# else, default to test message
						else:
							results_dict['failed'].append(t_msg)

				# if problem, report as failure with Exception string
				except Exception as e:
					results_dict['fail_count'] += 1
					results_dict['failed'].append("test '%s' had exception: %s" % (func_name, str(e)))

			# if failed, return Row
			if results_dict['fail_count'] > 0:

				# return row
				return Row(
					record_id=row._id,
					record_identifier=row.record_id,
					job_id=row.job_id,
					validation_scenario_id=int(vs_id),
					validation_scenario_name=vs_name,
					valid=False,
					results_payload=json.dumps(results_dict),
					fail_count=results_dict['fail_count']
				)

		# parse user defined functions from validation scenario payload
		temp_pyvs = ModuleType('temp_pyvs')
		exec(vs.payload, temp_pyvs.__dict__)

		# get defined functions
		pyvs_funcs = []
		test_labeled_attrs = [ attr for attr in dir(temp_pyvs) if attr.lower().startswith('test') ]
		for attr in test_labeled_attrs:
			attr = getattr(temp_pyvs, attr)
			if isfunction(attr):
				pyvs_funcs.append(attr)

		validation_fails_rdd = self.records_df.rdd.\
			map(lambda row: validate_python_udf(vs_id, vs_name, pyvs_funcs, row))\
			.filter(lambda row: row is not None)

		# return
		return validation_fails_rdd


	def _es_query_validation(self, vs, vs_id, vs_name, vs_filepath):

		self.logger.info('running es_query validation: %s' % vs.name)

		# set es index
		# TODO: how handle published Jobs?
		es_index = 'j%s' % self.job.id

		# loads validation payload as dictionary
		validations = json.loads(vs.payload)

		# failure dfs
		fail_dfs = []

		# loop through validations
		for v in validations:

			# prepare query
			es_val_query = json.dumps(v['es_query'])

			# perform es query
			es_rdd = self.spark.sparkContext.newAPIHadoopRDD(
				inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
				keyClass="org.apache.hadoop.io.NullWritable",
				valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
				conf={
						"es.resource":"%s/record" % es_index,
						"es.query":es_val_query,
						"es.read.field.include":"db_id"
					}
				)

			# if query is not empty, map to DataFrame
			if not es_rdd.isEmpty():
				es_df = es_rdd.map(lambda row: (row[1]['db_id'], )).toDF()

			# handle validity matching
			# NOTE: matching on records_df['_id']['oid'] to get str cast of Mongo ObjectId
			# if a match is valid, report all Records that don't match
			if v['matches'] == 'valid':

				# if empty, assume all Records in Job are invalid
				if es_rdd.isEmpty():
					fail_df = self.records_df.select('_id','record_id')

				# else, perform join
				else:
					fail_df = self.records_df.alias('records_df').join(es_df, self.records_df['_id']['oid'] == es_df['_1'], 'leftanti').select('_id','records_df.record_id')

			# if a match is invalid, report all Records that match
			elif v['matches'] == 'invalid':

				# if empty, nothing to report, return None
				if es_rdd.isEmpty():
					return None

				# else, perform join
				else:
					fail_df = self.records_df.alias('records_df').join(es_df, self.records_df['_id']['oid'] == es_df['_1'], 'leftsemi').select('_id','records_df.record_id')

			# add columns to df to return
			fail_df = fail_df.withColumn('failed', pyspark_sql_functions.array(pyspark_sql_functions.lit(v['test_name'])))
			fail_df = fail_df.withColumn('fail_count', pyspark_sql_functions.lit(1))

			# append to validations dictionary
			fail_dfs.append(fail_df)

		# if dataframes to reduce and return, perform
		if len(fail_dfs) > 0:

			# merge and format
			new_df = reduce(lambda a, b: a.unionAll(b), fail_dfs)\
				.select("_id", "record_id", pyspark_sql_functions.explode("failed").alias("failed_values"), "fail_count")\
				.groupBy("_id","record_id")\
				.agg(pyspark_sql_functions.collect_list("failed_values").alias("failed"), pyspark_sql_functions.sum("fail_count").alias("fail_count"))\
				.select("_id", "record_id", pyspark_sql_functions.to_json(pyspark_sql_functions.struct("failed", "fail_count")).alias("data"), "fail_count")

			# write return failures as validation_fails_rdd
			job_id = self.job.id
			validation_fails_rdd = new_df.rdd.map(lambda row: Row(
				record_id=row._id,
				record_identifier=row.record_id,
				job_id=job_id,
				validation_scenario_id=int(vs_id),
				validation_scenario_name=vs_name,
				valid=False,
				results_payload=row.data,
				fail_count=int(row['fail_count']))
			)
			return validation_fails_rdd

		else:
			return None


	def _xsd_validation(self, vs, vs_id, vs_name, vs_filepath):

		self.logger.info('running xsd validation: %s' % vs.name)

		def validate_xsd_pt_udf(pt):

			# parse xsd
			xmlschema_doc = etree.parse(vs_filepath)
			xmlschema = etree.XMLSchema(xmlschema_doc)

			for row in pt:

				try:

					# get document xml
					record_xml = etree.fromstring(row.document.encode('utf-8'))

					# validate
					try:
						xmlschema.assertValid(record_xml)

					except etree.DocumentInvalid as e:

						# prepare results_dict
						results_dict = {
							'fail_count':1,
							'failed':[str(e)]
						}

						yield Row(
							record_id=row._id,
							record_identifier=row.record_id,
							job_id=row.job_id,
							validation_scenario_id=int(vs_id),
							validation_scenario_name=vs_name,
							valid=False,
							results_payload=json.dumps(results_dict),
							fail_count=results_dict['fail_count']
						)

				except Exception as e:

					results_dict = {
						'fail_count':1,
						'failed':[]
					}
					results_dict['failed'].append("XSD validation exception: %s" % (str(e)))

					yield Row(
							record_id=row._id,
							record_identifier=row.record_id,
							job_id=row.job_id,
							validation_scenario_id=int(vs_id),
							validation_scenario_name=vs_name,
							valid=False,
							results_payload=json.dumps(results_dict),
							fail_count=results_dict['fail_count']
						)

		# run pt_udf map
		validation_fails_rdd = self.records_df.rdd.mapPartitions(validate_xsd_pt_udf).filter(lambda row: row is not None)

		# return
		return validation_fails_rdd


	def remove_validation_scenarios(self):

		'''
		Method to update validity attribute of records after removal of validation scenarios
			- approach is to update all INVALID Records that may now be valid by lack of
			matching record_id in remaining validation failures
		'''

		# read current failures from Mongo
		failures_pipeline = json.dumps({'$match': {'job_id': self.job.id}})
		failures_df = self.spark.read.format("com.mongodb.spark.sql.DefaultSource")\
		.option("uri","mongodb://127.0.0.1")\
		.option("database","combine")\
		.option("collection","record_validation")\
		.option("pipeline",failures_pipeline).load()

		# if failures to work with, rewrite records with valid = True if NOT in remaining DB failures
		if not failures_df.rdd.isEmpty():
			set_valid_df = self.records_df.alias('records_df').join(
				failures_df.select('record_id').distinct().alias('failures_df'),
				failures_df['record_id'] == self.records_df['_id'],
				'leftanti')\
				.select(self.records_df.columns)\
				.withColumn('valid',pyspark_sql_functions.lit(True))
		else:
			# will write all previously invalid, as valid
			set_valid_df = self.records_df.withColumn('valid',pyspark_sql_functions.lit(True))

		# update validity of Records
		set_valid_df.write.format("com.mongodb.spark.sql.DefaultSource")\
		.mode("append")\
		.option("uri","mongodb://127.0.0.1")\
		.option("database","combine")\
		.option("collection", "record").save()


	def update_job_record_validity(self):

		'''
		Method to update validity of Records in Job based on found RecordValidadtions
		'''

		# get failures
		pipeline = json.dumps({'$match':{'$and':[{'job_id': self.job.id}]}})
		all_failures_df = self.spark.read.format("com.mongodb.spark.sql.DefaultSource")\
		.option("uri","mongodb://127.0.0.1")\
		.option("database","combine")\
		.option("collection","record_validation")\
		.option("partitioner","MongoSamplePartitioner")\
		.option("spark.mongodb.input.partitionerOptions.partitionSizeMB",settings.MONGO_READ_PARTITION_SIZE_MB)\
		.option("pipeline",pipeline).load()\
		.select('record_id')\
		.withColumnRenamed('record_id','fail_id')

		# join, writing potentially null `fail_id` column
		fail_join = self.records_df.alias('records_df').join(
			all_failures_df.select('fail_id').distinct().alias('all_failures_df'),
			self.records_df['_id'] == all_failures_df['fail_id'],
			'leftouter')

		# set valid column based on join and drop column
		updated_validity = fail_join.withColumn('update_valid', pyspark_sql_functions.when(fail_join['fail_id'].isNotNull(), False).otherwise(True))

		# subset those that need updating and flip validity
		to_update = updated_validity.where(updated_validity['valid'] != updated_validity['update_valid'])\
			.select(self.records_df.columns)\
			.withColumn('valid', pyspark_sql_functions.when(self.records_df.valid == True, False).otherwise(True))

		# update in DB by overwriting
		to_update.write.format("com.mongodb.spark.sql.DefaultSource")\
		.mode("append")\
		.option("uri","mongodb://127.0.0.1")\
		.option("database","combine")\
		.option("collection", "record").save()


	def export_job_validation_report(self):

		pass


















