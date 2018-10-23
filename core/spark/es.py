# imports
import django
from elasticsearch import Elasticsearch
import json
from lxml import etree
from lxml.etree import _Element, _ElementUnicodeResult
import os
import re
import requests
import sys
import time
import xmltodict

# pyjxslt
import pyjxslt

# import Row from pyspark
try:
	from pyspark.sql import Row
	from pyspark.sql.types import StringType, IntegerType
	from pyspark.sql.functions import udf, lit
except:
	pass

# check for registered apps signifying readiness, if not, run django.setup() to run as standalone
if not hasattr(django, 'apps'):
	os.environ['DJANGO_SETTINGS_MODULE'] = 'combine.settings'
	sys.path.append('/opt/combine')
	django.setup()	

# import django settings
from django.conf import settings

# import xml2kvp
try:
	from core.xml2kvp import XML2kvp
except:
	from xml2kvp import XML2kvp


class ESIndex(object):

	'''
	Class to organize methods for indexing mapped/flattened metadata into ElasticSearch (ES)
	'''

	@staticmethod
	def index_job_to_es_spark(spark, job, records_df, field_mapper_config):

		'''
		Method to index records dataframe into ES

		Args:
			spark (pyspark.sql.session.SparkSession): spark instance from static job methods
			job (core.models.Job): Job for records
			records_df (pyspark.sql.DataFrame): records as pyspark DataFrame 
			field_mapper_config (dict): XML2kvp field mapper configurations

		Returns:
			None
				- indexes records to ES
		'''

		# init logging support
		spark.sparkContext.setLogLevel('INFO')
		log4jLogger = spark.sparkContext._jvm.org.apache.log4j
		logger = log4jLogger.LogManager.getLogger(__name__)

		# get index mapper
		index_mapper_handle = globals()['XML2kvpMapper']

		# create rdd from index mapper
		def es_mapper_pt_udf(pt):

			# init mapper once per partition
			mapper = index_mapper_handle(field_mapper_config=field_mapper_config)

			for row in pt:

				yield mapper.map_record(
					record_string=row.document,
					db_id=row._id.oid,
					combine_id=row.combine_id,
					record_id=row.record_id,				
					publish_set_id=job.publish_set_id,
					fingerprint=row.fingerprint
				)

		logger.info('###ES 1 -- mapping records')
		mapped_records_rdd = records_df.rdd.mapPartitions(es_mapper_pt_udf)

		# attempt to write index mapping failures to DB
		# filter our failures
		logger.info('###ES 2 -- filtering failures')
		failures_rdd = mapped_records_rdd.filter(lambda row: row[0] == 'fail')

		# if failures, write
		if not failures_rdd.isEmpty():

			logger.info('###ES 3 -- writing indexing failures')

			failures_df = failures_rdd.map(lambda row: Row(
					db_id=row[1]['db_id'],
					record_id=row[1]['record_id'],
					mapping_error=row[1]['mapping_error']
				)).toDF()

			# add job_id as column
			failures_df = failures_df.withColumn('job_id', lit(job.id))

			# write mapping failures to DB
			failures_df.select(['db_id', 'record_id', 'job_id', 'mapping_error'])\
			.write.format("com.mongodb.spark.sql.DefaultSource")\
			.mode("append")\
			.option("uri","mongodb://127.0.0.1")\
			.option("database","combine")\
			.option("collection", "index_mapping_failure").save()
		
		# retrieve successes to index
		logger.info('###ES 4 -- filtering successes')
		to_index_rdd = mapped_records_rdd.filter(lambda row: row[0] == 'success')

		# create index in advance
		index_name = 'j%s' % job.id
		es_handle_temp = Elasticsearch(hosts=[settings.ES_HOST])
		if not es_handle_temp.indices.exists(index_name):
			
			# put combine es index templates
			template_body = {
					'template':'*',
					'settings':{
						'number_of_shards':1,
						'number_of_replicas':0,
						'refresh_interval':-1
					},
					'mappings':{
						'record':{
							'date_detection':False,
							'properties':{
								'combine_db_id':{
									'type':'integer'
								}
							}
						}
					}
				}
			es_handle_temp.indices.put_template('combine_template', body=json.dumps(template_body))
			
			# create index
			es_handle_temp.indices.create(index_name)

		# index to ES
		logger.info('###ES 5 -- writing to ES')
		to_index_rdd.saveAsNewAPIHadoopFile(
			path='-',
			outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
			keyClass="org.apache.hadoop.io.NullWritable",
			valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
			conf={
					"es.resource":"%s/record" % index_name,
					"es.nodes":"%s:9200" % settings.ES_HOST,
					"es.mapping.exclude":"temp_id",
					"es.mapping.id":"temp_id",
				}
		)

		# refresh index
		es_handle_temp.indices.refresh(index_name)

		# return
		return to_index_rdd


	@staticmethod
	def copy_es_index(
		source_index=None,
		target_index=None,
		create_target_index=True,		
		refresh=True,
		wait_for_completion=True,
		add_copied_from=None):

		'''
		Method to duplicate one ES index to another

		Args:
			create_target_index (boolean): If True, check for target and create
			source_index (str): Source ES index to copy from
			target_index (str): Target ES index to copy to			

		Returns:
			(dict): results of reindex via elasticsearch client reindex request
		'''

		# get ES handle
		es_handle_temp = Elasticsearch(hosts=[settings.ES_HOST])

		# put/confirm combine es index templates
		template_body = {
				'template':'*',
				'settings':{
					'number_of_shards':1,
					'number_of_replicas':0,
					'refresh_interval':-1
				},
				'mappings':{
					'record':{
						'date_detection':False,
						'properties':{
							'combine_db_id':{
								'type':'integer'
							}
						}
					}
				}
			}
		es_handle_temp.indices.put_template('combine_template', body=json.dumps(template_body))

		# if creating target index check if target index exists
		if create_target_index and not es_handle_temp.indices.exists(target_index):
			es_handle_temp.indices.create(target_index)

		# prepare reindex query
		dupe_dict = {
			'source':{
				'index': source_index,
				'query':{}
			},
			'dest': {
				'index':target_index
			}			
		}

		# if add_copied_from, include in reindexed document
		if add_copied_from:
			dupe_dict['script'] = {
				'inline': 'ctx._source.source_job_id = %s' % add_copied_from,
				'lang': 'painless'
			}
		
		# reindex using elasticsearch client
		reindex = es_handle_temp.reindex(body=dupe_dict, wait_for_completion=wait_for_completion, refresh=refresh)
		return reindex



class BaseMapper(object):

	'''
	All mappers extend this BaseMapper class.

	Contains some useful methods and attributes that other mappers may use

	Mappers expected to contain following methods:
		- map_record()
	'''

	# pre-compiled regex	
	blank_check_regex = re.compile(r"[^ \t\n]") # checker for blank spaces	
	namespace_prefix_regex = re.compile(r'(\{.+\})?(.*)') # element tag name


	def get_namespaces(self):
		
		'''
		Method to parse namespaces from XML document and save to self.nsmap
		'''

		nsmap = {}
		for ns in self.xml_root.xpath('//namespace::*'):
			if ns[0]:
				nsmap[ns[0]] = ns[1]
		self.nsmap = nsmap

		# set inverted nsmap
		self.nsmap_inv = {v: k for k, v in self.nsmap.items()}



class XML2kvpMapper(BaseMapper):

	'''
	Map XML to ElasticSearch friendly fields with XML2kvp
	'''


	def __init__(self, field_mapper_config=None):		

		self.field_mapper_config = field_mapper_config


	def map_record(self,
			record_string=None,
			db_id=None,
			combine_id=None,
			record_id=None,
			publish_set_id=None,
			fingerprint=None
		):

		'''
		Map record

		Args:
			record_string (str): string of record document
			db_id (str): mongo db id
			combine_id (str): combine_id id
			record_id (str): record id			
			publish_set_id (str): core.models.RecordGroup.published_set_id, used to build publish identifier
			fingerprint (str): fingerprint

		Returns:
			(tuple):
				0 (str): ['success','fail']
				1 (dict): details from mapping process, success or failure
		'''

		try:			

			# prepare literals
			if 'add_literals' not in self.field_mapper_config.keys():
				self.field_mapper_config['add_literals'] = {}


			self.field_mapper_config['add_literals'].update({

				# add temporary id field
				'temp_id':db_id,

				# add combine_id field
				'combine_id':combine_id,

				# add record_id field
				'record_id':record_id,

				# add publish set id
				'publish_set_id':publish_set_id,

				# add record's Combine DB id
				'db_id':db_id,

				# add record's crc32 document hash, aka "fingerprint"
				'fingerprint':fingerprint,

			})

			# map with XML2kvp
			kvp_dict = XML2kvp.xml_to_kvp(record_string, **self.field_mapper_config)

			return (
					'success',
					kvp_dict
				)

		except Exception as e:
			
			return (
				'fail',
				{
					'db_id':db_id,
					'record_id':record_id,
					'mapping_error':str(e)
				}
			)



