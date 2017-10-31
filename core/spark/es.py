# imports
import django
from elasticsearch import Elasticsearch
import json
from lxml import etree
import os
import requests
import sys
import xmltodict

# import Row from pyspark
from pyspark.sql import Row
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import udf

# init django settings file to retrieve settings
os.environ['DJANGO_SETTINGS_MODULE'] = 'combine.settings'
sys.path.append('/opt/combine')
django.setup()
from django.conf import settings


class ESIndex(object):

	'''
	Class to organize methods for indexing flattened metadata into ElasticSearch
	'''

	@staticmethod
	def index_job_to_es_spark(spark, job, records_df, index_mapper):

		'''
		Method to index records dataframe into ElasticSearch (ES)

		Args:
			job:
			records_rdd: dataframe
			index_mapper (str): string of indexing mapper to use (e.g. MODSMapper)

		TODO: Consider writing indexing failures to SQL DB as well
		'''

		# get index mapper
		index_mapper_handle = globals()[index_mapper]

		# create rdd from index mapper
		mapped_records_rdd = records_df.rdd.map(lambda row: index_mapper_handle().map_record(row.id, row.document, job.record_group.publish_set_id))

		# attempt to write index mapping failures to DB
		try:
			# filter out index mapping failures
			failures_df = mapped_records_rdd.filter(lambda row: row[0] == 'fail').map(lambda row: Row(id=row[1]['id'], mapping_error=row[1]['mapping_error'])).toDF()

			# add job_id as column
			job_id = job.id
			job_id_udf = udf(lambda id: job_id, IntegerType())
			failures_df = failures_df.withColumn('job_id', job_id_udf(failures_df.id))

			# write mapping failures to DB
			failures_df.withColumn('record_id', failures_df.id).select(['record_id', 'job_id', 'mapping_error']).write.jdbc(settings.COMBINE_DATABASE['jdbc_url'], 'core_indexmappingfailure', properties=settings.COMBINE_DATABASE, mode='append')
		
		except:
			pass

		# retrieve successes to index
		to_index_rdd = mapped_records_rdd.filter(lambda row: row[0] == 'success')

		# create index in advance
		es_handle_temp = Elasticsearch(hosts=[settings.ES_HOST])
		index_name = 'j%s' % job.id
		mapping = {'mappings':{'record':{'date_detection':False}}}
		es_handle_temp.indices.create(index_name, body=json.dumps(mapping))

		# index to ES
		to_index_rdd.saveAsNewAPIHadoopFile(
			path='-',
			outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
			keyClass="org.apache.hadoop.io.NullWritable",
			valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
			conf={
					"es.resource":"%s/record" % index_name,
					"es.nodes":"192.168.45.10:9200",
					"es.mapping.exclude":"temp_id",
					"es.mapping.id":"temp_id"
				}
		)


	@staticmethod
	def index_published_job(**kwargs):

		# copy indexed documents from job to /published
		es_handle_temp = Elasticsearch(hosts=[settings.ES_HOST])
		index_name = 'published'

		# check if published index exists
		if not es_handle_temp.indices.exists(index_name):
			mapping = {'mappings':{'record':{'date_detection':False}}}
			es_handle_temp.indices.create(index_name, body=json.dumps(mapping))

		# prepare _reindex query
		dupe_dict = {
			'source':{
				'index': 'j%s' % kwargs['job_id'],
				'query':{
					'term':{
						'publish_set_id':
						kwargs['publish_set_id']
					}
				}
			},
			'dest': {
				'index':index_name
			}
		}
		r = requests.post('http://%s:9200/_reindex' % settings.ES_HOST, data=json.dumps(dupe_dict), headers={'Content-Type':'application/json'})



class BaseMapper(object):

	'''
	All mappers extend this BaseMapper class.

	Mappers expected to contain following methods:
		- map_record():
			- sets self.mapped_record, and returns instance of self
		- as_dict():
			- returns self.mapped_record as dictionary
		- as_json():
			- returns self.mapped_record as json
	'''

	def __init__(self):

		logger.debug('init BaseMapper')



class MODSMapper(BaseMapper):

	'''
	Mapper for MODS metadata

		- flattens MODS with XSLT transformation (derivation of Islandora and Fedora GSearch xslt)
			- field names are combination of prefixes and attributes
		- convert to dictionary with xmltodict
	'''

	def __init__(self):

		# set xslt transformer		
		xslt_tree = etree.parse('/opt/combine/inc/xslt/MODS_extract.xsl')
		self.xsl_transform = etree.XSLT(xslt_tree)


	def map_record(self, record_id, record_string, publish_set_id):

		try:
			
			# flatten file with XSLT transformation
			xml_root = etree.fromstring(record_string)
			flat_xml = self.xsl_transform(xml_root)

			# convert to dictionary
			flat_dict = xmltodict.parse(flat_xml)

			# prepare as flat mapped
			fields = flat_dict['fields']['field']
			mapped_dict = { field['@name']:field['#text'] for field in fields }

			# add temporary id field
			mapped_dict['temp_id'] = record_id

			# add publish set id
			mapped_dict['publish_set_id'] = publish_set_id

			return (
				'success',
				mapped_dict
			)

		except Exception as e:
			
			return (
				'fail',
				{
					'id':record_id,
					'mapping_error':str(e)
				}
			)

