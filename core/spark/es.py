
import django
from elasticsearch import Elasticsearch
import json
from lxml import etree
import os
from pyspark.sql import Row
import sys
import xmltodict


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
	def index_job_to_es_spark(spark, **kwargs):

		# get records from job output
		records_df = spark.read.format('com.databricks.spark.avro').load(kwargs['job_output'])

		# get string of xslt
		with open('/opt/combine/inc/xslt/MODS_extract.xsl','r') as f:
			xslt = f.read().encode('utf-8')

		def record_generator(row):

			try:
				# flatten file
				xslt_tree = etree.fromstring(xslt)
				transform = etree.XSLT(xslt_tree)
				xml_root = etree.fromstring(row.document)
				flat_xml = transform(xml_root)

				# convert to dictionary
				flat_dict = xmltodict.parse(flat_xml)

				# prepare as ES-friendly JSON
				fields = flat_dict['fields']['field']
				es_dict = { field['@name']:field['#text'] for field in fields }

				# add temporary id field (consider hashing here?)
				es_dict['temp_id'] = row.id

				return (
					'success',
					es_dict
				)

			except Exception as e:
				
				return (
					'fail',
					{
						'id':row.id,
						'msg':str(e)
					}
				)

		# create rdd with results of function
		records_rdd = records_df.rdd.map(lambda row: record_generator(row))

		# filter out faliures, write to avro file for reporting on index process
		# if no errors are found, pass and interpret missing avro files in the positive during analysis
		failures_rdd = records_rdd.filter(lambda row: row[0] == 'fail').map(lambda row: Row(id=row[1]['id'], error=row[1]['msg']))
		try:
			failures_rdd.toDF().write.format("com.databricks.spark.avro").save(kwargs['index_results_save_path'])
		except:
			pass

		# retrieve successes to index
		to_index_rdd = records_rdd.filter(lambda row: row[0] == 'success')

		# create index in advance
		es_handle_temp = Elasticsearch(hosts=[settings.ES_HOST])
		index_name = 'j%s' % kwargs['job_id']
		mapping = {'mappings':{'record':{'date_detection':False}}}
		es_handle_temp.indices.create(index_name, body=json.dumps(mapping))

		# index to ES
		to_index_rdd.saveAsNewAPIHadoopFile(
			path='-',
			outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
			keyClass="org.apache.hadoop.io.NullWritable",
			valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
			conf={ "es.resource" : "%s/record" % index_name, "es.nodes":"192.168.45.10:9200", "es.mapping.exclude":"temp_id", "es.mapping.id":"temp_id"}
		)



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

		# init parent BaseMapper
		# super().__init__()

		# set xslt transformer		
		xslt_tree = etree.parse('/opt/combine/inc/xslt/MODS_extract.xsl')
		self.xsl_transform = etree.XSLT(xslt_tree)


	def map_record(self, record_id, record_string):

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

			return (
				'success',
				mapped_dict
			)

		except Exception as e:
			
			return (
				'fail',
				{
					'id':row.id,
					'msg':str(e)
				}
			)

