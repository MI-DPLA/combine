
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



def index_records_to_db(job, records):

	'''
	function to index records to SQL DB
	'''

	# add job_id as column
	job_id = job.id
	job_id_udf = udf(lambda id: job_id, IntegerType())
	records = records.withColumn('job_id', job_id_udf(records.id))

	# write records to DB
	records.withColumn('record_id', records.id).select(['record_id', 'job_id', 'document', 'error']).write.jdbc(settings.COMBINE_DATABASE['jdbc_url'], 'core_record', properties=settings.COMBINE_DATABASE, mode='append')



class HarvestSpark(object):

	'''
	spark code for Harvest jobs
	'''

	@staticmethod
	def spark_function(spark, **kwargs):

		'''
		Harvest records, select non-null, and write to avro files

		TODO
			- rework DPLA Harvest DF to document | error structure
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
		index_records_to_db(job, avro_records)

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
	spark code for Transform jobs
	'''

	@staticmethod
	def spark_function(spark, **kwargs):

		# get job
		job = Job.objects.get(pk=int(kwargs['job_id']))

		# read output from input_job
		df = spark.read.format('com.databricks.spark.avro').load(kwargs['job_input'])

		# get string of xslt
		with open(kwargs['transform_filepath'],'r') as f:
			xslt = f.read().encode('utf-8')

		# define function for transformation
		def transform_xml(record_id, xml, xslt):

			# attempt transformation and save out put to 'document'
			try:
				xslt_root = etree.fromstring(xslt)
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
		transformed = df.rdd.map(lambda row: transform_xml(row.id, row.document, xslt))

		# write them to avro files
		transformed.toDF().write.format("com.databricks.spark.avro").save(job.job_output)

		# index records to db
		index_records_to_db(job, transformed.toDF())

		# finally, index to ElasticSearch
		if settings.INDEX_TO_ES:
			ESIndex.index_job_to_es_spark(
				spark,
				job=job,
				records_df=transformed.toDF(),
				index_mapper=kwargs['index_mapper']
			)



class MergeSpark(object):

	@staticmethod
	def spark_function(spark, sc, **kwargs):

		# get job
		job = Job.objects.get(pk=int(kwargs['job_id']))

		# rehydrate list of input jobs
		input_jobs = ast.literal_eval(kwargs['job_inputs'])

		# get list of RDDs from input jobs
		input_jobs_rdds = [ spark.read.format('com.databricks.spark.avro').load(job).rdd for job in input_jobs ]

		# create aggregate rdd of frames
		agg_rdd = sc.union(input_jobs_rdds)

		# TODO: report duplicate IDs as errors in result

		# write agg to new avro files
		agg_rdd.toDF().write.format("com.databricks.spark.avro").save(job.job_output)

		# index records to db
		index_records_to_db(job, agg_rdd.toDF())

		# finally, index to ElasticSearch
		if settings.INDEX_TO_ES:
			ESIndex.index_job_to_es_spark(
				spark,
				job=job,
				records_df=agg_rdd.toDF(),
				index_mapper=kwargs['index_mapper']
			)



class PublishSpark(object):

	@staticmethod
	def spark_function(spark, **kwargs):

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
		index_records_to_db(job, docs)

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



