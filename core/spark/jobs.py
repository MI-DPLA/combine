

# load elasticsearch spark code
from es import ESIndex, MODSMapper

# imports
from lxml import etree
from pyspark.sql import Row



class HarvestSpark(object):

	'''
	spark code for Harvest jobs
	'''

	@staticmethod
	def spark_function(spark, **kwargs):

		'''
		Harvest records, select non-null, and write to avro files

		expecting kwargs from self.start_job()
		'''
		df = spark.read.format("dpla.ingestion3.harvesters.oai")\
		.option("endpoint", kwargs['endpoint'])\
		.option("verb", kwargs['verb'])\
		.option("metadataPrefix", kwargs['metadataPrefix'])\
		.option(kwargs['scope_type'], kwargs['scope_value'])\
		.load()
		
		# select only records
		records = df.select("record.*").where("record is not null")
		
		# write them to avro files
		records.write.format("com.databricks.spark.avro").save(kwargs['output_save_path'])

		# finally, index to ElasticSearch
		ESIndex.index_job_to_es_spark(
			spark,
			job_id = kwargs['job_id'],
			job_output = kwargs['job_output'],
			index_results_save_path=kwargs['index_results_save_path'],
			index_mapper=kwargs['index_mapper']
		)



class TransformSpark(object):

	'''
	spark code for Transform jobs
	'''

	@staticmethod
	def spark_function(spark, **kwargs):

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
		transformed.toDF().write.format("com.databricks.spark.avro").save(kwargs['output_save_path'])

		# finally, index to ElasticSearch
		ESIndex.index_job_to_es_spark(
			spark,
			job_id = kwargs['job_id'],
			job_output = kwargs['job_output'],
			index_results_save_path=kwargs['index_results_save_path'],
			index_mapper=kwargs['index_mapper']
		)



class MergeSpark(object):

	@staticmethod
	def spark_function(spark, sc, **kwargs):

		import ast

		# rehydrate list of input jobs
		input_jobs = ast.literal_eval(kwargs['job_inputs'])

		# get list of RDDs from input jobs
		input_jobs_rdds = [ spark.read.format('com.databricks.spark.avro').load(job).rdd for job in input_jobs ]

		# create aggregate rdd of frames
		agg_rdd = sc.union(input_jobs_rdds)

		# TODO: report duplicate IDs as errors in result

		# write agg to new avro files
		agg_rdd.toDF().write.format("com.databricks.spark.avro").save(kwargs['output_save_path'])

		# finally, index to ElasticSearch
		ESIndex.index_job_to_es_spark(
			spark,
			job_id = kwargs['job_id'],
			job_output = kwargs['job_output'],
			index_results_save_path=kwargs['index_results_save_path'],
			index_mapper=kwargs['index_mapper']
		)



class PublishSpark(object):

	@staticmethod
	def spark_function(spark, **kwargs):

		# read output from input_job
		df = spark.read.format('com.databricks.spark.avro').load(kwargs['job_input'])

		# write them to avro files
		docs = df[df['document'] != '']
		docs.write.format("com.databricks.spark.avro").save(kwargs['output_save_path'])

		# finally, index to ElasticSearch
		ESIndex.index_job_to_es_spark(
			spark,
			job_id = kwargs['job_id'],
			job_output = kwargs['job_output'],
			index_results_save_path=kwargs['index_results_save_path'],
			index_mapper=kwargs['index_mapper']
		)





