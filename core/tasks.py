from background_task import background

# generic imports 
import glob
import json
import math
import os
import polling
import shutil
import subprocess
import tarfile
import time
import uuid
import zipfile

# django imports
from django.db import connection, transaction

# Get an instance of a logger
import logging
logger = logging.getLogger(__name__)

from core import models as models

'''
This file provides background tasks that are performed with Django-Background-Tasks
'''

@background(schedule=1)
def delete_model_instance(instance_model, instance_id, verbose_name=uuid.uuid4().urn):

	'''
	Background task to delete generic DB model instance
	'''
	
	# try:

	# get model		
	m = getattr(models, instance_model, None)

	if m:

		# get model instance
		i = m.objects.get(pk=int(instance_id))
		logger.debug('retrieved %s model, instance ID %s, deleting' % (m.__name__, instance_id))
	
		# delete		
		return i.delete()

	else:
		logger.debug('Combine model %s not found, aborting' % (instance_model))	


@background(schedule=1)
def download_and_index_bulk_data(dbdd_id, verbose_name=uuid.uuid4().urn):

	'''
	Background task driver to manage downloading and indexing of bulk data

	Args:
		dbdd_id (int): ID of DPLABulkDataDownload (dbdd) instance
	'''

	# init bulk download instance
	dbdd = models.DPLABulkDataDownload.objects.get(pk=dbdd_id)

	# init data client with filepath
	dbdc = models.DPLABulkDataClient()

	# download data
	logger.debug('downloading %s' % dbdd.s3_key)
	dbdd.status = 'downloading'
	dbdd.save()
	download_results = dbdc.download_bulk_data(dbdd.s3_key, dbdd.filepath)	

	# index data
	logger.debug('indexing %s' % dbdd.filepath)
	dbdd.status = 'indexing'
	dbdd.save()
	es_index = dbdc.index_to_es(dbdd.s3_key, dbdd.filepath)	

	# update and return
	dbdd.es_index = es_index
	dbdd.status = 'finished'
	dbdd.save()



@background(schedule=1)
def create_validation_report(ct_id):

	'''
	Function to generate a Validation Report for a Job as a bg task

	Args:
		request (django.request): request object with parameters needed for report generation

	Returns:
		location on disk
	'''

	# get CombineTask (ct)
	ct = models.CombineBackgroundTask.objects.get(pk=int(ct_id))

	# get CombineJob
	cjob = models.CombineJob.get_combine_job(int(ct.task_params['job_id']))

	# run report generation
	report_output = cjob.generate_validation_report(
		report_format=ct.task_params['report_format'],
		validation_scenarios=ct.task_params['validation_scenarios'],
		mapped_field_include=ct.task_params['mapped_field_include']
	)
	logger.debug('validation report output: %s' % report_output)

	# save validation report output to Combine Task output
	ct.task_output_json = json.dumps({
		'report_format':ct.task_params['report_format'],		
		'report_output':report_output,
		'export_dir':"/".join(report_output.split('/')[:-1])
	})
	ct.save()


@background(schedule=1)
def export_mapped_fields(ct_id):

	# get CombineTask (ct)
	ct = models.CombineBackgroundTask.objects.get(pk=int(ct_id))

	# JSON export
	if ct.task_params['mapped_fields_export_type'] == 'json':

		# handle single Job
		if 'job_id' in ct.task_params.keys():

			# get CombineJob
			cjob = models.CombineJob.get_combine_job(int(ct.task_params['job_id']))

			# set output filename
			output_path = '/tmp/%s' % uuid.uuid4().hex
			os.mkdir(output_path)
			export_output = '%s/job_%s_mapped_fields.json' % (output_path, cjob.job.id)

			# build command list
			cmd = [
				"elasticdump",
				"--input=http://localhost:9200/j%s" % cjob.job.id,
				"--output=%s" % export_output,
				"--type=data",
				"--sourceOnly",
				"--ignore-errors",
				"--noRefresh"
			]

		# handle published records
		if 'published' in ct.task_params.keys():

			# set output filename
			output_path = '/tmp/%s' % uuid.uuid4().hex
			os.mkdir(output_path)
			export_output = '%s/published_mapped_fields.json' % (output_path)

			# get list of jobs ES indices to export
			pr = models.PublishedRecords()
			es_list = ','.join(['j%s' % job.id for job in pr.published_jobs])

			# build command list
			cmd = [
				"elasticdump",
				"--input=http://localhost:9200/%s" % es_list,
				"--output=%s" % export_output,
				"--type=data",
				"--sourceOnly",
				"--ignore-errors",
				"--noRefresh"
			]		

		# if fields provided, limit
		if ct.task_params['mapped_field_include']:
			logger.debug('specific fields selected, adding to elasticdump command:')
			searchBody = {
				"_source":ct.task_params['mapped_field_include']
			}
			cmd.append("--searchBody='%s'" % json.dumps(searchBody))


	# CSV export
	if ct.task_params['mapped_fields_export_type'] == 'csv':

		# handle single Job
		if 'job_id' in ct.task_params.keys():

			# get CombineJob
			cjob = models.CombineJob.get_combine_job(int(ct.task_params['job_id']))

			# set output filename
			output_path = '/tmp/%s' % uuid.uuid4().hex
			os.mkdir(output_path)
			export_output = '%s/job_%s_mapped_fields.csv' % (output_path, cjob.job.id)

			# build command list
			cmd = [
				"es2csv",
				"-q '*'",
				"-i 'j%s'" % cjob.job.id,
				"-D 'record'",
				"-o '%s'" % export_output
			]

		# handle published records
		if 'published' in ct.task_params.keys():

			# set output filename
			output_path = '/tmp/%s' % uuid.uuid4().hex
			os.mkdir(output_path)
			export_output = '%s/published_mapped_fields.csv' % (output_path)

			# get list of jobs ES indices to export
			pr = models.PublishedRecords()
			es_list = ','.join(['j%s' % job.id for job in pr.published_jobs])

			# build command list
			cmd = [
				"es2csv",
				"-q '*'",
				"-i '%s'" % es_list,
				"-D 'record'",
				"-o '%s'" % export_output
			]

		# handle kibana style
		if ct.task_params['kibana_style']:
			cmd.append('-k')

		# if fields provided, limit
		if ct.task_params['mapped_field_include']:
			logger.debug('specific fields selected, adding to es2csv command:')
			cmd.append('-f ' + " ".join(["'%s'" % field for field in ct.task_params['mapped_field_include']]))


	# execute compiled command
	logger.debug(cmd)
	os.system(" ".join(cmd))

	# handle compression
	if ct.task_params['archive_type'] == 'none':
		logger.debug('uncompressed csv file requested, continuing')

	elif ct.task_params['archive_type'] == 'zip':

		logger.debug('creating compressed zip archive')			
		content_type = 'application/zip'

		# establish output archive file
		export_output_archive = '%s/%s.zip' % (output_path, export_output.split('/')[-1])
		
		with zipfile.ZipFile(export_output_archive,'w', zipfile.ZIP_DEFLATED) as zip:
			zip.write(export_output, export_output.split('/')[-1])

		# set export output to archive file
		export_output = export_output_archive
		
	# tar.gz
	elif ct.task_params['archive_type'] == 'targz':

		logger.debug('creating compressed tar archive')
		content_type = 'application/gzip'

		# establish output archive file
		export_output_archive = '%s/%s.tar.gz' % (output_path, export_output.split('/')[-1])

		with tarfile.open(export_output_archive, 'w:gz') as tar:
			tar.add(export_output, arcname=export_output.split('/')[-1])

		# set export output to archive file
		export_output = export_output_archive

	# save export output to Combine Task output
	ct.task_output_json = json.dumps({		
		'export_output':export_output,
		'name':export_output.split('/')[-1],
		'export_dir':"/".join(export_output.split('/')[:-1])
	})
	ct.save()


@background(schedule=1)
def export_documents(ct_id):

	'''
	- submit livy job and poll until complete
		- use livy session from cjob (works, but awkward way to get this)
	- add wrapper element to file parts
	- rename file parts
	- tar/zip together
	'''

	# TODO: need some handling for failed Jobs which may not be available, but will not be changing
	# to prevent infinite polling (https://github.com/WSULib/combine/issues/192)
	def spark_job_done(response):
		return response['state'] == 'available'

	# get CombineTask (ct)
	try:

		# get CombineBackgroundTask
		ct = models.CombineBackgroundTask.objects.get(pk=int(ct_id))
		logger.debug('using %s' % ct)

		# generate spark code
		output_path = '/tmp/%s' % str(uuid.uuid4())

		# handle single Job
		if 'job_id' in ct.task_params.keys():

			# get CombineJob
			cjob = models.CombineJob.get_combine_job(int(ct.task_params['job_id']))

			# set archive filename of loose XML files
			archive_filename_root = 'j_%s_documents' % cjob.job.id

			# build job_dictionary
			job_dict = {'j%s' % cjob.job.id: [cjob.job.id]}
			logger.debug(job_dict)

			spark_code = "import math,uuid\nfrom console import *\nexport_records_as_xml(spark, '%(output_path)s', %(job_dict)s, %(records_per_file)d)" % {
				'output_path':output_path,
				'job_dict':job_dict,
				'records_per_file':ct.task_params['records_per_file']
			}
			logger.debug(spark_code)

		# handle published records
		if 'published' in ct.task_params.keys():

			# set archive filename of loose XML files
			archive_filename_root = 'published_documents'

			# get anonymous CombineJob
			cjob = models.CombineJob()

			# get published records to determine sets
			pr = models.PublishedRecords()

			# build job_dictionary
			job_dict = {}
			# handle published jobs with publish set ids
			for publish_id, jobs in pr.sets.items():
				job_dict[publish_id] = [ job.id for job in jobs ]
			# handle "loose" Jobs
			job_dict['no_publish_set_id'] = [job.id for job in pr.published_jobs.filter(publish_set_id=None)]
			logger.debug(job_dict)

			spark_code = "import math,uuid\nfrom console import *\nexport_records_as_xml(spark, '%(output_path)s', %(job_dict)s, %(records_per_file)d)" % {
				'output_path':output_path,
				'job_dict':job_dict,
				'records_per_file':ct.task_params['records_per_file']
			}
			logger.debug(spark_code)

		# submit to livy
		logger.debug('submitting code to Spark')
		submit = models.LivyClient().submit_job(cjob.livy_session.session_id, {'code':spark_code})		

		# poll until complete
		logger.debug('polling for Spark job to complete...')
		results = polling.poll(lambda: models.LivyClient().job_status(submit.headers['Location']).json(), check_success=spark_job_done, step=5, poll_forever=True)
		logger.debug(results)

		# loop through parts, group XML docs with rool XML element, and save as new XML file
		logger.debug('grouping documents in XML files')

		export_parts = glob.glob('%s/**/part*' % output_path)
		logger.debug('found %s documents to write as XML' % len(export_parts))
		for part in export_parts:
			with open('%s.xml' % part, 'w') as f:
				f.write('<?xml version="1.0" encoding="UTF-8"?><documents>')
				with open(part) as f_part:
					f.write(f_part.read())
				f.write('</documents>')

		# save list of directories to remove
		pre_archive_dirs = glob.glob('%s/**' % output_path)

		# zip
		if ct.task_params['archive_type'] == 'zip':

			logger.debug('creating compressed zip archive')			
			content_type = 'application/zip'

			# establish output archive file
			export_output_archive = '%s/%s.zip' % (output_path, archive_filename_root)
			
			with zipfile.ZipFile(export_output_archive,'w', zipfile.ZIP_DEFLATED) as zip:
				for f in glob.glob('%s/**/*.xml' % output_path):
					zip.write(f, '/'.join(f.split('/')[-2:]))
			
		# tar
		elif ct.task_params['archive_type'] == 'tar':

			logger.debug('creating uncompressed tar archive')
			content_type = 'application/tar'

			# establish output archive file
			export_output_archive = '%s/%s.tar' % (output_path, archive_filename_root)

			with tarfile.open(export_output_archive, 'w') as tar:
				for f in glob.glob('%s/**/*.xml' % output_path):
					tar.add(f, arcname='/'.join(f.split('/')[-2:]))

		# tar.gz
		elif ct.task_params['archive_type'] == 'targz':

			logger.debug('creating compressed tar archive')
			content_type = 'application/gzip'

			# establish output archive file
			export_output_archive = '%s/%s.tar.gz' % (output_path, archive_filename_root)

			with tarfile.open(export_output_archive, 'w:gz') as tar:
				for f in glob.glob('%s/**/*.xml' % output_path):
					tar.add(f, arcname='/'.join(f.split('/')[-2:]))

		# cleanup directory
		for d in pre_archive_dirs:
			logger.debug('removing dir: %s' % d)
			shutil.rmtree(d)

		# save export output to Combine Task output
		ct.task_output_json = json.dumps({		
			'export_output':export_output_archive,
			'name':export_output_archive.split('/')[-1],
			'content_type':content_type,
			'export_dir':"/".join(export_output_archive.split('/')[:-1])
		})
		ct.save()
		logger.debug(ct.task_output_json)

	except Exception as e:

		logger.debug(str(e))

		# attempt to capture error and return for task
		ct.task_output_json = json.dumps({		
			'error':str(e)
		})
		ct.save()


@background(schedule=1)
def job_reindex(ct_id):

	'''
	- submit livy job and poll until complete
		- use livy session from cjob (works, but awkward way to get this)	
	'''

	# TODO: need some handling for failed Jobs which may not be available, but will not be changing
	# to prevent infinite polling (https://github.com/WSULib/combine/issues/192)
	def spark_job_done(response):
		return response['state'] == 'available'

	# get CombineTask (ct)
	try:
		ct = models.CombineBackgroundTask.objects.get(pk=int(ct_id))
		logger.debug('using %s' % ct)				

		# get CombineJob
		cjob = models.CombineJob.get_combine_job(int(ct.task_params['job_id']))

		# drop Job's ES index
		cjob.job.drop_es_index()

		# drop previous index mapping failures
		cjob.job.remove_mapping_failures_from_db()

		# generate spark code		
		spark_code = 'from jobs import ReindexSparkPatch\nReindexSparkPatch(spark, job_id="%(job_id)s", fm_config_json=\'\'\'%(fm_config_json)s\'\'\').spark_function()' % {
			'job_id':cjob.job.id,
			'fm_config_json':ct.task_params['fm_config_json']
		}

		# submit to livy
		logger.debug('submitting code to Spark')
		submit = models.LivyClient().submit_job(cjob.livy_session.session_id, {'code':spark_code})

		# poll until complete		
		logger.debug('polling for Spark job to complete...')
		results = polling.poll(lambda: models.LivyClient().job_status(submit.headers['Location']).json(), check_success=spark_job_done, step=5, poll_forever=True)
		logger.debug(results)

		# get new mapping
		mapped_field_analysis = cjob.count_indexed_fields()
		cjob.job.update_job_details({
			'fm_config_json':ct.task_params['fm_config_json'],
			'mapped_field_analysis':mapped_field_analysis
			}, save=True)

		# save export output to Combine Task output
		ct.task_output_json = json.dumps({		
			'reindex_results':results
		})
		ct.save()
		logger.debug(ct.task_output_json)

	except Exception as e:

		logger.debug(str(e))

		# attempt to capture error and return for task
		ct.task_output_json = json.dumps({		
			'error':str(e)
		})
		ct.save()


@background(schedule=1)
def job_new_validations(ct_id):

	'''
	- submit livy job and poll until complete
		- use livy session from cjob (works, but awkward way to get this)	
	'''

	# get CombineTask (ct)
	try:
		ct = models.CombineBackgroundTask.objects.get(pk=int(ct_id))
		logger.debug('using %s' % ct)

		# get CombineJob
		cjob = models.CombineJob.get_combine_job(int(ct.task_params['job_id']))

		# loop through validation jobs, and remove from DB if share validation scenario
		cjob.job.remove_validation_jobs(validation_scenarios=[ int(vs_id) for vs_id in ct.task_params['validation_scenarios'] ])

		# generate spark code		
		spark_code = 'from jobs import RunNewValidationsSpark\nRunNewValidationsSpark(spark, job_id="%(job_id)s", validation_scenarios="%(validation_scenarios)s").spark_function()' % {
			'job_id':cjob.job.id,
			'validation_scenarios':str([ int(vs_id) for vs_id in ct.task_params['validation_scenarios'] ]),
		}
		logger.debug(spark_code)

		# submit to livy
		logger.debug('submitting code to Spark')
		submit = models.LivyClient().submit_job(cjob.livy_session.session_id, {'code':spark_code})

		# poll until complete
		# TODO: need some handling for failed Jobs which may not be available, but will not be changing
		# to prevent infinite polling (https://github.com/WSULib/combine/issues/192)
		def spark_job_done(response):
			return response['state'] == 'available'

		logger.debug('polling for Spark job to complete...')
		results = polling.poll(lambda: models.LivyClient().job_status(submit.headers['Location']).json(), check_success=spark_job_done, step=5, poll_forever=True)
		logger.debug(results)

		# write validation links		
		logger.debug('writing validations job links')
		for vs_id in ct.task_params['validation_scenarios']:
			val_job = models.JobValidation(
				job=cjob.job,
				validation_scenario=models.ValidationScenario.objects.get(pk=vs_id)
			)
			val_job.save()

		# save export output to Combine Task output
		ct.task_output_json = json.dumps({		
			'run_new_validations':results
		})
		ct.save()
		logger.debug(ct.task_output_json)

	except Exception as e:

		logger.debug(str(e))

		# attempt to capture error and return for task
		ct.task_output_json = json.dumps({		
			'error':str(e)
		})
		ct.save()


@background(schedule=1)
def job_remove_validation(ct_id):

	'''
	Task to remove a validation, and all failures, from a Job
	'''

	# get CombineTask (ct)
	try:
		ct = models.CombineBackgroundTask.objects.get(pk=int(ct_id))
		logger.debug('using %s' % ct)

		# get CombineJob
		cjob = models.CombineJob.get_combine_job(int(ct.task_params['job_id']))

		# get Job Validation and delete
		jv = models.JobValidation.objects.get(pk=int(ct.task_params['jv_id']))

		# delete validation failures associated with Validation Scenario and Job
		delete_results = jv.delete_record_validation_failures()

		# update valid field in Records via Spark
		# generate spark code		
		spark_code = 'from jobs import RemoveValidationsSpark\nRemoveValidationsSpark(spark, job_id="%(job_id)s", validation_scenarios="%(validation_scenarios)s").spark_function()' % {
			'job_id':cjob.job.id,
			'validation_scenarios':str([ jv.validation_scenario.id ]),
		}
		logger.debug(spark_code)

		# submit to livy
		logger.debug('submitting code to Spark')
		submit = models.LivyClient().submit_job(cjob.livy_session.session_id, {'code':spark_code})

		# poll until complete
		# TODO: need some handling for failed Jobs which may not be available, but will not be changing
		# to prevent infinite polling (https://github.com/WSULib/combine/issues/192)
		def spark_job_done(response):
			return response['state'] == 'available'

		logger.debug('polling for Spark job to complete...')
		results = polling.poll(lambda: models.LivyClient().job_status(submit.headers['Location']).json(), check_success=spark_job_done, step=5, poll_forever=True)
		logger.debug(results)

		# save export output to Combine Task output
		ct.task_output_json = json.dumps({		
			'delete_job_validation':str(jv),			
			'validation_failures_removed_':delete_results
		})
		ct.save()
		logger.debug(ct.task_output_json)

		# remove job validation link
		jv.delete()

	except Exception as e:

		logger.debug(str(e))

		# attempt to capture error and return for task
		ct.task_output_json = json.dumps({		
			'error':str(e)
		})
		ct.save()


@background(schedule=1)
def job_publish(ct_id):

	# get CombineTask (ct)
	try:
		ct = models.CombineBackgroundTask.objects.get(pk=int(ct_id))
		logger.debug('using %s' % ct)

		# get CombineJob
		cjob = models.CombineJob.get_combine_job(int(ct.task_params['job_id']))

		# publish job
		publish_results = cjob.job.publish(publish_set_id=ct.task_params['publish_set_id'])

		# save export output to Combine Task output
		ct.task_output_json = json.dumps({		
			'job_id':ct.task_params['job_id'],
			'publish_results':publish_results
		})
		ct.save()
		logger.debug(ct.task_output_json)		

	except Exception as e:

		logger.debug(str(e))

		# attempt to capture error and return for task
		ct.task_output_json = json.dumps({		
			'error':str(e)
		})
		ct.save()


@background(schedule=1)
def job_unpublish(ct_id):

	# get CombineTask (ct)
	try:
		ct = models.CombineBackgroundTask.objects.get(pk=int(ct_id))
		logger.debug('using %s' % ct)

		# get CombineJob
		cjob = models.CombineJob.get_combine_job(int(ct.task_params['job_id']))

		# publish job
		unpublish_results = cjob.job.unpublish()

		# save export output to Combine Task output
		ct.task_output_json = json.dumps({		
			'job_id':ct.task_params['job_id'],
			'unpublish_results':unpublish_results
		})
		ct.save()
		logger.debug(ct.task_output_json)		

	except Exception as e:

		logger.debug(str(e))

		# attempt to capture error and return for task
		ct.task_output_json = json.dumps({		
			'error':str(e)
		})
		ct.save()

