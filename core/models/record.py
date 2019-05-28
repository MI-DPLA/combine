# # -*- coding: utf-8 -*-
# from __future__ import unicode_literals
#
# # generic imports
# import binascii
# from collections import OrderedDict
# import difflib
# import io
# import json
# import logging
# from lxml import etree, isoschematron
# import requests
# import time
# import urllib.parse
#
# # django imports
# from django.conf import settings
# from django.core.urlresolvers import reverse
#
# # import elasticsearch and handles
# from core.es import es_handle
# from elasticsearch.exceptions import NotFoundError
# from elasticsearch_dsl import Search, A, Q
#
# # sxsdiff
# from sxsdiff import DiffCalculator
# from sxsdiff.generators.github import GitHubStyledGenerator
#
# # import mongo dependencies
# from core.mongo import *
#
# # Get an instance of a logger
# logger = logging.getLogger(__name__)
#
# # Set logging levels for 3rd party modules
# logging.getLogger("requests").setLevel(logging.WARNING)
#
# # core models imports
# from core.models.configurations import OAIEndpoint, Transformation, ValidationScenario, FieldMapper,\
#     RecordIdentifierTransformationScenario, DPLABulkDataDownload
# from core.models.job import Job, IndexMappingFailure, JobValidation, JobTrack, JobInput, CombineJob, HarvestJob,\
# 	HarvestOAIJob, HarvestStaticXMLJob, HarvestTabularDataJob, TransformJob, MergeJob, AnalysisJob
#
#
#
# class Record(mongoengine.Document):
#
# 	# fields
# 	combine_id = mongoengine.StringField()
# 	document = mongoengine.StringField()
# 	error = mongoengine.StringField()
# 	fingerprint = mongoengine.IntField()
# 	job_id = mongoengine.IntField()
# 	oai_set = mongoengine.StringField()
# 	publish_set_id = mongoengine.StringField()
# 	published = mongoengine.BooleanField(default=False)
# 	record_id = mongoengine.StringField()
# 	success = mongoengine.BooleanField(default=True)
# 	transformed = mongoengine.BooleanField(default=False)
# 	unique = mongoengine.BooleanField(default=True)
# 	unique_published = mongoengine.BooleanField(default=True)
# 	valid = mongoengine.BooleanField(default=True)
# 	dbdm = mongoengine.BooleanField(default=False)
# 	orig_id = mongoengine.StringField()
#
# 	# meta
# 	meta = {
# 		'index_options': {},
#         'index_background': False,
#         'auto_create_index': False,
#         'index_drop_dups': False,
# 		'indexes': [
# 			{'fields': ['job_id']},
# 			{'fields': ['record_id']},
# 			{'fields': ['combine_id']},
# 			{'fields': ['success']},
# 			{'fields': ['valid']},
# 			{'fields': ['published']},
# 			{'fields': ['publish_set_id']},
# 			{'fields': ['dbdm']}
# 		]
# 	}
#
# 	# cached attributes
# 	_job = None
#
#
# 	def __str__(self):
# 		return 'Record: %s, record_id: %s, Job: %s' % (self.id, self.record_id, self.job.name)
#
#
# 	# _id shim property
# 	@property
# 	def _id(self):
# 		return self.id
#
#
# 	# define job property
# 	@property
# 	def job(self):
#
# 		'''
# 		Method to retrieve Job from Django ORM via job_id
# 		'''
# 		if self._job is None:
# 			try:
# 				job = Job.objects.get(pk=self.job_id)
# 			except:
# 				job = False
# 			self._job = job
# 		return self._job
#
#
# 	def get_record_stages(self, input_record_only=False, remove_duplicates=True):
#
# 		'''
# 		Method to return all upstream and downstreams stages of this record
#
# 		Args:
# 			input_record_only (bool): If True, return only immediate record that served as input for this record.
# 			remove_duplicates (bool): Removes duplicates - handy for flat list of stages,
# 			but use False to create lineage
#
# 		Returns:
# 			(list): ordered list of Record instances from first created (e.g. Harvest), to last (e.g. Publish).
# 			This record is included in the list.
# 		'''
#
# 		record_stages = []
#
# 		def get_upstream(record, input_record_only):
#
# 			# check for upstream job
# 			upstream_job_query = record.job.jobinput_set
#
# 			# if upstream jobs found, continue
# 			if upstream_job_query.count() > 0:
#
# 				logger.debug('upstream jobs found, checking for combine_id')
#
# 				# loop through upstream jobs, look for record id
# 				for upstream_job in upstream_job_query.all():
# 					upstream_record_query = Record.objects.filter(
# 							job_id=upstream_job.input_job.id,
# 							combine_id=self.combine_id
# 						)
#
# 					# if count found, save record to record_stages and re-run
# 					if upstream_record_query.count() > 0:
# 						upstream_record = upstream_record_query.first()
# 						record_stages.insert(0, upstream_record)
# 						if not input_record_only:
# 							get_upstream(upstream_record, input_record_only)
#
#
# 		def get_downstream(record):
#
# 			# check for downstream job
# 			downstream_job_query = JobInput.objects.filter(input_job=record.job)
#
# 			# if downstream jobs found, continue
# 			if downstream_job_query.count() > 0:
#
# 				logger.debug('downstream jobs found, checking for combine_id')
#
# 				# loop through downstream jobs
# 				for downstream_job in downstream_job_query.all():
#
# 					downstream_record_query = Record.objects.filter(
# 						job_id=downstream_job.job.id,
# 						combine_id=self.combine_id
# 					)
#
# 					# if count found, save record to record_stages and re-run
# 					if downstream_record_query.count() > 0:
# 						downstream_record = downstream_record_query.first()
# 						record_stages.append(downstream_record)
# 						get_downstream(downstream_record)
#
# 		# run
# 		get_upstream(self, input_record_only)
# 		if not input_record_only:
# 			record_stages.append(self)
# 			get_downstream(self)
#
# 		# remove duplicate
# 		if remove_duplicates:
# 			record_stages = list(OrderedDict.fromkeys(record_stages))
#
# 		# return
# 		return record_stages
#
#
# 	def get_es_doc(self, drop_combine_fields=False):
#
# 		'''
# 		Return indexed ElasticSearch document as dictionary.
# 		Search is limited by ES index (Job associated) and combine_id
#
# 		Args:
# 			None
#
# 		Returns:
# 			(dict): ES document
# 		'''
#
# 		# init search
# 		s = Search(using=es_handle, index='j%s' % self.job_id)
# 		s = s.query('match', _id=str(self.id))
#
# 		# drop combine fields if flagged
# 		if drop_combine_fields:
# 			s = s.source(exclude=['combine_id','db_id','fingerprint','publish_set_id','record_id'])
#
# 		# execute search and capture as dictionary
# 		try:
# 			sr = s.execute()
# 			sr_dict = sr.to_dict()
# 		except NotFoundError:
# 			logger.debug('mapped fields for record not found in ElasticSearch')
# 			return {}
#
# 		# return
# 		try:
# 			return sr_dict['hits']['hits'][0]['_source']
# 		except:
# 			return {}
#
#
# 	@property
# 	def mapped_fields(self):
#
# 		'''
# 		Return mapped fields as property
# 		'''
#
# 		return self.get_es_doc()
#
#
# 	@property
# 	def document_mapped_fields(self):
#
# 		'''
# 		Method to return mapped fields, dropping internal Combine fields
# 		'''
#
# 		return self.get_es_doc(drop_combine_fields=True)
#
#
# 	def get_dpla_mapped_fields(self):
#
# 		'''
# 		Method to return DPLA specific mapped fields from Record's mapped fields
# 		'''
#
# 		# get mapped fields and return filtered
# 		return {f:v for f,v in self.get_es_doc().items() if f.startswith('dpla_')}
#
#
# 	def parse_document_xml(self):
#
# 		'''
# 		Parse self.document as XML node with etree
#
# 		Args:
# 			None
#
# 		Returns:
# 			(tuple): ((bool) result of XML parsing, (lxml.etree._Element) parsed document)
# 		'''
# 		try:
# 			return (True, etree.fromstring(self.document.encode('utf-8')))
# 		except Exception as e:
# 			logger.debug(str(e))
# 			return (False, str(e))
#
#
# 	def dpla_api_record_match(self, search_string=None):
#
# 		'''
# 		Method to query DPLA API for match against mapped fields
# 			- querying is an ranked list of fields to consecutively search
# 			- this method is recursive such that a preformatted search string can be fed back into it
#
# 		Args:
# 			search_string(str): Optional search_string override
#
# 		Returns:
# 			(dict): If match found, return dictionary of DPLA API response
# 		'''
#
# 		# check for DPLA_API_KEY, else return None
# 		if settings.DPLA_API_KEY:
#
# 			# check for any mapped DPLA fields, skipping altogether if none
# 			mapped_dpla_fields = self.get_dpla_mapped_fields()
# 			if len(mapped_dpla_fields) > 0:
#
# 				# attempt search if mapped fields present and search_string not provided
# 				if not search_string:
#
# 					# ranked search fields
# 					opinionated_search_fields = [
# 						('dpla_isShownAt', 'isShownAt'),
# 						('dpla_title', 'sourceResource.title'),
# 						('dpla_description', 'sourceResource.description')
# 					]
#
# 					# loop through ranked search fields
# 					for local_mapped_field, target_dpla_field in opinionated_search_fields:
#
# 						# if local_mapped_field in keys
# 						if local_mapped_field in mapped_dpla_fields.keys():
#
# 							# get value for mapped field
# 							field_value = mapped_dpla_fields[local_mapped_field]
#
# 							# if list, loop through and attempt searches
# 							if type(field_value) == list:
#
# 								for val in field_value:
# 									search_string = urllib.parse.urlencode({target_dpla_field:'"%s"' % val})
# 									match_results = self.dpla_api_record_match(search_string=search_string)
#
# 							# else if string, perform search
# 							else:
# 								search_string = urllib.parse.urlencode({target_dpla_field:'"%s"' % field_value})
# 								match_results = self.dpla_api_record_match(search_string=search_string)
#
#
# 					# parse results
# 					# count instances of isShownAt, a single one is good enough
# 					if 'isShownAt' in self.dpla_api_matches.keys() and len(self.dpla_api_matches['isShownAt']) == 1:
# 						self.dpla_api_doc = self.dpla_api_matches['isShownAt'][0]['hit']
#
# 					# otherwise, count all, and if only one, use
# 					else:
# 						matches = []
# 						for field,field_matches in self.dpla_api_matches.items():
# 							matches.extend(field_matches)
#
# 						if len(matches) == 1:
# 							self.dpla_api_doc = matches[0]['hit']
#
# 						else:
# 							self.dpla_api_doc = None
#
# 					# return
# 					return self.dpla_api_doc
#
# 				else:
# 					# prepare search query
# 					api_q = requests.get(
# 						'https://api.dp.la/v2/items?%s&api_key=%s' % (search_string, settings.DPLA_API_KEY))
#
# 					# attempt to parse response as JSON
# 					try:
# 						api_r = api_q.json()
# 					except:
# 						logger.debug('DPLA API call unsuccessful: code: %s, response: %s' % (api_q.status_code, api_q.content))
# 						self.dpla_api_doc = None
# 						return self.dpla_api_doc
#
# 					# if count present
# 					if type(api_r) == dict:
# 						if 'count' in api_r.keys():
#
# 							# response
# 							if api_r['count'] >= 1:
#
# 								# add matches to matches
# 								field,value = search_string.split('=')
# 								value = urllib.parse.unquote(value)
#
# 								# check for matches attr
# 								if not hasattr(self, "dpla_api_matches"):
# 									self.dpla_api_matches = {}
#
# 								# add mapped field used for searching
# 								if field not in self.dpla_api_matches.keys():
# 									self.dpla_api_matches[field] = []
#
# 								# add matches for values searched
# 								for doc in api_r['docs']:
# 									self.dpla_api_matches[field].append({
# 											"search_term":value,
# 											"hit":doc
# 										})
#
# 							else:
# 								if not hasattr(self, "dpla_api_matches"):
# 									self.dpla_api_matches = {}
# 						else:
# 							logger.debug('non-JSON response from DPLA API: %s' % api_r)
# 							if not hasattr(self, "dpla_api_matches"):
# 									self.dpla_api_matches = {}
#
# 					else:
# 						logger.debug(api_r)
#
# 		# return None by default
# 		self.dpla_api_doc = None
# 		return self.dpla_api_doc
#
#
# 	def get_validation_errors(self):
#
# 		'''
# 		Return validation errors associated with this record
# 		'''
#
# 		vfs = RecordValidation.objects.filter(record_id=self.id)
# 		return vfs
#
#
# 	def document_pretty_print(self):
#
# 		'''
# 		Method to return document as pretty printed (indented) XML
# 		'''
#
# 		# return as pretty printed string
# 		parsed_doc = self.parse_document_xml()
# 		if parsed_doc[0]:
# 			return etree.tostring(parsed_doc[1], pretty_print=True)
# 		else:
# 			raise Exception(parsed_doc[1])
#
#
# 	def get_lineage_url_paths(self):
#
# 		'''
# 		get paths of Record, Record Group, and Organzation
# 		'''
#
# 		record_lineage_urls = {
# 			'record':{
# 					'name':self.record_id,
# 					'path':reverse('record', kwargs={'org_id':self.job.record_group.organization.id, 'record_group_id':self.job.record_group.id, 'job_id':self.job.id, 'record_id':self.id})
# 				},
# 			'job':{
# 					'name':self.job.name,
# 					'path':reverse('job_details', kwargs={'org_id':self.job.record_group.organization.id, 'record_group_id':self.job.record_group.id, 'job_id':self.job.id})
# 				},
# 			'record_group':{
# 					'name':self.job.record_group.name,
# 					'path':reverse('record_group', kwargs={'org_id':self.job.record_group.organization.id, 'record_group_id':self.job.record_group.id})
# 				},
# 			'organization':{
# 					'name':self.job.record_group.organization.name,
# 					'path':reverse('organization', kwargs={'org_id':self.job.record_group.organization.id})
# 				}
# 		}
#
# 		return record_lineage_urls
#
#
# 	def get_dpla_bulk_data_match(self):
#
# 		'''
# 		Method to return single DPLA Bulk Data Match
# 		'''
#
# 		return DPLABulkDataMatch.objects.filter(record=self)
#
#
# 	def get_input_record_diff(self, output='all', combined_as_html=False):
#
# 		'''
# 		Method to return a string diff of this record versus the input record
# 			- this is primarily helpful for Records from Transform Jobs
# 			- use self.get_record_stages(input_record_only=True)[0]
#
# 		Returns:
# 			(str|list): results of Record documents diff, line-by-line
# 		'''
#
# 		# check if Record has input Record
# 		irq = self.get_record_stages(input_record_only=True)
# 		if len(irq) == 1:
# 			logger.debug('single, input Record found: %s' % irq[0])
#
# 			# get input record
# 			ir = irq[0]
#
# 			# check if fingerprints the same
# 			if self.fingerprint != ir.fingerprint:
#
# 				logger.debug('fingerprint mismatch, returning diffs')
# 				return self.get_record_diff(
# 						input_record=ir,
# 						output=output,
# 						combined_as_html=combined_as_html
# 					)
#
# 			# else, return None
# 			else:
# 				logger.debug('fingerprint match, returning None')
# 				return None
#
# 		else:
# 			return False
#
#
# 	def get_record_diff(self,
# 			input_record=None,
# 			xml_string=None,
# 			output='all',
# 			combined_as_html=False,
# 			reverse_direction=False
# 		):
#
# 		'''
# 		Method to return diff of document XML strings
#
# 		Args;
# 			input_record (core.models.Record): use another Record instance to compare diff
# 			xml_string (str): provide XML string to provide diff on
#
# 		Returns:
# 			(dict): {
# 				'combined_gen' : generator of diflibb
# 				'side_by_side_html' : html output of sxsdiff lib
# 			}
#
# 		'''
#
# 		if input_record:
# 			input_xml_string = input_record.document
#
# 		elif xml_string:
# 			input_xml_string = xml_string
#
# 		else:
# 			logger.debug('input record or XML string required, returning false')
# 			return False
#
# 		# prepare input / result
# 		docs = [input_xml_string, self.document]
# 		if reverse_direction:
# 			docs.reverse()
#
# 		# include combine generator in output
# 		if output in ['all','combined_gen']:
#
# 			# get generator of differences
# 			combined_gen = difflib.unified_diff(
# 				docs[0].splitlines(),
# 				docs[1].splitlines()
# 			)
#
# 			# return as HTML
# 			if combined_as_html:
# 				combined_gen = self._return_combined_diff_gen_as_html(combined_gen)
#
# 		else:
# 			combined_gen = None
#
# 		# include side_by_side html in output
# 		if output in ['all','side_by_side_html']:
#
# 			sxsdiff_result = DiffCalculator().run(docs[0], docs[1])
# 			sio = io.StringIO()
# 			GitHubStyledGenerator(file=sio).run(sxsdiff_result)
# 			sio.seek(0)
# 			side_by_side_html = sio.read()
#
# 		else:
# 			side_by_side_html = None
#
# 		return {
# 			'combined_gen':combined_gen,
# 			'side_by_side_html':side_by_side_html
# 		}
#
#
# 	def _return_combined_diff_gen_as_html(self, combined_gen):
#
# 		'''
# 		Small method to return combined diff generated as pre-compiled HTML
# 		'''
#
# 		html = '<pre><code>'
# 		for line in combined_gen:
# 			if line.startswith('-'):
# 				html += '<span style="background-color:#ffeef0;">'
# 			elif line.startswith('+'):
# 				html += '<span style="background-color:#e6ffed;">'
# 			else:
# 				html += '<span>'
# 			html += line.replace('<','&lt;').replace('>','&gt;')
# 			html += '</span><br>'
# 		html += '</code></pre>'
#
# 		return html
#
#
# 	def calc_fingerprint(self, update_db=False):
#
# 		'''
# 		Generate fingerprint hash with binascii.crc32()
# 		'''
#
# 		fingerprint = binascii.crc32(self.document.encode('utf-8'))
#
# 		if update_db:
# 			self.fingerprint = fingerprint
# 			self.save()
#
# 		return fingerprint
#
#
# 	def map_fields_for_es(self, mapper):
#
# 		'''
# 		Method for testing how a Record will map given an instance
# 		of a mapper from core.spark.es
# 		'''
#
# 		stime = time.time()
# 		mapped_fields = mapper.map_record(record_string=self.document)
# 		logger.debug('mapping elapsed: %s' % (time.time()-stime))
# 		return mapped_fields
#
#
#
# class RecordValidation(mongoengine.Document):
#
# 	# fields
# 	record_id = mongoengine.ReferenceField(Record, reverse_delete_rule=mongoengine.CASCADE)
# 	record_identifier = mongoengine.StringField()
# 	job_id = mongoengine.IntField()
# 	validation_scenario_id = mongoengine.IntField()
# 	validation_scenario_name = mongoengine.StringField()
# 	valid = mongoengine.BooleanField(default=True)
# 	results_payload = mongoengine.StringField()
# 	fail_count = mongoengine.IntField()
#
# 	# meta
# 	meta = {
# 		'index_options': {},
#         'index_background': False,
#         'auto_create_index': False,
#         'index_drop_dups': False,
# 		'indexes': [
# 			{'fields': ['record_id']},
# 			{'fields': ['job_id']},
# 			{'fields': ['validation_scenario_id']}
# 		]
# 	}
#
# 	# cache
# 	_validation_scenario = None
# 	_job = None
#
# 	# define Validation Scenario property
# 	@property
# 	def validation_scenario(self):
#
# 		'''
# 		Method to retrieve Job from Django ORM via job_id
# 		'''
# 		if self._validation_scenario is None:
# 			validation_scenario = ValidationScenario.objects.get(pk=self.validation_scenario_id)
# 			self._validation_scenario = validation_scenario
# 		return self._validation_scenario
#
#
# 	# define job property
# 	@property
# 	def job(self):
#
# 		'''
# 		Method to retrieve Job from Django ORM via job_id
# 		'''
# 		if self._job is None:
# 			job = Job.objects.get(pk=self.job_id)
# 			self._job = job
# 		return self._job
#
#
# 	# convenience method
# 	@property
# 	def record(self):
# 		return self.record_id
#
#
# 	# failed tests as property
# 	@property
# 	def failed(self):
# 		return json.loads(self.results_payload)['failed']