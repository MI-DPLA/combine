# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# generic imports
import logging
import sickle

# django imports
from django.conf import settings

# import mongo dependencies
from core.mongo import *

# Get an instance of a logger
logger = logging.getLogger(__name__)

# Set logging levels for 3rd party modules
logging.getLogger("requests").setLevel(logging.WARNING)



class CombineOAIClient(object):

	'''
	This class provides a client to test the built-in OAI server for Combine
	'''

	def __init__(self):

		# initiate sickle instance
		self.sickle = sickle.Sickle(settings.COMBINE_OAI_ENDPOINT)

		# set default metadata prefix
		# NOTE: Currently Combine's OAI server does not support this, a nonfunctional default is provided
		self.metadata_prefix = None

		# save results from identify
		self.identify = self.sickle.Identify()


	def get_records(self, oai_set=None):

		'''
		Method to return generator of records

		Args:
			oai_set ([str, sickle.models.Set]): optional OAI set, string or instance of Sickle Set to filter records
		'''

		# if oai_set is provided, filter records to set
		if oai_set:
			if type(oai_set) == sickle.models.Set:
				set_name = oai_set.setName
			elif type(oai_set) == str:
				set_name = oai_set

			# return records filtered by set
			return self.sickle.ListRecords(set=set_name, metadataPrefix=self.metadata_prefix)

		# no filter
		return self.sickle.ListRecords(metadataPrefix=self.metadata_prefix)


	def get_identifiers(self, oai_set=None):

		'''
		Method to return generator of identifiers

		Args:
			oai_set ([str, sickle.models.Set]): optional OAI set, string or instance of Sickle Set to filter records
		'''

		# if oai_set is provided, filter record identifiers to set
		if oai_set:
			if type(oai_set) == sickle.models.Set:
				set_name = oai_set.setName
			elif type(oai_set) == str:
				set_name = oai_set

			# return record identifiers filtered by set
			return self.sickle.ListIdentifiers(set=set_name, metadataPrefix=self.metadata_prefix)

		# no filter
		return self.sickle.ListIdentifiers(metadataPrefix=self.metadata_prefix)


	def get_sets(self):

		'''
		Method to return generator of all published sets
		'''

		return self.sickle.ListSets()


	def get_record(self, oai_record_id):

		'''
		Method to return a single record
		'''

		return sickle.GetRecord(identifier = oai_record_id, metadataPrefix = self.metadata_prefix)