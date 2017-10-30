# python modules
from concurrent.futures.thread import ThreadPoolExecutor
import datetime
import hashlib
import json
import logging
from lxml import etree
import time
import uuid

# django settings
from django.conf import settings

# import models
from core import models

# Get an instance of a logger
logger = logging.getLogger(__name__)


# attempt to load metadataPrefix map from localConfig, otherwise provide default
metadataPrefix_hash = {
	'mods':{
			'schema':'http://www.loc.gov/standards/mods/v3/mods.xsd',
			'namespace':'http://www.loc.gov/mods/v3'
		},
	'oai_dc':{
			'schema':'http://www.openarchives.org/OAI/2.0/oai_dc.xsd',
			'namespace':'http://purl.org/dc/elements/1.1/'
		},
	'dc':{
			'schema':'http://www.openarchives.org/OAI/2.0/oai_dc.xsd',
			'namespace':'http://purl.org/dc/elements/1.1/'
		},
}


class OAIProvider(object):

	'''
	Because the OAI-PMH protocol shares verbs with reserved words in Python (e.g. "set", or "from"),
	easier to keep the HTTP request args work with as a dictionary, and maintain the original OAI-PMH vocab.
	'''

	def __init__(self, args):

		# read args, route verb to verb handler
		self.verb_routes = {
			'GetRecord':self._GetRecord,
			'Identify':self._Identify,
			'ListIdentifiers':self._ListIdentifiers,
			'ListMetadataFormats':self._ListMetadataFormats,
			'ListRecords':self._ListRecords,
			'ListSets':self._ListSets
		}

		# debug
		logger.debug(args)

		self.args = args.copy()
		self.request_timestamp = datetime.datetime.now()
		self.request_timestamp_string = self.request_timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')
		self.record_nodes = []

		# published dataframe slice parameters
		self.start = 0
		self.chunk_size = settings.OAI_RESPONSE_SIZE
		self.publish_set_id = None
		if 'set' in self.args.keys():
			self.publish_set_id = self.args['set']
		else:
			self.publish_set_id = None

		# get instance of Published model
		self.published = models.PublishedRecords()

		# begin scaffolding
		self.scaffold()


	# generate XML root node with OAI-PMH scaffolding
	def scaffold(self):

		# build root node, nsmap, and attributes		
		NSMAP = {
			None:'http://www.openarchives.org/OAI/2.0/'
		}
		self.root_node = etree.Element('OAI-PMH', nsmap=NSMAP)
		self.root_node.set('{http://www.w3.org/2001/XMLSchema-instance}schemaLocation', 'http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd')

		# set responseDate node
		self.responseDate_node = etree.Element('responseDate')
		self.responseDate_node.text = self.request_timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')
		self.root_node.append(self.responseDate_node)
		
		# set request node
		self.request_node = etree.Element('request')

		# set verb
		try:
			self.request_node.attrib['verb'] = self.args['verb']
		except:
			self.args['verb'] = 'NULL'
			self.request_node.attrib['verb'] = 'NULL'

		# capture set if present
		if 'set' in self.args.keys():
			self.request_node.attrib['set'] = self.args['set']

		# metadataPrefix
		if 'metadataPrefix' in self.args.keys():
			self.request_node.attrib['metadataPrefix'] = self.args['metadataPrefix']

		self.request_node.text = settings.COMBINE_OAI_ENDPOINT
		self.root_node.append(self.request_node)

		# set verb node		
		self.verb_node = etree.Element(self.args['verb'])
		self.root_node.append(self.verb_node)


	def retrieve_records(self, include_metadata=False):

		'''
		retrieve records from DB
		'''

		stime = time.time()
		logger.debug("retrieving records for verb %s" % (self.args['verb']))

		# if set present, filter by this set
		if self.publish_set_id:
			logger.debug('applying publish_set_id filter')
			self.published.records = self.published.records.filter(job__record_group__publish_set_id = self.publish_set_id)

		# loop through rows, limited by current OAI transaction start / chunk
		for record in self.published.records[self.start:(self.start+self.chunk_size)]:

			record = OAIRecord(args=self.args, record_id=record.oai_id, document=record.document, timestamp=self.request_timestamp_string)

			# include full metadata in record
			if include_metadata:
				 record.include_metadata()

			# append to record_nodes
			self.record_nodes.append(record.oai_record_node)

		# add to verb node
		for oai_record_node in self.record_nodes:
			self.verb_node.append(oai_record_node)

		# finally, set resumption token
		self.set_resumption_token()

		# report
		etime = time.time()
		logger.debug("%s record(s) returned in %sms" % (len(self.record_nodes), (float(etime) - float(stime)) * 1000))


	def set_resumption_token(self):

		'''
		resumption tokens are set in SQL under OAITransaction model
		'''

		# set resumption token
		if self.start + self.chunk_size < self.published.records.count():

			# set token and slice parameters to DB
			token = str(uuid.uuid4())
			logger.debug('setting resumption token: %s' % token)
			oai_trans = models.OAITransaction(
				verb = self.args['verb'],
				start = self.start + self.chunk_size,
				chunk_size = self.chunk_size,
				publish_set_id = self.publish_set_id,
				token = token,
				args = json.dumps(self.args)
			)
			oai_trans.save()

			# set resumption token node and attributes
			self.resumptionToken_node = etree.Element('resumptionToken')
			self.resumptionToken_node.attrib['expirationDate'] = (self.request_timestamp + datetime.timedelta(0,3600)).strftime('%Y-%m-%dT%H:%M:%SZ')
			self.resumptionToken_node.attrib['completeListSize'] = str(self.published.records.count())
			self.resumptionToken_node.attrib['cursor'] = str(self.start)
			self.resumptionToken_node.text = token
			self.verb_node.append(self.resumptionToken_node)


	# convenience function to run all internal methods
	def generate_response(self):

		# check verb
		if self.args['verb'] not in self.verb_routes.keys():
			return self.raise_error('badVerb','The verb %s is not allowed, must be from: %s' % (self.args['verb'],str(self.verb_routes.keys())) )

		# check for resumption token
		if 'resumptionToken' in self.args.keys():

			# retrieve token params and alter args and search_params
			ot_query = models.OAITransaction.objects.filter(token=self.args['resumptionToken'])
			if ot_query.count() == 1:				 
				ot = ot_query.first()

				# set args and start and chunk_size
				self.args = json.loads(ot.args)
				self.start = ot.start
				self.chunk_size = ot.chunk_size
				self.publish_set_id = ot.publish_set_id

				logger.debug('following resumption token, altering dataframe slice params:')
				logger.debug([self.start, self.chunk_size, self.publish_set_id])

			# raise error
			else:
				return self.raise_error('badResumptionToken', 'The resumptionToken %s is not found' % self.args['resumptionToken'])

		# fire verb reponse building
		self.verb_routes[self.args['verb']]()
		return self.serialize()


	def raise_error(self, error_code, error_msg):

		# remove verb node
		try:
			self.root_node.remove(self.verb_node)
		except:
			logger.debug('verb_node not found')

		# create error node and append
		error_node = etree.SubElement(self.root_node, 'error')
		error_node.attrib['code'] = error_code
		error_node.text = error_msg

		# serialize and return
		return self.serialize()


	# serialize record nodes as XML response
	def serialize(self):
		return etree.tostring(self.root_node)


	# GetRecord
	def _GetRecord(self):

		'''
		Retrieve a single record from the published dataframe
		'''
		
		stime = time.time()
		logger.debug("retrieving record: %s" % (self.args['identifier']))

		# get single row
		single_record = self.published.get_record(self.args['identifier'])

		# open as OAIRecord 
		record = OAIRecord(args=self.args, record_id=single_record.oai_id, document=single_record.document, timestamp=self.request_timestamp_string)

		# include metadata
		record.include_metadata()

		# append to record_nodes
		self.record_nodes.append(record.oai_record_node)

		# add to verb node
		for oai_record_node in self.record_nodes:
			self.verb_node.append(oai_record_node)

		# report
		etime = time.time()
		logger.debug("%s record(s) returned in %sms" % (len(self.record_nodes), (float(etime) - float(stime)) * 1000))


	# Identify
	def _Identify(self):

		# init OAIRecord
		logger.debug('generating identify node')
		
		# write Identify node
		description_node = etree.Element('description')
		description_node.text = 'Combine, integrated OAI-PMH'
		self.verb_node.append(description_node)


	# ListIdentifiers
	def _ListIdentifiers(self):

		self.retrieve_records()


	# ListMetadataFormats
	def _ListMetadataFormats(self):

		'''
		List all metadataformats, or optionally, available metadataformats for one item based on available metadata datastreams
		'''

		return self.raise_error('generic','At this time, metadataPrefixes are not implemented for the Combine OAI server')


	# ListRecords
	def _ListRecords(self):

		self.retrieve_records(include_metadata=True)


	# ListSets
	def _ListSets(self):

		# get sets by faceted search
		'''
		query for objects with OAI-PMH identifier, and belong to sets with rels_isMemberOfOAISet relationship,
		then focus on rels_isMemberOfOAISet facet for list of sets
		'''
		
		# generate response
		for publish_set_id in self.published.sets:
			set_node = etree.Element('set')
			setSpec = etree.SubElement(set_node,'setSpec')
			setSpec.text = publish_set_id
			setName = etree.SubElement(set_node,'setName')
			setName.text = publish_set_id
			self.verb_node.append(set_node)



class OAIRecord(object):

	'''
	Initialize OAIRecord with pid and args
	'''

	def __init__(self, args=None, record_id=None, document=None, timestamp=None):

		self.args = args
		self.record_id = record_id
		self.document = document
		self.timestamp = timestamp
		self.oai_record_node = None

		# build record node
		self.init_record_node()


	def init_record_node(self):

		# init node
		# TODO: need to add additional namespace here, see https://github.com/WSULib/ouroboros/issues/59
		self.oai_record_node = etree.Element('record')

		# header node
		header_node = etree.Element('header')
		
		# identifier 
		identifier_node = etree.Element('identifier')
		identifier_node.text = self.record_id
		header_node.append(identifier_node)

		# datestamp
		datestamp_node = etree.Element('datestamp')
		datestamp_node.text = self.timestamp
		header_node.append(datestamp_node)

		if 'set' in self.args.keys():
			setSpec_node = etree.Element('setSpec')
			setSpec_node.text = self.args['set']
			header_node.append(setSpec_node)

		self.oai_record_node.append(header_node)


	def include_metadata(self):

		# metadate node
		metadata_node = etree.Element('metadata')
		metadata_node.append(etree.fromstring(self.document))
		self.oai_record_node.append(metadata_node)





