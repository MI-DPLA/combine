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
from django.urls import reverse

# import models
from core import models

# Get an instance of a logger
logger = logging.getLogger(__name__)


# attempt to load metadataPrefix map from localConfig, otherwise provide default
if hasattr(settings, 'METADATA_PREFIXES'):
	metadataPrefix_hash = settings.METADATA_PREFIXES
else:	
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
	Class for scaffolding and building responses to OAI queries

	NOTE: Because the OAI-PMH protocol shares verbs with reserved words in Python (e.g. "set", or "from"),
	easier to keep the HTTP request args to work with as a dictionary, and maintain the original OAI-PMH vocab.
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

		self.args = args.copy()
		self.request_timestamp = datetime.datetime.now()
		self.request_timestamp_string = self.request_timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')
		self.record_nodes = []

		# published dataframe slice parameters
		self.start = 0
		self.chunk_size = settings.OAI_RESPONSE_SIZE
		self.publish_set_id = None
		if 'set' in self.args.keys() and self.args['set'] != '':
			self.publish_set_id = self.args['set']
		else:
			self.publish_set_id = None

		# get instance of Published model
		self.published = models.PublishedRecords()

		# begin scaffolding
		self.scaffold()


	# generate XML root node with OAI-PMH scaffolding
	def scaffold(self):

		'''
		Scaffold XML, OAI response

		Args:
			None

		Returns:
			None
				- sets multiple attributes for response building
		'''

		# build root node, nsmap, and attributes		
		NSMAP = {
			None:'http://www.openarchives.org/OAI/2.0/'
		}
		self.root_node = etree.Element('OAI-PMH', nsmap=NSMAP)
		self.root_node.set(
				'{http://www.w3.org/2001/XMLSchema-instance}schemaLocation',
				'http://www.openarchives.org/OAI/2.0/ http://www.openarchives.org/OAI/2.0/OAI-PMH.xsd'
			)

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

		self.request_node.text = 'http://%s%s' % (settings.APP_HOST, reverse('oai'))
		self.root_node.append(self.request_node)

		# set verb node		
		self.verb_node = etree.Element(self.args['verb'])
		self.root_node.append(self.verb_node)


	def retrieve_records(self, include_metadata=False):

		'''
		Retrieve record(s) from DB for response

		Args:
			include_metadata (bool): If False, return only identifiers, if True, include record document as well

		Returns:
			None
				- adds record(s) to self.record_nodes
		'''

		stime = time.time()
		logger.debug("retrieving records for verb %s" % (self.args['verb']))

		# get records
		records = self.published.records

		# if set present, filter by this set
		if self.publish_set_id:
			logger.debug('applying publish_set_id filter: %s' % self.publish_set_id)			
			records = records.filter(publish_set_id = self.publish_set_id)

		# loop through rows, limited by current OAI transaction start / chunk
		
		# count records before slice
		records_count = records.count()
		
		# get slice for iteration
		records = records[self.start:(self.start+self.chunk_size)]				
		for record in records.iterator():

			record = OAIRecord(
					args=self.args,
					record_id=record.record_id,
					publish_set_id=record.publish_set_id,
					document=record.document,					
					timestamp=self.request_timestamp_string
				)
			
			# include full metadata in record
			if include_metadata:
				 record.include_metadata()

			# append to record_nodes
			self.record_nodes.append(record.oai_record_node)

		# add to verb node
		for oai_record_node in self.record_nodes:
			self.verb_node.append(oai_record_node)

		# finally, set resumption token		
		self.set_resumption_token(records, completeListSize=records_count)

		# report		
		record_nodes_num = len(self.record_nodes)		
		logger.debug("%s record(s) returned in %s" % (record_nodes_num, (float(time.time()) - float(stime))))


	def set_resumption_token(self, records, completeListSize=None):

		'''
		Set resumption tokens in DB under OAITransaction model

		Args:
			completeListSize (int): total number of records based on passed parameters

		Returns:
			None
				- sets attributes related to resumption tokens
		'''

		# set resumption token
		if self.start + self.chunk_size < completeListSize:

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
			self.resumptionToken_node.attrib['expirationDate'] = (self.request_timestamp + datetime.timedelta(0,3600))\
			.strftime('%Y-%m-%dT%H:%M:%SZ')
			self.resumptionToken_node.attrib['completeListSize'] = str(completeListSize)
			self.resumptionToken_node.attrib['cursor'] = str(self.start)
			self.resumptionToken_node.text = token
			self.verb_node.append(self.resumptionToken_node)


	# convenience function to run all internal methods
	def generate_response(self):

		'''
		Returns OAI response as XML

		Args:
			None

		Returns:
			(str): XML response
		'''

		# check verb
		if self.args['verb'] not in self.verb_routes.keys():
			return self.raise_error(
					'badVerb',
					'The verb %s is not allowed, must be from: %s' % (self.args['verb'],str(self.verb_routes.keys()))
				)

		# check for resumption token
		if 'resumptionToken' in self.args.keys():

			# retrieve token params and alter args and search_params
			ot_query = models.OAITransaction.objects.filter(token=self.args['resumptionToken'])
			if ot_query.count() == 1:				 
				ot = ot_query.first()

				# set args and start and chunk_size
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

		'''
		Returns error as XML, OAI response

		Args:
			error_code (str): OAI-PMH error codes (e.g. badVerb, generic, etc.)
			error_msg (str): details about error

		Returns:
			(str): XML response
		'''

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

		'''
		Serialize all nodes as XML for returning

		Args:
			None

		Returns:
			(str): XML response
		'''

		return etree.tostring(self.root_node)


	# GetRecord
	def _GetRecord(self):

		'''
		OAI-PMH verb: GetRecord
		Retrieve a single record based on record id, return

		Args:
			None

		Returns:
			None
				sets single record node to self.record_nodes
		'''
		
		stime = time.time()
		logger.debug("retrieving record: %s" % (self.args['identifier']))

		# get single row
		single_record = self.published.get_record(self.args['identifier'])

		# if single record found
		if single_record:

			# open as OAIRecord 
			record = OAIRecord(
					args=self.args,
					record_id=single_record.record_id,
					document=single_record.document,
					timestamp=self.request_timestamp_string
				)

			# include metadata
			record.include_metadata()

			# append to record_nodes
			self.record_nodes.append(record.oai_record_node)

			# add to verb node
			for oai_record_node in self.record_nodes:
				self.verb_node.append(oai_record_node)

		else:
			logger.debug('record not found for id: %s, not appending node' % self.args['identifier'])

		# report
		etime = time.time()
		logger.debug("%s record(s) returned in %sms" % (len(self.record_nodes), (float(etime) - float(stime)) * 1000))


	# Identify
	def _Identify(self):

		'''
		OAI-PMH verb: Identify
		Provide information about Repository / OAI Server

		Args:
			None

		Returns:
			None
				sets description node text
		'''

		# init OAIRecord
		logger.debug('generating identify node')
		
		# write Identify node
		description_node = etree.Element('description')
		description_node.text = 'Combine, integrated OAI-PMH'
		self.verb_node.append(description_node)


	# ListIdentifiers
	def _ListIdentifiers(self):

		'''
		OAI-PMH verb: ListIdentifiers
		Lists identifiers

		Args:
			None

		Returns:
			None
				sets multiple record nodes to self.record.nodes
		'''

		self.retrieve_records()


	# ListMetadataFormats
	def _ListMetadataFormats(self):

		'''
		OAI-PMH verb: ListMetadataFormats
		List all metadataformats, or optionally, available metadataformats for
		one item based on available metadata datastreams

		NOTE: Currently not implemented.  Metadata prefixes are ignored entirely.

		Args:
			None

		Returns:
			None
				sets multiple record nodes to self.record.nodes
		'''

		return self.raise_error('generic','At this time, metadataPrefixes are not implemented for the Combine OAI server')


	# ListRecords
	def _ListRecords(self):

		'''
		OAI-PMH verb: ListRecords
		Lists records; similar to ListIdentifiers, but includes metadata from record.document

		Args:
			None

		Returns:
			None
				sets multiple record nodes to self.record.nodes
		'''

		self.retrieve_records(include_metadata=True)


	# ListSets
	def _ListSets(self):

		'''
		OAI-PMH verb: ListSets
		Lists available sets.  Sets are derived from the associated Record Groups of all Publish jobs.

		Args:
			None

		Returns:
			None
				sets multiple set nodes
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

	def __init__(self, args=None, record_id=None, publish_set_id=None, document=None, timestamp=None):

		self.args = args
		self.record_id = record_id
		self.publish_set_id = publish_set_id
		self.document = document
		self.timestamp = timestamp
		self.oai_record_node = None

		# build record node
		self.init_record_node()


	def _construct_oai_identifier(self):

		'''
		build OAI identifier
		'''

		# if publish set id include
		if self.publish_set_id:
			return '%s:%s:%s' % (settings.COMBINE_OAI_IDENTIFIER, self.publish_set_id, self.record_id)

		# else, without
		else:
			return '%s:%s' % (settings.COMBINE_OAI_IDENTIFIER, self.record_id)


	def init_record_node(self):

		'''
		Initialize and scaffold record node

		Args:
			None

		Returns:
			None
				sets self.oai_record_node
		'''

		# init node
		self.oai_record_node = etree.Element('record')

		# header node
		header_node = etree.Element('header')
		
		# identifier 
		identifier_node = etree.Element('identifier')
		identifier_node.text = self._construct_oai_identifier()
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

		'''
		Method to retrieve metadata from record.document, and include in XML response (for GetRecord and ListRecords)

		Args:
			None

		Returns:
			None
				sets self.oai_record_node
		'''

		# metadate node
		metadata_node = etree.Element('metadata')
		metadata_node.append(etree.fromstring(self.document.encode('utf-8')))
		self.oai_record_node.append(metadata_node)





