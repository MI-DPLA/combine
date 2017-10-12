# python modules
from concurrent.futures.thread import ThreadPoolExecutor
import datetime
import hashlib
import json
import logging
from lxml import etree
import time

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

		# debug
		logger.debug("################ OAIPROVIDER INIT ################")
		logger.debug(args)
		logger.debug("################ /OAIPROVIDER INIT ################")

		self.args = args
		self.request_timestamp = datetime.datetime.now()
		self.request_timestamp_string = self.request_timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')
		self.record_nodes = []

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
		self.request_node.attrib['verb'] = self.args['verb']
		if 'set' in self.args.keys():
			self.request_node.attrib['set'] = self.args['set']
		if 'metadataPrefix' in self.args.keys():
			self.request_node.attrib['metadataPrefix'] = self.args['metadataPrefix']
		self.request_node.text = settings.COMBINE_OAI_ENDPOINT
		self.root_node.append(self.request_node)

		# set verb node		
		self.verb_node = etree.Element(self.args['verb'])
		self.root_node.append(self.verb_node)


	def retrieve_records(self, include_metadata=False):

		'''
		asynchronous record retrieval from Fedora
		'''

		stime = time.time()
		logger.debug("retrieving records for verb %s" % (self.args['verb']))

		# loop through rows
		for i, row in enumerate(self.published.df.iterrows()):
			record = OAIRecord(args=self.args, record_id=row[1].id, document=row[1].document, timestamp=self.request_timestamp_string)

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

		# # set resumption token
		# if self.search_params['start'] + self.search_params['rows'] < self.search_results.total_results:

		# 	# prepare token
		# 	token = hashlib.md5(self.request_timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')).hexdigest()
		# 	self.search_params['start'] = self.search_params['start'] + self.search_params['rows'] 
		# 	redisHandles.r_oai.setex(token, 3600, json.dumps({'args':self.args,'search_params':self.search_params}))

		# 	# set resumption token node and attributes
		# 	self.resumptionToken_node = etree.Element('resumptionToken')
		# 	self.resumptionToken_node.attrib['expirationDate'] = (self.request_timestamp + datetime.timedelta(0,3600)).strftime('%Y-%m-%dT%H:%M:%SZ')
		# 	self.resumptionToken_node.attrib['completeListSize'] = str(self.search_results.total_results)
		# 	self.resumptionToken_node.attrib['cursor'] = str(self.search_results.start)
		# 	self.resumptionToken_node.text = token
		# 	self.verb_node.append(self.resumptionToken_node)

		pass


	# convenience function to run all internal methods
	def generate_response(self):

		# read args, route verb to verb handler
		verb_routes = {
			'GetRecord':self._GetRecord,
			'Identify':self._Identify,
			'ListIdentifiers':self._ListIdentifiers,
			'ListMetadataFormats':self._ListMetadataFormats,
			'ListRecords':self._ListRecords,
			'ListSets':self._ListSets
		}

		# check verb
		if self.args['verb'] not in verb_routes.keys():
			return self.raise_error('badVerb','The verb %s is not allowed, must be from: %s' % (self.args['verb'],str(verb_routes.keys())) )

		# check for resumption token
		if 'resumptionToken' in self.args.keys():
			logger.debug('following resumption token, altering search_params')
			# retrieve token params and alter args and search_params
			resumption_params_raw = redisHandles.r_oai.get(self.args['resumptionToken'])
			if resumption_params_raw is not None:
				resumption_params = json.loads(resumption_params_raw)
				self.args = resumption_params['args']
				self.search_params = resumption_params['search_params']
			# raise error
			else:
				return self.raise_error('badResumptionToken', 'The resumptionToken %s is not found' % self.args['resumptionToken'])

		# fire verb reponse building
		verb_routes[self.args['verb']]()
		return self.serialize()


	def raise_error(self, error_code, error_msg):

		# remove verb node
		self.root_node.remove(self.verb_node)

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
		
		self.search_params['q'] = 'rels_itemID:%s' % self.args['identifier'].replace(":","\:")
		self.retrieve_records(include_metadata=True)


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

		# identifier provided
		if self.args['identifier']:
			try:
				logger.debug("identifier provided for ListMetadataFormats, confirming that identifier exists...")
				search_results = solr_handle.search(**{'q':'rels_itemID:%s' % self.args['identifier'].replace(":","\:"),'fl':['id']})
				if search_results.total_results > 0:
					fedora_object = fedora_handle.get_object(search_results.documents[0]['id'])
					if fedora_object.exists:
						logger.debug("record found, returning allowed metadataPrefixs")
						for ds in fedora_object.ds_list:
							if ds in inv_metadataPrefix_hash.keys():

								mf_node = etree.Element('metadataFormat')

								# write metadataPrefix node
								prefix = etree.SubElement(mf_node,'metadataPrefix')
								prefix.text = inv_metadataPrefix_hash[ds]['prefix']

								# write schema node
								schema = etree.SubElement(mf_node,'schema')
								schema.text = inv_metadataPrefix_hash[ds]['schema']

								# write schema node
								namespace = etree.SubElement(mf_node,'metadataNamespace')
								namespace.text = inv_metadataPrefix_hash[ds]['namespace']

								# append to verb_node and return
								self.verb_node.append(mf_node)
					else:
						raise Exception('record could not be located')		
				else:
					raise Exception('record could not be located')
			except:
				return self.raise_error('idDoesNotExist','The identifier %s is not found.' % self.args['identifier'])
			
		# no identifier, return all available metadataPrefixes
		else:
			# iterate through available metadataFormats and return
			for mf in metadataPrefix_hash.keys():

				mf_node = etree.Element('metadataFormat')

				# write metadataPrefix node
				prefix = etree.SubElement(mf_node,'metadataPrefix')
				prefix.text = mf

				# write schema node
				schema = etree.SubElement(mf_node,'schema')
				schema.text = metadataPrefix_hash[mf]['schema']

				# write schema node
				namespace = etree.SubElement(mf_node,'metadataNamespace')
				namespace.text = metadataPrefix_hash[mf]['namespace']

				# append to verb_node and return
				self.verb_node.append(mf_node)


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
		# determine sets
		search_results = solr_handle.search(**{
				'q':'*:*',
				'fq':['rels_itemID:*','rels_isMemberOfOAISet:*'],
				'rows':0,
				'facet':True,
				'facet.field':'rels_isMemberOfOAISet'
			})

		# generate response
		for oai_set in [k for k in search_results.facets['facet_fields']['rels_isMemberOfOAISet'].keys() if k.startswith('wayne')]:
			set_node = etree.Element('set')
			setSpec = etree.SubElement(set_node,'setSpec')
			setSpec.text = oai_set
			setName = etree.SubElement(set_node,'setName')
			setName.text = oai_set
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





