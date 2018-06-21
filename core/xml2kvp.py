# xml2kvp

from collections import OrderedDict
from lxml import etree
import logging
from pprint import pprint, pformat
import xmltodict

# init logger
logger = logging.getLogger(__name__)

class XML2kvp(object):

	'''
	Class to handle the parsing of XML into Key/Value Pairs

		- utilizes xmltodict (https://github.com/martinblech/xmltodict)			
		- static methods are designed to be called without user instantiating 
		instance of XML2kvp
	'''

	# demo xml
	test_xml = '''<?xml version="1.0" encoding="UTF-8"?>
<root>
	<foo>
		<bar>42</bar>
		<baz>109</baz>
	</foo>
	<foo>
		<bar>42</bar>
		<baz>109</baz>
	</foo>
	<foo>
		<bar>9393943</bar>
		<baz>3489234893</baz>
	</foo>
	<tronic type='tonguetwister'>Sally sells seashells by the seashore.</tronic>
	<tronic type='tonguetwister'>Red leather, yellow leather.</tronic>
	<tronic>You may disregard</tronic>
	<goober scrog='true' tonk='false'>
		<depths>
			<plunder>Willy Wonka</plunder>
		</depths>
	</goober>
	<nested_attribs type='first'>
		<another type='second'>paydirt</another>
	</nested_attribs>
</root>
	'''

	class DelimiterCollision(Exception):
		pass


	def __init__(self, **kwargs):

		'''
		Args
			kwargs (dict): Accepts named args from static methods
		'''

		# set overwritable class attributes
		self.xml_attribs = True
		self.node_delim = '_'
		self.ns_prefix_delim = '|'
		self.error_on_delims_collision = False

		# overwite with attributes from static methods
		for k,v in kwargs.items():
			setattr(self, k, v)

		# set non-overwritable class attributes
		self.kvp_dict = {}
		self.k_xpath_dict = {}


	
	def xml_dict_parser(self, in_k, in_v, hops=[]):

		if type(in_v) == OrderedDict:		

			hop_len = len(hops)
			for k, v in in_v.items():

				# add key to hops
				if k == '#text':
					self._process_kvp(hops, v)

				else:				
					if k.startswith('@'):
						hops.append(self._check_hop('%s=%s' % (k, v)))
					else:
						hops.append(self._check_hop(k))

						# recurse
						self.xml_dict_parser(k, v, hops=hops)

						# reset hops
						hops = hops[:hop_len]

		elif type(in_v) == list:

			hop_len = len(hops)
			for d in in_v:
				self.xml_dict_parser(None, d, hops=hops)
				hops = hops[:hop_len]

		elif type(in_v) in [str,int]:

			if in_k != '#text':
				self._process_kvp(hops, in_v)


	def _check_hop(self, hop):

		# if erroring on collision
		if self.error_on_delims_collision:
			if not set([self.node_delim, self.ns_prefix_delim]).isdisjoint(hop):				
				raise self.DelimiterCollision('collision for key: "%s", collides with configured delimiters: %s' % 
					(hop, {'node_delim':self.node_delim, 'ns_prefix_delim':self.ns_prefix_delim}))
		
		return hop
		

	def _process_kvp(self, hops, value):

		'''
		method to add key/value pairs to saved dictionary,
		appending new values to pre-existing keys
		'''

		# gen key
		k = self.node_delim.join(hops)

		# new key, new value
		if k not in self.kvp_dict.keys():
			self.kvp_dict[k] = value

		# pre-existing, but not yet list, convert
		elif k in self.kvp_dict.keys() and type(self.kvp_dict[k]) != list:

			if self.skip_repeating_values and value == self.kvp_dict[k]:
				pass				
			else:
				tval = self.kvp_dict[k]
				self.kvp_dict[k] = [tval, value]

		# already list, append
		else:
			if not self.skip_repeating_values or value not in self.kvp_dict[k]:
				self.kvp_dict[k].append(value)		


	def parse_xml_input(self, xml_input):

		# if string, save
		if type(xml_input) == str:
			return xml_input

		# if etree object, to string and save
		if type(xml_input) in [etree._Element, etree._ElementTree]:
			return etree.tostring(xml_input).decode('utf-8')


	@staticmethod
	def xml_to_kvp(
		xml_input,
		xml_attribs=True,
		node_delim='_',
		ns_prefix_delim='|',
		mixins = {
			'copy_to':None,
			'literals':None
		},
		skip_repeating_values=True,
		error_on_delims_collision=False,
		handler=None,
		return_handler=False):

		# init handler
		if not handler:
			handler = XML2kvp(
				xml_attribs=xml_attribs,
				node_delim=node_delim,
				ns_prefix_delim=ns_prefix_delim,
				skip_repeating_values=skip_repeating_values,
				error_on_delims_collision=error_on_delims_collision)

		# parse xml input
		handler.xml_string = handler.parse_xml_input(xml_input)

		# parse as dictionary
		handler.xml_dict = xmltodict.parse(handler.xml_string, xml_attribs=handler.xml_attribs)

		# walk xmltodict parsed dictionary and reutnr
		handler.xml_dict_parser(None, handler.xml_dict, hops=[])

		# handle literal mixins
		if mixins['literals']:
			for k,v in mixins['literals'].items():
				handler.kvp_dict[k] = v

		# return
		if return_handler:
			return handler
		else:
			return handler.kvp_dict


	@staticmethod
	def kvp_to_xml():
		pass


	@staticmethod
	def k_to_xpath(
		k,
		node_delim='_',
		ns_prefix_delim='|',
		skip_root=False,
		handler=None,
		return_handler=False):

		'''
		Method to derive xpath from kvp key
		'''

		# init handler
		if not handler:
			handler = XML2kvp(			
				node_delim=node_delim,
				ns_prefix_delim=ns_prefix_delim)

		# for each column, reconstitue columnName --> XPath				
		k_parts = k.split(handler.node_delim)
		if skip_root:
			k_parts = k_parts[1:]

		# loop through pieces and build xpath
		on_attrib = False
		xpath = '/' # begin with single slash, will get appended to

		for part in k_parts:

			# if not attribute, assume node hop
			if not part.startswith('@'):

				# handle closing attrib if present
				if on_attrib:
					xpath += ']/'
					on_attrib = False

				# close previous element
				else:
					xpath += '/'
			
				# replace pipe with colon for prefix
				part = part.replace(handler.ns_prefix_delim,':')

				# append to xpath string
				xpath += '%s' % part

			# if attribute, assume part of previous element and build
			else:

				# handle attribute
				attrib, value = part.split('=')

				# if not on_attrib, open xpath for attribute inclusion
				if not on_attrib:
					xpath += "[%s='%s'" % (attrib, value)

				# else, currently in attribute write block, continue
				else:
					xpath += " and %s='%s'" % (attrib, value)

				# set on_attrib flag for followup
				on_attrib = True

		# cleanup after loop
		if on_attrib:

			# close attrib brackets
			xpath += ']'

		# save to handler
		handler.k_xpath_dict[k] = xpath

		# return
		if return_handler:
			return handler
		else:
			return xpath


	@staticmethod
	def kvp_to_xpath(
		kvp,
		node_delim='_',
		ns_prefix_delim='|',
		handler=None,
		return_handler=False):

		# init handler
		if not handler:
			handler = XML2kvp(			
				node_delim=node_delim,
				ns_prefix_delim=ns_prefix_delim)

		# handle forms of kvp
		if type(kvp) == str:
			handler.kvp_dict = json.loads(kvp)
		if type(kvp) == dict:
			handler.kvp_dict = kvp

		# loop through and append to handler
		for k,v in handler.kvp_dict.items():
			XML2kvp.k_to_xpath(k, handler=handler)

		# return
		if return_handler:
			return handler
		else:
			return handler.k_xpath_dict



	@staticmethod
	def run_tests():

		# return handler of xml_to_kvp
		logger.debug('running tests with test XML...')
		logger.debug(XML2kvp.test_xml)

		# parse to kvp
		logger.debug('converting xml to kvp, default options...')
		h = XML2kvp.xml_to_kvp(XML2kvp.test_xml, return_handler=True)
		logger.debug(pformat(h.kvp_dict))

		# parse to kvp
		logger.debug('converting xml to kvp, default options...')
		h = XML2kvp.xml_to_kvp(XML2kvp.test_xml, return_handler=True)
		logger.debug(pformat(h.kvp_dict))

		







