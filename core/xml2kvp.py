# xml2kvp

from collections import OrderedDict
import json
from lxml import etree
import logging
from pprint import pprint, pformat
import re
import time
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
<root xmlns:internet="http://internet.com">
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
	<nested>
		<empty></empty>
	</nested>
	<internet:url url='http://example.com'>see my url</internet:url>	
</root>
	'''


	class DelimiterCollision(Exception):
		pass



	def __init__(self, **kwargs):

		'''
		Args
			kwargs (dict): Accepts named args from static methods
		'''

		# defaults, overwritten by methods
		self.as_tuples=True
		self.concat_values_on_all_fields=False
		self.concat_values_on_fields={}
		self.copy_to={}
		self.copy_to_regex={}
		self.error_on_delims_collision=True
		self.exclude_attributes=[]
		self.exclude_elements=[]
		self.include_attributes=True
		self.include_meta=False
		self.include_xml_prop=False
		self.literals={}
		self.node_delim='___'
		self.ns_prefix_delim='|'
		self.remove_copied_key=True
		self.remove_ns_prefix=False
		self.self_describing=False
		self.split_values_on_all_fields=False
		self.split_values_on_fields={}
		self.skip_attribute_ns_declarations=True
		self.skip_repeating_values=True
		self.skip_root=False

		# list of properties that are allowed to be overwritten with None
		arg_none_allowed = []

		# overwite with attributes from static methods
		for k,v in kwargs.items():
			if v is not None or k in arg_none_allowed:
				setattr(self, k, v)

		# set non-overwritable class attributes
		self.kvp_dict = {}
		self.k_xpath_dict = {}


	@property
	def config_json(self):

		config_dict = { k:v for k,v in self.__dict__.items() if k in [
			'concat_values_on_all_fields',
			'concat_values_on_fields',
			'copy_to',
			'copy_to_regex',
			'error_on_delims_collision',
			'exclude_attributes',
			'exclude_elements',
			'include_attributes',
			'literals',
			'node_delim',
			'ns_prefix_delim',
			'remove_copied_key',
			'remove_ns_prefix',
			'self_describing',
			'split_values_on_all_fields',
			'split_values_on_fields',
			'skip_attribute_ns_declarations',
			'skip_repeating_values',
			'skip_root'
		] }

		return json.dumps(config_dict, indent=2, sort_keys=True)


	def _xml_dict_parser(self, in_k, in_v, hops=[]):

		if type(in_v) == OrderedDict:		

			hop_len = len(hops)
			for k, v in in_v.items():

				# add key to hops
				if k == '#text':
					self._process_kvp(hops, v)

				else:				
					if k.startswith('@'):
						hops = self._format_and_append_hop(hops, 'attribute', k, v)
					else:
						hops = self._format_and_append_hop(hops, 'element', k, None)

						# recurse
						self._xml_dict_parser(k, v, hops=hops)

						# reset hops
						hops = hops[:hop_len]

		elif type(in_v) == list:

			hop_len = len(hops)
			for d in in_v:

				# recurse
				self._xml_dict_parser(None, d, hops=hops)
				
				# drop hops back one
				hops = hops[:hop_len]

		elif type(in_v) in [str,int]:

			if in_k != '#text':
				self._process_kvp(hops, in_v)


	def _format_and_append_hop(self, hops, hop_type, k, v):

		# handle elements
		if hop_type == 'element':

			# if erroring on collision
			if self.error_on_delims_collision:			
				self._check_delims_collision(k)

			# if skipping elements
			if len(self.exclude_elements) > 0:
				if k in self.exclude_elements:
					return hops
			
			# apply namespace delimiter
			if not self.remove_ns_prefix:
				hop = k.replace(':', self.ns_prefix_delim)
			else:
				if ':' in k:
					hop = k.split(':')[1]
				else:
					hop = k

		# handle elements
		if hop_type == 'attribute':

			# skip attribute namespace declarations
			if self.skip_attribute_ns_declarations:
				if k.startswith(('@xmlns', '@xsi')):
					return hops

			# if skipping attributes
			if len(self.exclude_attributes) > 0:
				if k.lstrip('@') in self.exclude_attributes:
					return hops

			# if erroring on collision
			if self.error_on_delims_collision:			
				self._check_delims_collision(k)
				self._check_delims_collision(v)
			
			# apply namespace delimiter
			k = k.replace(':', self.ns_prefix_delim)

			# combine
			hop = '%s=%s' % (k, v)

		# append and return
		hops.append(hop)
		return hops


	def _check_delims_collision(self, value):

		if any(delim in value for delim in [self.node_delim, self.ns_prefix_delim]):
			raise self.DelimiterCollision('collision for key value: "%s", collides with a configured delimiter: %s' % 
				(value, {'node_delim':self.node_delim, 'ns_prefix_delim':self.ns_prefix_delim}))
		

	def _process_kvp(self, hops, value):

		'''
		method to add key/value pairs to saved dictionary,
		appending new values to pre-existing keys
		'''

		# join on node delimiter
		k = self.node_delim.join(hops)

		# add delims suffix
		if self.self_describing:
			k = "%(k)s%(node_delim)s%(node_delim_len)s%(ns_prefix_delim)s%(ns_prefix_delim_len)s" % {
				'k':k,
				'node_delim':self.node_delim,
				'node_delim_len':len(self.node_delim),
				'ns_prefix_delim':self.ns_prefix_delim,
				'ns_prefix_delim_len':len(self.ns_prefix_delim)
			}

		# init k_list
		k_list = [k]

		# handle copy_to mixins
		if len(self.copy_to) > 0:
			slen = len(k_list)
			k_list.extend([ cv for ck, cv in self.copy_to.items() if ck == k ])
			if self.remove_copied_key:
				if slen != len(k_list) and k in k_list:
					k_list.remove(k)

		# handle copy_to_regex mixins
		if len(self.copy_to_regex) > 0:

			slen = len(k_list)

			# loop through copy_to_regex
			for rk, rv in self.copy_to_regex.items():

				# attempt sub
				try:
					sub = re.sub(rk, rv, k)
					if sub != k:
						k_list.append(sub)
				except:
					pass

			if self.remove_copied_key:
				if slen != len(k_list) and k in k_list:
					k_list.remove(k)

		# loop through keys
		for k in k_list:

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


	def _split_and_concat_fields(self):

		'''
		Method to group actions related to splitting and concatenating field values
		'''

		# concat values on all fields
		if self.concat_values_on_all_fields:
			for k,v in self.kvp_dict.items():
				if type(v) == list:
					self.kvp_dict[k] = self.concat_values_on_all_fields.join(v)

		# concat values on select fields
		if not self.concat_values_on_all_fields and len(self.concat_values_on_fields) > 0:
			for k,v in self.concat_values_on_fields.items():
				if k in self.kvp_dict.keys() and type(self.kvp_dict[k]) == list:					
					self.kvp_dict[k] = v.join(self.kvp_dict[k])

		# split values on all fields
		if self.split_values_on_all_fields:
			for k,v in self.kvp_dict.items():
				if type(v) == str:
					self.kvp_dict[k] = v.split(self.split_values_on_all_fields)

		# split values on select fields
		if not self.split_values_on_all_fields and len(self.split_values_on_fields) > 0:
			for k,v in self.split_values_on_fields.items():
				if k in self.kvp_dict.keys() and type(self.kvp_dict[k]) == str:					
					self.kvp_dict[k] = self.kvp_dict[k].split(v)


	def _parse_xml_input(self, xml_input):

		# if string, save
		if type(xml_input) == str:
			if self.include_xml_prop:
				self.xml = etree.fromstring(xml_input)
				self._parse_nsmap()
			return xml_input

		# if etree object, to string and save
		if type(xml_input) in [etree._Element, etree._ElementTree]:
			if self.include_xml_prop:
				self.xml = xml_input
				self._parse_nsmap()
			return etree.tostring(xml_input).decode('utf-8')


	def _parse_nsmap(self):

		# get namespace map, popping None values
		_nsmap = self.xml.nsmap.copy()
		try:
			global_ns = _nsmap.pop(None)
			_nsmap['global_ns'] = ns0
		except:
			pass
		self.nsmap = _nsmap


	@staticmethod
	def xml_to_kvp(
		xml_input,

		# flow control
		handler=None,
		return_handler=False,

		# kwargs
		as_tuples=True,
		concat_values_on_all_fields=None,
		concat_values_on_fields=None,
		copy_to = None,
		copy_to_regex = None,
		error_on_delims_collision=True,
		exclude_attributes=None,
		exclude_elements=None,
		include_attributes=None,
		include_meta=None,
		include_xml_prop=None,
		literals = None,
		node_delim=None,
		ns_prefix_delim=None,		
		remove_copied_key=None,
		remove_ns_prefix=None,		
		self_describing=None,
		split_values_on_all_fields=None,
		split_values_on_fields=None,
		skip_attribute_ns_declarations=True,
		skip_repeating_values=True,
		skip_root=None):

		# init handler, overwriting defaults if not None
		if not handler:
			handler = XML2kvp(
				as_tuples=as_tuples,
				concat_values_on_all_fields=concat_values_on_all_fields,
				concat_values_on_fields=concat_values_on_fields,
				copy_to=copy_to,
				copy_to_regex=copy_to_regex,
				error_on_delims_collision=error_on_delims_collision,
				exclude_attributes=exclude_attributes,
				exclude_elements=exclude_elements,
				include_attributes=include_attributes,
				include_meta=include_meta,
				include_xml_prop=include_xml_prop,
				literals=literals,
				node_delim=node_delim,
				ns_prefix_delim=ns_prefix_delim,		
				remove_copied_key=remove_copied_key,
				remove_ns_prefix=remove_ns_prefix,
				self_describing=self_describing,
				split_values_on_all_fields=split_values_on_all_fields,
				split_values_on_fields=split_values_on_fields,
				skip_attribute_ns_declarations=skip_attribute_ns_declarations,
				skip_repeating_values=skip_repeating_values,
				skip_root=skip_root)

		# clean kvp_dict
		handler.kvp_dict = {}

		# parse xml input
		handler.xml_string = handler._parse_xml_input(xml_input)

		# parse as dictionary
		handler.xml_dict = xmltodict.parse(handler.xml_string, xml_attribs=handler.include_attributes)

		# walk xmltodict parsed dictionary and reutnr
		handler._xml_dict_parser(None, handler.xml_dict, hops=[])

		# handle literal mixins
		if len(handler.literals) > 0:
			for k,v in handler.literals.items():
				handler.kvp_dict[k] = v

		# handle split and concatenations
		handler._split_and_concat_fields()

		# convert list to tuples if flagged
		if as_tuples:
			# convert all lists to tuples
			for k,v in handler.kvp_dict.items():
				if type(v) == list:
					handler.kvp_dict[k] = tuple(v)

		# include metadata about delimeters
		if include_meta:
			handler.kvp_dict['xml2kvp_meta'] = json.dumps({
					'node_delim':node_delim,
					'ns_prefix_delim':ns_prefix_delim
				})

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

		# flow control
		handler=None,
		return_handler=False,

		# kwargs
		node_delim=None,
		ns_prefix_delim=None,
		skip_root=None):

		'''
		Method to derive xpath from kvp key
		'''

		# init handler
		if not handler:
			handler = XML2kvp(			
				node_delim=node_delim,
				ns_prefix_delim=ns_prefix_delim,
				skip_root=skip_root)

		# for each column, reconstitue columnName --> XPath				
		k_parts = k.split(handler.node_delim)
		if handler.skip_root:
			k_parts = k_parts[1:]

		# set initial on_attrib flag
		on_attrib = False

		# init path string
		if not handler.skip_root:
			xpath = ''
		else:
			xpath = '/' # begin with single slash, will get appended to

		# loop through pieces and build xpath
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
			
				# replace delimiter with colon for prefix
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
		node_delim=None,
		ns_prefix_delim=None,
		skip_root=None,
		handler=None,
		return_handler=False):

		# init handler
		if not handler:
			handler = XML2kvp(			
				node_delim=node_delim,
				ns_prefix_delim=ns_prefix_delim,
				skip_root=skip_root)

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


	def test_kvp_to_xpath_roundtrip(self):

		# http://goodmami.org/2015/11/04/python-xpath-and-default-namespaces.html

		# check for self.xml and self.nsmap
		if not self.xml:
			self.xml = etree.fromstring(self.xml_string)
		if not self.nsmap:
			self._parse_nsmap()

		# generate xpaths values
		self = XML2kvp.kvp_to_xpath(self.kvp_dict, handler=self, return_handler=True)

		for k,v in self.k_xpath_dict.items():
			#logger.debug('checking xpath: %s' % v)
			matched_elements = self.xml.xpath(v, namespaces=self.nsmap)
			values = self.kvp_dict[k]
			if type(values) == str:
				values_len = 1
			elif type(values) == list:
				values_len = len(values)    
			if len(matched_elements) != values_len:
				logger.debug('mistmatch on %s --> %s, matched elements:values --> %s:%s' % (k,v,len(matched_elements),values_len))





		







