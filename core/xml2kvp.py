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

	# test xml
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
	<beat type="4/4">four on the floor</beat>
	<beat type="3/4">waltz</beat>
	<ordering>
		<duck>100</duck>
		<duck>101</duck>
		<goose>102</goose>
		<it>run!</it>
	</ordering>
	<ordering>
		<duck>200</duck>
		<duck>201</duck>
		<goose>202</goose>
		<it>run!</it>
	</ordering>
</root>
	'''


	# custom exception for delimiter collision
	class DelimiterCollision(Exception):
		pass


	# schema for validation
	schema = {
		"$id": "xml2kvp_config_schema",		
		"title": "XML2kvp configuration options schema",
		"type": "object",
		"properties": {
			"add_literals": {
				"description":"Key/value pairs for literals to mixin, e.g. 'foo':'bar' would create field 'foo' with value 'bar' [Default: {}]",				
				"type": "object"
			},
			"concat_values_on_all_fields": {
				"description": "Boolean or String to join all values from multivalued field on [Default: false]",
				"type": ["boolean","string"]
			},
			"concat_values_on_fields": {
				"description": "Key/value pairs for fields to concat on provided value, e.g. 'foo_bar':'-' would concatenate multiple values from 'foo_bar' with string '-' [Default: {}]",
				"type": "object"
			},
			"copy_to": {
				"description": "Key/value pairs to copy one field to another, e.g. 'foo':'bar' would create field 'bar' and copy all values when encountered for 'foo' to 'bar' [Default: {}}]",
				"type": "object"
			},
			"copy_to_regex": {
				"description": "Key/value pairs to copy one field to another based on regex match of field, e.g. '.*foo':'bar' would copy create field 'bar' and copy all values fields 'goober_foo' and 'tronic_foo' to 'bar' [Default: {}]",
				"type": "object"
			},
			"copy_value_to_regex": {
				"description": "Key/value pairs that match values based on regex and copy to new field if matching, e.g. 'http.*':'websites' would create new field 'websites' and copy 'http://exampl.com' and 'https://example.org' to new field 'websites' [Default: {}]",
				"type": "object"
			},
			"error_on_delims_collision": {
				"description": "Boolean to raise DelimiterCollision exception if delimiter strings from either 'node_delim' or 'ns_prefix_delim' collide with field name or field value (False by default for permissive mapping, but can be helpful if collisions are essential to detect) [Default: false]",
				"type": "boolean"
			},
			"exclude_attributes": {
				"description": "Array of attributes to skip when creating field names, e.g. ['baz'] when encountering XML '<foo><bar baz='42' goober='1000'>tronic</baz></foo>' would create field 'foo_bar_@goober=1000', skipping attribute 'baz' [Default: []]",
				"type": "array"
			},
			"exclude_elements": {
				"description": "Array of elements to skip when creating field names, e.g. ['baz'] when encountering field '<foo><baz><bar>tronic</bar></baz></foo>' would create field 'foo_bar', skipping element 'baz' [Default: [], After: include_all_attributes, include_attributes]",
				"type": "array"
			},
			"include_attributes": {
				"description": "Array of attributes to include when creating field names, e.g. ['baz'] when encountering XML '<foo><bar baz='42' goober='1000'>tronic</baz></foo>' would create field 'foo_bar_@baz=42' [Default: [], Before: exclude_attributes, After: include_all_attributes]",
				"type": "array"
			},
			"include_all_attributes": {
				"description": "Boolean to consider and include attributes when creating field names, e.g. if False, XML elements '<foo><bar baz='42' goober='1000'>tronic</baz></foo>' would result in field name 'foo_bar' without attributes included [Default: true, Before: include_attributes, exclude_attributes]",
				"type": "boolean"
			},
			"include_meta": {
				"description": "Boolean to include 'xml2kvp_meta' field with output that contains these configurations [Default: false]",
				"type": "boolean"
			},
			"node_delim": {
				"description": "String to use as delimiter between XML elements and attributes when creating field name, e.g. '___' will convert XML '<foo><bar>tronic</bar></foo>' to field name 'foo___bar' [Default: '_']",
				"type": "string"
			},
			"ns_prefix_delim": {
				"description": "String to use as delimiter between XML namespace prefixes and elements, e.g. '|' XML '<ns:foo><ns:bar>tronic</ns:bar></ns:foo>' will create field name 'ns|foo_ns:bar' [Default: '|']",
				"type": "string"
			},
			"remove_copied_key": {
				"description": "Boolean to determine if originating field will be removed from output if that field is copied to another field [Default: true]",
				"type": "boolean"
			},
			"remove_copied_value": {
				"description": "Boolean to determine if value will be removed from originating field if that value is copied to another field [Default: false]",
				"type": "boolean"
			},
			"remove_ns_prefix": {
				"description": "Boolean to determine if XML namespace prefixes are removed from field names, e.g. if False, XML '<ns:foo><ns:bar>tronic</ns:bar></ns:foo>' will result in field name 'foo_bar' without 'ns' prefix [Default: false]",
				"type": "boolean"
			},
			"self_describing": {
				"description": "Boolean to include machine parsable information about delimeters used (reading right-to-left, delimeter and its length in characters) as suffix to field name, e.g. if True, and 'node_delim' is '___' and 'ns_prefix_delim' is '|', suffix will be '___3|1' [Default: false]",
				"type": "boolean"
			},
			"split_values_on_all_fields": {
				"description": "If present, string to use for splitting values from all fields, e.g. ' ' will convert single value 'a foo bar please' into the array of values ['a','foo','bar','please'] for that field [Default: false]",
				"type": ["boolean","string"]
			},
			"split_values_on_fields": {
				"description": "Key/value pairs of field names to split, and the string to split on, e.g. 'foo_bar':',' will split all values on field 'foo_bar' on comma ',' [Default: {}]",
				"type": "object"
			},
			"skip_attribute_ns_declarations": {
				"description": "Boolean to remove namespace declarations as considered attributes when creating field names [Default: true]",
				"type": "boolean"
			},
			"skip_repeating_values": {
				"description": "Boolean to determine if a field is multivalued, if those values are allowed to repeat [Default: true]",
				"type": "boolean"
			},
			"skip_root": {
				"description": "Boolean to determine if the XML root element will be included in output field names [Default: false]",
				"type": "boolean"
			}
		}
	}


	def __init__(self, **kwargs):

		'''
		Args
			kwargs (dict): Accepts named args from static methods
		'''

		# defaults, overwritten by methods
		self.add_literals={}
		self.as_tuples=True
		self.concat_values_on_all_fields=False
		self.concat_values_on_fields={}
		self.copy_to={}
		self.copy_to_regex={}
		self.copy_value_to_regex={}
		self.error_on_delims_collision=False
		self.exclude_attributes=[]
		self.exclude_elements=[]
		self.include_attributes=[]
		self.include_all_attributes=False
		self.include_meta=False
		self.include_xml_prop=False		
		self.node_delim='_'
		self.ns_prefix_delim='|'
		self.remove_copied_key=True
		self.remove_copied_value=False
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
	def schema_json(self):
		return json.dumps(self.schema)


	@property
	def config_json(self):

		config_dict = { k:v for k,v in self.__dict__.items() if k in [
			'add_literals',
			'concat_values_on_all_fields',
			'concat_values_on_fields',
			'copy_to',
			'copy_to_regex',
			'copy_value_to_regex',
			'error_on_delims_collision',
			'exclude_attributes',
			'exclude_elements',
			'include_attributes',
			'include_all_attributes',
			'node_delim',
			'ns_prefix_delim',
			'remove_copied_key',
			'remove_copied_value',
			'remove_ns_prefix',
			'self_describing',
			'split_values_on_all_fields',
			'split_values_on_fields',
			'skip_attribute_ns_declarations',
			'skip_repeating_values'
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
						if self.include_all_attributes or (len(self.include_attributes) > 0 and k.lstrip('@') in self.include_attributes):
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

			# if excluded attributes
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

			# key list prior to copies
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

		# handle copy_value_to_regex mixins
		if len(self.copy_value_to_regex) > 0:

			# key list prior to copies
			slen = len(k_list)

			# loop through copy_value_to_regex
			for rk, rv in self.copy_value_to_regex.items():

				# attempt sub
				try:
					if re.match(r'%s' % rk, value):
						k_list.append(rv)
				except:
					pass

			if self.remove_copied_value:
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
	def xml_to_kvp(xml_input, handler=None, return_handler=False, **kwargs):

		# init handler, overwriting defaults if not None
		if not handler:
			handler = XML2kvp(**kwargs)

		# clean kvp_dict
		handler.kvp_dict = {}

		# parse xml input
		handler.xml_string = handler._parse_xml_input(xml_input)

		# parse as dictionary
		handler.xml_dict = xmltodict.parse(handler.xml_string, xml_attribs=True)

		# walk xmltodict parsed dictionary and reutnr
		handler._xml_dict_parser(None, handler.xml_dict, hops=[])

		# handle literal mixins
		if len(handler.add_literals) > 0:
			for k,v in handler.add_literals.items():
				handler.kvp_dict[k] = v

		# handle split and concatenations
		handler._split_and_concat_fields()

		# convert list to tuples if flagged
		if handler.as_tuples:
			# convert all lists to tuples
			for k,v in handler.kvp_dict.items():
				if type(v) == list:
					handler.kvp_dict[k] = tuple(v)

		# include metadata about delimeters
		if handler.include_meta:
			handler.kvp_dict['xml2kvp_meta'] = json.dumps({
					'node_delim':handler.node_delim,
					'ns_prefix_delim':handler.ns_prefix_delim
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
	def k_to_xpath(k, handler=None, return_handler=False, **kwargs):

		'''
		Method to derive xpath from kvp key
		'''

		# init handler
		if not handler:
			handler = XML2kvp(**kwargs)

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
		if not hasattr(self, 'xml'):
			try:
				self.xml = etree.fromstring(self.xml_string)
			except:
				self.xml = etree.fromstring(self.xml_string.encode('utf-8'))
		if not hasattr(self, 'nsmap'):
			self._parse_nsmap()

		# generate xpaths values
		self = XML2kvp.kvp_to_xpath(self.kvp_dict, handler=self, return_handler=True)

		for k,v in self.k_xpath_dict.items():
			matched_elements = self.xml.xpath(v, namespaces=self.nsmap)
			values = self.kvp_dict[k]
			if type(values) == str:
				values_len = 1
			elif type(values) == list:
				values_len = len(values)    
			if len(matched_elements) != values_len:
				logger.debug('mistmatch on %s --> %s, matched elements:values --> %s:%s' % (k,v,len(matched_elements),values_len))


	@staticmethod
	def test_xml_to_kvp_speed(iterations, kwargs):

		stime=time.time()
		for x in range(0,iterations):
			XML2kvp.xml_to_kvp(XML2kvp.test_xml, **kwargs)
		print("avg for %s iterations: %s" % (iterations, (time.time()-stime) / float(iterations)))

		







