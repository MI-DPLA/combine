# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# generic imports
import json
import logging

# import mongo dependencies
from core.mongo import *

# Get an instance of a logger
logger = logging.getLogger(__name__)

# Set logging levels for 3rd party modules
logging.getLogger("requests").setLevel(logging.WARNING)



class OpenRefineActionsClient(object):

	'''
	This class / client is to handle the transformation of Record documents (XML)
	using the history of actions JSON output from OpenRefine.
	'''

	def __init__(self, or_actions=None):

		'''
		Args:
			or_actions_json (str|dict): raw json or dictionary
		'''

		# handle or_actions
		if type(or_actions) == str:
			logger.debug('parsing or_actions as JSON string')
			self.or_actions_json = or_actions
			self.or_actions = json.loads(or_actions)
		elif type(or_actions) == dict:
			logger.debug('parsing or_actions as dictionary')
			self.or_actions_json = json.dumps(or_actions)
			self.or_actions = or_actions
		else:
			logger.debug('not parsing or_actions, storing as-is')
			self.or_actions = or_actions