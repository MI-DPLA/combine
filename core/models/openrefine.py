# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# generic imports
import json
import logging

# Get an instance of a LOGGER
LOGGER = logging.getLogger(__name__)

# Set logging levels for 3rd party modules
logging.getLogger("requests").setLevel(logging.WARNING)



class OpenRefineActionsClient():

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
        if isinstance(or_actions, str):
            LOGGER.debug('parsing or_actions as JSON string')
            self.or_actions_json = or_actions
            self.or_actions = json.loads(or_actions)
        elif isinstance(or_actions, dict):
            LOGGER.debug('parsing or_actions as dictionary')
            self.or_actions_json = json.dumps(or_actions)
            self.or_actions = or_actions
        else:
            LOGGER.debug('not parsing or_actions, storing as-is')
            self.or_actions = or_actions
