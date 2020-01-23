# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# generic imports
import json
import logging
import re
from types import ModuleType
# import ElasticSearch BaseMapper and PythonUDFRecord
from core.spark.utils import PythonUDFRecord

# Get an instance of a LOGGER
LOGGER = logging.getLogger(__name__)

# Set logging levels for 3rd party modules
logging.getLogger("requests").setLevel(logging.WARNING)


class RITSClient():

    '''
    class to handle the record_id transformation scenarios
    '''

    def __init__(self, query_dict):

        LOGGER.debug('initializing RITS')

        self.query_dict = query_dict

        # parse data
        self.target = self.query_dict.get('record_id_transform_target', None)
        LOGGER.debug('target is %s', self.target)

        # parse regex
        if self.query_dict.get('record_id_transform_type', None) == 'regex':

            # set type
            self.transform_type = 'regex'

            LOGGER.debug('parsing as %s type transformation', self.transform_type)

            # get args
            self.regex_match = self.query_dict.get('regex_match_payload', None)
            self.regex_replace = self.query_dict.get('regex_replace_payload', None)

        # parse python
        if self.query_dict.get('record_id_transform_type', None) == 'python':

            # set type
            self.transform_type = 'python'

            LOGGER.debug('parsing as %s type transformation', self.transform_type)

            # get args
            self.python_payload = self.query_dict.get('python_payload', None)

        # parse xpath
        if self.query_dict.get('record_id_transform_type', None) == 'xpath':

            # set type
            self.transform_type = 'xpath'

            LOGGER.debug('parsing as %s type transformation', self.transform_type)

            # get args
            self.xpath_payload = self.query_dict.get('xpath_payload', None)

        # capture test data if
        self.test_input = self.query_dict.get('test_transform_input', None)


    def test_user_input(self):

        '''
        method to test record_id transformation based on user input
        '''

        # handle regex
        if self.transform_type == 'regex':
            trans_result = re.sub(self.regex_match, self.regex_replace, self.test_input)


        # handle python
        if self.transform_type == 'python':

            if self.target == 'record_id':
                sr = PythonUDFRecord(None, non_row_input=True, record_id=self.test_input)
            if self.target == 'document':
                sr = PythonUDFRecord(None, non_row_input=True, document=self.test_input)

            # parse user supplied python code
            temp_mod = ModuleType('temp_mod')
            exec(self.python_payload, temp_mod.__dict__)

            try:
                trans_result = temp_mod.transform_identifier(sr)
            except Exception as err:
                trans_result = str(err)


        # handle xpath
        if self.transform_type == 'xpath':

            if self.target == 'record_id':
                trans_result = 'XPath only works for Record Document'

            if self.target == 'document':
                sr = PythonUDFRecord(None, non_row_input=True, document=self.test_input)

                # attempt xpath
                xpath_results = sr.xml.xpath(self.xpath_payload, namespaces=sr.nsmap)
                n = xpath_results[0]
                trans_result = n.text


        # return dict
        r_dict = {
            'results':trans_result,
            'success':True
        }
        return r_dict


    def params_as_json(self):

        '''
        Method to generate the required parameters to include in Spark job
        '''

        return json.dumps(self.__dict__)
