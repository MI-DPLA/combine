import json
import logging
from lxml import etree
import requests
import textwrap
import pyjxslt
from types import ModuleType

from django.conf import settings
from django.db import models

from core.xml2kvp import XML2kvp
from core.spark.utils import PythonUDFRecord

LOGGER = logging.getLogger(__name__)

# Set logging levels for 3rd party modules
logging.getLogger("requests").setLevel(logging.WARNING)


class Transformation(models.Model):

    '''
    Model to handle "transformation scenarios".   Envisioned to facilitate more than just XSL transformations, but
    currently, only XSLT is handled downstream
    '''

    name = models.CharField(max_length=255, null=True)
    payload = models.TextField()
    transformation_type = models.CharField(
        max_length=255,
        choices=[
            ('xslt', 'XSLT Stylesheet'),
            ('python', 'Python Code Snippet'),
            ('openrefine', 'Open Refine Actions')
        ]
    )
    filepath = models.TextField(max_length=1024, null=True, default=None, blank=True) # HiddenInput
    use_as_include = models.BooleanField(default=False)


    def __str__(self):
        return 'Transformation: %s, transformation type: %s' % (self.name, self.transformation_type)

    def as_dict(self):
        return self.__dict__

    def transform_record(self, row):

        '''
        Method to test transformation against a single record.

        Note: The code for self._transform_xslt() and self._transform_python() are similar,
        to staticmethods found in core.spark.jobs.py.   However, because those are running on spark workers,
        in a spark context, it makes it difficult to define once, but use in multiple places.   As such, these
        transformations are recreated here.

        Args:
            row (core.models.Record): Record instance, called "row" here to mirror spark job iterating over DataFrame
        '''

        valid_types = [type for (type, label) in get_transformation_type_choices()]
        requested_type = self.transformation_type
        if requested_type not in valid_types and requested_type is not None:
            raise Exception(f'requested invalid type for transformation scenario: {requested_type}')

        LOGGER.debug('transforming single record: %s', row)

        # run appropriate validation based on transformation type
        if self.transformation_type == 'xslt':
            result = self._transform_xslt(row)
        if self.transformation_type == 'python':
            result = self._transform_python(row)
        if self.transformation_type == 'openrefine':
            result = self._transform_openrefine(row)

        # return result
        return result


    def _transform_xslt(self, row):

        try:

            # attempt to parse xslt prior to submitting to pyjxslt
            try:
                etree.fromstring(self.payload.encode('utf-8'))
            except Exception as err:
                return str(err)

            # transform with pyjxslt gateway
            gateway = pyjxslt.Gateway(6767)
            gateway.add_transform('xslt_transform', self.payload)
            result = gateway.transform('xslt_transform', row.document)
            gateway.drop_transform('xslt_transform')

            # return
            return result

        except Exception as err:
            return str(err)


    def _transform_python(self, row):

        try:

            LOGGER.debug('python transformation running')

            # prepare row as parsed document with PythonUDFRecord class
            parsed_record = PythonUDFRecord(row)

            # get python function from Transformation Scenario
            temp_pyts = ModuleType('temp_pyts')
            exec(self.payload, temp_pyts.__dict__)

            # run transformation
            trans_result = temp_pyts.python_record_transformation(parsed_record)

            # check that trans_result is a list
            if not isinstance(trans_result, list):
                raise Exception('Python transformation should return a list, but got type %s' % type(trans_result))

            # convert any possible byte responses to string
            if trans_result[2] == True:
                if isinstance(trans_result[0], bytes):
                    trans_result[0] = trans_result[0].decode('utf-8')
                return trans_result[0]
            if trans_result[2] == False:
                if isinstance(trans_result[1], bytes):
                    trans_result[1] = trans_result[1].decode('utf-8')
                return trans_result[1]

        except Exception as err:
            return str(err)


    def _transform_openrefine(self, row):

        try:

            # parse or_actions
            or_actions = json.loads(self.payload)

            # load record as parsed_record
            parsed_record = PythonUDFRecord(row)

            # loop through actions
            for event in or_actions:

                # handle core/mass-edit
                if event['op'] == 'core/mass-edit':

                    # get xpath
                    xpath = XML2kvp.k_to_xpath(event['columnName'])
                    LOGGER.debug("using xpath value: %s", xpath)

                    # find elements for potential edits
                    elements = parsed_record.xml.xpath(xpath, namespaces=parsed_record.nsmap)

                    # loop through elements
                    for elem in elements:

                        # loop through edits
                        for edit in event['edits']:

                            # check if element text in from, change
                            if elem.text in edit['from']:
                                elem.text = edit['to']

                # handle jython
                if event['op'] == 'core/text-transform' and event['expression'].startswith('jython:'):

                    # fire up temp module
                    temp_pyts = ModuleType('temp_pyts')

                    # parse code
                    code = event['expression'].split('jython:')[1]

                    # wrap in function and write to temp module
                    code = 'def temp_func(value):\n%s' % textwrap.indent(code, prefix='       ')
                    exec(code, temp_pyts.__dict__)

                    # get xpath
                    xpath = XML2kvp.k_to_xpath(event['columnName'])
                    LOGGER.debug("using xpath value: %s", xpath)

                    # find elements for potential edits
                    elements = parsed_record.xml.xpath(xpath, namespaces=parsed_record.nsmap)

                    # loop through elements
                    for elem in elements:
                        elem.text = temp_pyts.temp_func(elem.text)

            # re-serialize as trans_result
            return etree.tostring(parsed_record.xml).decode('utf-8')

        except Exception as err:
            # set trans_result tuple
            return str(err)


    def _rewrite_xsl_http_includes(self):

        '''
        Method to check XSL payloads for external HTTP includes,
        if found, download and rewrite

            - do not save self (instance), firing during pre-save signal
        '''

        if self.transformation_type == 'xslt':

            LOGGER.debug('XSLT transformation, checking for external HTTP includes')

            # rewrite flag
            rewrite = False

            # output dir
            transformations_dir = '%s/transformations' % settings.BINARY_STORAGE.rstrip('/').split('file://')[-1]

            # parse payload
            xsl = etree.fromstring(self.payload.encode('utf-8'))

            # handle empty, global namespace
            _nsmap = xsl.nsmap.copy()
            try:
                global_ns = _nsmap.pop(None)
                _nsmap['global_ns'] = global_ns
            except:
                pass

            # xpath query for xsl:include
            includes = xsl.xpath('//xsl:include', namespaces=_nsmap)

            # loop through includes and check for HTTP hrefs
            for i in includes:

                # get href
                href = i.attrib.get('href', False)

                # check for http
                if href:
                    if href.lower().startswith('http'):

                        LOGGER.debug('external HTTP href found for xsl:include: %s', href)

                        # set flag for rewrite
                        rewrite = True

                        # download and save to transformations directory on filesystem
                        req = requests.get(href)
                        filepath = '%s/%s' % (transformations_dir, href.split('/')[-1])
                        with open(filepath, 'wb') as out_file:
                            out_file.write(req.content)

                        # rewrite href and add note
                        i.attrib['href'] = filepath

            # rewrite if need be
            if rewrite:
                LOGGER.debug('rewriting XSL payload')
                self.payload = etree.tostring(xsl, encoding='utf-8', xml_declaration=True).decode('utf-8')


def get_transformation_type_choices():
    choices = [
        ('xslt', 'XSLT Stylesheet'),
        ('openrefine', 'Open Refine Actions')
    ]
    if getattr(settings, 'ENABLE_PYTHON', 'false') == 'true':
        choices.append(('python', 'Python Code Snippet'))
    return choices
