# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# generic imports
from collections import OrderedDict
import gzip
import hashlib
import json
from json import JSONDecodeError
import logging
import os
import time
import uuid

# django imports
from django.conf import settings

# import background tasks
from core import tasks
from core.es import es_handle
from core.models.dpla_bulk_data_download import DPLABulkDataDownload
from core.models.tasks import CombineBackgroundTask


# AWS
import boto3

import elasticsearch as es

# Get an instance of a LOGGER
LOGGER = logging.getLogger(__name__)

# Set logging levels for 3rd party modules
logging.getLogger("requests").setLevel(logging.WARNING)

class DPLABulkDataClient():

    '''
    Client to facilitate browsing, downloading, and indexing of bulk DPLA data

    Args:
        filepath (str): optional filepath for downloaded bulk data on disk
    '''

    def __init__(self):

        self.service_hub_prefix = settings.SERVICE_HUB_PREFIX
        self.combine_oai_identifier = settings.COMBINE_OAI_IDENTIFIER
        self.bulk_dir = '%s/bulk' % settings.BINARY_STORAGE.rstrip('/').split('file://')[-1]

        # ES
        self.es_handle = es_handle

        # S3
        self.s3 = boto3.resource(
            's3',
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)

        # DPLA bucket
        self.dpla_bucket = self.s3.Bucket(settings.DPLA_S3_BUCKET)

        # boto3 client
        self.boto_client = boto3.client(
            's3',
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)


    def download_bulk_data(self, object_key, filepath):

        '''
        Method to bulk download a service hub's data from DPLA's S3 bucket
        '''

        # create bulk directory if not already present
        if not os.path.exists(self.bulk_dir):
            os.mkdir(self.bulk_dir)

        # download
        self.s3 = boto3.resource(
            's3',
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY)
        download_results = self.dpla_bucket.download_file(object_key, filepath)

        # return
        return download_results


    @staticmethod
    def get_bulk_reader(filepath, compressed=True):

        '''
        Return instance of BulkDataJSONReader
        '''

        return BulkDataJSONReader(filepath, compressed=compressed)


    def get_sample_record(self, filepath):

        return self.get_bulk_reader(filepath).get_next_record()


    def index_to_es(self, object_key, filepath, limit=False):

        '''
        Use streaming bulk indexer:
        http://elasticsearch-py.readthedocs.io/en/master/helpers.html
        '''

        stime = time.time()

        ##    prepare index
        index_name = hashlib.md5(object_key.encode('utf-8')).hexdigest()
        LOGGER.debug('indexing to %s', index_name)

        # if exists, delete
        if es_handle.indices.exists(index_name):
            es_handle.indices.delete(index_name)
        # set mapping
        mapping = {
            'mappings':{
                'item':{
                    'date_detection':False
                }
            }
        }
        # create index
        self.es_handle.indices.create(index_name, body=json.dumps(mapping))

        # get instance of bulk reader
        bulk_reader = self.get_bulk_reader(filepath)

        # index using streaming
        for _ in es.helpers.streaming_bulk(self.es_handle, bulk_reader.es_doc_generator(bulk_reader.get_record_generator(limit=limit, attr='record'), index_name=index_name), chunk_size=500):
            continue

        LOGGER.debug("index to ES elapsed: %s", (time.time() - stime))

        # return
        return index_name


    def retrieve_keys(self):

        '''
        Method to retrieve and parse key structure from S3 bucket

        Note: boto3 only returns 1000 objects from a list_objects
            - as such, need to add delimiters and prefixes to walk keys
            - OR, use bucket.objects.all() --> iterator
        '''

        stime = time.time()

        # get and return list of all keys
        keys = []
        for obj in self.dpla_bucket.objects.all():
            key = {
                'key':obj.key,
                'year':obj.key.split('/')[0],
                'month':obj.key.split('/')[1],
                'size':self._sizeof_fmt(int(obj.size))
            }
            keys.append(key)

        # return
        LOGGER.debug('retrieved %s keys in %s', len(keys), time.time()-stime)
        return keys


    @staticmethod
    def _sizeof_fmt(num, suffix='B'):

        '''
        https://stackoverflow.com/a/1094933/1196358
        '''

        for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
            if abs(num) < 1024.0:
                return "%3.1f%s%s" % (num, unit, suffix)
            num /= 1024.0
        return "%.1f%s%s" % (num, 'Yi', suffix)


    def download_and_index_bulk_data(self, object_key):

        '''
        Method to init background tasks of downloading and indexing bulk data
        '''

        # get object
        obj = self.s3.Object(self.dpla_bucket.name, object_key)

        # init DPLABulkDataDownload (dbdd) instance
        dbdd = DPLABulkDataDownload()

        # set key
        dbdd.s3_key = object_key

        # set filepath
        dbdd.filepath = '%s/%s' % (self.bulk_dir, object_key.replace('/', '_'))

        # set bulk data timestamp (when it was uploaded to S3 from DPLA)
        dbdd.uploaded_timestamp = obj.last_modified

        # save
        dbdd.save()

        # initiate Combine BG Task
        combine_task = CombineBackgroundTask(
            name='Download and Index DPLA Bulk Data: %s' % dbdd.s3_key,
            task_type='download_and_index_bulk_data',
            task_params_json=json.dumps({
                'dbdd_id':dbdd.id
            })
        )
        combine_task.save()

        # run celery task
        bg_task = tasks.download_and_index_bulk_data.delay(dbdd.id)
        LOGGER.debug('firing bg task: %s', bg_task)
        combine_task.celery_task_id = bg_task.task_id
        combine_task.save()



class BulkDataJSONReader():

    '''
    Class to handle the reading of DPLA bulk data
    '''

    def __init__(self, input_file, compressed=True):

        self.input_file = input_file
        self.compressed = compressed

        # not compressed
        if not self.compressed:
            self.file_handle = open(self.input_file, 'rb')

        # compressed
        if self.compressed:
            self.file_handle = gzip.open(self.input_file, 'rb')

        # bump file handle
        next(self.file_handle)
        self.records_gen = self.file_handle


    def get_next_record(self):

        r_string = next(self.file_handle).decode('utf-8').lstrip(',')
        return DPLARecord(r_string)


    def get_record_generator(self, limit=False, attr=None):

        i = 0
        while True:
            i += 1
            try:
                # if attr provided, return attribute of record
                if attr:
                    yield getattr(self.get_next_record(), attr)
                # else, return whole record
                else:
                    yield self.get_next_record()
                if limit and i >= limit:
                    break
            except JSONDecodeError:
                break


    @staticmethod
    def es_doc_generator(rec_gen, index_name=str(uuid.uuid4())):

        '''
        Create generator for explicit purpose of indexing to ES
            - pops _id and _rev from _source
            - writes custom _index
        '''

        for rec in rec_gen:

            # pop values
            for field in ['_id', '_rev', 'originalRecord']:
                try:
                    rec['_source'].pop(field)
                except:
                    pass

            # write new index
            rec['_index'] = index_name

            # yield
            yield rec



class DPLARecord():

    '''
    Small class to model a parsed DPLA JSON record
    '''

    def __init__(self, record):

        '''
        Expecting dictionary or json of record
        '''

        if isinstance(record, (dict, OrderedDict)):
            self.record = record
        elif isinstance(record, str):
            self.record = json.loads(record)

        # capture convenience values
        self.pre_hash_record_id = self.record['_id']
        self.dpla_id = self.record['_source']['id']
        self.dpla_url = self.record['_source']['@id']
        self.dpla_es_index = self.record['_index']
        try:
            self.original_metadata = self.record['_source']['originalRecord']['metadata']
        except:
            self.original_metadata = False
        self.metadata_string = str(self.original_metadata)
