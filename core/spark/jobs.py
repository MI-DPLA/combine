# imports
import ast
import datetime
import django
from elasticsearch import Elasticsearch
import hashlib
from itertools import groupby
import json
from lxml import etree
from operator import itemgetter
import os
import pdb
import polling
import re
import requests
import shutil
import sys
import textwrap
import time
from types import ModuleType
import uuid

# pyjxslt
import pyjxslt

# import from core.spark
try:
    from es import ESIndex
    from utils import PythonUDFRecord, refresh_django_db_connection, df_union_all
    from record_validation import ValidationScenarioSpark
    from console import get_job_as_df, get_job_es
    from xml2kvp import XML2kvp
except:
    from core.spark.es import ESIndex
    from core.spark.utils import PythonUDFRecord, refresh_django_db_connection, df_union_all
    from core.spark.record_validation import ValidationScenarioSpark
    from core.spark.console import get_job_as_df, get_job_es
    from core.xml2kvp import XML2kvp

# import Row from pyspark
from pyspark import StorageLevel
from pyspark.sql import Row
from pyspark.sql.types import StringType, StructField, StructType, BooleanType, ArrayType, IntegerType, LongType
import pyspark.sql.functions as pyspark_sql_functions
from pyspark.sql.functions import udf, regexp_replace, lit, crc32
from pyspark.sql.window import Window

# check for registered apps signifying readiness, if not, run django.setup() to run as standalone
if not hasattr(django, 'apps'):
    os.environ['DJANGO_SETTINGS_MODULE'] = 'combine.settings'
    sys.path.append('/opt/combine')
    django.setup()

# import django settings
from django.conf import settings
from django.db import connection, transaction

# import select models from Core
from core.models import CombineJob, Job, JobInput, JobTrack, Transformation, PublishedRecords, \
    RecordIdentifierTransformationScenario, RecordValidation, DPLABulkDataDownload

# import mongo dependencies
from core.mongo import *


####################################################################
# Custom Exceptions 											   #
####################################################################

class AmbiguousIdentifier(Exception):
    pass


####################################################################
# Dataframe Schemas 											   #
####################################################################

class CombineRecordSchema(object):

    """
    Class to organize Combine record spark dataframe schemas
    """

    def __init__(self):
        # schema for Combine records
        self.schema = StructType([
            StructField('combine_id', StringType(), True),
            StructField('record_id', StringType(), True),
            StructField('document', StringType(), True),
            StructField('error', StringType(), True),
            StructField('unique', BooleanType(), True),
            StructField('job_id', IntegerType(), False),
            StructField('oai_set', StringType(), True),
            StructField('success', BooleanType(), False),
            StructField('fingerprint', IntegerType(), False),
            StructField('transformed', BooleanType(), False),
            StructField('valid', BooleanType(), False),
            StructField('dbdm', BooleanType(), False)
        ]
        )

        # fields
        self.field_names = [
            f.name for f in self.schema.fields if f.name != 'id']

####################################################################
# Spark Jobs 		 											   #
####################################################################

class CombineSparkJob(object):

    """
    Base class for Combine Spark Jobs.
    Provides some usuable components for jobs.
    """

    def __init__(self, spark, **kwargs):

        self.spark = spark

        self.kwargs = kwargs

        # init logging support
        spark.sparkContext.setLogLevel('INFO')
        log4jLogger = spark.sparkContext._jvm.org.apache.log4j
        self.logger = log4jLogger.LogManager.getLogger(__name__)

    def init_job(self):

        # refresh Django DB Connection
        refresh_django_db_connection()

        # get job
        results = polling.poll(lambda: Job.objects.filter(id=int(self.kwargs['job_id'])).count() == 1, step=1,
                               timeout=60)
        self.job = Job.objects.get(pk=int(self.kwargs['job_id']))

        # start job_track instance, marking job start
        self.job_track = JobTrack(
            job_id=self.job.id
        )
        self.job_track.save()

        # retrieve job_details
        self.job_details = self.job.job_details_dict

    def close_job(self):
        """
        Note to Job tracker that finished, and perform other long-running, one-time calculations
        to speed up front-end
        """

        refresh_django_db_connection()

        # if re-run, check if job was previously published and republish
        if 'published' in self.job.job_details_dict.keys():
            if self.job.job_details_dict['published']['status']:
                self.logger.info('job params flagged for publishing')
                self.job.publish(publish_set_id=self.job.publish_set_id)
            elif not self.job.job_details_dict['published']['status']:
                self.logger.info('job params flagged for unpublishing')
                self.job.unpublish()

        # finally, update finish_timestamp of job_track instance
        self.job_track.finish_timestamp = datetime.datetime.now()
        self.job_track.save()

        # count new job validations
        for jv in self.job.jobvalidation_set.filter(failure_count=None):
            jv.validation_failure_count(force_recount=True)

        # unpersist cached dataframes
        self.spark.catalog.clearCache()

    def update_jobGroup(self, description):
        """
        Method to update spark jobGroup
        """

        self.logger.info("### %s" % description)
        self.spark.sparkContext.setJobGroup("%s" % self.job.id, "%s, Job #%s" % (description, self.job.id))

    def get_input_records(self, filter_input_records=True):

        # get input job ids
        input_job_ids = [int(job_id)
                         for job_id in self.job_details['input_job_ids']]

        # if job_specific input filters set, handle
        if 'job_specific' in self.job_details['input_filters'].keys() and len(
                self.job_details['input_filters']['job_specific']) > 0:

            # debug
            self.logger.info("Job specific input filters found, handling")

            # convenience dict
            job_spec_dicts = self.job_details['input_filters']['job_specific']

            # init list of dataframes to have union performed
            job_spec_dfs = []

            # remove job_specific from input_jobs
            for spec_input_job_id in self.job_details['input_filters']['job_specific'].keys():
                input_job_ids.remove(int(spec_input_job_id))

            # handle remaining, if any, non-specified jobs as per normal
            if len(input_job_ids) > 0:
                # retrieve from Mongo
                pipeline = json.dumps([
                    {
                        '$match': {
                            'job_id': {
                                '$in': input_job_ids
                            }
                        }
                    },
                    {
                        '$project': {field_name: 1 for field_name in CombineRecordSchema().field_names}
                    }
                ])
                records = self.spark.read.format("com.mongodb.spark.sql.DefaultSource")\
                    .option("uri", "mongodb://%s" % settings.MONGO_HOST)\
                    .option("database", "combine")\
                    .option("collection", "record")\
                    .option("partitioner", "MongoSamplePartitioner")\
                    .option("spark.mongodb.input.partitionerOptions.partitionSizeMB",
                            settings.MONGO_READ_PARTITION_SIZE_MB) \
                    .option("pipeline", pipeline).load()

                # optionally filter
                if filter_input_records:
                    records = self.record_input_filters(records)

                # add CombineRecordSchema columns if absent
                records = self.add_missing_columns(records)

                # append to list of dataframes
                job_spec_dfs.append(records)

            # group like/identical input filter parameters, run together
            # https://stackoverflow.com/questions/52484043/group-key-value-pairs-in-python-dictionary-by-value-maintaining-original-key-as
            grouped_spec_dicts = [
                {'input_filters': k, 'job_ids': [int(job_id) for job_id in list(map(itemgetter(0), g))]} for k, g in
                groupby(sorted(job_spec_dicts.items(), key=lambda t: t[1].items()), itemgetter(1))]

            # next, loop through spec'ed jobs, retrieve and filter
            for job_spec_group in grouped_spec_dicts:

                # debug
                self.logger.info(
                    "Handling specific input filters for job ids: %s" % job_spec_group['job_ids'])

                # handle remaining, non-specified jobs as per normal
                # retrieve from Mongo
                pipeline = json.dumps([
                    {
                        '$match': {
                            'job_id': {
                                '$in': job_spec_group['job_ids']
                            }
                        }
                    },
                    {
                        '$project': {field_name: 1 for field_name in CombineRecordSchema().field_names}
                    }
                ])
                job_spec_records = self.spark.read.format("com.mongodb.spark.sql.DefaultSource")\
                    .option("uri", "mongodb://%s" % settings.MONGO_HOST)\
                    .option("database", "combine")\
                    .option("collection", "record")\
                    .option("partitioner", "MongoSamplePartitioner")\
                    .option("spark.mongodb.input.partitionerOptions.partitionSizeMB",
                            settings.MONGO_READ_PARTITION_SIZE_MB) \
                    .option("pipeline", pipeline).load()

                # optionally filter
                if filter_input_records:
                    job_spec_records = self.record_input_filters(job_spec_records,
                                                                 input_filters=job_spec_group['input_filters'])

                # add CombineRecordSchema columns if absent
                job_spec_records = self.add_missing_columns(job_spec_records)

                # append dataframe
                job_spec_dfs.append(job_spec_records)

            # union spec'ed jobs with unspec'ed records
            self.logger.info("union-izing all job dataframes")
            unioned_records = df_union_all(job_spec_dfs)

            # count breakdown of input jobs/records, save to Job
            self.count_input_records(unioned_records)

            # finally, return records
            return unioned_records

        # else, handle filtering and retrieval same for each input job
        else:
            # retrieve from Mongo
            pipeline = json.dumps([
                {
                    '$match': {
                        'job_id': {
                            '$in': input_job_ids
                        }
                    }
                },
                {
                    '$project': {field_name: 1 for field_name in CombineRecordSchema().field_names}
                }
            ])
            records = self.spark.read.format("com.mongodb.spark.sql.DefaultSource")\
                .option("uri", "mongodb://%s" % settings.MONGO_HOST)\
                .option("database", "combine")\
                .option("collection", "record")\
                .option("partitioner", "MongoSamplePartitioner")\
                .option("spark.mongodb.input.partitionerOptions.partitionSizeMB", settings.MONGO_READ_PARTITION_SIZE_MB)\
                .option("pipeline", pipeline).load()

            # optionally filter
            if filter_input_records:
                records = self.record_input_filters(records)

            # add CombineRecordSchema columns if absent
            records = self.add_missing_columns(records)

        # count breakdown of input jobs/records, save to Job
        self.count_input_records(records)

        # return
        return records

    def add_missing_columns(self, records):
        """
        Method to ensure records dataframe has all required columns from CombineRecordSchema

        Args:
                records (DataFrame): dataframe of records
        """

        # loop through required columns from CombineRecordSchema
        self.logger.info("check for missing columns from CombineRecordSchema")
        for field_name in CombineRecordSchema().field_names:
            if field_name not in records.columns:
                self.logger.info("adding column: %s" % field_name)
                records = records.withColumn(
                    field_name, pyspark_sql_functions.lit(''))
        return records

    def save_records(self,
                     records_df=None,
                     write_avro=settings.WRITE_AVRO,
                     index_records=settings.INDEX_TO_ES,
                     assign_combine_id=False):
        """
        Method to index records to DB and trigger indexing to ElasticSearch (ES)

        Args:
                records_df (pyspark.sql.DataFrame): records as pyspark DataFrame
                write_avro (bool): boolean to write avro files to disk after DB indexing
                index_records (bool): boolean to index to ES
                assign_combine_id (bool): if True, establish `combine_id` column and populate with UUID

        Returns:
                None
                        - determines if record_id unique among records DataFrame
                        - selects only columns that match CombineRecordSchema
                        - writes to DB, writes to avro files
        """

        # assign combine ID
        if assign_combine_id:
            combine_id_udf = udf(lambda record_id: str(
                uuid.uuid4()), StringType())
            records_df = records_df.withColumn(
                'combine_id', combine_id_udf(records_df.record_id))

        # run record identifier transformation scenario if provided
        records_df = self.run_rits(records_df)

        # check uniqueness (overwrites if column already exists)
        records_df = records_df.withColumn("unique", (
            pyspark_sql_functions.count('record_id')
            .over(Window.partitionBy('record_id')) == True)
            .cast('boolean'))

        # add valid column
        records_df = records_df.withColumn(
            'valid', pyspark_sql_functions.lit(True))

        # add DPLA Bulk Data Match (dbdm) column
        records_df = records_df.withColumn(
            'dbdm', pyspark_sql_functions.lit(False))

        # ensure columns to avro and DB
        records_df_combine_cols = records_df.select(
            CombineRecordSchema().field_names)

        # write avro, coalescing for output
        if write_avro:
            records_df_combine_cols.coalesce(settings.SPARK_REPARTITION)\
                .write.format("com.databricks.spark.avro").save(self.job.job_output)

        # write records to MongoDB
        self.update_jobGroup('Saving Records to DB')
        records_df_combine_cols.write.format("com.mongodb.spark.sql.DefaultSource")\
            .mode("append")\
            .option("uri", "mongodb://%s" % settings.MONGO_HOST)\
            .option("database", "combine")\
            .option("collection", "record").save()

        # check if anything written to DB to continue, else abort
        if self.job.get_records().count() > 0:

            # read rows from Mongo with minted ID for future stages
            pipeline = json.dumps(
                {'$match': {'job_id': self.job.id, 'success': True}})
            db_records = self.spark.read.format("com.mongodb.spark.sql.DefaultSource")\
                .option("uri", "mongodb://%s" % settings.MONGO_HOST)\
                .option("database", "combine")\
                .option("collection", "record")\
                .option("partitioner", "MongoSamplePartitioner")\
                .option("spark.mongodb.input.partitionerOptions.partitionSizeMB", settings.MONGO_READ_PARTITION_SIZE_MB)\
                .option("pipeline", pipeline).load()

            # index to ElasticSearch
            self.update_jobGroup('Indexing to ElasticSearch')
            if index_records and settings.INDEX_TO_ES:
                es_rdd = ESIndex.index_job_to_es_spark(
                    self.spark,
                    job=self.job,
                    records_df=db_records,
                    field_mapper_config=self.job_details['field_mapper_config']
                )

            # run Validation Scenarios
            if 'validation_scenarios' in self.job_details.keys():
                self.update_jobGroup('Running Validation Scenarios')
                vs = ValidationScenarioSpark(
                    spark=self.spark,
                    job=self.job,
                    records_df=db_records,
                    validation_scenarios=self.job_details['validation_scenarios']
                )
                vs.run_record_validation_scenarios()

            # handle DPLA Bulk Data matching, rewriting/updating records where match is found
            self.dpla_bulk_data_compare(db_records, es_rdd)

            # return
            return db_records

        else:
            raise Exception("No successful records written to disk for Job: %s" % self.job.name)

    def record_input_filters(self, filtered_df, input_filters=None):
        """
        Method to apply filters to input Records

        Args:
                spark (pyspark.sql.session.SparkSession): provided by pyspark context
                records_df (pyspark.sql.DataFrame): DataFrame of records pre validity filtering

        Returns:
                (pyspark.sql.DataFrame): DataFrame of records post filtering
        """

        # use input filters if provided, else fall back to job
        if input_filters is None:
            input_filters = self.job_details['input_filters']

        # filter to input record appropriate field
        # filtered_df = filtered_df.select(CombineRecordSchema().field_names)

        # handle validity filters
        input_validity_valve = input_filters['input_validity_valve']

        # filter to valid or invalid records
        # return valid records
        if input_validity_valve == 'valid':
            filtered_df = filtered_df.filter(filtered_df.valid == 1)

        # return invalid records
        elif input_validity_valve == 'invalid':
            filtered_df = filtered_df.filter(filtered_df.valid == 0)

        # handle numerical filters
        input_numerical_valve = input_filters['input_numerical_valve']
        if input_numerical_valve is not None:
            filtered_df = filtered_df.limit(input_numerical_valve)

        # handle es query valve
        if 'input_es_query_valve' in input_filters.keys():
            input_es_query_valve = input_filters['input_es_query_valve']
            if input_es_query_valve not in [None, '{}']:
                filtered_df = self.es_query_valve_filter(
                    input_es_query_valve, filtered_df)

        # filter duplicates
        if 'filter_dupe_record_ids' in input_filters.keys() and input_filters['filter_dupe_record_ids'] == True:
            filtered_df = filtered_df.dropDuplicates(['record_id'])

        # after input filtering which might leverage db_id, drop
        filtered_df = filtered_df.select(
            [c for c in filtered_df.columns if c != '_id'])

        # return
        return filtered_df

    def count_input_records(self, records):
        """
        Method to count records by job_id
                - count records from input jobs if > 1
                - otherwise assume Job.udpate_status() will calculate from single input job

        Args:
                records (dataframe): Records to count based on job_id
        """

        refresh_django_db_connection()
        if 'input_job_ids' in self.job_details.keys() and len(self.job_details['input_job_ids']) > 1:

            # cache
            records.cache()

            # copy input job ids to mark done (cast to int)
            input_jobs = [int(job_id)
                          for job_id in self.job_details['input_job_ids'].copy()]

            # group by job_ids
            record_counts = records.groupBy('job_id').count()

            # loop through input jobs, init, and write
            for input_job_count in record_counts.collect():
                # remove from input_jobs
                input_jobs.remove(input_job_count['job_id'])

                # set passed records and save
                input_job = JobInput.objects.filter(job_id=self.job.id,
                                                    input_job_id=int(input_job_count['job_id'])).first()
                input_job.passed_records = input_job_count['count']
                input_job.save()

            # loop through any remaining jobs, where absence indicates 0 records passed
            for input_job_id in input_jobs:
                input_job = JobInput.objects.filter(job_id=self.job.id, input_job_id=int(input_job_id)).first()
                input_job.passed_records = 0
                input_job.save()

    def es_query_valve_filter(self, input_es_query_valve, filtered_df):
        """
        Method to handle input valve based on ElasticSearch query

                - perform union if multiple input Jobs are used

        """

        # prepare input jobs list
        if 'input_job_ids' in self.job_details.keys():
            input_jobs_ids = [int(job_id)
                              for job_id in self.job_details['input_job_ids']]
        elif 'input_job_id' in self.job_details:
            input_jobs_ids = [int(self.job_details['input_job_id'])]

        # loop through and create es.resource string
        es_indexes = ','.join(['j%s' % job_id for job_id in input_jobs_ids])

        # get es index as RDD
        es_rdd = self.spark.sparkContext.newAPIHadoopRDD(
            inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
            keyClass="org.apache.hadoop.io.NullWritable",
            valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
            conf={
                "es.resource": "%s/record" % es_indexes,
                "es.nodes": "%s:9200" % settings.ES_HOST,
                "es.query": input_es_query_valve,
                "es.read.field.exclude": "*"})
        es_df = es_rdd.map(lambda row: (row[0], )).toDF()

        # perform join on ES documents
        filtered_df = filtered_df.join(
            es_df, filtered_df['_id']['oid'] == es_df['_1'], 'leftsemi')

        # return
        return filtered_df

    def run_rits(self, records_df):
        """
        Method to run Record Identifier Transformation Scenarios (rits) if present.

        RITS can be of three types:
                1) 'regex' - Java Regular Expressions
                2) 'python' - Python Code Snippets
                3) 'xpath' - XPath expression

        Each are handled differently, but all strive to return a dataframe (records_df),
        with the `record_id` column modified.

        Args:
                records_df (pyspark.sql.DataFrame): records as pyspark DataFrame
                rits_id (string|int): DB identifier of pre-configured RITS

        Returns:

        """

        # get rits ID from kwargs
        rits_id = self.job_details.get('rits', False)

        # if rits id provided
        if rits_id and rits_id is not None:

            # get RITS
            rits = RecordIdentifierTransformationScenario.objects.get(
                pk=int(rits_id))

            # handle regex
            if rits.transformation_type == 'regex':

                # define udf function for python transformation
                def regex_record_id_trans_udf(row, match, replace, trans_target):

                    try:

                        # use python's re module to perform regex
                        if trans_target == 'record_id':
                            trans_result = re.sub(
                                match, replace, row.record_id)
                        if trans_target == 'document':
                            trans_result = re.sub(match, replace, row.document)

                        # run transformation
                        success = True
                        error = row.error

                    except Exception as e:
                        trans_result = str(e)
                        error = 'record_id transformation failure'
                        success = False

                    # return Row
                    return Row(
                        combine_id=row.combine_id,
                        record_id=trans_result,
                        document=row.document,
                        error=error,
                        job_id=row.job_id,
                        oai_set=row.oai_set,
                        success=success,
                        fingerprint=row.fingerprint,
                        transformed=row.transformed
                    )

                # transform via rdd.map and return
                match = rits.regex_match_payload
                replace = rits.regex_replace_payload
                trans_target = rits.transformation_target
                records_rdd = records_df.rdd.map(
                    lambda row: regex_record_id_trans_udf(row, match, replace, trans_target))
                records_df = records_rdd.toDF()

            # handle python
            if rits.transformation_type == 'python':

                # define udf function for python transformation
                def python_record_id_trans_udf(row, python_code, trans_target):

                    try:
                        # get python function from Transformation Scenario
                        temp_mod = ModuleType('temp_mod')
                        exec(python_code, temp_mod.__dict__)

                        # establish python udf record
                        if trans_target == 'record_id':
                            pyudfr = PythonUDFRecord(
                                None, non_row_input=True, record_id=row.record_id)
                        if trans_target == 'document':
                            pyudfr = PythonUDFRecord(
                                None, non_row_input=True, document=row.document)

                        # run transformation
                        trans_result = temp_mod.transform_identifier(pyudfr)
                        success = True
                        error = row.error

                    except Exception as e:
                        trans_result = str(e)
                        error = 'record_id transformation failure'
                        success = False

                    # return Row
                    return Row(
                        combine_id=row.combine_id,
                        record_id=trans_result,
                        document=row.document,
                        error=error,
                        job_id=row.job_id,
                        oai_set=row.oai_set,
                        success=success,
                        fingerprint=row.fingerprint,
                        transformed=row.transformed
                    )

                # transform via rdd.map and return
                python_code = rits.python_payload
                trans_target = rits.transformation_target
                records_rdd = records_df.rdd.map(
                    lambda row: python_record_id_trans_udf(row, python_code, trans_target))
                records_df = records_rdd.toDF()

            # handle xpath
            if rits.transformation_type == 'xpath':

                '''
                Currently XPath RITS are handled via python and etree,
                but might be worth investigating if this could be performed
                via pyjxslt to support XPath 2.0
                '''

                # define udf function for xpath expression
                def xpath_record_id_trans_udf(row, xpath):

                    # establish python udf record, forcing 'document' type trans for XPath
                    pyudfr = PythonUDFRecord(
                        None, non_row_input=True, document=row.document)

                    # run xpath and retrieve value
                    xpath_query = pyudfr.xml.xpath(
                        xpath, namespaces=pyudfr.nsmap)
                    if len(xpath_query) == 1:
                        trans_result = xpath_query[0].text
                        success = True
                        error = row.error
                    elif len(xpath_query) == 0:
                        trans_result = 'xpath expression found nothing'
                        success = False
                        error = 'record_id transformation failure'
                    else:
                        trans_result = 'more than one node found for XPath query'
                        success = False
                        error = 'record_id transformation failure'

                    # return Row
                    return Row(
                        combine_id=row.combine_id,
                        record_id=trans_result,
                        document=row.document,
                        error=error,
                        job_id=row.job_id,
                        oai_set=row.oai_set,
                        success=success,
                        fingerprint=row.fingerprint,
                        transformed=row.transformed
                    )

                # transform via rdd.map and return
                xpath = rits.xpath_payload
                records_rdd = records_df.rdd.map(
                    lambda row: xpath_record_id_trans_udf(row, xpath))
                records_df = records_rdd.toDF()

            # return
            return records_df

        # else return dataframe untouched
        else:
            return records_df

    def dpla_bulk_data_compare(self, records_df, es_rdd):
        """
        Method to compare against bulk data if provided

        Args:
                records_df (dataframe): records post-write to DB
                es_rdd (rdd): RDD of documents as written to ElasticSearch
                        Columns:
                                _1 : boolean, 'success'/'failure'
                                _2 : map, mapped fields
        """

        self.logger.info('Running DPLA Bulk Data Compare')
        self.update_jobGroup('Running DPLA Bulk Data Compare')

        # check for dbdm params, get dbdd ID from kwargs
        if 'dbdm' in self.job_details.keys():
            dbdd_id = self.job_details['dbdm'].get('dbdd', False)
        else:
            dbdd_id = False

        # if rits id provided
        if dbdd_id and dbdd_id is not None:

            self.logger.info('DBDD id provided, retrieving and running...')

            # get dbdd instance
            dbdd = DPLABulkDataDownload.objects.get(pk=int(dbdd_id))
            self.logger.info('DBDD retrieved: %s @ ES index %s' %
                             (dbdd.s3_key, dbdd.es_index))

            # get DPLA bulk data from ES as DF
            dpla_df = get_job_es(self.spark, indices=[
                                 dbdd.es_index], doc_type='item')

            # get job mapped fields from es_rdd
            es_df = es_rdd.toDF()

            # join on isShownAt
            matches_df = es_df.join(
                dpla_df, es_df['_2']['dpla_isShownAt'] == dpla_df['isShownAt'], 'leftsemi')

            # select records from records_df for updating (writing)
            update_dbdm_df = records_df.join(matches_df, records_df['_id']['oid'] == matches_df['_2']['db_id'],
                                             'leftsemi')

            # set dbdm column to True
            update_dbdm_df = update_dbdm_df.withColumn(
                'dbdm', pyspark_sql_functions.lit(True))

            # write to DB
            update_dbdm_df.write.format("com.mongodb.spark.sql.DefaultSource")\
                .mode("append")\
                .option("uri", "mongodb://%s" % settings.MONGO_HOST)\
                .option("database", "combine")\
                .option("collection", "record").save()

        # else, return with dbdm column all False
        else:
            return records_df.withColumn('dbdm', pyspark_sql_functions.lit(False))

    def fingerprint_records(self, df):
        """
        Method to generate a crc32 hash "fingerprint" for each Record
        """

        # fingerprint Record document
        df = df.withColumn('fingerprint', crc32(df.document))
        return df


class HarvestOAISpark(CombineSparkJob):

    """
    Spark code for harvesting OAI records
    """

    def spark_function(self):
        """
        Harvest records via OAI.

        As a harvest type job, unlike other jobs, this introduces various fields to the Record for the first time:
                - record_id
                - job_id
                - oai_set
                - publish_set_id
                - unique (TBD)

        Args:
                spark (pyspark.sql.session.SparkSession): provided by pyspark context
                job_id (int): Job ID

        Returns:
                None:
                - harvests OAI records and writes to disk as avro files
                - indexes records into DB
                - map / flatten records and indexes to ES
        """

        # init job
        self.init_job()
        self.update_jobGroup('Running Harvest OAI Job')

        # prepare to harvest OAI records via Ingestion3
        df = self.spark.read.format("dpla.ingestion3.harvesters.oai")\
            .option("endpoint", self.job_details['oai_params']['endpoint'])\
            .option("verb", self.job_details['oai_params']['verb'])\
            .option("metadataPrefix", self.job_details['oai_params']['metadataPrefix'])

        # remove scope entirely if harvesting all records, not sets
        if self.job_details['oai_params']['scope_type'] != 'harvestAllRecords':
            df = df.option(self.job_details['oai_params']['scope_type'], self.job_details['oai_params']['scope_value'])\
 \
        # harvest
        df = df.load()

        # select records with content
        records = df.select("record.*").where("record is not null")

        # repartition
        records = records.repartition(settings.SPARK_REPARTITION)

        # if removing OAI record <header>
        if not self.job_details['oai_params']['include_oai_record_header']:
            # attempt to find and select <metadata> element from OAI record, else filter out
            def find_metadata_udf(document):
                if type(document) == str:
                    xml_root = etree.fromstring(document)
                    m_root = xml_root.find(
                        '{http://www.openarchives.org/OAI/2.0/}metadata')
                    if m_root is not None:
                        # expecting only one child to <metadata> element
                        m_children = m_root.getchildren()
                        if len(m_children) == 1:
                            m_child = m_children[0]
                            m_string = etree.tostring(m_child).decode('utf-8')
                            return m_string
                    else:
                        return 'none'
                else:
                    return 'none'

            metadata_udf = udf(lambda col_val: find_metadata_udf(col_val), StringType())
            records = records.select(
                *[metadata_udf(col).alias('document') if col == 'document' else col for col in records.columns])

        # filter where not none
        records = records.filter(records.document != 'none')

        # establish 'success' column, setting all success for Harvest
        records = records.withColumn(
            'success', pyspark_sql_functions.lit(True))

        # copy 'id' from OAI harvest to 'record_id' column
        records = records.withColumn('record_id', records.id)

        # add job_id as column
        job_id = self.job.id
        job_id_udf = udf(lambda id: job_id, IntegerType())
        records = records.withColumn('job_id', job_id_udf(records.id))

        # add oai_set, accomodating multiple sets
        records = records.withColumn('oai_set', records.setIds)

        # add blank error column
        error = udf(lambda id: '', StringType())
        records = records.withColumn('error', error(records.id))

        # fingerprint records and set transformed
        records = self.fingerprint_records(records)
        records = records.withColumn(
            'transformed', pyspark_sql_functions.lit(True))

        # index records to DB and index to ElasticSearch
        self.save_records(
            records_df=records,
            assign_combine_id=True
        )

        # close job
        self.close_job()


class HarvestStaticXMLSpark(CombineSparkJob):
    """
    Spark code for harvesting static xml records
    """

    def spark_function(self):
        """
        Harvest static XML records provided by user.

        Expected input structure:
                /foo/bar <-- self.static_payload
                        baz1.xml <-- record at self.xpath_query within file
                        baz2.xml
                        baz3.xml

        As a harvest type job, unlike other jobs, this introduces various fields to the Record for the first time:
                - record_id
                - job_id
                - oai_set
                - publish_set_id
                - unique (TBD)

        Args:
                spark (pyspark.sql.session.SparkSession): provided by pyspark context
                kwargs:
                        job_id (int): Job ID
                        static_payload (str): path of static payload on disk
                        # TODO: add other kwargs here from static job
                        index_mapper (str): class name from core.spark.es, extending BaseMapper
                        validation_scenarios (list): list of Validadtion Scenario IDs

        Returns:
                None:
                - opens and parses static files from payload
                - indexes records into DB
                - map / flatten records and indexes to ES
        """

        # init job
        self.init_job()
        self.update_jobGroup('Running Harvest Static Job')

        # use Spark-XML's XmlInputFormat to stream globbed files, parsing with user provided `document_element_root`
        static_rdd = self.spark.sparkContext.newAPIHadoopFile(
            'file://%s/**' % self.job_details['payload_dir'].rstrip('/'),
            'com.databricks.spark.xml.XmlInputFormat',
            'org.apache.hadoop.io.LongWritable',
            'org.apache.hadoop.io.Text',
            conf={
                'xmlinput.start': '<%s>' % self.job_details['document_element_root'],
                'xmlinput.end': '</%s>' % self.job_details['document_element_root'],
                'xmlinput.encoding': 'utf-8'
            }
        )

        # parse namespaces

        def get_namespaces(xml_node):
            nsmap = {}
            for ns in xml_node.xpath('//namespace::*'):
                if ns[0]:
                    nsmap[ns[0]] = ns[1]
            return nsmap

        def parse_records_udf(job_id, row, job_details):

            # get doc string
            doc_string = row[1]

            # if optional (additional) namespace declaration provided, use
            if job_details['additional_namespace_decs']:
                doc_string = re.sub(
                    ns_regex,
                    r'<%s %s>' % (
                        job_details['document_element_root'], job_details['additional_namespace_decs']),
                    doc_string
                )

            try:

                # parse with lxml
                try:
                    xml_root = etree.fromstring(doc_string.encode('utf-8'))
                except Exception as e:
                    raise Exception('Could not parse record XML: %s' % str(e))

                # get namespaces
                nsmap = get_namespaces(xml_root)

                # get unique identifier
                if job_details['xpath_record_id'] != '':
                    record_id = xml_root.xpath(
                        job_details['xpath_record_id'], namespaces=nsmap)
                    if len(record_id) == 1:
                        record_id = record_id[0].text
                    elif len(xml_root) > 1:
                        raise AmbiguousIdentifier(
                            'multiple elements found for identifier xpath: %s' % job_details['xpath_record_id'])
                    elif len(xml_root) == 0:
                        raise AmbiguousIdentifier(
                            'no elements found for identifier xpath: %s' % job_details['xpath_record_id'])
                else:
                    record_id = hashlib.md5(
                        doc_string.encode('utf-8')).hexdigest()

                # return success Row
                return Row(
                    record_id=record_id,
                    document=etree.tostring(xml_root).decode('utf-8'),
                    error='',
                    job_id=int(job_id),
                    oai_set='',
                    success=True
                )

            # catch missing or ambiguous identifiers
            except AmbiguousIdentifier as e:

                # hash record string to produce a unique id
                record_id = hashlib.md5(doc_string.encode('utf-8')).hexdigest()

                # return error Row
                return Row(
                    record_id=record_id,
                    document=etree.tostring(xml_root).decode('utf-8'),
                    error="AmbiguousIdentifier: %s" % str(e),
                    job_id=int(job_id),
                    oai_set='',
                    success=True
                )

            # handle all other exceptions
            except Exception as e:

                # hash record string to produce a unique id
                record_id = hashlib.md5(doc_string.encode('utf-8')).hexdigest()

                # return error Row
                return Row(
                    record_id=record_id,
                    document=doc_string,
                    error=str(e),
                    job_id=int(job_id),
                    oai_set='',
                    success=False
                )

        # map with parse_records_udf
        job_id = self.job.id
        job_details = self.job_details
        ns_regex = re.compile(r'<%s(.?|.+?)>' %
                              self.job_details['document_element_root'])
        records = static_rdd.map(
            lambda row: parse_records_udf(job_id, row, job_details))

        # convert back to DF
        records = records.toDF()

        # fingerprint records and set transformed
        records = self.fingerprint_records(records)
        records = records.withColumn(
            'transformed', pyspark_sql_functions.lit(True))

        # index records to DB and index to ElasticSearch
        self.save_records(
            records_df=records,
            assign_combine_id=True
        )

        # close job
        self.close_job()


class HarvestTabularDataSpark(CombineSparkJob):
    """
    Spark code for harvesting tabular data (e.g. spreadsheets)
    """

    def spark_function(self):
        """
        Harvest tabular data provided by user, convert to XML records.
                - handles Delimited data (e.g. csv, tsv) or JSON lines

        Args:
                spark (pyspark.sql.session.SparkSession): provided by pyspark context
                kwargs:
                        job_id (int): Job ID
                        static_payload (str): path of static payload on disk
                        # TODO: add other kwargs here from static job
                        index_mapper (str): class name from core.spark.es, extending BaseMapper
                        validation_scenarios (list): list of Validadtion Scenario IDs

        Returns:
                None:
                - opens and parses static files from payload
                - indexes records into DB
                - map / flatten records and indexes to ES
        """

        # init job
        self.init_job()
        self.update_jobGroup('Running Harvest Tabular Data Job')

        # load CSV
        if self.job_details['payload_filepath'].endswith('.csv'):
            dc_df = self.spark.read.format('com.databricks.spark.csv')\
                .options(header=True, inferschema=True, multiLine=True)\
                .load('file://%s' % self.job_details['payload_filepath'])

        # load JSON
        elif self.job_details['payload_filepath'].endswith('.json'):
            dc_df = self.spark.read.json(
                'file://%s' % self.job_details['payload_filepath'])

        # repartition
        dc_df = dc_df.repartition(settings.SPARK_REPARTITION)

        # partition udf
        def kvp_to_xml_pt_udf(pt):

            for row in pt:

                # get as dict
                row_dict = row.asDict()

                # pop combine fields if exist, ascribe to new dictionary
                fields = ['combine_id', 'db_id', 'fingerprint',
                          'publish_set_id', 'record_id', 'xml2kvp_meta']
                combine_vals_dict = {field: row_dict.pop(
                    field, None) for field in fields}

                try:

                    # convert dictionary to XML with XML2kvp
                    xml_record_str = XML2kvp.kvp_to_xml(
                        row_dict, serialize_xml=True, **xml2kvp_config.__dict__)

                    # return success Row
                    yield Row(
                        record_id=combine_vals_dict.get('record_id'),
                        document=xml_record_str,
                        error='',
                        job_id=int(job_id),
                        oai_set='',
                        success=True
                    )

                # handle all other exceptions
                except Exception as e:

                    # return error Row
                    yield Row(
                        record_id=combine_vals_dict.get('record_id'),
                        document='',
                        error=str(e),
                        job_id=int(job_id),
                        oai_set='',
                        success=False
                    )

        # mixin passed configurations with defaults
        fm_config = json.loads(self.job_details['fm_harvest_config_json'])
        xml2kvp_config = XML2kvp(**fm_config)

        # map partitions
        job_id = self.job.id
        job_details = self.job_details
        records = dc_df.rdd.mapPartitions(kvp_to_xml_pt_udf)

        # convert back to DF
        records = records.toDF()

        # fingerprint records and set transformed
        records = self.fingerprint_records(records)
        records = records.withColumn(
            'transformed', pyspark_sql_functions.lit(True))

        # index records to DB and index to ElasticSearch
        self.save_records(
            records_df=records,
            assign_combine_id=True
        )

        # close job
        self.close_job()


class TransformSpark(CombineSparkJob):
    """
    Spark code for Transform jobs
    """

    def spark_function(self):
        """
        Transform records based on Transformation Scenario.

        Args:
                spark (pyspark.sql.session.SparkSession): provided by pyspark context
                kwargs:
                        job_id (int): Job ID
                        job_input (str): location of avro files on disk
                        transformation_id (str): id of Transformation Scenario
                        index_mapper (str): class name from core.spark.es, extending BaseMapper
                        validation_scenarios (list): list of Validadtion Scenario IDs

        Returns:
                None
                - transforms records via XSL, writes new records to avro files on disk
                - indexes records into DB
                - map / flatten records and indexes to ES
        """

        # init job
        self.init_job()
        self.update_jobGroup('Running Transform Job')

        # get input records
        records = self.get_input_records(filter_input_records=True)

        # fork as input_records
        input_records = records

        # get transformation json
        sel_trans = json.loads(
            self.job_details['transformation']['scenarios_json'])

        # loop through oredered transformations
        for trans in sel_trans:

            # load transformation
            transformation = Transformation.objects.get(
                pk=int(trans['trans_id']))
            self.logger.info('Applying transformation #%s: %s' %
                             (trans['index'], transformation.name))

            # if xslt type transformation
            if transformation.transformation_type == 'xslt':
                records = self.transform_xslt(transformation, records)

            # if python type transformation
            if transformation.transformation_type == 'python':
                records = self.transform_python(transformation, records)

            # if OpenRefine type transformation
            if transformation.transformation_type == 'openrefine':
                # get XML2kvp settings from input Job
                input_job_details = input_job.job_details_dict
                input_job_fm_config = input_job_details['field_mapper_config']

                # pass config json
                records = self.transform_openrefineactions(
                    transformation, records, input_job_fm_config)

            # convert back to DataFrame
            records = records.toDF()

        # fingerprint Record document
        records = self.fingerprint_records(records)

        # assign to records_trans
        records_trans = records

        # write `transformed` column based on new fingerprint
        records_trans = records_trans.alias("records_trans").join(input_records.alias("input_records"),
                                                                  input_records.combine_id == records_trans.combine_id,
                                                                  'left').select(
            *['records_trans.%s' % c for c in records_trans.columns if c not in ['transformed']],
            pyspark_sql_functions.when(records_trans.fingerprint != input_records.fingerprint,
                                       pyspark_sql_functions.lit(True)).otherwise(
                pyspark_sql_functions.lit(False)).alias('transformed'))

        # index records to DB and index to ElasticSearch
        self.save_records(
            records_df=records_trans
        )

        # close job
        self.close_job()

    def transform_xslt(self, transformation, records):
        """
        Method to transform records with XSLT, using pyjxslt server

        Args:
                job: job from parent job
                transformation: Transformation Scenario from parent job
                records (pyspark.sql.DataFrame): DataFrame of records pre-transformation

        Return:
                records_trans (rdd): transformed records as RDD
        """

        def transform_xslt_pt_udf(pt):

            # transform with pyjxslt gateway
            gw = pyjxslt.Gateway(6767)
            gw.add_transform('xslt_transform', xslt_string)

            # loop through rows in partition
            for row in pt:

                try:
                    result = gw.transform('xslt_transform', row.document)
                    # attempt XML parse to confirm well-formedness
                    # error will bubble up in try/except
                    valid_xml = etree.fromstring(result.encode('utf-8'))

                    # set trans_result tuple
                    trans_result = (result, '', True)

                # catch transformation exception and save exception to 'error'
                except Exception as e:
                    # set trans_result tuple
                    trans_result = (row.document, str(e), False)

                # yield each Row in mapPartition
                yield Row(
                    combine_id=row.combine_id,
                    record_id=row.record_id,
                    document=trans_result[0],
                    error=trans_result[1],
                    job_id=int(job_id),
                    oai_set=row.oai_set,
                    success=trans_result[2],
                    fingerprint=row.fingerprint,
                    transformed=row.transformed
                )

            # drop transform
            gw.drop_transform('xslt_transform')

        # get XSLT transformation as string
        xslt_string = transformation.payload

        # transform via rdd.map and return
        job_id = self.job.id

        # perform transformations a la mapPartitions
        records_trans = records.rdd.mapPartitions(transform_xslt_pt_udf)
        return records_trans

    def transform_python(self, transformation, records):
        """
        Transform records via python code snippet.

        Required:
                - a function named `python_record_transformation(record)` in transformation.payload python code

        Args:
                job: job from parent job
                transformation: Transformation Scenario from parent job
                records (pyspark.sql.DataFrame): DataFrame of records pre-transformation

        Return:
                records_trans (rdd): transformed records as RDD
        """

        # define udf function for python transformation
        def transform_python_pt_udf(pt):

            # get python function from Transformation Scenario
            temp_pyts = ModuleType('temp_pyts')
            exec(python_code, temp_pyts.__dict__)

            for row in pt:

                # try:

                # prepare row as parsed document with PythonUDFRecord class
                prtb = PythonUDFRecord(row)

                # run transformation
                trans_result = temp_pyts.python_record_transformation(prtb)

                # convert any possible byte responses to string
                if type(trans_result[0]) == bytes:
                    trans_result[0] = trans_result[0].decode('utf-8')
                if type(trans_result[1]) == bytes:
                    trans_result[1] = trans_result[1].decode('utf-8')

                # except Exception as e:
                # 	# set trans_result tuple
                # 	trans_result = (row.document, str(e), False)

                # return Row
                yield Row(
                    combine_id=row.combine_id,
                    record_id=row.record_id,
                    document=trans_result[0],
                    error=trans_result[1],
                    job_id=int(job_id),
                    oai_set=row.oai_set,
                    success=trans_result[2],
                    fingerprint=row.fingerprint,
                    transformed=row.transformed
                )

        # transform via rdd.mapPartitions and return
        job_id = self.job.id
        python_code = transformation.payload
        records_trans = records.rdd.mapPartitions(transform_python_pt_udf)
        return records_trans

    def transform_openrefineactions(self, transformation, records, input_job_fm_config):
        """
        Transform records per OpenRefine Actions JSON

        Args:
                job: job from parent job
                transformation: Transformation Scenario from parent job
                records (pyspark.sql.DataFrame): DataFrame of records pre-transformation

        Return:
                records_trans (rdd): transformed records as RDD
        """

        # define udf function for python transformation
        def transform_openrefine_pt_udf(pt):

            # parse OpenRefine actions JSON
            or_actions = json.loads(or_actions_json)

            # loop through rows
            for row in pt:

                try:

                    # prepare row as parsed document with PythonUDFRecord class
                    prtb = PythonUDFRecord(row)

                    # loop through actions
                    for event in or_actions:

                        # handle mass edits
                        if event['op'] == 'core/mass-edit':

                            # for each column, reconstitue columnName --> XPath
                            xpath = XML2kvp.k_to_xpath(
                                event['columnName'], **input_job_fm_config)

                            # find elements for potential edits
                            eles = prtb.xml.xpath(xpath, namespaces=prtb.nsmap)

                            # loop through elements
                            for ele in eles:

                                # loop through edits
                                for edit in event['edits']:

                                    # check if element text in from, change
                                    if ele.text in edit['from']:
                                        ele.text = edit['to']

                        # handle jython
                        if event['op'] == 'core/text-transform' and event['expression'].startswith('jython:'):

                            # fire up temp module
                            temp_pyts = ModuleType('temp_pyts')

                            # parse code
                            code = event['expression'].split('jython:')[1]

                            # wrap in function and write to temp module
                            code = 'def temp_func(value):\n%s' % textwrap.indent(
                                code, prefix='    ')
                            exec(code, temp_pyts.__dict__)

                            # get xpath (unique to action, can't pre learn)
                            xpath = XML2kvp.k_to_xpath(
                                event['columnName'], **input_job_fm_config)

                            # find elements for potential edits
                            eles = prtb.xml.xpath(xpath, namespaces=prtb.nsmap)

                            # loop through elements
                            for ele in eles:
                                ele.text = temp_pyts.temp_func(ele.text)

                    # re-serialize as trans_result
                    trans_result = (etree.tostring(
                        prtb.xml).decode('utf-8'), '', 1)

                except Exception as e:
                    # set trans_result tuple
                    trans_result = (row.document, str(e), False)

                # return Row
                yield Row(
                    combine_id=row.combine_id,
                    record_id=row.record_id,
                    document=trans_result[0],
                    error=trans_result[1],
                    job_id=int(job_id),
                    oai_set=row.oai_set,
                    success=trans_result[2],
                    fingerprint=row.fingerprint,
                    transformed=row.transformed
                )

        # transform via rdd.mapPartitions and return
        job_id = self.job.id
        or_actions_json = transformation.payload
        records_trans = records.rdd.mapPartitions(transform_openrefine_pt_udf)
        return records_trans


class MergeSpark(CombineSparkJob):
    """
    Spark code for running Merge type jobs.  Also used for duplciation, analysis, and others.
    Note: Merge jobs merge only successful documents from an input job, not the errors
    """

    def spark_function(self):
        """
        Harvest records, select non-null, and write to avro files

        Args:
                spark (pyspark.sql.session.SparkSession): provided by pyspark context
                kwargs:
                        job_id (int): Job ID
                        job_inputs (list): list of locations of avro files on disk
                        index_mapper (str): class name from core.spark.es, extending BaseMapper
                        validation_scenarios (list): list of Validadtion Scenario IDs

        Returns:
                None
                - merges records from previous jobs, writes new aggregated records to avro files on disk
                - indexes records into DB
                - map / flatten records and indexes to ES
        """

        # init job
        self.init_job()
        self.update_jobGroup('Running Merge/Duplicate Job')

        # get input records
        records = self.get_input_records(filter_input_records=True)

        # update job column, overwriting job_id from input jobs in merge
        job_id = self.job.id
        job_id_udf = udf(lambda record_id: job_id, IntegerType())
        records = records.withColumn('job_id', job_id_udf(records.record_id))

        # set transformed column to False
        records = records.withColumn(
            'transformed', pyspark_sql_functions.lit(False))

        # if Analysis Job, do not write avro
        if self.job.job_type == 'AnalysisJob':
            write_avro = False
        else:
            write_avro = settings.WRITE_AVRO

        # index records to DB and index to ElasticSearch
        self.save_records(
            records_df=records,
            write_avro=write_avro
        )

        # close job
        self.close_job()


####################################################################
# Combine Spark Patches											   #
####################################################################


class CombineSparkPatch(object):
    """
    Base class for Combine Spark Patches.
            - these are considered categorically "secondary" to the main
            CombineSparkJobs above, but may be just as long running
    """

    def __init__(self, spark, **kwargs):
        self.spark = spark

        self.kwargs = kwargs

        # init logging support
        spark.sparkContext.setLogLevel('INFO')
        log4jLogger = spark.sparkContext._jvm.org.apache.log4j
        self.logger = log4jLogger.LogManager.getLogger(__name__)

    def update_jobGroup(self, description, job_id):
        """
        Method to update spark jobGroup
        """

        self.logger.info("### %s" % description)
        self.spark.sparkContext.setJobGroup(
            "%s" % job_id, "%s, Job #%s" % (description, job_id))

class ReindexSparkPatch(CombineSparkPatch):
    """
    Class to handle Job re-indexing

    Args:
            kwargs(dict):
                    - job_id (int): ID of Job to reindex
    """

    def spark_function(self):
        # get job and set to self
        self.job = Job.objects.get(pk=int(self.kwargs['job_id']))
        self.update_jobGroup('Running Re-Index Job', self.job.id)

        # get records as DF
        pipeline = json.dumps({'$match': {'job_id': self.job.id}})
        db_records = self.spark.read.format("com.mongodb.spark.sql.DefaultSource")\
            .option("uri", "mongodb://%s" % settings.MONGO_HOST)\
            .option("database", "combine")\
            .option("collection", "record")\
            .option("partitioner", "MongoSamplePartitioner")\
            .option("spark.mongodb.input.partitionerOptions.partitionSizeMB", settings.MONGO_READ_PARTITION_SIZE_MB)\
            .option("pipeline", pipeline).load()

        # reindex
        ESIndex.index_job_to_es_spark(
            self.spark,
            job=self.job,
            records_df=db_records,
            field_mapper_config=json.loads(self.kwargs['fm_config_json'])
        )


class RunNewValidationsSpark(CombineSparkPatch):
    """
    Class to run new validations for Job

    Args:
            kwargs(dict):
                    - job_id (int): ID of Job
                    - validation_scenarios (list): list of validation scenarios to run
    """

    def spark_function(self):
        # get job and set to self
        self.job = Job.objects.get(pk=int(self.kwargs['job_id']))
        self.update_jobGroup('Running New Validation Scenarios', self.job.id)

        pipeline = json.dumps({'$match': {'job_id': self.job.id}})
        db_records = self.spark.read.format("com.mongodb.spark.sql.DefaultSource")\
            .option("uri", "mongodb://%s" % settings.MONGO_HOST)\
            .option("database", "combine")\
            .option("collection", "record")\
            .option("partitioner", "MongoSamplePartitioner")\
            .option("spark.mongodb.input.partitionerOptions.partitionSizeMB", settings.MONGO_READ_PARTITION_SIZE_MB)\
            .option("pipeline", pipeline).load()

        # run Validation Scenarios
        if 'validation_scenarios' in self.kwargs.keys():
            vs = ValidationScenarioSpark(
                spark=self.spark,
                job=self.job,
                records_df=db_records,
                validation_scenarios=ast.literal_eval(
                    self.kwargs['validation_scenarios'])
            )
            vs.run_record_validation_scenarios()


class RemoveValidationsSpark(CombineSparkPatch):
    """
    Class to remove validations for Job

    Args:
            kwargs(dict):
                    - job_id (int): ID of Job
                    - validation_scenarios (list): list of validation scenarios to run
    """

    def spark_function(self):

        # get job and set to self
        self.job = Job.objects.get(pk=int(self.kwargs['job_id']))
        self.update_jobGroup('Removing Validation Scenario', self.job.id)

        # create pipeline to select INVALID records, that may become valid
        pipeline = json.dumps(
            {'$match': {'$and': [{'job_id': self.job.id}, {'valid': False}]}})
        db_records = self.spark.read.format("com.mongodb.spark.sql.DefaultSource")\
            .option("uri", "mongodb://%s" % settings.MONGO_HOST)\
            .option("database", "combine")\
            .option("collection", "record")\
            .option("partitioner", "MongoSamplePartitioner")\
            .option("spark.mongodb.input.partitionerOptions.partitionSizeMB", settings.MONGO_READ_PARTITION_SIZE_MB)\
            .option("pipeline", pipeline).load()

        # if not nothing to update, skip
        if not db_records.rdd.isEmpty():
            # run Validation Scenarios
            if 'validation_scenarios' in self.kwargs.keys():
                vs = ValidationScenarioSpark(
                    spark=self.spark,
                    job=self.job,
                    records_df=db_records,
                    validation_scenarios=ast.literal_eval(
                        self.kwargs['validation_scenarios'])
                )
                vs.remove_validation_scenarios()


class RunDBDM(CombineSparkPatch):
    """
    Class to run DPLA Bulk Data Match as patch job

    Args:
            kwargs(dict):
                    - job_id (int): ID of Job
                    - dbdd_id (int): int of DBDD instance to use
    """

    def spark_function(self):
        # get job and set to self
        self.job = Job.objects.get(pk=int(self.kwargs['job_id']))
        self.update_jobGroup('Running DPLA Bulk Data Match', self.job.id)

        # get full dbdd es
        dbdd = DPLABulkDataDownload.objects.get(pk=int(self.kwargs['dbdd_id']))
        dpla_df = get_job_es(self.spark, indices=[
                             dbdd.es_index], doc_type='item')

        # get job mapped fields
        es_df = get_job_es(self.spark, job_id=self.job.id)

        # get job records
        records_df = get_job_as_df(self.spark, self.job.id)

        # join on isShownAt
        matches_df = es_df.join(
            dpla_df, es_df['dpla_isShownAt'] == dpla_df['isShownAt'], 'leftsemi')

        # select records_df for writing
        update_dbdm_df = records_df.join(
            matches_df, records_df['_id']['oid'] == matches_df['db_id'], 'leftsemi')

        # set dbdm column to match
        update_dbdm_df = update_dbdm_df.withColumn(
            'dbdm', pyspark_sql_functions.lit(True))

        # write to DB
        update_dbdm_df.write.format("com.mongodb.spark.sql.DefaultSource")\
            .mode("append")\
            .option("uri", "mongodb://%s" % settings.MONGO_HOST)\
            .option("database", "combine")\
            .option("collection", "record").save()


####################################################################
# State IO          											   #
####################################################################

class CombineStateIO(object):
    """
    Base class for Combine State IO work.
    """

    def __init__(self, spark, **kwargs):
        self.spark = spark

        self.kwargs = kwargs

        # capture common params
        self.import_path = kwargs.get('import_path', None)
        self.import_manifest = kwargs.get('import_manifest', None)

        # init logging support
        spark.sparkContext.setLogLevel('INFO')
        log4jLogger = spark.sparkContext._jvm.org.apache.log4j
        self.logger = log4jLogger.LogManager.getLogger(__name__)

    def update_jobGroup(self, group_id, description):
        """
        Method to update spark jobGroup
        """

        self.spark.sparkContext.setJobGroup(group_id, description)


class CombineStateIOImport(CombineStateIO):
    """
    Class to handle state imports

    Args:
            kwargs(dict):
                    - import_path (str): string of unzipped export directory on disk
                    - import_manifest (dict): dictionary containing import information, including hash of old:new primary keys
    """

    def spark_function(self):

        # import records
        self._import_records()

        # import validations
        self._import_validations()

        # import mapped fields (ES)
        self._import_mapped_fields()

    def _import_records(self):
        """
        Method to import records to Mongo
        """

        # import records
        self.update_jobGroup(self.import_manifest.get(
            'import_id', uuid.uuid4().hex), 'StateIO: Importing Records')

        # loop through jobs
        for orig_job_id, clone_job_id in self.import_manifest['pk_hash']['jobs'].items():

            # assemple location of export
            records_json_filepath = '%s/record_exports/j%s_mongo_records.json' % (
                self.import_path, orig_job_id)

            # load as dataframe
            records_df = self.spark.read.json(records_json_filepath)

            # copy original _id
            records_df = records_df\
                .withColumn('orig_id', records_df['_id']['$oid'])

            # flatten fingerprint column
            try:
                records_df = records_df.withColumn(
                    'fingerprint', records_df.fingerprint['$numberLong'])
            except:
                records_df = records_df.withColumn(
                    'fingerprint', records_df.fingerprint)

            # update job_id
            records_df = records_df.withColumn(
                'job_id', pyspark_sql_functions.lit(int(clone_job_id)))

            # write records to MongoDB, dropping _id in process
            records_df.select([col for col in records_df.columns if col != '_id'])\
                .write.format("com.mongodb.spark.sql.DefaultSource")\
                .mode("append")\
                .option("uri", "mongodb://%s" % settings.MONGO_HOST)\
                .option("database", "combine")\
                .option("collection", "record").save()

    def _import_validations(self):
        """
        Method to import validations to Mongo
        """

        # import validations
        self.update_jobGroup(self.import_manifest.get(
            'import_id', uuid.uuid4().hex), 'StateIO: Importing Validations')

        # loop through jobs
        for orig_job_id, clone_job_id in self.import_manifest['pk_hash']['jobs'].items():

            # assemple location of export
            validations_json_filepath = '%s/validation_exports/j%s_mongo_validations.json' % (
                self.import_path, orig_job_id)

            # load as dataframe
            validations_df = self.spark.read.json(validations_json_filepath)

            # check for dataframe rows to proceed
            if len(validations_df.take(1)) > 0:
                # read first row to get old validation_scenario_id, and run through pk_hash for new one
                row = validations_df.take(1)[0]
                vs_id = int(row.validation_scenario_id['$numberLong'])
                new_vs_id = self.import_manifest['pk_hash']['validations'][vs_id]

                # flatten record_id
                validations_df = validations_df.withColumn(
                    'record_id', validations_df['record_id']['$oid'])

                # retrieve newly written records for this Job
                pipeline = json.dumps(
                    {'$match': {'job_id': clone_job_id, 'success': True}})
                records_df = self.spark.read.format("com.mongodb.spark.sql.DefaultSource")\
                    .option("uri", "mongodb://%s" % settings.MONGO_HOST)\
                    .option("database", "combine")\
                    .option("collection", "record")\
                    .option("partitioner", "MongoSamplePartitioner")\
                    .option("spark.mongodb.input.partitionerOptions.partitionSizeMB",
                            settings.MONGO_READ_PARTITION_SIZE_MB) \
                    .option("pipeline", pipeline).load()

                # join on validations_df.record_id : records_df.orig_id
                updated_validations_df = validations_df.drop('_id').alias('validations_df').join(
                    records_df.select('_id', 'orig_id').alias('records_df'),
                    validations_df['record_id'] == records_df['orig_id'])

                # update record_id
                updated_validations_df = updated_validations_df.withColumn(
                    'record_id', updated_validations_df['_id'])

                # limit to validation columns
                updated_validations_df = updated_validations_df.select(
                    validations_df.columns).drop('_id')

                # flatten
                updated_validations_df = updated_validations_df.withColumn('fail_count',
                                                                           updated_validations_df.fail_count[
                                                                               '$numberLong'].cast(LongType()))
                updated_validations_df = updated_validations_df.withColumn('job_id', pyspark_sql_functions.lit(
                    int(clone_job_id)).cast(LongType()))

                # update validation scenario id
                updated_validations_df = updated_validations_df.withColumn('validation_scenario_id',
                                                                           pyspark_sql_functions.lit(
                                                                               int(new_vs_id)).cast(LongType()))

                # write records to MongoDB
                updated_validations_df.write.format("com.mongodb.spark.sql.DefaultSource")\
                    .mode("append")\
                    .option("uri", "mongodb://%s" % settings.MONGO_HOST)\
                    .option("database", "combine")\
                    .option("collection", "record_validation").save()

    def _import_mapped_fields(self, reindex=True):
        """
        Method to import mapped fields to ElasticSearch
                - re-map and index, based on saved Job params
                - inefficient to export/import ElasticSearch records, when modifying values

        QUESTION: Why is this partitioned to 200, when reading from Mongo appears to be
                the same for Re-Indexing?
        """

        # import mapped fields
        self.update_jobGroup(self.import_manifest.get('import_id', uuid.uuid4().hex),
                             'StateIO: Importing Mapped Fields')

        # loop through jobs
        for orig_job_id, clone_job_id in self.import_manifest['pk_hash']['jobs'].items():

            # re-index (default)
            if reindex:

                # get job and set to self
                job = Job.objects.get(pk=int(clone_job_id))

                # get records as DF
                pipeline = json.dumps({'$match': {'job_id': job.id}})
                records_df = self.spark.read.format("com.mongodb.spark.sql.DefaultSource")\
                    .option("uri", "mongodb://%s" % settings.MONGO_HOST)\
                    .option("database", "combine")\
                    .option("collection", "record")\
                    .option("partitioner", "MongoSamplePartitioner")\
                    .option("spark.mongodb.input.partitionerOptions.partitionSizeMB",
                            settings.MONGO_READ_PARTITION_SIZE_MB) \
                    .option("pipeline", pipeline).load()

                # reindex
                ESIndex.index_job_to_es_spark(
                    self.spark,
                    job=job,
                    records_df=records_df,
                    field_mapper_config=job.job_details_dict.get(
                        'field_mapper_config')
                )

            # else, fallback on slower, import of serialized records
            else:

                # assemple location of export
                mapped_fields_json_filepath = '%s/mapped_fields_exports/j%s_mapped_fields.json' % (
                    self.import_path, orig_job_id)

                # read raw JSON lines
                json_lines_rdd = self.spark.sparkContext.textFile(
                    mapped_fields_json_filepath)

                # parse to expose record db_id
                def parser_udf(row):

                    # parse JSON
                    d = json.loads(row)

                    # return tuple with exposed original id
                    return d['db_id'], row

                orig_id_rdd = json_lines_rdd.map(lambda row: parser_udf(row))

                # to dataframe for join
                orig_id_df = orig_id_rdd.toDF()

                # retrieve newly written records for this Job
                pipeline = json.dumps(
                    {'$match': {'job_id': clone_job_id, 'success': True}})
                records_df = self.spark.read.format("com.mongodb.spark.sql.DefaultSource")\
                    .option("uri", "mongodb://%s" % settings.MONGO_HOST)\
                    .option("database", "combine")\
                    .option("collection", "record")\
                    .option("partitioner", "MongoSamplePartitioner")\
                    .option("spark.mongodb.input.partitionerOptions.partitionSizeMB", 4)\
                    .option("pipeline", pipeline).load()

                # join on id
                join_id_df = orig_id_df.join(
                    records_df, orig_id_df['_1'] == records_df['orig_id'])

                # rewrite _1 as new id for Record
                new_id_df = join_id_df.withColumn(
                    '_1', join_id_df['_id']['oid'])

                # select only what's needed
                new_id_df = new_id_df.select('_1', '_2')

                # convert back to RDD
                new_id_rdd = new_id_df.rdd

                # update db_id in JSON destined for ES
                def update_db_id_udf(row):

                    # load json
                    d = json.loads(row['_2'])

                    # set identifiers
                    d['db_id'] = row['_1']
                    d['temp_id'] = row['_1']

                    # convert lists to tuples
                    for k, v in d.items():
                        if type(v) == list:
                            d[k] = tuple(v)

                    return row['_1'], d

                new_id_rdd = new_id_rdd.map(lambda row: update_db_id_udf(row))

                # create index in advance
                index_name = 'j%s' % clone_job_id
                es_handle_temp = Elasticsearch(hosts=[settings.ES_HOST])
                if not es_handle_temp.indices.exists(index_name):
                    # put combine es index templates
                    template_body = {
                        'template': '*',
                                    'settings': {
                                        'number_of_shards': 1,
                                        'number_of_replicas': 0,
                                        'refresh_interval': -1
                                    },
                        'mappings': {
                                        'record': {
                                            'date_detection': False,
                                            'properties': {
                                                'combine_db_id': {
                                                    'type': 'integer'
                                                }
                                            }
                                        }
                                    }
                    }
                    es_handle_temp.indices.put_template(
                        'combine_template', body=json.dumps(template_body))

                    # create index
                    es_handle_temp.indices.create(index_name)

                # index
                new_id_rdd.saveAsNewAPIHadoopFile(
                    path='-',
                    outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
                    keyClass="org.apache.hadoop.io.NullWritable",
                    valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",
                    conf={
                        "es.resource": "%s/record" % index_name,
                        "es.nodes": "%s:9200" % settings.ES_HOST,
                        "es.mapping.exclude": "temp_id",
                        "es.mapping.id": "temp_id",
                    }
                )
