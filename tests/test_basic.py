import django
from lxml import etree
import os
import pytest
import shutil
import sys
import time
import uuid

# logging
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# init django settings file to retrieve settings
os.environ['DJANGO_SETTINGS_MODULE'] = 'combine.settings'
sys.path.append('/opt/combine')
django.setup()
from django.conf import settings

# import core
from core.models import *


#############################################################################
# Tests Setup
#############################################################################

@pytest.mark.run(order=1)
def test_organization_create(VO):
    '''
    Test creation of organization
    '''

    # instantiate and save
    VO.org = Organization(
        name='test_org_%s' % uuid.uuid4().hex,
        description=''
    )
    VO.org.save()
    assert type(VO.org.id) == int


@pytest.mark.run(order=2)
def test_record_group_create(VO):
    '''
    Test creation of record group
    '''

    # instantiate and save
    VO.rg = RecordGroup(
        organization=VO.org,
        name='test_record_group_%s' % uuid.uuid4().hex,
        description=''
    )
    VO.rg.save()
    assert type(VO.rg.id) == int


#############################################################################
# Test Harvest
#############################################################################

@pytest.mark.run(order=3)
def test_static_harvest(VO):
    '''
    Test static harvest of XML records from disk
    '''

    # copy test data to /tmp
    payload_dir = '/tmp/%s' % uuid.uuid4().hex
    shutil.copytree('/opt/combine/tests/data/static_harvest_data', payload_dir)

    # emulate request.POST
    request_dict = {
        'dbdd': '',
        'job_note': '',
        'xpath_record_id': '',
        'static_filepath': payload_dir,
        'fm_config_json': '{"add_literals":{},"capture_attribute_values":[],"concat_values_on_all_fields":false,"concat_values_on_fields":{},"copy_to":{},"copy_to_regex":{},"copy_value_to_regex":{},"error_on_delims_collision":false,"exclude_attributes":[],"exclude_elements":[],"include_all_attributes":false,"include_attributes":[],"include_sibling_id":false,"multivalue_delim":"|","node_delim":"_","ns_prefix_delim":"|","remove_copied_key":true,"remove_copied_value":false,"remove_ns_prefix":true,"repeating_element_suffix_count":false,"self_describing":false,"skip_attribute_ns_declarations":true,"skip_repeating_values":true,"skip_root":false,"split_values_on_all_fields":false,"split_values_on_fields":{}}',
        'static_payload': '',
        'job_name': '',
        'field_mapper': 'default',
        'rits': '',
        'additional_namespace_decs': 'xmlns:mods="http://www.loc.gov/mods/v3"',
        'document_element_root': 'mods:mods'
    }
    query_dict = QueryDict('', mutable=True)
    query_dict.update(request_dict)

    # init job, using Variable Object (VO)
    cjob = CombineJob.init_combine_job(
        user=VO.user,
        record_group=VO.rg,
        job_type_class=HarvestStaticXMLJob,
        job_params=query_dict,
        files={},
        hash_payload_filename=False
    )

    # start job and update status
    job_status = cjob.start_job()

    # if job_status is absent, report job status as failed
    if job_status == False:
        cjob.job.status = 'failed'
        cjob.job.save()

    # poll until complete
    for x in range(0, 480):

        # pause
        time.sleep(1)

        # refresh session
        cjob.job.update_status()

        # check status
        if cjob.job.status != 'available':
            continue
        else:
            break

    # save static harvest job to VO
    VO.static_harvest_cjob = cjob

    # remove payload_dir
    shutil.rmtree(payload_dir)

    # assert job is done and available via livy
    assert VO.static_harvest_cjob.job.status == 'available'

    # assert record count is 250
    assert VO.static_harvest_cjob.job.record_count == 250

    # assert no indexing failures
    assert len(VO.static_harvest_cjob.get_indexing_failures()) == 0


# #############################################################################
# # Test Transform
# #############################################################################

def prepare_transform():
    '''
    Create temporary transformation scenario based on tests/data/mods_transform.xsl
    '''

    with open('tests/data/mods_transform.xsl', 'r') as f:
        xsl_string = f.read()
    trans = Transformation(
        name='temp_mods_transformation',
        payload=xsl_string,
        transformation_type='xslt',
        filepath='will_be_updated'
    )
    trans.save()

    # return transformation
    return trans


@pytest.mark.run(order=4)
def test_static_transform(VO):
    '''
    Test static harvest of XML records from disk
    '''

    # prepare and capture temporary transformation scenario
    VO.transformation_scenario = prepare_transform()

    # emulate request.POST
    request_dict = {
        'dbdd': '',
        'field_mapper': 'default',
        'filter_dupe_record_ids': 'true',
        'fm_config_json': '{"add_literals":{},"capture_attribute_values":[],"concat_values_on_all_fields":false,"concat_values_on_fields":{},"copy_to":{},"copy_to_regex":{},"copy_value_to_regex":{},"error_on_delims_collision":false,"exclude_attributes":[],"exclude_elements":[],"include_all_attributes":false,"include_attributes":[],"include_sibling_id":false,"multivalue_delim":"|","node_delim":"_","ns_prefix_delim":"|","remove_copied_key":true,"remove_copied_value":false,"remove_ns_prefix":true,"repeating_element_suffix_count":false,"self_describing":false,"skip_attribute_ns_declarations":true,"skip_repeating_values":true,"skip_root":false,"split_values_on_all_fields":false,"split_values_on_fields":{}}',
        'input_es_query_valve': '',
        'input_job_id': VO.static_harvest_cjob.job.id,
        'input_numerical_valve': '',
        'input_validity_valve': 'all',
        'job_name': '',
        'job_note': '',
        'rits': '',
        'sel_trans_json': '[{"index":0,"trans_id":%s}]' % VO.transformation_scenario.id
    }
    query_dict = QueryDict('', mutable=True)
    query_dict.update(request_dict)

    # init job
    cjob = CombineJob.init_combine_job(
        user=VO.user,
        record_group=VO.rg,
        job_type_class=TransformJob,
        job_params=query_dict)

    # start job and update status
    job_status = cjob.start_job()

    # if job_status is absent, report job status as failed
    if job_status == False:
        cjob.job.status = 'failed'
        cjob.job.save()

    # poll until complete
    for x in range(0, 480):

        # pause
        time.sleep(1)

        # refresh session
        cjob.job.update_status()

        # check status
        if cjob.job.status != 'available':
            continue
        else:
            break

    # save static harvest job to VO
    VO.static_transform_cjob = cjob

    # assert job is done and available via livy
    assert VO.static_transform_cjob.job.status == 'available'

    # assert record count is 250
    assert VO.static_transform_cjob.job.record_count == 250

    # assert no indexing failures
    assert len(VO.static_transform_cjob.get_indexing_failures()) == 0

    # remove transformation
    assert VO.transformation_scenario.delete()[0] > 0


# #############################################################################
# # Test Validation Scenarios
# #############################################################################

@pytest.mark.run(order=5)
def test_add_schematron_validation_scenario(VO):
    '''
    Add schematron validation
    '''

    # get schematron validation from test data
    with open('tests/data/schematron_validation.sch', 'r') as f:
        sch_payload = f.read()

    # init new validation scenario
    schematron_validation_scenario = ValidationScenario(
        name='temp_vs_%s' % str(uuid.uuid4()),
        payload=sch_payload,
        validation_type='sch',
        default_run=False
    )
    schematron_validation_scenario.save()

    # pin to VO
    VO.schematron_validation_scenario = schematron_validation_scenario

    # assert creation
    assert type(VO.schematron_validation_scenario.id) == int


@pytest.mark.run(order=6)
def test_add_python_validation_scenario(VO):
    '''
    Add python code snippet validation
    '''

    # get python validation from test data
    with open('tests/data/python_validation.py', 'r') as f:
        py_payload = f.read()

    # init new validation scenario
    python_validation_scenario = ValidationScenario(
        name='temp_vs_%s' % str(uuid.uuid4()),
        payload=py_payload,
        validation_type='python',
        default_run=False
    )
    python_validation_scenario.save()

    # pin to VO
    VO.python_validation_scenario = python_validation_scenario

    # assert creation
    assert type(VO.python_validation_scenario.id) == int


@pytest.mark.run(order=7)
def test_schematron_validation(VO):
    # get target records
    VO.harvest_record = VO.static_harvest_cjob.job.get_records().first()
    VO.transform_record = VO.static_transform_cjob.job.get_records().first()

    # validate harvest record with schematron
    '''
    expecting failure count of 2
    '''
    vs_results = VO.schematron_validation_scenario.validate_record(VO.harvest_record)
    assert vs_results['parsed']['fail_count'] == 2

    # validate transform record with schematron
    '''
    expecting failure count of 1
    '''
    vs_results = VO.schematron_validation_scenario.validate_record(VO.transform_record)
    assert vs_results['parsed']['fail_count'] == 1


@pytest.mark.run(order=8)
def test_python_validation(VO):
    # validate harvest record with python
    '''
    expecting failure count of 1
    '''
    vs_results = VO.python_validation_scenario.validate_record(VO.harvest_record)
    print(vs_results)
    assert vs_results['parsed']['fail_count'] == 1

    # validate transform record with python
    '''
    expecting failure count of 1
    '''
    vs_results = VO.python_validation_scenario.validate_record(VO.transform_record)
    print(vs_results)
    assert vs_results['parsed']['fail_count'] == 1


# #############################################################################
# # Test Duplicate/Merge Job
# #############################################################################

@pytest.mark.run(order=9)
def test_merge_duplicate(VO):
    '''
    Duplicate Transform job, applying newly created validation scenarios
    '''

    # emulate request.POST
    request_dict = {
        'dbdd': '',
        'field_mapper': 'default',
        'filter_dupe_record_ids': 'true',
        'fm_config_json': '{"add_literals":{},"capture_attribute_values":[],"concat_values_on_all_fields":false,"concat_values_on_fields":{},"copy_to":{},"copy_to_regex":{},"copy_value_to_regex":{},"error_on_delims_collision":false,"exclude_attributes":[],"exclude_elements":[],"include_all_attributes":false,"include_attributes":[],"include_sibling_id":false,"multivalue_delim":"|","node_delim":"_","ns_prefix_delim":"|","remove_copied_key":true,"remove_copied_value":false,"remove_ns_prefix":true,"repeating_element_suffix_count":false,"self_describing":false,"skip_attribute_ns_declarations":true,"skip_repeating_values":true,"skip_root":false,"split_values_on_all_fields":false,"split_values_on_fields":{}}',
        'input_es_query_valve': '',
        'input_numerical_valve': '',
        'input_validity_valve': 'all',
        'job_name': '',
        'job_note': '',
        'rits': ''
    }
    query_dict = QueryDict('', mutable=True)
    query_dict.update(request_dict)

    # set input jobs with QueryDict.setlist
    query_dict.setlist('input_job_id', [
        VO.static_harvest_cjob.job.id,
        VO.static_transform_cjob.job.id
    ])
    # set validation scenarios with QueryDict.setlist
    query_dict.setlist('validation_scenario', [
        VO.schematron_validation_scenario.id,
        VO.python_validation_scenario.id
    ])

    # init job
    cjob = CombineJob.init_combine_job(
        user=VO.user,
        record_group=VO.rg,
        job_type_class=MergeJob,
        job_params=query_dict)

    # start job and update status
    job_status = cjob.start_job()

    # if job_status is absent, report job status as failed
    if job_status == False:
        cjob.job.status = 'failed'
        cjob.job.save()

    # poll until complete
    for x in range(0, 480):

        # pause
        time.sleep(1)

        # refresh session
        cjob.job.update_status()

        # check status
        if cjob.job.status != 'available':
            continue
        else:
            break

    # save static harvest job to VO
    VO.merge_cjob = cjob

    # assert job is done and available via livy
    assert VO.merge_cjob.job.status == 'available'

    # assert record count is 250
    assert VO.merge_cjob.job.record_count == 250

    # assert validation scenarios applied
    job_validation_scenarios = VO.merge_cjob.job.jobvalidation_set.all()
    assert job_validation_scenarios.count() == 2

    # loop through validation scenarios and confirm that both show 250 failures
    for jv in job_validation_scenarios:
        assert jv.get_record_validation_failures().count() == 232

    # assert no indexing failures
    assert len(VO.merge_cjob.get_indexing_failures()) == 0


#############################################################################
# Tests Teardown
#############################################################################

@pytest.mark.last
def test_teardown(keep_records, VO):
    '''
    Test teardown
    '''

    # assert delete of org and children
    if not keep_records:
        assert VO.org.delete()[0] > 0
    else:
        assert True

    assert VO.schematron_validation_scenario.delete()[0] > 0
    assert VO.python_validation_scenario.delete()[0] > 0
