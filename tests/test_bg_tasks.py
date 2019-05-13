
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
# Background Tasks
#############################################################################

# test re-indexing
@pytest.mark.run(order=10)
def test_static_harvest_reindex(VO):

	# refresh job
	VO.static_harvest_cjob = CombineJob.get_combine_job(VO.static_harvest_cjob.job.id)

	# fm config json, adding literal foo:bar
	fm_config_json = '{"concat_values_on_all_fields": false, "capture_attribute_values": [], "remove_ns_prefix": true, "skip_attribute_ns_declarations": true, "remove_copied_key": true, "node_delim": "_", "copy_to": {}, "copy_value_to_regex": {}, "copy_to_regex": {}, "split_values_on_all_fields": false, "add_literals": {"foo":"bar"}, "exclude_attributes": [], "ns_prefix_delim": "|", "self_describing": false, "split_values_on_fields": {}, "include_attributes": [], "include_sibling_id": false, "multivalue_delim": "|", "skip_repeating_values": true, "repeating_element_suffix_count": false, "exclude_elements": [], "concat_values_on_fields": {}, "remove_copied_value": false, "error_on_delims_collision": false, "include_all_attributes": false, "skip_root": false}'

	# reindex static harvest
	bg_task = VO.static_harvest_cjob.reindex_bg_task(fm_config_json=fm_config_json)

	# poll until complete
	for x in range(0, 480):

		# pause
		time.sleep(1)
		logger.debug('polling for reindexing %s seconds...' % (x))

		# refresh session
		bg_task.update()

		# check status
		if bg_task.celery_status not in ['SUCCESS','FAILURE']:
			continue
		else:
			break

	# assert 250 records have foo:bar, indicating successful reindexing
	results = VO.static_harvest_cjob.field_analysis('foo')
	assert results['metrics']['doc_instances'] == 250









