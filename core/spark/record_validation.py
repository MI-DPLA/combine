# imports
import django
import hashlib
import json
from lxml import etree, isoschematron
import os
import shutil
import sys

# import Row from pyspark
from pyspark.sql import Row
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import udf

# init django settings file to retrieve settings
os.environ['DJANGO_SETTINGS_MODULE'] = 'combine.settings'
sys.path.append('/opt/combine')
django.setup()
from django.conf import settings

# import select models from Core
from core.models import Job, PythonRecordValidationBase, ValidationScenario


def run_record_validation_scenarios(
		spark=None,
		job=None,
		records_df=None,
		validation_scenarios=None
	):

	'''
	Function to run validation scenarios if requested during job launch.
	Results are written to RecordValidation table, one result, per record, per failed validation test.

	Validation tests may be of type:
		- 'sch': Schematron based validation, performed with lxml etree
		- 'python': custom python code snippets

	Args:
		spark (pyspark.sql.session.SparkSession): spark instance from static job methods
		job (core.models.Job): Job instance		
		records_df (pyspark.sql.DataFrame): records as pyspark DataFrame
		validation_scenarios (list): list of ValidationScenario job ids as integers

	Returns:
		None
			- writes validation fails to RecordValidation table
	'''

	def validate_udf(vs_id, vs_filepath, row):

		# parse schematron
		sct_doc = etree.parse(vs_filepath)
		validator = isoschematron.Schematron(sct_doc, store_report=True)

		# get document xml
		record_xml = etree.fromstring(row.document.encode('utf-8'))

		# validate
		is_valid = validator.validate(record_xml)

		# if not valid, prepare Row
		if not is_valid:

			# prepare fail_dict
			fail_dict = {
				'count':0,
				'failures':[]
			}

			# get failures
			report_root = validator.validation_report.getroot()
			fails = report_root.findall('svrl:failed-assert', namespaces=report_root.nsmap)

			# log count
			fail_dict['count'] = len(fails)

			# loop through fails and add to dictionary
			for fail in fails:
				fail_text_elem = fail.find('svrl:text', namespaces=fail.nsmap)
				fail_dict['failures'].append(fail_text_elem.text)
			
			return Row(
				record_id=int(row.id),
				validation_scenario_id=int(vs_id),
				valid=0,
				results_payload=json.dumps(fail_dict),
				fail_count=fail_dict['count']
			)

	# loop through validation scenarios
	'''
	TODO: Return to this, and look for ways to improve efficiency in DB I/O
	'''
	for vs_id in validation_scenarios:

		# get validation scenario
		vs = ValidationScenario.objects.get(pk=vs_id)

		# run udf map
		vs_id = vs.id
		vs_filepath = vs.filepath
		validation_fails_rdd = records_df.rdd.\
			map(lambda row: validate_udf(vs_id, vs_filepath, row))\
			.filter(lambda row: row is not None)

		# write to DB if validation failures
		if not validation_fails_rdd.isEmpty():
			validation_fails_rdd.toDF().write.jdbc(
				settings.COMBINE_DATABASE['jdbc_url'],
				'core_recordvalidation',
				properties=settings.COMBINE_DATABASE,
				mode='append')





