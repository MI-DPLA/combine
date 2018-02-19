from django.core.management.base import BaseCommand, CommandError

# import core
from core.models import *

class Command(BaseCommand):

	help = 'Bootstrap Combine with some demo Transformation and Validation scenarios'

	# def add_arguments(self, parser):
	#     parser.add_argument('poll_id', nargs='+', type=int)

	def handle(self, *args, **options):

		## prepare demo MODS files		
		# parse file
		xml_tree = etree.parse('tests/data/mods_250.xml')
		xml_root = xml_tree.getroot()		
		# get namespaces
		nsmap = {}
		for ns in xml_root.xpath('//namespace::*'):
			if ns[0]:
				nsmap[ns[0]] = ns[1]
		# find mods records
		mods_roots = xml_root.xpath('//mods:mods', namespaces=nsmap)
		# create temp dir
		payload_dir = '/tmp/combine/qs/mods'
		os.makedirs(payload_dir)
		# write MODS to temp dir
		for mods in mods_roots:
			with open(os.path.join(payload_dir, '%s.xml' % uuid.uuid4().hex), 'w') as f:
				f.write(etree.tostring(mods).decode('utf-8'))


		## create demo XSLT transformation
		with open('tests/data/mods_transform.xsl','r') as f:
			xsl_string = f.read()			
		trans = Transformation(
			name='MODS to Service Hub profile',
			payload=xsl_string,
			transformation_type='xslt'			
		)	
		trans.save()


		## create demo validation scenarios
		# schematron validation
		with open('tests/data/qs_schematron_validation.sch','r') as f:
			sch_payload = f.read()		
		schematron_validation_scenario = ValidationScenario(
			name='DPLA minimum',
			payload=sch_payload,
			validation_type='sch',
			default_run=True
		)
		schematron_validation_scenario.save()

		# python validation
		with open('tests/data/qs_python_validation.py','r') as f:
			py_payload = f.read()		
		python_validation_scenario = ValidationScenario(
			name='Date checker',
			payload=py_payload,
			validation_type='python',
			default_run=True
		)
		python_validation_scenario.save()


		# return
		self.stdout.write(self.style.SUCCESS('Demo Transformation and Validation Scenarios created.'))