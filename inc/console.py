# convenience methods for Django's shell_plus

import os
from core.models import *



# get Record instance
def get_r(id):
	return Record.objects.get(id=id)


# get Job instance
def get_j(id):
	return Job.objects.get(pk=int(id))


# get CombineJob instance
def get_cj(id):
	return CombineJob.get_combine_job(int(id))


# get RecordGroup instance
def get_rg(id):
	return RecordGroup.objects.get(pk=int(id))


# get Organization instance
def get_o(id):
	return Organization.objects.get(pk=int(id))


# tail livy
def tail_livy():
	os.system('tail -f /var/log/livy/livy.stderr')


# tail django
def tail_bg():
	os.system('tail -f /var/log/combine_background_tasks.stdout')