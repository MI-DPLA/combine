# convenience methods for Django's shell_plus

from core.models import *



# get Record instance
def r(id):
	return Record.objects.get(pk=int(id))


# get Job instance
def j(id):
	return Job.objects.get(pk=int(id))


# get CombineJob instance
def cj(id):
	return CombineJob.get_combine_job(int(id))


# get RecordGroup instance
def rg(id):
	return RecordGroup.objects.get(pk=int(id))


# get Organization instance
def o(id):
	return Organization.objects.get(pk=int(id))

