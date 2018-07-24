from django.forms import ModelForm

# import models from core for forms
from core.models import Organization, RecordGroup, OAIEndpoint, Transformation



class OrganizationForm(ModelForm):

	class Meta:
		model = Organization
		fields = ['name', 'description']



class RecordGroupForm(ModelForm):

	class Meta:
		model = RecordGroup
		fields = ['organization', 'name', 'description']

