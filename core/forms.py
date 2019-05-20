from django.forms import ModelForm

# import models from core for forms
from core.models import Organization, RecordGroup, ValidationScenario


class OrganizationForm(ModelForm):

    class Meta:
        model = Organization
        fields = ['name', 'description']


class RecordGroupForm(ModelForm):

    class Meta:
        model = RecordGroup
        fields = ['organization', 'name', 'description']


class ValidationScenarioForm(ModelForm):

    class Meta:
        model = ValidationScenario
        fields = ['name', 'payload', 'validation_type', 'filepath', 'default_run']
