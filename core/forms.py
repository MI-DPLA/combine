from django.forms import ModelForm

# import models from core for forms
from core.models import Organization, RecordGroup, Transformation, ValidationScenario

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


class TransformationForm(ModelForm):

    class Meta:
        model = Transformation
        fields = ['name', 'payload', 'transformation_type', 'filepath', 'use_as_include']
