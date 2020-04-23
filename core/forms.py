from django.forms import ModelForm, ValidationError

# import models from core for forms
from core.models import Organization, RecordGroup, RecordIdentifierTransformation,\
    Transformation, ValidationScenario, OAIEndpoint, FieldMapper, get_rits_choices,\
    get_field_mapper_choices, get_validation_scenario_choices, get_transformation_type_choices


class OrganizationForm(ModelForm):
    class Meta:
        model = Organization
        fields = ['name', 'description']


class RecordGroupForm(ModelForm):
    class Meta:
        model = RecordGroup
        fields = ['organization', 'name', 'description']


class ValidationScenarioForm(ModelForm):

    def __init__(self, *args, **kwargs):
        super(ValidationScenarioForm, self).__init__(*args, **kwargs)
        self.fields['validation_type'].choices = get_validation_scenario_choices()

    def clean_validation_type(self):
        cleaned_data = super().clean()
        value = cleaned_data['validation_type']
        choices = [choice for (choice, label) in self.fields['validation_type'].choices]
        if value in choices:
            return value
        raise ValidationError(code='invalid')

    class Meta:
        model = ValidationScenario
        fields = ['name', 'payload', 'validation_type', 'default_run']
        labels = {
            'payload': 'Validation Code'
        }
        help_texts = {
            'validation_type': 'If you want to use python code and it is not available, ask your server administrator to set ENABLE_PYTHON=true in the server settings file.'
        }


class TransformationForm(ModelForm):

    def __init__(self, *args, **kwargs):
        super(TransformationForm, self).__init__(*args, **kwargs)
        self.fields['transformation_type'].choices = get_transformation_type_choices()

    def clean_transformation_type(self):
        cleaned_data = super().clean()
        value = cleaned_data['transformation_type']
        choices = [choice for (choice, label) in self.fields['transformation_type'].choices]
        if value in choices:
            return value
        return ValidationError(code='invalid')

    class Meta:
        model = Transformation
        fields = ['name', 'payload', 'transformation_type', 'use_as_include']
        labels = {
            'payload': 'Transformation Code'
        }
        help_texts = {
            'transformation_type': 'If you want to use python code and it is not available, ask your server administrator to set ENABLE_PYTHON=true in the server settings file.'
        }


class RITSForm(ModelForm):

    def __init__(self, *args, **kwargs):
        super(RITSForm, self).__init__(*args, **kwargs)
        self.fields['transformation_type'].choices = get_rits_choices()

    def clean_transformation_type(self):
        cleaned_data = super().clean()
        value = cleaned_data['transformation_type']
        choices = [choice for (choice, label) in self.fields['transformation_type'].choices]
        if value in choices:
            return value
        raise ValidationError(code='invalid')

    class Meta:
        model = RecordIdentifierTransformation
        fields = ['name', 'transformation_type', 'transformation_target', 'regex_match_payload',
                  'regex_replace_payload', 'python_payload', 'xpath_payload']
        labels = {
            'regex_match_payload': 'Regex to Match',
            'regex_replace_payload': 'Regex Replacement',
            'python_payload': 'Python Code',
            'xpath_payload': 'XPath Query'
        }
        help_texts = {
            'transformation_type': 'If you want to use python code and it is not available, ask your server administrator to set ENABLE_PYTHON=true in the server settings file.'
        }


class OAIEndpointForm(ModelForm):

    class Meta:
        model = OAIEndpoint
        fields = ['name', 'endpoint', 'metadataPrefix', 'scope_type', 'scope_value']


class FieldMapperForm(ModelForm):

    def __init__(self, *args, **kwargs):
        super(FieldMapperForm, self).__init__(*args, **kwargs)
        self.fields['field_mapper_type'].choices = get_field_mapper_choices()

    def clean_field_mapper_type(self):
        cleaned_data = super().clean()
        value = cleaned_data['field_mapper_type']
        choices = [choice for (choice, label) in self.fields['field_mapper_type'].choices]
        if value in choices:
            return value
        raise ValidationError(code='invalid')

    class Meta:
        model = FieldMapper
        fields = ['name', 'payload', 'config_json', 'field_mapper_type']
        labels = {
            'payload': 'Field Mapping Code'
        }
        help_texts = {
            'field_mapper_type': 'If you want to use python code and it is not available, ask your server administrator to set ENABLE_PYTHON=true in the server settings file.'
        }
