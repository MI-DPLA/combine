from django.db import models

class OAIEndpoint(models.Model):

    '''
    Model to manage user added OAI endpoints
    For more information, see: https://github.com/dpla/ingestion3
    '''

    name = models.CharField(max_length=255, null=True)
    endpoint = models.CharField(max_length=255)
    verb = models.CharField(max_length=128, null=True, default='ListRecords', blank=True) # HiddenInput
    metadataPrefix = models.CharField(max_length=128, null=True, blank=True)
    scope_type = models.CharField(
        max_length=128,
        null=True,
        blank=True,
        choices=[
            ('harvestAllSets', 'Harvest records from all sets'),
            ('setList', 'Comma-separated lists of sets to include in the harvest'),
            ('blackList', 'Comma-separated lists of sets to exclude from the harvest')
        ],
        default='harvestAllSets')
    scope_value = models.CharField(max_length=1024, null=True, blank=True, default='true')


    def __str__(self):
        return 'OAI endpoint: %s' % self.name


    def as_dict(self):

        '''
        Return model attributes as dictionary

        Args:
            None

        Returns:
            (dict): attributes for model instance
        '''

        dct = self.__dict__
        dct.pop('_state', None)
        return dct

