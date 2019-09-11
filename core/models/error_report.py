from django.db import models


class ErrorReport(models.Model):
    type = models.TextField()
    args = models.TextField()
    message = models.TextField()
    timestamp = models.DateTimeField(null=True, auto_now_add=True)

    @classmethod
    def create(cls, error):
        err_type = type(error)
        err_args = format(error.args)
        message = format(error)
        error_report = cls(type=err_type, args=err_args, message=message)
        return error_report

    def __str__(self):
        string = '{} at {} with args {}: {}'.format(self.type, self.timestamp, self.args, self.message)
        return string

