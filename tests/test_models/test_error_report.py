from django.test import TestCase

from core.models import ErrorReport


class ErrorReportTestCase(TestCase):

    def test_creation(self):
        try:
            raise Exception('hi!')
        except Exception as err:
            report = ErrorReport.create(err)
            report.save()
        report = ErrorReport.objects.get(message='hi!')
        self.assertEqual("<class 'Exception'> at {} with args ('hi!',): hi!".format(report.timestamp), format(report))
        self.assertEqual(report.type, "<class 'Exception'>")
        self.assertEqual(report.args, "('hi!',)")
        self.assertEqual(report.message, 'hi!')
