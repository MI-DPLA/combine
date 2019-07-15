from django.test import TestCase
from core.models import DPLABulkDataDownload


class DPLABulkDataDownloadTestCase(TestCase):
    def test_str(self):
        download = DPLABulkDataDownload(s3_key='s3://something/another_thing/file.txt')
        self.assertEqual('s3://something/another_thing/file.txt, DPLABulkDataDownload: #{}'.format(download.id),
                         format(download))

    def test_name(self):
        download = DPLABulkDataDownload(s3_key='s3://testing/testing/1/2/3')
        self.assertEqual(download.name, 's3://testing/testing/1/2/3')