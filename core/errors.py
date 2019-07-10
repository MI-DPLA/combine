from django.shortcuts import render

from core.models import ErrorReport


class ErrorMiddleware():
    def __init__(self, get_response):
        self.get_response = get_response

    def __call__(self, request):
        response = self.get_response(request)
        return response

    @staticmethod
    def process_exception(request, exception):
        report = ErrorReport.create(exception)
        report.save()
        return render(request, 'core/error.html', {'report': report})
