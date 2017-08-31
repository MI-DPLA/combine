from django.conf.urls import url

from . import views

urlpatterns = [
    url(r'^tests/test_create_session$', views.test_create_session, name='test_create_session'),
    url(r'^', views.index, name='test'),
]