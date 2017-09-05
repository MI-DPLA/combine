from django.conf.urls import url

from . import views

urlpatterns = [

	# Livy sessions
	url(r'^livy_sessions$', views.livy_sessions, name='livy_sessions'),
	url(r'^livy_sessions/create$', views.livy_session_create, name='livy_session_create'),
	url(r'^livy_sessions/([0-9]*)$', views.livy_session_status, name='livy_session_status'),

	# Jobs
	url(r'^record_groups$', views.record_groups, name='record_groups'),
	url(r'^record_groups/([0-9]*)$', views.record_group, name='record_groups'),

	# default index
    url(r'^', views.index, name='test'),
]