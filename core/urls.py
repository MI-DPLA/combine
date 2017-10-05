from django.conf.urls import url
from django.contrib import admin
from django.contrib.auth import views as auth_views

from . import views

urlpatterns = [

	# User Livy sessions
	url(r'^livy_sessions$', views.livy_sessions, name='livy_sessions'),
	url(r'^livy_sessions/stop$', views.livy_session_start, name='livy_session_start'),
	url(r'^livy_sessions/(?P<session_id>[0-9]+)/stop$', views.livy_session_stop, name='livy_session_stop'),

	# Organizations
	url(r'^organizations$', views.organizations, name='organizations'),
	url(r'^organizations/(?P<org_id>[0-9]+)$', views.organization, name='organization'),

	# Record Groups
	url(r'^record_groups/(?P<record_group_id>[0-9]+)$', views.record_group, name='record_group'),

	# Record Group Jobs
	url(r'^record_groups/(?P<record_group_id>[0-9]+)/jobs/(?P<job_id>[0-9]+)/delete$', views.job_delete, name='job_delete'),
	url(r'^record_groups/(?P<record_group_id>[0-9]+)/jobs/(?P<job_id>[0-9]+)/details$', views.job_details, name='job_details'),
	url(r'^record_groups/(?P<record_group_id>[0-9]+)/jobs/(?P<job_id>[0-9]+)/errors$', views.job_errors, name='job_errors'),
	url(r'^record_groups/(?P<record_group_id>[0-9]+)/jobs/harvest/new$', views.job_harvest, name='job_harvest'),
	url(r'^record_groups/(?P<record_group_id>[0-9]+)/jobs/transform/new$', views.job_transform, name='job_transform'),
	url(r'^record_groups/(?P<record_group_id>[0-9]+)/jobs/publish/new$', views.job_publish, name='job_publish'),

	# Record Group Job Analysis
	url(r'^record_groups/(?P<record_group_id>[0-9]+)/jobs/(?P<job_id>[0-9]+)/analysis/fields$', views.field_analysis, name='field_analysis'),
	url(r'^record_groups/(?P<record_group_id>[0-9]+)/jobs/(?P<job_id>[0-9]+)/analysis/indexing_failures$', views.job_indexing_failures, name='job_indexing_failures'),

	# Jobs General
	url(r'^jobs/input_select$', views.job_input_select, name='job_input_select'),

	# general views
	url(r'^login$', auth_views.login, name='login'),
    url(r'^logout$', auth_views.logout, name='logout'),
    url(r'^', views.index, name='combine_home'),
]