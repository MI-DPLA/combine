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
	url(r'^job_harvest/(?P<record_group_id>[0-9]+)/jobs/harvest/new$', views.job_harvest, name='job_harvest'),

	# general views
	url(r'^login$', auth_views.login, name='login'),
    url(r'^logout$', auth_views.logout, name='logout'),
    url(r'^', views.index, name='combine_home'),
]