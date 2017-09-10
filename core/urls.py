from django.conf.urls import url
from django.contrib import admin
from django.contrib.auth import views as auth_views

from . import views

urlpatterns = [

	# # User Livy sessions
	url(r'^livy_sessions$', views.livy_sessions, name='livy_sessions'),
	url(r'^livy_sessions/(?P<session_id>[0-9]+)/delete$', views.livy_session_delete, name='livy_session_delete'),

	# # Record Groups
	# url(r'^record_groups$', views.record_groups, name='record_groups'),
	# url(r'^record_groups/([0-9]*)$', views.record_group, name='record_groups'),

	# general views
	url(r'^login$', auth_views.login, name='login'),
    url(r'^logout$', auth_views.logout, name='logout'),
    url(r'^', views.index, name='combine_home'),
]