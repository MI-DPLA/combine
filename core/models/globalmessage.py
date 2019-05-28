# -*- coding: utf-8 -*-
from __future__ import unicode_literals

# generic imports
import logging
import time
import uuid

# django imports
from django.contrib.sessions.backends.db import SessionStore
from django.contrib.sessions.models import Session

# import mongo dependencies
from core.mongo import *

# Get an instance of a logger
logger = logging.getLogger(__name__)

# Set logging levels for 3rd party modules
logging.getLogger("requests").setLevel(logging.WARNING)



class GlobalMessageClient(object):

	'''
	Client to handle CRUD for global messages

	Message dictionary structure {
		html (str): string of HTML content to display
		class (str): class of Bootstrap styling, [success, warning, danger, info]
		id (uuid4): unique id for removing
	}
	'''

	def __init__(self, session=None):

		# use session if provided
		if type(session) == SessionStore:
			self.session = session

		# if session_key provided, use
		elif type(session) == str:
			self.session = SessionStore(session_key=session)

		# else, set to session
		else:
			self.session = session


	def load_most_recent_session(self):

		'''
		Method to retrieve most recent session
		'''

		s = Session.objects.order_by('expire_date').last()
		self.__init__(s.session_key)
		return self


	def add_gm(self, gm_dict, forced_delay=2):

		'''
		Method to add message

		Args:
			gm_dict (dict): Dictionary of message contents
			forced_delay (int): Forced delay as convenient squeeze point to stave race condititions
		'''

		# check for 'gms' key in session, create if not present
		if 'gms' not in self.session:
			self.session['gms'] = []

		# create unique id and add
		if 'id' not in gm_dict:
			gm_dict['id'] = uuid.uuid4().hex

		# append gm dictionary
		self.session['gms'].append(gm_dict)

		# save
		self.session.save()

		# enforce forced delay
		if forced_delay:
			time.sleep(forced_delay)


	def delete_gm(self, gm_id):

		'''
		Method to remove message
		'''

		if 'gms' not in self.session:
			logger.debug('no global messages found, returning False')
			return False

		else:

			logger.debug('removing gm: %s' % gm_id)

			# grab total gms
			pre_len = len(self.session['gms'])

			# loop through messages to find and remove
			self.session['gms'][:] = [gm for gm in self.session['gms'] if gm.get('id') != gm_id]
			self.session.save()

			# return results
			return pre_len - len(self.session['gms'])


	def clear(self):

		'''
		Method to clear all messages
		'''

		self.session.clear()
		self.session.save()