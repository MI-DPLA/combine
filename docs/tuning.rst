*****************************
Tuning and Configuring Server
*****************************

Combine is designed to handle sets of metadata small to large, 400 to 4,000,000 Records.  Some of the major associated server components include:

  - `MySQL <#mysql>`__
  
    - store Records and their associated, full XML documents
    - store Transformations, Validations, and most other enduring, user defined data
    - store transactions from Validations, OAI requests, etc.

  - `ElasticSearch <#elasticsearch>`__

    - used for indexing mapped fields from Records
    - main engine of field-level analysis

  - `Apache Spark <#apache-spark>`__

    - the workhorse for running Jobs, including Harvests, Transformations, Validations, etc.

  - `Apache Livy <#apache-livy>`__

    - used to send and queue Jobs to Spark

  - `Django <#django>`__

    - the GUI

  - `Django Background Tasks <#django-background-tasks>`__

    - for long running tasks that may that would otherwise prevent the GUI from being responsive
    - includes deleting, re-indexing, exporting Jobs, etc.

Given the relative complexity of this stack, and the innerconnected nature of the components, Combine is designed to be deployed via an Ansible playbook, `which you can read more about here <installing.html>`_.  The default build requires **8g** of RAM, with the more CPU cores the better.

This part of the documentation aims to explain, and indicate how to modify of configure, some of the these critical components.


MySQL
=====


ElasticSearch
=============


Apache Spark
============


Apache Livy
===========


Django
======


Django Background Tasks
=======================


