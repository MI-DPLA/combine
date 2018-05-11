*************
Configuration
*************

Combine relies heavily on front-loading configuration, so that the process of running Jobs is largely selecting pre-existing "scenarios" that have already been tested and configured.

This section will outline configuration options and associated configuration pages.

**Note:** Currently, Combine leverages Django's built-in admin interface for actually editing and creating model instances -- transformations, validations, and other scenarios -- below.  This will likely evolve into more tailored CRUDs for each, but for the time being, there is a link to the Django admin panel on the Configuration screen.

**Note:** What settings are not configurable via the GUI in Combine, are configurable in the file ``combine/localsettings.py``.


OAI Server Endpoints
====================


Transformation Scenario
=======================

Transformation Scenarios are configured and saved payloads that are used for transforming the XML of Records during Transformation Jobs.  Currently, there are two types of transformation supported: XSLT and Python code snippets.  These are described in more detail below.

XSLT
----

XSLT transformations are performed by Spark, via `pyjxslt <https://github.com/cts2/pyjxslt>`_.  However, there are perform XSLT transformations in a more Spark friendly approach.


Python Code Snippet
-------------------

Python transformations are performed via a Spark user defined function (UDF).  


Validation Scenario
===================


Record Identifier Transformation Scenario
=========================================


Combine OAI-PMH Server
======================


DPLA Bulk Data Downloads
========================