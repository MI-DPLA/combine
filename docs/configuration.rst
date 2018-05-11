*************
Configuration
*************



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