*************
Configuration
*************

Combine relies heavily on front-loading configuration, so that the process of running Jobs is largely selecting pre-existing "scenarios" that have already been tested and configured.

This section will outline configuration options and associated configuration pages.

**Note:** Currently, Combine leverages Django's built-in admin interface for actually editing and creating model instances -- transformations, validations, and other scenarios -- below.  This will likely evolve into more tailored CRUDs for each, but for the time being, there is a link to the Django admin panel on the Configuration screen.

**Note:** What settings are not configurable via the GUI in Combine, are configurable in the file ``combine/localsettings.py``.


OAI Server Endpoints
====================

Configuring OAI endpoints is the first step for harvesting from OAI endpoints.

To configure a new OAI endpoint, navigate to the Django admin screen, under the section "Core" select ``Oai endpoints``.

This model is unique among other Combine models in that these values are sent directly to the DPLA Ingestion 3 OAI harvesting codebase.  More `information on these fields can be found here <https://digitalpubliclibraryofamerica.atlassian.net/wiki/spaces/TECH/pages/87658172/Spark+OAI+Harvester>`_.

The following fields are all required:

  - ``Name`` - Human readable name for OAI endpoint, used in dropdown menu when running harvest
  - ``Endpoint`` - URL for OAI server endpoint.  This should include the full URL up until, but not including, GET paremeters that begin with a question mark ``?``.
  - ``Verb`` - This pertains to the OAI-PMH verb that will be used for harvesting.  Almost always, ``ListRecords`` is the required verb here.  So much, this will default to ``ListRecords`` if left blank.
  - ``MetadataPrefix`` - Another OAI-PMH term, the metadata prefix that will be used during harvesting.
  - ``Scope type`` - Not an OAI term, this refers to what kind of harvesting should be performed.  Possible values include:

    - ``setList`` - This will harvest the comma seperated sets provided for ``Scope value``.
    - ``harvestAllSets`` - The most performant option, this will harvest all sets from the OAI endpoint.  If this is set, the ``Scope value`` field must be set to ``true``.
    - ``blacklist`` - Comma seperated list of OAI sets to **exclude** from harvesting.

  - ``Scope value`` - String to be used in conjunction with ``Scope type`` outline above.

    - If ``setList`` is used, provide a comma seperated string of OAI sets to harvest
    - If ``harvestAllSets``, provide just the single string ``true``.

 Once the OAI endpoint has been added in the Django admin, from the configurations page you are presented with a table showing all configured OAI endpoints.  The last column includes a link to issue a command to view all OAI sets from that endopint.


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