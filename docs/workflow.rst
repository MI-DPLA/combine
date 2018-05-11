*****************************
Workflows and Viewing Results
*****************************

This section will describe different parts of workflows for running Jobs, and viewing detailed results of Jobs and their Records.


Record Versioning
=================

In an effort to preserve various stages of a Record through harvest, possible multiple transformation, merges, and eventually publishing, Combine takes the approach of copying the Record each time.

As outlined in the `Data Model <data_model.html>`_, Records are represented in both MySQL and ElasticSearch.  Each time a Job is run, and a Record is duplicated, it gets a new row in MySQL, with the full XML of the Record duplicated.  Records are associated with each other across Jobs by their `Combine ID <data_model.html#identifiers>`_.

This approach has pros and cons:

  - Pros

    - simple data model, each version of a Record is stored separately
    - each Record stage can be indexed and analyzed separately
    - Jobs containing Records can be deleted without effecting up/downstream Records (they will vanish from the lineage)

  - Cons

    - duplication of data is potentially unnecessary if Record information has not changed


Running Jobs
============

**Note:** For all Jobs in Combine, confirm that an `active Livy session is up and running <spark_and_livy.html#livy-session>`_ before proceeding.

Optional Parameters
-------------------

When running any type of Job in Combine, you are presented with a section near the bottom for **Optional Parameters** for the job:

.. figure:: img/job_optional_parameters.png
   :alt: Optional Parameters for all Jobs
   :target: _images/job_optional_parameters.png

   Optional Parameters for all Jobs

These options are split across various tabs, and include:

  - `Validation Tests <#validation-tests>`_
  - `Index Mapping <#index-mapping>`_
  - `Transform Identifier <#transform-identifier>`_
  - `Record Input Validity <#record-input-validity-valve>`_
  - `DPLA Bulk Data Compare <#dpla-bulk-data-compare>`_

Validation Tests
~~~~~~~~~~~~~~~~

Index Mapping
~~~~~~~~~~~~~

Transform Identifier
~~~~~~~~~~~~~~~~~~~~

Record Input Validity Valve
~~~~~~~~~~~~~~~~~~~~~~~~~~~

DPLA Bulk Data Compare
~~~~~~~~~~~~~~~~~~~~~~


Viewing Job Details
===================


Viewing Record Details
======================















