**********
Data Model
**********

Overview
========

Combine's Data Model can be roughly broken down into the following hierachy:

.. code-block:: text

    Organization --> RecordGroup --> Job --> Record


Organization
============

Organizations are the highest level of organization in Combine.  It is loosely based on a "Data Provider" in REPOX, also the highest level of hierarchy.  Organizations contain Record Groups.

Combine was designed to be flexible as to where it exists in a complicated ecosystem of metadata providers and harvesters.  Organizations are meant to be helpful if a single instance of Combine is used to manage metadata from a variety of institutions or organizations.  

Other than a level of hierarchy, Organizations have virtually no other affordances.

We might imagine a single instance of Combine, with two Organizations:

  * Foo University
  * Bar Historical Society

Foo University would contain all Record Groups that pertain to Foo University.  One can imagine that Foo University has a Fedora Repository, Omeka, and might even aggregate records for a small historical society or library as well, each of which would fall under the Organization.

Record Group
============

Record Groups fall under Organizations, and are loosely based on a "Data Set" in REPOX.  Record Groups contain Jobs.

Record Groups are envisioned as the right level of hierarchy for a group of records that are intellectually grouped, come from the same system, or might be managed with the same transformations and validations.

From our Foo University example above, the Fedora Repository, Omeka installs, and the records from a small historical society -- all managed and mediated by Foo University -- might make nice, individual, distinct Record Groups.


Job
===

Jobs are contained with a Record Group, and contain Records.

This is where the model forks from REPOX, in that a Record Group can, and likely will, contain multiple Jobs.  It is reasonable to also think of a Job as a *stage* of records.

Jobs represent Records as they move through the various stages of harvesting, sub-dividing, and transforming.  In a typical Record Group, you may see Jobs that represent a harvest of records, another for transforming the records, perhaps yet another transformation, and finally a Job that is "published".  In this way, Jobs also provide an approach to versioning Records.

Imagine the record ``baz`` that comes with the harvest from ``Job1``.  ``Job2`` is then a transformation style Job that uses ``Job1`` as input.  ``Job3`` might be another transformation, and ``Job4`` a final publishing of the records.   In each Job, the record ``baz`` exists, at those various stages of harvesting and transformation.  Combine errs on the side of duplicating data in the name of lineage and transparency as to how and why a Record "downstream" looks they way it does.

As may be clear by this point, Jobs are used as **input** for other Jobs.  ``Job1`` serves as the input Records for ``Job2``, ``Job2`` for ``Job3``, etc.

There are four primary types of Jobs:

  * Harvests
  * Transformations
  * Merge / Duplicate
  * Analysis

It is up to the user how to manage Jobs in Combine, but one strategy might be to leave previous harvests, transforms, and merges of Jobs within a RecordGroup for historical purposes.  From an organizational standpoint, this may look like:

.. code-block:: text

    Harvest, 1/1/2017 --> Transform to Service Hub Profile
    Harvest, 4/1/2017 --> Transform to Service Hub Profile
    Harvest, 8/1/2017 --> Transform to Service Hub Profile
    Harvest, 1/1/2018 --> Transform to Service Hub Profile (Published)

In this scenario, this Record Group would have **9** total Jobs, but only only the last "set" of Jobs would represent the currently published Records.


Harvest Jobs
------------

`Harvest Jobs <harvesting.html>`_ are how Records are initially created in Combine.  This might be through OAI-PMH harvesting, or loading from static files.

As the creator of Records, Harvest Jobs do *not* have input Jobs.


Transformation Jobs
-------------------

`Transformation Jobs <transforming.html>`_, unsurprisingly, transform the Records in some way!  Currently, XSLT and python code snippets are supported.

Transformation Jobs allow a **single** input Job, and are limited to Jobs within the same RecordGroup.


Merge / Duplicate Jobs
----------------------

`Merge / Duplicate Jobs <merging.html>`_ are true to their namesake: merging Records across multiple Jobs, or duplicating all Records from a single Job, into a new, single Job.


Analysis Jobs
-------------

`Analysis Jobs <analysis.html#analysis-jobs>`_ are Merge / Duplicate Jobs in nature, but exist outside of the normal 

.. code-block:: text

  Organization --> Record Group

hierarchy.  Analysis Jobs are meant as ephemeral, disposable, one-off Jobs for analysis purposes only.



Record
======

The most granular level of hierarchy in Combine is a single Record.  Records are part of Jobs.

Record's actual XML content, and other attributes, are recorded in MySQL, while their indexed fields are stored in ElasticSearch.

Identifiers
-----------

Additionally, Record's have three important identifiers:

  - **Database ID**

    - ``id`` (integer)
    - This is the Primary Key (PK) in MySQL, unique for all Records

  - **Combine ID**

    - ``combine_id`` (string)
    - this is randomly generated for a Record on creation, and is what allows for linking of Records across Jobs, and is unique for all Records

  - **Record ID**

    - ``record_id`` (string)
    - not necessarily unique for all Records, this is identifier is used for publishing
    - in the case of OAI-PMH harvesting, this is likely populated from the OAI identifier that the Record came in with
    - this can be modified with a Record Identifier Transform when run with a Job

Why the need to transform identifiers?
--------------------------------------

Imagine the following scenario:

Originally, there were multiple REPOX instances in play for a series of harvests and transforms.  With each OAI "hop", the identifier for a Record is prefixed with information about that particular REPOX instance.

Now, with a single instance of Combine replacing multiple REPOX instances and OAI "hops", records that are harvested are missing pieces of the identifier that were previously created along the way.  

Or, insert a myriad of other reasons why an identifier may drift or change.

Combine allows for the creation of `Record Identifier Transformation Scenarios <configuration.html#record-identifier-transformation-scenario>`_ that allow for the modification of the ``record_id``.  This allows for the emulation of previous configurations or ecosystems, or optionally creating Record Identifiers -- what is used for publishing -- based on information from the Record's XML record with XPath or python code snippets.



















