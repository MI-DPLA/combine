******************
Harvesting Records
******************

Harvesting is how Records are first introduced to Combine.  Like all Jobs, Harvest Jobs are run from the the Record Group overview page.

The following will outline specifics for running Harvest Jobs, with more `general information about running Jobs here <workflow.html#running-jobs>`_.


OAI-PMH Harvesting
==================

OAI-PMH harvesting in Combine utilizes the Apache Spark OAI harvester from `DPLA's Ingestion 3 engine <https://github.com/dpla/ingestion3>`_.

Before running an OAI harvest, you must first `configure an OAI Endpoint <configuration.html#oai-server-endpoints>`_ in Combine that will be used for harvesting from.  This only needs to be done once, and can then be reused for future harvests.

From the Record Group page, click the "Harvest OAI-PMH" button at the bottom.

Like all Jobs, you may optionally give the Job a name or add notes.  

Below that, indicated by a green alert, are the required parameters for an OAI Job.  First, is to select your pre-configured OAI endpoint.  In the screenshot below, an example OAI endpoint has been selected:

.. figure:: img/oai_harvest_required.png
   :alt: Selecting OAI endpoint and configuring parameters
   :target: _images/oai_harvest_required.png

   Selecting OAI endpoint and configuring parameters

Default values for harvesting are automatically populated from your configured endpoint, but can be overridden at this time, for this harvest only.  Changes are not saved for future harvests.

Once configurations are set, click "Run Job" at the bottom to harvest.


Identifiers for OAI-PMH harvesting
----------------------------------

As an Harvest type Job, OAI harvests are responsible for creating a Record Identifier (``record_id``) for each Record.  The ``record_id`` is pulled from the ``record/header/identifier`` field for each Record harvested.

As you continue on your metadata harvesting, transforming, and publishing journey, and you are thinking about how identifiers came to be, or might be changed, this is a good place to start from to see what the originating identifier was.


Static File Harvest
===================

While partially functional, this is an area still under development.
