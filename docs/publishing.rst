******************
Publishing Records
******************

The following will outline specifics for Publishing a Record Group, with more `general information about running Jobs here <workflow.html#running-jobs>`_.

How does Publishing work in Combine?
====================================

As a tool for aggregating metadata, Combine must also have the ability to serve aggregated Records again.  This is done by "publishing" in Combine, and this happens at the `Record Group level <data_model.html#record-group>`_.  Only **one** Job may be published per Record Group.

When a Record Group is published, a user may create a "Publish Set Identifier" (``publish_id``) that is used to aggregate and group published Records.  For example, in the built-in OAI-PMH server, this Publish ID becomes the OAI set ID.  Multiple Record Groups can publish under the same Publish Set ID, allowing for grouping of materials in publishing.

Why publishing at the Record Group level?  This reinforces the idea that a Record Group is an intellectual group of records, and though it may contain many Jobs of various stages, or previous versions, there should be only one, representative, published body of Records from this intellectual grouping of Records.  This is another instance where `Merge Jobs <merging.html>`_ can be useful, by allowing users to merge all Records / Jobs within a Record Group for publishing.

When a Record Group is published, a Publish Job is run that works very similar to a Merge / Duplicate Job in that it takes the input Job wholesale, copying the Records and the ElasticSearch documents.  What differentiates a Publish job from other Jobs is that each Record contained in that Job is considered "published" and will get returned through publishing routes.

Currently, the only protocol for publishing Records from Combine is a built-in OAI-PMH server.  But, there are plans for static publishing of Records (i.e. data dumps), and `ResourceSync <http://www.openarchives.org/rs/toc>`_ as well.


Publishing a Record Group
=========================

From the Record Group screen, click the blue "Publish" button near the top of the page:

.. figure:: img/publish_init.png
   :alt: Beginning the publish process from a Record Group
   :target: _images/publish_init.png

   Beginning the publish process from a Record Group

The next screen will be a familiar Job running screen, with a red section near the top to enter, or use a previously created, Publish Set ID for this Job:

.. figure:: img/publish_set_id.png
   :alt: Create or reuse a Publish Set ID when publishing a Record Group
   :target: _images/publish_set_id.png

   Create or reuse a Publish Set ID when publishing a Record Group