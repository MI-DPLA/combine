*********
Exporting
*********

Exporting Records
=================

Records can be exported in two ways: a series of XML files aggregating the XML document for each Record, or the Mapped Fields for each Record as structured data.  Records from a Job, or all Published Records, may be exported.  Both are found under the "Export" tab in their respective screens.


Export XML Documents
--------------------

Exporting documents will export the XML document for all Records in a Job or published, distributed across a series of XML files with an optional number of Records per file and a root element ``<root>`` to contain them.  This is for ease of working with outside of Combine, where a single XML document containing 50k, 500k, 1m records is cumbersome to work with.  The default is 500 Records per file.

.. figure:: img/export_documents.png
   :alt: Export Documents tab
   :target: _images/export_documents.png

   Export Documents tab

You may enter how many records per file, and what kind of compression to use (if any) on the output archive file.


Export Mapped Fields
--------------------

Mapped fields from Records may also be exported, in one of two ways:

  - Line-delimited JSON documents (**recommended**)
  - Comma-seperated, tabular .csv file

Both default to exporting all fields, but these may be limited by selecting specific fields to include in the export by clicking the "Select Mapped Fields for Export".

Both styles may be exported with an optional compression for output.


JSON Documents
~~~~~~~~~~~~~~

This is the preferred way to export mapped fields, as it handles characters for field values that may disrupt column delimiters and/or newlines.

.. figure:: img/export_mapped_json.png
   :alt: Export Mapped Fields as JSON documents
   :target: _images/export_mapped_json.png

   Export Mapped Fields as JSON documents

Combine uses `ElasticSearch-Dump <https://github.com/taskrabbit/elasticsearch-dump>`_  to export Records as line-delimited JSON documents.  This library handles well special characters and newlines, and as such, is recommended.  This output format also handles multivalued fields and maintains field type (integer, string).


CSV
~~~

Alternatively, mapped fields can be exported as comma-seperated, tabular data in .csv format.  As mentioned, this does not as deftly handle characters that may disrupt column delimiters

.. figure:: img/export_mapped_json.png
   :alt: Export Mapped Fields as JSON documents
   :target: _images/export_mapped_json.png

   Export Mapped Fields as JSON documents

If a Record contains a mapped field such as ``mods_subject_topic`` that is repeating, the default export format is to create multiple columns in the export, appending an integer for each instance of that field, e.g.,

.. code-block:: text

    mods_subject_topic.0, mods_subject_topic.1, mods_subject_topic.0
    history, michigan, snow

But if the checkbox, ``Export CSV "Kibana style"?`` is checked, all multi-valued fields will export in the "Kibana style" where a single column is added to the export and the values are comma separated, e.g.,

.. code-block:: text

    mods_subject_topic
    history,michigan,snow