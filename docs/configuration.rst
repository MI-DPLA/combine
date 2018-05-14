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

Transformation Scenarios are used for transforming the XML of Records during Transformation Jobs.  Currently, there are two types of transformation supported: XSLT and Python code snippets.  These are described in more detail below.

It is worth considering, when thinking about Transformations of Records in Combine, that Combine allows for multiple transformations of the same Record.  Imagine a scenario where ``Transformation A`` crosswalks metadata from a repository to something more aligned with a state service hub, ``Transformation B`` fixes some particular date formats, and ``Transformation C`` -- a python transformation -- looks for a particular identifier field and creates a new field based on that.  Each of the transformations would be a seperate Transformation Scenario, and would be run as seperate Jobs in Combine, but in effect would be "chained" together by the user for a group of Records.

All Transformations require the following information:

  - ``Name`` - Human readable name for Transformation Scenario
  - ``Payload`` - This is where the actual transformation code is added (more on the different types below)
  - ``Transformation Type`` - ``xslt`` for XSLT transformations, or ``python`` for python code snippets
  - ``Filepath`` - *This may be ignored* (in some cases, transformation payloads were written to disk to be used, but likely depracated moving forward)

.. figure:: img/config_add_transform.png
   :alt: Adding Transformation Scenario in Django admin screen
   :target: _images/config_add_transform.png

   Adding Transformation Scenario in Django admin screen

Finally, Transformation Scenarios may be tested within Combine over a pre-existing Record.  This is done by clicking the "Test Transformation Scenario" button from Configuration page.  This will take you to a screen that is similarly used for testing Transformations, Validations, and Record Identifier Transformations.  For Transformations, it looks like the following:

.. figure:: img/test_transform_screen.png
   :alt: Testing Transformation Scenario with pre-existing Record
   :target: _images/test_transform_screen.png

   Testing Transformation Scenario with pre-existing Record

In this screenshot, a few things are happening:

  - a single Record has been clicked from the sortable, searchable table, indicating it will be used for the Transformation testing
  - a *pre-existing* Transformation Scenario has been selected from the dropdown menu, automatically populating the payload and transformation type inputs

    - however, a user may also add or edit the payload and transformation types live here, for testing purposes

  - at the very bottom, you can see the immediate results of the Transformation as applied to the selected Record

Currently, there is no way to save changes to a Transformation Scenario, or add a new one, from this screen, but it allows for realtime testing of Transformation Scenarios.

XSLT
----

XSLT transformations are performed by a small XSLT processor servlet called via `pyjxslt <https://github.com/cts2/pyjxslt>`_.  Pyjxslt uses a built-in Saxon HE XSLT processor that supports XSLT 2.0.  Currently, XSLT stylesheets that **import** other stylesheets -- either locally or remotely -- are not supported.  There are designs to incorporate `Elsevier's "spark-xml-utils" <https://github.com/elsevierlabs-os/spark-xml-utils>`_ Spark library for XSLT transformations, which would address this issue, but this has not been implemented at this time.


Python Code Snippet
-------------------

An alternative to XSLT transformations are created Transformation Scenarios that use python code snippets to transform the Record.  The key to making a successful python Transformation Scenario is to add code matches the pattern Combine is looking for from a python Transformation.  This requires a bit of explanation about how Records are transformed in Spark.

For Transformation Jobs in Combine, each Record in the input Job is fed to the Transformation Scenario.  If the ``transformation type`` is ``xslt``, the XSLT stylesheet for that Transformation Scenario is used as-is on the Record's raw XML.  However, if the ``transformation type`` is ``python``, the python code provided for the Transformation Scenario will be used.

The python code snippet may include as many imports or function definitions as needed, but will require one function that each Record will be passed to, and this function must be named ``python_record_transformation``.  Additionally, this function must expect one function argument, a passed instance of what is called a `PythonUDFRecord <https://github.com/WSULib/combine/blob/master/core/spark/utils.py#L45-L105>`_.  In Spark, "UDF" oftens refers to a "User Defined Function"; which is precisely what this parsed Record instance is passed to in the case of a Transformation.  This is a convenience class that parses a Record in Combine for easy interaction within Transformation, Validation, and Record Identifier Transformation Scenarios.   A ``PythonUDFRecord`` instance has the following representations of the Record:

  - ``record_id`` - The Record Identifier of the Record
  - ``document`` - raw, XML for the Record (what is passed to XSLT records)
  - ``xml`` - raw XML parsed with lxml's etree, an ``ElementTree`` instance
  - ``nsmap`` - dictionary of namespaces, useful for working with ``self.xml`` instance

Finally, the function ``python_record_transformation`` must return a python **list** with the following, ordered elements: [ transformed XML as a string, any errors if they occurred as a string, True/False for successful transformation ].  For example, a valid return might be, with the middle value a blank string indicating no error:

.. code-block:: python

    [ "<xml>....</xml>", "", True ]

A full example of a python code snippet transformation might look like the following.  In this example, a ``<mods:accessCondition>`` element is added or updated.  Note the imports, the comments, the use of the ``PythonUDFRecord`` as the single argument for the function ``python_record_transformation``, all fairly commonplace python code:

.. code-block:: python

    # NOTE: ability to import libraries as needed
    from lxml import etree

    def python_record_transformation(record):

      '''
      Python transformation to add / update <mods:accessCondition> element
      '''

      # check for <mods:accessCondition type="use and reproduction">
      # NOTE: not built-in record.xml, parsed Record document as etree instance
      # NOTE: not built-in record.nsmap that comes with record instance
      ac_ele_query = record.xml.xpath('mods:accessCondition', namespaces=record.nsmap)

      # if single <mods:accessCondition> present
      if len(ac_ele_query) == 1:

        # get single instance
        ac_ele = ac_ele_query[0]

        # confirm type attribute
        if 'type' in ac_ele.attrib.keys():

          # if present, but not 'use and reproduction', update
          if ac_ele.attrib['type'] != 'use and reproduction':
            ac_ele.attrib['type'] = 'use and reproduction'


      # if <mods:accessCondition> not present at all, create
      elif len(ac_ele_query) == 0:
        
        # build element
        rights = etree.Element('{http://www.loc.gov/mods/v3}accessCondition')
        rights.attrib['type'] = 'use and reproduction'
        rights.text = 'Here is a blanket rights statement for our institution in the absence of a record specific one.'

        # append
        record.xml.append(rights)


      # finally, serialize and return as required list [document, error, success (bool)]
      return [etree.tostring(record.xml), '', True]

In many if not most cases, XSLT will fit the bill and provide the needed transformation in Combine.  But the ability to write python code for transformation opens up the door to complex and/or precise transformations if needed.


Validation Scenario
===================

Validation Scenarios are by which Records in Combine are validated against.  Similar to Transformation Scenarios outlined above, they currently accept two formats: Schematron and python code snippets.  Each Validation Scenario requires the following fields:

  - ``Name`` - human readable name for Validation Scenario
  - ``Payload`` - pasted schematron or python code
  - ``Validation type`` - ``sch`` for Schematron, or ``python`` for python code snippet
  - ``Filepath`` - *This may be ignored* (in some cases, validation payloads were written to disk to be used, but likely depracated moving forward)
  - ``Default run`` - if checked, this Validation Scenario will be automatically checked when running a new Job

.. figure:: img/config_add_validation.png
   :alt: Adding Validation Scenario in Django admin
   :target: _images/config_add_validation.png

   Adding Validation Scenario in Django admin

When running a Job, **multiple** Validation Scenarios may be applied to the Job, each of which will run for every Record.  Validation Scenarios -- Schematron or python code snippets -- may include multiple tests or "rules" with a single scenario.  So, for example, ``Validation A`` may contain ``Test 1`` and ``Test 2``.  If run for a Job, and ``Record Foo`` fails ``Test 2`` for the ``Validation A``, the results will show the failure for that Validation Scenario as a whole.  

When thinking about creating Validation Scenarios, there is flexibility in how many tests to put in a single Validation Scenario, versus splitting up those tests between distinct Validation Scenarios, recalling that **multiple** Validation Scenarios may be run for a single Job.  It is worth pointing out, multiple Validation Scenarios for a Job will likely degrade performance *more* than a multiple tests within a single Scenario, though this has not been testing thoroughly, just speculation based on how Records are passed to Validation Scenarios in Spark in Combine.

Like Transformation Scenarios, Validation Scenarios may also be tested in Combine.  This is done by clicking the button, "Test Validation Scenario", resulting in the following screen:

.. figure:: img/test_validation_screen.png
   :alt: Testing Validation Scenario
   :target: _images/test_validation_screen.png

   Testing Validation Scenario

In this screenshot, we an see the following happening:

  - a single Record has been clicked from the sortable, searchable table, indicating it will be used for the Validation testing
  - a pre-existing Validation Scenario -- ``DPLA minimum``, a Schematron validation -- has been selected, automatically populating the payload and validation type inputs

    - However, a user may choose to edit or input their own validation payload here, understanding that editing and saving cannot currently be done from this screen, only testing

  - Results are shown at the bottom in two areas:

    - ``Parsed Validation Results`` - parsed results of the Validation, showing tests that have **passed**, **failed**, and a **total count** of failures
    - ``Raw Validation Results`` - raw resutls of Validation Scenario, in this case XML from the Schematron response, but would be a JSON string for a python code snippet Validation Scenario

As mentioned, two types of Validation Scenarios are currently supported, Schematron and python code snippets, and are detailed below.

Schematron
----------

A valid `Schematron XML <http://schematron.com/>`_ document may be used as the Validation Scenario payload, and will validate the Record's raw XML.  Schematron validations are rule-based, and can be configured to return the validation results as XML, which is the case in Combine.  This XML is parsed, and each distinct, defined test is noted and parsed by Combine.

Below is an example of a small Schematron validation that looks for some required fields in an XML document that would help make it DPLA compliant:

.. code-block:: xml

    <?xml version="1.0" encoding="UTF-8"?>
    <schema xmlns="http://purl.oclc.org/dsdl/schematron" xmlns:mods="http://www.loc.gov/mods/v3">
      <ns prefix="mods" uri="http://www.loc.gov/mods/v3"/>
      <!-- Required top level Elements for all records record -->
      <pattern>
        <title>Required Elements for Each MODS record</title>
        <rule context="mods:mods">
          <assert test="mods:titleInfo">There must be a title element</assert>
          <assert test="count(mods:location/mods:url[@usage='primary'])=1">There must be a url pointing to the item</assert>
          <assert test="count(mods:location/mods:url[@access='preview'])=1">There must be a url pointing to a thumnail version of the item</assert>
          <assert test="count(mods:accessCondition[@type='use and reproduction'])=1">There must be a rights statement</assert>
        </rule>
      </pattern>
       
      <!-- Additional Requirements within Required Elements -->
      <pattern>
        <title>Subelements and Attributes used in TitleInfo</title>
        <rule context="mods:mods/mods:titleInfo">
          <assert test="*">TitleInfo must contain child title elements</assert>
        </rule>
        <rule context="mods:mods/mods:titleInfo/*">
          <assert test="normalize-space(.)">The title elements must contain text</assert>
        </rule>
      </pattern>
      
      <pattern>
        <title>Additional URL requirements</title>
        <rule context="mods:mods/mods:location/mods:url">
          <assert test="normalize-space(.)">The URL field must contain text</assert>
        </rule> 
      </pattern>
      
    </schema>


Python Code Snippet
-------------------

Similar to Transformation Scenarios, python code may also be used for the Validation Scenarios payload.  When a Validation is run for a Record, and a python code snippet type is detected, all defined function names that begin with ``test_`` will be used as separate, distinct Validation tests.  This very similar to how `pytest <https://docs.pytest.org/en/latest/contents.html>`_ looks for function names prefixed with ``test_``.  It is not perfect, but relatively simple and effective.

These functions must expect two arguments.  The first is an instance of a `PythonUDFRecord <https://github.com/WSULib/combine/blob/master/core/spark/utils.py#L45-L105>`_.  As detailed above, ``PythonUDFRecord`` instances are a parsed, convenient way to interact with Combine Records.  A ``PythonUDFRecord`` instance has the following representations of the Record:

  - ``record_id`` - The Record Identifier of the Record
  - ``document`` - raw, XML for the Record (what is passed to XSLT records)
  - ``xml`` - raw XML parsed with lxml's etree, an ``ElementTree`` instance
  - ``nsmap`` - dictionary of namespaces, useful for working with ``self.xml`` instance

The second argument is named and must be called ``test_message``.  The string value for the ``test_message`` argument will be used for reporting if that particular test if failed; this is the human readable name of the validation test.

All validation tests, recalling the name of the function must be prefixed with ``test_``, must return ``True`` or ``False`` to indicate if the Record passed the validation test.

An example of an arbitrary Validation Scenario that looks for MODS titles longer than 30 characters might look like the following:

.. code-block:: python

    # note the ability to import (just for demonstration, not actually used below)
    import re


    def test_title_length_30(record, test_message="check for title length > 30"):

      # using PythonUDFRecord's parsed instance of Record with .xml attribute, and namespaces from .nsmap
      titleInfo_elements = record.xml.xpath('//mods:titleInfo', namespaces=record.nsmap)
      if len(titleInfo_elements) > 0:
        title = titleInfo_elements[0].text
        if len(title) > 30:
          # returning False fails the validation test
          return False
        else:
          # returning True, passes
          return True


    # note ability to define other functions
    def other_function():
      pass


    def another_function();
      pass


Record Identifier Transformation Scenario (RITS)
================================================

Another configurable "Scenario" in Combine is a Record Identifier Transformation Scenario or "RITS" for short.  A RITS allows the transformation of a Record's "Record Identifier".  A Record has `three identifiers in Combine <data_model.html#identifiers>`_, with the Record Identifier (``record_id``) as the only changable, mutable of the three.  The Record ID is what is used for publishing, and for all intents and purposes, the unique identifier for the Record *outside* of Combine.

Record Identifiers are created during Harvest Jobs, when a Record is first created.  This Record Identifier may come from the OAI server in which the Record was harvested from, it might be derived from an identifier in the Record's XML in the case of a static harvest, or it may be minted as a UUID4 on creation.  Where the Record ID is picked up from OAI or the Record's XML itself, it might not need transformation before publishing, and can "go out" just as it "came in."  However, there are instances where transforming the Record's ID can be quite helpful.

Take the following scenario.  A digital object's metadata is harvested from ``Repository A`` with the ID ``foo``, as part of OAI set ``bar``, by ``Metadata Aggregator A``.  Inside ``Metadata Aggregator A``, which has its own OAI server prefix of ``baz`` considers the full identifier of this record: ``baz:bar:foo``.  Next, ``Metadata Aggregator B`` harvests this record from ``Metadata Aggregator A``, under the OAI set ``scrog``.  ``Metadata Aggregator B`` has its own OAI server prefix of ``tronic``.  Finally, when a terminal harvester like DPLA harvests this record from ``Metadata Aggregator B`` under the set ``goober``, it might have a motely identifier, constructed through all these OAI "hops" of something like: ``tronic:goober:baz:bar:foo``.  

If one of these hops were replaced by an instance of Combine, one of the OAI "hops" would be removed, and the dynamically crafted identifier for that same record would change.  Combine allows the ability to transform the identifier -- emulating previous OAI "hops", completely re-writing, or any other transformation -- through a Record Identifier Transformation Scenario (RITS).

RITS are performed, just like Transformation Scenarios or Validation Scenarios, for every Record in the Job.  RITS may be in the form of:

  - Regular Expressions - specifically, python flavored regex
  - Python code snippet - a snippet of code that will transform the identifier
  - XPATH expression - given the Record's raw XML, an XPath expression may be given to extract a value to be used as the Record Identifier

All RITS must have the following values:

  - ``Name`` - Human readable name for RITS
  - ``Transformation type`` - ``regex`` for Regular Expression, ``python`` for Python code snippet, or ``xpath`` for XPath expression
  - ``Transformation target`` - the RITS payload and type may use the pre-existing Record Identifier as input, or the Record's raw, XML record
  - ``Regex match payload`` - If using regex, the regular expression to **match**
  - ``Regex replace playload`` - If using regex, the regular expression to **replace** that match with (allows values from groups)
  - ``Python payload`` - python code snippet, that will be passed an instance of a `PythonUDFRecord <https://github.com/WSULib/combine/blob/master/core/spark/utils.py#L45-L105>`_
  - ``Xpath payload`` - single XPath expression as a string

.. figure:: img/config_add_rits.png
   :alt: Adding Record Identifier Transformation Scenario (RITS)
   :target: _images/config_add_rits.png

   Adding Record Identifier Transformation Scenario (RITS)

Similar to Transformation and Validation scenarios, RITS can be tested by clicking the "Test Record Identifier Transformation Scenario" button at the bottom.  You will be presented with a familiar screen of a table of Records, and the ability to select a pre-existing RITS, edit that one, and/or create a new one.  Similarly, without the ability to update or save a new one, merely to test the results of one.



These different types will be outline in a bit more detail below.


Regular Expression
------------------

If transforming the Record ID with regex, two "payloads" are required for the RITS scenario: a match expression, and a replace expression.  Also of note, these regex matche and replace expressions are the python flavor of regex matching, performed with python's ``re.sub()``.

The screenshot belows shows an example of a regex match / replace used to replace ``digital.library.wayne.edu`` with ``goober.tronic.org``, also highlighting the ability to use groups:

.. figure:: img/test_rits_regex.png
   :alt: Example of RITS with Regular Expression
   :target: _images/test_rits_regex.png

   Example of RITS with Regular Expression

A contrived example, this shows a regex expression applied to the input Record identifier of ``oai:digital.library.wayne.edu:wayne:Livingto1876b22354748```.  


Python Code Snippet
-------------------

Python code snippets for RITS operate similarly to Transformation and Validation scenarios in that the python code snippet is given an instance of a PythonUDFRecord for each Record.  However, it differs slightly in that if the RITS ``Transformation target`` is the Record ID only, the PythonUDFRecord will have only the ``.record_id`` attribute to work with.

For a python code snippet RITS, a function named ``transform_identifier`` is required, with a single unnamed, passed argument of a PythonUDFRecord instance.  An example may look like the following:

.. code-block:: python

    # ability to import modules as needed (just for demonstration)
    import re
    import time

    # function named `transform_identifier`, with single passed argument of PythonUDFRecord instance
    def transform_identifier(record):
      
      '''
      In this example, a string replacement is performed on the record identifier,
      but this could be much more complex, using a combination of the Record's parsed
      XML and/or the Record Identifier.  This example is meant ot show the structure of a 
      python based RITS only.
      '''

      # function must return string of new Record Identifier  
        return record.record_id.replace('digital.library.wayne.edu','goober.tronic.org')

And a screenshot of this RITS in action:

.. figure:: img/test_rits_python.png
   :alt: Example of RITS with Python code snippet
   :target: _images/test_rits_python.png

   Example of RITS with Python code snippet


XPath Expression
----------------

Finally, a single XPath expression may be used to extract a new Record Identifier from the Record's XML record.  **Note:** The input must be the Record's Document, not the current Record Identifier, as the XPath must have valid XML to retrieve a value from.  Below is a an example screenshot:

.. figure:: img/test_rits_xpath.png
   :alt: Example of RITS with XPath expression
   :target: _images/test_rits_xpath.png

   Example of RITS with XPath expression


Combine OAI-PMH Server
======================


DPLA Bulk Data Downloads
========================