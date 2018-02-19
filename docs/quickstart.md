# Quickstart

Sometimes you can't beat kicking the tires to see how an application works.  Demo data from unit tests will be reused here to avoid the need to provide actual OAI-PMH endpoints, transformation XSLT, or other configurations unique to a DPLA Service Hub.  

This quickstart guide will walk through the following, and it's recommended to do so in order:

  * Combine python environment
  * starting / stopping Combine
  * data model (Organizations, RecordGroups, Jobs, Records)
  * configuration
    * setting up OAI-PMH endpoints
    * creating Transformation Scenarios
    * creating Validation Scenarios
  * harvesting records
  * transforming records
  * looking at Jobs and Records
  * duplicating / merging jobs
  * validation of records
    * running reports
  * publishing records

For simplicity's sake, we will assume Combine is installed on a server with the domain name of `combine`, though likely running at the IP `192.168.45.10`, which the Ansible/Vagrant install from [Combine-Playbook](https://github.com/WSULib/combine-playbook) defaults to.  On most systems you can point that IP to a domain name like `combine` by modifying your `/etc/hosts` file on your local machine.  **Fair warning:** `combine` and `192.168.45.10` might be used interchangeably throughout.


## Combine python environment

Combine runs in a [Miniconda](https://conda.io/miniconda.html) python environement, which can be activated from any filepath location by typing:

```
source active combine
```

**Note:** Most commands in this QuickStart guide require you to be in this environment!
  

## Starting / Stopping Combine

### Gunicorn

For normal operations, Combine is run using [Supervisor](http://supervisord.org/), a python based application for running system processes.  Specifically, it's running under the Python WSGI server [Gunicorn](http://gunicorn.org/), under the supervisor program named `gunicorn`.

Start Combine:
```
sudo supervisorctl start gunicorn
```

Stop Combine:
```
sudo supervisorctl stop gunicorn
```

You can confirm that Combine is running by visiting [`http://192.168.45.10/combine`](http://combine-vm/combine), where you should be prompted to login with a username and password.  For default/testing installations, you can use `combine` / `combine` for these credentials.

### Django runserver

You can also run Combine via Django's built-in server.

Convenience script, from `/opt/combine`:
```
./runserver.sh
```

Or, you can run the Django command explicitly from `/opt/combine`:
```
./manage.py runserver --noreload 0.0.0.0:8000
```

You can confirm that Combine is running by visiting [`http://192.168.45.10:8000/combine`](http://combine-vm/combine) (note the `8000` port number).

### Livy Sessions

To run any Jobs, Combine relies on an active (idle) Apache Livy session.  Livy is what makes running Spark jobs possible via the familiar request/response cycle of a Django application.  

Currently, users are responsible for determining if the Livy session is ready, though there are plans to have this automatically handled.  

To check and/or start new Livy session, navigate to: [http://192.168.45.10/combine/livy_sessions](http://192.168.45.10/combine/livy_sessions).  The important column is `status` which should read `idle`.  If not, click `Stop` or `Remove` under the `actions` column, and once stopped, click the `start new session` link.  Takes anywhere from 10-20 seconds to become `idle`.  

You can check the status of the Livy session at a glance from the Combine navigation, which should read `Livy/Spark Session (idle)` and be green if active.


## Combine Data Model 

### Organization

The highest level of organization in Combine is an **Organization**.  Organizations are intended to group and organize records at the level of an institution or organization, hence the name!

You can create a new Organization from the Organizations page at [http://192.168.45.10/combine/organizations](http://192.168.45.10/combine/organizations), or by clicking "Organizations" from Combine navigation.

An example, we can create one with the name "Amazing University".  Only the `name` field is required, others are optional.

### RecordGroup

Within Organizations are **RecordGroups**.  RecordGroups are a "bucket" at the level of a bunch of intellectually similar records.  It is worth noting now that a single RecordGroup can contain multiple **Jobs**, whether they are failed or incomplete attempts, or across time.  Suffice it to say for now that RecordGroups may contain lots of Jobs, which we will create here in a minute through harvests, transforms, etc.

For our example Organization, "Amazing University", an example of a reasonable RecordGroup might be their Fedora Commons based digital repository.  To create a new one, from the [Organizations page](http://192.168.45.10/combine/organizations), click on "Amazing University".  From the Organiation page for "Amazing University" you can create a new RecordGroup.  Let's call it "Fedora Repository"; again, no other fields are required beyond `name`.

Finally, click into the newly created RecordGroup "Fedora Repository" to see the RecordGroup's page, where we can begin to run Jobs.

### Jobs

Central to Combine's workflow philosophy are the ideas of **Jobs**.  Jobs include any of the following:

  * **Harvest** (OAI-PMH, static XML, and others to come)
  * **Transform**
  * **Merge/Duplicate**
  * **Publish**
  * **Analysis**

Within the context of a RecordGroup, one can think of Jobs as "stages" of a group of records, one Job serving as the **input** for the next Job run on those records. i.e.

```
OAI-PMH Harvest Job ---> XSLT Transform Job --> Publish Job
```

### Record

Lastly, the most granular major entity in Combine is an individual **Record**.  Records exist within a Job.  When a Job is deleted, so are the Records (the same can be said for any of these hierarchies moving up).  Records will be created in the course of running Jobs, so won't dwell on them much now.


## Configuration

Currently, there are three main areas in Combine that require user configuration:

  * OAI-PMH endpoints
  * Transformation Scenarios
  * Validation Scenarios

For the sake of this quickstart demo, we can bootstrap our instance of Combine with some demo configurations, creating the following:

  * Transformation Scenario
    * *MODS to Service Hub profile (XSLT transformation)*
  * Validation Scenarios
    * *DPLA minimum (schematron validation)*
    * *Date checker (python validation)*

To boostrap, from `/opt/combine` run:

```
./manage.py bootstrapdemoconfig
```

You can confirm these demo configurations were created by navigating to the configuration screen at [http://192.168.45.10/combine/configurations](http://192.168.45.10/combine/configurations).
  

## Harvesting Records

### Static XML harvest

Now we're ready to run our first Job and generate our first Records.  For this QuickStart, as we have not yet configured any OAI-PMH endpoints, we can run a **static XML** harvest on some demo data included with Combine.

From the RecordGroup screen, near the bottom and under "Harvest", click "Static XML".  You will be presented with a screen to run a harvest job of static XML files from disk.

Many fields are option -- e.g. Name, Description -- but we will need to tell it where to find the files.  For the field `Location of XML files on disk:`, enter the following, which points to a MODS file containing 250 demo records:

```
/tmp/combine/qs/mods
```

Next, we need to provide an XPath query that locates each discrete record within the providedd MODS file.  For the field `XPath for metadata document root (default /*)`, enter the following:

```
/mods:mods
```

Next, we can select the **Index Mapping** we will use for this job.  The default "Generic XPath based mapper" will work for now, with more discussion on Index Mapping later.

Next, we can select Validation Scenarios to run for all Records in this Job.  If you bootstrapped the demo configurations from steps above, you should see two options, "DPLA minimum" and "Date checker"; make sure both are checked.

Finally, click "Harvest Static Files" at the bottom!

This should return you to the RecordGroup page, where a new Job has appeared and is `running` under the `Status` column in the Job table.  A static job of this size should not take long, refresh the page in 10-20 seconds, and hopefully, you should see the Job status switch to `available`.

This table represents all Jobs run for this RecordGroup, and will grow as we run some more.  You may also note that the `Is Valid` column is red and shows `False`, meaning some records have failed the Validation Scenarios we optionally ran for this Job.  We will return to this later.

For now, let's continue by running an XSLT Transformation on these records!


## Transforming Records

In the previous step, we harvestd 250 records from a bunch of static MODS XML documents.  Now, we will transform all the Records in that Job with an XSLT Transformation Scenario.  

From the RecordGroup screen, click the "Transform" link at the bottom.

For a Transform job, you are presented with other Jobs from this RecordGroup that will be used as an **input** job for this Transformation.  

Again, Job Name and Description are both optional.  What is required, is selecting what job will be transformed.  This can be done by clicking the radio button next to the job in the table of Jobs (at this stage, we likely only have the one Harvest we just performed).

Next, we must select a **Transformation Scenario** to apply to the records from the input Job.  We have a Transformation Scenario prepared for us from the quickstart bootstrapping, but this is where you might optionally select different transforms depending on your task at hand.  While only one Transformation Scenario can be applied to a single Transform job, many scenarios can be saved for use by all users, ready for different needs.

Select `MODS to Service Hub profile / xslt` from the dropdown.

Once the input job (radio button) and transformation scenario (dropdown) are selected, we are presented with the same Index Mapping and Validation Scenario options.  As will become apparent, these are configurable for each Job that is run.  We can leave the defaults again, double checking that the two Validation Scenarios -- *DPLA minimum* and *Date checker* -- are both checked.

Finally, click "Transform" at the bottom.

Again, we are kicked back to the RecordGroup screen, and should hopefully see a Transform job `running` as status.  Note, the graph on this page indicates the original Harvest Job was the **input** for the new Transform Job.

Transforms can take a bit longer than harvests, particularly with the additional Validation Scenarios we are running; but still a small job, might take anywhere from 15-30 seconds.  Refresh the page until it shows the status as `available`.

Also of note, hopefully the `Is Valid` column is not red now, and should read `True`.  We will look at validations in more detail, but because we ran the same Validation Scenarios on both Jobs, this suggests the XSLT transformation fixed whatever Validation problems there were.


## Looking at Jobs and Records

At this point, it might be a good time to look at the details of the jobs we have run.  Let's start with the Harvest Job.  Clicking the Job name in the table, or "details" link at the far-right will take you to a Job details page.

**Note:** Clicking the Job in the graph will gray out any other jobs in the table below that are not a) the job itself, or b) upstream jobs that served as inputs (internally referred to as "job lineage").

### Job Details

Here, you will find details about a specific Job.  Major sections include:

  * Job lineage graph - similar to what is seen on RecordGroup page   
  * Notes - user entered notes about the job
  * Records table - sortable, searchable table that searches Records as stored in DB
  * Validation results - results of validation scenarios run
  * Indexed fields analysis - table of fields mapped from Record XML, stored in ElasticSearch

#### Records table

Sortable, searchable, this shows all the individual, discrete Records for this Job.  This is the one entry point for viewing the details about a single Record.  It is also helpful for determining if the Record *is unique with respect to this Job*.

#### Indexed Fields

This table represents individual fields as mapped from a Record's source XML record to ElasticSearch.  This relates back the "Index Mapper" that we select when running each Job.  

To this point, we have been using the default "Generic XPath based mapper", which is a general purpose way of "flattening" an XML document into fields that can be indexed in ElasticSearch for analysis.

For example, it might map the following XML block:

```
<mods:titleInfo>
    <mods:title>Edmund Dulac's fairy-book : </mods:title>
    <mods:subTitle>fairy tales of the allied nations</mods:subTitle>
</mods:titleInfo>
```

into the following *two* ElasticSearch fields:

```
mods_titleInfo_title
mods_titleInfo_subTitle
```

It's not pretty to look at, but it's a convenient way to break records down in such a way that we can analyze across all Records in a Job.  This table represents the results of that mapping across all Records in a Job.

Clicking on the field name on the far-left will reveal all indexed values for that field.  Clicking on the count from "Document with Field" will return a table of Records that have a value for that field, "Document without" will show Records that do not have a value for this field.  

An example of how this may be helpful: sorting by "Documents without", with zero at the top, you can scroll down until you see the number "11".  This represents a subset of Records, 11 of them, that *don't* have the field `mods_subject_topic`, which might itself be illuminating.

Clicking on the button "Show field analysis explanation" will reveal some information about other columns from this table.

**Note:** Short of an extended discussion about this mapping, and possible value, it is worth noting these indexed fields are used almost exclusively for **analysis**, and are not any kind of final mapping or transformation on the Record itself.  The Record's XML is always stored seperately in MySQL (and on disk as Avro files), which is used for any downstream transformations or publishing.  The only exception being where Combine attempts to query the DPLA API to match records, which is based on these mapped fields, but more on that later.



























