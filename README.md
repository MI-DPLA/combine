# &#128668; Combine 

## Overview

Combine is a Django application to facilitate the harvesting, transformation, analysis, and publishing of metadata records by Service Hubs for inclusion in the [Digital Public Library of America (DPLA)](https://dp.la/).

Combine is part of the larger Aggregator-in-a-Box project, and would not function well, if it all, on its own.

The name "Combine", pronounced /kämˌbīn/, is a nod to the [combine harvester used in farming](https://en.wikipedia.org/wiki/Combine_harvester) used to, "efficiently harvest a variety of grain crops."  Instead of grains, we have metadata records!  These metadata records may come in a variety of metadata formats (e.g. MODS, Dublin Core, METS, etc.), and in various states of completeness or DPLA-readiness.  Like the combine equipment used for farming, this application is designed to provide a single point of interaction for multiple steps along the way of harvesting, transforming, and analyzing metadata in preperation for inclusion in DPLA.


## Installation

Combine has a decent amount of dependencies and configurations that must be in place to work, as it leverages [Apache Spark](https://spark.apache.org/) for processing on the backend.

As mentioned above, Combine is meant to operate in the context of the Aggregator-in-a-Box server environment.  A separate GitHub repository, [AgBox](https://github.com/WSULib/AgBox), has been created to assist with provisioning a server with all the neccessary components and dependencies to run Combine.  Please visit the [AgBox](https://github.com/WSULib/AgBox) repository for more information about installation.