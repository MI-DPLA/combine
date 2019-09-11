# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## Unreleased

## `v0.10`
This is a really big release! In addition to all the changes called out here, the codebase has been refactored and cleaned up quite a bit. Some of the dependencies have also been updated, including ElasticSearch. You may need to re-index your Jobs if upgrading in place.

### Added
- **Add Configuration page to allow editing Validation Scenarios, Transformations, OAI Endpoints, etc. inside the Combine user interface** [#87](https://github.com/MI-DPLA/combine/issues/87)
- Allow changing the Publish Set ID on a Job without unpublishing/republishing [#407](https://github.com/MI-DPLA/combine/issues/407)
- "Re-run all jobs" button on Organizations and Record Groups [#410](https://github.com/MI-DPLA/combine/issues/410)
- Global error recording in admin panel [#430](https://github.com/MI-DPLA/combine/issues/430)
- Add logout link [#194](https://github.com/MI-DPLA/combine/issues/194)
- Add 'include upstream Jobs' toggle to Job re-run options [#358](https://github.com/MI-DPLA/combine/issues/358)
- Include OAI harvest details in Job details [#374](https://github.com/MI-DPLA/combine/issues/374)

### Changed
- FIXED: trying to view the Test Validation Scenario and related pages when a Record exists with an invalid Job ID [#426](https://github.com/MI-DPLA/combine/issues/426)
- FIXED: Malformed validation scenarios fail silently when running in a Job [#431](https://github.com/MI-DPLA/combine/issues/431)
- Give background tasks the same status display as Jobs and Exports [#438](https://github.com/MI-DPLA/combine/issues/438)
- Improve stateio status indicators [#382](https://github.com/MI-DPLA/combine/issues/382)
- Clarify wording on configuration 'payloads' [#441](https://github.com/MI-DPLA/combine/issues/441)
- FIXED: timestamp sorts [#199](https://github.com/MI-DPLA/combine/issues/199)
- FIXED: Job on rerun with invalid records still marked Valid [#379](https://github.com/MI-DPLA/combine/issues/379)

## `v0.9`
### Added
  - Automatically start Livy session if none present [#398](https://github.com/MI-DPLA/combine/issues/398)
### Changed
  - Update XML2kvp to limit values at 32k characters [#403](https://github.com/MI-DPLA/combine/issues/403)
  - Globally converting tabs to spaces per PEP 8 spec
  - Updating [DPLA Ingestion3](https://github.com/dpla/ingestion3) build
  - For Ansible build, setting default Spark application to `local[*]` as opposed to local, standalone cluster [#29](https://github.com/MI-DPLA/combine-playbook/issues/29)


## `v0.8`
### Added
  - Global search of Record's mapped fields
  - Ability to add Organizations, Record Groups, and/or Jobs to Published Subsets [#395](https://github.com/MI-DPLA/combine/issues/395)
  - Remove temporary payloads of static harvests on Job delete [#394](https://github.com/MI-DPLA/combine/issues/394)
  - Added `CHANGELOG.md`
### Changed
  - Fixed precounts for Published Subsets when included Jobs mutate [#396](https://github.com/MI-DPLA/combine/issues/396)


## Previous
See [GitHub wiki Roadmap](https://github.com/MI-DPLA/combine/wiki/Roadmap).
