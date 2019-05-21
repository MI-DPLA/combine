# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## Unreleased

## `v0.9`
### Added
  - Automatically start Livy session if none present (#398)[https://github.com/WSULib/combine/issues/398]
### Changed
  - Update XML2kvp to limit values at 32k characters [#403](https://github.com/WSULib/combine/issues/403)
  - Globally converting tabs to spaces per PEP 8 spec
  - Updating [DPLA Ingestion3](https://github.com/dpla/ingestion3) build
  - For Ansible build, setting default Spark application to `local[*]` as opposed to local, standalone cluster [#29](https://github.com/WSULib/combine-playbook/issues/29)


## `v0.8`
### Added
  - Global search of Record's mapped fields
  - Ability to add Organizations, Record Groups, and/or Jobs to Published Subsets [#395](https://github.com/WSULib/combine/issues/395)
  - Remove temporary payloads of static harvests on Job delete [#394](https://github.com/WSULib/combine/issues/394)
  - Added `CHANGELOG.md`
### Changed
  - Fixed precounts for Published Subsets when included Jobs mutate [#396](https://github.com/WSULib/combine/issues/396)


## Previous
See [GitHub wiki Roadmap](https://github.com/WSULib/combine/wiki/Roadmap).