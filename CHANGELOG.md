# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

That is, given a version number `MAJOR.MINOR.PATCH`, increment the:

- **MAJOR** version when you make incompatible changes
- **MINOR** version when you add functionality in a backwards compatible manner
- **PATCH** version when you make backwards compatible bug fixes

## [0.4.4] - 2023-10-17
### Fixed
- Issue with `cls.__getattribute__` as raised a `TypeError` due to `staticmethod` not being callable.
- Have changed this to `getattr(cls, <>)` and no longer errors.

## [0.4.3] - 2023-10-13
### Amended
- File compression no longer a required parameter for CREATE TABLE.

## [0.4.2] - 2023-10-10
### Fixed
- Manifest includes `.sql` files now; as was excluding the CREATE TABLE SQL query before, causing errors.

## [0.4.1] - 2023-10-09
### Fixed
- Issue with unpacking in typing hint (in Python version `3.10` and less); removed.

## [0.4.0] - 2023-10-07
### Added
- Added `AthenaClient` class with **CREATE TABLE** functionality.
- Also added `Schema` and `data_types` in order to create _schema_ which can be used as input for the **CREATE TABLE** functionality.
### Amended
- Put the utility functions into a `Utils` class.
- Added new custom exceptions and changed previous ones to be more general.
- Updated old custom exceptions to the new format.

## [0.3.2] - 2023-10-07
### Fixed
- Error with `from typing import Self` import, so removed.

## [0.3.1] - 2023-10-07
### Added
- Added `GitHub` action to deal with packaging the project and publishing to PyPI.

## [0.3.0] - 2023-10-01
### Added
- Added a `CLogger` class for logging functionality (to stream and a file).
- Made a `Boto3Base` class for all other classes to inherit from; generic base class.

## [0.2.1] - 2023-09-18
### Added
- Added `.join` functionality to `S3Url` objects.

## [0.2.0] - 2023-09-17
### Added
- Initial `S3Client` class for working with `S3`; fully unit tested.
- This includes functionality to **list objects** and also get **file sizes** from `S3`.

## [0.1.0] - 2023-09-17
### Added
- Initial `S3Url` class for working with `S3`; fully unit tested.

## [0.0.1] - 2023-09-17
- Initial project commit.
