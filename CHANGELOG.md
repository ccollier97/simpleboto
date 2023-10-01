# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

That is, given a version number `MAJOR.MINOR.PATCH`, increment the:

- **MAJOR** version when you make incompatible changes
- **MINOR** version when you add functionality in a backwards compatible manner
- **PATCH** version when you make backwards compatible bug fixes

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
