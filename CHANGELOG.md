# Changelog

All notable changes to this project will be documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and releases use
[Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- PostgreSQL 17 logical capture with atomic segmented redb persistence.
- Ordered authenticated gRPC consumers with replay, acknowledgement, and seek.
- Optional replay-safe Redis cache connector.
- Offline integrity check, checksummed backup, and verified restore commands.
- TLS, source identity/gap protection, bounded resources, graceful shutdown,
  durable-format migrations, and production support validation.

[Unreleased]: https://github.com/EricH10/lightcdc/compare/v0.1.0...HEAD
