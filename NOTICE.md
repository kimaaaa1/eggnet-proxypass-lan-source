# Notices

This repository is a source publication for Eggnet's ProxyPass-derived LAN component.

## ProxyPass

The main Java application is derived from ProxyPass:

- Upstream: `https://github.com/Kas-tle/ProxyPass`
- License: AGPL-3.0
- Base merge point used for this source publication: `07683ddca0b6f0e654702b04ba69fac26b1d8b3e`

Eggnet modifications include LAN connector behavior, live Eggnet server list integration, community/follow routing support, and build tasks for `EggnetLan`.

## NetworkCompatible

The `network/` directory contains vendored source from NetworkCompatible:

- Upstream: `https://github.com/Kas-tle/NetworkCompatible`
- License: Apache-2.0, see `network/LICENSE`
- Base commit: `9f3c0b7e72fb6f8934a7d36a518c74afe02c8a6d`
- Included Eggnet commit: `31ebf5083b856993821dc2a3b8ab26cb2e611292`

Eggnet-related modifications are included directly in this repository so the published source is complete without a private submodule.

## Protocol

The `protocol/` directory contains vendored source from Protocol:

- Upstream: `https://github.com/Kas-tle/Protocol`
- License: Apache-2.0, see `protocol/LICENSE`
- Included commit: `9a90fd087a9164ecbd066c1eaa2209a61b251157`

## Excluded Material

This publication intentionally excludes unrelated Eggnet backend/frontend services, deployment files, local runtime logs, editor settings, private helper scripts, credentials, and temporary files.
