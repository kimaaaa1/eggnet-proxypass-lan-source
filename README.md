# Eggnet ProxyPass LAN Source

This repository contains the corresponding source for the Eggnet LAN component derived from ProxyPass.

It is published for AGPL-3.0 compliance for the ProxyPass-derived Java LAN connector code used by Eggnet.
It does not contain unrelated Eggnet backend, frontend, analytics, deployment, account, or service infrastructure code.

## Contents

- `src/`: ProxyPass-derived Java application and Eggnet LAN connector sources.
- `network/`: vendored NetworkCompatible source used by this component, including Eggnet-required NetherNet signaling changes.
- `protocol/`: vendored Protocol source used by this component.
- `gradle/`, `build.gradle.kts`, `settings.gradle.kts`, `gradlew`, `gradlew.bat`: build files needed to compile the component.
- `LICENSE`: AGPL-3.0 license for the ProxyPass-derived work.

## Upstream Sources

- ProxyPass upstream: `https://github.com/Kas-tle/ProxyPass`
- ProxyPass base merge point: `07683ddca0b6f0e654702b04ba69fac26b1d8b3e`
- Published Eggnet source snapshot: `ea19886d402aae9ecf076dda2432ad5cbfb60dd0`
- Protocol source snapshot: `9a90fd087a9164ecbd066c1eaa2209a61b251157`
- NetworkCompatible base: `9f3c0b7e72fb6f8934a7d36a518c74afe02c8a6d`
- NetworkCompatible included snapshot: `31ebf5083b856993821dc2a3b8ab26cb2e611292`

## License

The ProxyPass-derived component is licensed under AGPL-3.0. See `LICENSE`.

The vendored `network/` and `protocol/` trees retain their upstream Apache-2.0 license files and copyright notices.
See `NOTICE.md` for attribution details.

## Build

Requirements:

- Java 25 toolchain for the main Gradle build.
- Java 21 or newer runtime for the generated connector artifact.
- Windows is required for `createExe` because Launch4j produces `EggnetLan.exe`.

Commands:

```powershell
.\gradlew.bat compileJava
.\gradlew.bat connectorShadowJar
.\gradlew.bat createExe
```

Outputs:

- `build/libs/EggnetLan.jar`
- `build/launch4j/connector/EggnetLan.exe`

## Scope

This source drop is intentionally limited to the ProxyPass-derived LAN component and the source needed to build it.
Private Eggnet service repositories are separate programs communicating over HTTP/API boundaries and are not included here.
