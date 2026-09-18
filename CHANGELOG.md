# Changelog

## [0.1.0](https://github.com/eventsalsa/snapshot/compare/v0.0.1...v0.1.0) (2026-09-18)


### Features

* **ci:** align release-please with manifest mode ([db6bef3](https://github.com/eventsalsa/snapshot/commit/db6bef3a806b33af32fe4e442540931dd4d9f3c1))
* **config:** validate positive SchemaVersion in NewRepository ([#12](https://github.com/eventsalsa/snapshot/issues/12)) ([8fb8b3a](https://github.com/eventsalsa/snapshot/commit/8fb8b3a47676d38dc74cb5a0f33e4c5663b81502))
* **core:** add SaveAppended and sanitize infrastructure docs ([#16](https://github.com/eventsalsa/snapshot/issues/16)) ([0bd6db0](https://github.com/eventsalsa/snapshot/commit/0bd6db053a1473a8b54868ccff37a361bddf98e6))
* **core:** fallback to full stream replay on corrupt snapshot payload ([#14](https://github.com/eventsalsa/snapshot/issues/14)) ([78347c6](https://github.com/eventsalsa/snapshot/commit/78347c6c4ec34a730a2b81479d7e0a4fbb549ded))
* **core:** return unified Result[T] from Load with snapshot policy helpers ([#15](https://github.com/eventsalsa/snapshot/issues/15)) ([85b0cf1](https://github.com/eventsalsa/snapshot/commit/85b0cf1099aeb75e72e5a7f28409ea69d75d4deb))
* **postgres:** enforce monotonic version guard in snapshot upsert ([#13](https://github.com/eventsalsa/snapshot/issues/13)) ([18b1dde](https://github.com/eventsalsa/snapshot/commit/18b1ddeaa12780ca4669db2bf9352105f9ac8938))
* **snapshot:** introduce aggregate state snapshotting component ([5df40c9](https://github.com/eventsalsa/snapshot/commit/5df40c91c57cbb0f4bb75cb90460064285407d22))
* **store:** support compound pk and in-memory upcasters ([#18](https://github.com/eventsalsa/snapshot/issues/18)) ([773109a](https://github.com/eventsalsa/snapshot/commit/773109a954b63c0983f1aeb3ea90ef8be0975524))
* upgrade to eventsalsa/store:v0.1.0 and adopt stream terminology ([#9](https://github.com/eventsalsa/snapshot/issues/9)) ([c14dd20](https://github.com/eventsalsa/snapshot/commit/c14dd20d493de2fbfe5141b5611843751230fd65))
* validate stream head and boundaries during save and load ([#17](https://github.com/eventsalsa/snapshot/issues/17)) ([c6fe55f](https://github.com/eventsalsa/snapshot/commit/c6fe55f4796a27ec53fa248abd5d47298ef8eab5))


### Bug Fixes

* **deps:** bump the go-dependencies group across 1 directory with 2 updates ([#11](https://github.com/eventsalsa/snapshot/issues/11)) ([b1647c8](https://github.com/eventsalsa/snapshot/commit/b1647c8d797db82c1452b5e498302d32157c47bd))
* **deps:** bump the go-dependencies group with 3 updates ([#8](https://github.com/eventsalsa/snapshot/issues/8)) ([4de7b92](https://github.com/eventsalsa/snapshot/commit/4de7b9263dee0b39c4574299226b39d48d8cee37))
