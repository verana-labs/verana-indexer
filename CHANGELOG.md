# Changelog

## [2.0.0](https://github.com/verana-labs/verana-indexer/compare/v1.3.1...v2.0.0) (2026-04-29)


### ⚠ BREAKING CHANGES

* 

### Features

* add support for v4 verifiable transactions in indexer ([#220](https://github.com/verana-labs/verana-indexer/issues/220)) ([11083fc](https://github.com/verana-labs/verana-indexer/commit/11083fc0d69058ac3a3baa9abba8bab01f74f2a0))
* remove mx reputation module ([#237](https://github.com/verana-labs/verana-indexer/issues/237)) ([2769445](https://github.com/verana-labs/verana-indexer/commit/2769445d4adf22edbfc8cc4914a1edb17f447533))


### Bug Fixes

* verana-types import path (.js.js) causing startup failure ([#236](https://github.com/verana-labs/verana-indexer/issues/236)) ([c1cee21](https://github.com/verana-labs/verana-indexer/commit/c1cee21f2b34566bd29da396c78b3a19bf9a5952))

## [1.3.1](https://github.com/verana-labs/verana-indexer/compare/v1.3.0...v1.3.1) (2026-04-28)


### Bug Fixes

* prevent reentrancy in resolver poll service ([0c0ee68](https://github.com/verana-labs/verana-indexer/commit/0c0ee6869b570964a145b0bef10817e8c97f7e9b))

## [1.3.0](https://github.com/verana-labs/verana-indexer/compare/v1.2.1...v1.3.0) (2026-04-27)


### Features

* implement active participants state model with scheduled flips and counters ([#211](https://github.com/verana-labs/verana-indexer/issues/211)) ([484b1cb](https://github.com/verana-labs/verana-indexer/commit/484b1cb52651d5ab22dcdf0e68c83c450211f466))
* implement DID-based event system for WebSocket and VS Agent integration ([#223](https://github.com/verana-labs/verana-indexer/issues/223)) ([4b4cabb](https://github.com/verana-labs/verana-indexer/commit/4b4cabbe1b8acbfcbf1f946cd8a4d9c3f0b0419d))
* merge trust resolver into indexer ([#221](https://github.com/verana-labs/verana-indexer/issues/221)) ([69e5864](https://github.com/verana-labs/verana-indexer/commit/69e5864a34c0d6699be8a92ce6d46e08e16fa775))
* pause crawling on database timeout and pool exhaustion ([#213](https://github.com/verana-labs/verana-indexer/issues/213)) ([ed3ce1d](https://github.com/verana-labs/verana-indexer/commit/ed3ce1d1b82fab34ecc58d946bb039ef86937563))
* update pending tasks vp_state filtering conditions ([#209](https://github.com/verana-labs/verana-indexer/issues/209)) ([1f5e41e](https://github.com/verana-labs/verana-indexer/commit/1f5e41ed4ad99b9fdbd87512e71e25098dc94155))


### Bug Fixes

* add some guards in reindex startup to prevent unrecoverable errors ([#208](https://github.com/verana-labs/verana-indexer/issues/208)) ([c338d08](https://github.com/verana-labs/verana-indexer/commit/c338d0869e22caec935650a804cb0fce2bd71428))
* correct weight aggregation for total trust deposits ([#215](https://github.com/verana-labs/verana-indexer/issues/215)) ([b3133c4](https://github.com/verana-labs/verana-indexer/commit/b3133c4767b7b1e6144bc7b887eaede15df93939))
* start mode detection consuming lot of time ([7d923da](https://github.com/verana-labs/verana-indexer/commit/7d923dabbe8805789f6e7780ad28e57d23686453))

## [1.2.1](https://github.com/verana-labs/verana-indexer/compare/v1.2.0...v1.2.1) (2026-03-25)


### Bug Fixes

* add transaction message table as critical ([#202](https://github.com/verana-labs/verana-indexer/issues/202)) ([5e217ce](https://github.com/verana-labs/verana-indexer/commit/5e217ce683e4f9f9e1ab45de4d71410bce1d794a))
* archived-trust-registry-sync-versions ([#205](https://github.com/verana-labs/verana-indexer/issues/205)) ([d4d8906](https://github.com/verana-labs/verana-indexer/commit/d4d8906136236e50a4fa2c61262ccc3b088374cd))
* remove temporary backfill ([#206](https://github.com/verana-labs/verana-indexer/issues/206)) ([2b9e1e2](https://github.com/verana-labs/verana-indexer/commit/2b9e1e2f6837a4295ec44a3216e2dd706939ac32))

## [1.2.0](https://github.com/verana-labs/verana-indexer/compare/v1.1.0...v1.2.0) (2026-03-19)


### Features

* add granular participant attributes, filters and sorting ([#190](https://github.com/verana-labs/verana-indexer/issues/190)) ([fc7ff3f](https://github.com/verana-labs/verana-indexer/commit/fc7ff3fa8d3bf00d9c3ffba353ae2e8696005269))
* add height-sync reconciliation for trust deposit module ([#196](https://github.com/verana-labs/verana-indexer/issues/196)) ([2140049](https://github.com/verana-labs/verana-indexer/commit/21400493acc4dcc921388fd09e47bf3a50a8d857))
* add reindex job for k8s deployment ([#198](https://github.com/verana-labs/verana-indexer/issues/198)) ([f5d1309](https://github.com/verana-labs/verana-indexer/commit/f5d1309a74f5616edd7c0e8964a6f2727bee4e2a))
* permission message processing using height-based ledger sync ([#192](https://github.com/verana-labs/verana-indexer/issues/192)) ([0ea57bd](https://github.com/verana-labs/verana-indexer/commit/0ea57bdf4a1718ffa3674288fe2ae7b4eec91bda))
* trust registry message processing using height-sync strategy ([#194](https://github.com/verana-labs/verana-indexer/issues/194)) ([aa78587](https://github.com/verana-labs/verana-indexer/commit/aa785872bed79801f6ac26c1b48e73fc973acdb2))


### Bug Fixes

* include use secret to db ([#195](https://github.com/verana-labs/verana-indexer/issues/195)) ([8c36d3a](https://github.com/verana-labs/verana-indexer/commit/8c36d3a370f5cd7b1ec93e21e019c9050c887875))
* resolve trust registry sync issue and normalize permissions fields to null ([#201](https://github.com/verana-labs/verana-indexer/issues/201)) ([da53a8e](https://github.com/verana-labs/verana-indexer/commit/da53a8ec4a05e98ea6f018799ed663c099b280f7))
* several improvements in logging/queries/reindex mode tweaks ([#197](https://github.com/verana-labs/verana-indexer/issues/197)) ([00a3ce7](https://github.com/verana-labs/verana-indexer/commit/00a3ce7dfbb9915f2e231725ff3faf4df656f30b))

## [1.1.0](https://github.com/verana-labs/verana-indexer/compare/v1.0.0...v1.1.0) (2026-03-04)


### Features

* add next_change_at attribute to changes endpoint ([#189](https://github.com/verana-labs/verana-indexer/issues/189)) ([28b2962](https://github.com/verana-labs/verana-indexer/commit/28b296255ecbec7edc4f05b296f0efcaef868c2c))
* optimize crawl speed for fresh and reindex modes with memory-safe tuning ([#180](https://github.com/verana-labs/verana-indexer/issues/180)) ([4d242cb](https://github.com/verana-labs/verana-indexer/commit/4d242cb42bea5c25e98cc93e945b9f4718147b24))


### Bug Fixes

* archive inconsistency in MsgArchiveCredentialSchema ([#178](https://github.com/verana-labs/verana-indexer/issues/178)) ([7d6b1ff](https://github.com/verana-labs/verana-indexer/commit/7d6b1ff8617b0e315473d0df0b00d0f43fa2ff0b))
* optimize all APIs with proper indexing and query alignment ([#188](https://github.com/verana-labs/verana-indexer/issues/188)) ([3e766c2](https://github.com/verana-labs/verana-indexer/commit/3e766c2ba23fb85b8c6707ec5312057ca7f4ce0b))
* reduce latency in perm list endpoint ([#183](https://github.com/verana-labs/verana-indexer/issues/183)) ([d628f5f](https://github.com/verana-labs/verana-indexer/commit/d628f5ff2e44d816acdc8cbe56884ec672409161))
* refactor credential schema ([#182](https://github.com/verana-labs/verana-indexer/issues/182)) ([27436eb](https://github.com/verana-labs/verana-indexer/commit/27436ebf2529f7eae25dedddf22d1624ae72986e))

## [1.0.0](https://github.com/verana-labs/verana-indexer/compare/v1.0.0...v1.0.0) (2026-02-19)


### Features

* account reputation ([#52](https://github.com/verana-labs/verana-indexer/issues/52)) ([85dd8c9](https://github.com/verana-labs/verana-indexer/commit/85dd8c929f05bd794e5f08d2709604d43f929957))
* add indexer version ([#83](https://github.com/verana-labs/verana-indexer/issues/83)) ([15578fd](https://github.com/verana-labs/verana-indexer/commit/15578fd5cc54995c7052ee4a2512f1c66e1f5373))
* add new attributes to trust registry and credential schema ([#108](https://github.com/verana-labs/verana-indexer/issues/108)) ([bdd732f](https://github.com/verana-labs/verana-indexer/commit/bdd732ff7aa7565d763079c788bbd4d7a987b8b2))
* add new permission attributes ([#105](https://github.com/verana-labs/verana-indexer/issues/105)) ([63b9b5a](https://github.com/verana-labs/verana-indexer/commit/63b9b5a6c75d9cd8a93e24dd4cc690f3e32ab807))
* add participant filter for TR and CS ([#161](https://github.com/verana-labs/verana-indexer/issues/161)) ([ec84944](https://github.com/verana-labs/verana-indexer/commit/ec84944edd3230f9e5bd0837d40d6e408259e5ea))
* add perm query params ([#78](https://github.com/verana-labs/verana-indexer/issues/78)) ([abef17e](https://github.com/verana-labs/verana-indexer/commit/abef17efc2238281606bd82d5af30bc0ee80dad6))
* add permission attributes ([#70](https://github.com/verana-labs/verana-indexer/issues/70)) ([84ecbd8](https://github.com/verana-labs/verana-indexer/commit/84ecbd8a71ec0011afc36ad68ef2966d1ccb7894))
* add statistics persistence with HOUR, DAY, and MONTH granularities ([#107](https://github.com/verana-labs/verana-indexer/issues/107)) ([45846b0](https://github.com/verana-labs/verana-indexer/commit/45846b0a5c53699ebc7bdd6d00c039f73a96c11f))
* add swagger ui ([#43](https://github.com/verana-labs/verana-indexer/issues/43)) ([46a3189](https://github.com/verana-labs/verana-indexer/commit/46a3189cf4eaab0a3932f8712282cd31fc3d0e8f))
* add update params support ([#86](https://github.com/verana-labs/verana-indexer/issues/86)) ([7903c7c](https://github.com/verana-labs/verana-indexer/commit/7903c7c2ae3f129eb001e02826e4983b3fc67e50))
* added unknown messages guard ([#87](https://github.com/verana-labs/verana-indexer/issues/87)) ([b106036](https://github.com/verana-labs/verana-indexer/commit/b1060369d3f327e7a2216fb8e50fa94383df78b5))
* At-Block-Height header ([#66](https://github.com/verana-labs/verana-indexer/issues/66)) ([d3eb795](https://github.com/verana-labs/verana-indexer/commit/d3eb795f0c1e3e2f7972c9f8858d56ca3086c2ec))
* basic module for DID directory ([#6](https://github.com/verana-labs/verana-indexer/issues/6)) ([4a1bd33](https://github.com/verana-labs/verana-indexer/commit/4a1bd33ccb3251b73efc3056ee2342d4eec04b3b))
* events endpoint with 'block-processed' event ([#71](https://github.com/verana-labs/verana-indexer/issues/71)) ([22e88a9](https://github.com/verana-labs/verana-indexer/commit/22e88a91cc1ce75e930a91112b96318977cb5ce0))
* get trust deposit ([#47](https://github.com/verana-labs/verana-indexer/issues/47)) ([d56021e](https://github.com/verana-labs/verana-indexer/commit/d56021e4c2d044743ddc1a22965afe8bbaa3b190))
* implement perm pending tasks, cs metadata, and global metrics ([#157](https://github.com/verana-labs/verana-indexer/issues/157)) ([dca8905](https://github.com/verana-labs/verana-indexer/commit/dca8905dd6fa79e4f06ef6d617ed87f27581ae0e))
* import from aura-nw/horoscope-v2 commit f8e3b10ecc5bf905a44ad7d946bb5706827e429d ([#4](https://github.com/verana-labs/verana-indexer/issues/4)) ([7b7a684](https://github.com/verana-labs/verana-indexer/commit/7b7a684666087b1f48a5094d483339234989ab1f))
* indexer response metadata ([#32](https://github.com/verana-labs/verana-indexer/issues/32)) ([e808e85](https://github.com/verana-labs/verana-indexer/commit/e808e85b48630a93ac41a7832e23d582b3b9c737))
* initial credential schema service ([#31](https://github.com/verana-labs/verana-indexer/issues/31)) ([43f8182](https://github.com/verana-labs/verana-indexer/commit/43f81825a70a5c54ac7835022a22ca66d68fe9a1))
* initial dids module ([#28](https://github.com/verana-labs/verana-indexer/issues/28)) ([0c8d170](https://github.com/verana-labs/verana-indexer/commit/0c8d17078dbab879e1b1836558c3d83c2b531e96))
* initial trust registry crawler service ([#30](https://github.com/verana-labs/verana-indexer/issues/30)) ([dd40264](https://github.com/verana-labs/verana-indexer/commit/dd402648e26f70f6df6b1179b479a7eaaa33afca))
* reindexing ([#79](https://github.com/verana-labs/verana-indexer/issues/79)) ([451e51d](https://github.com/verana-labs/verana-indexer/commit/451e51d4fb20a02120aed6a3c63996edcbb45560))
* remove unused services ([#7](https://github.com/verana-labs/verana-indexer/issues/7)) ([8f6eb0e](https://github.com/verana-labs/verana-indexer/commit/8f6eb0e8d2d562b15f778c5f37cb127bd0bc8e45))
* return results with pagination and ordered by timestamp ([#101](https://github.com/verana-labs/verana-indexer/issues/101)) ([4d64951](https://github.com/verana-labs/verana-indexer/commit/4d6495143a9f5b941a2ae966c2300075c6067d0a))
* trust resolver api ([#59](https://github.com/verana-labs/verana-indexer/issues/59)) ([9d00fdf](https://github.com/verana-labs/verana-indexer/commit/9d00fdf87a1db8b78d4b97f36c018f0bb941cedd))
* upgrade codebase to node 22 ([#5](https://github.com/verana-labs/verana-indexer/issues/5)) ([2a2a34b](https://github.com/verana-labs/verana-indexer/commit/2a2a34ba079c4ad049cc8d53c2d62b3af9da50fa))
* use verana types npm ([#76](https://github.com/verana-labs/verana-indexer/issues/76)) ([f009788](https://github.com/verana-labs/verana-indexer/commit/f009788ba4505076779d830d5f91eadcb9fee5a7))


### Bug Fixes

* add custom storage chart ([#166](https://github.com/verana-labs/verana-indexer/issues/166)) ([1cd1ab9](https://github.com/verana-labs/verana-indexer/commit/1cd1ab90234e0df34134a7d195fdd9efa8accd13))
* add helm registry ([#114](https://github.com/verana-labs/verana-indexer/issues/114)) ([66c423a](https://github.com/verana-labs/verana-indexer/commit/66c423ac85959ef9584d78e528fd5351b764ede4))
* add memory limits ([#109](https://github.com/verana-labs/verana-indexer/issues/109)) ([2d1ac24](https://github.com/verana-labs/verana-indexer/commit/2d1ac24ac7413f651ce5550cdc7ef04d63c80804))
* api path ([#62](https://github.com/verana-labs/verana-indexer/issues/62)) ([bd27d4f](https://github.com/verana-labs/verana-indexer/commit/bd27d4faa0dfd5ac8cf5a2fc7c687100cf46e629))
* api responses ([#42](https://github.com/verana-labs/verana-indexer/issues/42)) ([795ac51](https://github.com/verana-labs/verana-indexer/commit/795ac5147ab050e3a827e62730b0863b404a8eef))
* block crawl optimization ([#74](https://github.com/verana-labs/verana-indexer/issues/74)) ([230cd84](https://github.com/verana-labs/verana-indexer/commit/230cd849cbaceb47d0ec59e3a67e9c3b0eb49177))
* block height based on latest processed block (with or without tx) ([#173](https://github.com/verana-labs/verana-indexer/issues/173)) ([3885eca](https://github.com/verana-labs/verana-indexer/commit/3885eca4dde812a09964544673810c20d73faa87))
* broken ci fixed ([#58](https://github.com/verana-labs/verana-indexer/issues/58)) ([8d4b8a5](https://github.com/verana-labs/verana-indexer/commit/8d4b8a55f49f476b89134758c033c182f9ec78e8))
* build error after ReindexCheckpoint type update ([#117](https://github.com/verana-labs/verana-indexer/issues/117)) ([f90f25a](https://github.com/verana-labs/verana-indexer/commit/f90f25ab67f46a3eea318de6ddb9514299a1301b))
* chart version ([cd4c2b6](https://github.com/verana-labs/verana-indexer/commit/cd4c2b6d9c0299255798fa6386dbe16390333466))
* cs endpoint protobuf update ([#73](https://github.com/verana-labs/verana-indexer/issues/73)) ([1cac789](https://github.com/verana-labs/verana-indexer/commit/1cac78953c33cf0c2dc03bd8e1cda17b0a385308))
* current host by default in swagger API ([7da2c15](https://github.com/verana-labs/verana-indexer/commit/7da2c15a48c3bf1bfe7185de2c54d0a46968c5c7))
* deploy dev to k8s ([2306856](https://github.com/verana-labs/verana-indexer/commit/230685669e003d043e5bbe3d5404ee74aaab1010))
* docker deployment (use js + include api docs) ([491d901](https://github.com/verana-labs/verana-indexer/commit/491d901f2874096beddb0c3fbc698e8b70e4e354))
* docker file path ([5776c68](https://github.com/verana-labs/verana-indexer/commit/5776c68935479ba468f89759b8b1bcc433906049))
* docker improvements and repo structure updated ([#56](https://github.com/verana-labs/verana-indexer/issues/56)) ([ceedc38](https://github.com/verana-labs/verana-indexer/commit/ceedc3815c404075b1feda1bbc16f193b8e52a05))
* extraEnv management in Helm chart ([303793d](https://github.com/verana-labs/verana-indexer/commit/303793d6fd69df4e56646c5fe0353a2c9a8ab32c))
* imports from verana-types ([#81](https://github.com/verana-labs/verana-indexer/issues/81)) ([d4f192a](https://github.com/verana-labs/verana-indexer/commit/d4f192a7fe7d51ab0b5a9c8092cfa2a6ea332591))
* improve API consistency across CS, TR, Permission and add account validation ([#174](https://github.com/verana-labs/verana-indexer/issues/174)) ([11d916d](https://github.com/verana-labs/verana-indexer/commit/11d916daed2c0dc56320dab6dd8b8ea98a2c0bc7))
* improve error handling and database pool stability ([#89](https://github.com/verana-labs/verana-indexer/issues/89)) ([11c5c05](https://github.com/verana-labs/verana-indexer/commit/11c5c057541f6a4f8ecbab31c75e929e8d70298b))
* incorrect document ID fixed and documentation updated ([#156](https://github.com/verana-labs/verana-indexer/issues/156)) ([ebb3632](https://github.com/verana-labs/verana-indexer/commit/ebb3632cbb002bbe35e0ccd5fb2c2e2244a90a36))
* increase default CPU and memory limits in deployment ([#120](https://github.com/verana-labs/verana-indexer/issues/120)) ([4eec857](https://github.com/verana-labs/verana-indexer/commit/4eec85708defd0dbaf71a310d3e42defbf51d2ac))
* indexer integrity, permissions, and JSON API errors ([#118](https://github.com/verana-labs/verana-indexer/issues/118)) ([3c0551e](https://github.com/verana-labs/verana-indexer/commit/3c0551e9445c597242ae3a1621bcca02fa12096e))
* linter in main ([b98c090](https://github.com/verana-labs/verana-indexer/commit/b98c09045ac6e53a98deefed772ac9b8cc42fb67))
* memory leak container restarts ([#104](https://github.com/verana-labs/verana-indexer/issues/104)) ([82770fe](https://github.com/verana-labs/verana-indexer/commit/82770fe806b98429a53ea16472df6ad15577ca88))
* persist and expose new attributes across cs, tr, and perm ([#127](https://github.com/verana-labs/verana-indexer/issues/127)) ([59460b8](https://github.com/verana-labs/verana-indexer/commit/59460b8ea99591c5c80a9f6bb0ce115e2690df6e))
* remove conflicting kubectl cmd in CI ([c303326](https://github.com/verana-labs/verana-indexer/commit/c303326a597a5de146b32bd69f29233743c5d898))
* remove swagger default filters and init genesis file/service params ([#46](https://github.com/verana-labs/verana-indexer/issues/46)) ([6a4dca5](https://github.com/verana-labs/verana-indexer/commit/6a4dca5fcfac4655be3ac25f04f5944f2c3f761c))
* removed the hasura ([#33](https://github.com/verana-labs/verana-indexer/issues/33)) ([675df38](https://github.com/verana-labs/verana-indexer/commit/675df386ffdd20540ebcbab60dc0c0e9a719a921))
* resolve history mutation and is_active issues ([#100](https://github.com/verana-labs/verana-indexer/issues/100)) ([1886368](https://github.com/verana-labs/verana-indexer/commit/1886368acd3dd3ac662f6be28557127298e711c2))
* revert getLatestBlockHeight changes ([#177](https://github.com/verana-labs/verana-indexer/issues/177)) ([a43bdbf](https://github.com/verana-labs/verana-indexer/commit/a43bdbfa7673ce5324244a7ef39a73421db0111b))
* semantic release output variables in ci ([ea670c0](https://github.com/verana-labs/verana-indexer/commit/ea670c06c77e743268b8817dcf8f7e5eda4b0d80))
* standardize blockchain numeric fields across TR, CS, DID, PERM,TD modules ([#164](https://github.com/verana-labs/verana-indexer/issues/164)) ([3cd2b4f](https://github.com/verana-labs/verana-indexer/commit/3cd2b4f1c6c7475e8e14a8c28f2f16a62d25e0cd))
* swagger vocabulary updated and permissions type fixed ([#51](https://github.com/verana-labs/verana-indexer/issues/51)) ([9c46cfa](https://github.com/verana-labs/verana-indexer/commit/9c46cfaf0ba861d2151d84c5d6eed854bd7f1f82))
* typing class storage ([#167](https://github.com/verana-labs/verana-indexer/issues/167)) ([938a9a5](https://github.com/verana-labs/verana-indexer/commit/938a9a562dd9257fb7816f646f0889942b82f90f))
* update permissions in dockerfile ([b540e69](https://github.com/verana-labs/verana-indexer/commit/b540e693e12688deeb532c23603ef08774b2a445))
* use configurable Docker image tag in Helm chart ([#121](https://github.com/verana-labs/verana-indexer/issues/121)) ([d6a6536](https://github.com/verana-labs/verana-indexer/commit/d6a6536d2a5d9a48d6ebeff189fef68a11761550))


### Miscellaneous Chores

* release 1.0.0 ([36e30e8](https://github.com/verana-labs/verana-indexer/commit/36e30e87b48787ea8edce5f51e20cdea0d3a6ca1))
