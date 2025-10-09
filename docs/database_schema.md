## Database schema

### account

| Column             | Description                                        |
| ------------------ | -------------------------------------------------- |
| id                 | table's identity                                   |
| created_at         | created time                                       |
| updated_at         | updated time                                       |
| address            |                                                    |
| balances           | balances take from LCD balance                     |
| spendable_balances | spendable balance take from LCD spendable_balances |
| type               | address's type                                     |
| pubkey             | address's pubkey                                   |
| account_number     | account number                                     |
| sequence           | address's sequence                                 |

### account_vesting

| Column            | Description                      |
| ----------------- | -------------------------------- |
| id                | table's identity                 |
| created_at        | created time                     |
| updated_at        | updated time                     |
| account_id        | reference to id in account table |
| orginal_vesting   | original vesting                 |
| delegated_free    | delegated free                   |
| delegated_vesting | delegated vesting                |
| start_time        | start time vesting               |
| end_time          | end time vesting                 |

### block

| Column           | Description                                      |
| ---------------- | ------------------------------------------------ |
| height           | table's identity, block height                   |
| hash             | block hash                                       |
| time             | block time                                       |
| proposer_address | validator who proposed this block, in hex format |
| data             | full log block take from RPC block?height        |

### block_checkpoint

| Column     | Description                    |
| ---------- | ------------------------------ |
| id         | table's identity               |
| created_at | created time                   |
| updated_at | updated time                   |
| job_name   | job name                       |
| height     | height checkpoint for this job |

### signature

| Column            | Description                     |
| ----------------- | ------------------------------- |
| id                | table's identity                |
| height            | block height                    |
| block_id_flag     | block id flag                   |
| validator_address | validator address in hex format |
| timestamp         | timestamp                       |
| signature         | signature                       |

### transaction

| Column     | Description                  |
| ---------- | ---------------------------- |
| id         | table's identity             |
| height     | tx's height                  |
| hash       | tx's hash                    |
| codespace  | codespace result tx          |
| code       | code result tx               |
| gas_used   | gas used by tx               |
| gas_wanted | gas wanted by tx             |
| gas_limit  | gas limit by tx              |
| fee        | fee in tx                    |
| timestamp  | timestamp                    |
| data       | full log decoded from RPC tx |
| memo       | memo                         |
| index      | tx's index in one block      |

### transaction_message

| Column    | Description                                                |
| --------- | ---------------------------------------------------------- |
| id        | table's identity                                           |
| tx_id     | reference to id in transaction table                       |
| index     | message index in one transaction                           |
| type      | message type                                               |
| sender    | message sender                                             |
| content   | decoded from message's value                               |
| parent_id | reference to id in this table (used in case authz message) |

### transaction_message_receiver

| Column    | Description                                       |
| --------- | ------------------------------------------------- |
| id        | table's identity                                  |
| tx_msg_id | reference to id in transaction_message table      |
| address   |                                                   |
| reason    | composite key (transfer.recipient/wasm.recipient) |

### event

| Column       | Description                                                         |
| ------------ | ------------------------------------------------------------------- |
| id           | table's identity                                                    |
| tx_id        | reference to id in transaction table (if this is transaction_event) |
| block_height | reference to height in block table                                  |
| tx_msg_index | event of message's index                                            |
| type         | type                                                                |
| source       | source's event (BEGIN_BLOCK_EVENT/END_BLOCK_EVENT/TX_EVENT)         |

### event_attribute

| Column        | Description                          |
| ------------- | ------------------------------------ |
| event_id      | reference to id in event table       |
| key           | key                                  |
| value         | value                                |
| tx_id         | reference to id in transaction table |
| block_height  | reference to id in block table       |
| composite_key | type (in event) + key                |
| index         | event attribute's index in one event |

### code

| Column                 | Description                          |
| ---------------------- | ------------------------------------ |
| code_id                | table's identity, code id in onchain |
| created_at             | created time                         |
| updated_at             | updated time                         |
| creator                | who stored this code on network      |
| data_hash              | data hash of code                    |
| instantiate_permission | instantiate permission for this code |
| type                   | code's type (CW721/CW4973/CW20/...)  |
| status                 | code's status                        |
| store_hash             | hash of tx store code                |
| store_height           | height of tx store code              |

### code_id_verification

| Column                 | Description                    |
| ---------------------- | ------------------------------ |
| id                     | table's identity               |
| created_at             | created time                   |
| updated_at             | updated time                   |
| code_id                | code id                        |
| data_hash              | data hash of code              |
| instantiate_msg_schema | schema for message instantiate |
| query_msg_schema       | schema for message query       |
| execute_msg_schema     | schema for message execute     |
| s3_location            | link s3 save this code         |
| verification_status    | verification status            |
| compiler_version       | compiler version               |
| github_url             | github link to commit          |
| verify_step            | current step verify            |
| verified_at            | verified time                  |

### smart_contract

| Column             | Description                               |
| ------------------ | ----------------------------------------- |
| id                 | table's identity                          |
| created_at         | created time                              |
| updated_at         | updated time                              |
| name               | contract's name                           |
| address            | contract's address                        |
| creator            | contract's creator                        |
| code_id            | code id used to instantiate this contract |
| instantiate_hash   | hash of instantiate tx                    |
| instantiate_height | height of instantiate tx                  |
| version            | contract's version                        |

### smart_contract_event

| Column            | Description                             |
| ----------------- | --------------------------------------- |
| id                | table's identity                        |
| smart_contract_id | reference to id in smart_contract table |
| action            | action with this contract               |
| event_id          | reference to id in event table          |
| index             |                                         |
| created_at        | created time                            |
| updated_at        | updated time                            |

### smart_contract_event_attribute

| Column                  | Description                                   |
| ----------------------- | --------------------------------------------- |
| id                      | table's identity                              |
| smart_contract_event_id | reference to id in smart_contract_event table |
| key                     |                                               |
| value                   |                                               |
| created_at              | created time                                  |
| updated_at              | updated time                                  |

### proposal

| Column            | Description                 |
| ----------------- | --------------------------- |
| created_at        | created time                |
| updated_at        | updated time                |
| proposal_id       | proposal id onchain         |
| voting_start_time |                             |
| voting_end_time   |                             |
| submit_time       |                             |
| deposit_end_time  |                             |
| type              | proposal's type             |
| title             | proposal's title            |
| description       |                             |
| content           | proposal's content          |
| status            |                             |
| tally             |                             |
| initial_deposit   |                             |
| total_deposit     |                             |
| turnout           |                             |
| proposer_address  | address who create proposal |
| count_vote        | count vote result           |

### vote

| Column      | Description                       |
| ----------- | --------------------------------- |
| id          | table's identity                  |
| created_at  | created time                      |
| updated_at  | updated time                      |
| voter       | voter address                     |
| tx_id       | reference to tx vote              |
| vote_option | YES/NO/NO_WITH_VETO/ABSTAIN       |
| proposal_id | reference to id in proposal table |
| txhash      | hash of tx vote                   |
| height      | height of tx vote                 |

### validator

| Column                  | Description                                                                     |
| ----------------------- | ------------------------------------------------------------------------------- |
| id                      | table's identity                                                                |
| created_at              | created time                                                                    |
| updated_at              | updated time                                                                    |
| account_address         | normal account address                                                          |
| commission              | commission rate (rate, max rate, max change rate)                               |
| consensus_address       | consensus address (...valcon... format)                                         |
| consensus_hex_address   | consensus address in hex format                                                 |
| consensus_pubkey        | pubkey consensus                                                                |
| delegators_shares       |                                                                                 |
| delegators_count        |                                                                                 |
| delegators_last_height  |                                                                                 |
| description             | validator's description (details, moniker, website, identity, security contact) |
| image_url               | image url from keybase                                                          |
| index_offset            |                                                                                 |
| jailed                  | true/false                                                                      |
| jailed_until            |                                                                                 |
| min_self_delegation     |                                                                                 |
| missed_block_counter    |                                                                                 |
| operator_address        | operator address (...valoper... format)                                         |
| percent_voting_power    |                                                                                 |
| self_delegation_balance |                                                                                 |
| start_height            |                                                                                 |
| status                  |                                                                                 |
| tokens                  |                                                                                 |
| tombstoned              |                                                                                 |
| unbonding_height        |                                                                                 |
| unbonding_time          |                                                                                 |
| updated_at              |                                                                                 |
| uptime                  |                                                                                 |

### power event

| Column           | Description                          |
| ---------------- | ------------------------------------ |
| id               | table's identity                     |
| tx_id            | reference to id in transaction table |
| height           | transaction height                   |
| validator_src_id | reference to id in validator table   |
| validator_dst_id | reference to id in validator table   |
| type             |                                      |
| amount           |                                      |
| time             |                                      |

### delegator

| Column            | Description                        |
| ----------------- | ---------------------------------- |
| id                | table's identity                   |
| validator_id      | reference to id in validator table |
| delegator_address |                                    |
| amount            |                                    |

### feegrant

| Column       | Description                                                |
| ------------ | ---------------------------------------------------------- |
| id           | table's identity                                           |
| init_tx_id   | reference to id which init feegrant in transaction table   |
| revoke_tx_id | reference to id which revoke feegrant in transaction table |
| granter      | granter address                                            |
| grantee      | grantee address                                            |
| type         |                                                            |
| expiration   |                                                            |
| status       |                                                            |
| spend_limit  |                                                            |
| denom        |                                                            |

### feegrant_history

| Column      | Description                          |
| ----------- | ------------------------------------ |
| id          | table's identity                     |
| tx_id       | reference to id in transaction table |
| feegrant_id | reference to id in feegrant table    |
| granter     | address granter                      |
| grantee     | address grantee                      |
| action      | amount                               |
| denom       |                                      |
| processed   | marked this feegrant is done or not  |

### `dids`

| Column       | Description                                                             |
| ------------ | ----------------------------------------------------------------------- |
| `id`         | Primary key of the record                                               |
| `height`     | Block height when the DID record was created or last updated            |
| `did`        | Decentralized Identifier (DID) string (must be unique)                  |
| `controller` | Controller of the DID                                                   |
| `created`    | Creation date/time of the DID record                                    |
| `modified`   | Last modification date/time of the DID record                           |
| `exp`        | Expiration date/time of the DID                                         |
| `deposit`    | Deposit amount associated with the DID                                  |
| `event_type` | Event type that triggered the DID change (e.g., create, update, revoke) |
| `years`      | Number of years the DID is valid (if applicable)                        |
| `is_deleted` | Whether the DID has been marked as deleted                              |
| `deleted_at` | Date/time when the DID was marked as deleted (if applicable)            |

### `trust_registry`

| Column           | Description                                                                 |
| ---------------- | --------------------------------------------------------------------------- |
| `id`             | Primary key of the trust registry record                                    |
| `did`            | Decentralized Identifier (DID) string associated with the trust registry    |
| `controller`     | Controller of the trust registry                                            |
| `created`        | Creation date/time of the trust registry record                             |
| `modified`       | Last modification date/time of the trust registry record                    |
| `archived`       | Date/time when the trust registry was archived (nullable, if ever archived) |
| `deposit`        | Deposit amount associated with the trust registry                           |
| `aka`            | Alternative name or alias for the trust registry (if provided)              |
| `language`       | Default language of the trust registry                                      |
| `active_version` | ID of the currently active governance framework version (if applicable)     |

**Relations**

- One `trust_registry` → Many `governance_framework_version`

### `governance_framework_version`

| Column         | Description                                                            |
| -------------- | ---------------------------------------------------------------------- |
| `id`           | Primary key of the governance framework version record                 |
| `tr_id`        | Foreign key → `trust_registry.id` (links to the parent trust registry) |
| `created`      | Creation date/time of the governance framework version                 |
| `active_since` | Date/time when this version became active                              |
| `version`      | Version number of the governance framework                             |

**Relations**

- One `governance_framework_version` → Many `governance_framework_document`
- Many `governance_framework_version` → One `trust_registry`

### `governance_framework_document`

| Column       | Description                                                               |
| ------------ | ------------------------------------------------------------------------- |
| `id`         | Primary key of the governance framework document                          |
| `gfv_id`     | Foreign key → `governance_framework_version.id` (links to the version)    |
| `created`    | Creation date/time of the governance framework document                   |
| `language`   | Language of the governance framework document                             |
| `url`        | URL pointing to the governance framework document                         |
| `digest_sri` | Subresource Integrity (SRI) digest for verifying the document’s integrity |

**Relations**

- Many `governance_framework_document` → One `governance_framework_version`

## `credential_schemas`

| Column                                        | Description                                                            |
| --------------------------------------------- | ---------------------------------------------------------------------- |
| `id`                                          | Primary key of the credential schema record                            |
| `tr_id`                                       | Foreign key → `trust_registry.id` (links schema to its trust registry) |
| `json_schema`                                 | JSON schema definition for the credential                              |
| `issuer_grantor_validation_validity_period`   | Validity period (in blocks/days/units) for issuer-grantor validation   |
| `verifier_grantor_validation_validity_period` | Validity period for verifier-grantor validation                        |
| `issuer_validation_validity_period`           | Validity period for issuer validation                                  |
| `verifier_validation_validity_period`         | Validity period for verifier validation                                |
| `holder_validation_validity_period`           | Validity period for holder validation                                  |
| `issuer_perm_management_mode`                 | Permission management mode for issuers                                 |
| `verifier_perm_management_mode`               | Permission management mode for verifiers                               |
| `deposit`                                     | Deposit amount associated with the schema                              |
| `is_active`                                   | Boolean flag indicating if the schema is active                        |
| `archived`                                    | Date/time when the schema was archived (nullable)                      |
| `created`                                     | Creation date/time of the schema record                                |
| `modified`                                    | Last modification date/time of the schema record                       |

### `permissions`

| Column                  | Description                                                                                  |
| ----------------------- | -------------------------------------------------------------------------------------------- |
| `id`                    | Primary key of the permission record                                                         |
| `schema_id`             | Reference to `credential_schemas.id`                                                         |
| `type`                  | Permission type (ECOSYSTEM / ISSUER_GRANTOR / VERIFIER_GRANTOR / ISSUER / VERIFIER / HOLDER) |
| `did`                   | Optional DID associated with this permission                                                 |
| `grantee`               | Account address granted this permission                                                      |
| `created`               | Permission creation timestamp                                                                |
| `created_by`            | Who created this permission                                                                  |
| `extended`              | Optional field for extended permission                                                       |
| `extended_by`           | Who extended this permission                                                                 |
| `slashed`               | Timestamp when slashed (optional)                                                            |
| `slashed_by`            | Who slashed the permission (optional)                                                        |
| `repaid`                | Timestamp when repaid (optional)                                                             |
| `repaid_by`             | Who repaid the permission (optional)                                                         |
| `effective_from`        | Effective start date of permission (optional)                                                |
| `effective_until`       | Effective end date of permission (optional)                                                  |
| `revoked`               | Timestamp when revoked (optional)                                                            |
| `revoked_by`            | Who revoked the permission (optional)                                                        |
| `country`               | ISO 3166-1 alpha-2 country code                                                              |
| `validator_perm_id`     | Reference to another permission which acts as validator (optional)                           |
| `vp_state`              | Validation state (VALIDATION_STATE_UNSPECIFIED / PENDING / VALIDATED / TERMINATED)           |
| `vp_exp`                | Validation expiration timestamp (optional)                                                   |
| `vp_last_state_change`  | Last validation state change timestamp (optional)                                            |
| `vp_validator_deposit`  | Validator deposit amount                                                                     |
| `vp_current_fees`       | Current fees for validation                                                                  |
| `vp_current_deposit`    | Current deposit for validation                                                               |
| `vp_summary_digest_sri` | Optional SRI digest summary                                                                  |
| `vp_term_requested`     | Optional term requested                                                                      |
| `validation_fees`       | Validation fees amount                                                                       |
| `issuance_fees`         | Issuance fees amount                                                                         |
| `verification_fees`     | Verification fees amount                                                                     |
| `deposit`               | Deposit associated with the permission                                                       |
| `slashed_deposit`       | Amount slashed                                                                               |
| `repaid_deposit`        | Amount repaid                                                                                |
| `modified`              | Last modified timestamp                                                                      |

### `permission_sessions`

| Column                 | Description                                                                                         |
| ---------------------- | --------------------------------------------------------------------------------------------------- |
| `id`                   | Primary key of the permission session                                                               |
| `controller`           | Controller account address                                                                          |
| `agent_perm_id`        | Reference to agent permission ID                                                                    |
| `wallet_agent_perm_id` | Reference to wallet agent permission ID                                                             |
| `authz`                | JSON array of Authz entries containing `issuer_perm_id`, `verifier_perm_id`, `wallet_agent_perm_id` |
| `created`              | Creation timestamp                                                                                  |
| `modified`             | Last modified timestamp                                                                             |
