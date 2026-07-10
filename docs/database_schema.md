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
| instantiate_participant | instantiate participant for this code |
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

### `ecosystem`

| Column           | Description                                                                 |
| ---------------- | --------------------------------------------------------------------------- |
| `id`             | Primary key of the ecosystem record                                    |
| `did`            | Decentralized Identifier (DID) string associated with the ecosystem    |
| `corporation`    | Corporation  that controls this ecosystem               |
| `created`        | Creation date/time of the ecosystem record                             |
| `modified`       | Last modification date/time of the ecosystem record                    |
| `archived`       | Date/time when the ecosystem was archived (nullable, if ever archived) |
| `aka`            | Alternative name or alias for the ecosystem (if provided)              |
| `language`       | Default language of the ecosystem                                      |
| `active_version` | ID of the currently active governance framework version (if applicable)     |
| `participants`   | Total active participants across all role subtrees                           |
| `participants_ecosystem` | Active ECOSYSTEM participants in the ecosystem subtree         |
| `participants_issuer_grantor` | Active ISSUER_GRANTOR participants in the ecosystem subtree |
| `participants_issuer` | Active ISSUER participants in the ecosystem subtree               |
| `participants_verifier_grantor` | Active VERIFIER_GRANTOR participants in the ecosystem subtree |
| `participants_verifier` | Active VERIFIER participants in the ecosystem subtree           |
| `participants_holder` | Active HOLDER participants in the ecosystem subtree               |

**Relations**

- One `ecosystem` → Many `governance_framework_version`

### `governance_framework_version`

| Column         | Description                                                            |
| -------------- | ---------------------------------------------------------------------- |
| `id`           | Primary key of the governance framework version record                 |
| `ecosystem_id`        | Foreign key → `ecosystem.id` (links to the parent ecosystem) |
| `created`      | Creation date/time of the governance framework version                 |
| `active_since` | Date/time when this version became active                              |
| `version`      | Version number of the governance framework                             |

**Relations**

- One `governance_framework_version` → Many `governance_framework_document`
- Many `governance_framework_version` → One `ecosystem`

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
| `ecosystem_id`                                       | Foreign key → `ecosystem.id` (links schema to its ecosystem) |
| `json_schema`                                 | JSON schema definition for the credential                              |
| `issuer_grantor_validation_validity_period`   | Validity period (in blocks/days/units) for issuer-grantor validation   |
| `verifier_grantor_validation_validity_period` | Validity period for verifier-grantor validation                        |
| `issuer_validation_validity_period`           | Validity period for issuer validation                                  |
| `verifier_validation_validity_period`         | Validity period for verifier validation                                |
| `holder_validation_validity_period`           | Validity period for holder validation                                  |
| `issuer_onboarding_mode`                      | Issuer onboarding mode                                       |
| `verifier_onboarding_mode`                    | Verifier onboarding mode                                       |
| `holder_onboarding_mode`                      | Holder onboarding mode                                         |
| `pricing_asset_type`                          | Pricing asset type                                                     |
| `pricing_asset`                               | Pricing asset identifier                                               |
| `digest_algorithm`                            | Digest algorithm for schema canonicalization                           |
| `is_active`                                   | Boolean flag indicating if the schema is active                        |
| `archived`                                    | Date/time when the schema was archived (nullable)                      |
| `created`                                     | Creation date/time of the schema record                                |
| `modified`                                    | Last modification date/time of the schema record                       |
| `participants`                                | Total active participants for the schema participant tree               |
| `participants_ecosystem`                      | Active ECOSYSTEM participants for the schema participant tree           |
| `participants_issuer_grantor`                 | Active ISSUER_GRANTOR participants for the schema participant tree      |
| `participants_issuer`                         | Active ISSUER participants for the schema participant tree              |
| `participants_verifier_grantor`               | Active VERIFIER_GRANTOR participants for the schema participant tree    |
| `participants_verifier`                       | Active VERIFIER participants for the schema participant tree            |
| `participants_holder`                         | Active HOLDER participants for the schema participant tree              |

### `participants`

| Column                  | Description                                                                                  |
| ----------------------- | -------------------------------------------------------------------------------------------- |
| `id`                    | Primary key of the participant record                                                         |
| `schema_id`             | Reference to `credential_schemas.id`                                                         |
| `type`                  | Participant type (ECOSYSTEM / ISSUER_GRANTOR / VERIFIER_GRANTOR / ISSUER / VERIFIER / HOLDER) |
| `did`                   | Optional DID associated with this participant                                                 |
| `corporation`           | Corporation (account) owning this participant                                         |
| `vs_operator`           | Validator-side operator address                                                              |
| `created`               | Participant creation timestamp                                                                |
| `adjusted`              | Last adjustment timestamp                             |
| `slashed`               | Timestamp when slashed (optional)                                                            |
| `repaid`                | Timestamp when repaid (optional)                                                             |
| `effective_from`        | Effective start date of participant (optional)                                                |
| `effective_until`       | Effective end date of participant (optional)                                                  |
| `revoked`               | Timestamp when revoked (optional)                                                            |
| `validator_participant_id`     | Reference to another participant which acts as validator (optional)                           |
| `op_state`              | Validation state (VALIDATION_STATE_UNSPECIFIED / PENDING / VALIDATED / TERMINATED)           |
| `op_exp`                | Validation expiration timestamp (optional)                                                   |
| `op_last_state_change`  | Last validation state change timestamp (optional)                                            |
| `op_validator_deposit`  | Validator deposit amount                                                                     |
| `op_current_fees`       | Current fees for validation                                                                  |
| `op_current_deposit`    | Current deposit for validation                                                               |
| `op_summary_digest`     | VP summary digest                                                                    |
| `vs_operator_authz_enabled` | Whether VS operator authorization is enabled                                               |
| `vs_operator_authz_spend_limit` | Spend limit for VS operator authz                                                     |
| `vs_operator_authz_with_feegrant` | Fee grant flag for VS operator authz                                                  |
| `vs_operator_authz_fee_spend_limit` | Fee spend limit for VS operator authz                                             |
| `vs_operator_authz_spend_period` | Spend period for VS operator authz                                                     |
| `validation_fees`       | Validation fees amount                                                                       |
| `issuance_fees`         | Issuance fees amount                                                                         |
| `verification_fees`     | Verification fees amount                                                                     |
| `deposit`               | Deposit associated with the participant                                                       |
| `slashed_deposit`       | Amount slashed                                                                               |
| `repaid_deposit`        | Amount repaid                                                                                |
| `modified`              | Last modified timestamp                                                                      |
| `participants`          | Total active participants in the participant subtree (current state)                          |
| `participants_ecosystem` | Active ECOSYSTEM participants in the participant subtree (current state)                     |
| `participants_issuer_grantor` | Active ISSUER_GRANTOR participants in the participant subtree (current state)          |
| `participants_issuer`   | Active ISSUER participants in the participant subtree (current state)                         |
| `participants_verifier_grantor` | Active VERIFIER_GRANTOR participants in the participant subtree (current state)      |
| `participants_verifier` | Active VERIFIER participants in the participant subtree (current state)                       |
| `participants_holder`   | Active HOLDER participants in the participant subtree (current state)                         |
| `last_valid_flip_version` | Version counter used to validate scheduled participant flips                                |
| `is_active_now`         | Boolean flag indicating whether the participant is currently ACTIVE                           |

### `participant_sessions`

| Column                 | Description                                                                                         |
| ---------------------- | --------------------------------------------------------------------------------------------------- |
| `id`                   | Primary key of the participant session                                                               |
| `corporation`          | Corporation account                                                                       |
| `vs_operator`          | Validator-side operator address                                                                   |
| `agent_participant_id`        | Reference to agent participant ID                                                                    |
| `wallet_agent_participant_id` | Reference to wallet agent participant ID                                                             |
| `session_records`      | JSON array of session records (replaces legacy `authz` tuples)                              |
| `created`              | Creation timestamp                                                                                  |
| `modified`             | Last modified timestamp                                                                             |

### `trust_deposits`

| Column            | Description                                               |
| ----------------- | --------------------------------------------------------- |
| `id`              | Table's identity (primary key)                            |
| `corporation`     | Corporation  owning this trust deposit     |
| `share`           | Total share amount currently held                         |
| `deposit`         | Total trust deposit amount                  |
| `claimable`       | Amount that can currently be claimed                      |
| `slashed_deposit` | Total amount slashed from this trust deposit              |
| `repaid_deposit`  | Total amount repaid to this trust deposit after slash     |
| `last_slashed`    | Timestamp of the last slashing event (nullable)           |
| `last_repaid`     | Timestamp of the last repayment event (nullable)          |
| `slash_count`     | Total number of times this trust deposit has been slashed |

### `participant_scheduled_flips`

| Column          | Description                                                                                         |
| --------------- | --------------------------------------------------------------------------------------------------- |
| `participant_id`       | Foreign key → `participants.id` (participant affected by this flip)                                   |
| `flip_at_time`  | Timestamp when the participant should ENTER or EXIT the ACTIVE state                                |
| `flip_kind`     | Flip kind: 1 = ENTER_ACTIVE, 2 = EXIT_ACTIVE                                                       |
| `status`        | Flip status: 0 = PENDING, 1 = APPLIED, 2 = STALE                                                   |
| `version`       | Version number, must match `participants.last_valid_flip_version` when applied                      |
| `applied_height` | Block height when the flip was applied (nullable until applied)                                   |
| `applied_time`  | Timestamp when the flip was applied (nullable until applied)                                       |
| `created_at`    | Timestamp when this flip entry was created                                                         |

### `entity_participant_changes`

| Column       | Description                                                                                                             |
| ------------ | ----------------------------------------------------------------------------------------------------------------------- |
| `height`     | Block height when the participant count changed                                                                        |
| `block_time` | Block timestamp corresponding to `height`                                                                              |
| `entity_kind`| Entity kind: 0=GLOBAL, 1=ECOSYSTEM, 2=CRED_SCHEMA, 3=PARTICIPANT                                                   |
| `entity_id`  | Entity identifier (null for GLOBAL)                                                                                    |
| `type`       | Participant role type: 0=ANY, 1=ECOSYSTEM, 2=ISSUER_GRANTOR, 3=ISSUER, 4=VERIFIER_GRANTOR, 5=VERIFIER, 6=HOLDER        |
| `value`      | Current number of participants for the given entity_kind/entity_id/type combination at the specified `height`          |

### `operator_authorizations`

Latest on-chain state of each `OperatorAuthorization` from the `verana.de.v1` (delegation) module. One row per authorization; `(corporation_id, operator)` is unique. Rows are deleted when the authorization is revoked on-chain.

| Column                | Description                                                                                              |
| --------------------- | -------------------------------------------------------------------------------------------------------- |
| `id`                  | Primary key — the on-chain uint64 id of the OperatorAuthorization                                        |
| `corporation_id`      | Id of the corporation granting the authorization                                                        |
| `operator`            | Grantee operator account receiving the authorization                                                    |
| `msg_types`           | jsonb array of module message types this authorization applies to                                       |
| `spend_limit`         | jsonb array of `{denom, amount}` — max spendable amount (nullable)                                       |
| `remaining_spend`     | jsonb array of `{denom, amount}` — runtime balance for `spend_limit` (present when `spend_limit` is set) |
| `fee_spend_limit`     | jsonb array of `{denom, amount}` — fee allowance ceiling, hydrated from the mirrored `x/feegrant` grant (nullable) |
| `remaining_fee_spend` | jsonb array of `{denom, amount}` — runtime fee balance from the `x/feegrant` grant (present when a fee allowance exists) |
| `expiration`          | Timestamp after which the authorization is no longer valid (nullable)                                   |
| `period`              | Reset period for `spend_limit`, stored as a duration string (nullable)                                  |
| `modified`            | Block time of the last change to this authorization (drives the `modified_after` list filter)           |
| `height`              | Block height at which this state was captured                                                           |
| `created_at`          | Timestamp when this row was first inserted                                                              |

### `operator_authorization_history`

Append-only history of `OperatorAuthorization` changes, used to serve `At-Block-Height` queries. A `revoked` row records the deletion.

| Column                      | Description                                                                          |
| --------------------------- | ------------------------------------------------------------------------------------ |
| `id`                        | Primary key of the history record (auto-increment)                                   |
| `operator_authorization_id` | The OperatorAuthorization id this record refers to                                   |
| `corporation_id`            | Id of the corporation granting the authorization                                     |
| `operator`                  | Grantee operator account                                                             |
| `msg_types`                 | jsonb array of module message types (nullable for revoke records)                    |
| `spend_limit`               | jsonb array of `{denom, amount}` (nullable)                                           |
| `remaining_spend`           | jsonb array of `{denom, amount}` (nullable)                                           |
| `fee_spend_limit`           | jsonb array of `{denom, amount}` (nullable)                                           |
| `remaining_fee_spend`       | jsonb array of `{denom, amount}` (nullable)                                           |
| `expiration`                | Timestamp after which the authorization expires (nullable)                           |
| `period`                    | Reset period as a duration string (nullable)                                         |
| `modified`                  | Block time of this change (nullable)                                                 |
| `revoked`                   | `true` when this record captures an on-chain revocation (deletion)                   |
| `height`                    | Block height of this change                                                          |
| `created_at`                | Timestamp when this row was inserted                                                 |

### `vs_operator_authorizations`

Latest on-chain state of each `VSOperatorAuthorization` from the `verana.de.v1` (delegation) module. One row per authorization; `(corporation_id, vs_operator)` is unique. The row is deleted when the authorization no longer exists on-chain (its last record was revoked).

| Column           | Description                                                                                                    |
| ---------------- | -------------------------------------------------------------------------------------------------------------- |
| `id`             | Primary key — the on-chain uint64 id of the VSOperatorAuthorization                                            |
| `corporation_id` | Id of the corporation granting the authorization                                                              |
| `vs_operator`    | Grantee VS-operator account receiving the authorization                                                       |
| `records`        | jsonb array of `ParticipantAuthorizationRecord` — one per controlled participant, each carrying `participant_id`, `msg_types`, `spend_limit`, `remaining_spend`, `fee_spend_limit`, `remaining_fee_spend`, `with_feegrant`, `expiration`, `period` |
| `modified`       | Block time of the last change to this authorization (drives the `modified_after` list filter)                |
| `height`         | Block height at which this state was captured                                                                 |
| `created_at`     | Timestamp when this row was first inserted                                                                    |
