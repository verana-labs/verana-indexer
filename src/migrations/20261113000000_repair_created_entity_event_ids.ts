import { Knex } from 'knex'

const CREATED_ENTITY_EVENTS = [
  ['/verana.cs.v1.MsgCreateCredentialSchema', 'create_credential_schema', 'credential_schema_id', 'schema_id'],
  ['/verana.ec.v1.MsgCreateEcosystem', 'create_ecosystem', 'ecosystem_id', 'ecosystem_id'],
  ['/verana.pp.v1.MsgCreateRootParticipant', 'create_root_participant', 'root_participant_id', 'participant_id'],
  ['/verana.pp.v1.MsgSelfCreateParticipant', 'create_participant', 'participant_id', 'participant_id'],
  ['/verana.pp.v1.MsgStartParticipantOP', 'start_participant_op', 'participant_id', 'participant_id'],
]

export async function up(knex: Knex): Promise<void> {
  if (!(await knex.schema.hasTable('indexer_events'))) return

  const values = CREATED_ENTITY_EVENTS.map(() => '(?, ?, ?, ?)').join(', ')
  await knex.raw(
    `
    CREATE TEMP TABLE created_event_ids ON COMMIT DROP AS
    WITH created(message_type, chain_type, id_key, payload_key) AS (VALUES ${values}),
    targets AS (
      SELECT ie.id, ie.tx_hash, ie.block_height, ie.message_index, c.chain_type, c.id_key, c.payload_key,
        row_number() OVER (
          PARTITION BY ie.tx_hash, ie.message_index, COALESCE(ie.payload ->> 'proposal_id', ''), ie.message_type
          ORDER BY COALESCE((ie.payload ->> 'proposal_message_index')::int, 0), ie.id
        ) - 1 AS occurrence
      FROM indexer_events ie
      JOIN created c ON c.message_type = ie.message_type
    )
    SELECT t.id, t.payload_key, (
      SELECT btrim(ea.value, '"')
      FROM transaction tx
      JOIN event e ON e.tx_id = tx.id
      JOIN event_attribute ea ON ea.event_id = e.id AND ea.block_height = e.block_height
      WHERE tx.hash = t.tx_hash AND tx.height = t.block_height
        AND e.type = t.chain_type AND COALESCE(e.tx_msg_index, 0) = t.message_index AND ea.key = t.id_key
      ORDER BY e.id
      OFFSET t.occurrence
      LIMIT 1
    ) AS chain_id
    FROM targets t
    `,
    CREATED_ENTITY_EVENTS.flat()
  )

  await knex.raw(`
    DELETE FROM indexer_events ie
    USING created_event_ids c
    WHERE ie.id = c.id AND c.chain_id IS NOT NULL AND c.chain_id IS DISTINCT FROM ie.entity_id
      AND EXISTS (
        SELECT 1 FROM indexer_events other
        WHERE other.id <> ie.id AND other.tx_hash = ie.tx_hash AND other.tx_index = ie.tx_index
          AND other.message_index = ie.message_index AND other.event_type = ie.event_type
          AND other.entity_type IS NOT DISTINCT FROM ie.entity_type AND other.entity_id = c.chain_id
          AND COALESCE(other.payload ->> 'proposal_id', '') = COALESCE(ie.payload ->> 'proposal_id', '')
          AND COALESCE(other.payload ->> 'proposal_message_index', '') = COALESCE(ie.payload ->> 'proposal_message_index', '')
      )
  `)

  await knex.raw(`
    UPDATE indexer_events ie
    SET entity_id = c.chain_id,
      payload = ie.payload || jsonb_build_object('entity_id', c.chain_id, c.payload_key, c.chain_id)
    FROM created_event_ids c
    WHERE ie.id = c.id AND c.chain_id IS NOT NULL AND c.chain_id IS DISTINCT FROM ie.entity_id
  `)

  await knex.raw(`
    UPDATE indexer_events ie
    SET payload = ie.payload || jsonb_build_object('corporation_id', e.corporation_id)
    FROM ecosystem e
    WHERE ie.event_type = 'CreateNewEcosystem' AND e.id::text = ie.entity_id AND e.corporation_id IS NOT NULL
      AND (ie.payload ->> 'corporation_id') IS DISTINCT FROM e.corporation_id::text
  `)
}

export async function down(): Promise<void> {}
