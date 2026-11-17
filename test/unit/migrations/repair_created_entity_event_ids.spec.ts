import knex from '../../../src/common/utils/db_connection'
import { up } from '../../../src/migrations/20261113000000_repair_created_entity_event_ids'

describe('migration 20261113000000_repair_created_entity_event_ids', () => {
  const runId = `${Date.now()}-${Math.floor(Math.random() * 10000)}`
  const height = 8_500_000 + Math.floor(Math.random() * 100_000)
  let nextId = 92_000_000 + Math.floor(Math.random() * 1_000_000)
  const txIds: number[] = []
  const hashes: string[] = []
  const corporationId = 980_000 + Math.floor(Math.random() * 10_000)
  const ecosystemId = corporationId

  async function insertTx(hash: string, index: number): Promise<number> {
    const txId = nextId++
    txIds.push(txId)
    hashes.push(hash)
    await knex('transaction').insert({
      id: txId,
      height,
      hash,
      codespace: '',
      code: 0,
      gas_used: 1,
      gas_wanted: 1,
      gas_limit: 1,
      fee: {},
      timestamp: new Date(),
      data: {},
      index,
    })
    return txId
  }

  async function insertChainEvent(txId: number, msgIndex: number, type: string, key: string, value: string) {
    const eventId = nextId++
    await knex('event').insert({
      id: eventId,
      tx_id: txId,
      tx_msg_index: msgIndex,
      type,
      block_height: height,
      source: 'TX_EVENT',
    })
    await knex('event_attribute').insert({
      event_id: eventId,
      index: 0,
      block_height: height,
      composite_key: `${type}.${key}`,
      key,
      value,
    })
  }

  async function insertStoredEvent(args: {
    hash: string
    txIndex: number
    eventType: string
    messageType: string
    entityType: string
    entityId: string | null
    payload?: Record<string, unknown>
  }): Promise<number> {
    const [row] = await knex('indexer_events')
      .insert({
        event_type: args.eventType,
        did: `did:web:repair-${runId}.example`,
        block_height: height,
        tx_hash: args.hash,
        tx_index: args.txIndex,
        message_index: 0,
        message_type: args.messageType,
        module: 'x',
        entity_type: args.entityType,
        entity_id: args.entityId,
        timestamp: new Date(),
        payload: { entity_id: args.entityId, ...args.payload },
      })
      .returning('id')
    return Number(typeof row === 'object' ? row.id : row)
  }

  const stored = (id: number) => knex('indexer_events').where({ id }).first()

  beforeAll(async () => {
    await knex('block').insert({ height, hash: `block-${runId}`, time: new Date(), proposer_address: 'v', data: {} })
    await knex('corporation').insert({
      id: corporationId,
      did: `did:web:corp-${runId}.example`,
      created: new Date(),
      modified: new Date(),
      height: 1,
    })
    await knex('ecosystem').insert({
      id: ecosystemId,
      did: `did:web:eco-${runId}.example`,
      corporation_id: corporationId,
      created: new Date(),
      modified: new Date(),
      language: 'en',
      height: 1,
    })
  })

  afterAll(async () => {
    await knex('indexer_events').whereIn('tx_hash', hashes).delete()
    await knex('event_attribute').where({ block_height: height }).delete()
    await knex('event').whereIn('tx_id', txIds).delete()
    await knex('transaction').whereIn('id', txIds).delete()
    await knex('ecosystem').where({ id: ecosystemId }).delete()
    await knex('corporation').where({ id: corporationId }).delete()
    await knex('block').where({ height }).delete()
    await knex.destroy()
  })

  it('rewrites wrong and missing created ids from the chain event, and adds the ecosystem corporation', async () => {
    const schemaTx = await insertTx(`cs-${runId}`, 0)
    await insertChainEvent(schemaTx, 0, 'create_credential_schema', 'credential_schema_id', '44')
    const schemaEvent = await insertStoredEvent({
      hash: `cs-${runId}`,
      txIndex: 0,
      eventType: 'CreateNewCredentialSchema',
      messageType: '/verana.cs.v1.MsgCreateCredentialSchema',
      entityType: 'CredentialSchema',
      entityId: null,
    })

    const ecosystemTx = await insertTx(`ec-${runId}`, 1)
    await insertChainEvent(ecosystemTx, 0, 'create_ecosystem', 'ecosystem_id', String(ecosystemId))
    const ecosystemEvent = await insertStoredEvent({
      hash: `ec-${runId}`,
      txIndex: 1,
      eventType: 'CreateNewEcosystem',
      messageType: '/verana.ec.v1.MsgCreateEcosystem',
      entityType: 'Ecosystem',
      entityId: '4',
      payload: { ecosystem_id: '4' },
    })

    const duplicateTx = await insertTx(`dup-${runId}`, 2)
    await insertChainEvent(duplicateTx, 0, 'create_participant', 'participant_id', '502')
    const wrongDuplicate = await insertStoredEvent({
      hash: `dup-${runId}`,
      txIndex: 2,
      eventType: 'SelfCreateParticipant',
      messageType: '/verana.pp.v1.MsgSelfCreateParticipant',
      entityType: 'Participant',
      entityId: '501',
    })
    const correctDuplicate = await insertStoredEvent({
      hash: `dup-${runId}`,
      txIndex: 2,
      eventType: 'SelfCreateParticipant',
      messageType: '/verana.pp.v1.MsgSelfCreateParticipant',
      entityType: 'Participant',
      entityId: '502',
    })

    await knex.transaction((trx) => up(trx))

    const schema = await stored(schemaEvent)
    expect(schema.entity_id).toBe('44')
    expect(schema.payload).toEqual(expect.objectContaining({ entity_id: '44', schema_id: '44' }))

    const ecosystem = await stored(ecosystemEvent)
    expect(ecosystem.entity_id).toBe(String(ecosystemId))
    expect(ecosystem.payload).toEqual(
      expect.objectContaining({
        entity_id: String(ecosystemId),
        ecosystem_id: String(ecosystemId),
        corporation_id: corporationId,
      })
    )

    expect(await stored(wrongDuplicate)).toBeUndefined()
    expect((await stored(correctDuplicate)).entity_id).toBe('502')
  })

  it('leaves correct rows untouched when run again', async () => {
    const before = await knex('indexer_events').whereIn('tx_hash', hashes).orderBy('id')

    await knex.transaction((trx) => up(trx))

    expect(await knex('indexer_events').whereIn('tx_hash', hashes).orderBy('id')).toEqual(before)
  })
})
