import knex from '../../../../src/common/utils/db_connection'
import { VeranaEcosystemMessageTypes, VeranaParticipantMessageTypes } from '../../../../src/common/verana-message-types'
import { persistIndexerEventsForBlock } from '../../../../src/services/api/indexer_events_query'

describe('indexer events take created entity ids from the chain event', () => {
  const runId = `${Date.now()}-${Math.floor(Math.random() * 10000)}`
  const baseHeight = 8_000_000 + Math.floor(Math.random() * 100_000)
  const baseId = 91_000_000 + Math.floor(Math.random() * 1_000_000)
  let nextId = baseId
  const heights: number[] = []
  const txIds: number[] = []
  const corporationId = 970_000 + Math.floor(Math.random() * 10_000)
  const sharedDid = `did:web:shared-${runId}.example`

  async function insertTx(height: number, hash: string, messages: Array<{ type: string; content: object }>) {
    heights.push(height)
    await knex('block').insert({
      height,
      hash: `block-${runId}-${height}`,
      time: new Date(),
      proposer_address: 'v',
      data: {},
    })
    const txId = nextId++
    txIds.push(txId)
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
      timestamp: new Date('2026-11-13T00:00:00Z'),
      data: {},
      index: 0,
    })
    for (const [index, message] of messages.entries()) {
      await knex('transaction_message').insert({
        id: nextId++,
        tx_id: txId,
        index,
        type: message.type,
        sender: 'verana1operator',
        content: message.content,
      })
    }
    return txId
  }

  async function insertChainEvent(txId: number, height: number, msgIndex: number, type: string, attrs: object) {
    const eventId = nextId++
    await knex('event').insert({
      id: eventId,
      tx_id: txId,
      tx_msg_index: msgIndex,
      type,
      block_height: height,
      source: 'TX_EVENT',
    })
    await knex('event_attribute').insert(
      Object.entries(attrs).map(([key, value], index) => ({
        event_id: eventId,
        index,
        block_height: height,
        composite_key: `${type}.${key}`,
        key,
        value: String(value),
      }))
    )
  }

  beforeAll(async () => {
    await knex('corporation').insert({
      id: corporationId,
      did: `did:web:corp-${runId}.example`,
      created: new Date(),
      modified: new Date(),
      height: 1,
    })
    for (const id of [corporationId, corporationId + 1]) {
      await knex('ecosystem').insert({
        id,
        did: sharedDid,
        corporation_id: corporationId,
        created: new Date(),
        modified: new Date(),
        language: 'en',
        height: 1,
      })
    }
  })

  afterAll(async () => {
    await knex('indexer_events').whereIn('block_height', heights).delete()
    await knex('event_attribute').whereIn('block_height', heights).delete()
    await knex('event').whereIn('tx_id', txIds).delete()
    await knex('transaction_message').whereIn('tx_id', txIds).delete()
    await knex('transaction').whereIn('id', txIds).delete()
    await knex('block').whereIn('height', heights).delete()
    await knex('ecosystem')
      .whereIn('id', [corporationId, corporationId + 1])
      .delete()
    await knex('corporation').where({ id: corporationId }).delete()
    await knex.destroy()
  })

  it('labels a CreateNewEcosystem with the id and corporation of the ecosystem it created, not an older one with the same DID', async () => {
    const height = baseHeight
    const txId = await insertTx(height, `eco-${runId}`, [
      { type: VeranaEcosystemMessageTypes.CreateEcosystem, content: { did: sharedDid, corporation: 'verana1corp' } },
    ])
    await insertChainEvent(txId, height, 0, 'create_ecosystem', {
      ecosystem_id: corporationId + 1,
      corporation_id: corporationId,
      msg_index: 0,
    })

    const [record] = await persistIndexerEventsForBlock(height)

    expect(record.event_type).toBe('CreateNewEcosystem')
    expect(record.payload.entity_id).toBe(String(corporationId + 1))
    expect(record.payload.corporation_id).toBe(corporationId)
  })

  it('labels two participant creates with the same DID in one tx with their own ids', async () => {
    const height = baseHeight + 1
    const content = { did: sharedDid, corporation: 'verana1corp', operator: 'verana1operator', role: 'ISSUER' }
    const txId = await insertTx(height, `pp-${runId}`, [
      { type: VeranaParticipantMessageTypes.SelfCreateParticipant, content },
      { type: VeranaParticipantMessageTypes.SelfCreateParticipant, content },
    ])
    await insertChainEvent(txId, height, 0, 'create_participant', { participant_id: 501, msg_index: 0 })
    await insertChainEvent(txId, height, 1, 'create_participant', { participant_id: 502, msg_index: 1 })

    const records = await persistIndexerEventsForBlock(height)

    expect(records.map((record) => record.payload.entity_id)).toEqual(['501', '502'])
  })
})
