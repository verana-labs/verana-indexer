import knex from '../../../../src/common/utils/db_connection'
import { VeranaDelegationMessageTypes, VeranaParticipantMessageTypes } from '../../../../src/common/verana-message-types'
import { up as createIndexerEventsTable } from '../../../../src/migrations/20260420000000_create_indexer_events'
import { up as hardenIndexerEventsTable } from '../../../../src/migrations/20260421000000_harden_indexer_events_replay'
import { listIndexerEvents, persistIndexerEventsForBlock } from '../../../../src/services/api/indexer_events_query'

describe('indexer_events_query', () => {
  const runId = `${Date.now()}-${Math.floor(Math.random() * 10000)}`
  const baseHeight = 7_000_000 + Math.floor(Math.random() * 100_000)
  let nextId = 90_000_000 + Math.floor(Math.random() * 1_000_000)
  const did = `did:web:indexer-events-${runId}.example`
  const otherDid = `did:web:indexer-events-other-${runId}.example`
  const txHashes: string[] = []
  const heights: number[] = []
  const corporationIds: number[] = []
  const participantIds: number[] = []
  const createdTables: string[] = []

  beforeAll(async () => {
    await ensureBaseTables()
    await createIndexerEventsTable(knex)
    await hardenIndexerEventsTable(knex)
  })

  afterAll(async () => {
    for (const table of createdTables.reverse()) {
      await knex.schema.dropTableIfExists(table)
    }
  })

  async function ensureBaseTables(): Promise<void> {
    if (!(await knex.schema.hasTable('block'))) {
      await knex.schema.createTable('block', (table) => {
        table.bigInteger('height').primary()
        table.text('hash').notNullable()
        table.timestamp('time').notNullable()
        table.text('proposer_address').nullable()
        table.jsonb('data').nullable()
      })
      createdTables.push('block')
    }

    if (!(await knex.schema.hasTable('transaction'))) {
      await knex.schema.createTable('transaction', (table) => {
        table.bigInteger('id').primary()
        table.bigInteger('height').notNullable()
        table.text('hash').notNullable()
        table.text('codespace').nullable()
        table.integer('code').notNullable().defaultTo(0)
        table.bigInteger('gas_used').nullable()
        table.bigInteger('gas_wanted').nullable()
        table.bigInteger('gas_limit').nullable()
        table.jsonb('fee').nullable()
        table.timestamp('timestamp').notNullable()
        table.jsonb('data').nullable()
        table.integer('index').notNullable().defaultTo(0)
      })
      createdTables.push('transaction')
    }

    if (!(await knex.schema.hasTable('transaction_message'))) {
      await knex.schema.createTable('transaction_message', (table) => {
        table.bigInteger('id').primary()
        table.bigInteger('tx_id').notNullable()
        table.integer('index').notNullable().defaultTo(0)
        table.text('type').notNullable()
        table.text('sender').nullable()
        table.jsonb('content').nullable()
      })
      createdTables.push('transaction_message')
    }

    if (!(await knex.schema.hasTable('ecosystem'))) {
      await knex.schema.createTable('ecosystem', (table) => {
        table.bigInteger('id').primary()
        table.text('did').notNullable()
      })
      createdTables.push('ecosystem')
    }

    if (!(await knex.schema.hasTable('credential_schemas'))) {
      await knex.schema.createTable('credential_schemas', (table) => {
        table.bigInteger('id').primary()
        table.bigInteger('ecosystem_id').notNullable()
      })
      createdTables.push('credential_schemas')
    }

    if (!(await knex.schema.hasTable('participants'))) {
      await knex.schema.createTable('participants', (table) => {
        table.bigInteger('id').primary()
        table.bigInteger('schema_id').nullable()
        table.text('did').nullable()
        table.bigInteger('corporation_id').nullable()
        table.text('vs_operator').nullable()
        table.bigInteger('validator_participant_id').nullable()
      })
      createdTables.push('participants')
    }

    if (!(await knex.schema.hasTable('corporation'))) {
      await knex.schema.createTable('corporation', (table) => {
        table.bigInteger('id').primary()
        table.text('did').nullable()
        table.text('corporation').nullable()
        table.text('policy_address').nullable()
      })
      createdTables.push('corporation')
    }

    if (!(await knex.schema.hasTable('event'))) {
      await knex.schema.createTable('event', (table) => {
        table.bigInteger('id').primary()
        table.bigInteger('tx_id').notNullable()
        table.integer('tx_msg_index').nullable()
        table.text('type').notNullable()
        table.bigInteger('block_height').notNullable()
        table.text('source').nullable()
      })
      createdTables.push('event')
    }

    if (!(await knex.schema.hasTable('event_attribute'))) {
      await knex.schema.createTable('event_attribute', (table) => {
        table.bigInteger('event_id').notNullable()
        table.integer('index').notNullable().defaultTo(0)
        table.bigInteger('block_height').notNullable()
        table.text('composite_key').nullable()
        table.text('key').nullable()
        table.text('value').nullable()
      })
      createdTables.push('event_attribute')
    }
  }

  async function insertBlock(height: number): Promise<void> {
    heights.push(height)
    await knex('block').insert({
      height,
      hash: `block-${runId}-${height}`,
      time: new Date('2025-01-15T10:30:00Z'),
      proposer_address: 'validator',
      data: {},
    })
  }

  async function insertTxMessage(args: {
    height: number
    txIndex: number
    messageIndex: number
    hash: string
    sender?: string
    content?: Record<string, unknown>
  }): Promise<void> {
    txHashes.push(args.hash)
    const txId = nextId++
    const messageId = nextId++
    const [tx] = await knex('transaction')
      .insert({
        id: txId,
        height: args.height,
        hash: args.hash,
        codespace: '',
        code: 0,
        gas_used: 1,
        gas_wanted: 1,
        gas_limit: 1,
        fee: {},
        timestamp: new Date('2025-01-15T10:30:00Z'),
        data: {},
        index: args.txIndex,
      })
      .returning('id')

    await knex('transaction_message').insert({
      id: messageId,
      tx_id: typeof tx === 'object' ? tx.id : tx,
      index: args.messageIndex,
      type: VeranaParticipantMessageTypes.StartParticipantOP,
      sender: args.sender ?? did,
      content: args.content ?? { id: 42, applicant: did },
    })
  }

  async function insertStoredIndexerEvent(args: {
    did: string
    relatedDids: string[]
    height: number
    txHash: string
    txIndex?: number
    messageIndex?: number
    corporationId?: number
    relatedCorporationIds?: number[]
  }): Promise<void> {
    txHashes.push(args.txHash)
    await knex('indexer_events').insert({
      event_type: 'StartParticipantOP',
      did: args.did,
      block_height: args.height,
      tx_hash: args.txHash,
      tx_index: args.txIndex ?? 0,
      message_index: args.messageIndex ?? 0,
      message_type: VeranaParticipantMessageTypes.StartParticipantOP,
      module: 'participant',
      entity_type: 'Participant',
      entity_id: '42',
      timestamp: new Date('2025-01-15T10:30:00Z'),
      payload: {
        module: 'participant',
        action: 'StartParticipantOP',
        message_type: VeranaParticipantMessageTypes.StartParticipantOP,
        tx_index: args.txIndex ?? 0,
        message_index: args.messageIndex ?? 0,
        sender: otherDid,
        related_dids: args.relatedDids,
        entity_type: 'Participant',
        entity_id: '42',
        ...(args.corporationId !== undefined ? { corporation_id: args.corporationId } : {}),
        ...(args.relatedCorporationIds ? { related_corporation_ids: args.relatedCorporationIds } : {}),
      },
    })
  }

  afterEach(async () => {
    if (heights.length > 0) {
      await knex('event_attribute').whereIn('block_height', heights).delete()
      await knex('event').whereIn('block_height', heights).delete()
    }
    if (txHashes.length > 0) {
      await knex('indexer_events').whereIn('tx_hash', txHashes).delete()
      const txIds = await knex('transaction').whereIn('hash', txHashes).pluck('id')
      if (txIds.length > 0) await knex('transaction_message').whereIn('tx_id', txIds).delete()
      await knex('transaction').whereIn('hash', txHashes).delete()
      txHashes.length = 0
    }

    if (corporationIds.length > 0) {
      await knex('corporation').whereIn('id', corporationIds).delete()
      corporationIds.length = 0
    }
    if (participantIds.length > 0) {
      await knex('participants').whereIn('id', participantIds).delete()
      participantIds.length = 0
    }

    if (heights.length > 0) {
      await knex('block').whereIn('height', heights).delete()
      heights.length = 0
    }
  })

  it('replays events after a block height for one DID', async () => {
    await insertBlock(baseHeight)
    await insertBlock(baseHeight + 1)
    await insertTxMessage({ height: baseHeight, txIndex: 0, messageIndex: 0, hash: `tx-${runId}-before` })
    await insertTxMessage({ height: baseHeight + 1, txIndex: 0, messageIndex: 0, hash: `tx-${runId}-after` })

    await persistIndexerEventsForBlock(baseHeight)
    await persistIndexerEventsForBlock(baseHeight + 1)

    const events = await listIndexerEvents({ dids: [did], afterBlockHeight: baseHeight, limit: 10 })
    expect(events).toHaveLength(1)
    expect(events[0]).toMatchObject({
      did,
      block_height: baseHeight + 1,
      tx_hash: `tx-${runId}-after`,
      type: 'indexer-event',
    })
  })

  it('returns deterministic replay ordering', async () => {
    await insertBlock(baseHeight + 10)
    await insertTxMessage({ height: baseHeight + 10, txIndex: 1, messageIndex: 0, hash: `tx-${runId}-order-2` })
    await insertTxMessage({ height: baseHeight + 10, txIndex: 0, messageIndex: 1, hash: `tx-${runId}-order-1b` })
    await insertTxMessage({ height: baseHeight + 10, txIndex: 0, messageIndex: 0, hash: `tx-${runId}-order-1a` })

    await persistIndexerEventsForBlock(baseHeight + 10)

    const events = await listIndexerEvents({ dids: [did], afterBlockHeight: baseHeight + 9, limit: 10 })
    expect(events.map((event) => event.tx_hash)).toEqual([
      `tx-${runId}-order-1a`,
      `tx-${runId}-order-1b`,
      `tx-${runId}-order-2`,
    ])
  })

  it('persists one event with all affected DIDs and protects duplicate inserts', async () => {
    await insertBlock(baseHeight + 20)
    await insertTxMessage({
      height: baseHeight + 20,
      txIndex: 0,
      messageIndex: 0,
      hash: `tx-${runId}-multi-did`,
      content: { id: 42, applicant: did, validator: otherDid, nested: { duplicate: did } },
    })

    const firstPersist = await persistIndexerEventsForBlock(baseHeight + 20)
    const secondPersist = await persistIndexerEventsForBlock(baseHeight + 20)
    const rows = await knex('indexer_events').where('tx_hash', `tx-${runId}-multi-did`).orderBy('did', 'asc')

    expect(firstPersist).toHaveLength(1)
    expect(firstPersist[0].did).toBe(did)
    expect(firstPersist[0].payload.related_dids).toEqual([did, otherDid])
    expect(secondPersist).toEqual([])
    expect(rows).toHaveLength(1)
    expect(rows[0].did).toBe(did)
  })

  it('matches persisted events by event.did', async () => {
    const height = baseHeight + 30
    await insertStoredIndexerEvent({
      did,
      relatedDids: [did],
      height,
      txHash: `tx-${runId}-match-did`,
    })

    const events = await listIndexerEvents({ dids: [did], afterBlockHeight: height - 1, limit: 10 })

    expect(events).toHaveLength(1)
    expect(events[0]).toMatchObject({ did, tx_hash: `tx-${runId}-match-did` })
  })

  it('matches persisted events by payload.related_dids', async () => {
    const height = baseHeight + 40
    const relatedDid = `did:web:indexer-events-related-${runId}.example`
    await insertStoredIndexerEvent({
      did: otherDid,
      relatedDids: [relatedDid],
      height,
      txHash: `tx-${runId}-related-dids`,
    })

    const events = await listIndexerEvents({ dids: [relatedDid], afterBlockHeight: height - 1, limit: 10 })

    expect(events).toHaveLength(1)
    expect(events[0].did).toBe(otherDid)
    expect(events[0].payload.related_dids).toContain(relatedDid)
  })

  it('normalizes URL-encoded DID input before matching', async () => {
    const height = baseHeight + 50
    await insertStoredIndexerEvent({
      did,
      relatedDids: [did],
      height,
      txHash: `tx-${runId}-encoded-did`,
    })

    const events = await listIndexerEvents({
      dids: [encodeURIComponent(` ${did} `)],
      afterBlockHeight: 0,
      limit: 10,
    })

    expect(events.map((event) => event.tx_hash)).toContain(`tx-${runId}-encoded-did`)
  })

  it('after_block_height excludes old events', async () => {
    await insertStoredIndexerEvent({
      did,
      relatedDids: [did],
      height: baseHeight + 60,
      txHash: `tx-${runId}-old-height`,
    })
    await insertStoredIndexerEvent({
      did,
      relatedDids: [did],
      height: baseHeight + 61,
      txHash: `tx-${runId}-new-height`,
    })

    const events = await listIndexerEvents({ dids: [did], afterBlockHeight: baseHeight + 60, limit: 10 })

    expect(events.map((event) => event.tx_hash)).toEqual([`tx-${runId}-new-height`])
  })

  it('applies limit after deterministic ordering', async () => {
    await insertStoredIndexerEvent({
      did,
      relatedDids: [did],
      height: baseHeight + 70,
      txHash: `tx-${runId}-limit-1`,
      txIndex: 0,
    })
    await insertStoredIndexerEvent({
      did,
      relatedDids: [did],
      height: baseHeight + 70,
      txHash: `tx-${runId}-limit-2`,
      txIndex: 1,
    })

    const events = await listIndexerEvents({ dids: [did], afterBlockHeight: baseHeight + 69, limit: 1 })

    expect(events.map((event) => event.tx_hash)).toEqual([`tx-${runId}-limit-1`])
  })

  it('returns an empty array for an unknown DID', async () => {
    await insertStoredIndexerEvent({
      did,
      relatedDids: [did],
      height: baseHeight + 80,
      txHash: `tx-${runId}-unknown-did-control`,
    })

    const events = await listIndexerEvents({
      dids: [`did:web:indexer-events-missing-${runId}.example`],
      afterBlockHeight: 0,
      limit: 10,
    })

    expect(events).toEqual([])
  })

  it('does not reconstruct unpersisted historical transaction events in the request path', async () => {
    const height = baseHeight + 90
    const unpersistedDid = `did:web:indexer-events-unpersisted-${runId}.example`
    await insertBlock(height)
    await insertTxMessage({
      height,
      txIndex: 0,
      messageIndex: 0,
      hash: `tx-${runId}-unpersisted-history`,
      sender: otherDid,
      content: { id: 42, applicant: unpersistedDid },
    })

    const events = await listIndexerEvents({ dids: [unpersistedDid], afterBlockHeight: height - 1, limit: 10 })

    expect(events).toEqual([])
  })

  it('does not leak unrelated rows when after_block_height is combined with related_dids matching', async () => {
    const height = baseHeight + 100
    const relatedDid = `did:web:indexer-events-grouping-${runId}.example`
    await insertStoredIndexerEvent({
      did: otherDid,
      relatedDids: [relatedDid],
      height,
      txHash: `tx-${runId}-grouping-match`,
    })
    await insertStoredIndexerEvent({
      did: otherDid,
      relatedDids: [relatedDid],
      height: height - 50,
      txHash: `tx-${runId}-grouping-too-old`,
    })
    await insertStoredIndexerEvent({
      did: otherDid,
      relatedDids: [`did:web:indexer-events-unrelated-${runId}.example`],
      height: height + 1,
      txHash: `tx-${runId}-grouping-unrelated`,
    })

    const events = await listIndexerEvents({ dids: [relatedDid], afterBlockHeight: height - 1, limit: 10 })

    expect(events.map((event) => event.tx_hash)).toEqual([`tx-${runId}-grouping-match`])
  })

  it('matches events by corporation_id via payload.corporation_id', async () => {
    const height = baseHeight + 110
    const corpId = 4242
    await insertStoredIndexerEvent({
      did: otherDid,
      relatedDids: [],
      height,
      txHash: `tx-${runId}-corp-scalar`,
      corporationId: corpId,
    })
    await insertStoredIndexerEvent({
      did: otherDid,
      relatedDids: [],
      height,
      txHash: `tx-${runId}-corp-other`,
      txIndex: 1,
      corporationId: corpId + 1,
    })

    const events = await listIndexerEvents({ corporationId: corpId, afterBlockHeight: height - 1, limit: 10 })

    expect(events.map((event) => event.tx_hash)).toEqual([`tx-${runId}-corp-scalar`])
  })

  it('matches events by corporation_id via payload.related_corporation_ids', async () => {
    const height = baseHeight + 120
    const corpId = 5252
    await insertStoredIndexerEvent({
      did: otherDid,
      relatedDids: [],
      height,
      txHash: `tx-${runId}-corp-related`,
      relatedCorporationIds: [9999, corpId],
    })

    const events = await listIndexerEvents({ corporationId: corpId, afterBlockHeight: height - 1, limit: 10 })

    expect(events.map((event) => event.tx_hash)).toEqual([`tx-${runId}-corp-related`])
  })

  it('matches events by any DID in the dids list', async () => {
    const height = baseHeight + 130
    const didA = `did:web:indexer-events-list-a-${runId}.example`
    const didB = `did:web:indexer-events-list-b-${runId}.example`
    await insertStoredIndexerEvent({ did: didA, relatedDids: [didA], height, txHash: `tx-${runId}-dids-a` })
    await insertStoredIndexerEvent({ did: didB, relatedDids: [didB], height, txHash: `tx-${runId}-dids-b`, txIndex: 1 })
    await insertStoredIndexerEvent({
      did: otherDid,
      relatedDids: [otherDid],
      height,
      txHash: `tx-${runId}-dids-c`,
      txIndex: 2,
    })

    const events = await listIndexerEvents({ dids: [didA, didB], afterBlockHeight: height - 1, limit: 10 })

    expect(events.map((event) => event.tx_hash)).toEqual([`tx-${runId}-dids-a`, `tx-${runId}-dids-b`])
  })

  it('intersects dids AND corporation_id when both are present', async () => {
    const height = baseHeight + 140
    const corpId = 6363
    const targetDid = `did:web:indexer-events-both-${runId}.example`
    await insertStoredIndexerEvent({
      did: targetDid,
      relatedDids: [targetDid],
      height,
      txHash: `tx-${runId}-both-match`,
      corporationId: corpId,
    })
    await insertStoredIndexerEvent({
      did: targetDid,
      relatedDids: [targetDid],
      height,
      txHash: `tx-${runId}-did-only`,
      txIndex: 1,
      corporationId: corpId + 1,
    })
    await insertStoredIndexerEvent({
      did: otherDid,
      relatedDids: [otherDid],
      height,
      txHash: `tx-${runId}-corp-only`,
      txIndex: 2,
      corporationId: corpId,
    })

    const events = await listIndexerEvents({
      dids: [targetDid],
      corporationId: corpId,
      afterBlockHeight: height - 1,
      limit: 10,
    })

    expect(events.map((event) => event.tx_hash)).toEqual([`tx-${runId}-both-match`])
  })

  it('returns all events (wildcard) when neither dids nor corporation_id is given', async () => {
    const height = baseHeight + 150
    await insertStoredIndexerEvent({ did, relatedDids: [did], height, txHash: `tx-${runId}-wild-1` })
    await insertStoredIndexerEvent({
      did: otherDid,
      relatedDids: [],
      height,
      txHash: `tx-${runId}-wild-2`,
      txIndex: 1,
      corporationId: 7777,
    })

    const events = await listIndexerEvents({ afterBlockHeight: height - 1, limit: 10 })

    expect(events.map((event) => event.tx_hash)).toEqual([`tx-${runId}-wild-1`, `tx-${runId}-wild-2`])
  })

  async function insertCorporation(args: {
    id: number
    did: string
    corporation?: string
    policyAddress?: string
  }): Promise<void> {
    corporationIds.push(args.id)
    await knex('corporation').insert({
      id: args.id,
      did: args.did,
      corporation: args.corporation ?? null,
      policy_address: args.policyAddress ?? null,
    })
  }

  async function insertParticipant(args: {
    id: number
    did?: string
    corporationId?: number
    vsOperator?: string
  }): Promise<void> {
    participantIds.push(args.id)
    await knex('participants').insert({
      id: args.id,
      schema_id: null,
      did: args.did ?? null,
      corporation_id: args.corporationId ?? null,
      vs_operator: args.vsOperator ?? null,
      validator_participant_id: null,
    })
  }

  async function insertDelegationMessage(args: {
    height: number
    txIndex: number
    messageIndex: number
    hash: string
    type: string
    content: Record<string, unknown>
  }): Promise<void> {
    txHashes.push(args.hash)
    const txId = nextId++
    const messageId = nextId++
    const [tx] = await knex('transaction')
      .insert({
        id: txId,
        height: args.height,
        hash: args.hash,
        codespace: '',
        code: 0,
        gas_used: 1,
        gas_wanted: 1,
        gas_limit: 1,
        fee: {},
        timestamp: new Date('2025-01-15T10:30:00Z'),
        data: {},
        index: args.txIndex,
      })
      .returning('id')
    await knex('transaction_message').insert({
      id: messageId,
      tx_id: typeof tx === 'object' ? tx.id : tx,
      index: args.messageIndex,
      type: args.type,
      sender: (args.content.corporation as string) ?? '',
      content: args.content,
    })
  }

  async function insertKeeperEvent(args: {
    height: number
    txIndex: number
    txMsgIndex: number
    hash: string
    type: string
    attributes: Record<string, string>
  }): Promise<void> {
    txHashes.push(args.hash)
    const txId = nextId++
    const eventId = nextId++
    const [tx] = await knex('transaction')
      .insert({
        id: txId,
        height: args.height,
        hash: args.hash,
        codespace: '',
        code: 0,
        gas_used: 1,
        gas_wanted: 1,
        gas_limit: 1,
        fee: {},
        timestamp: new Date('2025-01-15T10:30:00Z'),
        data: {},
        index: args.txIndex,
      })
      .returning('id')
    await knex('event').insert({
      id: eventId,
      tx_id: typeof tx === 'object' ? tx.id : tx,
      tx_msg_index: args.txMsgIndex,
      type: args.type,
      block_height: args.height,
      source: 'TX_EVENT',
    })
    const attributeRows = Object.entries(args.attributes).map(([key, value], index) => ({
      event_id: eventId,
      index,
      block_height: args.height,
      composite_key: `${args.type}.${key}`,
      key,
      value,
    }))
    if (attributeRows.length > 0) await knex('event_attribute').insert(attributeRows)
  }

  it('enriches GrantOperatorAuthorization with corporation and grantee (gap 2)', async () => {
    const height = baseHeight + 200
    const corpId = 51_000 + Math.floor(Math.random() * 1000)
    const corpDid = `did:web:corp-${runId}.example`
    const granteeParticipantDid = `did:web:grantee-${runId}.example`
    await insertBlock(height)
    await insertCorporation({ id: corpId, did: corpDid, policyAddress: 'verana1corp' })
    await insertParticipant({
      id: nextId++,
      did: granteeParticipantDid,
      corporationId: corpId,
      vsOperator: 'verana1grantee',
    })
    await insertDelegationMessage({
      height,
      txIndex: 0,
      messageIndex: 0,
      hash: `tx-${runId}-oa`,
      type: VeranaDelegationMessageTypes.GrantOperatorAuthorization,
      content: {
        corporation: 'verana1corp',
        operator: '',
        grantee: 'verana1grantee',
        msgTypes: ['/verana.pp.v1.MsgStartParticipantOP'],
        withFeegrant: true,
      },
    })

    const [record] = await persistIndexerEventsForBlock(height)

    expect(record.event_type).toBe('GrantOperatorAuthorization')
    expect(record.payload.module).toBe('de')
    expect(record.payload.corporation_id).toBe(corpId)
    expect(record.payload.grantee).toBe('verana1grantee')
    expect(record.payload.with_feegrant).toBe(true)
    expect(record.did).toBe(corpDid)
    expect(record.payload.related_dids).toEqual(expect.arrayContaining([corpDid, granteeParticipantDid]))
  })

  it('forwards VSOA keeper events with vs_operator/participant/corporation (gap 1)', async () => {
    const height = baseHeight + 220
    const corpId = 53_000 + Math.floor(Math.random() * 1000)
    const participantId = nextId++
    const participantDid = `did:web:vsoa-${runId}.example`
    await insertBlock(height)
    await insertParticipant({ id: participantId, did: participantDid, corporationId: corpId })
    await insertKeeperEvent({
      height,
      txIndex: 0,
      txMsgIndex: 0,
      hash: `tx-${runId}-vsoa`,
      type: 'grant_vs_operator_authorization',
      attributes: {
        vs_operator: 'verana1vsoperator',
        participant_id: String(participantId),
        corporation_id: String(corpId),
      },
    })

    const [record] = await persistIndexerEventsForBlock(height)

    expect(record.event_type).toBe('GrantVSOperatorAuthorization')
    expect(record.payload.module).toBe('de')
    expect(record.did).toBe(participantDid)
    expect(record.payload.participant_id).toBe(String(participantId))
    expect(record.payload.corporation_id).toBe(corpId)
    expect(record.payload.vs_operator).toBe('verana1vsoperator')

    const byCorp = await listIndexerEvents({ corporationId: corpId, afterBlockHeight: height - 1, limit: 10 })
    expect(byCorp.map((event) => event.event_type)).toContain('GrantVSOperatorAuthorization')
  })

  it('persists v4 payload values while keeping event_type as the Cosmos action name', async () => {
    const height = baseHeight + 160
    await insertBlock(height)
    await insertTxMessage({ height, txIndex: 0, messageIndex: 0, hash: `tx-${runId}-v4-values` })

    const [record] = await persistIndexerEventsForBlock(height)

    expect(record.event_type).toBe('StartParticipantOP')
    expect(record.payload.module).toBe('pp')
    expect(record.payload.action).toBe('start_participant_op')
    expect(record.payload.message_type).toBe('MsgStartParticipantOP')
    expect(record.payload.entity_type).toBe('Participant')
  })
})
