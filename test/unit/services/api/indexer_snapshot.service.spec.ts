import { ServiceBroker } from 'moleculer'
import knex from '../../../../src/common/utils/db_connection'
import IndexerSnapshotService, { getDidSnapshotAtHeight } from '../../../../src/services/api/snapshot.service'

describe('IndexerSnapshotService snapshot endpoint', () => {
  const runId = `${Date.now()}-${Math.floor(Math.random() * 10000)}`
  const baseHeight = 7_500_000 + Math.floor(Math.random() * 10_000)
  const didA = `did:web:snapshot-a-${runId}.example`
  const otherDid = `did:web:snapshot-other-${runId}.example`

  const createdAt = new Date('2026-01-15T10:30:00Z')

  let nextEntityId = 90_000_000 + Math.floor(Math.random() * 1_000_000)
  function allocateEntityId(): number {
    nextEntityId += 1
    return nextEntityId
  }

  const inserted = {
    ecosystemIds: [] as number[],
    credentialSchemaIds: [] as number[],
    participantIds: [] as number[],
  }

  const createdTables: string[] = []
  const columnInfoCache = new Map<string, Promise<Record<string, any>>>()

  async function getColumnInfo(table: string): Promise<Record<string, any>> {
    const cached = columnInfoCache.get(table)
    if (cached) return cached
    const info = knex(table).columnInfo()
    columnInfoCache.set(table, info)
    return info
  }

  async function insertRow(table: string, row: Record<string, any>): Promise<void> {
    const info = await getColumnInfo(table)
    const filtered: Record<string, any> = {}
    for (const [column, value] of Object.entries(row)) {
      if (Object.hasOwn(info, column)) filtered[column] = value
    }
    await knex(table).insert(filtered)
  }

  beforeAll(async () => {
    if (!(await knex.schema.hasTable('credential_schemas'))) {
      await knex.schema.createTable('credential_schemas', (table) => {
        table.increments('id').primary()
        table.integer('ecosystem_id').notNullable()
        table.text('json_schema').notNullable()
        table.boolean('is_active').notNullable().defaultTo(false)
      })
      createdTables.push('credential_schemas')
    }

    if (!(await knex.schema.hasTable('ecosystem_history'))) {
      await knex.schema.createTable('ecosystem_history', (table) => {
        table.bigIncrements('id').primary()
        table.bigInteger('ecosystem_id').notNullable()
        table.string('did').notNullable()
        table.bigInteger('corporation_id').notNullable().defaultTo(0)
        table.timestamp('created').notNullable()
        table.timestamp('modified').notNullable()
        table.timestamp('archived').nullable()
        table.string('aka').nullable()
        table.string('language', 2).notNullable()
        table.integer('active_version').nullable()
        table.text('event_type').notNullable()
        table.bigInteger('height').notNullable()
        table.jsonb('changes').nullable()
        table.timestamp('created_at').defaultTo(knex.fn.now())
      })
      createdTables.push('ecosystem_history')
    }

    if (!(await knex.schema.hasTable('credential_schema_history'))) {
      await knex.schema.createTable('credential_schema_history', (table) => {
        table.increments('id').primary()
        table.integer('credential_schema_id').notNullable()
        table.integer('ecosystem_id').notNullable()
        table.text('json_schema').notNullable()
        table.timestamp('archived').nullable()
        table.boolean('is_active').notNullable().defaultTo(false)
        table.timestamp('created').notNullable()
        table.timestamp('modified').notNullable()
        table.jsonb('changes').nullable()
        table.string('action').notNullable()
        table.bigInteger('height').notNullable().defaultTo(0)
        table.timestamp('created_at').defaultTo(knex.fn.now())
      })
      createdTables.push('credential_schema_history')
    }

    if (!(await knex.schema.hasTable('participant_history'))) {
      await knex.schema.createTable('participant_history', (table) => {
        table.increments('id').primary()
        table.integer('participant_id').notNullable()
        table.integer('schema_id').notNullable()
        table.string('role').nullable()
        table.string('did', 255).nullable()
        table.bigInteger('corporation_id').notNullable().defaultTo(0)
        table.timestamp('created').nullable()
        table.timestamp('modified').nullable()
        table.timestamp('revoked').nullable()
        table.integer('height').notNullable()
        table.timestamp('created_at').defaultTo(knex.fn.now())
      })
      createdTables.push('participant_history')
    }
  })

  afterAll(async () => {
    for (const table of createdTables.reverse()) {
      // eslint-disable-next-line no-await-in-loop
      await knex.schema.dropTableIfExists(table)
    }
  })

  async function insertEcosystemHistory(args: {
    ecosystemId: number
    did: string
    corporationId: number
    height: number
    eventType?: string
    archived?: Date | null
  }): Promise<number> {
    await insertRow('ecosystem_history', {
      ecosystem_id: args.ecosystemId,
      did: args.did,
      corporation_id: args.corporationId,
      created: createdAt,
      modified: createdAt,
      archived: args.archived ?? null,
      aka: null,
      language: 'en',
      active_version: null,
      event_type: args.eventType ?? 'CreateNewEcosystem',
      height: args.height,
      changes: null,
      created_at: createdAt,
    })
    if (!inserted.ecosystemIds.includes(args.ecosystemId)) inserted.ecosystemIds.push(args.ecosystemId)
    return args.ecosystemId
  }

  async function insertCredentialSchemaParent(args: {
    credentialSchemaId: number
    ecosystemId: number
  }): Promise<void> {
    const existing = await knex('credential_schemas').where('id', args.credentialSchemaId).first()
    if (existing) return
    await insertRow('credential_schemas', {
      id: args.credentialSchemaId,
      ecosystem_id: args.ecosystemId,
      tr_id: args.ecosystemId,
      json_schema: JSON.stringify({ $id: `schema-${args.credentialSchemaId}` }),
      is_active: true,
      issuer_grantor_validation_validity_period: 365,
      verifier_grantor_validation_validity_period: 365,
      issuer_validation_validity_period: 365,
      verifier_validation_validity_period: 365,
      holder_validation_validity_period: 365,
      issuer_onboarding_mode: 'OPEN',
      verifier_onboarding_mode: 'OPEN',
      holder_onboarding_mode: 'PERMISSIONLESS',
    })
  }

  async function insertCredentialSchemaHistory(args: {
    credentialSchemaId: number
    ecosystemId: number
    height: number
    action?: string
    archived?: Date | null
  }): Promise<number> {
    await insertCredentialSchemaParent({
      credentialSchemaId: args.credentialSchemaId,
      ecosystemId: args.ecosystemId,
    })
    await insertRow('credential_schema_history', {
      credential_schema_id: args.credentialSchemaId,
      ecosystem_id: args.ecosystemId,
      json_schema: JSON.stringify({ $id: `schema-${args.credentialSchemaId}` }),
      issuer_grantor_validation_validity_period: 365,
      verifier_grantor_validation_validity_period: 365,
      issuer_validation_validity_period: 365,
      verifier_validation_validity_period: 365,
      holder_validation_validity_period: 365,
      issuer_onboarding_mode: 'OPEN',
      verifier_onboarding_mode: 'OPEN',
      holder_onboarding_mode: 'PERMISSIONLESS',
      archived: args.archived ?? null,
      is_active: !args.archived,
      created: createdAt,
      modified: createdAt,
      changes: null,
      action: args.action ?? 'CreateNewCredentialSchema',
      height: args.height,
      created_at: createdAt,
    })
    if (!inserted.credentialSchemaIds.includes(args.credentialSchemaId)) {
      inserted.credentialSchemaIds.push(args.credentialSchemaId)
    }
    return args.credentialSchemaId
  }

  async function insertParticipantHistory(args: {
    participantId: number
    schemaId: number
    did?: string | null
    corporationId: number
    height: number
    eventType?: string
    revoked?: Date | null
  }): Promise<number> {
    await insertRow('participant_history', {
      participant_id: args.participantId,
      schema_id: args.schemaId,
      role: 'ISSUER',
      type: 'ISSUER',
      did: args.did ?? null,
      corporation_id: args.corporationId,
      created: createdAt,
      modified: createdAt,
      revoked: args.revoked ?? null,
      event_type: args.eventType ?? 'CreateRootParticipant',
      height: args.height,
      created_at: createdAt,
    })
    if (!inserted.participantIds.includes(args.participantId)) inserted.participantIds.push(args.participantId)
    return args.participantId
  }

  afterEach(async () => {
    if (inserted.participantIds.length > 0) {
      await knex('participant_history').whereIn('participant_id', inserted.participantIds).delete()
      inserted.participantIds.length = 0
    }
    if (inserted.credentialSchemaIds.length > 0) {
      await knex('credential_schema_history').whereIn('credential_schema_id', inserted.credentialSchemaIds).delete()
      await knex('credential_schemas').whereIn('id', inserted.credentialSchemaIds).delete()
      inserted.credentialSchemaIds.length = 0
    }
    if (inserted.ecosystemIds.length > 0) {
      await knex('ecosystem_history').whereIn('ecosystem_id', inserted.ecosystemIds).delete()
      inserted.ecosystemIds.length = 0
    }
  })

  function buildService() {
    const broker = new ServiceBroker({ logger: false })
    const svc = new IndexerSnapshotService(broker as any)
    ;(svc as any).logger = { error: () => {} }
    return svc
  }

  it('returns 400 for missing did', async () => {
    const svc = buildService()
    const ctx: any = { params: {}, meta: {} }
    const res: any = await svc.getSnapshot(ctx)
    expect(res).toMatchObject({ code: 400 })
    expect(String(res.error)).toContain('Missing did')
  })

  it('returns 400 for invalid did', async () => {
    const svc = buildService()
    const ctx: any = { params: { did: 'not-a-did' }, meta: {} }
    const res: any = await svc.getSnapshot(ctx)
    expect(res).toMatchObject({ code: 400 })
    expect(String(res.error)).toContain('Invalid did')
  })

  it('uses the block height from the At-Block-Height header (ctx.meta.blockHeight)', async () => {
    const svc = buildService()
    await insertEcosystemHistory({ ecosystemId: allocateEntityId(), did: didA, corporationId: 1, height: baseHeight })

    const ctx: any = { params: { did: didA }, meta: { blockHeight: baseHeight } }
    const res: any = await svc.getSnapshot(ctx)
    expect(res).toMatchObject({ did: didA, block_height: baseHeight })
    expect(res.count.ecosystems).toBe(1)
  })

  it('defaults to the latest indexed block when At-Block-Height header is omitted', async () => {
    const svc = buildService()
    const ctx: any = { params: { did: didA }, meta: {} }
    const res: any = await svc.getSnapshot(ctx)
    expect(res.code).toBeUndefined()
    expect(res.did).toBe(didA)
    expect(Number.isInteger(res.block_height)).toBe(true)
    expect(res.block_height).toBeGreaterThanOrEqual(0)
  })

  it('returns empty arrays for unknown DID', async () => {
    const snap = await getDidSnapshotAtHeight({ did: didA, blockHeight: baseHeight })
    expect(snap).toMatchObject({
      did: didA,
      block_height: baseHeight,
      count: { ecosystems: 0, schemas: 0, participants: 0 },
    })
    expect(snap.ecosystems).toEqual([])
    expect(snap.schemas).toEqual([])
    expect(snap.participants).toEqual([])
  })

  it('returns DID-linked snapshot objects reconstructed from history', async () => {
    const ecosystemId = await insertEcosystemHistory({
      ecosystemId: allocateEntityId(),
      did: didA,
      corporationId: 1,
      height: baseHeight,
    })
    const schemaId = await insertCredentialSchemaHistory({
      credentialSchemaId: allocateEntityId(),
      ecosystemId,
      height: baseHeight,
    })
    await insertParticipantHistory({
      participantId: allocateEntityId(),
      schemaId: schemaId + 100000,
      did: didA,
      corporationId: 999,
      height: baseHeight,
    })

    const snap = await getDidSnapshotAtHeight({ did: didA, blockHeight: baseHeight })
    expect(snap.count.ecosystems).toBe(1)
    expect(snap.count.schemas).toBe(1)
    expect(snap.count.participants).toBe(1)
    expect(Number(snap.ecosystems[0]?.id)).toBe(ecosystemId)
    expect(snap.ecosystems[0]?.did).toBe(didA)
    expect(snap.participants[0]?.did).toBe(didA)
  })

  it('projects the entity id onto id and drops history bookkeeping columns', async () => {
    const ecosystemId = await insertEcosystemHistory({
      ecosystemId: allocateEntityId(),
      did: didA,
      corporationId: 1,
      height: baseHeight,
    })

    const snap = await getDidSnapshotAtHeight({ did: didA, blockHeight: baseHeight })
    const ecosystem = snap.ecosystems[0] as Record<string, unknown>
    expect(Number(ecosystem.id)).toBe(ecosystemId)
    expect(ecosystem).not.toHaveProperty('ecosystem_id')
    expect(ecosystem).not.toHaveProperty('event_type')
    expect(ecosystem).not.toHaveProperty('changes')
    expect(ecosystem).not.toHaveProperty('created_at')
  })

  it('excludes entities created after the requested block', async () => {
    await insertEcosystemHistory({
      ecosystemId: allocateEntityId(),
      did: didA,
      corporationId: 1,
      height: baseHeight + 100,
    })

    const snap = await getDidSnapshotAtHeight({ did: didA, blockHeight: baseHeight })
    expect(snap.count.ecosystems).toBe(0)
  })

  it('returns the latest state at or before the requested block', async () => {
    const ecosystemId = allocateEntityId()
    await insertEcosystemHistory({ ecosystemId, did: didA, corporationId: 1, height: baseHeight })
    await insertEcosystemHistory({
      ecosystemId,
      did: didA,
      corporationId: 7,
      height: baseHeight + 50,
      eventType: 'UpdateEcosystem',
    })

    const atCreation = await getDidSnapshotAtHeight({ did: didA, blockHeight: baseHeight })
    expect(Number(atCreation.ecosystems[0]?.corporation_id)).toBe(1)

    const afterUpdate = await getDidSnapshotAtHeight({ did: didA, blockHeight: baseHeight + 50 })
    expect(afterUpdate.count.ecosystems).toBe(1)
    expect(Number(afterUpdate.ecosystems[0]?.corporation_id)).toBe(7)
  })

  it('reports an ecosystem archived after the requested block as not archived', async () => {
    const ecosystemId = allocateEntityId()
    await insertEcosystemHistory({ ecosystemId, did: didA, corporationId: 1, height: baseHeight })
    await insertEcosystemHistory({
      ecosystemId,
      did: didA,
      corporationId: 1,
      height: baseHeight + 100,
      eventType: 'ArchiveEcosystem',
      archived: new Date('2026-02-01T00:00:00Z'),
    })

    const beforeArchival = await getDidSnapshotAtHeight({ did: didA, blockHeight: baseHeight + 50 })
    expect(beforeArchival.count.ecosystems).toBe(1)
    expect(beforeArchival.ecosystems[0]?.archived).toBeNull()

    const afterArchival = await getDidSnapshotAtHeight({ did: didA, blockHeight: baseHeight + 100 })
    expect(afterArchival.count.ecosystems).toBe(1)
    expect(afterArchival.ecosystems[0]?.archived).not.toBeNull()
  })

  it('returns schema-linked participants', async () => {
    const ecosystemId = await insertEcosystemHistory({
      ecosystemId: allocateEntityId(),
      did: didA,
      corporationId: 1,
      height: baseHeight,
    })
    const schemaId = await insertCredentialSchemaHistory({
      credentialSchemaId: allocateEntityId(),
      ecosystemId,
      height: baseHeight,
    })
    await insertParticipantHistory({
      participantId: allocateEntityId(),
      schemaId,
      did: otherDid,
      corporationId: 999,
      height: baseHeight,
    })

    const snap = await getDidSnapshotAtHeight({ did: didA, blockHeight: baseHeight })
    expect(snap.count.participants).toBe(1)
    expect(Number(snap.participants[0]?.schema_id)).toBe(schemaId)
    expect(snap.participants[0]?.did).toBe(otherDid)
  })

  it('returns corporation-linked participants from derived corporation_id', async () => {
    const corporationId = 4242
    const ecosystemId = await insertEcosystemHistory({
      ecosystemId: allocateEntityId(),
      did: didA,
      corporationId,
      height: baseHeight,
    })
    const schemaId = await insertCredentialSchemaHistory({
      credentialSchemaId: allocateEntityId(),
      ecosystemId,
      height: baseHeight,
    })
    await insertParticipantHistory({
      participantId: allocateEntityId(),
      schemaId: schemaId + 100000,
      did: null,
      corporationId,
      height: baseHeight,
    })

    const snap = await getDidSnapshotAtHeight({ did: didA, blockHeight: baseHeight })
    expect(snap.count.participants).toBe(1)
    expect(Number(snap.participants[0]?.corporation_id)).toBe(corporationId)
    expect(Number(snap.participants[0]?.schema_id)).not.toBe(schemaId)
  })

  it('returns participants linked only through the DID even when no ecosystem matches', async () => {
    await insertParticipantHistory({
      participantId: allocateEntityId(),
      schemaId: allocateEntityId(),
      did: didA,
      corporationId: 555,
      height: baseHeight,
    })

    const snap = await getDidSnapshotAtHeight({ did: didA, blockHeight: baseHeight })
    expect(snap.count.ecosystems).toBe(0)
    expect(snap.count.participants).toBe(1)
    expect(snap.participants[0]?.did).toBe(didA)
  })
})
