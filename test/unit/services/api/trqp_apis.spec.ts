jest.mock('../../../../src/common/utils/db_connection', () => ({ __esModule: true, default: jest.fn() }))

import { existsSync, readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { ServiceBroker } from 'moleculer'
import knex from '../../../../src/common/utils/db_connection'
import { Network } from '../../../../src/network'
import TrqpApiService from '../../../../src/services/api/trqp_apis.service'

const PUBLISHED_PROFILE_PATH = resolve(process.cwd(), 'docs/api/schemas/v4/trqp/profile.json')

describe('Verana TRQP profile descriptor', () => {
  const broker = new ServiceBroker({ logger: false })
  const service = new TrqpApiService(broker)

  it('is published where the spec links it', () => {
    expect(existsSync(PUBLISHED_PROFILE_PATH)).toBe(true)
  })

  it('serves a body byte-identical to the published file, trailing newline included', async () => {
    const published = readFileSync(PUBLISHED_PROFILE_PATH, 'utf8')
    const ctx: any = { meta: {} }

    const body = await service.getProfile(ctx)

    expect(body).toBe(published)
    expect(body.endsWith('}\n')).toBe(true)
    expect(ctx.meta.$rawJsonResponse).toBe(true)
  })

  it('declares the endpoint paths with the /v4 prefix', () => {
    const descriptor = JSON.parse(readFileSync(PUBLISHED_PROFILE_PATH, 'utf8'))

    expect(descriptor.endpoints).toEqual({
      authorization: '/v4/trqp/v2/authorization',
      recognition: '/v4/trqp/v2/recognition',
      profile: '/v4/trqp/v2/profile',
    })
    expect(descriptor.discovery.did_document_service_endpoint_base_path).toBe('/v4/trqp/v2/')
    expect(descriptor.discovery.profile_document_path).toBe('/v4/trqp/v2/profile')
  })

  it('keeps the $id and profile version the spec names', () => {
    const descriptor = JSON.parse(readFileSync(PUBLISHED_PROFILE_PATH, 'utf8'))

    expect(descriptor.$id).toBe('https://verana.io/schemas/v4/trqp/profile.json')
    expect(descriptor.profile_version).toBe('verana-trqp/spec-v4')
  })
})

describe('TRQP context.session_id', () => {
  const broker = new ServiceBroker({ logger: false })
  const service = new TrqpApiService(broker)
  const SESSION_ID = '5a1d3c9e-0000-4000-8000-000000000001'
  const SESSION_CREATED = new Date('2026-07-01T10:00:00.250Z')
  const ECOSYSTEM_DID = 'did:web:ecosystem.example'
  const CORPORATION_DID = 'did:web:corp.example'
  const RESOURCE = `vpr:verana:${Network.chainId}:cs:7`

  type TrqpAnswer = { authorized?: boolean; recognized?: boolean; message?: string; verana?: unknown }
  type TrqpContext = Parameters<TrqpApiService['authorize']>[0]

  function fakeTable(result: { first?: unknown; rows?: unknown[] }) {
    const builder = {
      select: jest.fn(),
      join: jest.fn(),
      where: jest.fn(),
      whereNull: jest.fn(),
      orderBy: jest.fn(),
      first: jest.fn(async () => result.first),
      then: (resolve: (rows: unknown[]) => unknown, reject: (err: unknown) => unknown) =>
        Promise.resolve(result.rows ?? []).then(resolve, reject),
    }
    for (const chained of [builder.select, builder.join, builder.where, builder.whereNull, builder.orderBy]) {
      chained.mockReturnValue(builder)
    }
    return builder
  }

  function mockTables(session: { created: Date } | undefined) {
    const tables: Record<string, ReturnType<typeof fakeTable>> = {
      block: fakeTable({ first: { time: '2026-09-01T12:00:00Z' } }),
      corporation: fakeTable({ first: { id: 3 } }),
      participant_sessions: fakeTable({ first: session }),
      'participants as p': fakeTable({
        rows: [
          {
            role: 'ISSUER',
            deposit: 0,
            modified: null,
            effective_from: '2026-01-01T00:00:00Z',
            effective_until: null,
            revoked: null,
            slashed: null,
            repaid: null,
          },
        ],
      }),
      ecosystem: fakeTable({ first: { id: 9, corporation_id: 3, created: '2026-01-01T00:00:00Z', active_version: 1 } }),
      credential_schemas: fakeTable({ first: { id: 7 } }),
    }
    ;(knex as unknown as jest.Mock).mockImplementation((table: string) => tables[table])
    return tables
  }

  function query(method: 'authorize' | 'recognize', context: Record<string, string>): Promise<TrqpAnswer> {
    const [authorityId, entityId] =
      method === 'authorize' ? [ECOSYSTEM_DID, CORPORATION_DID] : [CORPORATION_DID, ECOSYSTEM_DID]
    const ctx = {
      meta: {},
      params: { authority_id: authorityId, entity_id: entityId, action: 'issue', resource: RESOURCE, context },
    } as unknown as TrqpContext
    return service[method](ctx) as Promise<TrqpAnswer>
  }

  it('denies an unknown session with session not found', async () => {
    mockTables(undefined)

    const res = await query('authorize', { session_id: SESSION_ID, time: '2026-08-01T00:00:00Z' })

    expect(res).toMatchObject({ authorized: false, message: 'session not found' })
  })

  it('denies a time earlier than the session creation with session out of window', async () => {
    const tables = mockTables({ created: SESSION_CREATED })

    const res = await query('authorize', { session_id: SESSION_ID, time: '2026-07-01T10:00:00Z' })

    expect(res).toMatchObject({ authorized: false, message: 'session out of window' })
    expect(res.verana).toBeUndefined()
    expect(tables.corporation.where).toHaveBeenCalledWith({ did: CORPORATION_DID })
    expect(tables.participant_sessions.where).toHaveBeenCalledWith({ id: SESSION_ID, corporation_id: 3 })
    expect(tables['participants as p'].where).not.toHaveBeenCalled()
  })

  it('evaluates a time at or after the session creation', async () => {
    mockTables({ created: SESSION_CREATED })

    const atCreation = await query('authorize', { session_id: SESSION_ID, time: '2026-07-01T10:00:00.250Z' })
    const later = await query('authorize', { session_id: SESSION_ID, time: '2027-01-01T00:00:00Z' })

    expect(atCreation).toMatchObject({ authorized: true })
    expect(atCreation.message).toBeUndefined()
    expect(later).toMatchObject({ authorized: true })
  })

  it('does not check the window when time is omitted', async () => {
    mockTables({ created: new Date(Date.now() + 60_000) })

    const res = await query('authorize', { session_id: SESSION_ID })

    expect(res).toMatchObject({ authorized: true })
  })

  it('applies the same window to recognition', async () => {
    const tables = mockTables({ created: SESSION_CREATED })

    const before = await query('recognize', { session_id: SESSION_ID, time: '2026-06-30T00:00:00Z' })
    const after = await query('recognize', { session_id: SESSION_ID, time: '2026-07-02T00:00:00Z' })

    expect(before).toMatchObject({ recognized: false, message: 'session out of window' })
    expect(after).toMatchObject({ recognized: true })
    expect(after.message).toBeUndefined()
    expect(tables.corporation.where).not.toHaveBeenCalledWith({ did: ECOSYSTEM_DID })
    expect(tables.participant_sessions.where).toHaveBeenCalledWith({ id: SESSION_ID, corporation_id: 3 })
  })
})
