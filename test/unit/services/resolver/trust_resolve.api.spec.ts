import { ServiceBroker } from 'moleculer'

const TrustResolutionOutcome = {
  VERIFIED: 'verified',
  VERIFIED_TEST: 'verified-test',
  NOT_TRUSTED: 'not-trusted',
  INVALID: 'invalid',
} as const

jest.mock(
  '@verana-labs/verre',
  () => ({
    __esModule: true,
    resolveDID: jest.fn(async () => ({ verified: true })),
    verifyParticipants: jest.fn(async () => ({ verified: true })),
    ParticipantType: { ISSUER: 'ISSUER', VERIFIER: 'VERIFIER' },
    TrustResolutionOutcome,
  }),
  { virtual: true }
)

jest.mock('../../../../src/models', () => ({
  __esModule: true,
  BlockCheckpoint: {
    query: () => ({
      where: jest.fn().mockReturnThis(),
      first: jest.fn().mockResolvedValue({ height: 10 }),
    }),
  },
}))

jest.mock('canonicalize', () => ({ __esModule: true, default: (obj: any) => JSON.stringify(obj) }), { virtual: true })

jest.mock('../../../../src/common/utils/db_connection', () => {
  const chain: any = {}
  chain.select = jest.fn(() => chain)
  chain.where = jest.fn(() => chain)
  chain.whereNull = jest.fn(() => chain)
  chain.whereNotNull = jest.fn(() => chain)
  chain.whereIn = jest.fn(() => chain)
  chain.orderBy = jest.fn(() => chain)
  chain.limit = jest.fn(async () => [])
  chain.first = jest.fn(async () => ({ height: 9 }))

  const knexMock: any = jest.fn((table?: string) => {
    if (table === 'credential_schemas') {
      const listChain: any = {}
      listChain.select = jest.fn(() => listChain)
      listChain.whereNull = jest.fn(() => listChain)
      listChain.limit = jest.fn(async () => [{ id: 1 }])

      const schemaChain: any = {}
      schemaChain.select = jest.fn(() => schemaChain)
      schemaChain.where = jest.fn(() => schemaChain)
      schemaChain.first = jest.fn(async () => ({
        json_schema: {
          digest_algorithm: 'sha256',
          $id: 'https://example.com/schemas/ecs-service/v1',
        },
      }))

      return {
        select: (...cols: any[]) => {
          if (cols.length === 1 && cols[0] === 'id') return listChain
          return schemaChain
        },
      }
    }
    if (table === 'block') return chain
    return chain
  })
  knexMock.raw = jest.fn(async () => ({ rows: [] }))
  return knexMock
})

describe('TrustV1ApiService POST /v4/verifiable-trust/resolve (resolveV4)', () => {
  let broker: ServiceBroker
  let service: any

  beforeAll(async () => {
    const { TrustV1ApiService } = await import('../../../../src/services/resolver/trust-api.service')
    broker = new ServiceBroker({ logger: false })
    service = broker.createService(TrustV1ApiService)
  })

  afterEach(() => {
    jest.restoreAllMocks()
  })

  function mockStoredRow(overrides: Record<string, unknown> = {}) {
    return {
      did: 'did:verana:test123',
      height: 10,
      resolve_result: { verified: true, outcome: TrustResolutionOutcome.VERIFIED },
      created_at: '2026-01-01T00:00:00Z',
      ...overrides,
    } as any
  }

  it('returns trust-core fields in the normative camelCase shape', async () => {
    const TrustResolve = await import('../../../../src/services/resolver/trust-resolve')
    jest.spyOn(TrustResolve, 'getTrustResultLatestByDidAtOrBeforeHeight').mockResolvedValue(mockStoredRow())

    const ctx: any = { params: { did: 'did:verana:test123' }, meta: {} }
    const res = await service.resolveV4(ctx)

    expect(TrustResolve.getTrustResultLatestByDidAtOrBeforeHeight).toHaveBeenCalledWith('did:verana:test123', 10)
    expect(ctx.meta.$statusCode).toBe(200)
    expect(res).toMatchObject({
      did: 'did:verana:test123',
      trusted: true,
      evaluatedAtBlock: 10,
      corporationId: 0,
    })
    expect(typeof res.evaluatedAtTime).toBe('string')
    expect(typeof res.expiresAtTime).toBe('string')
    // legacy fields must be gone
    expect(res.trust_status).toBeUndefined()
    expect(res.evaluated_at_block).toBeUndefined()
    // opt-in sections excluded by default
    expect(res.participations).toBeUndefined()
    expect(res.ecosystems).toBeUndefined()
  })

  it('trusted is true for PARTIAL (verified-test) outcomes', async () => {
    const TrustResolve = await import('../../../../src/services/resolver/trust-resolve')
    jest
      .spyOn(TrustResolve, 'getTrustResultLatestByDidAtOrBeforeHeight')
      .mockResolvedValue(
        mockStoredRow({ resolve_result: { verified: true, outcome: TrustResolutionOutcome.VERIFIED_TEST } })
      )

    const ctx: any = { params: { did: 'did:verana:test123' }, meta: {} }
    const res = await service.resolveV4(ctx)
    expect(res.trusted).toBe(true)
  })

  it('uses the At-Block-Height header (ctx.meta.blockHeight), clamped to last trust block', async () => {
    const TrustResolve = await import('../../../../src/services/resolver/trust-resolve')
    const spy = jest
      .spyOn(TrustResolve, 'getTrustResultLatestByDidAtOrBeforeHeight')
      .mockResolvedValue(mockStoredRow({ height: 5 }))

    const ctx: any = { params: { did: 'did:verana:test123' }, meta: { blockHeight: 5 } }
    await service.resolveV4(ctx)
    expect(spy).toHaveBeenCalledWith('did:verana:test123', 5)
  })

  it('includes participations only when selected, parsing the states filter', async () => {
    const TrustResolve = await import('../../../../src/services/resolver/trust-resolve')
    jest.spyOn(TrustResolve, 'getTrustResultLatestByDidAtOrBeforeHeight').mockResolvedValue(mockStoredRow())
    const Builders = await import('../../../../src/services/resolver/trust-resolve-v4.builders')
    const partSpy = jest
      .spyOn(Builders, 'buildParticipations')
      .mockResolvedValue([{ id: 501, role: 'ISSUER', state: 'ACTIVE' } as any])

    const ctx: any = {
      params: { did: 'did:verana:test123', participations: { states: ['ACTIVE', 'REVOKED'] } },
      meta: {},
    }
    const res = await service.resolveV4(ctx)

    expect(partSpy).toHaveBeenCalledWith('did:verana:test123', expect.any(Date), ['ACTIVE', 'REVOKED'], undefined)
    expect(res.participations).toEqual([{ id: 501, role: 'ISSUER', state: 'ACTIVE' }])
  })

  it('includes ecosystems only when selected', async () => {
    const TrustResolve = await import('../../../../src/services/resolver/trust-resolve')
    jest.spyOn(TrustResolve, 'getTrustResultLatestByDidAtOrBeforeHeight').mockResolvedValue(mockStoredRow())
    const Builders = await import('../../../../src/services/resolver/trust-resolve-v4.builders')
    const ecoSpy = jest
      .spyOn(Builders, 'buildEcosystems')
      .mockResolvedValue([{ id: 1234, corporationId: 0, archived: false } as any])

    const ctx: any = { params: { did: 'did:verana:test123', ecosystems: true }, meta: {} }
    const res = await service.resolveV4(ctx)

    expect(ecoSpy).toHaveBeenCalledWith(
      'did:verana:test123',
      {
        includeArchived: false,
        credentialSchemas: { include: false, includeArchived: false },
      },
      undefined
    )
    expect(res.ecosystems).toEqual([{ id: 1234, corporationId: 0, archived: false }])
  })

  it('rejects a non-DID parameter with 400', async () => {
    const ctx: any = { params: { did: 'not-a-did' }, meta: {} }
    const res = await service.resolveV4(ctx)
    expect(ctx.meta.$statusCode).toBe(400)
    expect(res.error).toMatch(/did:/)
  })
})
