const TrustResolutionOutcome = {
  VERIFIED: 'verified',
  VERIFIED_TEST: 'verified-test',
  NOT_TRUSTED: 'not-trusted',
  INVALID: 'invalid',
} as const

const resolveDIDMock = jest.fn()

jest.mock(
  '@verana-labs/verre',
  () => ({
    __esModule: true,
    resolveDID: (...args: unknown[]) => resolveDIDMock(...args),
    TrustResolutionOutcome,
  }),
  { virtual: true }
)

jest.mock('../../../../src/config.json', () => {
  const actual = jest.requireActual('../../../../src/config.json')
  return {
    __esModule: true,
    default: {
      ...actual,
      resolver: {
        ...actual.resolver,
        enabled: true,
        trustEvaluationTtlSeconds: 3600,
        pollObjectCachingRetryDays: 7,
      },
    },
  }
})

jest.mock('../../../../src/common/utils/start_mode_detector', () => ({
  __esModule: true,
  detectStartMode: async () => ({ isFreshStart: false }),
}))

jest.mock('../../../../src/services/resolver/ecs-allowlist', () => ({
  __esModule: true,
  isEcsAllowlistEnforced: () => false,
}))

jest.mock('../../../../src/services/resolver/trust-resolve-v4.builders', () => ({
  __esModule: true,
  hasAllowlistedEcsServiceCredential: async () => true,
}))

jest.mock('../../../../src/services/resolver/verre-registry-adapter', () => ({
  __esModule: true,
  attachRegistryAdapters: (registries: unknown) => registries,
}))

type Row = Record<string, any>
const store: Record<string, Row[]> = { trust_results: [], trust_reattemptable: [] }

jest.mock('../../../../src/common/utils/db_connection', () => {
  type Cond = { key: string; op: string; value: any }

  const makeChain = (table: string) => {
    const conds: Cond[] = []
    let order: { key: string; dir: string } | null = null

    const matching = () => {
      let rows = store[table].filter((row) =>
        conds.every((c) => {
          if (c.op === '=') return row[c.key] === c.value
          if (c.op === '<=') return Number(row[c.key]) <= Number(c.value)
          return true
        })
      )
      if (order) {
        const { key, dir } = order
        rows = [...rows].sort((a, b) =>
          dir === 'desc' ? Number(b[key]) - Number(a[key]) : Number(a[key]) - Number(b[key])
        )
      }
      return rows
    }

    const chain: any = {}
    chain.select = () => chain
    chain.whereNotNull = () => chain
    chain.limit = () => chain
    chain.orderBy = (key: string, dir = 'asc') => {
      order = { key, dir }
      return chain
    }
    chain.where = (a: any, b?: any, c?: any) => {
      if (a && typeof a === 'object') {
        for (const [key, value] of Object.entries(a)) conds.push({ key, op: '=', value })
      } else if (c === undefined) {
        conds.push({ key: a, op: '=', value: b })
      } else {
        conds.push({ key: a, op: b, value: c })
      }
      return chain
    }
    chain.first = async () => matching()[0]
    chain.delete = async () => {
      const doomed = new Set(matching())
      store[table] = store[table].filter((row) => !doomed.has(row))
      return doomed.size
    }
    chain.insert = (row: Row) => {
      const inserted = { ...row }
      return {
        onConflict: (keys: string | string[]) => {
          const keyList = Array.isArray(keys) ? keys : [keys]
          return {
            merge: async (patch: Row) => {
              const existing = store[table].find((r) => keyList.every((k) => r[k] === inserted[k]))
              if (existing) Object.assign(existing, patch)
              else store[table].push(inserted)
            },
            ignore: async () => {
              const existing = store[table].find((r) => keyList.every((k) => r[k] === inserted[k]))
              if (!existing) store[table].push(inserted)
            },
          }
        },
      }
    }
    return chain
  }

  const knexMock: any = jest.fn((table: string) => makeChain(table))
  knexMock.raw = jest.fn(async () => ({ rows: [] }))
  return { __esModule: true, default: knexMock }
})

const DID = 'did:webvh:QmScid:issuer.example.org'
const ANNOUNCE_BLOCK = 100
const CURRENT_HEAD = 5000

const successResolution = {
  verified: true,
  outcome: TrustResolutionOutcome.VERIFIED,
  didDocument: {
    id: DID,
    service: [
      { type: 'LinkedVerifiablePresentation', serviceEndpoint: 'https://issuer.example.org/vp.json' },
      { type: 'did-communication', serviceEndpoint: 'wss://issuer.example.org' },
    ],
  },
}

const rowsAt = (height: number) => store.trust_results.filter((r) => r.did === DID && r.height === height)

describe('resolveTrustForDidAtHeight — late-arriving DID document attribution', () => {
  beforeEach(() => {
    store.trust_results = []
    store.trust_reattemptable = []
    resolveDIDMock.mockReset()
  })

  it('attributes a late successful retry to the landing block, not the block that announced the DID', async () => {
    const { resolveTrustForDidAtHeight } = await import('../../../../src/services/resolver/trust-resolve')

    resolveDIDMock.mockRejectedValueOnce(new Error('DID document not found'))
    const initial = await resolveTrustForDidAtHeight(DID, ANNOUNCE_BLOCK)

    expect(initial).toBe(false)
    expect(rowsAt(ANNOUNCE_BLOCK)[0].resolve_result.error).toBe(true)

    resolveDIDMock.mockResolvedValueOnce(successResolution)
    const forwarded = await resolveTrustForDidAtHeight(DID, ANNOUNCE_BLOCK, CURRENT_HEAD)

    expect(forwarded).toBe(true)

    const landed = rowsAt(CURRENT_HEAD)
    expect(landed).toHaveLength(1)
    expect(landed[0].resolve_result.didDocument.service).toHaveLength(2)
    expect(landed[0].trust_status).toBe('TRUSTED')

    // The announcing block keeps the failure it actually had at that point in time.
    expect(rowsAt(ANNOUNCE_BLOCK)).toHaveLength(1)
    expect(rowsAt(ANNOUNCE_BLOCK)[0].resolve_result.error).toBe(true)
  })

  it('leaves an unchanged repeated failure on the original block so no spurious change is broadcast', async () => {
    const { resolveTrustForDidAtHeight } = await import('../../../../src/services/resolver/trust-resolve')

    resolveDIDMock.mockRejectedValueOnce(new Error('DID document not found'))
    await resolveTrustForDidAtHeight(DID, ANNOUNCE_BLOCK)

    resolveDIDMock.mockRejectedValueOnce(new Error('DID document not found'))
    const forwarded = await resolveTrustForDidAtHeight(DID, ANNOUNCE_BLOCK, CURRENT_HEAD)

    expect(forwarded).toBe(false)
    expect(rowsAt(CURRENT_HEAD)).toHaveLength(0)
    expect(rowsAt(ANNOUNCE_BLOCK)).toHaveLength(1)
  })

  it('keeps the in-block resolution on its own block when no landing block is given', async () => {
    const { resolveTrustForDidAtHeight } = await import('../../../../src/services/resolver/trust-resolve')

    resolveDIDMock.mockResolvedValueOnce(successResolution)
    const forwarded = await resolveTrustForDidAtHeight(DID, ANNOUNCE_BLOCK)

    expect(forwarded).toBe(false)
    expect(rowsAt(ANNOUNCE_BLOCK)).toHaveLength(1)
    expect(rowsAt(ANNOUNCE_BLOCK)[0].trust_status).toBe('TRUSTED')
  })
})
