const TrustResolutionOutcome = {
  VERIFIED: 'verified',
  VERIFIED_TEST: 'verified-test',
  NOT_TRUSTED: 'not-trusted',
  INVALID: 'invalid',
} as const

const resolveDIDMock = jest.fn()
const createResolverMock = jest.fn()

jest.mock(
  '@verana-labs/verre',
  () => ({
    __esModule: true,
    resolveDID: (...args: unknown[]) => resolveDIDMock(...args),
    TrustResolutionOutcome,
  }),
  { virtual: true }
)

jest.mock('../../../../src/services/resolver/did-document-resolver', () => ({
  __esModule: true,
  createDidDocumentResolver: () => createResolverMock(),
}))

jest.mock('../../../../src/config.json', () => {
  const actual = jest.requireActual('../../../../src/config.json')
  return {
    __esModule: true,
    default: {
      ...actual,
      resolver: { ...actual.resolver, enabled: true, maxDidsPerTrustBlock: 50, didResolveConcurrency: 2 },
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
  getEcsEcosystems: () => [],
}))

jest.mock('../../../../src/services/resolver/trust-resolve-v4.builders', () => ({
  __esModule: true,
  hasAllowlistedEcsServiceCredential: async () => true,
  resolveCorporationId: async () => 0,
}))

jest.mock('../../../../src/services/resolver/verre-registry-adapter', () => ({
  __esModule: true,
  attachRegistryAdapters: (registries: unknown) => registries,
}))

const rawMock = jest.fn()

jest.mock('../../../../src/common/utils/db_connection', () => {
  const chain: any = {}
  for (const m of ['select', 'where', 'whereNotNull', 'limit', 'orderBy']) chain[m] = () => chain
  chain.first = async () => undefined
  chain.delete = async () => 0
  chain.insert = () => ({
    onConflict: () => ({ merge: async () => undefined, ignore: async () => undefined }),
  })

  const knexMock: any = jest.fn(() => chain)
  knexMock.raw = (...args: unknown[]) => rawMock(...args)
  return { __esModule: true, default: knexMock }
})

const AGENT = 'did:webvh:QmAgent:agent.example.org'
const CHILD = 'did:webvh:QmChild:child.example.org'

function resolversPassed(): unknown[] {
  return resolveDIDMock.mock.calls.map((c) => (c[1] as { didResolver?: unknown }).didResolver)
}

describe('DID document resolver scope', () => {
  beforeEach(() => {
    resolveDIDMock.mockReset()
    rawMock.mockReset()
    createResolverMock.mockReset()
    let n = 0
    createResolverMock.mockImplementation(() => ({ resolverId: ++n }))
    resolveDIDMock.mockResolvedValue({ verified: true, outcome: TrustResolutionOutcome.VERIFIED })
    rawMock.mockImplementation(async (sql: string) => {
      if (sql.includes('transaction_message')) return { rows: [{ d: AGENT }] }
      if (sql.includes("resolve_result->'service'")) return { rows: [{ d: CHILD }] }
      return { rows: [] }
    })
  })

  it('shares one resolver across every DID of a block, cascade included, and none across blocks', async () => {
    const { resolveTrustForBlock } = await import('../../../../src/services/resolver/trust-resolve')

    await resolveTrustForBlock(500)
    await resolveTrustForBlock(501)

    expect(resolveDIDMock.mock.calls.map((c) => c[0])).toEqual([AGENT, CHILD, AGENT, CHILD])
    expect(resolversPassed()).toEqual([{ resolverId: 1 }, { resolverId: 1 }, { resolverId: 2 }, { resolverId: 2 }])
  })

  it('gives a standalone evaluation its own resolver', async () => {
    const { resolveTrustForDidAtHeight } = await import('../../../../src/services/resolver/trust-resolve')

    await resolveTrustForDidAtHeight(AGENT, 500)
    await resolveTrustForDidAtHeight(AGENT, 500)

    expect(resolversPassed()).toEqual([{ resolverId: 1 }, { resolverId: 2 }])
  })
})
