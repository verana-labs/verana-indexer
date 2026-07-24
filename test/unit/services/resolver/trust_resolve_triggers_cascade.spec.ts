export {}

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
}))

jest.mock('../../../../src/services/resolver/trust-resolve-v4.builders', () => ({
  __esModule: true,
  hasAllowlistedEcsServiceCredential: async () => true,
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
const BLOCK = 500

function rowsOf(dids: string[]) {
  return { rows: dids.map((d) => ({ d })) }
}

function resolvedDids(): string[] {
  return resolveDIDMock.mock.calls.map((c) => String(c[0]))
}

describe('resolveTrustForBlock — TriggerResolver consumption and dependency cascade', () => {
  beforeEach(() => {
    resolveDIDMock.mockReset()
    rawMock.mockReset()
    resolveDIDMock.mockResolvedValue({
      verified: true,
      outcome: TrustResolutionOutcome.VERIFIED,
      didDocument: { id: AGENT, service: [] },
    })
  })

  function driveSql(cascade: Record<string, string[]>, impacted: string[] = [], triggered: string[] = []) {
    rawMock.mockImplementation(async (sql: string, bindings: unknown[]) => {
      if (sql.includes('ecosystem_history')) return rowsOf(impacted)
      if (sql.includes('transaction_message')) return rowsOf(triggered)
      if (sql.includes("resolve_result->'service'")) {
        const issuers = (bindings[1] as string[]) ?? []
        const children = issuers.flatMap((issuer) => cascade[issuer] ?? [])
        return rowsOf([...new Set(children)])
      }
      return { rows: [] }
    })
  }

  it('re-evaluates the DID of a Participant named by a consumed TriggerResolver event', async () => {
    const { resolveTrustForBlock } = await import('../../../../src/services/resolver/trust-resolve')
    driveSql({}, [], [AGENT])

    await resolveTrustForBlock(BLOCK)

    expect(resolvedDids()).toEqual([AGENT])
  })

  it('cascades to services whose ECS-SERVICE credential is issued by the re-evaluated DID', async () => {
    const { resolveTrustForBlock } = await import('../../../../src/services/resolver/trust-resolve')
    driveSql({ [AGENT]: [CHILD] }, [], [AGENT])

    await resolveTrustForBlock(BLOCK)

    expect(resolvedDids().sort()).toEqual([AGENT, CHILD].sort())
  })

  it('terminates on a cyclic issuer graph without re-evaluating a DID twice', async () => {
    const { resolveTrustForBlock } = await import('../../../../src/services/resolver/trust-resolve')
    driveSql({ [AGENT]: [CHILD], [CHILD]: [AGENT] }, [], [AGENT])

    await resolveTrustForBlock(BLOCK)

    const seen = resolvedDids()
    expect(seen.sort()).toEqual([AGENT, CHILD].sort())
    expect(new Set(seen).size).toBe(seen.length)
  })

  it('re-evaluates every anchored service even past the per-block DID cap (IDX-VT-EVAL-4)', async () => {
    const { resolveTrustForBlock } = await import('../../../../src/services/resolver/trust-resolve')
    const children = Array.from({ length: 120 }, (_, i) => `did:webvh:QmChild${i}:c${i}.example.org`)
    driveSql({ [AGENT]: children }, [], [AGENT])

    await resolveTrustForBlock(BLOCK)

    const seen = resolvedDids()
    expect(seen).toHaveLength(121)
    for (const child of children) expect(seen).toContain(child)
  })

  it('does not resolve anything when the block carries no trigger and no impacted DID', async () => {
    const { resolveTrustForBlock } = await import('../../../../src/services/resolver/trust-resolve')
    driveSql({}, [], [])

    await resolveTrustForBlock(BLOCK)

    expect(resolveDIDMock).not.toHaveBeenCalled()
  })
})
