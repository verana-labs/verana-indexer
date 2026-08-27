/**
 * Per-table knex mock. `__rows` maps a table name to the rows its query builder
 * resolves to. Every builder method returns the same chainable object, which is
 * thenable (resolves to the configured rows for the table it was created with).
 * `where({ col: value })` narrows the rows, but only on columns a row declares,
 * so fixtures that omit a filtered column still match every query.
 */
const tableRows: Record<string, any[]> = {}

jest.mock('../../../../src/common/utils/db_connection', () => {
  function makeChain(table: string) {
    const chain: any = {}
    const passthrough = () => chain
    const criteria: Array<Record<string, unknown>> = []
    for (const m of ['select', 'where', 'whereIn', 'whereNull', 'andWhere', 'orderBy', 'first']) {
      chain[m] = jest.fn(passthrough)
    }
    for (const m of ['where', 'andWhere']) {
      chain[m] = jest.fn((arg: unknown) => {
        if (arg && typeof arg === 'object') criteria.push(arg as Record<string, unknown>)
        return chain
      })
    }
    const rows = () =>
      (tableRows[table] ?? []).filter((row) =>
        criteria.every((c) => Object.entries(c).every(([col, val]) => row[col] === undefined || row[col] === val))
      )
    chain.first = jest.fn(() => Promise.resolve(rows()[0]))
    chain.then = (resolve: any, reject: any) => Promise.resolve(rows()).then(resolve, reject)
    return chain
  }
  const knexMock: any = jest.fn((table: string) => makeChain(table))
  knexMock.client = { config: { client: 'pg' } }
  return knexMock
})

jest.mock('@verana-labs/verre', () => ({
  fetchJson: jest.fn(async () => ({})),
  computeCredentialDigestJCS: jest.fn(() => 'q2VwbGFjZWRkaWdlc3Q='),
}))

jest.mock('canonicalize', () => ({ __esModule: true, default: (value: unknown) => JSON.stringify(value) }), {
  virtual: true,
})
const isEcosystemEcsAllowlistedMock = jest.fn(async (_id: number) => true)
jest.mock('../../../../src/services/resolver/ecs-allowlist', () => ({
  __esModule: true,
  isEcosystemEcsAllowlisted: (id: number) => isEcosystemEcsAllowlistedMock(id),
  isEcsAllowlistEnforced: () => false,
}))

import {
  buildCorporation,
  buildEcsCredentials,
  buildParticipations,
  buildPresentations,
  buildServices,
  deriveParticipantState,
  resolveCorporationId,
} from '../../../../src/services/resolver/trust-resolve-v4.builders'

const NOW = new Date('2026-06-02T00:00:00Z')
const past = '2026-01-01T00:00:00Z'
const future = '2027-01-01T00:00:00Z'

describe('deriveParticipantState', () => {
  it('1. REPAID when slashed and repaid >= slashed', () => {
    expect(deriveParticipantState({ slashed: past, repaid: future }, NOW)).toBe('REPAID')
  })
  it('2. SLASHED when slashed and not repaid (or repaid < slashed)', () => {
    expect(deriveParticipantState({ slashed: past }, NOW)).toBe('SLASHED')
    expect(deriveParticipantState({ slashed: future, repaid: past }, NOW)).toBe('SLASHED')
  })
  it('3. REVOKED when revoked <= now', () => {
    expect(deriveParticipantState({ revoked: past }, NOW)).toBe('REVOKED')
  })
  it('4. EXPIRED when effective_until <= now', () => {
    expect(deriveParticipantState({ effective_until: past }, NOW)).toBe('EXPIRED')
  })
  it('5. FUTURE when effective_from > now', () => {
    expect(deriveParticipantState({ effective_from: future }, NOW)).toBe('FUTURE')
  })
  it('6. ACTIVE within the effective window', () => {
    expect(deriveParticipantState({ effective_from: past, effective_until: future }, NOW)).toBe('ACTIVE')
    expect(deriveParticipantState({}, NOW)).toBe('ACTIVE')
  })
  it('priority: SLASHED outranks REVOKED/EXPIRED', () => {
    expect(deriveParticipantState({ slashed: past, revoked: past, effective_until: past }, NOW)).toBe('SLASHED')
  })
  it('revoked in the future does not count as REVOKED', () => {
    expect(deriveParticipantState({ revoked: future, effective_from: past }, NOW)).toBe('ACTIVE')
  })
})

describe('buildParticipations', () => {
  beforeEach(() => {
    for (const k of Object.keys(tableRows)) delete tableRows[k]
  })

  it('maps participant rows to Participant entries and filters by state', async () => {
    tableRows.participants = [
      {
        id: 501,
        schema_id: 1234,
        role: 'ISSUER',
        did: 'did:example:1',
        vs_operator: 'verana1op',
        weight: 10000000,
        validator_participant_id: 401,
        issued: 2345,
        verified: 0,
        participants_holder: 75,
        effective_from: past,
        effective_until: future,
      },
      {
        id: 502,
        schema_id: 1234,
        role: 'VERIFIER',
        did: 'did:example:1',
        weight: 5000000,
        validator_participant_id: 402,
        revoked: past, // -> REVOKED, filtered out when only ACTIVE requested
      },
    ]
    tableRows.credential_schemas = [{ id: 1234, ecosystem_id: 9876 }]

    const active = await buildParticipations('did:example:1', NOW, ['ACTIVE'])
    expect(active).toHaveLength(1)
    expect(active[0]).toMatchObject({
      id: 501,
      role: 'ISSUER',
      state: 'ACTIVE',
      credentialSchemaId: 1234,
      ecosystemId: 9876,
      weight: '10000000uvna',
      vsOperator: 'verana1op',
      validatorParticipantId: 401,
      issuedCredentials: 2345,
      participants: { HOLDER: 75 },
    })

    const both = await buildParticipations('did:example:1', NOW, ['ACTIVE', 'REVOKED'])
    expect(both.map((p) => p.state).sort()).toEqual(['ACTIVE', 'REVOKED'])
  })

  it('emits validatorParticipantId null only for ECOSYSTEM role', async () => {
    tableRows.participants = [
      { id: 1, schema_id: 7, role: 'ECOSYSTEM', did: 'did:example:eco', validator_participant_id: null },
    ]
    tableRows.credential_schemas = [{ id: 7, ecosystem_id: 70 }]
    const out = await buildParticipations('did:example:eco', NOW, ['ACTIVE'])
    expect(out[0].role).toBe('ECOSYSTEM')
    expect(out[0].validatorParticipantId).toBeNull()
  })
})

describe('buildServices', () => {
  const mcp = { id: 'did:example:1#mcp', type: 'MCP', serviceEndpoint: 'https://x/mcp' }
  const didcomm = {
    id: 'did:example:1#did-communication',
    type: 'did-communication',
    serviceEndpoint: 'wss://x/didcomm',
    accept: ['didcomm/v2'],
  }
  const linkedVp = {
    id: 'did:example:1#vt-vp1',
    type: 'LinkedVerifiablePresentation',
    serviceEndpoint: 'https://x/vp1.json',
  }

  it('returns the non-LinkedVerifiablePresentation service entries verbatim', () => {
    const out = buildServices({ didDocument: { service: [mcp, linkedVp, didcomm] } })
    expect(out).toEqual([mcp, didcomm])
  })

  it('filters LinkedVerifiablePresentation when type is an array', () => {
    const arrayTyped = { ...linkedVp, type: ['LinkedVerifiablePresentation'] }
    const out = buildServices({ didDocument: { service: [mcp, arrayTyped] } })
    expect(out).toEqual([mcp])
  })

  it('returns [] when the resolution has no DID Document or services', () => {
    expect(buildServices(undefined)).toEqual([])
    expect(buildServices({ error: true })).toEqual([])
    expect(buildServices({ didDocument: {} })).toEqual([])
    expect(buildServices({ didDocument: { service: null } })).toEqual([])
  })
})

describe('buildPresentations', () => {
  const noFlags = { unresolvableCredentialIds: false, invalidCredentialIds: false }
  const resolution = {
    didDocument: {
      id: 'did:example:x',
      service: [
        { id: 'did:example:x#whois', type: 'LinkedVerifiablePresentation', serviceEndpoint: 'https://x/vp1.json' },
        { id: '#files', type: 'relativeRef', serviceEndpoint: 'https://x' },
        { id: '#vp2', type: 'LinkedVerifiablePresentation', serviceEndpoint: 'https://x/vp2.json' },
      ],
    },
  }

  it('maps LinkedVerifiablePresentation entries, resolving relative service ids', async () => {
    expect(await buildPresentations(resolution, noFlags)).toEqual([
      { id: 'https://x/vp1.json', serviceId: 'did:example:x#whois', vtcCredentials: [] },
      { id: 'https://x/vp2.json', serviceId: 'did:example:x#vp2', vtcCredentials: [] },
    ])
  })

  it('includes the empty sub-lists only when their flags are set', async () => {
    const [first] = await buildPresentations(resolution, {
      unresolvableCredentialIds: true,
      invalidCredentialIds: true,
    })
    expect(first).toMatchObject({ unresolvableCredentialIds: [], invalidCredentialIds: [] })
  })

  it('dedupes by resolved serviceId and returns [] without a DID Document', async () => {
    const dup = {
      didDocument: {
        id: 'did:example:x',
        service: [
          { id: '#vp', type: 'LinkedVerifiablePresentation', serviceEndpoint: 'https://x/a.json' },
          { id: 'did:example:x#vp', type: 'LinkedVerifiablePresentation', serviceEndpoint: 'https://x/b.json' },
        ],
      },
    }
    expect(await buildPresentations(dup, noFlags)).toHaveLength(1)
    expect(await buildPresentations({ error: true }, noFlags)).toEqual([])
  })
})

describe('buildEcsCredentials', () => {
  const service = {
    ecs: 'ecs-service',
    id: 'urn:uuid:service-vc-1',
    issuer: 'did:example:org',
    subject: { id: 'did:example:sub', name: 'Gov ID issuer', type: 'VerifiableService' },
    validFrom: '2026-05-28T13:35:24.887Z',
    validUntil: '2036-05-25T13:35:24.887Z',
    digestJCS: 'q2VwbGFjZWRkaWdlc3Q=',
    issuedAtTime: '2026-02-10T09:15:00Z',
    credentialSchemaId: 1,
    ecosystemId: 9,
    ecsSchemaVersion: 'v4',
    issuerParticipant: { id: 601, role: 'ISSUER' },
    subjectParticipants: [{ id: 501, role: 'HOLDER' }],
    raw: {},
  }

  it('reshapes the resolved credential into an ecsCredentials entry', async () => {
    const out = await buildEcsCredentials({ service })
    expect(out).toHaveLength(1)
    expect(out[0]).toMatchObject({
      ecsSchema: 'ServiceCredential',
      ecsSchemaVersion: 'v4',
      credentialSchemaId: 1,
      ecosystemId: 9,
      issuerParticipantId: 601,
      participantId: 501,
      id: 'urn:uuid:service-vc-1',
      digestJCS: 'q2VwbGFjZWRkaWdlc3Q=',
      issuedAtTime: '2026-02-10T09:15:00.000Z',
      validFrom: '2026-05-28T13:35:24.887Z',
      validUntil: '2036-05-25T13:35:24.887Z',
      credentialSubject: { id: 'did:example:sub', name: 'Gov ID issuer', type: 'VerifiableService' },
    })
  })

  it('excludes a credential with no anchored digest', async () => {
    expect(await buildEcsCredentials({ service: { ...service, digestJCS: undefined } })).toEqual([])
    expect(await buildEcsCredentials({ service: { ...service, issuedAtTime: undefined } })).toEqual([])
  })

  it('excludes a credential with no VC id', async () => {
    expect(await buildEcsCredentials({ service: { ...service, id: '' } })).toEqual([])
  })

  it('excludes same-response duplicates by id and by digestJCS', async () => {
    const byId = await buildEcsCredentials({ service, serviceProvider: { ...service, ecs: 'ecs-org' } })
    expect(byId).toHaveLength(1)

    const byDigest = await buildEcsCredentials({
      service,
      serviceProvider: { ...service, ecs: 'ecs-org', id: 'urn:uuid:other-vc' },
    })
    expect(byDigest).toHaveLength(1)
  })

  it('emits both entries when they are distinct credentials', async () => {
    const out = await buildEcsCredentials({
      service,
      serviceProvider: { ...service, ecs: 'ecs-org', id: 'urn:uuid:org-vc-1', digestJCS: 'b3RoZXJkaWdlc3Q=' },
    })
    expect(out.map((entry) => entry.ecsSchema)).toEqual(['ServiceCredential', 'OrganizationCredential'])
  })

  it('emits null validFrom/validUntil when the credential declares no validity window', async () => {
    const out = await buildEcsCredentials({
      service: { ...service, validFrom: undefined, validUntil: undefined },
    })
    expect(out[0].validFrom).toBeNull()
    expect(out[0].validUntil).toBeNull()
  })

  it('ignores resolutions with no ECS classification', async () => {
    expect(await buildEcsCredentials({ service: { ...service, ecs: null } })).toEqual([])
    expect(await buildEcsCredentials({ error: true })).toEqual([])
  })
})

describe('buildCorporation', () => {
  beforeEach(() => {
    for (const k of Object.keys(tableRows)) delete tableRows[k]
  })

  it('returns null when no Corporation owns the DID', async () => {
    expect(await buildCorporation('did:example:none')).toBeNull()
  })

  it('returns null when the DID is merely owned by a Corporation and is not its declared did', async () => {
    tableRows.corporation = [{ id: 8, did: 'did:example:corp-declared', policy_address: 'verana1ys0' }]
    tableRows.ecosystem = [{ did: 'did:webvh:owned-ecosystem', corporation_id: 8 }]

    expect(await buildCorporation('did:webvh:owned-ecosystem')).toBeNull()
    expect(await resolveCorporationId('did:webvh:owned-ecosystem')).toBe(8)
  })

  it('joins the Corporation with its trust deposit and active CGF', async () => {
    tableRows.corporation = [{ id: 42, did: 'did:example:corp', policy_address: 'verana1policy' }]
    tableRows.trust_deposits = [
      {
        corporation: 'verana1policy',
        deposit: 40000000,
        slashed_deposit: 1000000,
        slash_count: 1,
        last_slashed: '2026-01-01T03:00:00Z',
      },
    ]
    tableRows.co_governance_framework_version = [{ id: 7, version: 3, active_since: '2026-02-15T09:00:00Z' }]
    tableRows.co_governance_framework_document = [
      { language: 'en', url: 'https://corp/cgf/v3/en.html', digest_sri: 'sha384-x' },
    ]

    expect(await buildCorporation('did:example:corp')).toMatchObject({
      id: 42,
      policyAddress: 'verana1policy',
      deposit: '40000000uvna',
      slashedEvents: 1,
      slashedValue: '1000000uvna',
      lastSlashedAtTime: '2026-01-01T03:00:00.000Z',
      cgf: {
        version: 3,
        activeSince: '2026-02-15T09:00:00.000Z',
        documents: [{ language: 'en', url: 'https://corp/cgf/v3/en.html', digestSri: 'sha384-x' }],
      },
    })
  })

  it('omits slash fields and cgf when absent', async () => {
    tableRows.corporation = [{ id: 5, did: 'did:example:corp2', policy_address: 'verana1p2' }]
    tableRows.trust_deposits = [{ corporation: 'verana1p2', deposit: 0, slash_count: 0 }]

    const out = await buildCorporation('did:example:corp2')
    expect(out).toMatchObject({ id: 5, policyAddress: 'verana1p2', deposit: '0uvna' })
    expect(out).not.toHaveProperty('slashedEvents')
    expect(out).not.toHaveProperty('cgf')
  })
})

describe('resolveCorporationId', () => {
  beforeEach(() => {
    for (const k of Object.keys(tableRows)) delete tableRows[k]
  })

  it('returns the id of the Corporation whose did matches', async () => {
    tableRows.corporation = [{ id: 42 }]
    tableRows.ecosystem = [{ corporation_id: 7 }]
    expect(await resolveCorporationId('did:example:owner')).toBe(42)
  })

  it('falls back to the claiming Ecosystem corporation_id', async () => {
    tableRows.ecosystem = [{ corporation_id: 7 }]
    expect(await resolveCorporationId('did:example:eco')).toBe(7)
  })

  it('falls back to the claiming Participant corporation_id', async () => {
    tableRows.participants = [{ corporation_id: 9 }]
    expect(await resolveCorporationId('did:example:issuer')).toBe(9)
  })

  it('returns the 0 sentinel (never null) for a DID with no indexed owner', async () => {
    const out = await resolveCorporationId('did:example:unknown')
    expect(out).toBe(0)
    expect(out).not.toBeNull()
  })

  it('returns 0 for an empty did', async () => {
    expect(await resolveCorporationId('')).toBe(0)
  })
})
