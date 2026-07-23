/**
 * Regression: a VC whose `credentialSchema` is a `JsonSchemaCredential` (W3C indirection over an
 * https URL) must resolve to its on-chain schema and land in `vtcCredentials`, not in
 * `unresolvableCredentialIds`. Before the fix the indexer exact-matched the https URL against local
 * `$id`s and always missed.
 */
const tableRows: Record<string, any[]> = {}

jest.mock('../../../../src/common/utils/db_connection', () => {
  function makeChain(table: string) {
    const chain: any = {}
    const passthrough = () => chain
    for (const m of ['select', 'where', 'whereIn', 'whereNull', 'andWhere', 'orderBy']) {
      chain[m] = jest.fn(passthrough)
    }
    chain.first = jest.fn(() => Promise.resolve((tableRows[table] ?? [])[0]))
    chain.then = (resolve: any, reject: any) => Promise.resolve(tableRows[table] ?? []).then(resolve, reject)
    return chain
  }
  const knexMock: any = jest.fn((table: string) => makeChain(table))
  knexMock.client = { config: { client: 'pg' } }
  knexMock.raw = jest.fn(async () => ({ rows: [] }))
  return knexMock
})

const fetchJsonMock = jest.fn()
jest.mock('@verana-labs/verre', () => ({
  fetchJson: (...args: unknown[]) => fetchJsonMock(...args),
}))

jest.mock('../../../../src/services/resolver/ecs-allowlist', () => ({
  __esModule: true,
  isEcosystemEcsAllowlisted: jest.fn(async () => true),
  isEcsAllowlistEnforced: jest.fn(() => false),
}))

import { buildPresentations } from '../../../../src/services/resolver/trust-resolve-v4.builders'

const VP_ENDPOINT = 'https://agent.example.org/vt/vp.json'
const JSC_URL = 'https://tr.example.org/vt/schemas-service-jsc.json'
const VPR_REF = 'vpr:verana:vna-testnet-1/cs/v1/js/137'

const resolveResult = {
  didDocument: {
    id: 'did:webvh:scid:agent.example.org',
    service: [{ id: '#vp', type: 'LinkedVerifiablePresentation', serviceEndpoint: VP_ENDPOINT }],
  },
}

const vtcVc = {
  id: 'did:webvh:scid:agent.example.org#vtc-1',
  issuer: 'did:webvh:scid:org.example.org',
  credentialSubject: { id: 'did:webvh:scid:agent.example.org' },
  credentialSchema: { id: JSC_URL, type: 'JsonSchemaCredential' },
}

beforeEach(() => {
  fetchJsonMock.mockReset()
  tableRows.credential_schemas = [
    { id: 137, ecosystem_id: 5, json_schema: JSON.stringify({ $id: VPR_REF, title: 'SomeVtcCredential' }) },
  ]
  tableRows.participants = [{ id: 42 }]

  fetchJsonMock.mockImplementation(async (url: string) => {
    if (url === VP_ENDPOINT) return { verifiableCredential: [vtcVc] }
    if (url === JSC_URL) {
      return {
        type: ['VerifiableCredential', 'JsonSchemaCredential'],
        credentialSubject: { type: 'JsonSchema', jsonSchema: { $ref: VPR_REF }, id: VPR_REF },
      }
    }
    return {}
  })
})

describe('buildPresentations — JsonSchemaCredential dereferencing', () => {
  it('resolves an https JsonSchemaCredential ref to its on-chain schema (not unresolvable)', async () => {
    const [entry] = await buildPresentations(resolveResult, {
      unresolvableCredentialIds: true,
      invalidCredentialIds: false,
    })

    expect(entry.unresolvableCredentialIds).toEqual([])
    expect(entry.vtcCredentials).toHaveLength(1)
    expect((entry.vtcCredentials as any[])[0]).toMatchObject({ id: vtcVc.id, credentialSchemaId: 137, ecosystemId: 5 })
    // the JsonSchemaCredential url was actually dereferenced
    expect(fetchJsonMock).toHaveBeenCalledWith(JSC_URL)
  })

  it('still reports a genuinely unknown schema as unresolvable', async () => {
    tableRows.credential_schemas = [] // schema 137 not indexed
    const [entry] = await buildPresentations(resolveResult, {
      unresolvableCredentialIds: true,
      invalidCredentialIds: false,
    })

    expect(entry.vtcCredentials).toEqual([])
    expect(entry.unresolvableCredentialIds).toEqual([vtcVc.id])
  })
})
