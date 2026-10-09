import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'

const doc = JSON.parse(readFileSync(resolve(process.cwd(), 'docs/api/openapi.json'), 'utf8'))

// Non-list response envelopes: the entity key and block_height are always present on 200.
const COMPONENTS: Record<string, string> = {
  CorporationV4Response: 'corporation',
  EcosystemResponse: 'ecosystem',
  CredentialSchemaResponse: 'schema',
  ParticipantResponse: 'participant',
  ParticipantSessionResponse: 'session',
  TrustDepositResponse: 'trust_deposit',
  GovernanceFrameworkVersionV4Response: 'version',
  GetPriceResponse: 'price',
  CorporationParamsResponse: 'params',
  EcosystemParamsResponse: 'params',
  CredentialSchemaParamsResponse: 'params',
  ParticipantParamsResponse: 'params',
  TrustDepositParamsResponse: 'params',
}

const INLINE_PATHS: Record<string, string> = {
  '/v4/group/get/{corporation_id}': 'group',
  '/v4/group/proposal/{id}': 'proposal',
  '/v4/exchange-rate/get': 'exchange_rate',
  '/v4/di/get/{digest}': 'digest',
  '/v4/delegation/operator-authorization/{id}': 'authorization',
  '/v4/delegation/vs-operator-authorization/{id}': 'authorization',
}

function expectEchoEnvelope(schema: any, key: string) {
  expect(schema.properties[key]).toBeDefined()
  expect(schema.properties.block_height).toMatchObject({ type: 'integer' })
  expect(schema.required).toEqual(expect.arrayContaining([key, 'block_height']))
}

describe('OpenAPI non-list responses declare block_height as required', () => {
  it.each(Object.entries(COMPONENTS))('component %s', (name, key) => {
    expectEchoEnvelope(doc.components.schemas[name], key)
  })

  it.each(Object.entries(INLINE_PATHS))('inline 200 schema of %s', (path, key) => {
    expectEchoEnvelope(doc.paths[path].get.responses['200'].content['application/json'].schema, key)
  })
})
