jest.mock('../../../../src/common/utils/db_connection', () => ({ __esModule: true, default: jest.fn() }))

import { existsSync, readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { ServiceBroker } from 'moleculer'
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
