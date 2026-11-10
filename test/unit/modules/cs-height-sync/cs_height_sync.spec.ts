import type { ServiceBroker } from 'moleculer'
import { withAbciQueryClient } from '../../../../src/common/utils/grpc_query'
import {
  extractImpactedCredentialSchemaIds,
  getCredentialSchema,
} from '../../../../src/modules/cs-height-sync/cs_height_sync_helpers'
import { runHeightSyncCS } from '../../../../src/modules/cs-height-sync/cs_height_sync_service'

jest.mock('../../../../src/common/utils/grpc_query', () => ({
  ...jest.requireActual('../../../../src/common/utils/grpc_query'),
  withAbciQueryClient: jest.fn(),
}))

const mockAbci = withAbciQueryClient as jest.MockedFunction<typeof withAbciQueryClient>

const NOT_FOUND = new Error(
  'Query failed with (38): rpc error: code = NotFound desc = credential schema not found: key not found'
)
const FUTURE_HEIGHT = new Error(
  'Query failed with (26): cannot query with height in the future; please provide a valid height: invalid height'
)

const createSchemaEvents = [
  { type: 'create_credential_schema', attributes: [{ key: 'credential_schema_id', value: '44' }] },
]
const createSchemaPayload = {
  credentialSchemaMessages: [{ type: '/verana.cs.v1.MsgCreateCredentialSchema', content: {} }],
  csEventsFromBlock: createSchemaEvents,
}

function mockBroker(syncResult: unknown = { success: true }) {
  const call = jest.fn().mockResolvedValue(syncResult)
  const logger = { info: jest.fn(), warn: jest.fn(), error: jest.fn(), debug: jest.fn() }
  return { broker: { call, logger } as unknown as ServiceBroker, call }
}

describe('getCredentialSchema', () => {
  beforeEach(() => mockAbci.mockReset())

  it('returns null when the ledger has no such schema', async () => {
    mockAbci.mockRejectedValue(NOT_FOUND)
    await expect(getCredentialSchema(44, 321409)).resolves.toBeNull()
  })

  it('rethrows a failed query instead of reporting not found', async () => {
    mockAbci.mockRejectedValue(FUTURE_HEIGHT)
    await expect(getCredentialSchema(44, 321409)).rejects.toBe(FUTURE_HEIGHT)
  })
})

describe('runHeightSyncCS', () => {
  beforeEach(() => mockAbci.mockReset())

  it('stores the schema the ledger returns at the block height', async () => {
    mockAbci.mockResolvedValue({ schema: { id: 44, ecosystem_id: 31 } })
    const { broker, call } = mockBroker()

    await runHeightSyncCS(broker, createSchemaPayload, 321409)

    expect(mockAbci).toHaveBeenCalledWith(321409, expect.any(Function))
    expect(call).toHaveBeenCalledWith(expect.stringContaining('syncFromLedger'), {
      ledgerResponse: { schema: { id: 44, ecosystem_id: 31 } },
      blockHeight: 321409,
    })
  })

  it('fails the block when the ledger query fails', async () => {
    mockAbci.mockRejectedValue(FUTURE_HEIGHT)
    const { broker, call } = mockBroker()

    await expect(runHeightSyncCS(broker, createSchemaPayload, 321409)).rejects.toThrow(
      'Failed to sync schema(s) 44 at block=321409'
    )
    expect(call).not.toHaveBeenCalled()
  })

  it('fails the block when the ledger has no schema the tx events name', async () => {
    mockAbci.mockRejectedValue(NOT_FOUND)
    const { broker } = mockBroker()

    await expect(runHeightSyncCS(broker, createSchemaPayload, 321409)).rejects.toThrow('schema(s) 44')
  })

  it('fails the block when the schema row is not stored', async () => {
    mockAbci.mockResolvedValue({ schema: { id: 44, ecosystem_id: 31 } })
    const { broker } = mockBroker({ success: false })

    await expect(runHeightSyncCS(broker, createSchemaPayload, 321409)).rejects.toThrow('schema(s) 44')
  })

  it('syncs every schema before failing on the ones that did not sync', async () => {
    mockAbci.mockResolvedValueOnce({ schema: { id: 42 } }).mockRejectedValueOnce(FUTURE_HEIGHT)
    const { broker, call } = mockBroker()
    const payload = {
      credentialSchemaMessages: [],
      csEventsFromBlock: [
        { type: 'create_credential_schema', attributes: [{ key: 'credential_schema_id', value: '42' }] },
        ...createSchemaEvents,
      ],
    }

    await expect(runHeightSyncCS(broker, payload, 321409)).rejects.toThrow('schema(s) 44 at block=321409')
    expect(call).toHaveBeenCalledTimes(1)
  })
})

describe('extractImpactedCredentialSchemaIds', () => {
  it('reads ids that also look like base64 as plain numbers', () => {
    const events = ['44', '1400', '12345678'].map((id) => ({
      type: 'create_credential_schema',
      attributes: [{ key: 'credential_schema_id', value: id }],
    }))
    expect(extractImpactedCredentialSchemaIds([], events)).toEqual([44, 1400, 12345678])
  })
})
