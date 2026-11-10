import { withAbciQueryClient } from '../../../../src/common/utils/grpc_query'
import {
  extractImpactedParticipantIds,
  fetchParticipant,
  fetchParticipantSession,
} from '../../../../src/modules/pp-height-sync/pp_height_sync_helpers'

jest.mock('../../../../src/common/utils/grpc_query', () => ({
  ...jest.requireActual('../../../../src/common/utils/grpc_query'),
  withAbciQueryClient: jest.fn(),
}))

const mockAbci = withAbciQueryClient as jest.MockedFunction<typeof withAbciQueryClient>

describe('extractImpactedParticipantIds', () => {
  it('reads ids that also look like base64 as plain numbers', () => {
    const txEvents = ['355', '1400', '12345678'].map((id) => ({
      type: 'create_participant',
      attributes: [{ key: 'participant_id', value: id }],
    }))
    expect(
      extractImpactedParticipantIds({ type: '/verana.pp.v1.MsgSelfCreateParticipant', content: {}, txEvents })
    ).toEqual([355, 1400, 12345678])
  })
})

describe('participant ledger reads', () => {
  const notFound = new Error('Query failed with (38): rpc error: code = NotFound desc = participant not found')
  const futureHeight = new Error('Query failed with (26): cannot query with height in the future')

  beforeEach(() => mockAbci.mockReset())

  it('return null when the ledger has no such record', async () => {
    mockAbci.mockRejectedValue(notFound)

    await expect(fetchParticipant(5, 500)).resolves.toBeNull()
    await expect(fetchParticipantSession('s-1', 500)).resolves.toBeNull()
  })

  it('return null for a participant the node reports as an internal not-found', async () => {
    mockAbci.mockRejectedValue(
      new Error(
        "Query failed with (6): rpc error: code = Internal desc = failed to get participant: collections: not found: key '1' of type github.com/cosmos/gogoproto/verana.pp.v1.Participant: unknown request"
      )
    )

    await expect(fetchParticipant(1, 53043)).resolves.toBeNull()
  })

  it('throw when the query fails', async () => {
    mockAbci.mockRejectedValue(futureHeight)

    await expect(fetchParticipant(5, 500)).rejects.toBe(futureHeight)
    await expect(fetchParticipantSession('s-1', 500)).rejects.toBe(futureHeight)
  })
})
