import { VeranaParticipantMessageTypes } from '../../../../src/common/verana-message-types'
import {
  fetchParticipant,
  fetchParticipantSession,
} from '../../../../src/modules/pp-height-sync/pp_height_sync_helpers'
import { runHeightSyncParticipant } from '../../../../src/modules/pp-height-sync/pp_height_sync_service'

jest.mock('../../../../src/modules/pp-height-sync/pp_height_sync_helpers', () => ({
  ...jest.requireActual('../../../../src/modules/pp-height-sync/pp_height_sync_helpers'),
  fetchParticipant: jest.fn(),
  fetchParticipantSession: jest.fn(),
}))

const ledger: Record<number, { id: number; validator_participant_id: number }> = {
  3: { id: 3, validator_participant_id: 0 },
  5: { id: 5, validator_participant_id: 3 },
  6: { id: 6, validator_participant_id: 5 },
}

describe('runHeightSyncParticipant fee beneficiaries', () => {
  beforeEach(() => {
    jest
      .mocked(fetchParticipant)
      .mockImplementation(async (id) =>
        ledger[id] ? { participant: ledger[id] as unknown as Record<string, unknown> } : null
      )
    jest.mocked(fetchParticipantSession).mockResolvedValue(null)
  })

  it('syncs every participant up the issuer validator chain after a session', async () => {
    const broker = { call: jest.fn().mockResolvedValue({ success: true, matches: true }) }

    await runHeightSyncParticipant(broker as never, [
      {
        type: VeranaParticipantMessageTypes.CreateOrUpdateParticipantSession,
        height: 100,
        content: { id: 'session-uuid', issuer_participant_id: 6 },
        txEvents: [],
      },
    ])

    const synced = broker.call.mock.calls
      .filter(([action]) => action === 'participantIngest.syncParticipantFromLedger')
      .map(([, params]) => params.ledgerParticipant.id)
    expect(synced).toEqual([6, 5, 3])
  })

  it('does not walk validator chains for other participant messages', async () => {
    const broker = { call: jest.fn().mockResolvedValue({ success: true, matches: true }) }

    await runHeightSyncParticipant(broker as never, [
      { type: VeranaParticipantMessageTypes.RevokeParticipant, height: 100, content: { id: 6 }, txEvents: [] },
    ])

    const synced = broker.call.mock.calls
      .filter(([action]) => action === 'participantIngest.syncParticipantFromLedger')
      .map(([, params]) => params.ledgerParticipant.id)
    expect(synced).toEqual([6])
  })
})
