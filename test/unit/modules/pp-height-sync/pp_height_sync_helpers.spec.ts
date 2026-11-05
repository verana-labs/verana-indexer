import { extractImpactedParticipantIds } from '../../../../src/modules/pp-height-sync/pp_height_sync_helpers'

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
