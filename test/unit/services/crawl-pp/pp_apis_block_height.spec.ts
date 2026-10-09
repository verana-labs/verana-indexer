jest.mock('../../../../src/common/utils/blockHeight', () => ({
  ...jest.requireActual('../../../../src/common/utils/blockHeight'),
  getResolvedBlockHeight: jest.fn(async (height?: number) => height ?? 777),
}))

import { ServiceBroker } from 'moleculer'
import knex from '../../../../src/common/utils/db_connection'
import ParticipantAPIService from '../../../../src/services/crawl-pp/pp_apis.service'

const PARTICIPANT_ID = 501
const SESSION_ID = '0f3d2f6c-1111-4222-8333-444455556666'
const created = new Date('2026-10-01T00:00:00.000Z')

describe('ParticipantAPIService block_height echo', () => {
  const broker = new ServiceBroker({ logger: false })
  const service: any = broker.createService(ParticipantAPIService)

  beforeAll(async () => {
    for (const table of [
      'participant_session_history',
      'participant_sessions',
      'participant_history',
      'participants',
    ]) {
      await knex(table)
        .del()
        .catch(() => {})
    }
    await knex('participants').insert({
      id: PARTICIPANT_ID,
      schema_id: 999001,
      role: 'ISSUER',
      did: 'did:example:pp-block-height',
      created,
      modified: created,
      effective_from: created,
      corporation_id: 1,
      issuance_fee_discount: 2500,
      verification_fee_discount: 1,
    })
    await knex('participant_history').insert({
      participant_id: PARTICIPANT_ID,
      schema_id: 999001,
      role: 'ISSUER',
      did: 'did:example:pp-block-height',
      created,
      modified: created,
      effective_from: created,
      participants: 0,
      weight: 0,
      ecosystem_slash_events: 0,
      ecosystem_slashed_amount: 0,
      ecosystem_slashed_amount_repaid: 0,
      network_slash_events: 0,
      network_slashed_amount: 0,
      network_slashed_amount_repaid: 0,
      issued: 0,
      verified: 0,
      issuance_fee_discount: 2500,
      verification_fee_discount: 1,
      vs_operator_authz_enabled: false,
      vs_operator_authz_with_feegrant: false,
      event_type: 'Create',
      height: 100,
      corporation_id: 1,
    })
    await knex('participant_sessions').insert({
      id: SESSION_ID,
      agent_participant_id: PARTICIPANT_ID,
      wallet_agent_participant_id: PARTICIPANT_ID,
      session_records: JSON.stringify([]),
      corporation_id: 1,
      created,
      modified: created,
    })
    await knex('participant_session_history').insert({
      session_id: SESSION_ID,
      agent_participant_id: String(PARTICIPANT_ID),
      wallet_agent_participant_id: String(PARTICIPANT_ID),
      session_records: JSON.stringify([]),
      event_type: 'Create',
      height: 100,
      corporation_id: 1,
    })
  })

  afterAll(async () => {
    await knex('participant_session_history').where({ session_id: SESSION_ID }).del()
    await knex('participant_sessions').where({ id: SESSION_ID }).del()
    await knex('participant_history').where({ participant_id: PARTICIPANT_ID }).del()
    await knex('participants').where({ id: PARTICIPANT_ID }).del()
    await broker.stop()
    await knex.destroy()
  })

  it('getParticipant echoes the latest indexed height as block_height', async () => {
    const res: any = await service.getParticipant({ params: { id: PARTICIPANT_ID }, meta: {} })

    expect(res.participant.id).toBe(PARTICIPANT_ID)
    expect(res.block_height).toBe(777)
  })

  it('getParticipant echoes At-Block-Height as block_height on the history path', async () => {
    const res: any = await service.getParticipant({ params: { id: PARTICIPANT_ID }, meta: { blockHeight: 150 } })

    expect(res.participant.id).toBe(PARTICIPANT_ID)
    expect(res.block_height).toBe(150)
  })

  it('getParticipantSession echoes the latest indexed height as block_height', async () => {
    const res: any = await service.getParticipantSession({ params: { id: SESSION_ID }, meta: {} })

    expect(res.session.id).toBe(SESSION_ID)
    expect(res.block_height).toBe(777)
  })

  it('getParticipantSession echoes At-Block-Height as block_height on the history path', async () => {
    const res: any = await service.getParticipantSession({ params: { id: SESSION_ID }, meta: { blockHeight: 150 } })

    expect(res.session.id).toBe(SESSION_ID)
    expect(res.block_height).toBe(150)
  })

  it('getParticipant scales the stored fee discounts to decimals exactly once', async () => {
    const res: any = await service.getParticipant({ params: { id: PARTICIPANT_ID }, meta: {} })

    expect(res.participant.issuance_fee_discount).toBe(0.25)
    expect(res.participant.verification_fee_discount).toBe(0.0001)
  })

  it('getParticipant scales the fee discounts once on the At-Block-Height history path', async () => {
    const res: any = await service.getParticipant({ params: { id: PARTICIPANT_ID }, meta: { blockHeight: 150 } })

    expect(res.participant.issuance_fee_discount).toBe(0.25)
    expect(res.participant.verification_fee_discount).toBe(0.0001)
  })

  it('listParticipants scales the fee discounts once', async () => {
    const res: any = await service.listParticipants({ params: { participant_id: PARTICIPANT_ID }, meta: {} })

    const participant = res.participants.find((entry: any) => entry.id === PARTICIPANT_ID)
    expect(participant.issuance_fee_discount).toBe(0.25)
    expect(participant.verification_fee_discount).toBe(0.0001)
  })
})
