import { ServiceBroker } from 'moleculer'
import knex from '../../../../src/common/utils/db_connection'
import ParticipantIngestService from '../../../../src/services/crawl-pp/pp_database.service'

const PARTICIPANT_ID = 9101
const HEIGHT = 53046
const TX_HASH = 'A1F38F907B0B00708E9B2BA8D2224E98B1818BBB1979A39AAC491D773EDC77C2'
const MSG_TYPE = '/verana.pp.v1.MsgSelfCreateParticipant'

const SESSION_ID = 'b40f1b01-2425-4559-bbd3-86889d5af55c'
const SESSION_TX_HASH = 'D7F74163E0BE0DBC3C96D641EDB2E9E3876F50159AE629D00AB4F4F5F54B1C2C'
const SESSION_MSG_TYPE = '/verana.pp.v1.MsgCreateOrUpdateParticipantSession'

const ledgerSession = {
  id: SESSION_ID,
  corporation_id: 6,
  vs_operator: 'verana12t9rtx3j5z08fxazrqw99j9qkgyzgxavwwl9ge',
  agent_participant_id: 0,
  wallet_agent_participant_id: 0,
  session_records: [{ created: '2026-11-10T02:28:31.325Z', issuer_participant_id: PARTICIPANT_ID }],
  created: '2026-11-10T02:28:31.325Z',
  modified: '2026-11-10T02:28:31.325Z',
}

const ledgerParticipant = {
  id: String(PARTICIPANT_ID),
  schema_id: '9100',
  role: 'ISSUER',
  did: 'did:example:issuer-replay',
  corporation_id: 6,
  validator_participant_id: '9100',
  created: '2026-11-10T01:24:16.296Z',
  modified: '2026-11-10T01:24:16.296Z',
  effective_from: '2026-11-10T01:24:58Z',
  effective_until: null,
  op_state: 'VALIDATED',
}

describe('ParticipantIngestService.syncParticipantFromLedger replay', () => {
  const broker = new ServiceBroker({ logger: false })
  const service: any = broker.createService(ParticipantIngestService)
  const sync = () => service.syncParticipantFromLedger(ledgerParticipant, HEIGHT, TX_HASH, MSG_TYPE)

  const cleanup = async () => {
    await knex('participant_session_history').where({ session_id: SESSION_ID }).del()
    await knex('participant_sessions').where({ id: SESSION_ID }).del()
    await knex('participant_scheduled_flips').where({ participant_id: PARTICIPANT_ID }).del()
    await knex('participant_history').where({ participant_id: PARTICIPANT_ID }).del()
    await knex('participants').where({ id: PARTICIPANT_ID }).del()
  }

  beforeAll(cleanup)

  afterAll(async () => {
    await cleanup()
    await broker.stop()
    await knex.destroy()
  })

  it('writes the history row and scheduled flips once when the same message is synced again', async () => {
    expect((await sync())?.success).toBe(true)
    const history = await knex('participant_history').where({ participant_id: PARTICIPANT_ID })
    const flips = await knex('participant_scheduled_flips').where({ participant_id: PARTICIPANT_ID })
    expect(history).toHaveLength(1)
    expect(flips.length).toBeGreaterThan(0)

    expect((await sync())?.success).toBe(true)
    expect(await knex('participant_history').where({ participant_id: PARTICIPANT_ID })).toHaveLength(1)
    expect(await knex('participant_scheduled_flips').where({ participant_id: PARTICIPANT_ID })).toHaveLength(
      flips.length
    )
    const participant = await knex('participants').where({ id: PARTICIPANT_ID }).first()
    expect(participant.last_valid_flip_version).toBe(flips[0].version)
  })

  it('writes the session history row once when the same session message is synced again', async () => {
    const syncSession = () =>
      service.syncParticipantSessionFromLedger(ledgerSession, HEIGHT, SESSION_TX_HASH, SESSION_MSG_TYPE)

    await syncSession()
    await syncSession()

    expect(await knex('participant_session_history').where({ session_id: SESSION_ID })).toHaveLength(1)
  })
})
