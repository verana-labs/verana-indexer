import { ServiceBroker } from 'moleculer'
import knex from '../../../../src/common/utils/db_connection'
import CredentialSchemaDatabaseService from '../../../../src/services/crawl-cs/cs_database.service'

const SCHEMA_ID = 9201
const HEIGHT = 53041
const now = '2026-11-10T01:23:51.000Z'

const ledgerSchema = {
  id: SCHEMA_ID,
  ecosystem_id: '2',
  json_schema: JSON.stringify({ type: 'object', title: 'AtomicSchema', properties: { name: { type: 'string' } } }),
  issuer_grantor_validation_validity_period: 365,
  verifier_grantor_validation_validity_period: 365,
  issuer_validation_validity_period: 365,
  verifier_validation_validity_period: 365,
  holder_validation_validity_period: 365,
  issuer_onboarding_mode: 'OPEN',
  verifier_onboarding_mode: 'OPEN',
  archived: null,
  created: now,
  modified: now,
}

describe('CredentialSchemaDatabaseService.syncFromLedger atomicity', () => {
  const broker = new ServiceBroker({ logger: false })
  const service: any = broker.createService(CredentialSchemaDatabaseService)
  const sync = () =>
    service.syncFromLedger({
      params: { ledgerResponse: { schema: { ...ledgerSchema } }, blockHeight: HEIGHT },
      meta: {},
    })

  const cleanup = async () => {
    await knex.raw('ALTER TABLE credential_schema_history DROP CONSTRAINT IF EXISTS fail_cs_history')
    await knex('credential_schema_history').where({ credential_schema_id: SCHEMA_ID }).del()
    await knex('credential_schemas').where({ id: SCHEMA_ID }).del()
  }

  beforeAll(cleanup)

  afterAll(async () => {
    await cleanup()
    await broker.stop()
    await knex.destroy()
  })

  it('writes no schema row when its history insert fails, and both rows on the retry', async () => {
    await knex.raw('ALTER TABLE credential_schema_history ADD CONSTRAINT fail_cs_history CHECK (false) NOT VALID')
    const failed = await sync()
    expect(failed?.success).not.toBe(true)
    expect(await knex('credential_schemas').where({ id: SCHEMA_ID }).first()).toBeUndefined()

    await knex.raw('ALTER TABLE credential_schema_history DROP CONSTRAINT fail_cs_history')
    const retried = await sync()
    expect(retried?.success).toBe(true)
    expect(await knex('credential_schemas').where({ id: SCHEMA_ID }).first()).toBeDefined()
    const history = await knex('credential_schema_history').where({ credential_schema_id: SCHEMA_ID, height: HEIGHT })
    expect(history.map((row) => row.action)).toEqual(['create'])
  })
})
