import knex from '../../../../src/common/utils/db_connection'
import { calculateCredentialSchemaStatsBatch } from '../../../../src/services/crawl-cs/cs_stats'

const SCHEMA_ID = 909101

const WEIGHT_A = '250000000000000000123'
const WEIGHT_B = '250000000000000000456'
const EXACT_TOTAL = '500000000000000000579'

const baseParticipant = {
  schema_id: SCHEMA_ID,
  validation_fees: '0',
  issuance_fees: '0',
  verification_fees: '0',
  deposit: '0',
  slashed_deposit: '0',
  repaid_deposit: '0',
  op_current_fees: '0',
  op_current_deposit: '0',
  op_validator_deposit: '0',
  issuance_fee_discount: '0',
  verification_fee_discount: '0',
  vs_operator_authz_enabled: false,
  vs_operator_authz_with_feegrant: false,
  participants: 0,
  participants_ecosystem: 0,
}

describe('cs_stats weight aggregation', () => {
  beforeAll(async () => {
    await knex('participants').where('schema_id', SCHEMA_ID).del()
    await knex('participants').insert([
      { ...baseParticipant, id: 909101, role: 'ISSUER', weight: WEIGHT_A },
      { ...baseParticipant, id: 909102, role: 'VERIFIER', weight: WEIGHT_B },
    ])
  })

  afterAll(async () => {
    await knex('participants').where('schema_id', SCHEMA_ID).del()
    await knex.destroy()
  })

  it('sums participant weights far above 2^53 without losing a unit', async () => {
    const stats = await calculateCredentialSchemaStatsBatch([SCHEMA_ID])

    expect(stats.get(SCHEMA_ID)?.weight).toBe(BigInt(EXACT_TOTAL))
  })
})
