import createKnex from 'knex'
import { applyParticipantStateFilter, type ParticipantState } from '../../../../src/services/crawl-pp/pp_state_utils'

describe('🧪 applyParticipantStateFilter', () => {
  const NOW_ISO = '2025-01-10T00:00:00.000Z'
  const qb = createKnex({ client: 'pg' })

  afterAll(async () => {
    await qb.destroy()
  })

  const sqlFor = (state: ParticipantState, col?: (name: string) => string) => {
    const query = qb('participants').select('id')
    applyParticipantStateFilter(query, state, NOW_ISO, col)
    return query.toQuery()
  }

  it('REPAID requires a slash and a repayment at or after it', () => {
    expect(sqlFor('REPAID')).toContain('"slashed" is not null and "repaid" >= "slashed"')
  })

  it('SLASHED requires a slash that was never repaid or was repaid before it', () => {
    expect(sqlFor('SLASHED')).toContain('"slashed" is not null and ("repaid" is null or "repaid" < "slashed")')
  })

  it('REVOKED matches unslashed entries revoked at or before the evaluation instant', () => {
    expect(sqlFor('REVOKED')).toContain(`"slashed" is null and "revoked" <= '${NOW_ISO}'`)
  })

  it('EXPIRED matches entries whose window ended at or before the evaluation instant', () => {
    const sql = sqlFor('EXPIRED')
    expect(sql).toContain(`"revoked" is null or "revoked" > '${NOW_ISO}'`)
    expect(sql).toContain(`"effective_until" <= '${NOW_ISO}'`)
  })

  it('FUTURE matches unslashed, unrevoked entries that start after the evaluation instant', () => {
    const sql = sqlFor('FUTURE')
    expect(sql).toContain('"slashed" is null')
    expect(sql).toContain(`"revoked" is null or "revoked" > '${NOW_ISO}'`)
    expect(sql).toContain(`"effective_until" is null or "effective_until" > '${NOW_ISO}'`)
    expect(sql).toContain(`"effective_from" > '${NOW_ISO}'`)
  })

  it('INACTIVE matches unslashed, unrevoked, never-started entries with a pending onboarding process', () => {
    const sql = sqlFor('INACTIVE')
    expect(sql).toContain('"slashed" is null')
    expect(sql).toContain(`"revoked" is null or "revoked" > '${NOW_ISO}'`)
    expect(sql).toContain(`"effective_until" is null or "effective_until" > '${NOW_ISO}'`)
    expect(sql).toContain(`"effective_from" is null and "op_state" in ('PENDING', 'TERMINATED')`)
  })

  it('ACTIVE excludes slashed entries', () => {
    expect(sqlFor('ACTIVE')).toContain('"slashed" is null')
  })

  it('honours the table prefix used by joined queries', () => {
    expect(sqlFor('REPAID', (name) => `p.${name}`)).toContain('"p"."repaid" >= "p"."slashed"')
  })
})
