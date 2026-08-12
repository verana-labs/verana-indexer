import createKnex from 'knex'
import { applyActiveParticipantFilter } from '../../../../src/services/crawl-pp/pp_state_utils'

describe('🧪 applyActiveParticipantFilter', () => {
  const NOW_ISO = '2025-01-10T00:00:00.000Z'
  const qb = createKnex({ client: 'pg' })

  afterAll(async () => {
    await qb.destroy()
  })

  const sqlFor = (col?: (name: string) => string) => {
    const query = qb('participants').distinct('schema_id')
    applyActiveParticipantFilter(query, NOW_ISO, col)
    return query.toQuery()
  }

  it('excludes repaid and slashed entries', () => {
    const sql = sqlFor()
    expect(sql).toContain('"repaid" is null')
    expect(sql).toContain('"slashed" is null')
  })

  it('excludes entries revoked at or before the evaluation instant', () => {
    expect(sqlFor()).toContain(`"revoked" is null or "revoked" >= '${NOW_ISO}'`)
  })

  it('keeps only entries whose effective window covers the evaluation instant', () => {
    const sql = sqlFor()
    expect(sql).toContain(`"effective_until" is null or "effective_until" >= '${NOW_ISO}'`)
    expect(sql).toContain(`"effective_from" is not null and "effective_from" <= '${NOW_ISO}'`)
  })

  it('keeps onboarded entries that never got an effective_from', () => {
    const sql = sqlFor()
    expect(sql).toContain(
      `"effective_from" is null and ("op_state" is null or "op_state" not in ('PENDING', 'TERMINATED'))`
    )
  })

  it('honours the table prefix used by joined queries', () => {
    expect(sqlFor((name) => `p.${name}`)).toContain('"p"."effective_from"')
  })
})
