import createKnex from 'knex'
import { applyActiveEffectiveFromFilter } from '../../../../src/services/crawl-pp/pp_state_utils'

describe('🧪 applyActiveEffectiveFromFilter', () => {
  const NOW_ISO = '2025-01-10T00:00:00.000Z'
  const qb = createKnex({ client: 'pg' })

  afterAll(async () => {
    await qb.destroy()
  })

  const sqlFor = (col?: (name: string) => string) => {
    const query = qb('participants').select('id')
    applyActiveEffectiveFromFilter(query, NOW_ISO, col)
    return query.toQuery()
  }

  it('matches rows already started and rows that never scheduled an effective_from', () => {
    const sql = sqlFor()
    expect(sql).toContain('"effective_from" is not null')
    expect(sql).toContain('"effective_from" is null')
    expect(sql).toContain('"op_state" is null')
    expect(sql).toContain("not in ('PENDING', 'TERMINATED')")
  })

  it('honours the table prefix used by history queries', () => {
    expect(sqlFor((name) => `ph.${name}`)).toContain('"ph"."effective_from"')
  })
})
