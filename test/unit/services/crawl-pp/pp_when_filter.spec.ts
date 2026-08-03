import Knex from 'knex'

import ParticipantAPIService from '../../../../src/services/crawl-pp/pp_apis.service'

const knex = Knex({ client: 'pg' })

const applyFilters = (whenIso: string) => {
  const query = knex('participants')
  const apply = (ParticipantAPIService.prototype as any).applyBaseListFiltersToQuery
  apply.call({}, query, {}, undefined, undefined, whenIso, false, false, false, '2026-08-03T00:00:00.000Z')
  return query.toSQL()
}

describe('listParticipants when filter', () => {
  const WHEN = '2026-07-18T19:41:00.000Z'

  it('filters on the effective range, not on modified', () => {
    const { sql, bindings } = applyFilters(WHEN)

    expect(sql).toContain('"effective_from" is null or "effective_from" <= ?')
    expect(sql).toContain('"effective_until" is null or "effective_until" > ?')
    expect(sql).not.toContain('"modified" <=')
    expect(bindings).toEqual([WHEN, WHEN])
  })
})
