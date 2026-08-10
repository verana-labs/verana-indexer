import Knex from 'knex'

import ParticipantAPIService from '../../../../src/services/crawl-pp/pp_apis.service'

const knex = Knex({ client: 'pg' })

const applyFilters = (whenIso: string | undefined, params: Record<string, unknown> = {}) => {
  const query = knex('participants')
  const apply = (ParticipantAPIService.prototype as any).applyBaseListFiltersToQuery
  apply.call({}, query, params, undefined, undefined, whenIso, false, false, false, '2026-08-03T00:00:00.000Z')
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

describe('listParticipants ecosystem_id filter', () => {
  it('resolves the ecosystem through the participant credential schema', () => {
    const { sql, bindings } = applyFilters(undefined, { ecosystem_id: 7 })

    expect(sql).toContain('"schema_id" in (select "id" from "credential_schemas" where "ecosystem_id" = ?)')
    expect(bindings).toEqual([7])
  })

  it('is omitted when no ecosystem_id is requested', () => {
    const { sql } = applyFilters(undefined, { schema_id: 3 })

    expect(sql).not.toContain('credential_schemas')
  })
})
