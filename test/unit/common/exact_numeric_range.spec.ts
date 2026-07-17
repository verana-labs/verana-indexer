import { afterAll, beforeAll, describe, expect, test } from '@jest/globals'
import knex from '../../../src/common/utils/db_connection'
import {
  applyExactRangeToQuery,
  filterRowsByExactRange,
  isImpossibleExactRange,
  parseExactInteger,
} from '../../../src/common/utils/exact_numeric_range'

const ABOVE_2_53 = '250000000000000000123'
const NEIGHBOUR_BELOW = '250000000000000000000'
const BIGINT_BOUNDARY = '9007199254740993'

describe('parseExactInteger', () => {
  test('keeps precision far above Number.MAX_SAFE_INTEGER', () => {
    expect(parseExactInteger(ABOVE_2_53)).toBe(BigInt(ABOVE_2_53))
    expect(parseExactInteger(BIGINT_BOUNDARY)).toBe(BigInt(BIGINT_BOUNDARY))
  })

  test('distinguishes values that collapse onto each other as Number', () => {
    expect(Number(ABOVE_2_53)).toBe(Number(NEIGHBOUR_BELOW))
    expect(parseExactInteger(ABOVE_2_53)).not.toBe(parseExactInteger(NEIGHBOUR_BELOW))
  })

  test('returns undefined for absent or non-integer input', () => {
    expect(parseExactInteger(undefined)).toBeUndefined()
    expect(parseExactInteger('')).toBeUndefined()
    expect(parseExactInteger('abc')).toBeUndefined()
    expect(parseExactInteger('1.5')).toBeUndefined()
    expect(parseExactInteger(Number.NaN)).toBeUndefined()
  })
})

describe('isImpossibleExactRange', () => {
  test('is true only when the half-open range cannot contain anything', () => {
    expect(isImpossibleExactRange('10', '10')).toBe(true)
    expect(isImpossibleExactRange('11', '10')).toBe(true)
    expect(isImpossibleExactRange('10', '11')).toBe(false)
    expect(isImpossibleExactRange('10', undefined)).toBe(false)
  })
})

describe('applyExactRangeToQuery', () => {
  const makeQuery = () => {
    const calls: Array<{ sql: string; bindings: any[] }> = []
    const query: any = {
      calls,
      whereRaw(sql: string, bindings: any[] = []) {
        calls.push({ sql, bindings })
        return query
      },
    }
    return query
  }

  test('binds the bound as an exact numeric string, never a JS number', () => {
    const query = makeQuery()
    applyExactRangeToQuery(query, 'weight', ABOVE_2_53, undefined)

    expect(query.calls).toHaveLength(1)
    expect(query.calls[0].sql).toBe('?? >= ?::numeric')
    expect(query.calls[0].bindings).toEqual(['weight', ABOVE_2_53])
  })

  test('emits a half-open range', () => {
    const query = makeQuery()
    applyExactRangeToQuery(query, 'weight', '10', '20')

    expect(query.calls.map((c: any) => c.sql)).toEqual(['?? >= ?::numeric', '?? < ?::numeric'])
    expect(query.calls[1].bindings).toEqual(['weight', '20'])
  })

  test('short-circuits an empty range', () => {
    const query = makeQuery()
    applyExactRangeToQuery(query, 'weight', '10', '10')

    expect(query.calls).toEqual([{ sql: '1 = 0', bindings: [] }])
  })
})

describe('filterRowsByExactRange', () => {
  const rows = [{ weight: NEIGHBOUR_BELOW }, { weight: ABOVE_2_53 }]

  test('excludes a row that Number-based comparison would wrongly include', () => {
    const filtered = filterRowsByExactRange(rows, ABOVE_2_53, undefined, (r) => r.weight)
    expect(filtered).toEqual([{ weight: ABOVE_2_53 }])
  })

  test('applies the max bound exclusively', () => {
    const filtered = filterRowsByExactRange(rows, undefined, ABOVE_2_53, (r) => r.weight)
    expect(filtered).toEqual([{ weight: NEIGHBOUR_BELOW }])
  })

  test('treats missing values as zero', () => {
    const filtered = filterRowsByExactRange([{ weight: null }], '1', undefined, (r) => r.weight)
    expect(filtered).toEqual([])
  })

  test('returns rows untouched when no bound is given', () => {
    expect(filterRowsByExactRange(rows, undefined, undefined, (r) => r.weight)).toBe(rows)
  })
})

describe('against postgres', () => {
  const TABLE = 'exact_numeric_range_probe'

  beforeAll(async () => {
    await knex.schema.dropTableIfExists(TABLE)
    await knex.schema.createTable(TABLE, (table) => {
      table.bigInteger('id')
      table.specificType('weight', 'NUMERIC(38,0)')
      table.bigInteger('verified')
    })
    await knex(TABLE).insert([
      { id: 1, weight: NEIGHBOUR_BELOW, verified: '9007199254740992' },
      { id: 2, weight: ABOVE_2_53, verified: BIGINT_BOUNDARY },
    ])
  })

  afterAll(async () => {
    await knex.schema.dropTableIfExists(TABLE)
    await knex.destroy()
  })

  test('a numeric column round-trips as an exact string', async () => {
    const row = await knex(TABLE).where('id', 2).first()

    expect(typeof row.weight).toBe('string')
    expect(row.weight).toBe(ABOVE_2_53)
  })

  test('bigint columns stay numbers so ids and heights are unaffected', async () => {
    const row = await knex(TABLE).where('id', 2).first()

    expect(typeof row.id).toBe('number')
  })

  test('the value survives the subquery-plus-star shape the list endpoints use', async () => {
    const ranked = knex(TABLE)
      .select('*', knex.raw('ROW_NUMBER() OVER (PARTITION BY id ORDER BY id DESC) as rn'))
      .as('ranked')
    const row = await knex.from(ranked).where('rn', 1).andWhere('id', 2).first()

    expect(row.weight).toBe(ABOVE_2_53)
  })

  test('min bound does not produce the false positive a JS number would', async () => {
    const rows = await applyExactRangeToQuery(knex(TABLE), 'weight', ABOVE_2_53, undefined).orderBy('id')

    expect(rows.map((r: any) => r.id)).toEqual([2])
  })

  test('filters a bigint column past 2^53 exactly', async () => {
    const rows = await applyExactRangeToQuery(knex(TABLE), 'verified', BIGINT_BOUNDARY, undefined).orderBy('id')

    expect(rows.map((r: any) => r.id)).toEqual([2])
  })

  test('max bound is exclusive and exact', async () => {
    const rows = await applyExactRangeToQuery(knex(TABLE), 'weight', undefined, ABOVE_2_53).orderBy('id')

    expect(rows.map((r: any) => r.id)).toEqual([1])
  })

  test('an empty half-open range returns nothing', async () => {
    const rows = await applyExactRangeToQuery(knex(TABLE), 'weight', ABOVE_2_53, ABOVE_2_53)

    expect(rows).toEqual([])
  })
})
