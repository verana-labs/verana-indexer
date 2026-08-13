import { describe, expect, test } from '@jest/globals'
import type { Knex } from 'knex'
import { getLatestIndexedBlockTime, resolveEvaluationTime } from '../../../src/common/utils/block_time'

const LATEST_BLOCK_TIME = '2026-08-12T18:04:45.268Z'
const HEIGHT_50_TIME = '2026-08-12T12:00:00.000Z'

type Recorded = { orderedBy: Array<[string, string]>; wheres: Array<[string, unknown, unknown]> }

function fakeDb(rowFor: (recorded: Recorded) => { time: string } | undefined): { db: Knex; recorded: Recorded } {
  const recorded: Recorded = { orderedBy: [], wheres: [] }
  const builder: any = {
    select: () => builder,
    orderBy: (column: string, direction: string) => {
      recorded.orderedBy.push([column, direction])
      return builder
    },
    where: (column: unknown, operator?: unknown, value?: unknown) => {
      recorded.wheres.push([String(column), operator, value])
      return builder
    },
    first: async () => rowFor(recorded),
  }
  return { db: (() => builder) as unknown as Knex, recorded }
}

describe('🧪 getLatestIndexedBlockTime', () => {
  test('returns the time of the highest indexed block', async () => {
    const { db, recorded } = fakeDb(() => ({ time: LATEST_BLOCK_TIME }))
    const result = await getLatestIndexedBlockTime({ db })
    expect(result.toISOString()).toBe(LATEST_BLOCK_TIME)
    expect(recorded.orderedBy).toEqual([['height', 'desc']])
    expect(recorded.wheres).toEqual([])
  })

  test('falls back when no block has been indexed yet', async () => {
    const fallback = new Date('2020-01-01T00:00:00.000Z')
    const { db } = fakeDb(() => undefined)
    expect((await getLatestIndexedBlockTime({ db, fallback })).toISOString()).toBe(fallback.toISOString())
  })

  test('falls back when the stored time is not a valid date', async () => {
    const fallback = new Date('2020-01-01T00:00:00.000Z')
    const { db } = fakeDb(() => ({ time: 'not-a-date' }))
    expect((await getLatestIndexedBlockTime({ db, fallback })).toISOString()).toBe(fallback.toISOString())
  })
})

describe('🧪 resolveEvaluationTime', () => {
  test('uses the latest indexed block when no height is requested', async () => {
    const { db, recorded } = fakeDb(() => ({ time: LATEST_BLOCK_TIME }))
    const result = await resolveEvaluationTime(undefined, { db })
    expect(result.toISOString()).toBe(LATEST_BLOCK_TIME)
    expect(recorded.wheres).toEqual([])
  })

  test('uses the requested block, matching at or before that height', async () => {
    const { db, recorded } = fakeDb(() => ({ time: HEIGHT_50_TIME }))
    const result = await resolveEvaluationTime(50, { db })
    expect(result.toISOString()).toBe(HEIGHT_50_TIME)
    expect(recorded.wheres).toEqual([['height', '<=', 50]])
    expect(recorded.orderedBy).toEqual([['height', 'desc']])
  })
})
