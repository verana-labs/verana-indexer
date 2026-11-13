import knex from '../../../src/common/utils/db_connection'
import { BlockCheckpoint } from '../../../src/models'

describe('BlockCheckpoint.getCheckpoint with a dependency at 0', () => {
  const jobs = ['crawl:block', 'dependency-test:job', 'dependency-test:dep']
  const blockHeights = [5_000, 5_001]
  let saved: Array<{ job_name: string; height: number }> = []

  beforeAll(async () => {
    saved = await knex('block_checkpoint').whereIn('job_name', jobs).select('job_name', 'height')
    await knex('block_checkpoint').whereIn('job_name', jobs).delete()
    await knex('block_checkpoint').insert([
      { job_name: 'crawl:block', height: 5_001 },
      { job_name: 'dependency-test:job', height: 0 },
    ])
    for (const height of blockHeights) {
      await knex('block')
        .insert({ height, hash: `dep-${height}`, time: new Date(), proposer_address: 'v', data: {} })
        .onConflict('height')
        .ignore()
    }
  })

  afterAll(async () => {
    await knex('block_checkpoint').whereIn('job_name', jobs).delete()
    if (saved.length > 0) await knex('block_checkpoint').insert(saved)
    await knex('block')
      .whereIn(
        'hash',
        blockHeights.map((height) => `dep-${height}`)
      )
      .delete()
    await knex.destroy()
  })

  it('waits for a dependency that was reset to 0 instead of running to the highest block', async () => {
    await knex('block_checkpoint').insert({ job_name: 'dependency-test:dep', height: 0 })

    const [start, end] = await BlockCheckpoint.getCheckpoint('dependency-test:job', ['dependency-test:dep'])

    expect([start, end]).toEqual([0, 0])
  })
})
