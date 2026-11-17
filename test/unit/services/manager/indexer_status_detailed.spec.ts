import { BULL_JOB_NAME } from '../../../../src/common/constant'
import knex from '../../../../src/common/utils/db_connection'
import { indexerStatusManager } from '../../../../src/services/manager/indexer_status.manager'

describe('indexerStatusManager.getDetailedStatus', () => {
  const jobs = [BULL_JOB_NAME.CRAWL_BLOCK, BULL_JOB_NAME.HANDLE_TRANSACTION]
  let saved: Array<{ job_name: string; height: number }> = []

  const setHeight = (jobName: string, height: number) =>
    knex('block_checkpoint').insert({ job_name: jobName, height }).onConflict('job_name').merge({ height })

  beforeAll(async () => {
    saved = await knex('block_checkpoint').whereIn('job_name', jobs).select('job_name', 'height')
  })

  afterAll(async () => {
    await knex('block_checkpoint').whereIn('job_name', jobs).delete()
    if (saved.length > 0) await knex('block_checkpoint').insert(saved)
    await knex.destroy()
  })

  it('reports the last fully processed block, not the last crawled one', async () => {
    await setHeight(BULL_JOB_NAME.CRAWL_BLOCK, 5_000)
    await setHeight(BULL_JOB_NAME.HANDLE_TRANSACTION, 4_200)

    const status = await indexerStatusManager.getDetailedStatus()

    expect(status.lastProcessedBlock).toBe(4_200)
  })
})
