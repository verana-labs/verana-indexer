import { BULL_JOB_NAME } from '../../../../src/common/constant'
import { getBlockHeight, getResolvedBlockHeight } from '../../../../src/common/utils/blockHeight'
import knex from '../../../../src/common/utils/db_connection'

const JOB = BULL_JOB_NAME.HANDLE_TRANSACTION

async function setCheckpoint(height: number | null) {
  await knex('block_checkpoint').where({ job_name: JOB }).del()
  if (height !== null) await knex('block_checkpoint').insert({ job_name: JOB, height })
}

describe('getResolvedBlockHeight', () => {
  afterAll(async () => {
    await setCheckpoint(null)
    await knex.destroy()
  })

  it('returns the At-Block-Height value without touching the checkpoint', async () => {
    await setCheckpoint(4242)
    expect(await getResolvedBlockHeight(42)).toBe(42)
  })

  it('resolves the header value read by getBlockHeight', async () => {
    const ctx: any = { meta: { blockHeight: 7 } }
    expect(await getResolvedBlockHeight(getBlockHeight(ctx))).toBe(7)
  })

  it('falls back to the handle:transaction checkpoint row', async () => {
    await setCheckpoint(4242)
    expect(await getResolvedBlockHeight()).toBe(4242)
    expect(await getResolvedBlockHeight(undefined)).toBe(4242)
  })

  it('returns 0 when no checkpoint row exists, like the block-height endpoint', async () => {
    await setCheckpoint(null)
    expect(await getResolvedBlockHeight()).toBe(0)
  })
})
