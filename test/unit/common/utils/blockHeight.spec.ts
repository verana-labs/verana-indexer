const mockCheckpointFirst = jest.fn()
const mockBlockMaxFirst = jest.fn()

jest.mock('../../../../src/common/utils/db_connection', () => ({
  __esModule: true,
  default: jest.fn((table: string) => {
    if (table === 'block_checkpoint') {
      return { where: jest.fn(() => ({ first: mockCheckpointFirst })) }
    }
    if (table === 'block') {
      return { max: jest.fn(() => ({ first: mockBlockMaxFirst })) }
    }
    throw new Error(`unexpected table ${table}`)
  }),
}))

import { getBlockHeight, getResolvedBlockHeight } from '../../../../src/common/utils/blockHeight'

describe('getResolvedBlockHeight', () => {
  beforeEach(() => jest.clearAllMocks())

  it('returns the provided block height without a DB lookup', async () => {
    expect(await getResolvedBlockHeight(42)).toBe(42)
    expect(mockCheckpointFirst).not.toHaveBeenCalled()
  })

  it('falls back to the latest indexed height from block_checkpoint', async () => {
    mockCheckpointFirst.mockResolvedValueOnce({ height: 175 })
    expect(await getResolvedBlockHeight()).toBe(175)
  })

  it('falls back to the latest block-table height when no checkpoint row exists', async () => {
    mockCheckpointFirst.mockResolvedValueOnce(undefined)
    mockBlockMaxFirst.mockResolvedValueOnce({ max: 1234 })
    expect(await getResolvedBlockHeight()).toBe(1234)
  })

  it('returns 0 when neither a checkpoint row nor an indexed block exists', async () => {
    mockCheckpointFirst.mockResolvedValueOnce(undefined)
    mockBlockMaxFirst.mockResolvedValueOnce(undefined)
    expect(await getResolvedBlockHeight()).toBe(0)
  })

  it('resolves the header value read by getBlockHeight', async () => {
    const ctx: any = { meta: { blockHeight: 7 } }
    expect(await getResolvedBlockHeight(getBlockHeight(ctx))).toBe(7)
  })
})
