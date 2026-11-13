import type { JsonRpcSuccessResponse } from '@cosmjs/json-rpc'
import { takeContiguousBlocks } from '../../../../src/services/crawl-block/contiguous_blocks'

const ok = (result: unknown): PromiseSettledResult<JsonRpcSuccessResponse> => ({
  status: 'fulfilled',
  value: { jsonrpc: '2.0', id: 1, result } as JsonRpcSuccessResponse,
})
const failed: PromiseSettledResult<JsonRpcSuccessResponse> = { status: 'rejected', reason: new Error('boom') }
const block = (height: number) => ok({ block: { header: { height: String(height) } } })
const results = (height: number) => ok({ height: String(height), txs_results: [] })

describe('takeContiguousBlocks', () => {
  it('keeps every block when the whole range was fetched', () => {
    const out = takeContiguousBlocks([block(10), results(10), block(11), results(11)], 10)
    expect(out.blocks).toHaveLength(2)
    expect(out.firstMissingHeight).toBeNull()
  })

  it('stops at a block whose fetch failed, even when later blocks were fetched', () => {
    const out = takeContiguousBlocks([block(10), results(10), failed, results(11), block(12), results(12)], 10)
    expect(out.blocks).toHaveLength(1)
    expect(out.firstMissingHeight).toBe(11)
  })

  it('stops at a block whose result is null', () => {
    const out = takeContiguousBlocks([block(10), results(10), ok(null), results(11), block(12), results(12)], 10)
    expect(out.blocks).toHaveLength(1)
    expect(out.firstMissingHeight).toBe(11)
  })

  it('stops at block results without a height', () => {
    const out = takeContiguousBlocks([block(10), ok({}), block(11), results(11)], 10)
    expect(out.blocks).toHaveLength(0)
    expect(out.firstMissingHeight).toBe(10)
  })

  it('stops at a response for another height', () => {
    const out = takeContiguousBlocks([block(10), results(10), block(12), results(12)], 10)
    expect(out.blocks).toHaveLength(1)
    expect(out.firstMissingHeight).toBe(11)
  })
})
