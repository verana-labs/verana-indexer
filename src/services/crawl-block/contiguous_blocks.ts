import type { JsonRpcSuccessResponse } from '@cosmjs/json-rpc'

export interface FetchedBlock {
  block: JsonRpcSuccessResponse
  blockResults: JsonRpcSuccessResponse
}

const isBlockAt = (response: JsonRpcSuccessResponse, height: number) =>
  Number((response.result as { block?: { header?: { height?: string } } } | null)?.block?.header?.height) === height

const isBlockResultsAt = (response: JsonRpcSuccessResponse, height: number) =>
  Number((response.result as { height?: string } | null)?.height) === height

export function takeContiguousBlocks(
  settled: PromiseSettledResult<JsonRpcSuccessResponse>[],
  startBlock: number
): { blocks: FetchedBlock[]; firstMissingHeight: number | null } {
  const blocks: FetchedBlock[] = []
  for (let i = 0; i < settled.length; i += 2) {
    const height = startBlock + i / 2
    const block = settled[i]
    const blockResults = settled[i + 1]
    if (
      block?.status !== 'fulfilled' ||
      blockResults?.status !== 'fulfilled' ||
      !isBlockAt(block.value, height) ||
      !isBlockResultsAt(blockResults.value, height)
    ) {
      return { blocks, firstMissingHeight: height }
    }
    blocks.push({ block: block.value, blockResults: blockResults.value })
  }
  return { blocks, firstMissingHeight: null }
}
