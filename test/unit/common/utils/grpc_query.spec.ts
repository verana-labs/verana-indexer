import { QueryClient } from '@cosmjs/stargate'
import { Tendermint37Client } from '@cosmjs/tendermint-rpc'
import { isAbciNotFound, withAbciQueryClient } from '../../../../src/common/utils/grpc_query'

jest.mock('@cosmjs/tendermint-rpc', () => ({
  Tendermint37Client: { connect: jest.fn() },
}))
jest.mock('@cosmjs/stargate', () => ({
  QueryClient: jest.fn(),
}))

const FUTURE_HEIGHT = new Error(
  'Query failed with (26): cannot query with height in the future; please provide a valid height: invalid height'
)
const NOT_FOUND = new Error(
  'Query failed with (38): rpc error: code = NotFound desc = credential schema not found: key not found'
)

const queryAbci = jest.fn()
const disconnect = jest.fn()
const status = jest.fn()

async function query(blockHeight: number | undefined): Promise<Uint8Array> {
  return withAbciQueryClient(blockHeight, (rpc) =>
    rpc.request('verana.cs.v1.Query', 'GetCredentialSchema', new Uint8Array())
  )
}

describe('withAbciQueryClient', () => {
  beforeEach(() => {
    jest.useFakeTimers()
    process.env.RPC_ENDPOINT = 'http://rpc.test'
    queryAbci.mockReset()
    disconnect.mockReset()
    status.mockReset().mockResolvedValue({ syncInfo: { latestBlockHeight: 321408 } })
    ;(Tendermint37Client.connect as jest.Mock).mockResolvedValue({ disconnect, status })
    ;(QueryClient as unknown as jest.Mock).mockImplementation(() => ({ queryAbci }))
  })

  afterEach(() => {
    jest.useRealTimers()
  })

  it('waits for a backend that has not committed the height yet', async () => {
    const value = new Uint8Array([1])
    queryAbci.mockRejectedValueOnce(FUTURE_HEIGHT).mockRejectedValueOnce(FUTURE_HEIGHT).mockResolvedValue({ value })

    const result = query(321409)
    await jest.advanceTimersByTimeAsync(1500)

    await expect(result).resolves.toBe(value)
    expect(queryAbci).toHaveBeenCalledTimes(3)
    expect(queryAbci).toHaveBeenLastCalledWith('/verana.cs.v1.Query/GetCredentialSchema', expect.anything(), 321409)
    expect(disconnect).toHaveBeenCalledTimes(1)
  })

  it('throws once the height never becomes available', async () => {
    queryAbci.mockRejectedValue(FUTURE_HEIGHT)

    const result = query(321409)
    const assertion = expect(result).rejects.toBe(FUTURE_HEIGHT)
    await jest.advanceTimersByTimeAsync(20000)

    await assertion
    expect(queryAbci).toHaveBeenCalledTimes(8)
    expect(disconnect).toHaveBeenCalledTimes(1)
  })

  it('does not wait for a height the chain is nowhere near', async () => {
    queryAbci.mockRejectedValue(FUTURE_HEIGHT)

    await expect(query(7085567)).rejects.toBe(FUTURE_HEIGHT)
    expect(queryAbci).toHaveBeenCalledTimes(1)
  })

  it('does not retry other errors', async () => {
    queryAbci.mockRejectedValue(NOT_FOUND)

    await expect(query(321409)).rejects.toBe(NOT_FOUND)
    expect(queryAbci).toHaveBeenCalledTimes(1)
  })

  it('does not retry a query that is not pinned to a height', async () => {
    queryAbci.mockRejectedValue(FUTURE_HEIGHT)

    await expect(query(undefined)).rejects.toBe(FUTURE_HEIGHT)
    expect(queryAbci).toHaveBeenCalledTimes(1)
  })
})

describe('isAbciNotFound', () => {
  it('matches the gRPC NotFound status only', () => {
    expect(isAbciNotFound(NOT_FOUND)).toBe(true)
    expect(isAbciNotFound(FUTURE_HEIGHT)).toBe(false)
    expect(isAbciNotFound(new Error('getaddrinfo ENOTFOUND rpc.testnet.verana.network'))).toBe(false)
  })
})
