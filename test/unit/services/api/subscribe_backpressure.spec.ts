jest.mock('../../../../src/services/manager/indexer_status.manager', () => ({
  __esModule: true,
  indexerStatusManager: {
    getDetailedStatus: jest.fn(async () => ({ lastProcessedBlock: 0, lastBlockTime: '2026-01-01T00:00:00Z' })),
  },
}))

jest.mock('../../../../src/common/utils/db_connection', () => ({
  __esModule: true,
  default: Object.assign(jest.fn(), { raw: jest.fn() }),
}))

import { WebSocket } from 'ws'
import { BaseSubscribeServer, type ControlParseResult } from '../../../../src/services/api/subscribe_ws_server'

class TestServer extends BaseSubscribeServer<{ action: string }, { established: boolean }> {
  protected readonly path = '/test'
  protected createInitialState() {
    return { established: false }
  }
  protected parseControl(_raw: string): ControlParseResult<{ action: string }> {
    return { ok: true, message: { action: 'subscribe' } }
  }
  protected applyControl(state: { established: boolean }) {
    return state
  }
  // expose protected members for the test
  public send(ws: WebSocket) {
    return this.sendJson(ws, { type: 'block', block: 1 })
  }
  public track(ws: WebSocket) {
    ;(this as any).clients.set(ws, this.createInitialState())
  }
  public isTracked(ws: WebSocket) {
    return (this as any).clients.has(ws)
  }
}

function fakeWs(bufferedAmount: number) {
  return {
    readyState: WebSocket.OPEN,
    bufferedAmount,
    send: jest.fn(),
    close: jest.fn(),
  } as unknown as WebSocket & { send: jest.Mock; close: jest.Mock }
}

describe('BaseSubscribeServer backpressure', () => {
  const OLD_ENV = process.env.WS_MAX_BUFFERED_BYTES

  afterEach(() => {
    if (OLD_ENV === undefined) delete process.env.WS_MAX_BUFFERED_BYTES
    else process.env.WS_MAX_BUFFERED_BYTES = OLD_ENV
  })

  it('closes a client with 1011 and drops it when its buffer exceeds the limit', () => {
    const server = new TestServer()
    const ws = fakeWs(9 * 1024 * 1024) // above the 8 MB default
    server.track(ws)

    const ok = server.send(ws)

    expect(ok).toBe(false)
    expect(ws.send).not.toHaveBeenCalled()
    expect(ws.close).toHaveBeenCalledWith(1011, 'Server overloaded')
    expect(server.isTracked(ws)).toBe(false)
  })

  it('delivers normally to a client that is draining its buffer', () => {
    const server = new TestServer()
    const ws = fakeWs(0)
    server.track(ws)

    const ok = server.send(ws)

    expect(ok).toBe(true)
    expect(ws.send).toHaveBeenCalledTimes(1)
    expect(ws.close).not.toHaveBeenCalled()
    expect(server.isTracked(ws)).toBe(true)
  })
})
