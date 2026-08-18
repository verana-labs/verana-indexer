import { createServer, Server } from 'http'
import { type RawData, WebSocket } from 'ws'
import { VtSubscribeBroadcaster } from '../../../../src/services/api/vt_subscribe_broadcaster'
import type { VtRawChange } from '../../../../src/services/api/vt_subscribe_protocol'

jest.setTimeout(15000)

function waitForMessage(ws: WebSocket, predicate: (message: any) => boolean, timeoutMs = 5000): Promise<any> {
  return new Promise((resolve, reject) => {
    const timeout = setTimeout(() => {
      ws.off('message', onMessage)
      reject(new Error('Timed out waiting for WebSocket message'))
    }, timeoutMs)
    const onMessage = (data: RawData) => {
      const message = JSON.parse(data.toString())
      if (!predicate(message)) return
      clearTimeout(timeout)
      ws.off('message', onMessage)
      resolve(message)
    }
    ws.on('message', onMessage)
  })
}

function waitForOpen(ws: WebSocket): Promise<void> {
  return new Promise((resolve, reject) => {
    ws.once('open', () => resolve())
    ws.once('error', reject)
  })
}

function waitForClose(ws: WebSocket): Promise<{ code: number; reason: string }> {
  return new Promise((resolve) => {
    ws.once('close', (code, reason) => resolve({ code, reason: reason.toString() }))
  })
}

function waitForCondition(predicate: () => boolean, timeoutMs = 1500): Promise<void> {
  return new Promise((resolve, reject) => {
    const startedAt = Date.now()
    const check = () => {
      if (predicate()) return resolve()
      if (Date.now() - startedAt >= timeoutMs) return reject(new Error('Timed out waiting for condition'))
      setTimeout(check, 25)
    }
    check()
  })
}

function closeSocket(ws: WebSocket): void {
  if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) ws.close()
}

const TRUST_CORE = {
  trusted: true,
  evaluatedAtTime: '2026-05-11T13:00:05Z',
  evaluatedAtBlock: 1500005,
  expiresAtTime: '2026-05-12T13:00:05Z',
  corporationId: 42,
}

function rawChange(did: string, overrides: Partial<VtRawChange> = {}): VtRawChange {
  return {
    did,
    relatedDids: new Set(),
    corporationIds: new Set(),
    trust: null,
    corporation: null,
    participations: null,
    ecosystems: null,
    content: false,
    ...overrides,
  }
}

async function openSubscribed(url: string, body: Record<string, unknown>): Promise<WebSocket> {
  const ws = new WebSocket(url)
  await waitForOpen(ws)
  await waitForMessage(ws, (msg) => msg.type === 'ready')
  ws.send(JSON.stringify({ action: 'subscribe', ...body }))
  await new Promise((resolve) => setTimeout(resolve, 20))
  return ws
}

describe('VtSubscribeBroadcaster', () => {
  let broadcaster: VtSubscribeBroadcaster
  let httpServer: Server
  let WS_URL = ''

  beforeAll((done) => {
    httpServer = createServer()
    httpServer.listen(0, () => {
      const addr = httpServer.address()
      const port = typeof addr === 'object' && addr ? addr.port : 0
      WS_URL = `ws://localhost:${port}/v4/verifiable-trust/subscribe`
      done()
    })
  })

  afterAll((done) => {
    broadcaster?.close()
    httpServer.close(() => done())
  })

  beforeEach(() => {
    broadcaster?.close()
    broadcaster = new VtSubscribeBroadcaster()
    broadcaster.setLogger({ info: () => {}, warn: () => {}, error: () => {} })
    broadcaster.initialize(httpServer)
  })

  afterEach(() => {
    broadcaster.close()
  })

  it('sends a ready message on connect', async () => {
    const ws = new WebSocket(WS_URL)
    await waitForOpen(ws)
    const message = await waitForMessage(ws, (msg) => msg.type === 'ready')
    expect(message).toMatchObject({ type: 'ready' })
    expect(typeof message.blockIntervalMs).toBe('number')
    expect(broadcaster.getClientCount()).toBe(1)
    closeSocket(ws)
    await waitForClose(ws)
    await waitForCondition(() => broadcaster.getClientCount() === 0)
  })

  it('rejects a subscribe with no enabled channels (close 1008)', async () => {
    const ws = new WebSocket(WS_URL)
    await waitForOpen(ws)
    await waitForMessage(ws, (msg) => msg.type === 'ready')
    ws.send(JSON.stringify({ action: 'subscribe', channels: {} }))
    const close = await waitForClose(ws)
    expect(close.code).toBe(1008)
  })

  it('delivers a changes envelope with the inline trust object to a wildcard subscriber', async () => {
    const ws = await openSubscribed(WS_URL, { channels: { trust: true } })
    const promise = waitForMessage(ws, (msg) => msg.type === 'block')
    broadcaster.broadcastChangesEnvelope({
      block: 1500005,
      blockTime: '2026-05-11T13:00:05Z',
      changes: [rawChange('did:web:a', { trust: TRUST_CORE })],
    })
    const msg = await promise
    expect(msg.block).toBe(1500005)
    expect(msg.changes).toHaveLength(1)
    expect(msg.changes[0]).toMatchObject({ did: 'did:web:a', trust: TRUST_CORE })
    closeSocket(ws)
  })

  it('applies channel sub-option gating (weight-only suppressed without includeWeightChanges)', async () => {
    const ws = await openSubscribed(WS_URL, { channels: { participations: true } })
    let received: any = null
    ws.on('message', (data) => {
      const m = JSON.parse(data.toString())
      if (m.type === 'block') received = m
    })
    broadcaster.broadcastChangesEnvelope({
      block: 10,
      blockTime: 't',
      changes: [
        rawChange('did:web:a', {
          participations: { structural: false, weight: true, counts: false, issued: false, verified: false },
        }),
      ],
    })
    await new Promise((resolve) => setTimeout(resolve, 150))
    expect(received).toMatchObject({ type: 'block', block: 10, changes: [] })
    closeSocket(ws)
  })

  it('filters changes by DID subscription', async () => {
    const wsMatch = await openSubscribed(WS_URL, { channels: { trust: true }, dids: ['did:web:a'] })
    const wsOther = await openSubscribed(WS_URL, { channels: { trust: true }, dids: ['did:web:z'] })

    const matchPromise = waitForMessage(wsMatch, (msg) => msg.type === 'block' && msg.changes.length > 0)
    let otherGotChange = false
    wsOther.on('message', (data) => {
      const m = JSON.parse(data.toString())
      if (m.type === 'block' && m.changes.length > 0) otherGotChange = true
    })

    broadcaster.broadcastChangesEnvelope({
      block: 10,
      blockTime: 't',
      changes: [rawChange('did:web:a', { trust: TRUST_CORE })],
    })

    const env = await matchPromise
    await new Promise((resolve) => setTimeout(resolve, 150))
    expect(env.changes[0].did).toBe('did:web:a')
    expect(otherGotChange).toBe(false)
    closeSocket(wsMatch)
    closeSocket(wsOther)
  })

  it('emits empty changes as a heartbeat', async () => {
    const ws = await openSubscribed(WS_URL, { channels: { trust: true }, dids: ['did:web:a'] })
    const promise = waitForMessage(ws, (msg) => msg.type === 'block')
    broadcaster.broadcastChangesEnvelope({
      block: 777,
      blockTime: 't',
      changes: [rawChange('did:web:other', { trust: TRUST_CORE })],
    })
    const msg = await promise
    expect(msg).toMatchObject({ type: 'block', block: 777, changes: [] })
    closeSocket(ws)
  })

  describe('Subscribed acknowledgement', () => {
    const HEIGHT = 1500006

    function recordTypes(ws: WebSocket): string[] {
      const types: string[] = []
      ws.on('message', (data) => types.push(JSON.parse(data.toString()).type))
      return types
    }

    it('acknowledges a subscribe with the next deliverable block', async () => {
      broadcaster.noteBlockProcessed(HEIGHT)
      const ws = new WebSocket(WS_URL)
      await waitForOpen(ws)
      await waitForMessage(ws, (msg) => msg.type === 'ready')

      ws.send(JSON.stringify({ action: 'subscribe', channels: { trust: true } }))
      const ack = await waitForMessage(ws, (msg) => msg.type === 'subscribed')

      expect(ack).toEqual({ type: 'subscribed', block: HEIGHT + 1, blockTime: expect.any(String) })
      closeSocket(ws)
    })

    it('does not acknowledge an unsubscribe', async () => {
      const ws = await openSubscribed(WS_URL, { channels: { trust: true } })
      const types = recordTypes(ws)

      ws.send(JSON.stringify({ action: 'unsubscribe' }))
      await new Promise((resolve) => setTimeout(resolve, 150))

      expect(types).toEqual([])
      closeSocket(ws)
    })

    it('sends the acknowledgement before any block message', async () => {
      broadcaster.noteBlockProcessed(HEIGHT)
      const ws = new WebSocket(WS_URL)
      await waitForOpen(ws)
      await waitForMessage(ws, (msg) => msg.type === 'ready')

      const types = recordTypes(ws)
      ws.send(JSON.stringify({ action: 'subscribe', channels: { trust: true } }))
      for (let i = 1; i <= 40; i++) {
        broadcaster.broadcastChangesEnvelope({ block: HEIGHT + i, blockTime: 't', changes: [] })
        await new Promise((resolve) => setTimeout(resolve, 2))
      }
      await new Promise((resolve) => setTimeout(resolve, 100))

      expect(types.length).toBeGreaterThan(1)
      expect(types[0]).toBe('subscribed')
      expect(types.slice(1).every((type) => type === 'block')).toBe(true)
      closeSocket(ws)
    })

    it('sends ready before acknowledging a subscribe that arrives on open', async () => {
      const ws = new WebSocket(WS_URL)
      const types = recordTypes(ws)
      await waitForOpen(ws)

      ws.send(JSON.stringify({ action: 'subscribe', channels: { trust: true } }))
      await waitForMessage(ws, (msg) => msg.type === 'subscribed')

      expect(types.slice(0, 2)).toEqual(['ready', 'subscribed'])
      closeSocket(ws)
    })

    it('does not inherit the indexer height reported by ready', async () => {
      const ws = new WebSocket(WS_URL)
      await waitForOpen(ws)
      const ready = await waitForMessage(ws, (msg) => msg.type === 'ready')

      ws.send(JSON.stringify({ action: 'subscribe', channels: { trust: true } }))
      const ack = await waitForMessage(ws, (msg) => msg.type === 'subscribed')

      // The resolver poller seeds this counter from its own checkpoint; until it does, coverage cannot claim any
      // block the indexer reached but this stream never broadcast.
      expect(ack.block).toBe(1)
      expect(ack.block).toBeLessThanOrEqual(Math.max(ready.block, 1))
      closeSocket(ws)
    })

    it('keeps tracking heights while no client is connected', () => {
      const idle = new VtSubscribeBroadcaster()
      idle.setLogger({ info: () => {}, warn: () => {}, error: () => {} })
      idle.broadcastChangesEnvelope({ block: 900, blockTime: 't', changes: [] })

      expect(idle.getClientCount()).toBe(0)
      expect(idle.getNextDeliverableBlock()).toBe(901)
    })
  })

  it('does not deliver to clients that have not subscribed', async () => {
    const ws = new WebSocket(WS_URL)
    await waitForOpen(ws)
    await waitForMessage(ws, (msg) => msg.type === 'ready')
    let gotBlock = false
    ws.on('message', (data) => {
      const m = JSON.parse(data.toString())
      if (m.type === 'block') gotBlock = true
    })
    broadcaster.broadcastChangesEnvelope({
      block: 10,
      blockTime: 't',
      changes: [rawChange('did:web:a', { trust: TRUST_CORE })],
    })
    await new Promise((resolve) => setTimeout(resolve, 150))
    expect(gotBlock).toBe(false)
    closeSocket(ws)
  })
})
