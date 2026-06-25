import { Buffer } from 'node:buffer'
import { type IncomingMessage, Server } from 'http'
import type { Duplex } from 'stream'
import { WebSocket, WebSocketServer } from 'ws'
import { indexerStatusManager } from '../manager/indexer_status.manager'
import { createLogger, type LoggerLike, toIsoSeconds } from './api_shared'
import { buildReadyMessage } from './subscribe_protocol'

export type ControlParseResult<TMessage> = { ok: true; message: TMessage } | { ok: false; error: string }

export abstract class BaseSubscribeServer<TControl, TState> {
  private wss: WebSocketServer | null = null
  private server: Server | null = null
  private upgradeHandler: ((req: IncomingMessage, socket: Duplex, head: Buffer) => void) | null = null
  protected clients: Map<WebSocket, TState> = new Map()
  private clientAlive: Map<WebSocket, boolean> = new Map()
  private pingInterval: NodeJS.Timeout | null = null
  private readonly MAX_CLIENTS = 10000 // TODO: Review this values
  private readonly PING_INTERVAL = 30000
  private readonly MAX_CONTROL_MESSAGE_BYTES = 64 * 1024
  protected logger = createLogger(console)

  protected abstract readonly path: string

  protected abstract createInitialState(): TState

  protected abstract parseControl(raw: string): ControlParseResult<TControl>

  protected abstract applyControl(state: TState, message: TControl): TState

  setLogger(logger: LoggerLike): void {
    this.logger = createLogger(logger)
  }

  initialize(server: Server): void {
    if (this.wss) {
      this.logger.warn(`[${this.constructor.name}] WebSocket server already initialized`)
      return
    }

    this.wss = new WebSocketServer({
      noServer: true,
      perMessageDeflate: false,
      maxPayload: this.MAX_CONTROL_MESSAGE_BYTES,
    })

    this.server = server
    this.upgradeHandler = (req: IncomingMessage, socket: Duplex, head: Buffer) => {
      let pathname: string
      try {
        pathname = new URL(req.url ?? '', 'http://localhost').pathname
      } catch {
        return
      }
      if (pathname !== this.path) return

      if (this.clients.size >= this.MAX_CLIENTS) {
        this.logger.warn(`[${this.constructor.name}] Max clients reached (${this.MAX_CLIENTS}), rejecting connection`)
        socket.destroy()
        return
      }

      const wss = this.wss
      if (!wss) {
        socket.destroy()
        return
      }
      wss.handleUpgrade(req, socket, head, (ws) => {
        wss.emit('connection', ws, req)
      })
    }
    server.on('upgrade', this.upgradeHandler)

    this.wss.on('connection', async (ws: WebSocket) => {
      if (this.clients.size >= this.MAX_CLIENTS) {
        ws.close(1013, 'Server overloaded')
        return
      }

      this.clients.set(ws, this.createInitialState())
      this.clientAlive.set(ws, true)

      this.logger.info(`[${this.constructor.name}] New client connected. Total clients: ${this.clients.size}`)

      ws.on('close', () => this.cleanupClient(ws))
      ws.on('error', (error) => {
        this.logger.error(`[${this.constructor.name}] WebSocket error:`, error)
        this.cleanupClient(ws)
      })
      ws.on('pong', () => {
        this.clientAlive.set(ws, true)
      })
      ws.on('message', (raw) => this.handleControlMessage(ws, raw.toString()))

      try {
        await this.sendReady(ws)
      } catch (error) {
        this.logger.error(`[${this.constructor.name}] Error sending ready message:`, error)
        this.cleanupClient(ws)
      }
    })

    this.wss.on('error', (error) => {
      this.logger.error(`[${this.constructor.name}] WebSocketServer error:`, error)
    })

    this.pingInterval = setInterval(() => {
      this.pingClients()
    }, this.PING_INTERVAL)

    this.logger.info(`[${this.constructor.name}] WebSocket server initialized on ${this.path}`)
  }

  private async sendReady(ws: WebSocket): Promise<void> {
    const status = await indexerStatusManager.getDetailedStatus()
    const lastProcessedBlock = status.lastProcessedBlock ?? 0
    const lastBlockTime = status.lastBlockTime ?? toIsoSeconds()
    const ready = buildReadyMessage(lastProcessedBlock, lastBlockTime)
    this.sendJson(ws, ready as unknown as Record<string, unknown>)
  }

  private handleControlMessage(ws: WebSocket, raw: string): void {
    const result = this.parseControl(raw)
    if (!result.ok) {
      this.logger.warn(`[${this.constructor.name}] Invalid control message: ${result.error}`)
      try {
        ws.close(1008, result.error)
      } catch {}
      this.cleanupClient(ws)
      return
    }

    const state = this.clients.get(ws)
    if (!state) return

    this.clients.set(ws, this.applyControl(state, result.message))
  }

  protected sendJson(ws: WebSocket, payload: Record<string, unknown>, closeCodeOnFailure = 0): boolean {
    if (ws.readyState !== WebSocket.OPEN) {
      this.cleanupClient(ws)
      return false
    }
    try {
      ws.send(JSON.stringify(payload), { compress: false })
      return true
    } catch {
      if (closeCodeOnFailure) {
        try {
          ws.close(closeCodeOnFailure, 'Server overloaded')
        } catch {}
      }
      this.cleanupClient(ws)
      return false
    }
  }

  private cleanupClient(ws: WebSocket): void {
    const had = this.clients.delete(ws)
    this.clientAlive.delete(ws)
    if (had) {
      this.logger.info(`[${this.constructor.name}] Client disconnected. Total clients: ${this.clients.size}`)
    }
  }

  private pingClients(): void {
    const stale: WebSocket[] = []
    this.clients.forEach((_, ws) => {
      const alive = this.clientAlive.get(ws)
      if (alive === false) {
        stale.push(ws)
        return
      }
      this.clientAlive.set(ws, false)
      try {
        ws.ping()
      } catch {
        stale.push(ws)
      }
    })
    stale.forEach((ws) => {
      try {
        ws.terminate()
      } catch {}
      this.cleanupClient(ws)
    })
  }

  getClientCount(): number {
    return this.clients.size
  }

  close(): void {
    if (this.pingInterval) {
      clearInterval(this.pingInterval)
      this.pingInterval = null
    }
    this.clients.forEach((_, ws) => {
      try {
        if (ws.readyState === WebSocket.OPEN || ws.readyState === WebSocket.CONNECTING) {
          ws.close()
        } else {
          ws.terminate()
        }
      } catch {}
    })
    this.clients.clear()
    this.clientAlive.clear()

    if (this.server && this.upgradeHandler) {
      this.server.off('upgrade', this.upgradeHandler)
    }
    this.server = null
    this.upgradeHandler = null

    if (this.wss) {
      this.wss.close(() => {
        this.logger.info(`[${this.constructor.name}] WebSocket server closed`)
      })
      this.wss = null
    }
  }
}
