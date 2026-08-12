import { BULL_JOB_NAME } from '../../common'
import knex from '../../common/utils/db_connection'
import { BaseSubscribeServer, type ControlParseResult } from './subscribe_ws_server'
import {
  buildVtChangesEnvelope,
  parseVtControlMessage,
  type VtChannelOptions,
  type VtControlMessage,
  type VtRawChange,
} from './vt_subscribe_protocol'

type VtClientState = {
  established: boolean
  dids: Set<string> | null
  corporationId: number | null
  channels: VtChannelOptions | null
}

export class VtSubscribeBroadcaster extends BaseSubscribeServer<VtControlMessage, VtClientState> {
  protected readonly path = '/v4/verifiable-trust/subscribe'

  protected createInitialState(): VtClientState {
    return { established: false, dids: null, corporationId: null, channels: null }
  }

  protected parseControl(raw: string): ControlParseResult<VtControlMessage> {
    return parseVtControlMessage(raw)
  }

  protected isSubscribeControl(message: VtControlMessage): boolean {
    return message.action === 'subscribe'
  }

  protected async resolveSeedHeight(_lastProcessedBlock: number): Promise<number> {
    try {
      const row = (await knex('block_checkpoint')
        .select('height')
        .where('job_name', BULL_JOB_NAME.HANDLE_TRUST_RESOLVE)
        .first()) as { height?: number | string } | undefined
      const height = Number(row?.height ?? 0)
      return Number.isFinite(height) ? height : 0
    } catch (error) {
      this.logger.warn(`[${this.constructor.name}] Could not read the resolver checkpoint to seed acks:`, error)
      return 0
    }
  }

  protected applyControl(_state: VtClientState, message: VtControlMessage): VtClientState {
    if (message.action === 'unsubscribe') {
      return { established: false, dids: null, corporationId: null, channels: null }
    }

    return {
      established: true,
      dids: message.dids === null ? null : new Set(message.dids),
      corporationId: message.corporationId,
      channels: message.channels,
    }
  }

  broadcastChangesEnvelope(args: { block: number; blockTime: string; changes: VtRawChange[] }): void {
    this.noteBlockProcessed(args.block)
    if (this.clients.size === 0) return

    let sent = 0
    this.clients.forEach((state, ws) => {
      if (!state.established || !state.channels) return
      const envelope = buildVtChangesEnvelope(
        args.block,
        args.blockTime,
        args.changes,
        state.dids,
        state.corporationId,
        state.channels
      )
      if (this.sendJson(ws, envelope as unknown as Record<string, unknown>, 1011)) sent++
    })

    if (sent > 0) {
      this.logger.info(`[VtSubscribeBroadcaster] Broadcasted block ${args.block} to ${sent} subscriber(s)`)
    }
  }
}

export const vtSubscribeBroadcaster = new VtSubscribeBroadcaster()
