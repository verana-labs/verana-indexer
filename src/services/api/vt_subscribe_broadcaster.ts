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
