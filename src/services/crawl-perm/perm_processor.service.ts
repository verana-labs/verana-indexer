import {
  Action,
  Service,
} from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BullableService from "../../base/bullable.service";
import { VeranaParticipantMessageTypes } from "../../common/verana-message-types";
import { SERVICE } from "../../common";
import { MessageProcessorBase } from "../../common/utils/message_processor_base";
import { detectStartMode } from "../../common/utils/start_mode_detector";
import { runHeightSyncPerm } from "../../modules/perm-height-sync/perm_height_sync_service";
import type { PermissionMessagePayload } from "../../modules/perm-height-sync/perm_height_sync_helpers";
import {
  extractImpactedPermissionIds,
  extractStartPermissionVpNewPermissionId,
} from "../../modules/perm-height-sync/perm_height_sync_helpers";


@Service({
  name: SERVICE.V1.ParticipantProcessorService.key,
  version: 1,
})
export default class ParticipantProcessorService extends BullableService {
  private processorBase: MessageProcessorBase;
  private _isFreshStart: boolean = false;

  constructor(broker: ServiceBroker) {
    super(broker);
    this.processorBase = new MessageProcessorBase(this);
  }

  public async _start() {
    const startMode = await detectStartMode();
    this._isFreshStart = startMode.isFreshStart;
    this.processorBase.setFreshStartMode(this._isFreshStart);
    this.logger.info(`Permission processor started | Mode: ${this._isFreshStart ? 'Fresh Start' : 'Reindexing'}`);
    return super._start();
  }

  @Action({ name: "handlePermissionMessages" })
  async handlePermissionMessages(
    ctx: Context<{
      permissionMessages: PermissionMessagePayload[];
    }>
  ) {
    const { permissionMessages } = ctx.params;
    const totalMessages = permissionMessages?.length || 0;
    this.logger.info(` Processing ${totalMessages} Permission messages`);
    
    if (!permissionMessages || permissionMessages.length === 0) {
      return { success: true };
    }

    const failedMessages: Array<{ message: any; error: string }> = [];

    const sortedMessages = [...permissionMessages].sort((a, b) => {
      const heightDiff = (a.height || 0) - (b.height || 0);
      if (heightDiff !== 0) return heightDiff;

      const priority = (type: string | undefined) => {
        switch (type) {
          case VeranaParticipantMessageTypes.CreateRootParticipant:
          case VeranaParticipantMessageTypes.SelfCreateParticipant:
            return 1;
          case VeranaParticipantMessageTypes.SetParticipantEffectiveUntil:
          case VeranaParticipantMessageTypes.RevokeParticipant:
            return 2;
          case VeranaParticipantMessageTypes.StartParticipantOP:
            return 3;
          case VeranaParticipantMessageTypes.RenewParticipantOP:
          case VeranaParticipantMessageTypes.CancelParticipantOPLastRequest:
            return 4;
          case VeranaParticipantMessageTypes.SetParticipantOPToValidated:
            return 5;
          case VeranaParticipantMessageTypes.CreateOrUpdateParticipantSession:
          case VeranaParticipantMessageTypes.SlashParticipantTrustDeposit:
          case VeranaParticipantMessageTypes.RepayParticipantSlashedTrustDeposit:
            return 6;
          default:
            return 99;
        }
      };

      return priority(a.type) - priority(b.type);
    });

    for (let i = 0; i < sortedMessages.length; i++) {
      const msg = sortedMessages[i];
      try {
        this.logger.info(` Processing Permission message ${i + 1}/${totalMessages}: type=${msg.type}, height=${msg.height}`);
        const useHeightSyncPerm = process.env.USE_HEIGHT_SYNC_PERM !== "false";
        if (useHeightSyncPerm) {
          const res = await runHeightSyncPerm(this.broker, [msg]);
          const synced = (res as any)?.synced;
          const attempted = (res as any)?.attempted;

          if (typeof synced === "number" && synced > 0) {
            continue;
          }
          this.logger.warn(
            `[perm] Height-sync enabled but synced 0/${typeof attempted === "number" ? attempted : "?"}. Falling back to direct message handlers for type=${msg.type} height=${msg.height} txHash=${msg.txHash ?? "unknown"}`
          );
        }

        const payload = {
          ...msg.content,
          timestamp: msg.timestamp,
          height: msg.height,
        };
        delete payload["@type"];

       
        if (
          (msg.type === VeranaParticipantMessageTypes.CreateRootParticipant
            || msg.type === VeranaParticipantMessageTypes.SelfCreateParticipant)
          && (payload as any)?.id == null
        ) {
          const impacted = extractImpactedPermissionIds(msg as PermissionMessagePayload);
          if (impacted.length === 1) {
            (payload as any).id = impacted[0];
          }
        }
        if (
          msg.type === VeranaParticipantMessageTypes.StartParticipantOP
          && (payload as any)?.id == null
        ) {
          const vpNewId = extractStartPermissionVpNewPermissionId(
            msg as PermissionMessagePayload
          );
          if (vpNewId != null) {
            (payload as any).id = vpNewId;
          }
        }

        let result: any;
        switch (msg.type) {
          case VeranaParticipantMessageTypes.CreateRootParticipant:
            result = await this.broker.call("participantIngest.handleMsgCreateRootParticipant", {
              data: payload,
            });
            break;
          case VeranaParticipantMessageTypes.SelfCreateParticipant:
            result = await this.broker.call("participantIngest.handleMsgSelfCreateParticipant", {
              data: payload,
            });
            break;
          case VeranaParticipantMessageTypes.SetParticipantEffectiveUntil:
            result = await this.broker.call("participantIngest.handleMsgSetParticipantEffectiveUntil", {
              data: payload,
            });
            break;
          case VeranaParticipantMessageTypes.RevokeParticipant:
            result = await this.broker.call("participantIngest.handleMsgRevokeParticipant", {
              data: payload,
            });
            break;
          case VeranaParticipantMessageTypes.StartParticipantOP:
            result = await this.broker.call("participantIngest.handleMsgStartParticipantOP", {
              data: payload,
            });
            break;
          case VeranaParticipantMessageTypes.SetParticipantOPToValidated:
            result = await this.broker.call(
              "participantIngest.handleMsgSetParticipantOPToValidated",
              { data: payload }
            );
            if (result && result.success === false) {
              this.logger.warn(` SetParticipantOPToValidated failed for id=${payload.id}: ${result.reason}`);
            }
            break;
          case VeranaParticipantMessageTypes.RenewParticipantOP:
            result = await this.broker.call("participantIngest.handleMsgRenewParticipantOP", {
              data: payload,
            });
            break;
          case VeranaParticipantMessageTypes.CancelParticipantOPLastRequest:
            result = await this.broker.call(
              "participantIngest.handleMsgCancelParticipantOPLastRequest",
              { data: payload }
            );
            break;
          case VeranaParticipantMessageTypes.CreateOrUpdateParticipantSession:
            result = await this.broker.call(
              "participantIngest.handleMsgCreateOrUpdateParticipantSession",
              { data: payload }
            );
            break;
          case VeranaParticipantMessageTypes.SlashParticipantTrustDeposit:
            result = await this.broker.call(
              "participantIngest.handleMsgSlashParticipantTrustDeposit",
              { data: payload }
            );
            break;
          case VeranaParticipantMessageTypes.RepayParticipantSlashedTrustDeposit:
            result = await this.broker.call(
              "participantIngest.handleMsgRepayParticipantSlashedTrustDeposit",
              { data: payload }
            );
            break;
          default:
            this.logger.warn(` Unknown permission message type: ${msg.type}`);
            break;
        }

        if (result && result.success === false && result.reason) {
          this.logger.warn(` Permission message processing returned failure: ${result.reason}`);
        }
      } catch (err: any) {
        failedMessages.push({ message: msg, error: err.message || String(err) });
        this.logger.error(`❌ Error processing Permission message ${i + 1}/${totalMessages}:`, err);
      }
    }

    if (failedMessages.length > 0) {
      this.logger.warn(` ${failedMessages.length}/${totalMessages} Permission messages failed to process`);
    }

    this.logger.info(` Permission processing complete: ${totalMessages - failedMessages.length}/${totalMessages} successful`);
    return { success: true, failed: failedMessages.length };
  }

  @Action({ name: "getPermission" })
  async getPermission(
    ctx: Context<{ schema_id: number; corporation: string; role: string }>
  ) {
    const { schema_id: schemaId, corporation, role } = ctx.params;
    const permission = await this.broker.call("participantIngest.getPermission", {
      schema_id: schemaId,
      corporation,
      role,
    });
    return permission;
  }

  @Action({ name: "listPermissions" })
  async listPermissions(
    ctx: Context<{ schema_id?: number; corporation?: string; role?: string }>
  ) {
    const { schema_id: schemaId, corporation, role } = ctx.params;
    const permissions = await this.broker.call("participantIngest.listPermissions", {
      schema_id: schemaId,
      corporation,
      role,
    });
    return permissions;
  }
}
