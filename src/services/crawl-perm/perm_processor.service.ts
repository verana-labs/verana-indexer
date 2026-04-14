import {
  Action,
  Service,
} from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BullableService from "../../base/bullable.service";
import { VeranaPermissionMessageTypes } from "../../common/verana-message-types";
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
  name: SERVICE.V1.PermProcessorService.key,
  version: 1,
})
export default class PermProcessorService extends BullableService {
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
          case VeranaPermissionMessageTypes.CreateRootPermission:
          case VeranaPermissionMessageTypes.SelfCreatePermission:
            return 1;
          case VeranaPermissionMessageTypes.AdjustPermission:
          case VeranaPermissionMessageTypes.RevokePermission:
            return 2;
          case VeranaPermissionMessageTypes.StartPermissionVP:
            return 3;
          case VeranaPermissionMessageTypes.RenewPermissionVP:
          case VeranaPermissionMessageTypes.CancelPermissionVPLastRequest:
            return 4;
          case VeranaPermissionMessageTypes.SetPermissionVPToValidated:
            return 5;
          case VeranaPermissionMessageTypes.CreateOrUpdatePermissionSession:
          case VeranaPermissionMessageTypes.SlashPermissionTrustDeposit:
          case VeranaPermissionMessageTypes.RepayPermissionSlashedTrustDeposit:
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

        // Some v4 tx messages don't include the created permission id in the message body.
        // When available in tx events, inject it so the DB layer can upsert by on-chain id
        // (avoids sequence collisions and makes replays idempotent).
        if (
          (msg.type === VeranaPermissionMessageTypes.CreateRootPermission
            || msg.type === VeranaPermissionMessageTypes.SelfCreatePermission)
          && (payload as any)?.id == null
        ) {
          const impacted = extractImpactedPermissionIds(msg as PermissionMessagePayload);
          if (impacted.length === 1) {
            (payload as any).id = impacted[0];
          }
        }
        if (
          msg.type === VeranaPermissionMessageTypes.StartPermissionVP
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
          case VeranaPermissionMessageTypes.CreateRootPermission:
            result = await this.broker.call("permIngest.handleMsgCreateRootPermission", {
              data: payload,
            });
            break;
          case VeranaPermissionMessageTypes.SelfCreatePermission:
            result = await this.broker.call("permIngest.handleMsgSelfCreatePermission", {
              data: payload,
            });
            break;
          case VeranaPermissionMessageTypes.AdjustPermission:
            result = await this.broker.call("permIngest.handleMsgAdjustPermission", {
              data: payload,
            });
            break;
          case VeranaPermissionMessageTypes.RevokePermission:
            result = await this.broker.call("permIngest.handleMsgRevokePermission", {
              data: payload,
            });
            break;
          case VeranaPermissionMessageTypes.StartPermissionVP:
            result = await this.broker.call("permIngest.handleMsgStartPermissionVP", {
              data: payload,
            });
            break;
          case VeranaPermissionMessageTypes.SetPermissionVPToValidated:
            result = await this.broker.call(
              "permIngest.handleMsgSetPermissionVPToValidated",
              { data: payload }
            );
            if (result && result.success === false) {
              this.logger.warn(` SetPermissionVPToValidated failed for id=${payload.id}: ${result.reason}`);
            }
            break;
          case VeranaPermissionMessageTypes.RenewPermissionVP:
            result = await this.broker.call("permIngest.handleMsgRenewPermissionVP", {
              data: payload,
            });
            break;
          case VeranaPermissionMessageTypes.CancelPermissionVPLastRequest:
            result = await this.broker.call(
              "permIngest.handleMsgCancelPermissionVPLastRequest",
              { data: payload }
            );
            break;
          case VeranaPermissionMessageTypes.CreateOrUpdatePermissionSession:
            result = await this.broker.call(
              "permIngest.handleMsgCreateOrUpdatePermissionSession",
              { data: payload }
            );
            break;
          case VeranaPermissionMessageTypes.SlashPermissionTrustDeposit:
            result = await this.broker.call(
              "permIngest.handleMsgSlashPermissionTrustDeposit",
              { data: payload }
            );
            break;
          case VeranaPermissionMessageTypes.RepayPermissionSlashedTrustDeposit:
            result = await this.broker.call(
              "permIngest.handleMsgRepayPermissionSlashedTrustDeposit",
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
    ctx: Context<{ schema_id: number; corporation: string; type: string }>
  ) {
    const { schema_id: schemaId, corporation, type } = ctx.params;
    const permission = await this.broker.call("permIngest.getPermission", {
      schema_id: schemaId,
      corporation,
      type,
    });
    return permission;
  }

  @Action({ name: "listPermissions" })
  async listPermissions(
    ctx: Context<{ schema_id?: number; corporation?: string; type?: string }>
  ) {
    const { schema_id: schemaId, corporation, type } = ctx.params;
    const permissions = await this.broker.call("permIngest.listPermissions", {
      schema_id: schemaId,
      corporation,
      type,
    });
    return permissions;
  }
}
