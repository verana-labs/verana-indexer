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
      permissionMessages: Array<{
        type: string;
        content: any;
        timestamp?: string;
        height?: number;
      }>;
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
      return 0;
    });

    for (let i = 0; i < sortedMessages.length; i++) {
      const msg = sortedMessages[i];
      try {
        this.logger.info(` Processing Permission message ${i + 1}/${totalMessages}: type=${msg.type}, height=${msg.height}`);
        const payload = {
          ...msg.content,
          timestamp: msg.timestamp,
          height: msg.height,
        };
        delete payload["@type"];

        let result: any;
        switch (msg.type) {
          case VeranaPermissionMessageTypes.CreateRootPermission:
            result = await this.broker.call("permIngest.handleMsgCreateRootPermission", {
              data: payload,
            });
            break;
          case VeranaPermissionMessageTypes.CreatePermission:
            result = await this.broker.call("permIngest.handleMsgCreatePermission", {
              data: payload,
            });
            break;
          case VeranaPermissionMessageTypes.ExtendPermission:
            result = await this.broker.call("permIngest.handleMsgExtendPermission", {
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
    ctx: Context<{ schema_id: number; grantee: string; type: string }>
  ) {
    const { schema_id: schemaId, grantee, type } = ctx.params;
    const permission = await this.broker.call("permIngest.getPermission", {
      schema_id: schemaId,
      grantee,
      type,
    });
    return permission;
  }

  @Action({ name: "listPermissions" })
  async listPermissions(
    ctx: Context<{ schema_id?: number; grantee?: string; type?: string }>
  ) {
    const { schema_id: schemaId, grantee, type } = ctx.params;
    const permissions = await this.broker.call("permIngest.listPermissions", {
      schema_id: schemaId,
      grantee,
      type,
    });
    return permissions;
  }
}
