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
    this.logger.info(`Processing ${permissionMessages?.length || 0} Permission messages`);
    
    if (!permissionMessages || permissionMessages.length === 0) {
      return { success: true };
    }

    const processMessage = async (msg: {
      type: string;
      content: any;
      timestamp?: string;
      height?: number;
    }) => {
      this.logger.info(`Processing Permission message: type=${msg.type}, height=${msg.height}`);
      const payload = {
        ...msg.content,
        timestamp: msg.timestamp,
        height: msg.height,
      };
      delete payload["@type"];

      switch (msg.type) {
        case VeranaPermissionMessageTypes.CreateRootPermission:
          await this.broker.call("permIngest.handleMsgCreateRootPermission", {
            data: payload,
          });
          break;
        case VeranaPermissionMessageTypes.CreatePermission:
          await this.broker.call("permIngest.handleMsgCreatePermission", {
            data: payload,
          });
          break;
        case VeranaPermissionMessageTypes.ExtendPermission:
          await this.broker.call("permIngest.handleMsgExtendPermission", {
            data: payload,
          });
          break;
        case VeranaPermissionMessageTypes.RevokePermission:
          await this.broker.call("permIngest.handleMsgRevokePermission", {
            data: payload,
          });
          break;
        case VeranaPermissionMessageTypes.StartPermissionVP:
          await this.broker.call("permIngest.handleMsgStartPermissionVP", {
            data: payload,
          });
          break;
        case VeranaPermissionMessageTypes.SetPermissionVPToValidated:
          await this.broker.call(
            "permIngest.handleMsgSetPermissionVPToValidated",
            { data: payload }
          );
          break;
        case VeranaPermissionMessageTypes.RenewPermissionVP:
          await this.broker.call("permIngest.handleMsgRenewPermissionVP", {
            data: payload,
          });
          break;
        case VeranaPermissionMessageTypes.CancelPermissionVPLastRequest:
          await this.broker.call(
            "permIngest.handleMsgCancelPermissionVPLastRequest",
            { data: payload }
          );
          break;
        case VeranaPermissionMessageTypes.CreateOrUpdatePermissionSession:
          await this.broker.call(
            "permIngest.handleMsgCreateOrUpdatePermissionSession",
            { data: payload }
          );
          break;
        case VeranaPermissionMessageTypes.SlashPermissionTrustDeposit:
          await this.broker.call(
            "permIngest.handleMsgSlashPermissionTrustDeposit",
            { data: payload }
          );
          break;
        case VeranaPermissionMessageTypes.RepayPermissionSlashedTrustDeposit:
          await this.broker.call(
            "permIngest.handleMsgRepayPermissionSlashedTrustDeposit",
            { data: payload }
          );
          break;
        default:
          break;
      }
    };

    await this.processorBase.processInBatches(
      permissionMessages,
      processMessage,
      {
        maxConcurrent: this._isFreshStart ? 3 : 8,
        batchSize: this._isFreshStart ? 20 : 50,
        delayBetweenBatches: this._isFreshStart ? 500 : 200,
      }
    );

    return { success: true };
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
