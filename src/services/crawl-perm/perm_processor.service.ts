import {
  Action,
  Service,
} from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BullableService from "../../base/bullable.service";
import { PermissionMessageTypes, SERVICE } from "../../common/constant";

@Service({
  name: SERVICE.V1.PermProcessorService.key,
  version: 1,
})
export default class PermProcessorService extends BullableService {
  constructor(broker: ServiceBroker) {
    super(broker);
  }

  @Action({ name: "handlePermissionMessages" })
  async handlePermissionMessages(
    ctx: Context<{
      permissionMessages: Array<{
        type: string;
        content: any;
        timestamp?: string;
      }>;
    }>
  ) {
    const { permissionMessages } = ctx.params;
    for (const msg of permissionMessages) {
      const payload = {
        ...msg.content,
        timestamp: msg.timestamp,
      };
      delete payload["@type"];

      switch (msg.type) {
        case PermissionMessageTypes.CreateRootPermission:
          await this.broker.call("permIngest.handleMsgCreateRootPermission", {
            data: payload,
          });
          break;
        case PermissionMessageTypes.CreatePermission:
          await this.broker.call("permIngest.handleMsgCreatePermission", {
            data: payload,
          });
          break;
        case PermissionMessageTypes.ExtendPermission:
          await this.broker.call("permIngest.handleMsgExtendPermission", {
            data: payload,
          });
          break;
        case PermissionMessageTypes.RevokePermission:
          await this.broker.call("permIngest.handleMsgRevokePermission", {
            data: payload,
          });
          break;
        case PermissionMessageTypes.StartPermissionVP:
          await this.broker.call("permIngest.handleMsgStartPermissionVP", {
            data: payload,
          });
          break;
        case PermissionMessageTypes.SetPermissionVPToValidated:
          await this.broker.call(
            "permIngest.handleMsgSetPermissionVPToValidated",
            { data: payload }
          );
          break;
        case PermissionMessageTypes.RenewPermissionVP:
          await this.broker.call("permIngest.handleMsgRenewPermissionVP", {
            data: payload,
          });
          break;
        case PermissionMessageTypes.CancelPermissionVPLastRequest:
          await this.broker.call(
            "permIngest.handleMsgCancelPermissionVPLastRequest",
            { data: payload }
          );
          break;
        case PermissionMessageTypes.CreateOrUpdatePermissionSession:
          await this.broker.call(
            "permIngest.handleMsgCreateOrUpdatePermissionSession",
            { data: payload }
          );
          break;
        case PermissionMessageTypes.SlashPermissionTrustDeposit:
          await this.broker.call(
            "permIngest.handleMsgSlashPermissionTrustDeposit",
            { data: payload }
          );
          break;
        case PermissionMessageTypes.RepayPermissionSlashedTrustDeposit:
          await this.broker.call(
            "permIngest.handleMsgRepayPermissionSlashedTrustDeposit",
            { data: payload }
          );
          break;
        default:
          break;
      }
    }

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
