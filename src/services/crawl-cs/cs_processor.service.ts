import { Action, Service } from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BullableService from "../../base/bullable.service";
import { CredentialSchemaMessageType, SERVICE } from "../../common";
import { formatTimestamp } from "../../common/utils/date_utils";
import knex from "../../common/utils/db_connection";

interface CredentialSchemaMessage {
  tr_id: number;
  id: number;
  type: string;
  content?: any;
  json_schema: string;
  deposit?: string;
  issuer_grantor_validation_validity_period: number;
  verifier_grantor_validation_validity_period: number;
  issuer_validation_validity_period: number;
  verifier_validation_validity_period: number;
  holder_validation_validity_period: number;
  issuer_perm_management_mode: number;
  verifier_perm_management_mode: number;
  timestamp?: string;
  creator?: string;
  archive?: string;
  "@type"?: string;
}


async function calculateDeposit(): Promise<number> {
  const csParamsRow = await knex("module_params").where({ module: "credentialschema" }).first();
  const trParamsRow = await knex("module_params").where({ module: "trustregistry" }).first();

  if (!csParamsRow || !trParamsRow) {
    return 0;
  }

  const csParams = typeof csParamsRow.params === "string" ? JSON.parse(csParamsRow.params) : csParamsRow.params;
  const trParams = typeof trParamsRow.params === "string" ? JSON.parse(trParamsRow.params) : trParamsRow.params;

  const credentialSchemaTrustDeposit = Number(csParams?.params?.credential_schema_trust_deposit || 0);
  const trustUnitPrice = Number(trParams?.params?.trust_unit_price || 1);

  return credentialSchemaTrustDeposit * trustUnitPrice;
}

@Service({
  name: SERVICE.V1.ProcessCredentialSchemaService.key,
  version: 1,
})
export default class ProcessCredentialSchemaService extends BullableService {
  constructor(broker: ServiceBroker) {
    super(broker);
  }

  @Action({ name: "handleCredentialSchemas" })
  async handleCredentialSchemas(ctx: Context<{ credentialSchemaMessages: CredentialSchemaMessage[] }>) {
    this.logger.info("📥 Processing credential schemas:", ctx.params);

    const { credentialSchemaMessages } = ctx.params;
    if (!credentialSchemaMessages?.length) {
      return { success: false, message: "No credential schemas" };
    }

    const deposit = await calculateDeposit();

    for (const schemaMessage of credentialSchemaMessages) {
      if (schemaMessage.type === CredentialSchemaMessageType.Create || schemaMessage.type === CredentialSchemaMessageType.CreateLegacy) {
        await this.createSchema(ctx, schemaMessage, deposit);
      }
      if (schemaMessage.type === CredentialSchemaMessageType.Update) {
        await this.updateSchema(ctx, schemaMessage, deposit);
      }
      if (schemaMessage.type === CredentialSchemaMessageType.Archive) {
        await this.archiveSchema(ctx, schemaMessage);
      }
    }

    return { success: true };
  }


  private async createSchema(ctx: Context, schemaMessage: CredentialSchemaMessage, deposit: number) {
    try {
      const payload: Record<string, any> = {
        ...schemaMessage,
        ...schemaMessage.content,
        deposit: deposit.toString(),
        created: formatTimestamp(schemaMessage.timestamp),
        modified: formatTimestamp(schemaMessage.timestamp),
      };

      delete payload.content;
      delete payload.timestamp;
      delete payload.type;
      delete payload.creator;
      delete payload["@type"];

      const result = await ctx.call(`${SERVICE.V1.CredentialSchemaDatabaseService.path}.upsert`, { payload });
      this.logger.info(`✅ Stored credential schema tr_id=${payload.tr_id} with deposit=${payload.deposit}`, result);
    } catch (err) {
      this.logger.error("❌ Error storing credential schema:", err);
    }
  }

 
  private async updateSchema(ctx: Context, schemaMessage: CredentialSchemaMessage, deposit: number) {
    try {
      const payload: Record<string, any> = {
        ...schemaMessage,
        ...schemaMessage.content,
        deposit: deposit.toString(),
        modified: formatTimestamp(schemaMessage.timestamp),
      };

      delete payload.content;
      delete payload.timestamp;
      delete payload.type;
      delete payload.creator;
      delete payload["@type"];

      const result = await ctx.call(`${SERVICE.V1.CredentialSchemaDatabaseService.path}.update`, { payload });
      this.logger.info(`✅ Updated credential schema id=${payload.id}`, result);
    } catch (err) {
      this.logger.error(`❌ Error updating credential schema id=${schemaMessage.id}:`, err);
    }
  }

 
  private async archiveSchema(ctx: Context, schemaMessage: CredentialSchemaMessage) {
    try {
      const payload: Record<string, any> = {
        ...schemaMessage,
        ...schemaMessage.content,
        modified: formatTimestamp(schemaMessage.timestamp),
      };

      delete payload.content;
      delete payload.timestamp;
      delete payload.type;
      delete payload.creator;
      delete payload["@type"];

      await ctx.call(`${SERVICE.V1.CredentialSchemaDatabaseService.path}.archive`, { payload });

      this.logger.info(`📦 Archive action executed for credential schema id=${payload.id}, archive=${payload.archive}`);
    } catch (err) {
      this.logger.error(`❌ Error archiving credential schema id=${schemaMessage.id}:`, err);
    }
  }
}
