import {
  Action,
  Service,
} from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BullableService from "../../base/bullable.service";
import {
  CredentialSchemaMessageType,
  ModulesParamsNamesTypes,
  SERVICE,
} from "../../common";
import { formatTimestamp } from "../../common/utils/date_utils";
import knex from "../../common/utils/db_connection";
import { getModeString } from "./cs_types";

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
  issuer_perm_management_mode: string;
  verifier_perm_management_mode: string;
  timestamp?: string;
  creator?: string;
  archive?: string;
  "@type"?: string;
  height?: number;
}

async function calculateDeposit(): Promise<number> {
  const csParamsRow = await knex("module_params")
    .where({ module: ModulesParamsNamesTypes?.CS })
    .first();
  const trParamsRow = await knex("module_params")
    .where({ module: ModulesParamsNamesTypes?.TR })
    .first();

  if (!csParamsRow || !trParamsRow) {
    return 0;
  }

  const csParams =
    typeof csParamsRow.params === "string"
      ? JSON.parse(csParamsRow.params)
      : csParamsRow.params;
  const trParams =
    typeof trParamsRow.params === "string"
      ? JSON.parse(trParamsRow.params)
      : trParamsRow.params;

  const credentialSchemaTrustDeposit = Number(
    csParams?.params?.credential_schema_trust_deposit || 0
  );
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
  async handleCredentialSchemas(
    ctx: Context<{ credentialSchemaMessages: CredentialSchemaMessage[] }>
  ) {
    const { credentialSchemaMessages } = ctx.params;
    this.logger.info(`🔄 Processing ${credentialSchemaMessages?.length || 0} CredentialSchema messages`);

    if (!credentialSchemaMessages?.length) {
      return { success: false, message: "No credential schemas" };
    }

    const deposit = await calculateDeposit();

    for (const schemaMessage of credentialSchemaMessages) {
      try {
        this.logger.info(`📝 Processing CS message: type=${schemaMessage.type}, height=${schemaMessage.height}`);
        if (
          schemaMessage.type === CredentialSchemaMessageType.Create ||
          schemaMessage.type === CredentialSchemaMessageType.CreateLegacy
        ) {
          this.logger.info(`🆕 Creating new CredentialSchema at height ${schemaMessage.height}`);
          await this.createSchema(ctx, schemaMessage, deposit);
        }
        if (schemaMessage.type === CredentialSchemaMessageType.Update) {
          await this.updateSchema(ctx, schemaMessage, deposit);
        }
        if (schemaMessage.type === CredentialSchemaMessageType.Archive) {
          await this.archiveSchema(ctx, schemaMessage);
        }
      } catch (err) {
        this.logger.error(`❌ Error processing CS message:`, err);
        console.error("FATAL CS ERROR:", err);
        
      }
    }

    return { success: true };
  }
  private async createSchema(
    ctx: Context,
    schemaMessage: CredentialSchemaMessage,
    deposit: number
  ) {
    try {
      const timestamp = formatTimestamp(schemaMessage.timestamp);
      const content = schemaMessage?.content ?? {};
      const trId = content.tr_id ?? "";
      const chainId = process.env.CHAIN_ID || "UNKNOWN_CHAIN";

      const baseSchema =
        typeof content.json_schema === "string"
          ? JSON.parse(content.json_schema)
          : content.json_schema ?? {};

      const payload: Record<string, any> = {
        tr_id: trId,
        json_schema: JSON.stringify(baseSchema),
        deposit: deposit?.toString() ?? "0",
        created: timestamp ?? null,
        modified: timestamp ?? null,
        archived: null,
        is_active: content.is_active ?? false,
        issuer_grantor_validation_validity_period:
          content.issuer_grantor_validation_validity_period ?? 0,
        verifier_grantor_validation_validity_period:
          content.verifier_grantor_validation_validity_period ?? 0,
        issuer_validation_validity_period:
          content.issuer_validation_validity_period ?? 0,
        verifier_validation_validity_period:
          content.verifier_validation_validity_period ?? 0,
        holder_validation_validity_period:
          content.holder_validation_validity_period ?? 0,
        issuer_perm_management_mode: getModeString(
          content.issuer_perm_management_mode ?? 0
        ),
        verifier_perm_management_mode: getModeString(
          content.verifier_perm_management_mode ?? 0
        ),
        height: schemaMessage.height ?? 0,
      };

      const insertResult = await ctx.call(
        `${SERVICE.V1.CredentialSchemaDatabaseService.path}.upsert`,
        { payload }
      );

      const generatedId =
        (insertResult as any)?.data?.result?.id ??
        (insertResult as any)?.result?.id ??
        (insertResult as any)?.id;

      if (!generatedId) {
        throw new Error("❌ Failed to get generated ID from DB");
      }

      const updatedSchema = {
        ...baseSchema,
        $id: `vpr:verana:${chainId}/cs/v1/js/${generatedId}`,
      };

      const updatePayload: Record<string, any> = {
        id: generatedId,
        json_schema: JSON.stringify(updatedSchema),
        height: schemaMessage.height ?? 0,
      };

      await ctx.call(
        `${SERVICE.V1.CredentialSchemaDatabaseService.path}.update`,
        { payload: updatePayload }
      );

      this.logger.info(
        `✅ Stored credential schema tr_id=${trId} with final ID=${generatedId}`,
        updatePayload
      );
    } catch (err) {
      this.logger.error("❌ Error storing credential schema:", err);
    }
  }




  private async updateSchema(
    ctx: Context,
    schemaMessage: CredentialSchemaMessage,
    deposit: number
  ) {
    try {
      const payload: Record<string, any> = {
        ...schemaMessage,
        ...schemaMessage.content,
        deposit: deposit.toString(),
        modified: formatTimestamp(schemaMessage.timestamp),
        height: schemaMessage.height ?? 0,
      };

      delete payload.content;
      delete payload.timestamp;
      delete payload.type;
      delete payload.creator;
      delete payload["@type"];

      const result = await ctx.call(
        `${SERVICE.V1.CredentialSchemaDatabaseService.path}.update`,
        { payload }
      );
      this.logger.info(`✅ Updated credential schema id=${payload.id}`, result);
    } catch (err) {
      this.logger.error(
        `❌ Error updating credential schema id=${schemaMessage.id}:`,
        err
      );
    }
  }

  private async archiveSchema(
    ctx: Context,
    schemaMessage: CredentialSchemaMessage
  ) {
    try {
      const payload: Record<string, any> = {
        ...schemaMessage,
        ...schemaMessage.content,
        modified: formatTimestamp(schemaMessage.timestamp),
        height: schemaMessage.height ?? 0,
      };

      delete payload.content;
      delete payload.timestamp;
      delete payload.type;
      delete payload.creator;
      delete payload["@type"];

      await ctx.call(
        `${SERVICE.V1.CredentialSchemaDatabaseService.path}.archive`,
        { payload }
      );

      this.logger.info(
        `📦 Archive action executed for credential schema id=${payload.id}, archive=${payload.archive}`
      );
    } catch (err) {
      this.logger.error(
        `❌ Error archiving credential schema id=${schemaMessage.id}:`,
        err
      );
    }
  }
}
