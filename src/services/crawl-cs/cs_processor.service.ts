import {
  Action,
  Service,
} from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BullableService from "../../base/bullable.service";
import {
  ModulesParamsNamesTypes,
  SERVICE,
} from "../../common";
import { VeranaCredentialSchemaMessageTypes } from "../../common/verana-message-types";
import { formatTimestamp } from "../../common/utils/date_utils";
import knex from "../../common/utils/db_connection";
import { getModeString } from "./cs_types";
import { MessageProcessorBase } from "../../common/utils/message_processor_base";
import { detectStartMode } from "../../common/utils/start_mode_detector";

function extractOptionalUInt32(value: unknown): number {
  if (value === undefined || value === null) {
    return 0;
  }
  if (typeof value === 'number') {
    return value;
  }
  if (typeof value === 'object' && value !== null) {
    if ('value' in value && typeof value.value === 'number') {
      return value.value;
    }
    if (Object.keys(value).length === 0) {
      return 0;
    }
  }
  return 0;
}

function getValidityPeriod(fieldName: string, content: Record<string, unknown> | undefined, schemaMessage: CredentialSchemaMessage | Record<string, unknown> | undefined): number {
  const snakeCase = fieldName;
  const camelCase = fieldName.replace(/_([a-z])/g, (_, letter) => letter.toUpperCase());
  
  const value = content?.[snakeCase] ?? content?.[camelCase] ?? 
              schemaMessage?.[snakeCase] ?? schemaMessage?.[camelCase];
  
  return extractOptionalUInt32(value);
}

interface CredentialSchemaMessage {
  tr_id: number;
  id: number;
  type: string;
  content?: Record<string, unknown>;
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
  [key: string]: unknown;
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
    this.logger.info(`CredentialSchema processor started | Mode: ${this._isFreshStart ? 'Fresh Start' : 'Reindexing'}`);
    return super._start();
  }

  @Action({ name: "handleCredentialSchemas" })
  async handleCredentialSchemas(
    ctx: Context<{ credentialSchemaMessages: CredentialSchemaMessage[] }>
  ) {
    const { credentialSchemaMessages } = ctx.params;

    if (!credentialSchemaMessages?.length) {
      return { success: false, message: "No credential schemas" };
    }

    const deposit = await calculateDeposit();

    const processMessage = async (schemaMessage: CredentialSchemaMessage) => {
      if (
        schemaMessage.type === VeranaCredentialSchemaMessageTypes.CreateCredentialSchema ||
        schemaMessage.type === VeranaCredentialSchemaMessageTypes.CreateCredentialSchemaLegacy
      ) {
        await this.createSchema(ctx, schemaMessage, deposit);
      } else if (schemaMessage.type === VeranaCredentialSchemaMessageTypes.UpdateCredentialSchema) {
        await this.updateSchema(ctx, schemaMessage, deposit);
      } else if (schemaMessage.type === VeranaCredentialSchemaMessageTypes.ArchiveCredentialSchema) {
        await this.archiveSchema(ctx, schemaMessage);
      }
    };

    await this.processorBase.processInBatches(
      credentialSchemaMessages,
      processMessage,
      {
        maxConcurrent: this._isFreshStart ? 3 : 8,
        batchSize: this._isFreshStart ? 20 : 50,
        delayBetweenBatches: this._isFreshStart ? 500 : 200,
      }
    );

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
      const trId = content.tr_id ?? content.trId ?? "";
      const chainId = process.env.CHAIN_ID || "UNKNOWN_CHAIN";

      const jsonSchema = content.json_schema ?? content.jsonSchema ?? "";
      const baseSchema =
        typeof jsonSchema === "string"
          ? JSON.parse(jsonSchema)
          : jsonSchema ?? {};

      const payload: Record<string, any> = {
        tr_id: trId,
        json_schema: JSON.stringify(baseSchema),
        deposit: deposit?.toString() ?? "0",
        created: timestamp ?? null,
        modified: timestamp ?? null,
        archived: null,
        is_active: content.is_active ?? content.isActive ?? false,
        issuer_grantor_validation_validity_period:
          getValidityPeriod('issuer_grantor_validation_validity_period', content, schemaMessage),
        verifier_grantor_validation_validity_period:
          getValidityPeriod('verifier_grantor_validation_validity_period', content, schemaMessage),
        issuer_validation_validity_period:
          getValidityPeriod('issuer_validation_validity_period', content, schemaMessage),
        verifier_validation_validity_period:
          getValidityPeriod('verifier_validation_validity_period', content, schemaMessage),
        holder_validation_validity_period:
          getValidityPeriod('holder_validation_validity_period', content, schemaMessage),
        issuer_perm_management_mode: getModeString(
          Number(content.issuer_perm_management_mode ?? content.issuerPermManagementMode ?? 0)
        ),
        verifier_perm_management_mode: getModeString(
          Number(content.verifier_perm_management_mode ?? content.verifierPermManagementMode ?? 0)
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
      const content = schemaMessage.content ?? {};
      const payload: Record<string, any> = {
        id: schemaMessage.id ?? content.id,
        deposit: deposit.toString(),
        modified: formatTimestamp(schemaMessage.timestamp),
        height: schemaMessage.height ?? 0,
        issuer_grantor_validation_validity_period:
          getValidityPeriod('issuer_grantor_validation_validity_period', content, schemaMessage),
        verifier_grantor_validation_validity_period:
          getValidityPeriod('verifier_grantor_validation_validity_period', content, schemaMessage),
        issuer_validation_validity_period:
          getValidityPeriod('issuer_validation_validity_period', content, schemaMessage),
        verifier_validation_validity_period:
          getValidityPeriod('verifier_validation_validity_period', content, schemaMessage),
        holder_validation_validity_period:
          getValidityPeriod('holder_validation_validity_period', content, schemaMessage),
      };

      if (content.json_schema || content.jsonSchema) {
        const jsonSchema = content.json_schema ?? content.jsonSchema;
        payload.json_schema = typeof jsonSchema === "string" ? jsonSchema : JSON.stringify(jsonSchema);
      }

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
