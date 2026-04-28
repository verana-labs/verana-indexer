import {
  Action,
  Service,
} from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import BullableService from "../../base/bullable.service";
import { SERVICE } from "../../common";
import { VeranaCredentialSchemaMessageTypes } from "../../common/verana-message-types";
import { formatTimestamp } from "../../common/utils/date_utils";
import knex from "../../common/utils/db_connection";
import { getHolderOnboardingModeString, getModeString } from "./cs_types";
import { MessageProcessorBase } from "../../common/utils/message_processor_base";
import { detectStartMode } from "../../common/utils/start_mode_detector";
import { enrichSchemaMessageWithEvent } from "./cs_payload_helper";
import { parseCredentialSchemaEvent } from "./cs_event_mapper";

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

function extractNullableStringFromContent(
  content: Record<string, unknown>,
  snake: string,
  camel: string
): string | null {
  const v = content[snake] ?? content[camel];
  if (v == null || v === "") return null;
  return String(v);
}

function extractHolderOnboardingMode(content: Record<string, unknown>): string | null {
  const raw = content.holder_onboarding_mode ?? content.holderOnboardingMode;
  if (typeof raw === "string" && raw.trim()) return raw.trim();
  const n = Number(raw);
  if (Number.isFinite(n)) return getHolderOnboardingModeString(n);
  return null;
}

interface CredentialSchemaMessage {
  tr_id: number;
  id: number;
  type: string;
  content?: Record<string, unknown>;
  json_schema: string;
  deposit?: number;
  issuer_grantor_validation_validity_period: number;
  verifier_grantor_validation_validity_period: number;
  issuer_validation_validity_period: number;
  verifier_validation_validity_period: number;
  holder_validation_validity_period: number;
  issuer_onboarding_mode: string;
  verifier_onboarding_mode: string;
  timestamp?: string;
  creator?: string;
  archive?: string;
  "@type"?: string;
  height?: number;
  [key: string]: unknown;
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

    const processMessage = async (schemaMessageParams: CredentialSchemaMessage) => {
      const schemaMessage = enrichSchemaMessageWithEvent(
      schemaMessageParams,
      schemaMessageParams.txResponse
    );
      if (
        schemaMessage.type === VeranaCredentialSchemaMessageTypes.CreateCredentialSchema
      ) {
        await this.createSchema(ctx, schemaMessage);
      } else if (schemaMessage.type === VeranaCredentialSchemaMessageTypes.UpdateCredentialSchema) {
        await this.updateSchema(ctx, schemaMessage);
      } else if (schemaMessage.type === VeranaCredentialSchemaMessageTypes.ArchiveCredentialSchema) {
        await this.archiveSchema(ctx, schemaMessage);
      } else if (
        schemaMessage.type === VeranaCredentialSchemaMessageTypes.CreateSchemaAuthorizationPolicy ||
        schemaMessage.type === VeranaCredentialSchemaMessageTypes.IncreaseActiveSchemaAuthorizationPolicyVersion ||
        schemaMessage.type === VeranaCredentialSchemaMessageTypes.RevokeSchemaAuthorizationPolicy
      ) {
        this.logger.info(
          `[CredentialSchema] Schema authorization policy message observed (${schemaMessage.type}); no local projection table is defined yet.`
        );
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
    schemaMessage: CredentialSchemaMessage
  ) {
    try {
      const timestamp = formatTimestamp(schemaMessage.timestamp);
      const content = schemaMessage?.content ?? {};
      const trId = content.tr_id ?? content.trId ?? "";

      const blockchainSchemaId = schemaMessage.id ?? content.id ?? null;

      const jsonSchema = content.json_schema ?? content.jsonSchema ?? "{}";
      const jsonSchemaForDb =
        typeof jsonSchema === "string" ? jsonSchema : JSON.stringify(jsonSchema ?? {});

      const payload: Record<string, any> = {
        tr_id: trId,
        json_schema: jsonSchemaForDb,
        created: timestamp ?? null,
        modified: timestamp ?? null,
        archived: null,
        is_active: true,
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
        issuer_onboarding_mode: getModeString(
          Number(content.issuer_onboarding_mode ?? content.issuerOnboardingMode ?? 0)
        ),
        verifier_onboarding_mode: getModeString(
          Number(content.verifier_onboarding_mode ?? content.verifierOnboardingMode ?? 0)
        ),
        holder_onboarding_mode: extractHolderOnboardingMode(content as Record<string, unknown>),
        pricing_asset_type: extractNullableStringFromContent(
          content as Record<string, unknown>,
          "pricing_asset_type",
          "pricingAssetType"
        ),
        pricing_asset: extractNullableStringFromContent(
          content as Record<string, unknown>,
          "pricing_asset",
          "pricingAsset"
        ),
        digest_algorithm: extractNullableStringFromContent(
          content as Record<string, unknown>,
          "digest_algorithm",
          "digestAlgorithm"
        ),
        height: schemaMessage.height ?? 0,
        blockchainSchemaId: blockchainSchemaId,
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

      this.logger.info(
        `✅ Stored credential schema tr_id=${trId} with blockchain_id=${blockchainSchemaId} and database ID=${generatedId}`
      );
    } catch (err) {
      this.logger.error("❌ Error storing credential schema:", err);
    }
  }



  private async updateSchema(
    ctx: Context,
    schemaMessage: CredentialSchemaMessage
  ) {
    try {
      const content = schemaMessage.content ?? {};
      const payload: Record<string, any> = {
        id: schemaMessage.id ?? content.id,
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
        holder_onboarding_mode: extractHolderOnboardingMode(content as Record<string, unknown>),
        pricing_asset_type: extractNullableStringFromContent(
          content as Record<string, unknown>,
          "pricing_asset_type",
          "pricingAssetType"
        ),
        pricing_asset: extractNullableStringFromContent(
          content as Record<string, unknown>,
          "pricing_asset",
          "pricingAsset"
        ),
        digest_algorithm: extractNullableStringFromContent(
          content as Record<string, unknown>,
          "digest_algorithm",
          "digestAlgorithm"
        ),
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
      const eventData = parseCredentialSchemaEvent((schemaMessage as any).txResponse ?? null);
      const id = schemaMessage.id ?? (schemaMessage.content as any)?.id ?? eventData?.id;
      if (id == null) {
        this.logger.error("Archive: missing credential schema id");
        return;
      }
      const archive = eventData
        ? eventData.archived != null
        : (schemaMessage.content as any)?.archive === true;
      const modified = eventData?.modified
        ?? formatTimestamp(schemaMessage.timestamp)
        ?? new Date();
      const height = schemaMessage.height ?? 0;

      await ctx.call(
        `${SERVICE.V1.CredentialSchemaDatabaseService.path}.archive`,
        {
          payload: {
            id: Number(id),
            archive,
            modified: modified instanceof Date ? modified : new Date(modified as string),
            height,
          },
        }
      );
    } catch (err) {
      this.logger.error(
        `❌ Error archiving credential schema id=${schemaMessage?.id}:`,
        err
      );
    }
  }


}
