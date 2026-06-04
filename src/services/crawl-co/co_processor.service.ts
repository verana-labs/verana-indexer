import {
  Action,
  Service,
} from "@ourparentcenter/moleculer-decorators-extended";
import { Context, ServiceBroker } from "moleculer";
import type { Knex } from "knex";
import BullableService from "../../base/bullable.service";
import { SERVICE } from "../../common";
import {
  VeranaCorporationMessageTypes,
  VeranaGovernanceFrameworkMessageTypes,
} from "../../common/verana-message-types";
import { formatTimestamp } from "../../common/utils/date_utils";
import knex from "../../common/utils/db_connection";
import { MessageProcessorBase } from "../../common/utils/message_processor_base";

type ChangeRecord = Record<string, unknown>;

interface CorporationMemberInput {
  address: string;
  weight?: string;
  metadata?: string | null;
}

interface DecodedCoMessage {
  type: string;
  id?: number;
  height?: number | string;
  timestamp?: string;
  signer?: string;
  creator?: string;
  did?: string;
  language?: string;
  corporation?: string;
  members?: CorporationMemberInput[];
  group_metadata?: string;
  groupMetadata?: string;
  group_policy_metadata?: string;
  groupPolicyMetadata?: string;
  decision_policy?: unknown;
  decisionPolicy?: unknown;
  doc_url?: string;
  docUrl?: string;
  doc_digest_sri?: string;
  docDigestSri?: string;
  doc_language?: string;
  docLanguage?: string;
  ecosystem_id?: number | string;
  ecosystemId?: number | string;
  version?: number | string;
  content?: Record<string, unknown>;
}

interface CorporationRow {
  id: number;
  did: string;
  corporation: string | null;
  creator: string | null;
  language: string | null;
  group_metadata: string | null;
  group_policy_metadata: string | null;
  decision_policy: string | null;
  doc_url: string | null;
  doc_digest_sri: string | null;
  created?: string;
  modified?: string;
  height?: number;
}

interface GfvRow {
  id: number;
}

function getErrorMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

@Service({
  name: SERVICE.V1.CorporationMessageProcessorService.key,
  version: 1,
})
export default class CorporationMessageProcessorService extends BullableService {
  private processorBase: MessageProcessorBase;

  constructor(broker: ServiceBroker) {
    super(broker);
    this.processorBase = new MessageProcessorBase(this);
  }

  @Action({ name: "handleCorporationMessages" })
  async handleCorporationMessages(
    ctx: Context<{ corporationList: DecodedCoMessage[] }>
  ) {
    const { corporationList } = ctx.params;
    if (!corporationList || corporationList.length === 0) {
      this.logger.warn("No Corporation/GovernanceFramework messages to process");
      return;
    }

    const failThreshold = 0.1;
    const failedMessages: { message: DecodedCoMessage; error: string }[] = [];
    const totalMessages = corporationList.length;

    const sortedMessages = [...corporationList].sort((a, b) => {
      const heightDiff = (Number(a.height) || 0) - (Number(b.height) || 0);
      if (heightDiff !== 0) return heightDiff;
      return (a.id || 0) - (b.id || 0);
    });

    let successCount = 0;
    for (const message of sortedMessages) {
      try {
        await this.processMessage(message);
        successCount++;
      } catch (err: unknown) {
        failedMessages.push({ message, error: getErrorMessage(err) });
      }
    }

    this.logger.info(
      `Corporation processing complete: ${successCount} succeeded, ${failedMessages.length} failed out of ${totalMessages}`
    );

    if (failedMessages.length > totalMessages * failThreshold) {
      const failureRate = ((failedMessages.length / totalMessages) * 100).toFixed(2);
      throw new Error(
        `Failed to process ${failedMessages.length}/${totalMessages} Corporation messages (${failureRate}%). Exceeds ${(failThreshold * 100).toFixed(0)}% threshold.`
      );
    }
  }

  private async processMessage(message: DecodedCoMessage): Promise<void> {
    if (!message?.type) {
      throw new Error(`Corporation message missing type: ${JSON.stringify(message)}`);
    }

    const processed = { ...message, ...message.content } as DecodedCoMessage;
    delete processed.content;

    switch (processed.type) {
      case VeranaCorporationMessageTypes.CreateCorporation:
        await this.processCreateCorporation(processed);
        break;
      case VeranaCorporationMessageTypes.UpdateCorporation:
        await this.processUpdateCorporation(processed);
        break;
      case VeranaGovernanceFrameworkMessageTypes.AddGovernanceFrameworkDocument:
        await this.processAddGovernanceFrameworkDocument(processed);
        break;
      case VeranaGovernanceFrameworkMessageTypes.IncreaseActiveGovernanceFrameworkVersion:
        await this.processIncreaseActiveGovernanceFrameworkVersion(processed);
        break;
      default:
        throw new Error(`Unknown Corporation/GF message type: ${processed.type}`);
    }
  }

  private async recordCorporationHistory(
    trx: Knex.Transaction,
    corporationId: number,
    eventType: string,
    height: number,
    oldData: CorporationRow | null,
    newData: CorporationRow
  ): Promise<void> {
    let changes: ChangeRecord | null = null;
    if (oldData) {
      const computed = this.processorBase.computeChanges(oldData, newData);
      changes = Object.keys(computed).length > 0 ? computed : null;
      if (!changes) return;
    } else {
      const creationChanges: ChangeRecord = {};
      for (const [key, value] of Object.entries(newData)) {
        if (value !== null && value !== undefined && key !== "id") {
          creationChanges[key] = value;
        }
      }
      changes = Object.keys(creationChanges).length > 0 ? creationChanges : null;
    }

    await trx("corporation_history").insert({
      corporation_id: corporationId,
      did: newData.did ?? null,
      corporation: newData.corporation ?? null,
      language: newData.language ?? null,
      event_type: eventType,
      height: Number(height),
      changes: changes ? JSON.stringify(changes) : null,
      created_at: newData.modified ?? newData.created ?? new Date(),
    });
  }

  private async processCreateCorporation(message: DecodedCoMessage): Promise<void> {
    const did = message.did;
    if (!did) {
      throw new Error("CreateCorporation message missing required field: did");
    }

    const timestamp = formatTimestamp(message.timestamp);
    const blockHeight = Number(message.height || 0);
    const members: CorporationMemberInput[] = Array.isArray(message.members)
      ? message.members
      : [];

    const trx = await knex.transaction();
    try {
      const existing = (await trx("corporation").where({ did }).first()) as
        | CorporationRow
        | undefined;

      const row = {
        did,
        corporation: message.corporation ?? null,
        creator: message.signer ?? message.creator ?? null,
        language: message.language ?? null,
        group_metadata: message.group_metadata ?? message.groupMetadata ?? null,
        group_policy_metadata:
          message.group_policy_metadata ?? message.groupPolicyMetadata ?? null,
        decision_policy: this.serializeDecisionPolicy(
          message.decision_policy ?? message.decisionPolicy
        ),
        doc_url: message.doc_url ?? message.docUrl ?? null,
        doc_digest_sri: message.doc_digest_sri ?? message.docDigestSri ?? null,
        modified: timestamp,
        height: blockHeight,
      };

      let corporation: CorporationRow;
      if (existing) {
        [corporation] = (await trx("corporation")
          .where({ id: existing.id })
          .update(row)
          .returning("*")) as CorporationRow[];
      } else {
        [corporation] = (await trx("corporation")
          .insert({ ...row, created: timestamp })
          .returning("*")) as CorporationRow[];
      }

      await trx("corporation_member").where({ corporation_id: corporation.id }).del();
      for (const member of members) {
        await trx("corporation_member").insert({
          corporation_id: corporation.id,
          address: member.address,
          weight: member.weight ?? "0",
          metadata: member.metadata ?? null,
          created: timestamp,
        });
      }

      await this.recordCorporationHistory(
        trx,
        corporation.id,
        existing ? "Update" : "Create",
        blockHeight,
        existing ?? null,
        corporation
      );

      if (row.doc_url || row.doc_digest_sri) {
        const [gfv] = (await trx("co_governance_framework_version")
          .insert({
            corporation_id: corporation.id,
            ecosystem_id: 0,
            version: 1,
            created: timestamp,
            active_since: timestamp,
          })
          .onConflict(["corporation_id", "ecosystem_id", "version"])
          .merge({ active_since: timestamp })
          .returning("*")) as GfvRow[];

        await trx("co_governance_framework_document").insert({
          gfv_id: gfv.id,
          language: row.language ?? "",
          url: row.doc_url ?? "",
          digest_sri: row.doc_digest_sri ?? "",
          created: timestamp,
        });
      }

      await trx.commit();
      this.logger.info(`Corporation created/updated: did=${did}, id=${corporation.id}`);
    } catch (err: unknown) {
      await trx.rollback();
      this.logger.error(
        `Failed to process CreateCorporation for did=${did}: ${getErrorMessage(err)}`
      );
      throw err;
    }
  }

  private async processUpdateCorporation(message: DecodedCoMessage): Promise<void> {
    const trx = await knex.transaction();
    try {
      const corporation = await this.findCorporation(trx, message);
      if (!corporation) {
        await trx.rollback();
        this.logger.warn(
          `UpdateCorporation: corporation not found (corporation=${message.corporation}, did=${message.did}), height=${message.height}`
        );
        return;
      }

      const timestamp = formatTimestamp(message.timestamp);
      const updateData: CorporationRow = { ...corporation };
      if (message.did !== undefined) updateData.did = message.did;
      if (message.corporation !== undefined) updateData.corporation = message.corporation;
      updateData.modified = timestamp;
      updateData.height = Number(message.height || corporation.height || 0);

      await trx("corporation").where({ id: corporation.id }).update(updateData);
      await this.recordCorporationHistory(
        trx,
        corporation.id,
        "Update",
        Number(message.height || 0),
        corporation,
        updateData
      );

      await trx.commit();
      this.logger.info(`Corporation updated: id=${corporation.id}`);
    } catch (err: unknown) {
      await trx.rollback();
      this.logger.error(
        `Failed to process UpdateCorporation: ${getErrorMessage(err)}`
      );
      throw err;
    }
  }

  private async processAddGovernanceFrameworkDocument(message: DecodedCoMessage): Promise<void> {
    const trx = await knex.transaction();
    try {
      const corporation = await this.findCorporation(trx, message);
      if (!corporation) {
        await trx.rollback();
        this.logger.warn(
          `AddGovernanceFrameworkDocument: corporation not found (corporation=${message.corporation}), height=${message.height}`
        );
        return;
      }

      const timestamp = formatTimestamp(message.timestamp);
      const ecosystemId = Number(message.ecosystem_id ?? message.ecosystemId ?? 0);
      const version = Number(message.version ?? 0);
      const language = message.doc_language ?? message.docLanguage ?? message.language ?? "";
      const url = message.doc_url ?? message.docUrl ?? "";
      const digestSri = message.doc_digest_sri ?? message.docDigestSri ?? "";

      let gfv = (await trx("co_governance_framework_version")
        .where({ corporation_id: corporation.id, ecosystem_id: ecosystemId, version })
        .first()) as GfvRow | undefined;

      if (!gfv) {
        [gfv] = (await trx("co_governance_framework_version")
          .insert({
            corporation_id: corporation.id,
            ecosystem_id: ecosystemId,
            version,
            created: timestamp,
          })
          .returning("*")) as GfvRow[];
      }

      await trx("co_governance_framework_document").insert({
        gfv_id: gfv.id,
        language,
        url,
        digest_sri: digestSri,
        created: timestamp,
      });

      await trx.commit();
      this.logger.info(
        `AddGovernanceFrameworkDocument OK: corporation_id=${corporation.id}, ecosystem_id=${ecosystemId}, version=${version}`
      );
    } catch (err: unknown) {
      await trx.rollback();
      this.logger.error(
        `Failed to process AddGovernanceFrameworkDocument: ${getErrorMessage(err)}`
      );
      throw err;
    }
  }

  private async processIncreaseActiveGovernanceFrameworkVersion(message: DecodedCoMessage): Promise<void> {
    const trx = await knex.transaction();
    try {
      const corporation = await this.findCorporation(trx, message);
      if (!corporation) {
        await trx.rollback();
        this.logger.warn(
          `IncreaseActiveGovernanceFrameworkVersion: corporation not found (corporation=${message.corporation}), height=${message.height}`
        );
        return;
      }

      const timestamp = formatTimestamp(message.timestamp);
      const ecosystemId = Number(message.ecosystem_id ?? message.ecosystemId ?? 0);

      const activeRow = (await trx("co_governance_framework_version")
        .where({ corporation_id: corporation.id, ecosystem_id: ecosystemId })
        .whereNotNull("active_since")
        .orderBy("version", "desc")
        .first()) as { version: number | string } | undefined;
      const currentVersion = activeRow ? Number(activeRow.version) : 0;
      const nextVersion = currentVersion + 1;

      const gfv = (await trx("co_governance_framework_version")
        .where({ corporation_id: corporation.id, ecosystem_id: ecosystemId, version: nextVersion })
        .first()) as GfvRow | undefined;
      if (!gfv) {
        await trx.rollback();
        this.logger.warn(
          `IncreaseActiveGovernanceFrameworkVersion: version ${nextVersion} not found for corporation_id=${corporation.id}, ecosystem_id=${ecosystemId}. Retry needed.`
        );
        throw new Error(
          `GFV version ${nextVersion} not found for corporation_id=${corporation.id}, retry needed`
        );
      }

      await trx("co_governance_framework_version")
        .where({ id: gfv.id })
        .update({ active_since: timestamp });

      await trx.commit();
      this.logger.info(
        `IncreaseActiveGovernanceFrameworkVersion OK: corporation_id=${corporation.id}, ecosystem_id=${ecosystemId}, version=${nextVersion}`
      );
    } catch (err: unknown) {
      await trx.rollback();
      this.logger.error(
        `Failed to process IncreaseActiveGovernanceFrameworkVersion: ${getErrorMessage(err)}`
      );
      throw err;
    }
  }

  private async findCorporation(
    trx: Knex.Transaction,
    message: DecodedCoMessage
  ): Promise<CorporationRow | null> {
    const address = message.corporation;
    if (address && typeof address === "string") {
      const byAddress = (await trx("corporation").where({ corporation: address }).first()) as
        | CorporationRow
        | undefined;
      if (byAddress) return byAddress;
    }
    if (message.did) {
      const byDid = (await trx("corporation").where({ did: message.did }).first()) as
        | CorporationRow
        | undefined;
      if (byDid) return byDid;
    }
    return null;
  }

  private serializeDecisionPolicy(decisionPolicy: unknown): string | null {
    if (decisionPolicy === null || decisionPolicy === undefined) return null;
    try {
      return JSON.stringify(decisionPolicy);
    } catch {
      return null;
    }
  }
}
