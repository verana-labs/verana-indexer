/* eslint-disable @typescript-eslint/no-explicit-any */

import { Action, Service } from '@ourparentcenter/moleculer-decorators-extended'
import { Context, ServiceBroker } from 'moleculer'
import BaseService from '../../base/base.service'
import { MODULE_DISPLAY_NAMES, ModulesParamsNamesTypes, SERVICE } from '../../common'
import { validateParticipantParam } from '../../common/utils/accountValidation'
import ApiResponder from '../../common/utils/apiResponse'
import knex from '../../common/utils/db_connection'
import {
  ensureDepositDefaultIfColumnExists,
  finalizeEcosystemHistoryInsert,
  prepareEcosystemSnapshotRowForInsert,
  resolveEcosystemHistoryParticipantColumn,
  resolveEcosystemParticipantColumn,
  resolveParticipantHistoryParticipantColumn,
  resolveParticipantsParticipantColumn,
} from '../../common/utils/installed_table_columns'
import {
  applyOrdering,
  parseSortParameter,
  sortByStandardAttributes,
  validateSortParameter,
} from '../../common/utils/query_ordering'
import { mapEcosystemApiFields } from '../../common/vpr-v4-mapping'
import { Ecosystem } from '../../models/ecosystem'
import { resolveCorporationIdByAddress } from '../crawl-co/corporation_resolve'
import { enrichTrustDataDeep, parseTrustDataMode } from '../resolver/trust-data-enrichment'
import { calculateEcosystemStats, TR_STATS_FIELDS } from './ec_stats'

function ledgerHasKey(obj: unknown, key: string): boolean {
  return typeof obj === 'object' && obj !== null && Object.hasOwn(obj, key)
}

@Service({
  name: SERVICE.V1.EcosystemDatabaseService.key,
  version: 1,
})
export default class EcosystemDatabaseService extends BaseService {
  private ecosystemHistoryColumnExistsCache = new Map<string, boolean>()
  private ecosystemHistoryColumnsCache: Set<string> | null = null
  private static readonly SQL_SORTABLE_TR_ATTRIBUTES = new Set<string>([
    'id',
    'modified',
    'created',
    'participants',
    'participants_ecosystem',
    'participants_issuer_grantor',
    'participants_issuer',
    'participants_verifier_grantor',
    'participants_verifier',
    'participants_holder',
    'active_schemas',
    'weight',
    'issued',
    'verified',
    'ecosystem_slash_events',
    'ecosystem_slashed_amount',
    'network_slash_events',
    'network_slashed_amount',
  ])

  public constructor(public broker: ServiceBroker) {
    super(broker)
  }

  private valuesEquivalent(left: unknown, right: unknown): boolean {
    if ((left === null || left === undefined) && (right === null || right === undefined)) {
      return true
    }

    if (left instanceof Date || right instanceof Date) {
      const leftMs = left instanceof Date ? left.getTime() : Date.parse(String(left))
      const rightMs = right instanceof Date ? right.getTime() : Date.parse(String(right))
      if (Number.isFinite(leftMs) && Number.isFinite(rightMs)) {
        return leftMs === rightMs
      }
    }

    const leftNum = typeof left === 'number' ? left : Number(left)
    const rightNum = typeof right === 'number' ? right : Number(right)
    if (Number.isFinite(leftNum) && Number.isFinite(rightNum)) {
      return leftNum === rightNum
    }

    return String(left) === String(right)
  }

  @Action({
    name: 'syncFromLedger',
  })
  public async syncFromLedger(ctx: Context<{ ledgerResponse: { ecosystem?: any; ec?: any }; blockHeight: number }>) {
    try {
      const { ledgerResponse, blockHeight } = ctx.params
      const raw = ledgerResponse?.ecosystem ?? ledgerResponse?.ecosystem ?? ledgerResponse?.ec
      if (!raw || typeof raw !== 'object') {
        return ApiResponder.error(ctx, 'Missing or invalid ledger ecosystem', 400)
      }

      const ecosystemId = Number((raw as any).id ?? (raw as any).ecosystem_id)
      if (!Number.isInteger(ecosystemId) || ecosystemId <= 0) {
        return ApiResponder.error(ctx, 'Invalid ecosystem id from ledger', 400)
      }

      const blockHeightNum = Number(blockHeight) || 0
      if (!Number.isInteger(blockHeightNum) || blockHeightNum <= 0) {
        return ApiResponder.error(ctx, 'No valid block height available for Trust Registry ledger sync', 400)
      }
      const ledgerTrCreatedAt = (raw as any).modified ?? (raw as any).created ?? new Date()
      const preSyncTr = await knex('ecosystem').where({ id: ecosystemId }).first()

      await knex.transaction(async (trx) => {
        const existingTr = await trx('ecosystem').where({ id: ecosystemId }).first()
        const rawObj = raw as Record<string, unknown>

        const activeVersionFromLedger =
          ledgerHasKey(rawObj, 'active_version') || ledgerHasKey(rawObj, 'activeVersion')
            ? Number((raw as any).active_version ?? (raw as any).activeVersion ?? 0) || 0
            : Number(existingTr?.active_version ?? 0) || 0

        const corporationIdValue = ledgerHasKey(rawObj, 'corporation_id')
          ? Number((raw as any).corporation_id ?? 0) || 0
          : Number((existingTr as any)?.corporation_id ?? 0) || 0

        const basePayload: Record<string, unknown> = {
          corporation_id: corporationIdValue,
          did: ledgerHasKey(rawObj, 'did') ? ((raw as any).did ?? null) : (existingTr?.did ?? null),
          created: ledgerHasKey(rawObj, 'created') ? ((raw as any).created ?? null) : (existingTr?.created ?? null),
          modified: ledgerHasKey(rawObj, 'modified') ? ((raw as any).modified ?? null) : (existingTr?.modified ?? null),
          archived: ledgerHasKey(rawObj, 'archived') ? ((raw as any).archived ?? null) : (existingTr?.archived ?? null),
          aka: ledgerHasKey(rawObj, 'aka') ? ((raw as any).aka ?? null) : (existingTr?.aka ?? null),
          language: ledgerHasKey(rawObj, 'language') ? ((raw as any).language ?? null) : (existingTr?.language ?? null),
          active_version: activeVersionFromLedger,
          height: blockHeightNum,
        }

        // skip changing height (avoids 23505). Do NOT use try/catch — Postgres aborts the txn on error (25P02).
        if (existingTr) {
          let payloadForUpdate = basePayload
          if (
            Number.isFinite(blockHeightNum) &&
            blockHeightNum > 0 &&
            Number(existingTr.height) !== Number(blockHeightNum)
          ) {
            const otherOwner = await trx('ecosystem')
              .select('id')
              .where('height', blockHeightNum)
              .whereNot('id', ecosystemId)
              .first()
            if (otherOwner) {
              const { height: originalHeight, ...ledgerFieldsOnly } = basePayload
              payloadForUpdate = {
                ...ledgerFieldsOnly,
                height: existingTr.height ?? originalHeight,
              }
              this.logger.warn(
                `[EC syncFromLedger] ecosystem_id=${ecosystemId}: cannot set height=${blockHeightNum} (legacy UNIQUE(height) ` +
                  `held by id=${otherOwner.id}); kept height=${existingTr.height}. ` +
                  'Consider dropping/relaxing the legacy UNIQUE(height) constraint on ecosystem via your normal migration process.'
              )
            }
          }
          await trx('ecosystem').where({ id: ecosystemId }).update(payloadForUpdate)
        } else {
          const insertRow: Record<string, unknown> = {
            id: ecosystemId,
            ...basePayload,
          }
          await ensureDepositDefaultIfColumnExists(
            trx,
            'ecosystem',
            insertRow,
            (raw as any).deposit ?? (existingTr as any)?.deposit
          )
          await trx('ecosystem').insert(insertRow)
        }
        const hasVersionsInLedger = ledgerHasKey(rawObj, 'versions')
        const versions: any[] | null = hasVersionsInLedger
          ? Array.isArray((raw as any).versions)
            ? ((raw as any).versions as any[])
            : []
          : null

        if (versions === null) {
          this.logger.warn(
            `[EC syncFromLedger] Ledger response has no "versions" key for ecosystem_id=${ecosystemId} at height=${blockHeightNum}; ` +
              'skipping governance framework / version document reconciliation (trust row still updated).'
          )
        }

        const versionNumbers =
          versions === null
            ? []
            : versions.map((v) => Number((v as any).version ?? 0)).filter((v) => Number.isInteger(v) && v > 0)

        if (versions !== null && versionNumbers.length > 0) {
          await trx('governance_framework_version')
            .where({ ecosystem_id: ecosystemId })
            .whereNotIn('version', versionNumbers)
            .del()
        }

        const chainVersionIds: number[] = []
        const hasNewTrTables =
          (await trx.schema.hasTable('ecosystem_version')) && (await trx.schema.hasTable('ecosystem_document'))

        if (versions !== null) {
          for (const v of versions) {
            const versionNum = Number((v as any).version ?? 0) || 0
            if (!Number.isInteger(versionNum) || versionNum <= 0) continue

            const gfvBase: any = {
              ecosystem_id: ecosystemId,
              created: (v as any).created ?? null,
              version: versionNum,
              active_since: (v as any).active_since ?? (v as any).activeSince ?? null,
            }

            let gfvRow = await trx('governance_framework_version')
              .where({ ecosystem_id: ecosystemId, version: versionNum })
              .first()

            const oldGfvRow = gfvRow ? { ...gfvRow } : null
            const isGfvCreation = !gfvRow

            if (gfvRow) {
              await trx('governance_framework_version').where({ id: gfvRow.id }).update(gfvBase)
              gfvRow = await trx('governance_framework_version').where({ id: gfvRow.id }).first()
            } else {
              const [inserted] = await trx('governance_framework_version').insert(gfvBase).returning('*')
              gfvRow = inserted
            }

            const gfvId = Number(gfvRow.id)
            if (!Number.isInteger(gfvId) || gfvId <= 0) continue

            const chainVersionId = Number((v as any).id ?? (v as any).version_id ?? gfvRow.id)
            if (hasNewTrTables && Number.isInteger(chainVersionId) && chainVersionId > 0) {
              chainVersionIds.push(chainVersionId)
              await trx('ecosystem_version')
                .insert({
                  id: chainVersionId,
                  ecosystem_id: ecosystemId,
                  created: gfvRow.created ?? null,
                  version: gfvRow.version,
                  active_since: gfvRow.active_since ?? gfvRow.created ?? null,
                })
                .onConflict('id')
                .merge(['ecosystem_id', 'created', 'version', 'active_since'])
            }

            const gfvChanges: Record<string, any> = {}
            if (oldGfvRow) {
              for (const [key, value] of Object.entries(gfvRow)) {
                if (key !== 'id' && !this.valuesEquivalent(oldGfvRow[key], value)) {
                  gfvChanges[key] = value
                }
              }
            } else {
              for (const [key, value] of Object.entries(gfvRow)) {
                if (key !== 'id' && value !== null && value !== undefined) {
                  gfvChanges[key] = value
                }
              }
            }

            const docs: any[] = Array.isArray((v as any).documents) ? ((v as any).documents as any[]) : []

            const desiredDocKeys = new Set<string>()
            for (const d of docs) {
              const language = (d as any).language ?? null
              const digest = (d as any).digest_sri ?? (d as any).digestSri ?? null
              if (!digest) continue
              desiredDocKeys.add(`${language ?? ''}::${digest}`)
            }

            const existingDocs = await trx('governance_framework_document').where({ gfv_id: gfvId })
            for (const d of existingDocs) {
              const key = `${d.language ?? ''}::${d.digest_sri ?? ''}`
              if (!desiredDocKeys.has(key)) {
                await trx('governance_framework_document').where({ id: d.id }).del()
              }
            }

            for (const d of docs) {
              const language = (d as any).language ?? null
              const digest = (d as any).digest_sri ?? (d as any).digestSri ?? null
              if (!digest) continue

              const existingDoc = await trx('governance_framework_document')
                .where({
                  gfv_id: gfvId,
                  language,
                  digest_sri: digest,
                })
                .first()

              const docPayload: any = {
                gfv_id: gfvId,
                created: (d as any).created ?? null,
                language,
                url: (d as any).url ?? null,
                digest_sri: digest,
              }

              const oldDoc = existingDoc ? { ...existingDoc } : null
              const isDocCreation = !existingDoc

              if (existingDoc) {
                await trx('governance_framework_document').where({ id: existingDoc.id }).update(docPayload)
              } else {
                await trx('governance_framework_document').insert(docPayload)
              }

              const updatedDoc = await trx('governance_framework_document')
                .where({
                  gfv_id: gfvId,
                  language,
                  digest_sri: digest,
                })
                .first()

              if (!updatedDoc || !updatedDoc.id) continue

              const chainDocId = Number((d as any).id ?? (d as any).document_id ?? updatedDoc.id)
              if (hasNewTrTables && Number.isInteger(chainVersionId) && chainVersionId > 0) {
                const trDocBasePayload = {
                  version_id: chainVersionId,
                  created: updatedDoc.created ?? null,
                  language: updatedDoc.language ?? '',
                  url: updatedDoc.url ?? '',
                  digest_sri: updatedDoc.digest_sri ?? '',
                }

                const existingTrDoc = await trx('ecosystem_document')
                  .where({
                    version_id: chainVersionId,
                    url: updatedDoc.url ?? '',
                  })
                  .first()

                if (existingTrDoc && existingTrDoc.id != null) {
                  await trx('ecosystem_document').where({ id: existingTrDoc.id }).update(trDocBasePayload)
                } else if (Number.isInteger(chainDocId) && chainDocId > 0) {
                  await trx('ecosystem_document')
                    .insert({
                      id: chainDocId,
                      ...trDocBasePayload,
                    })
                    .onConflict(['version_id', 'url'])
                    .merge(['created', 'language', 'digest_sri'])
                }
              }

              const docChanges: Record<string, any> = {}
              if (oldDoc) {
                for (const [key, value] of Object.entries(updatedDoc)) {
                  if (key !== 'id' && !this.valuesEquivalent(oldDoc[key], value)) {
                    docChanges[key] = value
                  }
                }
              } else {
                for (const [key, value] of Object.entries(updatedDoc)) {
                  if (key !== 'id' && value !== null && value !== undefined) {
                    docChanges[key] = value
                  }
                }
              }

              if (isGfvCreation || isDocCreation || Object.keys(docChanges).length > 0) {
                await trx('governance_framework_document_history').insert({
                  gfv_id: gfvId,
                  ecosystem_id: ecosystemId,
                  created: updatedDoc.created || new Date(),
                  language: updatedDoc.language || '',
                  url: updatedDoc.url || '',
                  digest_sri: updatedDoc.digest_sri || '',
                  event_type: isDocCreation ? 'CreateGFD' : isGfvCreation ? 'CreateGFD' : 'UpdateGFD',
                  height: blockHeightNum,
                  changes: Object.keys(docChanges).length > 0 ? JSON.stringify(docChanges) : null,
                  created_at: updatedDoc.created || new Date(),
                })
              }
            }

            if (isGfvCreation || Object.keys(gfvChanges).length > 0) {
              await trx('governance_framework_version_history').insert({
                ecosystem_id: ecosystemId,
                created: gfvRow.created || new Date(),
                version: gfvRow.version,
                active_since: gfvRow.active_since || gfvRow.created || new Date(),
                event_type: isGfvCreation ? 'CreateGFV' : 'UpdateGFV',
                height: blockHeightNum,
                changes: Object.keys(gfvChanges).length > 0 ? JSON.stringify(gfvChanges) : null,
                created_at: gfvRow.active_since || gfvRow.created || new Date(),
              })
            }
          }

          if (hasNewTrTables) {
            const existingVersionRows = await trx('ecosystem_version').where({ ecosystem_id: ecosystemId }).select('id')
            const existingIds = (existingVersionRows || []).map((r: any) => Number(r.id))
            const toRemove = existingIds.filter((id: number) => !chainVersionIds.includes(id))
            if (toRemove.length > 0) {
              await trx('ecosystem_document').whereIn('version_id', toRemove).del()
            }
            if (chainVersionIds.length > 0) {
              await trx('ecosystem_version')
                .where({ ecosystem_id: ecosystemId })
                .whereNotIn('id', chainVersionIds)
                .del()
            } else {
              await trx('ecosystem_version').where({ ecosystem_id: ecosystemId }).del()
            }
          }
        }
      })

      const currentTr = await knex('ecosystem').where({ id: ecosystemId }).first()
      if (!currentTr) {
        return ApiResponder.error(ctx, 'EC not found after sync', 500)
      }

      let computedStats: any = null
      try {
        computedStats = await calculateEcosystemStats(ecosystemId, undefined)
      } catch (statsErr: any) {
        this.logger.warn(
          `[EC syncFromLedger] Failed to calculate stats for ecosystem_id=${ecosystemId}: ${statsErr?.message || String(statsErr)}`
        )
      }

      const statsUpdatePayload: Record<string, number> = {}
      for (const field of TR_STATS_FIELDS) {
        const fromComputed = computedStats != null && field in computedStats ? Number(computedStats[field]) : undefined
        const fromCurrent = currentTr[field] != null ? Number(currentTr[field]) : undefined
        const value: number = Number.isFinite(fromComputed)
          ? (fromComputed as number)
          : Number.isFinite(fromCurrent)
            ? (fromCurrent as number)
            : 0
        statsUpdatePayload[field] = value
      }

      await knex('ecosystem').where({ id: ecosystemId }).update(statsUpdatePayload)
      const updatedTr = await knex('ecosystem').where({ id: ecosystemId }).first()
      if (updatedTr) {
        for (const field of TR_STATS_FIELDS) {
          ;(updatedTr as any)[field] = statsUpdatePayload[field] ?? Number(updatedTr[field] ?? 0)
        }
      }

      if (updatedTr) {
        const oldTr = preSyncTr
        const trChanges: Record<string, any> = {}
        const statsChanges: Record<string, any> = {}
        const savedStats: Record<string, number> = { ...statsUpdatePayload }

        if (oldTr) {
          for (const [key, value] of Object.entries(updatedTr)) {
            if (
              key !== 'id' &&
              key !== 'height' &&
              !TR_STATS_FIELDS.includes(key) &&
              !this.valuesEquivalent(oldTr[key], value)
            ) {
              trChanges[key] = value
            }
          }
          for (const field of TR_STATS_FIELDS) {
            const oldVal = oldTr[field] != null ? Number(oldTr[field]) : 0
            const newVal = savedStats[field] ?? Number(updatedTr[field] ?? 0)
            if (oldVal !== newVal) {
              statsChanges[field] = newVal
            }
          }
        } else {
          for (const [key, value] of Object.entries(updatedTr)) {
            if (
              key !== 'id' &&
              key !== 'height' &&
              !TR_STATS_FIELDS.includes(key) &&
              value !== null &&
              value !== undefined
            ) {
              trChanges[key] = value
            }
          }
          for (const field of TR_STATS_FIELDS) {
            trChanges[field] = savedStats[field] ?? Number(updatedTr[field] ?? 0)
          }
        }

        const hasCoreChanges = Object.keys(trChanges).length > 0
        const hasStatsChanges = Object.keys(statsChanges).length > 0

        if (!oldTr || hasCoreChanges || hasStatsChanges) {
          await knex.transaction(async (trx) => {
            const baseHistoryPayload = await this.withDynamicEcosystemHistoryColumns(
              trx,
              {
                ecosystem_id: ecosystemId,
                did: updatedTr.did,
                corporation_id: updatedTr.corporation_id,
                created: updatedTr.created,
                modified: updatedTr.modified,
                archived: updatedTr.archived ?? null,
                aka: updatedTr.aka ?? null,
                language: updatedTr.language,
                active_version: updatedTr.active_version ?? null,
                participants: savedStats.participants ?? 0,
                participants_ecosystem: savedStats.participants_ecosystem ?? 0,
                participants_issuer_grantor: savedStats.participants_issuer_grantor ?? 0,
                participants_issuer: savedStats.participants_issuer ?? 0,
                participants_verifier_grantor: savedStats.participants_verifier_grantor ?? 0,
                participants_verifier: savedStats.participants_verifier ?? 0,
                participants_holder: savedStats.participants_holder ?? 0,
                active_schemas: savedStats.active_schemas ?? 0,
                archived_schemas: savedStats.archived_schemas ?? 0,
                weight: savedStats.weight ?? 0,
                issued: savedStats.issued ?? 0,
                verified: savedStats.verified ?? 0,
                ecosystem_slash_events: savedStats.ecosystem_slash_events ?? 0,
                ecosystem_slashed_amount: savedStats.ecosystem_slashed_amount ?? 0,
                ecosystem_slashed_amount_repaid: savedStats.ecosystem_slashed_amount_repaid ?? 0,
                network_slash_events: savedStats.network_slash_events ?? 0,
                network_slashed_amount: savedStats.network_slashed_amount ?? 0,
                network_slashed_amount_repaid: savedStats.network_slashed_amount_repaid ?? 0,
                event_type: oldTr ? 'Update' : 'Create',
                height: blockHeightNum,
                changes: Object.keys(trChanges).length > 0 ? JSON.stringify(trChanges) : null,
                created_at: ledgerTrCreatedAt,
              },
              { ...updatedTr, ...savedStats }
            )

            if (!oldTr || hasCoreChanges) {
              const existingSameBaseEvent = await trx('ecosystem_history')
                .where({
                  ecosystem_id: ecosystemId,
                  event_type: oldTr ? 'Update' : 'Create',
                  height: blockHeightNum,
                })
                .orderBy('id', 'desc')
                .first()
              const existingBaseChanges = existingSameBaseEvent?.changes ? String(existingSameBaseEvent.changes) : null
              const nextBaseChanges = baseHistoryPayload?.changes ? String(baseHistoryPayload.changes) : null
              if (!(existingSameBaseEvent && existingBaseChanges === nextBaseChanges)) {
                await trx('ecosystem_history').insert(baseHistoryPayload)
              }
            }

            if (oldTr && hasStatsChanges) {
              const statsHistoryPayload = await this.withDynamicEcosystemHistoryColumns(
                trx,
                {
                  ...baseHistoryPayload,
                  event_type: 'StatsUpdate',
                  height: blockHeightNum,
                  changes: JSON.stringify(statsChanges),
                },
                updatedTr
              )

              const existingSameStatsEvent = await trx('ecosystem_history')
                .where({
                  ecosystem_id: ecosystemId,
                  event_type: 'StatsUpdate',
                  height: blockHeightNum,
                })
                .orderBy('id', 'desc')
                .first()
              const existingStatsChanges = existingSameStatsEvent?.changes
                ? String(existingSameStatsEvent.changes)
                : null
              const nextStatsChanges = String(statsHistoryPayload.changes)
              if (!(existingSameStatsEvent && existingStatsChanges === nextStatsChanges)) {
                await trx('ecosystem_history').insert(statsHistoryPayload)
              }
            }
          })
        }

        if (
          process.env.USE_HEIGHT_SYNC_TR === 'true' &&
          updatedTr &&
          (await knex.schema.hasTable('ecosystem_snapshot')) &&
          (await knex.schema.hasTable('ecosystem_snapshot_diff'))
        ) {
          const toJsonSafe = (x: any): string | number | null => {
            if (x === null || x === undefined) return null
            if (typeof x === 'number' && Number.isFinite(x)) return x
            if (typeof x === 'string') return x
            if (x instanceof Date) return x.toISOString()
            return String(x)
          }
          const versionsForSnapshot: any[] = []
          const rawVersions: any[] = Array.isArray((raw as any).versions) ? (raw as any).versions : []
          for (const v of rawVersions) {
            const vid = Number((v as any).id ?? (v as any).version_id ?? 0)
            const docs: any[] = []
            const rawDocs: any[] = Array.isArray((v as any).documents) ? (v as any).documents : []
            for (const d of rawDocs) {
              docs.push({
                id: Number((d as any).id ?? (d as any).document_id ?? 0),
                version_id: vid,
                created: toJsonSafe((d as any).created),
                language: toJsonSafe((d as any).language),
                url: toJsonSafe((d as any).url),
                digest_sri: toJsonSafe((d as any).digest_sri ?? (d as any).digestSri),
              })
            }
            versionsForSnapshot.push({
              id: vid,
              ecosystem_id: ecosystemId,
              created: toJsonSafe((v as any).created),
              version: Number((v as any).version ?? 0),
              active_since: toJsonSafe((v as any).active_since ?? (v as any).activeSince),
              documents: docs,
            })
          }
          const versionsSnapshotJson = versionsForSnapshot.length > 0 ? JSON.stringify(versionsForSnapshot) : null
          const eventType = preSyncTr ? 'Update' : 'Create'
          const snapshotStats = savedStats ?? statsUpdatePayload
          const insertPayload: Record<string, any> = {
            ecosystem_id: ecosystemId,
            height: blockHeightNum,
            event_type: eventType,
            did: updatedTr.did,
            created: updatedTr.created,
            modified: updatedTr.modified,
            archived: updatedTr.archived ?? null,
            aka: updatedTr.aka ?? null,
            language: updatedTr.language,
            active_version: updatedTr.active_version ?? null,
            participants: snapshotStats.participants ?? 0,
            participants_ecosystem: snapshotStats.participants_ecosystem ?? 0,
            participants_issuer_grantor: snapshotStats.participants_issuer_grantor ?? 0,
            participants_issuer: snapshotStats.participants_issuer ?? 0,
            participants_verifier_grantor: snapshotStats.participants_verifier_grantor ?? 0,
            participants_verifier: snapshotStats.participants_verifier ?? 0,
            participants_holder: snapshotStats.participants_holder ?? 0,
            active_schemas: snapshotStats.active_schemas ?? 0,
            archived_schemas: snapshotStats.archived_schemas ?? 0,
            weight: snapshotStats.weight ?? 0,
            issued: snapshotStats.issued ?? 0,
            verified: snapshotStats.verified ?? 0,
            ecosystem_slash_events: snapshotStats.ecosystem_slash_events ?? 0,
            ecosystem_slashed_amount: snapshotStats.ecosystem_slashed_amount ?? 0,
            ecosystem_slashed_amount_repaid: snapshotStats.ecosystem_slashed_amount_repaid ?? 0,
            network_slash_events: snapshotStats.network_slash_events ?? 0,
            network_slashed_amount: snapshotStats.network_slashed_amount ?? 0,
            network_slashed_amount_repaid: snapshotStats.network_slashed_amount_repaid ?? 0,
          }
          if (versionsSnapshotJson !== null) {
            insertPayload.versions_snapshot = knex.raw('?::jsonb', [versionsSnapshotJson])
          }
          const snapshotInsertRow = await prepareEcosystemSnapshotRowForInsert(
            knex,
            insertPayload as Record<string, unknown>,
            {
              ecosystemRow: updatedTr as Record<string, unknown>,
              rawLedger: raw as Record<string, unknown>,
            }
          )
          const [snapshotRow] = await knex('ecosystem_snapshot').insert(snapshotInsertRow).returning('id')
          const nextSnapshotId = snapshotRow?.id
          if (nextSnapshotId) {
            const prevSnapshot = await knex('ecosystem_snapshot')
              .where({ ecosystem_id: ecosystemId })
              .where('height', '<', blockHeightNum)
              .orderBy('height', 'desc')
              .orderBy('id', 'desc')
              .first()
            const prevSnapshotId = prevSnapshot?.id ?? null
            const diffPayload: any = { added: {}, removed: {}, changed: {} }
            if (preSyncTr) {
              for (const key of Object.keys(updatedTr)) {
                if (key === 'id' || key === 'height') continue
                const oldVal = preSyncTr[key]
                const newVal = updatedTr[key]
                if (!this.valuesEquivalent(oldVal, newVal)) {
                  diffPayload.changed[key] = newVal
                }
              }
            } else {
              for (const key of Object.keys(updatedTr)) {
                if (key === 'id' || key === 'height') continue
                if (updatedTr[key] !== null && updatedTr[key] !== undefined) {
                  diffPayload.added[key] = updatedTr[key]
                }
              }
            }
            await knex('ecosystem_snapshot_diff').insert({
              ecosystem_id: ecosystemId,
              height: blockHeightNum,
              event_type: eventType,
              prev_snapshot_id: prevSnapshotId,
              next_snapshot_id: nextSnapshotId,
              diff: diffPayload,
            })
          }
        }
      }

      return ApiResponder.success(ctx, { success: true }, 200)
    } catch (err: any) {
      this.logger.error('Error in Ecosystem syncFromLedger:', err)
      return ApiResponder.error(ctx, 'Internal Server Error', 500)
    }
  }

  private async hasEcosystemHistoryColumn(tableName: string, columnName: string): Promise<boolean> {
    const key = `${tableName}.${columnName}`
    const cached = this.ecosystemHistoryColumnExistsCache.get(key)
    if (cached !== undefined) {
      return cached
    }
    const exists = await knex.schema.hasColumn(tableName, columnName)
    this.ecosystemHistoryColumnExistsCache.set(key, exists)
    return exists
  }

  private async getEcosystemHistoryColumns(trx: any): Promise<Set<string>> {
    if (this.ecosystemHistoryColumnsCache) {
      return this.ecosystemHistoryColumnsCache
    }
    const info = await trx('ecosystem_history').columnInfo()
    this.ecosystemHistoryColumnsCache = new Set(Object.keys(info || {}))
    return this.ecosystemHistoryColumnsCache
  }

  private async withDynamicEcosystemHistoryColumns(
    trx: any,
    payload: Record<string, any>,
    ecosystemRow: Record<string, any>
  ): Promise<Record<string, any>> {
    const historyColumns = await this.getEcosystemHistoryColumns(trx)
    return finalizeEcosystemHistoryInsert(historyColumns, payload, ecosystemRow) as Record<string, any>
  }

  private usesDerivedMetricSort(sort?: string): boolean {
    if (!sort || typeof sort !== 'string') return false
    const lower = sort.toLowerCase()
    const derivedKeys = [
      'participants',
      'active_schemas',
      'weight',
      'issued',
      'verified',
      'ecosystem_slash_events',
      'ecosystem_slashed_amount',
      'network_slash_events',
      'network_slashed_amount',
    ]
    return derivedKeys.some((key) => lower.includes(key))
  }

  private applyEcosystemSqlSort(query: any, sort?: string): { fullyApplied: boolean } {
    if (!sort || typeof sort !== 'string' || !sort.trim()) {
      query.orderBy('modified', 'desc').orderBy('id', 'desc')
      return { fullyApplied: true }
    }

    const sortOrders = parseSortParameter(sort)
    let hasIdSort = false
    let fullyApplied = true
    for (const { attribute, direction } of sortOrders) {
      if (!EcosystemDatabaseService.SQL_SORTABLE_TR_ATTRIBUTES.has(attribute)) {
        fullyApplied = false
        continue
      }
      query.orderBy(attribute, direction)
      if (attribute === 'id') hasIdSort = true
    }

    if (!hasIdSort) {
      query.orderBy('id', 'desc')
    }

    return { fullyApplied }
  }

  private static toFiniteNumber(value: unknown): number {
    const num = typeof value === 'number' ? value : Number(value ?? 0)
    return Number.isFinite(num) ? num : 0
  }

  private applyRangeToQuery(query: any, column: string, minValue?: number, maxValue?: number): any {
    if (minValue !== undefined && maxValue !== undefined && minValue === maxValue) {
      return query.whereRaw('1 = 0')
    }
    let nextQuery = query
    if (minValue !== undefined) {
      nextQuery = nextQuery.where(column, '>=', minValue)
    }
    if (maxValue !== undefined) {
      nextQuery = nextQuery.where(column, '<', maxValue)
    }
    return nextQuery
  }

  private applyRangeToRows<T>(
    rows: T[],
    minValue: number | string | undefined,
    maxValue: number | string | undefined,
    readValue: (row: T) => number
  ): T[] {
    if (minValue !== undefined && maxValue !== undefined && minValue === maxValue) {
      return []
    }

    let filtered = rows
    if (minValue !== undefined) {
      const minNum = Number(minValue)
      filtered = filtered.filter((row) => readValue(row) >= minNum)
    }
    if (maxValue !== undefined) {
      const maxNum = Number(maxValue)
      filtered = filtered.filter((row) => readValue(row) < maxNum)
    }
    return filtered
  }

  private applyMetricFiltersToRegistries(
    rows: any[],
    filters: {
      minActiveSchemas?: number
      maxActiveSchemas?: number
      minParticipants?: number
      maxParticipants?: number
      minParticipantsEcosystem?: number
      maxParticipantsEcosystem?: number
      minParticipantsIssuerGrantor?: number
      maxParticipantsIssuerGrantor?: number
      minParticipantsIssuer?: number
      maxParticipantsIssuer?: number
      minParticipantsVerifierGrantor?: number
      maxParticipantsVerifierGrantor?: number
      minParticipantsVerifier?: number
      maxParticipantsVerifier?: number
      minParticipantsHolder?: number
      maxParticipantsHolder?: number
      minWeight?: string
      maxWeight?: string
      minIssued?: string
      maxIssued?: string
      minVerified?: string
      maxVerified?: string
      minEcosystemSlashEvents?: number
      maxEcosystemSlashEvents?: number
      minNetworkSlashEvents?: number
      maxNetworkSlashEvents?: number
    }
  ): any[] {
    let filtered = rows
    filtered = this.applyRangeToRows(filtered, filters.minActiveSchemas, filters.maxActiveSchemas, (r) =>
      EcosystemDatabaseService.toFiniteNumber(r.active_schemas)
    )
    filtered = this.applyRangeToRows(filtered, filters.minParticipants, filters.maxParticipants, (r) =>
      EcosystemDatabaseService.toFiniteNumber(r.participants)
    )
    filtered = this.applyRangeToRows(
      filtered,
      filters.minParticipantsEcosystem,
      filters.maxParticipantsEcosystem,
      (r) => EcosystemDatabaseService.toFiniteNumber(r.participants_ecosystem)
    )
    filtered = this.applyRangeToRows(
      filtered,
      filters.minParticipantsIssuerGrantor,
      filters.maxParticipantsIssuerGrantor,
      (r) => EcosystemDatabaseService.toFiniteNumber(r.participants_issuer_grantor)
    )
    filtered = this.applyRangeToRows(filtered, filters.minParticipantsIssuer, filters.maxParticipantsIssuer, (r) =>
      EcosystemDatabaseService.toFiniteNumber(r.participants_issuer)
    )
    filtered = this.applyRangeToRows(
      filtered,
      filters.minParticipantsVerifierGrantor,
      filters.maxParticipantsVerifierGrantor,
      (r) => EcosystemDatabaseService.toFiniteNumber(r.participants_verifier_grantor)
    )
    filtered = this.applyRangeToRows(filtered, filters.minParticipantsVerifier, filters.maxParticipantsVerifier, (r) =>
      EcosystemDatabaseService.toFiniteNumber(r.participants_verifier)
    )
    filtered = this.applyRangeToRows(filtered, filters.minParticipantsHolder, filters.maxParticipantsHolder, (r) =>
      EcosystemDatabaseService.toFiniteNumber(r.participants_holder)
    )
    filtered = this.applyRangeToRows(filtered, filters.minWeight, filters.maxWeight, (r) =>
      EcosystemDatabaseService.toFiniteNumber(r.weight)
    )
    filtered = this.applyRangeToRows(filtered, filters.minIssued, filters.maxIssued, (r) =>
      EcosystemDatabaseService.toFiniteNumber(r.issued)
    )
    filtered = this.applyRangeToRows(filtered, filters.minVerified, filters.maxVerified, (r) =>
      EcosystemDatabaseService.toFiniteNumber(r.verified)
    )
    filtered = this.applyRangeToRows(filtered, filters.minEcosystemSlashEvents, filters.maxEcosystemSlashEvents, (r) =>
      EcosystemDatabaseService.toFiniteNumber(r.ecosystem_slash_events)
    )
    filtered = this.applyRangeToRows(filtered, filters.minNetworkSlashEvents, filters.maxNetworkSlashEvents, (r) =>
      EcosystemDatabaseService.toFiniteNumber(r.network_slash_events)
    )
    return filtered
  }

  private sortRegistries(rows: any[], sort: string | undefined, limit: number): any[] {
    if (!this.usesDerivedMetricSort(sort)) {
      return rows.slice(0, limit)
    }
    return sortByStandardAttributes(rows, sort, {
      getId: (row) => row.id,
      getCreated: (row) => row.created,
      getModified: (row) => row.modified,
      getParticipants: (row) => row.participants,
      getParticipantsEcosystem: (row) => row.participants_ecosystem,
      getParticipantsIssuerGrantor: (row) => row.participants_issuer_grantor,
      getParticipantsIssuer: (row) => row.participants_issuer,
      getParticipantsVerifierGrantor: (row) => row.participants_verifier_grantor,
      getParticipantsVerifier: (row) => row.participants_verifier,
      getParticipantsHolder: (row) => row.participants_holder,
      getActiveSchemas: (row) => row.active_schemas,
      getWeight: (row) => row.weight,
      getIssued: (row) => row.issued,
      getVerified: (row) => row.verified,
      getEcosystemSlashEvents: (row) => row.ecosystem_slash_events,
      getEcosystemSlashedAmount: (row) => row.ecosystem_slashed_amount,
      getNetworkSlashEvents: (row) => row.network_slash_events,
      getNetworkSlashedAmount: (row) => row.network_slashed_amount,
      defaultAttribute: 'modified',
      defaultDirection: 'desc',
    }).slice(0, limit)
  }

  private hasImpossibleMetricRanges(filters: {
    minActiveSchemas?: number
    maxActiveSchemas?: number
    minParticipants?: number
    maxParticipants?: number
    minParticipantsEcosystem?: number
    maxParticipantsEcosystem?: number
    minParticipantsIssuerGrantor?: number
    maxParticipantsIssuerGrantor?: number
    minParticipantsIssuer?: number
    maxParticipantsIssuer?: number
    minParticipantsVerifierGrantor?: number
    maxParticipantsVerifierGrantor?: number
    minParticipantsVerifier?: number
    maxParticipantsVerifier?: number
    minParticipantsHolder?: number
    maxParticipantsHolder?: number
    minWeight?: string
    maxWeight?: string
    minIssued?: string
    maxIssued?: string
    minVerified?: string
    maxVerified?: string
    minEcosystemSlashEvents?: number
    maxEcosystemSlashEvents?: number
    minNetworkSlashEvents?: number
    maxNetworkSlashEvents?: number
  }): boolean {
    return (
      (filters.minActiveSchemas !== undefined &&
        filters.maxActiveSchemas !== undefined &&
        filters.minActiveSchemas === filters.maxActiveSchemas) ||
      (filters.minParticipants !== undefined &&
        filters.maxParticipants !== undefined &&
        filters.minParticipants === filters.maxParticipants) ||
      (filters.minParticipantsEcosystem !== undefined &&
        filters.maxParticipantsEcosystem !== undefined &&
        filters.minParticipantsEcosystem === filters.maxParticipantsEcosystem) ||
      (filters.minParticipantsIssuerGrantor !== undefined &&
        filters.maxParticipantsIssuerGrantor !== undefined &&
        filters.minParticipantsIssuerGrantor === filters.maxParticipantsIssuerGrantor) ||
      (filters.minParticipantsIssuer !== undefined &&
        filters.maxParticipantsIssuer !== undefined &&
        filters.minParticipantsIssuer === filters.maxParticipantsIssuer) ||
      (filters.minParticipantsVerifierGrantor !== undefined &&
        filters.maxParticipantsVerifierGrantor !== undefined &&
        filters.minParticipantsVerifierGrantor === filters.maxParticipantsVerifierGrantor) ||
      (filters.minParticipantsVerifier !== undefined &&
        filters.maxParticipantsVerifier !== undefined &&
        filters.minParticipantsVerifier === filters.maxParticipantsVerifier) ||
      (filters.minParticipantsHolder !== undefined &&
        filters.maxParticipantsHolder !== undefined &&
        filters.minParticipantsHolder === filters.maxParticipantsHolder) ||
      (filters.minWeight !== undefined && filters.maxWeight !== undefined && filters.minWeight === filters.maxWeight) ||
      (filters.minIssued !== undefined && filters.maxIssued !== undefined && filters.minIssued === filters.maxIssued) ||
      (filters.minVerified !== undefined &&
        filters.maxVerified !== undefined &&
        filters.minVerified === filters.maxVerified) ||
      (filters.minEcosystemSlashEvents !== undefined &&
        filters.maxEcosystemSlashEvents !== undefined &&
        filters.minEcosystemSlashEvents === filters.maxEcosystemSlashEvents) ||
      (filters.minNetworkSlashEvents !== undefined &&
        filters.maxNetworkSlashEvents !== undefined &&
        filters.minNetworkSlashEvents === filters.maxNetworkSlashEvents)
    )
  }

  private async buildHistoricalVersionsByEcosystemIds(
    ecosystemIdsInput: number[],
    blockHeight: number,
    activeGfOnly: boolean,
    preferredLanguage?: string
  ): Promise<Map<number, any[]>> {
    const ecosystemIds = Array.from(
      new Set(ecosystemIdsInput.map((id) => Number(id)).filter((id) => Number.isFinite(id) && id > 0))
    )
    const versionsByTr = new Map<number, any[]>()
    if (ecosystemIds.length === 0) return versionsByTr

    const gfvHistoryRows = await knex('governance_framework_version_history')
      .select('id', 'ecosystem_id', 'created', 'version', 'active_since', 'height', 'created_at')
      .whereIn('ecosystem_id', ecosystemIds)
      .where('height', '<=', blockHeight)
      .orderBy('ecosystem_id', 'asc')
      .orderBy('version', 'asc')
      .orderBy('height', 'desc')
      .orderBy('created_at', 'desc')

    const gfvRows = await knex('governance_framework_version')
      .select('id', 'ecosystem_id', 'version')
      .whereIn('ecosystem_id', ecosystemIds)
    const gfvIdByTrVersion = new Map<string, number>()
    for (const row of gfvRows) {
      gfvIdByTrVersion.set(`${Number(row.ecosystem_id)}::${Number(row.version)}`, Number(row.id))
    }

    const latestByTrVersion = new Map<string, any>()
    for (const gfv of gfvHistoryRows) {
      const ecosystemId = Number(gfv.ecosystem_id)
      const version = Number(gfv.version)
      const key = `${ecosystemId}::${version}`
      if (latestByTrVersion.has(key)) continue

      const actualGfvId = gfvIdByTrVersion.get(key)
      const entry = {
        id: Number.isFinite(actualGfvId as number) ? Number(actualGfvId) : Number(gfv.id),
        ecosystem_id: ecosystemId,
        created: gfv.created,
        version,
        active_since: gfv.active_since,
        documents: [] as any[],
      }
      latestByTrVersion.set(key, entry)
      const trList = versionsByTr.get(ecosystemId) || []
      trList.push(entry)
      versionsByTr.set(ecosystemId, trList)
    }

    const gfvIds = Array.from(
      new Set(
        Array.from(latestByTrVersion.values())
          .map((v: any) => Number(v.id))
          .filter((id) => Number.isFinite(id) && id > 0)
      )
    )
    if (gfvIds.length > 0) {
      const hasGfdIdColumn = await this.hasEcosystemHistoryColumn('governance_framework_document_history', 'gfd_id')
      const gfdSelectColumns: Array<string | any> = [
        'id',
        'gfv_id',
        'ecosystem_id',
        'created',
        'language',
        'url',
        'digest_sri',
        'height',
        'created_at',
      ]
      if (hasGfdIdColumn) {
        gfdSelectColumns.splice(1, 0, 'gfd_id')
      }
      const gfdHistoryRows = await knex('governance_framework_document_history')
        .select(...gfdSelectColumns)
        .whereIn('ecosystem_id', ecosystemIds)
        .whereIn('gfv_id', gfvIds)
        .where('height', '<=', blockHeight)
        .orderBy('gfv_id', 'asc')
        .orderBy('height', 'desc')
        .orderBy('created_at', 'desc')
        .orderBy('id', 'desc')

      const seenDocs = new Set<string>()
      const entryByGfvId = new Map<number, any>()
      for (const versionEntry of latestByTrVersion.values()) {
        entryByGfvId.set(Number(versionEntry.id), versionEntry)
      }

      for (const gfd of gfdHistoryRows) {
        const gfvId = Number(gfd.gfv_id)
        const versionEntry = entryByGfvId.get(gfvId)
        if (!versionEntry) continue

        const docId = Number(hasGfdIdColumn ? gfd.gfd_id : gfd.id)
        const dedupeId =
          Number.isFinite(docId) && docId > 0 ? `doc:${docId}` : `url:${gfd.url || ''}:${gfd.language || ''}`
        const dedupeKey = `${Number(versionEntry.ecosystem_id)}::${gfvId}::${dedupeId}`
        if (seenDocs.has(dedupeKey)) continue
        seenDocs.add(dedupeKey)

        versionEntry.documents.push({
          id: Number.isFinite(docId) && docId > 0 ? docId : Number(gfd.id),
          gfv_id: gfvId,
          created: gfd.created,
          language: gfd.language,
          url: gfd.url,
          digest_sri: gfd.digest_sri,
        })
      }
    }

    for (const [ecosystemId, versions] of versionsByTr.entries()) {
      let normalized = versions
      if (activeGfOnly) {
        normalized = versions
          .sort((a, b) => new Date(b.active_since).getTime() - new Date(a.active_since).getTime())
          .slice(0, 1)
      }
      if (preferredLanguage) {
        normalized = normalized.map((v) => ({
          ...v,
          documents: (v.documents || []).filter((d: any) => d.language === preferredLanguage),
        }))
      }
      versionsByTr.set(ecosystemId, normalized)
    }

    return versionsByTr
  }
  @Action()
  public async getEcosystem(
    ctx: Context<{
      ecosystem_id: number
      active_gf_only?: string | boolean
      preferred_language?: string
      trust_data?: string
    }>
  ) {
    try {
      const { ecosystem_id: ecosystemId, preferred_language: preferredLanguage } = ctx.params
      const trustDataRaw = (ctx.params as any).trust_data
      const trustDataModeParsed = parseTrustDataMode(trustDataRaw)
      if (!trustDataModeParsed.ok) {
        return ApiResponder.error(ctx, trustDataModeParsed.message, 400)
      }
      const trustDataMode = trustDataModeParsed.mode
      const activeGfOnly = String(ctx.params.active_gf_only).toLowerCase() === 'true'
      const blockHeight = (ctx.meta as any)?.blockHeight
      const useHeightSync = process.env.NODE_ENV !== 'test' && process.env.USE_HEIGHT_SYNC_TR === 'true'

      if (useHeightSync) {
        if (typeof blockHeight === 'number') {
          const snapshot = await knex('ecosystem_snapshot')
            .where({ ecosystem_id: Number(ecosystemId) })
            .where('height', '<=', blockHeight)
            .orderBy('height', 'desc')
            .orderBy('id', 'desc')
            .first()
          if (!snapshot) {
            return ApiResponder.error(ctx, `Ecosystem with id ${ecosystemId} not found`, 404)
          }
          let versions = Array.isArray(snapshot.versions_snapshot) ? snapshot.versions_snapshot : []
          if (activeGfOnly) {
            versions = [...versions]
              .sort((a: any, b: any) => {
                if (!a?.active_since && !b?.active_since) return 0
                if (!a?.active_since) return 1
                if (!b?.active_since) return -1
                return new Date(b.active_since).getTime() - new Date(a.active_since).getTime()
              })
              .slice(0, 1)
          }
          if (preferredLanguage) {
            versions = versions.map((v: any) => ({
              ...v,
              documents: (v.documents || []).filter((d: any) => d.language === preferredLanguage),
            }))
          }
          const ecosystem = {
            id: snapshot.ecosystem_id,
            did: snapshot.did,
            corporation_id: snapshot.corporation_id,
            created: snapshot.created,
            modified: snapshot.modified,
            archived: snapshot.archived,
            aka: snapshot.aka,
            language: snapshot.language,
            active_version: snapshot.active_version,
            versions,
            participants: Number(snapshot.participants ?? 0),
            participants_ecosystem: Number(snapshot.participants_ecosystem ?? 0),
            participants_issuer_grantor: Number(snapshot.participants_issuer_grantor ?? 0),
            participants_issuer: Number(snapshot.participants_issuer ?? 0),
            participants_verifier_grantor: Number(snapshot.participants_verifier_grantor ?? 0),
            participants_verifier: Number(snapshot.participants_verifier ?? 0),
            participants_holder: Number(snapshot.participants_holder ?? 0),
            active_schemas: Number(snapshot.active_schemas ?? 0),
            archived_schemas: Number(snapshot.archived_schemas ?? 0),
            weight: Number(snapshot.weight ?? 0),
            issued: Number(snapshot.issued ?? 0),
            verified: Number(snapshot.verified ?? 0),
            ecosystem_slash_events: Number(snapshot.ecosystem_slash_events ?? 0),
            ecosystem_slashed_amount: Number(snapshot.ecosystem_slashed_amount ?? 0),
            ecosystem_slashed_amount_repaid: Number(snapshot.ecosystem_slashed_amount_repaid ?? 0),
            network_slash_events: Number(snapshot.network_slash_events ?? 0),
            network_slashed_amount: Number(snapshot.network_slashed_amount ?? 0),
            network_slashed_amount_repaid: Number(snapshot.network_slashed_amount_repaid ?? 0),
          }
          const responsePayload = { ecosystem: mapEcosystemApiFields(ecosystem as Record<string, unknown>) }
          const enrichedResponsePayload =
            trustDataMode === 'none'
              ? responsePayload
              : await enrichTrustDataDeep(responsePayload, trustDataMode, blockHeight)
          return ApiResponder.success(ctx, enrichedResponsePayload, 200)
        }
        const ec = await knex('ecosystem').where({ id: ecosystemId }).first()
        if (!ec) {
          return ApiResponder.error(ctx, `Ecosystem with id ${ecosystemId} not found`, 404)
        }
        const versionsRows = await knex('ecosystem_version')
          .where({ ecosystem_id: Number(ecosystemId) })
          .orderBy('version', 'asc')
        const versionIds = versionsRows.map((r: any) => r.id)
        const documentsRows = versionIds.length
          ? await knex('ecosystem_document').whereIn('version_id', versionIds)
          : []
        const docByVersion = new Map<number, any[]>()
        for (const d of documentsRows as any[]) {
          const vid = Number(d.version_id)
          let docList = docByVersion.get(vid)
          if (!docList) {
            docList = []
            docByVersion.set(vid, docList)
          }
          docList.push({
            id: d.id,
            gfv_id: d.version_id,
            created: d.created,
            language: d.language,
            url: d.url,
            digest_sri: d.digest_sri,
          })
        }
        let versions = versionsRows.map((v: any) => ({
          id: v.id,
          ecosystem_id: v.ecosystem_id,
          created: v.created,
          version: v.version,
          active_since: v.active_since,
          documents: docByVersion.get(Number(v.id)) || [],
        }))
        if (activeGfOnly) {
          versions = [...versions]
            .sort((a: any, b: any) => {
              if (!a.active_since && !b.active_since) return 0
              if (!a.active_since) return 1
              if (!b.active_since) return -1
              return new Date(b.active_since).getTime() - new Date(a.active_since).getTime()
            })
            .slice(0, 1)
        }
        if (preferredLanguage) {
          versions = versions.map((v: any) => ({
            ...v,
            documents: (v.documents || []).filter((d: any) => d.language === preferredLanguage),
          }))
        }
        const t = ec as any
        const responsePayload = {
          ecosystem: mapEcosystemApiFields({
            id: ec.id,
            did: ec.did,
            corporation_id: ec.corporation_id,
            created: ec.created,
            modified: ec.modified,
            archived: ec.archived,
            aka: ec.aka,
            language: ec.language,
            active_version: ec.active_version,
            versions,
            participants: Number(t.participants ?? 0),
            participants_ecosystem: Number(t.participants_ecosystem ?? 0),
            participants_issuer_grantor: Number(t.participants_issuer_grantor ?? 0),
            participants_issuer: Number(t.participants_issuer ?? 0),
            participants_verifier_grantor: Number(t.participants_verifier_grantor ?? 0),
            participants_verifier: Number(t.participants_verifier ?? 0),
            participants_holder: Number(t.participants_holder ?? 0),
            active_schemas: Number(t.active_schemas ?? 0),
            archived_schemas: Number(t.archived_schemas ?? 0),
            weight: Number(t.weight ?? 0),
            issued: Number(t.issued ?? 0),
            verified: Number(t.verified ?? 0),
            ecosystem_slash_events: Number(t.ecosystem_slash_events ?? 0),
            ecosystem_slashed_amount: Number(t.ecosystem_slashed_amount ?? 0),
            ecosystem_slashed_amount_repaid: Number(t.ecosystem_slashed_amount_repaid ?? 0),
            network_slash_events: Number(t.network_slash_events ?? 0),
            network_slashed_amount: Number(t.network_slashed_amount ?? 0),
            network_slashed_amount_repaid: Number(t.network_slashed_amount_repaid ?? 0),
          } as Record<string, unknown>),
        }
        const enrichedResponsePayload =
          trustDataMode === 'none'
            ? responsePayload
            : await enrichTrustDataDeep(responsePayload, trustDataMode, blockHeight)
        return ApiResponder.success(ctx, enrichedResponsePayload, 200)
      }

      if (typeof blockHeight === 'number') {
        const hasSnapshotTable = await knex.schema.hasTable('ecosystem_snapshot')
        if (hasSnapshotTable) {
          const snapshot = await knex('ecosystem_snapshot')
            .where({ ecosystem_id: Number(ecosystemId) })
            .where('height', '<=', blockHeight)
            .orderBy('height', 'desc')
            .orderBy('id', 'desc')
            .first()
          if (snapshot) {
            let versions = Array.isArray((snapshot as any).versions_snapshot) ? (snapshot as any).versions_snapshot : []
            if (activeGfOnly) {
              versions = [...versions]
                .sort((a: any, b: any) => {
                  if (!a?.active_since && !b?.active_since) return 0
                  if (!a?.active_since) return 1
                  if (!b?.active_since) return -1
                  return new Date(b.active_since).getTime() - new Date(a.active_since).getTime()
                })
                .slice(0, 1)
            }
            if (preferredLanguage) {
              versions = versions.map((v: any) => ({
                ...v,
                documents: (v.documents || []).filter((d: any) => d.language === preferredLanguage),
              }))
            }
            const s = snapshot as any
            const responsePayload = {
              ecosystem: mapEcosystemApiFields({
                id: s.ecosystem_id,
                did: s.did,
                corporation_id: s.corporation_id,
                created: s.created,
                modified: s.modified,
                archived: s.archived,
                aka: s.aka,
                language: s.language,
                active_version: s.active_version,
                versions,
                participants: Number(s.participants ?? 0),
                participants_ecosystem: Number(s.participants_ecosystem ?? 0),
                participants_issuer_grantor: Number(s.participants_issuer_grantor ?? 0),
                participants_issuer: Number(s.participants_issuer ?? 0),
                participants_verifier_grantor: Number(s.participants_verifier_grantor ?? 0),
                participants_verifier: Number(s.participants_verifier ?? 0),
                participants_holder: Number(s.participants_holder ?? 0),
                active_schemas: Number(s.active_schemas ?? 0),
                archived_schemas: Number(s.archived_schemas ?? 0),
                weight: Number(s.weight ?? 0),
                issued: Number(s.issued ?? 0),
                verified: Number(s.verified ?? 0),
                ecosystem_slash_events: Number(s.ecosystem_slash_events ?? 0),
                ecosystem_slashed_amount: Number(s.ecosystem_slashed_amount ?? 0),
                ecosystem_slashed_amount_repaid: Number(s.ecosystem_slashed_amount_repaid ?? 0),
                network_slash_events: Number(s.network_slash_events ?? 0),
                network_slashed_amount: Number(s.network_slashed_amount ?? 0),
                network_slashed_amount_repaid: Number(s.network_slashed_amount_repaid ?? 0),
              } as Record<string, unknown>),
            }
            const enrichedResponsePayload =
              trustDataMode === 'none'
                ? responsePayload
                : await enrichTrustDataDeep(responsePayload, trustDataMode, blockHeight)
            return ApiResponder.success(ctx, enrichedResponsePayload, 200)
          }
        }

        const ecosystemHistory = await knex('ecosystem_history')
          .where({ ecosystem_id: ecosystemId })
          .where('height', '<=', blockHeight)
          .orderBy('height', 'desc')
          .orderBy('created_at', 'desc')
          .first()

        if (!ecosystemHistory) {
          return ApiResponder.error(ctx, `Ecosystem with id ${ecosystemId} not found`, 404)
        }

        const versionsByEcosystemId = await this.buildHistoricalVersionsByEcosystemIds(
          [Number(ecosystemId)],
          blockHeight,
          activeGfOnly,
          preferredLanguage
        )
        const filteredVersions = versionsByEcosystemId.get(Number(ecosystemId)) || []

        const ecosystem = {
          id: ecosystemHistory.ecosystem_id,
          did: ecosystemHistory.did,
          corporation_id: ecosystemHistory.corporation_id,
          created: ecosystemHistory.created,
          modified: ecosystemHistory.modified,
          archived: ecosystemHistory.archived,
          aka: ecosystemHistory.aka,
          language: ecosystemHistory.language,
          active_version: ecosystemHistory.active_version,
          versions: filteredVersions,
          participants: Number(ecosystemHistory.participants ?? 0),
          participants_ecosystem: Number((ecosystemHistory as any).participants_ecosystem ?? 0),
          participants_issuer_grantor: Number((ecosystemHistory as any).participants_issuer_grantor ?? 0),
          participants_issuer: Number((ecosystemHistory as any).participants_issuer ?? 0),
          participants_verifier_grantor: Number((ecosystemHistory as any).participants_verifier_grantor ?? 0),
          participants_verifier: Number((ecosystemHistory as any).participants_verifier ?? 0),
          participants_holder: Number((ecosystemHistory as any).participants_holder ?? 0),
          active_schemas: Number((ecosystemHistory as any).active_schemas ?? 0),
          archived_schemas: Number((ecosystemHistory as any).archived_schemas ?? 0),
          weight: Number((ecosystemHistory as any).weight ?? 0),
          issued: Number((ecosystemHistory as any).issued ?? 0),
          verified: Number((ecosystemHistory as any).verified ?? 0),
          ecosystem_slash_events: Number((ecosystemHistory as any).ecosystem_slash_events ?? 0),
          ecosystem_slashed_amount: Number((ecosystemHistory as any).ecosystem_slashed_amount ?? 0),
          ecosystem_slashed_amount_repaid: Number((ecosystemHistory as any).ecosystem_slashed_amount_repaid ?? 0),
          network_slash_events: Number((ecosystemHistory as any).network_slash_events ?? 0),
          network_slashed_amount: Number((ecosystemHistory as any).network_slashed_amount ?? 0),
          network_slashed_amount_repaid: Number((ecosystemHistory as any).network_slashed_amount_repaid ?? 0),
        }

        const responsePayload = { ecosystem: mapEcosystemApiFields(ecosystem as Record<string, unknown>) }
        const enrichedResponsePayload =
          trustDataMode === 'none'
            ? responsePayload
            : await enrichTrustDataDeep(responsePayload, trustDataMode, blockHeight)
        return ApiResponder.success(ctx, enrichedResponsePayload, 200)
      }

      const registry = await Ecosystem.query()
        .findById(ecosystemId)
        .withGraphFetched('governanceFrameworkVersions.documents')

      if (!registry) {
        return ApiResponder.error(ctx, `Ecosystem with id ${ecosystemId} not found`, 404)
      }

      const plain = registry.toJSON()
      let versions = plain.governanceFrameworkVersions ?? []

      if (activeGfOnly) {
        versions = versions
          .sort((a, b) => {
            if (!a.active_since && !b.active_since) return 0
            if (!a.active_since) return 1
            if (!b.active_since) return -1
            return new Date(b.active_since).getTime() - new Date(a.active_since).getTime()
          })
          .slice(0, 1)
      }

      if (preferredLanguage) {
        for (const v of versions) {
          v.documents = v.documents?.filter((d) => d.language === preferredLanguage) ?? []
        }
      }
      delete plain.governanceFrameworkVersions
      delete (plain as any).height

      const p = plain as any
      const responsePayload = {
        ecosystem: mapEcosystemApiFields({
          ...plain,
          id: plain.id,
          versions,
          participants: Number(p.participants ?? 0),
          participants_ecosystem: Number(p.participants_ecosystem ?? 0),
          participants_issuer_grantor: Number(p.participants_issuer_grantor ?? 0),
          participants_issuer: Number(p.participants_issuer ?? 0),
          participants_verifier_grantor: Number(p.participants_verifier_grantor ?? 0),
          participants_verifier: Number(p.participants_verifier ?? 0),
          participants_holder: Number(p.participants_holder ?? 0),
          active_schemas: Number(p.active_schemas ?? 0),
          archived_schemas: Number(p.archived_schemas ?? 0),
          weight: Number(p.weight ?? 0),
          issued: Number(p.issued ?? 0),
          verified: Number(p.verified ?? 0),
          ecosystem_slash_events: Number(p.ecosystem_slash_events ?? 0),
          ecosystem_slashed_amount: Number(p.ecosystem_slashed_amount ?? 0),
          ecosystem_slashed_amount_repaid: Number(p.ecosystem_slashed_amount_repaid ?? 0),
          network_slash_events: Number(p.network_slash_events ?? 0),
          network_slashed_amount: Number(p.network_slashed_amount ?? 0),
          network_slashed_amount_repaid: Number(p.network_slashed_amount_repaid ?? 0),
        } as Record<string, unknown>),
      }
      const enrichedResponsePayload =
        trustDataMode === 'none'
          ? responsePayload
          : await enrichTrustDataDeep(responsePayload, trustDataMode, blockHeight)
      return ApiResponder.success(ctx, enrichedResponsePayload, 200)
    } catch (err: any) {
      return ApiResponder.error(ctx, err.message, 500)
    }
  }

  @Action({
    params: {
      corporation: { type: 'any', optional: true },
      participant: { type: 'any', optional: true },
      modified_after: { type: 'string', optional: true },
      only_active: { type: 'any', optional: true },
      active_gf_only: { type: 'any', optional: true },
      preferred_language: { type: 'string', optional: true },
      response_max_size: { type: 'number', optional: true, default: 64 },
      sort: { type: 'string', optional: true },
      min_active_schemas: { type: 'number', optional: true },
      max_active_schemas: { type: 'number', optional: true },
      min_participants: { type: 'number', optional: true },
      max_participants: { type: 'number', optional: true },
      min_participants_ecosystem: { type: 'number', optional: true },
      max_participants_ecosystem: { type: 'number', optional: true },
      min_participants_issuer_grantor: { type: 'number', optional: true },
      max_participants_issuer_grantor: { type: 'number', optional: true },
      min_participants_issuer: { type: 'number', optional: true },
      max_participants_issuer: { type: 'number', optional: true },
      min_participants_verifier_grantor: { type: 'number', optional: true },
      max_participants_verifier_grantor: { type: 'number', optional: true },
      min_participants_verifier: { type: 'number', optional: true },
      max_participants_verifier: { type: 'number', optional: true },
      min_participants_holder: { type: 'number', optional: true },
      max_participants_holder: { type: 'number', optional: true },
      min_weight: { type: 'string', optional: true },
      max_weight: { type: 'string', optional: true },
      min_issued: { type: 'string', optional: true },
      max_issued: { type: 'string', optional: true },
      min_verified: { type: 'string', optional: true },
      max_verified: { type: 'string', optional: true },
      min_ecosystem_slash_events: { type: 'number', optional: true },
      max_ecosystem_slash_events: { type: 'number', optional: true },
      min_network_slash_events: { type: 'number', optional: true },
      max_network_slash_events: { type: 'number', optional: true },
      trust_data: { type: 'string', optional: true },
    },
  })
  public async listEcosystems(
    ctx: Context<{
      corporation?: string
      participant?: string
      modified_after?: string
      only_active?: string | boolean
      active_gf_only?: string | boolean
      preferred_language?: string
      response_max_size?: number
      sort?: string
      min_active_schemas?: number
      max_active_schemas?: number
      min_participants?: number
      max_participants?: number
      min_participants_ecosystem?: number
      max_participants_ecosystem?: number
      min_participants_issuer_grantor?: number
      max_participants_issuer_grantor?: number
      min_participants_issuer?: number
      max_participants_issuer?: number
      min_participants_verifier_grantor?: number
      max_participants_verifier_grantor?: number
      min_participants_verifier?: number
      max_participants_verifier?: number
      min_participants_holder?: number
      max_participants_holder?: number
      min_weight?: string
      max_weight?: string
      min_issued?: string
      max_issued?: string
      min_verified?: string
      max_verified?: string
      min_ecosystem_slash_events?: number
      max_ecosystem_slash_events?: number
      min_network_slash_events?: number
      max_network_slash_events?: number
      trust_data?: string
    }>
  ) {
    try {
      const {
        corporation,
        participant,
        modified_after: modifiedAfter,
        preferred_language: preferredLanguage,
        only_active: onlyActiveRaw,
        response_max_size: responseMaxSizeRaw,
        sort,
        min_active_schemas: minActiveSchemas,
        max_active_schemas: maxActiveSchemas,
        min_participants: minParticipants,
        max_participants: maxParticipants,
        min_participants_ecosystem: minParticipantsEcosystem,
        max_participants_ecosystem: maxParticipantsEcosystem,
        min_participants_issuer_grantor: minParticipantsIssuerGrantor,
        max_participants_issuer_grantor: maxParticipantsIssuerGrantor,
        min_participants_issuer: minParticipantsIssuer,
        max_participants_issuer: maxParticipantsIssuer,
        min_participants_verifier_grantor: minParticipantsVerifierGrantor,
        max_participants_verifier_grantor: maxParticipantsVerifierGrantor,
        min_participants_verifier: minParticipantsVerifier,
        max_participants_verifier: maxParticipantsVerifier,
        min_participants_holder: minParticipantsHolder,
        max_participants_holder: maxParticipantsHolder,
        min_weight: minWeight,
        max_weight: maxWeight,
        min_issued: minIssued,
        max_issued: maxIssued,
        min_verified: minVerified,
        max_verified: maxVerified,
        min_ecosystem_slash_events: minEcosystemSlashEvents,
        max_ecosystem_slash_events: maxEcosystemSlashEvents,
        min_network_slash_events: minNetworkSlashEvents,
        max_network_slash_events: maxNetworkSlashEvents,
        trust_data: trustDataSnake,
      } = ctx.params
      const trustDataModeParsed = parseTrustDataMode(trustDataSnake)
      if (!trustDataModeParsed.ok) {
        return ApiResponder.error(ctx, trustDataModeParsed.message, 400)
      }
      const trustDataMode = trustDataModeParsed.mode

      const participantValidation = validateParticipantParam(participant, 'participant')
      if (!participantValidation.valid) {
        return ApiResponder.error(ctx, participantValidation.error, 400)
      }
      const participantAccount = participantValidation.value

      const corporationValidation = validateParticipantParam(corporation, 'corporation')
      if (!corporationValidation.valid) {
        return ApiResponder.error(ctx, corporationValidation.error, 400)
      }
      const corporationAccount = corporationValidation.value
      let corporationId: number | null = null
      if (corporationAccount) {
        corporationId = await resolveCorporationIdByAddress(corporationAccount)
        if (corporationId === null) {
          return ApiResponder.success(ctx, { ecosystems: [] }, 200)
        }
      }

      try {
        validateSortParameter(sort)
      } catch (err: any) {
        return ApiResponder.error(ctx, err.message, 400)
      }

      const hasOnlyActive = typeof onlyActiveRaw !== 'undefined'
      const onlyActive = String(onlyActiveRaw).toLowerCase() === 'true'
      const activeGfOnly = String(ctx.params.active_gf_only).toLowerCase() === 'true'
      const blockHeight = (ctx.meta as any)?.blockHeight

      const responseMaxSize = !responseMaxSizeRaw ? 64 : Math.min(Math.max(responseMaxSizeRaw, 1), 1024)

      if (responseMaxSizeRaw && (responseMaxSizeRaw < 1 || responseMaxSizeRaw > 1024)) {
        return ApiResponder.error(ctx, 'response_max_size must be between 1 and 1024', 400)
      }

      const metricFilters = {
        minActiveSchemas,
        maxActiveSchemas,
        minParticipants,
        maxParticipants,
        minParticipantsEcosystem,
        maxParticipantsEcosystem,
        minParticipantsIssuerGrantor,
        maxParticipantsIssuerGrantor,
        minParticipantsIssuer,
        maxParticipantsIssuer,
        minParticipantsVerifierGrantor,
        maxParticipantsVerifierGrantor,
        minParticipantsVerifier,
        maxParticipantsVerifier,
        minParticipantsHolder,
        maxParticipantsHolder,
        minWeight,
        maxWeight,
        minIssued,
        maxIssued,
        minVerified,
        maxVerified,
        minEcosystemSlashEvents,
        maxEcosystemSlashEvents,
        minNetworkSlashEvents,
        maxNetworkSlashEvents,
      }

      if (this.hasImpossibleMetricRanges(metricFilters)) {
        return ApiResponder.success(ctx, { ecosystems: [] }, 200)
      }

      if (typeof blockHeight === 'number') {
        const useHeightSyncList = process.env.NODE_ENV !== 'test' && process.env.USE_HEIGHT_SYNC_TR === 'true'
        const hasSnapshotTable = await knex.schema.hasTable('ecosystem_snapshot')

        if (useHeightSyncList && hasSnapshotTable) {
          let participantEcosystemIds: number[] | undefined
          if (participantAccount) {
            participantEcosystemIds = await this.getEcosystemIdsForParticipantAtHeight(participantAccount, blockHeight)
            if (participantEcosystemIds.length === 0) {
              return ApiResponder.success(ctx, { ecosystems: [] }, 200)
            }
          }
          let snapshotBase = knex('ecosystem_snapshot').where('height', '<=', blockHeight)
          if (corporationId !== null) snapshotBase = snapshotBase.where('corporation_id', corporationId)
          if (participantEcosystemIds !== undefined)
            snapshotBase = snapshotBase.whereIn('ecosystem_id', participantEcosystemIds)
          if (modifiedAfter) {
            const ts = new Date(modifiedAfter)
            if (!Number.isNaN(ts.getTime())) snapshotBase = snapshotBase.where('modified', '>', ts.toISOString())
          }
          if (hasOnlyActive) {
            if (onlyActive) snapshotBase = snapshotBase.whereNull('archived')
            else snapshotBase = snapshotBase.whereNotNull('archived')
          }
          const ranked = knex
            .select('*')
            .select(knex.raw('ROW_NUMBER() OVER (PARTITION BY ecosystem_id ORDER BY height DESC, id DESC) as rn'))
            .from(snapshotBase.as('snap'))
          const snapshotListQuery = knex.from(ranked.as('r')).where('rn', 1)
          this.applyEcosystemSqlSort(snapshotListQuery as any, sort)
          const latestSnapshots = await snapshotListQuery.limit(Math.max(responseMaxSize * 2, 256))
          const registriesWithStats = (latestSnapshots as any[]).map((row: any) => {
            let versions = Array.isArray(row.versions_snapshot) ? row.versions_snapshot : []
            if (activeGfOnly) {
              versions = [...versions]
                .sort((a: any, b: any) => {
                  if (!a?.active_since && !b?.active_since) return 0
                  if (!a?.active_since) return 1
                  if (!b?.active_since) return -1
                  return new Date(b.active_since).getTime() - new Date(a.active_since).getTime()
                })
                .slice(0, 1)
            }
            if (preferredLanguage) {
              versions = versions.map((v: any) => ({
                ...v,
                documents: (v.documents || []).filter((d: any) => d.language === preferredLanguage),
              }))
            }
            return {
              id: row.ecosystem_id,
              did: row.did,
              corporation_id: row.corporation_id,
              created: row.created,
              modified: row.modified,
              archived: row.archived,
              aka: row.aka,
              language: row.language,
              active_version: row.active_version,
              versions,
              participants: Number(row.participants ?? 0),
              participants_ecosystem: Number(row.participants_ecosystem ?? 0),
              participants_issuer_grantor: Number(row.participants_issuer_grantor ?? 0),
              participants_issuer: Number(row.participants_issuer ?? 0),
              participants_verifier_grantor: Number(row.participants_verifier_grantor ?? 0),
              participants_verifier: Number(row.participants_verifier ?? 0),
              participants_holder: Number(row.participants_holder ?? 0),
              active_schemas: Number(row.active_schemas ?? 0),
              archived_schemas: Number(row.archived_schemas ?? 0),
              weight: Number(row.weight ?? 0),
              issued: Number(row.issued ?? 0),
              verified: Number(row.verified ?? 0),
              ecosystem_slash_events: Number(row.ecosystem_slash_events ?? 0),
              ecosystem_slashed_amount: Number(row.ecosystem_slashed_amount ?? 0),
              ecosystem_slashed_amount_repaid: Number(row.ecosystem_slashed_amount_repaid ?? 0),
              network_slash_events: Number(row.network_slash_events ?? 0),
              network_slashed_amount: Number(row.network_slashed_amount ?? 0),
              network_slashed_amount_repaid: Number(row.network_slashed_amount_repaid ?? 0),
            }
          })
          const filteredRegistries = this.applyMetricFiltersToRegistries(registriesWithStats, metricFilters)
          const sortedRegistries = this.sortRegistries(filteredRegistries, sort, responseMaxSize)
          const responsePayload = {
            ecosystems: sortedRegistries.map((r) => mapEcosystemApiFields(r as Record<string, unknown>)),
          }
          const enrichedResponsePayload =
            trustDataMode === 'none'
              ? responsePayload
              : await enrichTrustDataDeep(responsePayload, trustDataMode, blockHeight)
          return ApiResponder.success(ctx, enrichedResponsePayload, 200)
        }

        let participantEcosystemIds: number[] | undefined
        if (participantAccount) {
          participantEcosystemIds = await this.getEcosystemIdsForParticipantAtHeight(participantAccount, blockHeight)
          if (participantEcosystemIds.length === 0) {
            return ApiResponder.success(ctx, { ecosystems: [] }, 200)
          }
        }

        let filteredSubquery = knex('ecosystem_history').select('*').where('height', '<=', blockHeight)

        if (corporationId !== null) {
          filteredSubquery = filteredSubquery.where('corporation_id', corporationId)
        }
        if (participantEcosystemIds !== undefined) {
          filteredSubquery = filteredSubquery.whereIn('ecosystem_id', participantEcosystemIds)
        }
        if (modifiedAfter) {
          const ts = new Date(modifiedAfter)
          if (!Number.isNaN(ts.getTime())) {
            filteredSubquery = filteredSubquery.where('modified', '>', ts.toISOString())
          }
        }
        if (hasOnlyActive) {
          if (onlyActive) {
            filteredSubquery = filteredSubquery.whereNull('archived')
          } else {
            filteredSubquery = filteredSubquery.whereNotNull('archived')
          }
        }

        const rankedSubquery = knex
          .select('*')
          .select(knex.raw('ROW_NUMBER() OVER (PARTITION BY ecosystem_id ORDER BY height DESC, created_at DESC) as rn'))
          .from(filteredSubquery.as('filtered'))
          .as('ranked')

        const latestEcosystemHistoryQuery = knex.from(rankedSubquery).where('rn', 1)

        const latestEcosystemHistory = (await applyOrdering(latestEcosystemHistoryQuery as any, sort).limit(
          Math.max(responseMaxSize * 2, 256)
        )) as any[]
        const hasDerivedSort = this.usesDerivedMetricSort(sort)
        const sortedHistory = hasDerivedSort
          ? latestEcosystemHistory.slice(0, Math.max(responseMaxSize * 2, 256))
          : latestEcosystemHistory.slice(0, responseMaxSize)

        if (sortedHistory.length === 0) {
          return ApiResponder.success(ctx, { ecosystems: [] }, 200)
        }
        const versionsByEcosystemId = await this.buildHistoricalVersionsByEcosystemIds(
          sortedHistory.map((row: any) => Number(row.ecosystem_id)),
          blockHeight,
          activeGfOnly,
          preferredLanguage
        )

        const registriesWithStats = await Promise.all(
          sortedHistory.map(async (ecosystemHistory: any) => {
            const ecosystemId = ecosystemHistory.ecosystem_id
            const filteredVersions = versionsByEcosystemId.get(Number(ecosystemId)) || []

            return {
              id: ecosystemHistory.ecosystem_id,
              did: ecosystemHistory.did,
              corporation_id: ecosystemHistory.corporation_id,
              created: ecosystemHistory.created,
              modified: ecosystemHistory.modified,
              archived: ecosystemHistory.archived,
              aka: ecosystemHistory.aka,
              language: ecosystemHistory.language,
              active_version: ecosystemHistory.active_version,
              versions: filteredVersions,
              participants: Number(ecosystemHistory.participants ?? 0),
              participants_ecosystem: Number((ecosystemHistory as any).participants_ecosystem ?? 0),
              participants_issuer_grantor: Number((ecosystemHistory as any).participants_issuer_grantor ?? 0),
              participants_issuer: Number((ecosystemHistory as any).participants_issuer ?? 0),
              participants_verifier_grantor: Number((ecosystemHistory as any).participants_verifier_grantor ?? 0),
              participants_verifier: Number((ecosystemHistory as any).participants_verifier ?? 0),
              participants_holder: Number((ecosystemHistory as any).participants_holder ?? 0),
              active_schemas: Number((ecosystemHistory as any).active_schemas ?? 0),
              archived_schemas: Number((ecosystemHistory as any).archived_schemas ?? 0),
              weight: Number((ecosystemHistory as any).weight ?? 0),
              issued: Number((ecosystemHistory as any).issued ?? 0),
              verified: Number((ecosystemHistory as any).verified ?? 0),
              ecosystem_slash_events: Number((ecosystemHistory as any).ecosystem_slash_events ?? 0),
              ecosystem_slashed_amount: Number((ecosystemHistory as any).ecosystem_slashed_amount ?? 0),
              ecosystem_slashed_amount_repaid: Number((ecosystemHistory as any).ecosystem_slashed_amount_repaid ?? 0),
              network_slash_events: Number((ecosystemHistory as any).network_slash_events ?? 0),
              network_slashed_amount: Number((ecosystemHistory as any).network_slashed_amount ?? 0),
              network_slashed_amount_repaid: Number((ecosystemHistory as any).network_slashed_amount_repaid ?? 0),
            }
          })
        )

        const filteredRegistries = this.applyMetricFiltersToRegistries(
          registriesWithStats.filter((r): r is NonNullable<(typeof registriesWithStats)[0]> => r !== null),
          metricFilters
        )

        const sortedRegistries = this.sortRegistries(filteredRegistries, sort, responseMaxSize)

        const responsePayload = {
          ecosystems: sortedRegistries.map((r) => mapEcosystemApiFields(r as Record<string, unknown>)),
        }
        const enrichedResponsePayload =
          trustDataMode === 'none'
            ? responsePayload
            : await enrichTrustDataDeep(responsePayload, trustDataMode, blockHeight)
        return ApiResponder.success(ctx, enrichedResponsePayload, 200)
      }

      const useHeightSyncListLive = process.env.NODE_ENV !== 'test' && process.env.USE_HEIGHT_SYNC_TR === 'true'
      const hasVersionTable = await knex.schema.hasTable('ecosystem_version')
      const hasDocumentTable = await knex.schema.hasTable('ecosystem_document')

      if (useHeightSyncListLive && hasVersionTable && hasDocumentTable) {
        let batchQuery = knex('ecosystem')
        if (participantAccount) {
          const participantEcosystemIds = await this.getEcosystemIdsForParticipant(participantAccount)
          if (participantEcosystemIds.length === 0) {
            return ApiResponder.success(ctx, { ecosystems: [] }, 200)
          }
          batchQuery = batchQuery.whereIn('id', participantEcosystemIds)
        }
        if (corporationId !== null) batchQuery = batchQuery.where('corporation_id', corporationId)
        batchQuery = this.applyRangeToQuery(batchQuery, 'participants', minParticipants, maxParticipants)
        batchQuery = this.applyRangeToQuery(
          batchQuery,
          'participants_ecosystem',
          minParticipantsEcosystem,
          maxParticipantsEcosystem
        )
        batchQuery = this.applyRangeToQuery(
          batchQuery,
          'participants_issuer_grantor',
          minParticipantsIssuerGrantor,
          maxParticipantsIssuerGrantor
        )
        batchQuery = this.applyRangeToQuery(
          batchQuery,
          'participants_issuer',
          minParticipantsIssuer,
          maxParticipantsIssuer
        )
        batchQuery = this.applyRangeToQuery(
          batchQuery,
          'participants_verifier_grantor',
          minParticipantsVerifierGrantor,
          maxParticipantsVerifierGrantor
        )
        batchQuery = this.applyRangeToQuery(
          batchQuery,
          'participants_verifier',
          minParticipantsVerifier,
          maxParticipantsVerifier
        )
        batchQuery = this.applyRangeToQuery(
          batchQuery,
          'participants_holder',
          minParticipantsHolder,
          maxParticipantsHolder
        )
        batchQuery = this.applyRangeToQuery(batchQuery, 'active_schemas', minActiveSchemas, maxActiveSchemas)
        batchQuery = this.applyRangeToQuery(
          batchQuery,
          'weight',
          minWeight !== undefined ? Number(minWeight) : undefined,
          maxWeight !== undefined ? Number(maxWeight) : undefined
        )
        batchQuery = this.applyRangeToQuery(
          batchQuery,
          'issued',
          minIssued !== undefined ? Number(minIssued) : undefined,
          maxIssued !== undefined ? Number(maxIssued) : undefined
        )
        batchQuery = this.applyRangeToQuery(
          batchQuery,
          'verified',
          minVerified !== undefined ? Number(minVerified) : undefined,
          maxVerified !== undefined ? Number(maxVerified) : undefined
        )
        batchQuery = this.applyRangeToQuery(
          batchQuery,
          'ecosystem_slash_events',
          minEcosystemSlashEvents,
          maxEcosystemSlashEvents
        )
        batchQuery = this.applyRangeToQuery(
          batchQuery,
          'network_slash_events',
          minNetworkSlashEvents,
          maxNetworkSlashEvents
        )
        if (modifiedAfter) batchQuery = batchQuery.where('modified', '>', modifiedAfter)
        if (hasOnlyActive) {
          if (onlyActive) batchQuery = batchQuery.whereNull('archived')
          else batchQuery = batchQuery.whereNotNull('archived')
        }
        const { fullyApplied: batchSortFullyApplied } = this.applyEcosystemSqlSort(batchQuery as any, sort)
        const batchLimit = batchSortFullyApplied ? responseMaxSize : Math.max(responseMaxSize * 2, 256)
        const ecosystemRows = (await batchQuery.limit(batchLimit)) as any[]
        if (ecosystemRows.length === 0) {
          return ApiResponder.success(ctx, { ecosystems: [] }, 200)
        }
        const ecosystemIds = ecosystemRows.map((r: any) => r.id)
        const versionsRows = (await knex('ecosystem_version')
          .whereIn('ecosystem_id', ecosystemIds)
          .orderBy('version', 'asc')) as any[]
        const versionIds = versionsRows.map((r: any) => r.id)
        const documentsRows = versionIds.length
          ? ((await knex('ecosystem_document').whereIn('version_id', versionIds)) as any[])
          : []
        const docByVersion = new Map<number, any[]>()
        for (const d of documentsRows) {
          const vid = Number(d.version_id)
          let docList = docByVersion.get(vid)
          if (!docList) {
            docList = []
            docByVersion.set(vid, docList)
          }
          docList.push({
            id: d.id,
            gfv_id: d.version_id,
            created: d.created,
            language: d.language,
            url: d.url,
            digest_sri: d.digest_sri,
          })
        }
        const versionsByEcosystemId = new Map<number, any[]>()
        for (const v of versionsRows) {
          const tid = Number(v.ecosystem_id)
          let versionList = versionsByEcosystemId.get(tid)
          if (!versionList) {
            versionList = []
            versionsByEcosystemId.set(tid, versionList)
          }
          versionList.push({
            id: v.id,
            ecosystem_id: v.ecosystem_id,
            created: v.created,
            version: v.version,
            active_since: v.active_since,
            documents: docByVersion.get(Number(v.id)) || [],
          })
        }
        const batchRegistries = ecosystemRows.map((ec: any) => {
          let versions = versionsByEcosystemId.get(Number(ec.id)) || []
          if (activeGfOnly) {
            versions = [...versions]
              .sort((a: any, b: any) => {
                if (!a.active_since && !b.active_since) return 0
                if (!a.active_since) return 1
                if (!b.active_since) return -1
                return new Date(b.active_since).getTime() - new Date(a.active_since).getTime()
              })
              .slice(0, 1)
          }
          if (preferredLanguage) {
            versions = versions.map((v: any) => ({
              ...v,
              documents: (v.documents || []).filter((d: any) => d.language === preferredLanguage),
            }))
          }
          return {
            id: ec.id,
            did: ec.did,
            corporation_id: ec.corporation_id,
            created: ec.created,
            modified: ec.modified,
            archived: ec.archived,
            aka: ec.aka,
            language: ec.language,
            active_version: ec.active_version,
            versions,
            participants: Number(ec.participants ?? 0),
            participants_ecosystem: Number(ec.participants_ecosystem ?? 0),
            participants_issuer_grantor: Number(ec.participants_issuer_grantor ?? 0),
            participants_issuer: Number(ec.participants_issuer ?? 0),
            participants_verifier_grantor: Number(ec.participants_verifier_grantor ?? 0),
            participants_verifier: Number(ec.participants_verifier ?? 0),
            participants_holder: Number(ec.participants_holder ?? 0),
            active_schemas: Number(ec.active_schemas ?? 0),
            archived_schemas: Number(ec.archived_schemas ?? 0),
            weight: Number(ec.weight ?? 0),
            issued: Number(ec.issued ?? 0),
            verified: Number(ec.verified ?? 0),
            ecosystem_slash_events: Number(ec.ecosystem_slash_events ?? 0),
            ecosystem_slashed_amount: Number(ec.ecosystem_slashed_amount ?? 0),
            ecosystem_slashed_amount_repaid: Number(ec.ecosystem_slashed_amount_repaid ?? 0),
            network_slash_events: Number(ec.network_slash_events ?? 0),
            network_slashed_amount: Number(ec.network_slashed_amount ?? 0),
            network_slashed_amount_repaid: Number(ec.network_slashed_amount_repaid ?? 0),
          }
        })
        const filteredBatch = this.applyMetricFiltersToRegistries(batchRegistries, metricFilters)
        const sortedBatch = this.sortRegistries(filteredBatch, sort, responseMaxSize)
        const responsePayload = {
          ecosystems: sortedBatch.map((r) => mapEcosystemApiFields(r as Record<string, unknown>)),
        }
        const enrichedResponsePayload =
          trustDataMode === 'none'
            ? responsePayload
            : await enrichTrustDataDeep(responsePayload, trustDataMode, blockHeight)
        return ApiResponder.success(ctx, enrichedResponsePayload, 200)
      }

      let query = Ecosystem.query()

      if (participantAccount) {
        const participantEcosystemIds = await this.getEcosystemIdsForParticipant(participantAccount)
        if (participantEcosystemIds.length === 0) {
          return ApiResponder.success(ctx, { ecosystems: [] }, 200)
        }
        query = query.where('id', 'in', participantEcosystemIds) as any
      }
      if (corporationId !== null) {
        query = query.where('corporation_id', corporationId)
      }

      query = query.withGraphFetched('governanceFrameworkVersions.documents') as any
      query = this.applyRangeToQuery(query, 'participants', minParticipants, maxParticipants)
      query = this.applyRangeToQuery(
        query,
        'participants_ecosystem',
        minParticipantsEcosystem,
        maxParticipantsEcosystem
      )
      query = this.applyRangeToQuery(
        query,
        'participants_issuer_grantor',
        minParticipantsIssuerGrantor,
        maxParticipantsIssuerGrantor
      )
      query = this.applyRangeToQuery(query, 'participants_issuer', minParticipantsIssuer, maxParticipantsIssuer)
      query = this.applyRangeToQuery(
        query,
        'participants_verifier_grantor',
        minParticipantsVerifierGrantor,
        maxParticipantsVerifierGrantor
      )
      query = this.applyRangeToQuery(query, 'participants_verifier', minParticipantsVerifier, maxParticipantsVerifier)
      query = this.applyRangeToQuery(query, 'participants_holder', minParticipantsHolder, maxParticipantsHolder)
      query = this.applyRangeToQuery(query, 'active_schemas', minActiveSchemas, maxActiveSchemas)
      query = this.applyRangeToQuery(
        query,
        'weight',
        minWeight !== undefined ? Number(minWeight) : undefined,
        maxWeight !== undefined ? Number(maxWeight) : undefined
      )
      query = this.applyRangeToQuery(
        query,
        'issued',
        minIssued !== undefined ? Number(minIssued) : undefined,
        maxIssued !== undefined ? Number(maxIssued) : undefined
      )
      query = this.applyRangeToQuery(
        query,
        'verified',
        minVerified !== undefined ? Number(minVerified) : undefined,
        maxVerified !== undefined ? Number(maxVerified) : undefined
      )
      query = this.applyRangeToQuery(query, 'ecosystem_slash_events', minEcosystemSlashEvents, maxEcosystemSlashEvents)
      query = this.applyRangeToQuery(query, 'network_slash_events', minNetworkSlashEvents, maxNetworkSlashEvents)

      if (modifiedAfter) {
        query = query.where('modified', '>', modifiedAfter)
      }

      if (hasOnlyActive) {
        if (onlyActive) {
          query = query.whereNull('archived')
        } else {
          query = query.whereNotNull('archived')
        }
      }

      const { fullyApplied: liveSortFullyApplied } = this.applyEcosystemSqlSort(query as any, sort)
      const liveFetchLimit = liveSortFullyApplied ? responseMaxSize : Math.max(responseMaxSize * 2, 256)
      const registries = await query.limit(liveFetchLimit)

      const registriesWithStats = registries.map((ec) => {
        const plain = ec.toJSON()
        let versions = plain.governanceFrameworkVersions ?? []

        if (activeGfOnly) {
          versions = versions
            .sort((a, b) => {
              if (!a.active_since && !b.active_since) return 0
              if (!a.active_since) return 1
              if (!b.active_since) return -1
              return new Date(b.active_since).getTime() - new Date(a.active_since).getTime()
            })
            .slice(0, 1)
        }

        if (preferredLanguage) {
          for (const v of versions) {
            v.documents = v.documents?.filter((d) => d.language === preferredLanguage) ?? []
          }
        }

        delete plain.governanceFrameworkVersions
        delete (plain as any).height

        return {
          ...plain,
          versions,
          participants: Number(plain.participants || 0),
          participants_ecosystem: Number((plain as any).participants_ecosystem || 0),
          participants_issuer_grantor: Number((plain as any).participants_issuer_grantor || 0),
          participants_issuer: Number((plain as any).participants_issuer || 0),
          participants_verifier_grantor: Number((plain as any).participants_verifier_grantor || 0),
          participants_verifier: Number((plain as any).participants_verifier || 0),
          participants_holder: Number((plain as any).participants_holder || 0),
          active_schemas: Number(plain.active_schemas || 0),
          archived_schemas: Number(plain.archived_schemas || 0),
          weight: Number(plain.weight || 0),
          issued: Number(plain.issued || 0),
          verified: Number(plain.verified || 0),
          ecosystem_slash_events: Number(plain.ecosystem_slash_events || 0),
          ecosystem_slashed_amount: Number(plain.ecosystem_slashed_amount || 0),
          ecosystem_slashed_amount_repaid: Number(plain.ecosystem_slashed_amount_repaid || 0),
          network_slash_events: Number(plain.network_slash_events || 0),
          network_slashed_amount: Number(plain.network_slashed_amount || 0),
          network_slashed_amount_repaid: Number(plain.network_slashed_amount_repaid || 0),
        }
      })

      const sortedRegistries = liveSortFullyApplied
        ? registriesWithStats.slice(0, responseMaxSize)
        : this.sortRegistries(registriesWithStats, sort, responseMaxSize)

      const responsePayload = {
        ecosystems: sortedRegistries.map((r) => mapEcosystemApiFields(r as Record<string, unknown>)),
      }
      const enrichedResponsePayload =
        trustDataMode === 'none'
          ? responsePayload
          : await enrichTrustDataDeep(responsePayload, trustDataMode, blockHeight)
      return ApiResponder.success(ctx, enrichedResponsePayload, 200)
    } catch (err: any) {
      return ApiResponder.error(ctx, err.message, 500)
    }
  }

  private async getEcosystemIdsForParticipant(account: string): Promise<number[]> {
    const corporationId = await resolveCorporationIdByAddress(account)
    if (corporationId === null) return []
    const trPart = await resolveEcosystemParticipantColumn(knex)
    const controllerRows = await knex('ecosystem').where(trPart, corporationId).select('id')
    const controllerIds = controllerRows.map((r: { id: number }) => r.id)

    const participantPart = await resolveParticipantsParticipantColumn(knex)
    const corpSchemaIds = await knex('participants').where(participantPart, corporationId).distinct('schema_id')
    const schemaIds = corpSchemaIds
      .map((r: { schema_id: string }) => {
        const id = r.schema_id ? parseFloat(r.schema_id) : null
        return id !== null && !Number.isNaN(id) ? id : null
      })
      .filter((id): id is number => id !== null)
    if (schemaIds.length === 0) {
      return [...new Set(controllerIds)]
    }
    const ecosystemIdRows = await knex('credential_schemas').whereIn('id', schemaIds).distinct('ecosystem_id')
    const corpEcosystemIds = ecosystemIdRows.map((r: { ecosystem_id: number }) => r.ecosystem_id)

    return [...new Set([...controllerIds, ...corpEcosystemIds])]
  }

  private async getEcosystemIdsForParticipantAtHeight(account: string, blockHeight: number): Promise<number[]> {
    const corporationId = await resolveCorporationIdByAddress(account)
    if (corporationId === null) return []
    const trHistPart = await resolveEcosystemHistoryParticipantColumn(knex)
    const ecosystemHistoryRows = await knex('ecosystem_history')
      .where('height', '<=', blockHeight)
      .where(trHistPart, corporationId)
      .select('ecosystem_id')
    const controllerEcosystemIds = [
      ...new Set(ecosystemHistoryRows.map((r: { ecosystem_id: number }) => r.ecosystem_id)),
    ]

    const participantHistPart = await resolveParticipantHistoryParticipantColumn(knex)
    const corpParticipantRows = await knex('participant_history')
      .where('height', '<=', blockHeight)
      .where(participantHistPart, corporationId)
      .distinct('schema_id')
    const schemaIds = corpParticipantRows
      .map((r: { schema_id: number }) => r.schema_id)
      .filter((id): id is number => id != null)
    if (schemaIds.length === 0) {
      return controllerEcosystemIds
    }

    const cshSub = knex('credential_schema_history')
      .select('credential_schema_id', 'ecosystem_id')
      .select(
        knex.raw('ROW_NUMBER() OVER (PARTITION BY credential_schema_id ORDER BY height DESC, created_at DESC) as rn')
      )
      .where('height', '<=', blockHeight)
      .whereIn('credential_schema_id', schemaIds)
      .as('ranked')
    const latestCsh = await knex.from(cshSub).where('rn', 1).select('ecosystem_id')
    const corpEcosystemIds = [...new Set(latestCsh.map((r: { ecosystem_id: number }) => r.ecosystem_id))]

    return [...new Set([...controllerEcosystemIds, ...corpEcosystemIds])]
  }

  @Action()
  public async getParams(ctx: Context) {
    const { getModuleParamsAction } = await import('../../common/utils/params_service')
    return getModuleParamsAction(ctx, ModulesParamsNamesTypes.EC, MODULE_DISPLAY_NAMES.ECOSYSTEM)
  }
}
