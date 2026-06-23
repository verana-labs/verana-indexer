import { Action, Service } from '@ourparentcenter/moleculer-decorators-extended'
import { Context, ServiceBroker } from 'moleculer'
import BullableService from '../../base/bullable.service'
import { SERVICE } from '../../common'
import { formatTimestamp } from '../../common/utils/date_utils'
import knex from '../../common/utils/db_connection'
import { finalizeEcosystemHistoryInsert } from '../../common/utils/installed_table_columns'
import { MessageProcessorBase } from '../../common/utils/message_processor_base'
import { detectStartMode } from '../../common/utils/start_mode_detector'
import { VeranaEcosystemMessageTypes } from '../../common/verana-message-types'
import { getEcosystem } from '../../modules/ec-height-sync/ec_height_sync_helpers'
import { resolveCorporationIdForMessage } from '../crawl-co/corporation_resolve'
import { calculateEcosystemStats, TR_STATS_FIELDS } from './ec_stats'

type ChangeRecord = Record<string, any>

function getDefaultTRStats(fallbackData?: any): any {
  return {
    participants: Number(fallbackData?.participants ?? 0) || 0,
    participants_ecosystem: Number(fallbackData?.participants_ecosystem ?? 0) || 0,
    participants_issuer_grantor: Number(fallbackData?.participants_issuer_grantor ?? 0) || 0,
    participants_issuer: Number(fallbackData?.participants_issuer ?? 0) || 0,
    participants_verifier_grantor: Number(fallbackData?.participants_verifier_grantor ?? 0) || 0,
    participants_verifier: Number(fallbackData?.participants_verifier ?? 0) || 0,
    participants_holder: Number(fallbackData?.participants_holder ?? 0) || 0,
    active_schemas: Number(fallbackData?.active_schemas ?? 0) || 0,
    archived_schemas: Number(fallbackData?.archived_schemas ?? 0) || 0,
    weight: Number(fallbackData?.weight ?? 0) || 0,
    issued: Number(fallbackData?.issued ?? 0) || 0,
    verified: Number(fallbackData?.verified ?? 0) || 0,
    ecosystem_slash_events: Number(fallbackData?.ecosystem_slash_events ?? 0) || 0,
    ecosystem_slashed_amount: Number(fallbackData?.ecosystem_slashed_amount ?? 0) || 0,
    ecosystem_slashed_amount_repaid: Number(fallbackData?.ecosystem_slashed_amount_repaid ?? 0) || 0,
    network_slash_events: Number(fallbackData?.network_slash_events ?? 0) || 0,
    network_slashed_amount: Number(fallbackData?.network_slashed_amount ?? 0) || 0,
    network_slashed_amount_repaid: Number(fallbackData?.network_slashed_amount_repaid ?? 0) || 0,
  }
}

@Service({
  name: SERVICE.V1.EcosystemMessageProcessorService.key,
  version: 1,
})
export default class EcosystemMessageProcessorService extends BullableService {
  private processorBase: MessageProcessorBase
  private _isFreshStart: boolean = false
  private ecosystemHistoryColumnsCache: Set<string> | null = null

  constructor(broker: ServiceBroker) {
    super(broker)
    this.processorBase = new MessageProcessorBase(this)
  }

  private extractEcosystemId(raw: any, options?: { allowTopLevelId?: boolean }): number | null {
    if (!raw || typeof raw !== 'object') return null
    const allowTopLevelId = options?.allowTopLevelId === true
    const candidates = [
      raw.ecosystem_id,
      raw.ecosystemId,
      raw.ecosystem_id,
      raw.ecosystemId,
      raw.ecosystem?.id,
      raw.ecosystem?.ecosystem_id,
      raw.ecosystem?.ecosystemId,
      raw.ecosystem?.id,
      raw.ecosystem?.ecosystem_id,
      raw.ecosystem?.ecosystemId,
      ...(allowTopLevelId ? [raw.id] : []),
    ]
    for (const candidate of candidates) {
      const n = Number(candidate)
      if (Number.isInteger(n) && n > 0) return n
    }
    return null
  }

  private resolveEcosystemIdForMessage(message: any): number | null {
    const eventEcosystemIds = Array.isArray(message?.eventEcosystemIds) ? message.eventEcosystemIds : []
    for (const rawEventId of eventEcosystemIds) {
      const eventId = Number(rawEventId)
      if (Number.isInteger(eventId) && eventId > 0) return eventId
    }

    const contentId = this.extractEcosystemId(message?.content, { allowTopLevelId: true })
    if (contentId) return contentId

    return this.extractEcosystemId(message)
  }

  public async _start() {
    const startMode = await detectStartMode()
    this._isFreshStart = startMode.isFreshStart
    this.processorBase.setFreshStartMode(this._isFreshStart)
    this.logger.info(`Ecosystem processor started | Mode: ${this._isFreshStart ? 'Fresh Start' : 'Reindexing'}`)
    await super._start()
    this.logger.info('EcosystemMessageProcessorService started and ready.')
  }

  @Action({ name: 'handleEcosystemMessages' })
  async handleEcosystemMessages(ctx: Context<{ ecosystemList: any[] }>) {
    const { ecosystemList } = ctx.params
    this.logger.info(` Processing ${ecosystemList?.length || 0} Ecosystem messages`)

    if (!ecosystemList || ecosystemList.length === 0) {
      this.logger.warn(' No Ecosystem messages to process')
      return
    }

    const failThreshold = 0.1
    const failedMessages: any[] = []
    const seenEcosystemIds: number[] = []
    const syncedEcosystemIds: number[] = []
    const seenHeightSyncKeys = new Set<string>()
    const totalMessages = ecosystemList.length
    const useHeightSyncTR = process.env.NODE_ENV !== 'test' && process.env.USE_HEIGHT_SYNC_TR === 'true'

    const processMessage = async (message: any, index: number) => {
      if (!message.type) {
        this.logger.error(`EC message missing type:`, JSON.stringify(message))
        failedMessages.push({ message, error: 'Missing type' })
        return
      }

      const messageEcosystemId = this.resolveEcosystemIdForMessage(message)
      this.logger.info(
        `Processing EC message ${index + 1}/${totalMessages}: type=${message.type}, height=${message.height}, ecosystem_id=${messageEcosystemId ?? 'n/a'}`
      )

      const processedTR: any = { ...message, ...message.content }
      const normalizedEcosystemId = this.resolveEcosystemIdForMessage(message)
      if (normalizedEcosystemId) {
        processedTR.ecosystem_id = normalizedEcosystemId
      }

      if (!useHeightSyncTR) {
        const numericId = this.resolveEcosystemIdForMessage(message)
        if (numericId) seenEcosystemIds.push(numericId)
      }

      delete processedTR?.content
      delete processedTR?.id
      delete processedTR?.tx_id
      delete processedTR?.['@type']

      let processed = false
      if (useHeightSyncTR) {
        if (normalizedEcosystemId && Number.isFinite(Number(message.height))) {
          const dedupeKey = `${Number(message.height)}::${normalizedEcosystemId}`
          if (seenHeightSyncKeys.has(dedupeKey)) {
            this.logger.debug(`[EC Height-Sync] Skip duplicate message for key=${dedupeKey}`)
            processed = true
          } else {
            seenHeightSyncKeys.add(dedupeKey)
          }
        }
        if (processed) return
        const syncedEcosystemId = await this.processEcosystemHeightSync(processedTR)
        if (syncedEcosystemId && Number.isInteger(syncedEcosystemId) && syncedEcosystemId > 0) {
          syncedEcosystemIds.push(syncedEcosystemId)
        }
        processed = true
      } else {
        if (processedTR.type === VeranaEcosystemMessageTypes.CreateEcosystem) {
          await this.processCreateTR(processedTR)
          processed = true
        }

        if (processedTR.type === VeranaEcosystemMessageTypes.AddGovernanceFrameworkDoc) {
          await this.processAddGovFrameworkDoc(processedTR)
          processed = true
        }

        if (processedTR.type === VeranaEcosystemMessageTypes.UpdateEcosystem) {
          await this.processUpdateTR(processedTR)
          processed = true
        }

        if (processedTR.type === VeranaEcosystemMessageTypes.IncreaseGovernanceFrameworkVersion) {
          await this.processIncreaseActiveGFV(processedTR)
          processed = true
        }

        if (processedTR.type === VeranaEcosystemMessageTypes.ArchiveEcosystem) {
          await this.processArchiveTR(processedTR)
          processed = true
        }
      }

      if (!processed) {
        this.logger.warn(`Unknown EC message type: ${processedTR.type}`)
        failedMessages.push({ message, error: `Unknown type: ${processedTR.type}` })
        throw new Error(`Unknown EC message type: ${processedTR.type}`)
      }
    }

    const sortedMessages = [...ecosystemList].sort((a, b) => {
      const heightDiff = (a.height || 0) - (b.height || 0)
      if (heightDiff !== 0) return heightDiff
      return (a.id || 0) - (b.id || 0)
    })

    let successCount = 0
    for (let i = 0; i < sortedMessages.length; i++) {
      const message = sortedMessages[i]
      try {
        await processMessage(message, i)
        successCount++
      } catch (err: any) {
        failedMessages.push({ message, error: err.message || String(err) })
      }
    }

    this.logger.info(
      `Ecosystem processing complete: ${successCount} succeeded, ${failedMessages.length} failed out of ${totalMessages} total`
    )

    if (failedMessages.length > 0) {
      this.logger.error(`Failed to process ${failedMessages.length} Ecosystem messages:`)
      failedMessages.forEach((failed, idx) => {
        this.logger.error(`  ${idx + 1}. Type: ${failed.message.type}, Error: ${failed.error}`)
      })

      if (failedMessages.length > totalMessages * failThreshold) {
        const failureRate = ((failedMessages.length / totalMessages) * 100).toFixed(2)
        this.logger.error(
          `CRITICAL: ${failureRate}% of EC messages failed (${failedMessages.length}/${totalMessages})! This indicates a serious issue.`
        )
        throw new Error(
          `Failed to process ${failedMessages.length} out of ${totalMessages} Ecosystem messages (${failureRate}% failure rate). This exceeds the ${(failThreshold * 100).toFixed(0)}% threshold.`
        )
      }
    }
  }

  private async updateTRStatsAndSync(
    ecosystemId: number,
    messageEcosystemId: number | string,
    height?: number
  ): Promise<void> {
    try {
      const oldTr = await knex('ecosystem').where('id', ecosystemId).first()
      if (!oldTr) return

      const stats = await calculateEcosystemStats(ecosystemId, height)
      const statsUpdate: any = {
        participants: Number(stats.participants ?? 0),
        participants_ecosystem: Number(stats.participants_ecosystem ?? 0),
        participants_issuer_grantor: Number(stats.participants_issuer_grantor ?? 0),
        participants_issuer: Number(stats.participants_issuer ?? 0),
        participants_verifier_grantor: Number(stats.participants_verifier_grantor ?? 0),
        participants_verifier: Number(stats.participants_verifier ?? 0),
        participants_holder: Number(stats.participants_holder ?? 0),
        active_schemas: Number(stats.active_schemas ?? 0),
        archived_schemas: Number(stats.archived_schemas ?? 0),
        weight: Number(stats.weight ?? 0),
        issued: Number(stats.issued ?? 0),
        verified: Number(stats.verified ?? 0),
        ecosystem_slash_events: Number(stats.ecosystem_slash_events ?? 0),
        ecosystem_slashed_amount: Number(stats.ecosystem_slashed_amount ?? 0),
        ecosystem_slashed_amount_repaid: Number(stats.ecosystem_slashed_amount_repaid ?? 0),
        network_slash_events: Number(stats.network_slash_events ?? 0),
        network_slashed_amount: Number(stats.network_slashed_amount ?? 0),
        network_slashed_amount_repaid: Number(stats.network_slashed_amount_repaid ?? 0),
      }

      const statsChanged =
        Number(oldTr.participants ?? 0) !== statsUpdate.participants ||
        Number(oldTr.participants_ecosystem ?? 0) !== statsUpdate.participants_ecosystem ||
        Number(oldTr.participants_issuer_grantor ?? 0) !== statsUpdate.participants_issuer_grantor ||
        Number(oldTr.participants_issuer ?? 0) !== statsUpdate.participants_issuer ||
        Number(oldTr.participants_verifier_grantor ?? 0) !== statsUpdate.participants_verifier_grantor ||
        Number(oldTr.participants_verifier ?? 0) !== statsUpdate.participants_verifier ||
        Number(oldTr.participants_holder ?? 0) !== statsUpdate.participants_holder ||
        Number(oldTr.active_schemas ?? 0) !== statsUpdate.active_schemas ||
        Number(oldTr.archived_schemas ?? 0) !== statsUpdate.archived_schemas ||
        Number(oldTr.weight ?? 0) !== statsUpdate.weight ||
        Number(oldTr.issued ?? 0) !== statsUpdate.issued ||
        Number(oldTr.verified ?? 0) !== statsUpdate.verified ||
        Number(oldTr.ecosystem_slash_events ?? 0) !== statsUpdate.ecosystem_slash_events ||
        Number(oldTr.ecosystem_slashed_amount ?? 0) !== statsUpdate.ecosystem_slashed_amount ||
        Number(oldTr.ecosystem_slashed_amount_repaid ?? 0) !== statsUpdate.ecosystem_slashed_amount_repaid ||
        Number(oldTr.network_slash_events ?? 0) !== statsUpdate.network_slash_events ||
        Number(oldTr.network_slashed_amount ?? 0) !== statsUpdate.network_slashed_amount ||
        Number(oldTr.network_slashed_amount_repaid ?? 0) !== statsUpdate.network_slashed_amount_repaid

      if (statsChanged) {
        this.logger.info(`Stats changed for EC ${ecosystemId}, updating main table and recording StatsUpdate history`)
      }

      await knex('ecosystem').where('id', ecosystemId).update(statsUpdate)

      if (statsChanged) {
        try {
          const updatedTr = await knex('ecosystem').where('id', ecosystemId).first()
          if (updatedTr) {
            const effectiveHeight = Number(height || updatedTr.height || oldTr.height || 0)
            await knex.transaction(async (trx) => {
              const updatedTrWithStats = { ...updatedTr, ...statsUpdate }
              await this.recordTRHistory(trx, ecosystemId, 'StatsUpdate', effectiveHeight, oldTr, updatedTrWithStats)
            })
          } else {
            this.logger.warn(` Updated EC ${ecosystemId} not found after stats update`)
          }
        } catch (historyErr: any) {
          this.logger.warn(
            ` Failed to record StatsUpdate history for EC ${ecosystemId}: ${historyErr?.message || String(historyErr)}`
          )
        }
      }

      if (!statsChanged) {
        this.logger.debug(` No stats changes detected for EC ${ecosystemId}, skipping history update`)
      }
    } catch (statsError: any) {
      this.logger.warn(
        ` Failed to update statistics for EC ${ecosystemId}: ${statsError?.message || String(statsError)}`
      )
    }

    if (process.env.USE_HEIGHT_SYNC_TR === 'true' && height) {
      try {
        const ecosystemIdNum = Number(messageEcosystemId)
        const blockHeight = Number(height || 0)
        if (Number.isInteger(ecosystemIdNum) && ecosystemIdNum > 0 && blockHeight > 0) {
          const ledgerResponse = await getEcosystem(ecosystemIdNum, blockHeight)
          if (ledgerResponse?.ecosystem) {
            await this.broker.call(`${SERVICE.V1.EcosystemDatabaseService.path}.syncFromLedger`, {
              ledgerResponse: { ecosystem: ledgerResponse.ecosystem },
              blockHeight,
            })
          } else {
            this.logger.warn(
              `[EC Ledger Sync] No ledger ecosystem found for id=${ecosystemIdNum} at height=${blockHeight}`
            )
          }
        }
      } catch (syncErr: any) {
        this.logger.warn(
          `[EC Ledger Sync] Failed to reconcile EC id=${messageEcosystemId}: ${syncErr?.message || String(syncErr)}`
        )
      }
    }
  }

  private async recordTRHistory(
    trx: any,
    ecosystemId: number,
    eventType: string,
    height: number,
    oldData: any,
    newData: any
  ) {
    const hasIndexedStats =
      !!newData && TR_STATS_FIELDS.some((field) => newData[field] !== undefined && newData[field] !== null)

    let stats: any
    if (hasIndexedStats) {
      stats = getDefaultTRStats(newData)
    } else {
      try {
        stats = await calculateEcosystemStats(ecosystemId, height)
      } catch (err: any) {
        this.logger.warn(
          `Failed to calculate stats for EC ${ecosystemId} at height ${height}: ${err?.message || String(err)}`
        )
        stats = getDefaultTRStats(newData)
      }
    }

    const changes: ChangeRecord = {}

    if (oldData) {
      for (const [key, value] of Object.entries(newData)) {
        if (key !== 'id' && key !== 'height' && !TR_STATS_FIELDS.includes(key)) {
          const oldVal = oldData[key]
          if (oldVal !== value) {
            changes[key] = value
          }
        }
      }

      for (const field of TR_STATS_FIELDS) {
        const oldVal = oldData[field] != null ? Number(oldData[field]) : 0
        const newVal = stats[field] != null ? Number(stats[field]) : 0
        if (oldVal !== newVal) {
          changes[field] = newVal
        }
      }
    } else {
      for (const [key, value] of Object.entries(newData)) {
        if (
          key !== 'id' &&
          key !== 'height' &&
          !TR_STATS_FIELDS.includes(key) &&
          value !== null &&
          value !== undefined
        ) {
          changes[key] = value
        }
      }
      for (const field of TR_STATS_FIELDS) {
        const val = stats[field] != null ? Number(stats[field]) : 0
        changes[field] = val
      }
    }

    changes.height = Number(height)

    const historyPayload: any = {
      ecosystem_id: ecosystemId,
      did: newData.did,
      corporation_id: Number(newData.corporation_id ?? 0) || 0,
      created: newData.created,
      modified: newData.modified,
      archived: newData.archived ?? null,
      aka: newData.aka ?? null,
      language: newData.language,
      active_version: newData.active_version ?? null,
      participants: Number(stats.participants ?? 0),
      participants_ecosystem: Number(stats.participants_ecosystem ?? 0),
      participants_issuer_grantor: Number(stats.participants_issuer_grantor ?? 0),
      participants_issuer: Number(stats.participants_issuer ?? 0),
      participants_verifier_grantor: Number(stats.participants_verifier_grantor ?? 0),
      participants_verifier: Number(stats.participants_verifier ?? 0),
      participants_holder: Number(stats.participants_holder ?? 0),
      active_schemas: Number(stats.active_schemas ?? 0),
      archived_schemas: Number(stats.archived_schemas ?? 0),
      weight: Number(stats.weight ?? 0),
      issued: Number(stats.issued ?? 0),
      verified: Number(stats.verified ?? 0),
      ecosystem_slash_events: Number(stats.ecosystem_slash_events ?? 0),
      ecosystem_slashed_amount: Number(stats.ecosystem_slashed_amount ?? 0),
      ecosystem_slashed_amount_repaid: Number(stats.ecosystem_slashed_amount_repaid ?? 0),
      network_slash_events: Number(stats.network_slash_events ?? 0),
      network_slashed_amount: Number(stats.network_slashed_amount ?? 0),
      network_slashed_amount_repaid: Number(stats.network_slashed_amount_repaid ?? 0),
      event_type: eventType,
      height: Number(height),
      changes: Object.keys(changes).length > 0 ? JSON.stringify(changes) : null,
      created_at: newData.modified ?? newData.created ?? new Date(),
    }

    const historyColumns = await this.getEcosystemHistoryColumns(trx)
    const rowForInsert = finalizeEcosystemHistoryInsert(historyColumns, historyPayload, newData) as Record<string, any>

    try {
      const existingSameEvent = await trx('ecosystem_history')
        .where({
          ecosystem_id: ecosystemId,
          event_type: eventType,
          height: Number(height),
        })
        .orderBy('id', 'desc')
        .first()
      if (existingSameEvent) {
        const existingChanges = existingSameEvent.changes ? String(existingSameEvent.changes) : null
        const nextChanges = rowForInsert.changes ? String(rowForInsert.changes) : null
        if (existingChanges === nextChanges) {
          this.logger.debug(
            `Skipping duplicate EC history for ecosystem_id=${ecosystemId}, event_type=${eventType}, height=${height}`
          )
          return
        }
      }

      await trx('ecosystem_history').insert(rowForInsert)
      this.logger.debug(
        ` Recorded EC history for ecosystem_id=${ecosystemId}, event_type=${eventType}, height=${height}`
      )
    } catch (insertErr: any) {
      this.logger.error(
        `❌ Failed to insert EC history for ecosystem_id=${ecosystemId}: ${insertErr?.message || String(insertErr)}`
      )
      throw insertErr
    }
  }

  private async getEcosystemHistoryColumns(trx: any): Promise<Set<string>> {
    if (this.ecosystemHistoryColumnsCache) {
      return this.ecosystemHistoryColumnsCache
    }
    const info = await trx('ecosystem_history').columnInfo()
    this.ecosystemHistoryColumnsCache = new Set(Object.keys(info || {}))
    return this.ecosystemHistoryColumnsCache
  }

  private async recordGFVHistory(
    trx: any,
    gfvId: number,
    ecosystemId: number,
    eventType: string,
    height: number,
    oldData: any,
    newData: any
  ) {
    let changes: ChangeRecord | null = null
    const isCreation = !oldData

    if (oldData) {
      const computed = this.processorBase.computeChanges(oldData, newData)
      changes = Object.keys(computed).length > 0 ? computed : null
    } else {
      const creationChanges: ChangeRecord = {}
      for (const [key, value] of Object.entries(newData)) {
        if (value !== null && value !== undefined && key !== 'id') {
          creationChanges[key] = value
        }
      }
      changes = Object.keys(creationChanges).length > 0 ? creationChanges : null
    }

    if (!isCreation && !changes) {
      return
    }

    await trx('governance_framework_version_history').insert({
      ecosystem_id: ecosystemId,
      created: newData.created || new Date(),
      version: newData.version,
      active_since: newData.active_since || newData.created || new Date(),
      event_type: eventType,
      height,
      changes: changes ? JSON.stringify(changes) : null,
      created_at: newData.active_since || newData.created || new Date(),
    })
  }

  private async recordGFDHistory(
    trx: any,
    gfdId: number,
    gfvId: number,
    ecosystemId: number,
    eventType: string,
    height: number,
    oldData: any,
    newData: any
  ) {
    let changes: ChangeRecord | null = null
    const isCreation = !oldData

    if (oldData) {
      const computed = this.processorBase.computeChanges(oldData, newData)
      changes = Object.keys(computed).length > 0 ? computed : null
    } else {
      const creationChanges: ChangeRecord = {}
      for (const [key, value] of Object.entries(newData)) {
        if (value !== null && value !== undefined && key !== 'id') {
          creationChanges[key] = value
        }
      }
      changes = Object.keys(creationChanges).length > 0 ? creationChanges : null
    }

    if (!isCreation && !changes) {
      return
    }

    await trx('governance_framework_document_history').insert({
      gfv_id: gfvId,
      ecosystem_id: ecosystemId,
      created: newData.created || new Date(),
      language: newData.language || '',
      url: newData.url || '',
      digest_sri: newData.digest_sri || '',
      event_type: eventType,
      height,
      changes: changes ? JSON.stringify(changes) : null,
      created_at: newData.created || new Date(),
    })
  }

  private async processEcosystemHeightSync(message: any): Promise<number | null> {
    const ecosystemId =
      this.resolveEcosystemIdForMessage(message) ?? this.extractEcosystemId(message, { allowTopLevelId: true })
    const heightNum = Number(message.height || 0)

    if (!ecosystemId) {
      this.logger.warn(
        `[EC Height-Sync] Skipping message with invalid ecosystem_id=${String(
          message?.ecosystem_id ?? message?.ecosystemId ?? message?.ecosystem_id ?? message?.ecosystemId ?? message?.id
        )}, height=${message.height}`
      )
      return null
    }
    if (!Number.isFinite(heightNum) || heightNum <= 0) {
      this.logger.warn(
        `[EC Height-Sync] Skipping message for ecosystem_id=${ecosystemId} due to invalid height=${message.height}`
      )
      return null
    }

    const blockHeight = heightNum

    try {
      await knex('ecosystem').where({ id: ecosystemId }).first()
    } catch (err: any) {
      this.logger.warn(
        `[EC Height-Sync] Failed to load previous EC row for id=${ecosystemId}: ${err?.message || String(err)}`
      )
    }

    let actualEcosystemId: number | null = null
    try {
      const ledgerResponse = await getEcosystem(ecosystemId, blockHeight)
      if (!ledgerResponse?.ecosystem) {
        this.logger.warn(`[EC Height-Sync] Ledger returned no ecosystem for id=${ecosystemId} at height=${blockHeight}`)
        return null
      }

      const ledgerTr = ledgerResponse.ecosystem
      const extractedEcosystemId = Number(ledgerTr.id ?? ledgerTr.ecosystem_id ?? ecosystemId)
      if (Number.isInteger(extractedEcosystemId) && extractedEcosystemId > 0) {
        actualEcosystemId = extractedEcosystemId
      } else {
        actualEcosystemId = ecosystemId
      }

      const syncResult: any = await this.broker.call(`${SERVICE.V1.EcosystemDatabaseService.path}.syncFromLedger`, {
        ledgerResponse: { ecosystem: ledgerResponse.ecosystem },
        blockHeight,
      })

      if (!syncResult || syncResult.success !== true) {
        this.logger.warn(
          `[EC Height-Sync] syncFromLedger reported failure for id=${actualEcosystemId} at height=${blockHeight}: ${JSON.stringify(syncResult)}`
        )
        return null
      }

      if (!actualEcosystemId) {
        actualEcosystemId = ecosystemId
      }
    } catch (err: any) {
      this.logger.warn(
        `[EC Height-Sync] Failed to sync EC id=${ecosystemId} from ledger at height=${blockHeight}: ${
          err?.message || String(err)
        }`
      )
      return null
    }

    let newTr: any | null = null
    if (actualEcosystemId != null) {
      try {
        newTr = await knex('ecosystem').where({ id: actualEcosystemId }).first()
      } catch (err: any) {
        this.logger.warn(
          `[EC Height-Sync] Failed to load updated EC row for id=${actualEcosystemId}: ${err?.message || String(err)}`
        )
      }
    }
    if (!newTr) {
      this.logger.warn(
        `[EC Height-Sync] No persisted EC row found after sync for id=${actualEcosystemId} at height=${blockHeight}`
      )
      return null
    }
    return actualEcosystemId
  }

  private async processArchiveTR(message: any) {
    const trx = await knex.transaction()
    try {
      const ec = await trx('ecosystem').where({ id: message.ecosystem_id }).first()
      if (!ec) {
        await trx.rollback()
        this.logger.warn(` ArchiveTR: EC not found for id=${message.ecosystem_id}, height=${message.height}`)
        return
      }

      const timestamp = formatTimestamp(message.timestamp)
      const shouldArchive = message.archive === true || message.archive === 'true'
      const newData = {
        ...ec,
        archived: shouldArchive ? timestamp : null,
        modified: timestamp,
      }

      await trx('ecosystem').where({ id: ec.id }).update(newData)
      const blockHeight = message.height || 0
      await this.recordTRHistory(trx, ec.id, 'Archive', blockHeight, ec, newData)

      await trx.commit()
      this.logger.info(` Successfully archived EC: id=${ec.id}`)

      await this.updateTRStatsAndSync(ec.id, message.ecosystem_id ?? ec.id, message.height)
    } catch (err: any) {
      await trx.rollback()
      const errorMessage = err?.message || String(err)
      this.logger.error(`❌ Failed to process ArchiveEcosystem for id=${message.ecosystem_id}:`, errorMessage)
      console.error('FATAL EC ARCHIVE ERROR:', err)
      throw err
    }
  }

  private async processUpdateTR(message: any) {
    const trx = await knex.transaction()
    try {
      const ec = await trx('ecosystem').where({ id: message.ecosystem_id }).first()
      if (!ec) {
        await trx.rollback()
        this.logger.warn(` UpdateTR: EC not found for id=${message.ecosystem_id}, height=${message.height}`)
        return
      }

      const updateData: any = { ...ec }
      if (message.did !== undefined) updateData.did = message.did
      if (message.aka !== undefined) updateData.aka = message.aka
      if (message.language !== undefined) updateData.language = message.language
      if (message.height !== undefined) updateData.height = message.height
      updateData.modified = formatTimestamp(message.timestamp)

      await trx('ecosystem').where({ id: ec.id }).update(updateData)
      const blockHeight = message.height || 0
      const updatedTr = await trx('ecosystem').where({ id: ec.id }).first()
      if (updatedTr) {
        await this.recordTRHistory(trx, ec.id, 'Update', blockHeight, ec, updatedTr)
      }

      await trx.commit()
      this.logger.info(` Successfully updated EC: id=${ec.id}`)

      await this.updateTRStatsAndSync(ec.id, message.ecosystem_id ?? ec.id, message.height)
    } catch (err: any) {
      await trx.rollback()
      const errorMessage = err?.message || String(err)
      this.logger.error(`❌ Failed to process UpdateEcosystem for id=${message.ecosystem_id}:`, errorMessage)
      console.error('FATAL EC UPDATE ERROR:', err)
      throw err
    }
  }

  private async processCreateTR(message: any) {
    this.logger.info(' Processing CreateTR message:', JSON.stringify(message))

    if (!message.did) {
      throw new Error('CreateTR message missing required field: did')
    }

    const trx = await knex.transaction()
    try {
      const timestamp = formatTimestamp(message.timestamp)
      const blockHeight = message.height || 0

      this.logger.info(` Creating EC with height: ${blockHeight}, did: ${message.did}`)

      const existingTR = await trx('ecosystem').where({ did: message.did, height: blockHeight }).first()

      let ec: any
      const corporationId = await resolveCorporationIdForMessage(message, trx)
      const isReindexing = !!existingTR

      if (isReindexing) {
        this.logger.info(
          `EC with did ${message.did} and height ${blockHeight} already exists, updating for reindexing...`
        )
        ;[ec] = await trx('ecosystem')
          .where({ id: existingTR.id })
          .update({
            did: message.did,
            corporation_id: corporationId,
            modified: timestamp,
            aka: message.aka,
            language: message.language,
            height: blockHeight,
          })
          .returning('*')
      } else {
        this.logger.info(`🆕 Creating new EC with did ${message.did} at height ${blockHeight}`)
        ;[ec] = await trx('ecosystem')
          .insert({
            did: message.did,
            corporation_id: corporationId,
            created: timestamp,
            modified: timestamp,
            aka: message.aka,
            language: message.language,
            height: blockHeight,
            active_version: 1,
          })
          .returning('*')
      }

      await this.recordTRHistory(trx, ec.id, 'Create', blockHeight, null, ec)

      let gfv = await trx('governance_framework_version')
        .where({
          ecosystem_id: ec.id,
          version: 1,
        })
        .first()

      if (!gfv) {
        ;[gfv] = await trx('governance_framework_version')
          .insert({
            ecosystem_id: ec.id,
            created: timestamp,
            version: 1,
            active_since: timestamp,
          })
          .returning('*')
      } else if (isReindexing) {
        await trx('governance_framework_version').where({ id: gfv.id }).update({
          created: timestamp,
          active_since: timestamp,
        })
        gfv = await trx('governance_framework_version').where({ id: gfv.id }).first()
      }

      await this.recordGFVHistory(trx, gfv.id, ec.id, 'CreateGFV', blockHeight, null, gfv)

      const language = message.language
      const digestSri = message.doc_digest_sri

      let gfd = await trx('governance_framework_document')
        .where({
          gfv_id: gfv.id,
          language,
          digest_sri: digestSri,
        })
        .first()

      if (!gfd) {
        ;[gfd] = await trx('governance_framework_document')
          .insert({
            gfv_id: gfv.id,
            created: timestamp,
            language,
            url: message.doc_url,
            digest_sri: digestSri,
          })
          .returning('*')
      }

      await this.recordGFDHistory(trx, gfd.id, gfv.id, ec.id, 'CreateGFD', blockHeight, null, gfd)

      await trx.commit()
      this.logger.info(` Successfully created/updated EC: did=${message.did}, id=${ec.id}`)

      await this.updateTRStatsAndSync(ec.id, ec.id, message.height)
    } catch (err: any) {
      await trx.rollback()
      const errorMessage = err?.message || String(err)
      this.logger.error(`❌ Failed to process CreateEcosystem for did=${message.did}:`, errorMessage)
      console.error('FATAL EC CREATE ERROR:', err)
      throw err
    }
  }

  private async processAddGovFrameworkDoc(message: any) {
    const trx = await knex.transaction()
    try {
      const ec = await trx('ecosystem').where({ id: message.ecosystem_id }).first()
      if (!ec) {
        await trx.rollback()
        throw new Error(`AddGovFrameworkDoc: EC not found for id=${message.ecosystem_id}, height=${message.height}`)
      }

      const timestamp = formatTimestamp(message.timestamp)
      const blockHeight = message.height || 0

      let gfv = await trx('governance_framework_version')
        .where({
          ecosystem_id: ec.id,
          version: message.version,
        })
        .first()

      if (!gfv) {
        const maxVersionResult = await trx('governance_framework_version')
          .where({ ecosystem_id: ec.id })
          .max('version as max_version')
          .first()
        const maxVersion = maxVersionResult?.max_version || 0

        if (message.version !== maxVersion + 1 || message.version <= ec.active_version) {
          await trx.rollback()
          const errMsg = `AddGovFrameworkDoc: Invalid version=${message.version} for ecosystem_id=${ec.id}, maxVersion=${maxVersion}, active_version=${ec.active_version}`
          this.logger.error(errMsg)
          this.logger.error('AddGovFrameworkDoc message payload:', JSON.stringify(message))
          console.error('FATAL: Invalid AddGovFrameworkDoc version. Exiting for debug.')
          throw new Error(errMsg)
        }

        ;[gfv] = await trx('governance_framework_version')
          .insert({
            ecosystem_id: ec.id,
            created: timestamp,
            version: message.version,
            // active_since: null, // Omit to allow default null
          })
          .returning('*')

        await this.recordGFVHistory(trx, gfv.id, ec.id, 'AddGFV', blockHeight, null, gfv)
      }

      const language = message.doc_language || message.language
      const digestSri = message.doc_digest_sri || message.digest_sri

      let gfd = await trx('governance_framework_document')
        .where({
          gfv_id: gfv.id,
          digest_sri: digestSri,
        })
        .first()

      if (gfd) {
      }

      const oldGfd = null
      ;[gfd] = await trx('governance_framework_document')
        .insert({
          gfv_id: gfv.id,
          created: timestamp,
          language,
          url: message.doc_url,
          digest_sri: digestSri,
        })
        .returning('*')

      await this.recordGFDHistory(trx, gfd.id, gfv.id, ec.id, oldGfd ? 'UpdateGFD' : 'AddGFD', blockHeight, oldGfd, gfd)

      await trx.commit()
      this.logger.info(
        ` AddGovFrameworkDoc OK: ecosystem_id=${ec.id}, gfv_version=${message.version}, gfd_id=${gfd.id}`
      )
    } catch (err: any) {
      await trx.rollback()
      this.logger.error(`❌ AddGovFrameworkDoc failed for ecosystem_id=${message.ecosystem_id}:`, err?.message || err)
      throw err
    }
  }

  private async processIncreaseActiveGFV(message: any) {
    const trx = await knex.transaction()
    try {
      const ec = await trx('ecosystem').where({ id: message.ecosystem_id }).first()
      if (!ec) {
        await trx.rollback()
        this.logger.warn(` IncreaseActiveGFV: EC not found for id=${message.ecosystem_id}, height=${message.height}`)
        return
      }

      const nextVersion = ec.active_version + 1
      const gfv = await trx('governance_framework_version').where({ ecosystem_id: ec.id, version: nextVersion }).first()
      if (!gfv) {
        await trx.rollback()
        this.logger.warn(
          ` IncreaseActiveGFV: GFV version ${nextVersion} not found for ecosystem_id=${ec.id}, height=${message.height}. Will retry.`
        )
        throw new Error(`GFV version ${nextVersion} not found for ecosystem_id=${ec.id}, retry needed`)
      }

      const timestamp = formatTimestamp(message.timestamp)

      await trx('ecosystem').where({ id: ec.id }).update({ active_version: nextVersion, modified: timestamp })
      const blockHeight = message.height || 0
      await this.recordTRHistory(trx, ec.id, 'IncreaseGFV', blockHeight, ec, {
        ...ec,
        active_version: nextVersion,
        modified: timestamp,
      })

      await trx('governance_framework_version').where({ id: gfv.id }).update({ active_since: timestamp })
      await this.recordGFVHistory(trx, gfv.id, ec.id, 'ActivateGFV', blockHeight, gfv, {
        ...gfv,
        active_since: timestamp,
      })

      await trx.commit()
      this.logger.info(` Successfully increased active GFV: ecosystem_id=${ec.id}, version=${nextVersion}`)

      await this.updateTRStatsAndSync(ec.id, message.ecosystem_id ?? ec.id, message.height)
    } catch (err: any) {
      await trx.rollback()
      const errorMessage = err?.message || String(err)
      this.logger.error(
        `❌ Failed to process IncreaseActiveGFV for ecosystem_id=${message.ecosystem_id}:`,
        errorMessage
      )
      console.error('FATAL EC INCREASE GFV ERROR:', err)
      throw err
    }
  }
}
