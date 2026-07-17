import { Action, Service } from '@ourparentcenter/moleculer-decorators-extended'
import { Context, ServiceBroker } from 'moleculer'
import BullableService from '../../base/bullable.service'
import { MODULE_DISPLAY_NAMES, ModulesParamsNamesTypes, SERVICE } from '../../common'
import { validateParticipantParam, validateRequiredAccountParam } from '../../common/utils/accountValidation'
import { buildActivityTimeline } from '../../common/utils/activity_timeline_helper'
import ApiResponder from '../../common/utils/apiResponse'
import { getBlockHeight, hasBlockHeight } from '../../common/utils/blockHeight'
import { isValidISO8601UTC } from '../../common/utils/date_utils'
import knex from '../../common/utils/db_connection'
import { getModuleParams, getModuleParamsAction } from '../../common/utils/params_service'
import { mapParticipantType, normalizeParticipantEmptyStringsToNull } from '../../common/utils/utils'
import { mapParticipantApiFields } from '../../common/vpr-v4-mapping'
import { compareById, parseIdSortDirection } from '../crawl-co/co_stats'
import { resolveCorporationIdByAddress } from '../crawl-co/corporation_resolve'
import { enrichTrustDataDeep, parseTrustDataMode, type TrustDataMode } from '../resolver/trust-data-enrichment'
import {
  calculateCorporationAvailableActions,
  calculateParticipantState,
  calculateValidatorAvailableActions,
  mapParticipantActionsToVprMessages,
  type ParticipantState,
  PENDING_FLAT_VALIDATOR_PARENT_TYPES,
  pendingFlatMatchesOpPendingWithEligibleParticipantState,
  type SchemaData,
} from './pp_state_utils'

const IS_PG_CLIENT = String((knex as any)?.client?.config?.client || '').includes('pg')

@Service({
  name: SERVICE.V1.ParticipantAPIService.key,
  version: 1,
})
export default class ParticipantAPIService extends BullableService {
  private static readonly LIST_PARTICIPANTS_SLOW_MS = 200
  private static readonly VALID_PARTICIPANT_TYPES = new Set([
    'ECOSYSTEM',
    'ISSUER_GRANTOR',
    'VERIFIER_GRANTOR',
    'ISSUER',
    'VERIFIER',
    'HOLDER',
  ])
  private static readonly VALID_OP_STATES = new Set([
    'VALIDATION_STATE_UNSPECIFIED',
    'PENDING',
    'VALIDATED',
    'TERMINATED',
  ])
  private readonly metricColumnAvailabilityCache = new Map<
    string,
    Promise<{
      hasIssuedColumn: boolean
      hasVerifiedColumn: boolean
      hasParticipantsColumn: boolean
      hasParticipantRoleColumns: boolean
      hasWeightColumn: boolean
      hasEcosystemSlashEventsColumn: boolean
      hasExpireSoonColumn: boolean
    }>
  >()

  constructor(broker: ServiceBroker) {
    super(broker)
  }

  private async enrichDidItemsWithTrustData<T>(items: T, mode: TrustDataMode, blockHeight?: number): Promise<T> {
    return enrichTrustDataDeep(items, mode, blockHeight)
  }

  private async getMetricColumnAvailability(tableName: 'participants' | 'participant_history'): Promise<{
    hasIssuedColumn: boolean
    hasVerifiedColumn: boolean
    hasParticipantsColumn: boolean
    hasParticipantRoleColumns: boolean
    hasWeightColumn: boolean
    hasEcosystemSlashEventsColumn: boolean
    hasExpireSoonColumn: boolean
  }> {
    const cacheKey = tableName
    const cached = this.metricColumnAvailabilityCache.get(cacheKey)
    if (cached) return cached

    const loadPromise = knex(tableName)
      .columnInfo()
      .then((columnInfo: any) => ({
        hasIssuedColumn: !!columnInfo.issued,
        hasVerifiedColumn: !!columnInfo.verified,
        hasParticipantsColumn: !!columnInfo.participants,
        hasParticipantRoleColumns:
          !!columnInfo.participants_ecosystem &&
          !!columnInfo.participants_issuer_grantor &&
          !!columnInfo.participants_issuer &&
          !!columnInfo.participants_verifier_grantor &&
          !!columnInfo.participants_verifier &&
          !!columnInfo.participants_holder,
        hasWeightColumn: !!columnInfo.weight,
        hasEcosystemSlashEventsColumn: !!columnInfo.ecosystem_slash_events,
        hasExpireSoonColumn: !!columnInfo.expire_soon,
      }))
      .catch((error) => {
        this.metricColumnAvailabilityCache.delete(cacheKey)
        throw error
      })

    this.metricColumnAvailabilityCache.set(cacheKey, loadPromise)
    return loadPromise
  }

  private shouldUseHistoryQuery(ctx: Context<any>, blockHeight: number | undefined): boolean {
    if (!hasBlockHeight(ctx) || blockHeight === undefined) return false
    const latestCheckpointHeight = Number((ctx.meta as any)?.latestCheckpoint?.height)
    if (Number.isFinite(latestCheckpointHeight) && latestCheckpointHeight > 0) {
      return blockHeight < latestCheckpointHeight
    }
    return true
  }

  private async getSchemaModes(schemaId: number, blockHeight?: number): Promise<SchemaData> {
    if (typeof blockHeight === 'number') {
      try {
        const schemaHistory = await knex('credential_schema_history')
          .where({ credential_schema_id: schemaId })
          .whereRaw('height <= ?', [Number(blockHeight)])
          .orderBy('height', 'desc')
          .orderBy('created_at', 'desc')
          .first()

        if (schemaHistory) {
          return {
            issuer_onboarding_mode: (schemaHistory as any).issuer_onboarding_mode ?? null,
            verifier_onboarding_mode: (schemaHistory as any).verifier_onboarding_mode ?? null,
          }
        }
      } catch (error: any) {
        this.logger.warn(
          `credential_schema_history table doesn't have height column, using main table. Error: ${error?.message || error}`
        )
      }
    }

    const schemaMain = await knex('credential_schemas').where({ id: schemaId }).first()

    const schema: SchemaData = {
      issuer_onboarding_mode: (schemaMain as any)?.issuer_onboarding_mode ?? null,
      verifier_onboarding_mode: (schemaMain as any)?.verifier_onboarding_mode ?? null,
    }

    return schema
  }

  private async getParticipantModuleParams(blockHeight?: number): Promise<any> {
    return getModuleParams(ModulesParamsNamesTypes.PP, blockHeight)
  }

  private normalizeParticipantSessionRow(row: any): any {
    if (!row) return row

    let sessionRecords: any[] = []
    try {
      if (typeof row.session_records === 'string') {
        sessionRecords = JSON.parse(row.session_records || '[]')
      } else if (Array.isArray(row.session_records)) {
        sessionRecords = row.session_records
      } else {
        sessionRecords = []
      }
    } catch {
      sessionRecords = []
    }

    return {
      id: row.id ?? row.session_id,
      corporation_id: Number(row.corporation_id ?? 0) || 0,
      vs_operator: row.vs_operator ?? null,
      session_records: sessionRecords,
      created: row.created ?? null,
      modified: row.modified ?? null,
    }
  }

  private isTrustResolutionListQuery(params: any, _blockHeight: number | undefined): boolean {
    if (!params?.did) return false
    if (params?.schema_id === undefined || params?.schema_id === null) return false

    const expensiveMetricFilters = [
      'min_participants',
      'max_participants',
      'min_weight',
      'max_weight',
      'min_issued',
      'max_issued',
      'min_verified',
      'max_verified',
      'min_ecosystem_slash_events',
      'max_ecosystem_slash_events',
      'min_network_slash_events',
      'max_network_slash_events',
    ]
    const hasExpensiveMetricFilter = expensiveMetricFilters.some((k) => params[k] !== undefined)
    if (hasExpensiveMetricFilter) return false

    return true
  }

  private shouldUseStrictTrustResolutionLightweightMode(params: any, limit: number): boolean {
    if (!params?.did) return false
    if (params?.schema_id === undefined || params?.schema_id === null) return false
    if (limit > 32) return false

    const disallowedKeys = [
      'corporation_id',
      'participant_id',
      'validator_participant_id',
      'participant_state',
      'role',
      'only_valid',
      'only_slashed',
      'only_repaid',
      'modified_after',
      'op_state',
      'when',
      'min_participants',
      'max_participants',
      'min_weight',
      'max_weight',
      'min_issued',
      'max_issued',
      'min_verified',
      'max_verified',
      'min_ecosystem_slash_events',
      'max_ecosystem_slash_events',
      'min_network_slash_events',
      'max_network_slash_events',
    ]
    return disallowedKeys.every((key) => params[key] === undefined)
  }

  private normalizeAndValidateTypeAndOpState(params: any): {
    ok: boolean
    message?: string
    role?: string
    op_state?: string
  } {
    const normalizedType = typeof params?.role === 'string' ? params.role.toUpperCase() : undefined
    const normalizedOpState = typeof params?.op_state === 'string' ? params.op_state.toUpperCase() : undefined
    let finalType = normalizedType
    let finalOpState = normalizedOpState

    if (
      normalizedType &&
      !normalizedOpState &&
      !ParticipantAPIService.VALID_PARTICIPANT_TYPES.has(normalizedType) &&
      ParticipantAPIService.VALID_OP_STATES.has(normalizedType)
    ) {
      finalOpState = normalizedType
      finalType = undefined
    }

    if (finalType !== undefined && !ParticipantAPIService.VALID_PARTICIPANT_TYPES.has(finalType)) {
      return {
        ok: false,
        message: `Invalid type '${finalType}'. Allowed values: ${Array.from(ParticipantAPIService.VALID_PARTICIPANT_TYPES).join(', ')}`,
      }
    }

    if (finalOpState !== undefined && !ParticipantAPIService.VALID_OP_STATES.has(finalOpState)) {
      return {
        ok: false,
        message: `Invalid op_state '${finalOpState}'. Allowed values: ${Array.from(ParticipantAPIService.VALID_OP_STATES).join(', ')}`,
      }
    }

    return {
      ok: true,
      role: finalType,
      op_state: finalOpState,
    }
  }

  private applyBaseListFiltersToQuery(
    query: any,
    params: any,
    corporationIdFilter: number | undefined,
    modifiedAfterIso: string | undefined,
    whenIso: string | undefined,
    onlyValid: boolean,
    onlySlashed: boolean,
    onlyRepaid: boolean,
    nowIso: string,
    tablePrefix?: string,
    participantIdColumn: string = 'id'
  ): void {
    const col = (name: string) => (tablePrefix ? `${tablePrefix}.${name}` : name)

    if (params.schema_id !== undefined) query.where(col('schema_id'), Number(params.schema_id))
    if (corporationIdFilter !== undefined) query.where(col('corporation_id'), corporationIdFilter)
    if (params.did) query.where(col('did'), params.did)
    if (params.participant_id !== undefined) query.where(col(participantIdColumn), Number(params.participant_id))
    if (params.validator_participant_id !== undefined) {
      if (params.validator_participant_id === null || params.validator_participant_id === 'null') {
        query.whereNull(col('validator_participant_id'))
      } else {
        query.where(col('validator_participant_id'), Number(params.validator_participant_id))
      }
    }
    if (params.role) query.where(col('role'), params.role)
    if (params.op_state) query.where(col('op_state'), params.op_state)
    if (modifiedAfterIso) query.where(col('modified'), '>', modifiedAfterIso)
    if (whenIso) query.where(col('modified'), '<=', whenIso)

    if (onlyValid) {
      query.where((qb: any) => {
        qb.whereNull(col('revoked'))
          .andWhere((q: any) => q.whereNull(col('slashed')).orWhereNotNull(col('repaid')))
          .andWhere((q: any) => q.whereNull(col('effective_until')).orWhere(col('effective_until'), '>', nowIso))
          .andWhere((q: any) => q.whereNull(col('effective_from')).orWhere(col('effective_from'), '<=', nowIso))
      })
    }

    if (params.only_slashed !== undefined) {
      if (onlySlashed) query.whereNotNull(col('slashed'))
      else query.whereNull(col('slashed'))
    }

    if (params.only_repaid !== undefined) {
      if (onlyRepaid) query.whereNotNull(col('repaid'))
      else query.whereNull(col('repaid'))
    }
  }

  private applyMetricFiltersToSql(
    query: any,
    params: any,
    options: {
      participants: boolean
      participantRoles: boolean
      weight: boolean
      issued: boolean
      verified: boolean
      slashStats: boolean
      tablePrefix?: string
    }
  ): { requiresPostFilter: boolean; impossibleRange: boolean } {
    const col = (name: string) => (options.tablePrefix ? `${options.tablePrefix}.${name}` : name)
    const metricSpecs = [
      { min: 'min_participants', max: 'max_participants', db: 'participants', enabled: options.participants },
      {
        min: 'min_participants_ecosystem',
        max: 'max_participants_ecosystem',
        db: 'participants_ecosystem',
        enabled: options.participantRoles,
      },
      {
        min: 'min_participants_issuer_grantor',
        max: 'max_participants_issuer_grantor',
        db: 'participants_issuer_grantor',
        enabled: options.participantRoles,
      },
      {
        min: 'min_participants_issuer',
        max: 'max_participants_issuer',
        db: 'participants_issuer',
        enabled: options.participantRoles,
      },
      {
        min: 'min_participants_verifier_grantor',
        max: 'max_participants_verifier_grantor',
        db: 'participants_verifier_grantor',
        enabled: options.participantRoles,
      },
      {
        min: 'min_participants_verifier',
        max: 'max_participants_verifier',
        db: 'participants_verifier',
        enabled: options.participantRoles,
      },
      {
        min: 'min_participants_holder',
        max: 'max_participants_holder',
        db: 'participants_holder',
        enabled: options.participantRoles,
      },
      { min: 'min_weight', max: 'max_weight', db: 'weight', enabled: options.weight, exact: true },
      { min: 'min_issued', max: 'max_issued', db: 'issued', enabled: options.issued, exact: true },
      { min: 'min_verified', max: 'max_verified', db: 'verified', enabled: options.verified, exact: true },
      {
        min: 'min_ecosystem_slash_events',
        max: 'max_ecosystem_slash_events',
        db: 'ecosystem_slash_events',
        enabled: options.slashStats,
      },
      {
        min: 'min_network_slash_events',
        max: 'max_network_slash_events',
        db: 'network_slash_events',
        enabled: options.slashStats,
      },
    ]

    let requiresPostFilter = false
    let impossibleRange = false

    for (const spec of metricSpecs) {
      const minRaw = params[spec.min]
      const maxRaw = params[spec.max]
      if (minRaw === undefined && maxRaw === undefined) continue

      if (!spec.enabled) {
        requiresPostFilter = true
        continue
      }

      if (spec.exact) {
        if (isImpossibleExactRange(minRaw, maxRaw)) {
          query.whereRaw('1 = 0')
          impossibleRange = true
          continue
        }
        applyExactRangeToQuery(query, col(spec.db), minRaw, maxRaw)
        continue
      }

      const minValue = minRaw !== undefined ? Number(minRaw) : undefined
      const maxValue = maxRaw !== undefined ? Number(maxRaw) : undefined
      if (minValue !== undefined && maxValue !== undefined && minValue === maxValue) {
        query.whereRaw('1 = 0')
        impossibleRange = true
        continue
      }
      if (minValue !== undefined) query.where(col(spec.db), '>=', minValue)
      if (maxValue !== undefined) query.where(col(spec.db), '<', maxValue)
    }

    return { requiresPostFilter, impossibleRange }
  }

  private applyMetricFiltersInMemory(participants: any[], params: any): any[] {
    const specs = [
      { min: 'min_participants', max: 'max_participants', field: 'participants' },
      { min: 'min_participants_ecosystem', max: 'max_participants_ecosystem', field: 'participants_ecosystem' },
      {
        min: 'min_participants_issuer_grantor',
        max: 'max_participants_issuer_grantor',
        field: 'participants_issuer_grantor',
      },
      { min: 'min_participants_issuer', max: 'max_participants_issuer', field: 'participants_issuer' },
      {
        min: 'min_participants_verifier_grantor',
        max: 'max_participants_verifier_grantor',
        field: 'participants_verifier_grantor',
      },
      { min: 'min_participants_verifier', max: 'max_participants_verifier', field: 'participants_verifier' },
      { min: 'min_participants_holder', max: 'max_participants_holder', field: 'participants_holder' },
      { min: 'min_weight', max: 'max_weight', field: 'weight', exact: true },
      { min: 'min_issued', max: 'max_issued', field: 'issued', exact: true },
      { min: 'min_verified', max: 'max_verified', field: 'verified', exact: true },
      { min: 'min_ecosystem_slash_events', max: 'max_ecosystem_slash_events', field: 'ecosystem_slash_events' },
      { min: 'min_network_slash_events', max: 'max_network_slash_events', field: 'network_slash_events' },
    ]

    let results = participants
    for (const spec of specs) {
      const minRaw = params[spec.min]
      const maxRaw = params[spec.max]
      if (minRaw === undefined && maxRaw === undefined) continue

      if (spec.exact) {
        if (isImpossibleExactRange(minRaw, maxRaw)) return []
        results = filterRowsByExactRange(results, minRaw, maxRaw, (participant) => participant?.[spec.field])
        continue
      }

      const minValue = minRaw !== undefined ? Number(minRaw) : undefined
      const maxValue = maxRaw !== undefined ? Number(maxRaw) : undefined
      if (minValue !== undefined && maxValue !== undefined && minValue === maxValue) {
        return []
      }
      if (minValue !== undefined) {
        results = results.filter((participant) => Number(participant?.[spec.field] || 0) >= minValue)
      }
      if (maxValue !== undefined) {
        results = results.filter((participant) => Number(participant?.[spec.field] || 0) < maxValue)
      }
    }
    return results
  }

  private applyParticipantStateFilterToQuery(
    query: any,
    participantStateRaw: any,
    nowIso: string,
    tablePrefix?: string
  ): { pushedDown: boolean } {
    if (!participantStateRaw) return { pushedDown: false }

    const participantState = String(participantStateRaw).toUpperCase()
    const col = (name: string) => (tablePrefix ? `${tablePrefix}.${name}` : name)
    const baseNotRepaidSlashed = (qb: any) => {
      qb.whereNull(col('repaid')).whereNull(col('slashed'))
    }
    const notRevokedAsOfNow = (qb: any) => {
      qb.whereNull(col('revoked')).orWhere(col('revoked'), '>=', nowIso)
    }

    if (participantState === 'REPAID') {
      query.whereNotNull(col('repaid'))
      return { pushedDown: true }
    }

    if (participantState === 'SLASHED') {
      query.whereNull(col('repaid')).whereNotNull(col('slashed'))
      return { pushedDown: true }
    }

    if (participantState === 'REVOKED') {
      query.where((qb: any) => {
        baseNotRepaidSlashed(qb)
        qb.whereNotNull(col('revoked')).andWhere(col('revoked'), '<', nowIso)
      })
      return { pushedDown: true }
    }

    if (participantState === 'EXPIRED') {
      query.where((qb: any) => {
        baseNotRepaidSlashed(qb)
        qb.where(notRevokedAsOfNow)
        qb.whereNotNull(col('effective_until')).andWhere(col('effective_until'), '<', nowIso)
      })
      return { pushedDown: true }
    }

    if (participantState === 'ACTIVE') {
      query.where((qb: any) => {
        baseNotRepaidSlashed(qb)
        qb.where(notRevokedAsOfNow)
        qb.where((q: any) => q.whereNull(col('effective_until')).orWhere(col('effective_until'), '>=', nowIso))
        qb.whereNotNull(col('effective_from')).andWhere(col('effective_from'), '<=', nowIso)
      })
      return { pushedDown: true }
    }

    if (participantState === 'FUTURE') {
      query.where((qb: any) => {
        baseNotRepaidSlashed(qb)
        qb.where(notRevokedAsOfNow)
        qb.where((q: any) => q.whereNull(col('effective_until')).orWhere(col('effective_until'), '>=', nowIso))
        qb.whereNotNull(col('effective_from')).andWhere(col('effective_from'), '>', nowIso)
      })
      return { pushedDown: true }
    }

    if (participantState === 'INACTIVE') {
      query.where((qb: any) => {
        baseNotRepaidSlashed(qb)
        qb.where(notRevokedAsOfNow)
        qb.where((q: any) => q.whereNull(col('effective_until')).orWhere(col('effective_until'), '>=', nowIso))
        qb.whereNull(col('effective_from'))
      })
      return { pushedDown: true }
    }

    return { pushedDown: false }
  }

  private async batchEnrichParticipants(
    participants: any[],
    blockHeight: number | undefined,
    now: Date,
    batchSize: number = 50,
    options?: {
      lightweightDerivedStats?: boolean
      schemaModesById?: Map<number, SchemaData>
      validatorParticipantStateById?: Map<number, ParticipantState | null>
      moduleParams?: any
    }
  ): Promise<any[]> {
    if (participants.length === 0) return []

    const requiresSchemaModes = (participantType: string | undefined): boolean => {
      if (!participantType) return false
      return (
        participantType === 'ISSUER_GRANTOR' ||
        participantType === 'ISSUER' ||
        participantType === 'VERIFIER_GRANTOR' ||
        participantType === 'VERIFIER'
      )
    }

    const schemaIds = Array.from(
      new Set(
        participants
          .filter((participant) => requiresSchemaModes(String(participant?.role || '').toUpperCase()))
          .map((participant) => Number(participant.schema_id))
          .filter((schemaId) => Number.isFinite(schemaId) && schemaId > 0)
      )
    )

    const locallyKnownParticipantStateById = new Map<number, ParticipantState>()
    for (const participant of participants) {
      const participantId = Number(participant?.id)
      if (!Number.isFinite(participantId) || participantId <= 0) continue
      locallyKnownParticipantStateById.set(
        participantId,
        calculateParticipantState(
          {
            repaid: participant.repaid,
            slashed: participant.slashed,
            revoked: participant.revoked,
            effective_from: participant.effective_from,
            effective_until: participant.effective_until,
            role: participant.role,
            op_state: participant.op_state,
            op_exp: participant.op_exp,
            validator_participant_id: participant.validator_participant_id,
          },
          now
        )
      )
    }

    const validatorParticipantIds = Array.from(
      new Set(
        participants
          .filter((participant) => String(participant?.role || '').toUpperCase() !== 'ECOSYSTEM')
          .map((participant) => Number(participant.validator_participant_id))
          .filter((validatorParticipantId) => Number.isFinite(validatorParticipantId) && validatorParticipantId > 0)
      )
    )
    const missingValidatorParticipantIds = validatorParticipantIds.filter(
      (validatorParticipantId) => !locallyKnownParticipantStateById.has(validatorParticipantId)
    )

    const shouldLoadModuleParams = participants.some(
      (participant) => participant?.effective_until !== null && participant?.effective_until !== undefined
    )

    const [schemaModesById, validatorParticipantStateById, moduleParams] = await Promise.all([
      options?.schemaModesById ?? this.getSchemaModesBatch(schemaIds, blockHeight),
      options?.validatorParticipantStateById ??
        this.getValidatorParticipantStateMap(missingValidatorParticipantIds, blockHeight, now),
      options?.moduleParams !== undefined || !shouldLoadModuleParams
        ? Promise.resolve(options?.moduleParams)
        : this.getParticipantModuleParams(blockHeight).catch(() => undefined),
    ])

    const mergedValidatorParticipantStateById = new Map<number, ParticipantState | null>()
    for (const [participantId, state] of locallyKnownParticipantStateById.entries()) {
      mergedValidatorParticipantStateById.set(participantId, state)
    }
    for (const [participantId, state] of validatorParticipantStateById.entries()) {
      mergedValidatorParticipantStateById.set(participantId, state)
    }

    const mergedOptions = {
      ...options,
      schemaModesById,
      validatorParticipantStateById: mergedValidatorParticipantStateById,
      moduleParams,
    }

    const results: any[] = []
    for (let i = 0; i < participants.length; i += batchSize) {
      const batch = participants.slice(i, i + batchSize)
      const batchResults = await Promise.all(
        batch.map((participant) =>
          this.enrichParticipantWithStateAndActions(participant, blockHeight, now, mergedOptions)
        )
      )
      results.push(...batchResults)
    }
    return results
  }

  private async getSchemaModesBatch(schemaIds: number[], blockHeight?: number): Promise<Map<number, SchemaData>> {
    const modeMap = new Map<number, SchemaData>()
    if (schemaIds.length === 0) return modeMap

    if (typeof blockHeight === 'number') {
      const rankedSchemas = knex('credential_schema_history as csh')
        .select(
          'csh.credential_schema_id',
          'csh.issuer_onboarding_mode',
          'csh.verifier_onboarding_mode',
          knex.raw(
            `ROW_NUMBER() OVER (PARTITION BY csh.credential_schema_id ORDER BY csh.height DESC, csh.created_at DESC) as rn`
          )
        )
        .whereIn('csh.credential_schema_id', schemaIds)
        .where('csh.height', '<=', blockHeight)
        .as('ranked')

      const historicalModes = await knex
        .from(rankedSchemas)
        .select('credential_schema_id', 'issuer_onboarding_mode', 'verifier_onboarding_mode')
        .where('rn', 1)

      for (const row of historicalModes) {
        const schemaId = Number(row.credential_schema_id)
        modeMap.set(schemaId, {
          issuer_onboarding_mode: (row as any).issuer_onboarding_mode || null,
          verifier_onboarding_mode: (row as any).verifier_onboarding_mode || null,
        })
      }

      const missingSchemaIds = schemaIds.filter((schemaId) => !modeMap.has(schemaId))
      if (missingSchemaIds.length > 0) {
        const fallbackRows = await knex('credential_schemas')
          .whereIn('id', missingSchemaIds)
          .select('id', 'issuer_onboarding_mode', 'verifier_onboarding_mode')

        for (const row of fallbackRows) {
          const schemaId = Number(row.id)
          modeMap.set(schemaId, {
            issuer_onboarding_mode: (row as any).issuer_onboarding_mode || null,
            verifier_onboarding_mode: (row as any).verifier_onboarding_mode || null,
          })
        }
      }

      return modeMap
    }

    const schemaRows = await knex('credential_schemas')
      .whereIn('id', schemaIds)
      .select('id', 'issuer_onboarding_mode', 'verifier_onboarding_mode')

    for (const row of schemaRows) {
      const schemaId = Number(row.id)
      modeMap.set(schemaId, {
        issuer_onboarding_mode: (row as any).issuer_onboarding_mode || null,
        verifier_onboarding_mode: (row as any).verifier_onboarding_mode || null,
      })
    }

    return modeMap
  }

  private async getValidatorParticipantStateMap(
    validatorParticipantIds: number[],
    blockHeight: number | undefined,
    now: Date
  ): Promise<Map<number, ParticipantState | null>> {
    const stateMap = new Map<number, ParticipantState | null>()
    if (validatorParticipantIds.length === 0) return stateMap

    if (typeof blockHeight === 'number') {
      const rankedValidators = knex('participant_history as ph')
        .select(
          'ph.participant_id',
          'ph.repaid',
          'ph.slashed',
          'ph.revoked',
          'ph.effective_from',
          'ph.effective_until',
          'ph.role',
          'ph.op_state',
          'ph.op_exp',
          'ph.validator_participant_id',
          knex.raw(
            `ROW_NUMBER() OVER (PARTITION BY ph.participant_id ORDER BY ph.height DESC, ph.created_at DESC, ph.id DESC) as rn`
          )
        )
        .whereIn('ph.participant_id', validatorParticipantIds)
        .where('ph.height', '<=', blockHeight)
        .as('ranked')

      const rows = await knex
        .from(rankedValidators)
        .select(
          'participant_id',
          'repaid',
          'slashed',
          'revoked',
          'effective_from',
          'effective_until',
          'role',
          'op_state',
          'op_exp',
          'validator_participant_id'
        )
        .where('rn', 1)

      for (const row of rows) {
        const participantId = Number(row.participant_id)
        stateMap.set(
          participantId,
          calculateParticipantState(
            {
              repaid: row.repaid,
              slashed: row.slashed,
              revoked: row.revoked,
              effective_from: row.effective_from,
              effective_until: row.effective_until,
              role: row.role,
              op_state: row.op_state,
              op_exp: row.op_exp,
              validator_participant_id: row.validator_participant_id,
            },
            now
          )
        )
      }
      return stateMap
    }

    const rows = await knex('participants')
      .whereIn('id', validatorParticipantIds)
      .select(
        'id',
        'repaid',
        'slashed',
        'revoked',
        'effective_from',
        'effective_until',
        'role',
        'op_state',
        'op_exp',
        'validator_participant_id'
      )

    for (const row of rows) {
      const participantId = Number(row.id)
      stateMap.set(
        participantId,
        calculateParticipantState(
          {
            repaid: row.repaid,
            slashed: row.slashed,
            revoked: row.revoked,
            effective_from: row.effective_from,
            effective_until: row.effective_until,
            role: row.role,
            op_state: row.op_state,
            op_exp: row.op_exp,
            validator_participant_id: row.validator_participant_id,
          },
          now
        )
      )
    }

    return stateMap
  }

  private normalizeOpStateForResponse(value: unknown): string | null {
    if (value === null || value === undefined) return null
    if (typeof value === 'string') {
      const upper = value.toUpperCase()
      if (upper === 'PENDING' || upper === 'VALIDATED' || upper === 'TERMINATED') return upper
      return null
    }
    const n = Number(value)
    if (n === 1) return 'PENDING'
    if (n === 2) return 'VALIDATED'
    if (n === 3 || n === 4) return 'TERMINATED'
    return null
  }

  private normalizeDenomAmountArray(value: unknown): unknown {
    if (value === null) return null
    if (Array.isArray(value)) return value
    return []
  }

  private normalizeParticipantRow(participant: any): any {
    let normalized: any = {
      ...participant,
      id: Number(participant.id),
      schema_id: Number(participant.schema_id),
      role:
        participant.role !== undefined && participant.role !== null
          ? mapParticipantType(participant.role)
          : participant.role,
      op_state: this.normalizeOpStateForResponse(participant.op_state),
      validator_participant_id: participant.validator_participant_id
        ? Number(participant.validator_participant_id)
        : null,
      validation_fees: participant.validation_fees != null ? Number(participant.validation_fees) : 0,
      issuance_fees: participant.issuance_fees != null ? Number(participant.issuance_fees) : 0,
      verification_fees: participant.verification_fees != null ? Number(participant.verification_fees) : 0,
      deposit: participant.deposit != null ? Number(participant.deposit) : 0,
      slashed_deposit: participant.slashed_deposit != null ? Number(participant.slashed_deposit) : 0,
      repaid_deposit: participant.repaid_deposit != null ? Number(participant.repaid_deposit) : 0,
      op_current_fees: participant.op_current_fees != null ? Number(participant.op_current_fees) : 0,
      op_current_deposit: participant.op_current_deposit != null ? Number(participant.op_current_deposit) : 0,
      op_validator_deposit: participant.op_validator_deposit != null ? Number(participant.op_validator_deposit) : 0,
      weight: participant.weight != null ? String(participant.weight) : '0',
      issued: participant.issued != null ? Number(participant.issued) : 0,
      verified: participant.verified != null ? Number(participant.verified) : 0,
      participants: participant.participants != null ? Number(participant.participants) : 0,
      participants_ecosystem:
        participant.participants_ecosystem != null ? Number(participant.participants_ecosystem) : 0,
      participants_issuer_grantor:
        participant.participants_issuer_grantor != null ? Number(participant.participants_issuer_grantor) : 0,
      participants_issuer: participant.participants_issuer != null ? Number(participant.participants_issuer) : 0,
      participants_verifier_grantor:
        participant.participants_verifier_grantor != null ? Number(participant.participants_verifier_grantor) : 0,
      participants_verifier: participant.participants_verifier != null ? Number(participant.participants_verifier) : 0,
      participants_holder: participant.participants_holder != null ? Number(participant.participants_holder) : 0,
      ecosystem_slash_events:
        participant.ecosystem_slash_events != null ? Number(participant.ecosystem_slash_events) : 0,
      ecosystem_slashed_amount:
        participant.ecosystem_slashed_amount != null ? Number(participant.ecosystem_slashed_amount) : 0,
      ecosystem_slashed_amount_repaid:
        participant.ecosystem_slashed_amount_repaid != null ? Number(participant.ecosystem_slashed_amount_repaid) : 0,
      network_slash_events: participant.network_slash_events != null ? Number(participant.network_slash_events) : 0,
      network_slashed_amount:
        participant.network_slashed_amount != null ? Number(participant.network_slashed_amount) : 0,
      network_slashed_amount_repaid:
        participant.network_slashed_amount_repaid != null ? Number(participant.network_slashed_amount_repaid) : 0,
      issuance_fee_discount: participant.issuance_fee_discount != null ? Number(participant.issuance_fee_discount) : 0,
      verification_fee_discount:
        participant.verification_fee_discount != null ? Number(participant.verification_fee_discount) : 0,
    }

    normalized = normalizeParticipantEmptyStringsToNull(normalized)

    return mapParticipantApiFields(normalized as Record<string, unknown>) as any
  }

  private async getParticipantsByIdsMap(participantIds: number[], blockHeight?: number): Promise<Map<number, any>> {
    const idMap = new Map<number, any>()
    if (participantIds.length === 0) return idMap
    const uniqueIds = Array.from(
      new Set(participantIds.map((id) => Number(id)).filter((id) => Number.isFinite(id) && id > 0))
    )
    if (uniqueIds.length === 0) return idMap

    if (typeof blockHeight === 'number') {
      let rows: any[] = []
      if (IS_PG_CLIENT) {
        rows = await knex('participant_history as ph')
          .distinctOn('ph.participant_id')
          .select([
            'ph.participant_id as id',
            'ph.schema_id',
            'ph.corporation_id',
            'ph.did',
            'ph.validator_participant_id',
            'ph.role',
            'ph.op_state',
            'ph.revoked',
            'ph.slashed',
            'ph.repaid',
            'ph.effective_from',
            'ph.effective_until',
            'ph.validation_fees',
            'ph.issuance_fees',
            'ph.verification_fees',
            'ph.deposit',
            'ph.slashed_deposit',
            'ph.repaid_deposit',
            'ph.op_last_state_change',
            'ph.op_current_fees',
            'ph.op_current_deposit',
            'ph.op_summary_digest',
            'ph.op_exp',
            'ph.op_validator_deposit',
            'ph.vs_operator',
            'ph.adjusted',
            'ph.vs_operator_authz_enabled',
            'ph.vs_operator_authz_spend_limit',
            'ph.vs_operator_authz_with_feegrant',
            'ph.vs_operator_authz_fee_spend_limit',
            'ph.vs_operator_authz_spend_period',
            'ph.issuance_fee_discount',
            'ph.verification_fee_discount',
            'ph.weight',
            'ph.issued',
            'ph.verified',
            'ph.participants',
            'ph.participants_ecosystem',
            'ph.participants_issuer_grantor',
            'ph.participants_issuer',
            'ph.participants_verifier_grantor',
            'ph.participants_verifier',
            'ph.participants_holder',
            'ph.ecosystem_slash_events',
            'ph.ecosystem_slashed_amount',
            'ph.ecosystem_slashed_amount_repaid',
            'ph.network_slash_events',
            'ph.network_slashed_amount',
            'ph.network_slashed_amount_repaid',
            'ph.created',
            'ph.modified',
          ])
          .whereIn('ph.participant_id', uniqueIds)
          .where('ph.height', '<=', blockHeight)
          .orderBy('ph.participant_id', 'asc')
          .orderBy('ph.height', 'desc')
          .orderBy('ph.created_at', 'desc')
          .orderBy('ph.id', 'desc')
      } else {
        const ranked = knex('participant_history as ph')
          .select([
            'ph.participant_id as id',
            'ph.schema_id',
            'ph.corporation_id',
            'ph.did',
            'ph.validator_participant_id',
            'ph.role',
            'ph.op_state',
            'ph.revoked',
            'ph.slashed',
            'ph.repaid',
            'ph.effective_from',
            'ph.effective_until',
            'ph.validation_fees',
            'ph.issuance_fees',
            'ph.verification_fees',
            'ph.deposit',
            'ph.slashed_deposit',
            'ph.repaid_deposit',
            'ph.op_last_state_change',
            'ph.op_current_fees',
            'ph.op_current_deposit',
            'ph.op_summary_digest',
            'ph.op_exp',
            'ph.op_validator_deposit',
            'ph.vs_operator',
            'ph.adjusted',
            'ph.vs_operator_authz_enabled',
            'ph.vs_operator_authz_spend_limit',
            'ph.vs_operator_authz_with_feegrant',
            'ph.vs_operator_authz_fee_spend_limit',
            'ph.vs_operator_authz_spend_period',
            'ph.issuance_fee_discount',
            'ph.verification_fee_discount',
            'ph.weight',
            'ph.issued',
            'ph.verified',
            'ph.participants',
            'ph.participants_ecosystem',
            'ph.participants_issuer_grantor',
            'ph.participants_issuer',
            'ph.participants_verifier_grantor',
            'ph.participants_verifier',
            'ph.participants_holder',
            'ph.ecosystem_slash_events',
            'ph.ecosystem_slashed_amount',
            'ph.ecosystem_slashed_amount_repaid',
            'ph.network_slash_events',
            'ph.network_slashed_amount',
            'ph.network_slashed_amount_repaid',
            'ph.created',
            'ph.modified',
            knex.raw(
              'ROW_NUMBER() OVER (PARTITION BY ph.participant_id ORDER BY ph.height DESC, ph.created_at DESC, ph.id DESC) as rn'
            ),
          ])
          .whereIn('ph.participant_id', uniqueIds)
          .where('ph.height', '<=', blockHeight)
          .as('ranked')

        rows = await knex.from(ranked).select('*').where('rn', 1)
      }

      for (const row of rows) {
        const normalized = this.normalizeParticipantRow(row)
        idMap.set(Number(normalized.id), normalized)
      }
      return idMap
    }

    const rows = await knex('participants')
      .select([
        'id',
        'schema_id',
        'corporation_id',
        'did',
        'validator_participant_id',
        'role',
        'op_state',
        'revoked',
        'slashed',
        'repaid',
        'effective_from',
        'effective_until',
        'validation_fees',
        'issuance_fees',
        'verification_fees',
        'deposit',
        'slashed_deposit',
        'repaid_deposit',
        'op_last_state_change',
        'op_current_fees',
        'op_current_deposit',
        'op_summary_digest',
        'op_exp',
        'op_validator_deposit',
        'vs_operator',
        'adjusted',
        'vs_operator_authz_enabled',
        'vs_operator_authz_spend_limit',
        'vs_operator_authz_with_feegrant',
        'vs_operator_authz_fee_spend_limit',
        'vs_operator_authz_spend_period',
        'issuance_fee_discount',
        'verification_fee_discount',
        'weight',
        'issued',
        'verified',
        'participants',
        'participants_ecosystem',
        'participants_issuer_grantor',
        'participants_issuer',
        'participants_verifier_grantor',
        'participants_verifier',
        'participants_holder',
        'ecosystem_slash_events',
        'ecosystem_slashed_amount',
        'ecosystem_slashed_amount_repaid',
        'network_slash_events',
        'network_slashed_amount',
        'network_slashed_amount_repaid',
        'created',
        'modified',
      ])
      .whereIn('id', uniqueIds)
    for (const row of rows) {
      const normalized = this.normalizeParticipantRow(row)
      idMap.set(Number(normalized.id), normalized)
    }
    return idMap
  }

  private async calculateExpireSoon(
    participant: any,
    now: Date,
    blockHeight?: number,
    preloadedModuleParams?: any
  ): Promise<boolean | null> {
    const participantState = calculateParticipantState(
      {
        repaid: participant.repaid,
        slashed: participant.slashed,
        revoked: participant.revoked,
        effective_from: participant.effective_from,
        effective_until: participant.effective_until,
        role: participant.role,
        op_state: participant.op_state,
        op_exp: participant.op_exp,
        validator_participant_id: participant.validator_participant_id,
      },
      now
    )
    if (participantState !== 'ACTIVE') {
      return null
    }
    if (!participant.effective_until) {
      return false
    }
    let nDaysBefore = 0
    try {
      const moduleParams = preloadedModuleParams ?? (await this.getParticipantModuleParams(blockHeight))
      if (moduleParams?.params) {
        nDaysBefore = moduleParams.params.PARTICIPANT_SET_EXPIRE_SOON_N_DAYS_BEFORE || 0
      }
    } catch (error) {
      this.logger.warn(`Failed to get PARTICIPANT module params:`, error)
      nDaysBefore = 0
    }
    const expirationCheckDate = new Date(now)
    expirationCheckDate.setDate(expirationCheckDate.getDate() + nDaysBefore)
    const effectiveUntil = new Date(participant.effective_until)
    return expirationCheckDate > effectiveUntil
  }

  private async enrichParticipantWithStateAndActions(
    participant: any,
    blockHeight?: number,
    now: Date = new Date(),
    options?: {
      lightweightDerivedStats?: boolean
      schemaModesById?: Map<number, SchemaData>
      validatorParticipantStateById?: Map<number, ParticipantState | null>
      moduleParams?: any
    }
  ): Promise<any> {
    const schemaId = Number(participant.schema_id)
    const schemaFromBatch = options?.schemaModesById?.get(schemaId)
    const schema =
      schemaFromBatch ||
      (options?.schemaModesById !== undefined ? {} : await this.getSchemaModes(schemaId, blockHeight))

    let validatorParticipantState: ParticipantState | null = null
    const validatorParticipantStateById = options?.validatorParticipantStateById
    if (participant.validator_participant_id) {
      const validatorParticipantId = Number(participant.validator_participant_id)
      validatorParticipantState = validatorParticipantStateById?.has(validatorParticipantId)
        ? validatorParticipantStateById.get(validatorParticipantId) || null
        : null
    }

    const participantState = calculateParticipantState(
      {
        repaid: participant.repaid,
        slashed: participant.slashed,
        revoked: participant.revoked,
        effective_from: participant.effective_from,
        effective_until: participant.effective_until,
        role: participant.role,
        op_state: participant.op_state,
        op_exp: participant.op_exp,
        validator_participant_id: participant.validator_participant_id,
      },
      now
    )

    const corporationActions = calculateCorporationAvailableActions(
      {
        repaid: participant.repaid,
        slashed: participant.slashed,
        revoked: participant.revoked,
        effective_from: participant.effective_from,
        effective_until: participant.effective_until,
        role: participant.role,
        op_state: participant.op_state,
        op_exp: participant.op_exp,
        validator_participant_id: participant.validator_participant_id,
      },
      schema,
      validatorParticipantState || undefined,
      now
    )

    const validatorActions = calculateValidatorAvailableActions(
      {
        repaid: participant.repaid,
        slashed: participant.slashed,
        revoked: participant.revoked,
        effective_from: participant.effective_from,
        effective_until: participant.effective_until,
        role: participant.role,
        op_state: participant.op_state,
        op_exp: participant.op_exp,
        validator_participant_id: participant.validator_participant_id,
      },
      schema,
      now
    )

    const weight = String(participant.weight ?? '0')
    const statistics = {
      issued: typeof participant.issued === 'number' ? participant.issued : Number(participant.issued || 0),
      verified: typeof participant.verified === 'number' ? participant.verified : Number(participant.verified || 0),
    }
    const participantsByRole = {
      participants_ecosystem:
        typeof participant.participants_ecosystem === 'number'
          ? participant.participants_ecosystem
          : Number(participant.participants_ecosystem || 0),
      participants_issuer_grantor:
        typeof participant.participants_issuer_grantor === 'number'
          ? participant.participants_issuer_grantor
          : Number(participant.participants_issuer_grantor || 0),
      participants_issuer:
        typeof participant.participants_issuer === 'number'
          ? participant.participants_issuer
          : Number(participant.participants_issuer || 0),
      participants_verifier_grantor:
        typeof participant.participants_verifier_grantor === 'number'
          ? participant.participants_verifier_grantor
          : Number(participant.participants_verifier_grantor || 0),
      participants_verifier:
        typeof participant.participants_verifier === 'number'
          ? participant.participants_verifier
          : Number(participant.participants_verifier || 0),
      participants_holder:
        typeof participant.participants_holder === 'number'
          ? participant.participants_holder
          : Number(participant.participants_holder || 0),
    }
    const participantsSum =
      participantsByRole.participants_ecosystem +
      participantsByRole.participants_issuer_grantor +
      participantsByRole.participants_issuer +
      participantsByRole.participants_verifier_grantor +
      participantsByRole.participants_verifier +
      participantsByRole.participants_holder
    const participants =
      participant.participants != null && participant.participants !== ''
        ? Number(participant.participants)
        : participantsSum
    const slashStats = {
      ecosystem_slash_events:
        typeof participant.ecosystem_slash_events === 'number'
          ? participant.ecosystem_slash_events
          : Number(participant.ecosystem_slash_events || 0),
      ecosystem_slashed_amount:
        typeof participant.ecosystem_slashed_amount === 'number'
          ? participant.ecosystem_slashed_amount
          : Number(participant.ecosystem_slashed_amount || 0),
      ecosystem_slashed_amount_repaid:
        typeof participant.ecosystem_slashed_amount_repaid === 'number'
          ? participant.ecosystem_slashed_amount_repaid
          : Number(participant.ecosystem_slashed_amount_repaid || 0),
      network_slash_events:
        typeof participant.network_slash_events === 'number'
          ? participant.network_slash_events
          : Number(participant.network_slash_events || 0),
      network_slashed_amount:
        typeof participant.network_slashed_amount === 'number'
          ? participant.network_slashed_amount
          : Number(participant.network_slashed_amount || 0),
      network_slashed_amount_repaid:
        typeof participant.network_slashed_amount_repaid === 'number'
          ? participant.network_slashed_amount_repaid
          : Number(participant.network_slashed_amount_repaid || 0),
    }

    const expireSoon = await this.calculateExpireSoon(participant, now, blockHeight, options?.moduleParams).catch(
      (err: any) => {
        this.logger.warn(`Failed to calculate expire_soon for participant ${participant.id}:`, err?.message || err)
        return null
      }
    )

    const enriched: any = {
      ...participant,
      participant_state: participantState,
      corporation_available_actions: mapParticipantActionsToVprMessages(corporationActions),
      validator_available_actions: mapParticipantActionsToVprMessages(validatorActions),
      id: Number(participant.id),
      schema_id: Number(participant.schema_id),
      validator_participant_id: participant.validator_participant_id
        ? Number(participant.validator_participant_id)
        : null,
      validation_fees: participant.validation_fees != null ? Number(participant.validation_fees) : 0,
      issuance_fees: participant.issuance_fees != null ? Number(participant.issuance_fees) : 0,
      verification_fees: participant.verification_fees != null ? Number(participant.verification_fees) : 0,
      deposit: participant.deposit != null ? Number(participant.deposit) : 0,
      slashed_deposit: participant.slashed_deposit != null ? Number(participant.slashed_deposit) : 0,
      repaid_deposit: participant.repaid_deposit != null ? Number(participant.repaid_deposit) : 0,
      op_current_fees: participant.op_current_fees != null ? Number(participant.op_current_fees) : 0,
      op_current_deposit: participant.op_current_deposit != null ? Number(participant.op_current_deposit) : 0,
      op_validator_deposit: participant.op_validator_deposit != null ? Number(participant.op_validator_deposit) : 0,
      weight: weight,
      issued: statistics.issued,
      verified: statistics.verified,
      participants: participants,
      participants_ecosystem: participantsByRole.participants_ecosystem,
      participants_issuer_grantor: participantsByRole.participants_issuer_grantor,
      participants_issuer: participantsByRole.participants_issuer,
      participants_verifier_grantor: participantsByRole.participants_verifier_grantor,
      participants_verifier: participantsByRole.participants_verifier,
      participants_holder: participantsByRole.participants_holder,
      ecosystem_slash_events: slashStats.ecosystem_slash_events,
      ecosystem_slashed_amount: slashStats.ecosystem_slashed_amount,
      ecosystem_slashed_amount_repaid: slashStats.ecosystem_slashed_amount_repaid,
      network_slash_events: slashStats.network_slash_events,
      network_slashed_amount: slashStats.network_slashed_amount,
      network_slashed_amount_repaid: slashStats.network_slashed_amount_repaid,
      expire_soon: expireSoon,
    }
    const normalized = normalizeParticipantEmptyStringsToNull(enriched) as Record<string, unknown>
    return mapParticipantApiFields(normalized) as any
  }

  /**
   * List Participants [MOD-PP-QRY-1]
   */
  @Action({
    rest: 'GET list',
    params: {
      schema_id: { type: 'number', integer: true, optional: true },
      corporation_id: { type: 'number', integer: true, optional: true },
      corporation: { type: 'string', optional: true },
      did: { type: 'string', optional: true },
      participant_id: { type: 'number', integer: true, optional: true },
      validator_participant_id: { type: 'number', integer: true, optional: true },
      participant_state: { type: 'string', optional: true },
      role: { type: 'string', optional: true },
      only_valid: { type: 'any', optional: true },
      only_slashed: { type: 'any', optional: true },
      only_repaid: { type: 'any', optional: true },
      modified_after: { type: 'string', optional: true },
      op_state: { type: 'string', optional: true },
      response_max_size: { type: 'number', optional: true, default: 64 },
      when: { type: 'string', optional: true },
      sort: { type: 'string', optional: true },
      min_participants: { type: 'number', integer: true, optional: true },
      max_participants: { type: 'number', integer: true, optional: true },
      min_participants_ecosystem: { type: 'number', integer: true, optional: true },
      max_participants_ecosystem: { type: 'number', integer: true, optional: true },
      min_participants_issuer_grantor: { type: 'number', integer: true, optional: true },
      max_participants_issuer_grantor: { type: 'number', integer: true, optional: true },
      min_participants_issuer: { type: 'number', integer: true, optional: true },
      max_participants_issuer: { type: 'number', integer: true, optional: true },
      min_participants_verifier_grantor: { type: 'number', integer: true, optional: true },
      max_participants_verifier_grantor: { type: 'number', integer: true, optional: true },
      min_participants_verifier: { type: 'number', integer: true, optional: true },
      max_participants_verifier: { type: 'number', integer: true, optional: true },
      min_participants_holder: { type: 'number', integer: true, optional: true },
      max_participants_holder: { type: 'number', integer: true, optional: true },
      min_weight: { type: 'string', pattern: INTEGER_PARAM_PATTERN, optional: true },
      max_weight: { type: 'string', pattern: INTEGER_PARAM_PATTERN, optional: true },
      min_issued: { type: 'string', pattern: INTEGER_PARAM_PATTERN, optional: true },
      max_issued: { type: 'string', pattern: INTEGER_PARAM_PATTERN, optional: true },
      min_verified: { type: 'string', pattern: INTEGER_PARAM_PATTERN, optional: true },
      max_verified: { type: 'string', pattern: INTEGER_PARAM_PATTERN, optional: true },
      min_ecosystem_slash_events: { type: 'number', integer: true, optional: true },
      max_ecosystem_slash_events: { type: 'number', integer: true, optional: true },
      min_network_slash_events: { type: 'number', integer: true, optional: true },
      max_network_slash_events: { type: 'number', integer: true, optional: true },
      trust_data: { type: 'string', optional: true },
    },
  })
  async listParticipants(ctx: Context<any>) {
    const requestStartedMs = Date.now()
    const perfMarks: Record<string, number> = {}
    let perfMeta: Record<string, any> = {}

    try {
      const p = ctx.params
      const corporationValidation = validateParticipantParam(p.corporation, 'corporation')
      if (!corporationValidation.valid) {
        return ApiResponder.error(ctx, corporationValidation.error, 400)
      }
      const corporationFilter = corporationValidation.value
      // VPR v4: filter by canonical corporation_id. Accept it directly, or resolve
      // the legacy account-address filter to the corresponding corporation_id.
      let corporationIdFilter: number | undefined
      if (p.corporation_id != null) {
        corporationIdFilter = Number(p.corporation_id)
      } else if (corporationFilter) {
        const resolvedCorpId = await resolveCorporationIdByAddress(corporationFilter)
        if (resolvedCorpId === null) {
          return ApiResponder.success(ctx, { participants: [] }, 200)
        }
        corporationIdFilter = resolvedCorpId
      }

      const typeOpValidation = this.normalizeAndValidateTypeAndOpState(p)
      if (!typeOpValidation.ok) {
        return ApiResponder.error(ctx, typeOpValidation.message || 'Invalid type/op_state', 400)
      }
      const normalizedParams = {
        ...p,
        role: typeOpValidation.role,
        op_state: typeOpValidation.op_state,
      }
      const trustDataRaw = (normalizedParams as any).trust_data
      const trustDataModeParsed = parseTrustDataMode(trustDataRaw)
      if (!trustDataModeParsed.ok) {
        return ApiResponder.error(ctx, trustDataModeParsed.message, 400)
      }
      const trustDataMode = trustDataModeParsed.mode

      const blockHeight = getBlockHeight(ctx)
      const useHistoryQuery = this.shouldUseHistoryQuery(ctx, blockHeight)
      const now = new Date().toISOString()
      const limit = Math.min(Math.max(normalizedParams.response_max_size || 64, 1), 1024)

      perfMeta = {
        did: normalizedParams.did ? '[set]' : undefined,
        role: normalizedParams.role,
        schema_id: normalizedParams.schema_id,
        limit,
        blockHeight: useHistoryQuery ? blockHeight : undefined,
      }

      const sortParsed = parseIdSortDirection(normalizedParams.sort)
      if (!sortParsed.ok) {
        return ApiResponder.error(ctx, sortParsed.message, 400)
      }
      const sortDirection = sortParsed.direction

      const onlyValid = normalizedParams.only_valid === 'true' || normalizedParams.only_valid === true
      const onlySlashed = normalizedParams.only_slashed === 'true' || normalizedParams.only_slashed === true
      const onlyRepaid = normalizedParams.only_repaid === 'true' || normalizedParams.only_repaid === true
      let modifiedAfterIso: string | undefined
      let whenIso: string | undefined

      if (normalizedParams.modified_after || normalizedParams.when) {
        if (normalizedParams.modified_after) {
          if (!isValidISO8601UTC(normalizedParams.modified_after)) {
            return ApiResponder.error(
              ctx,
              "Invalid modified_after format. Must be ISO 8601 UTC format (e.g., '2026-01-18T10:00:00Z' or '2026-01-18T10:00:00.000Z')",
              400
            )
          }
          const ts = new Date(normalizedParams.modified_after)
          if (!Number.isNaN(ts.getTime())) modifiedAfterIso = ts.toISOString()
        }
        if (normalizedParams.when) {
          if (!isValidISO8601UTC(normalizedParams.when)) {
            return ApiResponder.error(
              ctx,
              "Invalid when format. Must be ISO 8601 UTC format (e.g., '2026-01-18T10:00:00Z' or '2026-01-18T10:00:00.000Z')",
              400
            )
          }
          const whenTs = new Date(normalizedParams.when)
          if (!Number.isNaN(whenTs.getTime())) whenIso = whenTs.toISOString()
        }
      }
      const lightweightDerivedStats =
        this.shouldUseStrictTrustResolutionLightweightMode(normalizedParams, limit) ||
        this.isTrustResolutionListQuery(normalizedParams, useHistoryQuery ? blockHeight : undefined)

      if (useHistoryQuery && blockHeight !== undefined) {
        let historyRequiresMetricPostFilter = false
        let historyParticipantStatePushedDown = false

        const {
          hasIssuedColumn,
          hasVerifiedColumn,
          hasParticipantsColumn,
          hasParticipantRoleColumns,
          hasWeightColumn,
          hasEcosystemSlashEventsColumn,
        } = await this.getMetricColumnAvailability('participant_history')
        const historyHasAllDerivedColumns =
          hasIssuedColumn &&
          hasVerifiedColumn &&
          hasParticipantsColumn &&
          hasWeightColumn &&
          hasEcosystemSlashEventsColumn
        const historyColumns: any[] = [
          'ph.participant_id as id',
          'ph.schema_id',
          'ph.corporation_id',
          'ph.did',
          'ph.validator_participant_id',
          'ph.role',
          'ph.op_state',
          'ph.revoked',
          'ph.slashed',
          'ph.repaid',
          'ph.effective_from',
          'ph.effective_until',
          'ph.validation_fees',
          'ph.issuance_fees',
          'ph.verification_fees',
          'ph.deposit',
          'ph.slashed_deposit',
          'ph.repaid_deposit',
          'ph.op_last_state_change',
          'ph.op_current_fees',
          'ph.op_current_deposit',
          'ph.op_summary_digest',
          'ph.op_exp',
          'ph.op_validator_deposit',
          'ph.vs_operator',
          'ph.adjusted',
          'ph.vs_operator_authz_enabled',
          'ph.vs_operator_authz_spend_limit',
          'ph.vs_operator_authz_with_feegrant',
          'ph.vs_operator_authz_fee_spend_limit',
          'ph.vs_operator_authz_spend_period',
          'ph.issuance_fee_discount',
          'ph.verification_fee_discount',
          'ph.created',
          'ph.modified',
        ]
        if (hasIssuedColumn) historyColumns.push(knex.raw('COALESCE(ph.issued, 0) as issued'))
        if (hasVerifiedColumn) historyColumns.push(knex.raw('COALESCE(ph.verified, 0) as verified'))
        if (hasParticipantsColumn) historyColumns.push(knex.raw('COALESCE(ph.participants, 0) as participants'))
        if (hasParticipantRoleColumns) {
          historyColumns.push(
            knex.raw('COALESCE(ph.participants_ecosystem, 0) as participants_ecosystem'),
            knex.raw('COALESCE(ph.participants_issuer_grantor, 0) as participants_issuer_grantor'),
            knex.raw('COALESCE(ph.participants_issuer, 0) as participants_issuer'),
            knex.raw('COALESCE(ph.participants_verifier_grantor, 0) as participants_verifier_grantor'),
            knex.raw('COALESCE(ph.participants_verifier, 0) as participants_verifier'),
            knex.raw('COALESCE(ph.participants_holder, 0) as participants_holder')
          )
        }
        if (hasWeightColumn) historyColumns.push(knex.raw('COALESCE(ph.weight, 0) as weight'))
        if (hasEcosystemSlashEventsColumn) {
          historyColumns.push(
            knex.raw('COALESCE(ph.ecosystem_slash_events, 0) as ecosystem_slash_events'),
            knex.raw('COALESCE(ph.ecosystem_slashed_amount, 0) as ecosystem_slashed_amount'),
            knex.raw('COALESCE(ph.ecosystem_slashed_amount_repaid, 0) as ecosystem_slashed_amount_repaid'),
            knex.raw('COALESCE(ph.network_slash_events, 0) as network_slash_events'),
            knex.raw('COALESCE(ph.network_slashed_amount, 0) as network_slashed_amount'),
            knex.raw('COALESCE(ph.network_slashed_amount_repaid, 0) as network_slashed_amount_repaid')
          )
        }

        perfMarks.dbQueryStart = Date.now()
        let historyQuery: any
        if (IS_PG_CLIENT) {
          const latestHistory = knex('participant_history as ph')
            .distinctOn('ph.participant_id')
            .select(historyColumns)
            .where('ph.height', '<=', blockHeight)
            .modify((qb) => {
              this.applyBaseListFiltersToQuery(
                qb,
                normalizedParams,
                corporationIdFilter,
                modifiedAfterIso,
                whenIso,
                onlyValid,
                onlySlashed,
                onlyRepaid,
                now,
                'ph',
                'participant_id'
              )
              const metricPushdown = this.applyMetricFiltersToSql(qb, normalizedParams, {
                participants: hasParticipantsColumn,
                participantRoles: hasParticipantRoleColumns,
                weight: hasWeightColumn,
                issued: hasIssuedColumn,
                verified: hasVerifiedColumn,
                slashStats: hasEcosystemSlashEventsColumn,
                tablePrefix: 'ph',
              })
              historyRequiresMetricPostFilter = metricPushdown.requiresPostFilter
              const participantStatePushdown = this.applyParticipantStateFilterToQuery(
                qb,
                normalizedParams.participant_state,
                now,
                'ph'
              )
              historyParticipantStatePushedDown = participantStatePushdown.pushedDown
            })
            .orderBy('ph.participant_id', 'asc')
            .orderBy('ph.height', 'desc')
            .orderBy('ph.created_at', 'desc')
            .orderBy('ph.id', 'desc')
            .as('latest')
          historyQuery = knex.from(latestHistory).select('*')
        } else {
          const rankedHistory = knex('participant_history as ph')
            .select([
              ...historyColumns,
              knex.raw(
                `ROW_NUMBER() OVER (PARTITION BY ph.participant_id ORDER BY ph.height DESC, ph.created_at DESC, ph.id DESC) as rn`
              ),
            ])
            .where('ph.height', '<=', blockHeight)
            .modify((qb) => {
              this.applyBaseListFiltersToQuery(
                qb,
                normalizedParams,
                corporationIdFilter,
                modifiedAfterIso,
                whenIso,
                onlyValid,
                onlySlashed,
                onlyRepaid,
                now,
                'ph',
                'participant_id'
              )
              const metricPushdown = this.applyMetricFiltersToSql(qb, normalizedParams, {
                participants: hasParticipantsColumn,
                participantRoles: hasParticipantRoleColumns,
                weight: hasWeightColumn,
                issued: hasIssuedColumn,
                verified: hasVerifiedColumn,
                slashStats: hasEcosystemSlashEventsColumn,
                tablePrefix: 'ph',
              })
              historyRequiresMetricPostFilter = metricPushdown.requiresPostFilter
              const participantStatePushdown = this.applyParticipantStateFilterToQuery(
                qb,
                normalizedParams.participant_state,
                now,
                'ph'
              )
              historyParticipantStatePushedDown = participantStatePushdown.pushedDown
            })
            .as('ranked')
          historyQuery = knex.from(rankedHistory).select('*').where('rn', 1)
        }

        const needsPostEnrichFiltering =
          (!historyParticipantStatePushedDown && !!normalizedParams.participant_state) ||
          historyRequiresMetricPostFilter
        const historyFetchLimit = needsPostEnrichFiltering ? Math.min(Math.max(limit * 10, 500), 5000) : limit

        const orderedHistoryQuery = historyQuery.orderBy('id', sortDirection)
        const historyRows = await orderedHistoryQuery.limit(historyFetchLimit)
        perfMarks.dbQueryEnd = Date.now()

        if (historyRows.length === 0) {
          return ApiResponder.success(ctx, { participants: [] }, 200)
        }

        const normalizedHistoryRows = historyRows.map((historyRecord: any) => {
          const participant: any = {
            id: Number(historyRecord.id),
            schema_id: Number(historyRecord.schema_id),
            corporation_id: Number(historyRecord.corporation_id ?? 0) || 0,
            did: historyRecord.did,
            validator_participant_id: historyRecord.validator_participant_id
              ? Number(historyRecord.validator_participant_id)
              : null,
            role:
              historyRecord.role !== undefined && historyRecord.role !== null
                ? mapParticipantType(historyRecord.role)
                : historyRecord.role,
            op_state: this.normalizeOpStateForResponse(historyRecord.op_state),
            revoked: historyRecord.revoked,
            slashed: historyRecord.slashed,
            repaid: historyRecord.repaid,
            effective_from: historyRecord.effective_from,
            effective_until: historyRecord.effective_until,
            validation_fees: historyRecord.validation_fees != null ? Number(historyRecord.validation_fees) : 0,
            issuance_fees: historyRecord.issuance_fees != null ? Number(historyRecord.issuance_fees) : 0,
            verification_fees: historyRecord.verification_fees != null ? Number(historyRecord.verification_fees) : 0,
            deposit: historyRecord.deposit != null ? Number(historyRecord.deposit) : 0,
            slashed_deposit: historyRecord.slashed_deposit != null ? Number(historyRecord.slashed_deposit) : 0,
            repaid_deposit: historyRecord.repaid_deposit != null ? Number(historyRecord.repaid_deposit) : 0,
            op_last_state_change: historyRecord.op_last_state_change,
            op_current_fees: historyRecord.op_current_fees != null ? Number(historyRecord.op_current_fees) : 0,
            op_current_deposit: historyRecord.op_current_deposit != null ? Number(historyRecord.op_current_deposit) : 0,
            op_summary_digest: historyRecord.op_summary_digest,
            op_exp: historyRecord.op_exp,
            op_validator_deposit:
              historyRecord.op_validator_deposit != null ? Number(historyRecord.op_validator_deposit) : 0,
            vs_operator: historyRecord.vs_operator ?? null,
            adjusted: historyRecord.adjusted ?? null,
            vs_operator_authz_enabled: historyRecord.vs_operator_authz_enabled ?? undefined,
            vs_operator_authz_spend_limit: this.normalizeDenomAmountArray(historyRecord.vs_operator_authz_spend_limit),
            vs_operator_authz_with_feegrant: historyRecord.vs_operator_authz_with_feegrant ?? undefined,
            vs_operator_authz_fee_spend_limit: this.normalizeDenomAmountArray(
              historyRecord.vs_operator_authz_fee_spend_limit
            ),
            vs_operator_authz_spend_period: historyRecord.vs_operator_authz_spend_period ?? undefined,
            issuance_fee_discount:
              historyRecord.issuance_fee_discount != null ? Number(historyRecord.issuance_fee_discount) : 0,
            verification_fee_discount:
              historyRecord.verification_fee_discount != null ? Number(historyRecord.verification_fee_discount) : 0,
            created: historyRecord.created,
            modified: historyRecord.modified,
          }
          if (hasIssuedColumn && historyRecord.issued !== undefined) {
            participant.issued = Number(historyRecord.issued || 0)
          }
          if (hasVerifiedColumn && historyRecord.verified !== undefined) {
            participant.verified = Number(historyRecord.verified || 0)
          }
          if (hasParticipantsColumn && historyRecord.participants !== undefined) {
            participant.participants = Number(historyRecord.participants || 0)
          }
          if (hasParticipantRoleColumns) {
            participant.participants_ecosystem = Number(historyRecord.participants_ecosystem || 0)
            participant.participants_issuer_grantor = Number(historyRecord.participants_issuer_grantor || 0)
            participant.participants_issuer = Number(historyRecord.participants_issuer || 0)
            participant.participants_verifier_grantor = Number(historyRecord.participants_verifier_grantor || 0)
            participant.participants_verifier = Number(historyRecord.participants_verifier || 0)
            participant.participants_holder = Number(historyRecord.participants_holder || 0)
          }
          if (hasWeightColumn && historyRecord.weight !== undefined) {
            participant.weight = Number(historyRecord.weight || 0)
          }
          if (hasEcosystemSlashEventsColumn) {
            participant.ecosystem_slash_events = Number(historyRecord.ecosystem_slash_events || 0)
            participant.ecosystem_slashed_amount = Number(historyRecord.ecosystem_slashed_amount || 0)
            participant.ecosystem_slashed_amount_repaid = Number(historyRecord.ecosystem_slashed_amount_repaid || 0)
            participant.network_slash_events = Number(historyRecord.network_slash_events || 0)
            participant.network_slashed_amount = Number(historyRecord.network_slashed_amount || 0)
            participant.network_slashed_amount_repaid = Number(historyRecord.network_slashed_amount_repaid || 0)
          }
          return participant
        })

        perfMarks.enrichStart = Date.now()
        const historyLightweightDerivedStats = lightweightDerivedStats || historyHasAllDerivedColumns
        let filteredParticipants = await this.batchEnrichParticipants(
          normalizedHistoryRows,
          blockHeight,
          new Date(now),
          300,
          { lightweightDerivedStats: historyLightweightDerivedStats }
        )
        perfMarks.enrichEnd = Date.now()

        if (!historyParticipantStatePushedDown && normalizedParams.participant_state) {
          const requestedState = String(normalizedParams.participant_state).toUpperCase()
          filteredParticipants = filteredParticipants.filter(
            (participant) => participant.participant_state === requestedState
          )
        }
        if (historyRequiresMetricPostFilter) {
          filteredParticipants = this.applyMetricFiltersInMemory(filteredParticipants, normalizedParams)
        }

        filteredParticipants.sort((a: any, b: any) => compareById(a.id, b.id, sortDirection))
        filteredParticipants = filteredParticipants.slice(0, limit)

        const participantsWithTrustData =
          trustDataMode === 'none'
            ? filteredParticipants
            : await this.enrichDidItemsWithTrustData(filteredParticipants, trustDataMode, blockHeight)
        return ApiResponder.success(ctx, { participants: participantsWithTrustData }, 200)
      }

      const baseColumns = [
        'id',
        'schema_id',
        'role',
        'did',
        'corporation_id',
        'created',
        'modified',
        'slashed',
        'repaid',
        'effective_from',
        'effective_until',
        'revoked',
        'validation_fees',
        'issuance_fees',
        'verification_fees',
        'deposit',
        'slashed_deposit',
        'repaid_deposit',
        'validator_participant_id',
        'op_state',
        'op_last_state_change',
        'op_current_fees',
        'op_current_deposit',
        'op_summary_digest',
        'op_exp',
        'op_validator_deposit',
        'vs_operator',
        'adjusted',
        'vs_operator_authz_enabled',
        'vs_operator_authz_spend_limit',
        'vs_operator_authz_with_feegrant',
        'vs_operator_authz_fee_spend_limit',
        'vs_operator_authz_spend_period',
        'issuance_fee_discount',
        'verification_fee_discount',
      ]

      const {
        hasIssuedColumn,
        hasVerifiedColumn,
        hasParticipantsColumn,
        hasParticipantRoleColumns,
        hasWeightColumn,
        hasEcosystemSlashEventsColumn,
      } = await this.getMetricColumnAvailability('participants')
      const liveHasAllDerivedColumns =
        hasIssuedColumn &&
        hasVerifiedColumn &&
        hasParticipantsColumn &&
        hasWeightColumn &&
        hasEcosystemSlashEventsColumn

      const selectColumns: any[] = [...baseColumns]

      if (hasIssuedColumn) {
        selectColumns.push(knex.raw('COALESCE(issued, 0) as issued'))
      }
      if (hasVerifiedColumn) {
        selectColumns.push(knex.raw('COALESCE(verified, 0) as verified'))
      }
      if (hasParticipantsColumn) {
        selectColumns.push(knex.raw('COALESCE(participants, 0) as participants'))
      }
      if (hasParticipantRoleColumns) {
        selectColumns.push(
          knex.raw('COALESCE(participants_ecosystem, 0) as participants_ecosystem'),
          knex.raw('COALESCE(participants_issuer_grantor, 0) as participants_issuer_grantor'),
          knex.raw('COALESCE(participants_issuer, 0) as participants_issuer'),
          knex.raw('COALESCE(participants_verifier_grantor, 0) as participants_verifier_grantor'),
          knex.raw('COALESCE(participants_verifier, 0) as participants_verifier'),
          knex.raw('COALESCE(participants_holder, 0) as participants_holder')
        )
      }
      if (hasWeightColumn) {
        selectColumns.push(knex.raw('COALESCE(weight, 0) as weight'))
      }
      if (hasEcosystemSlashEventsColumn) {
        selectColumns.push(
          knex.raw('COALESCE(ecosystem_slash_events, 0) as ecosystem_slash_events'),
          knex.raw('COALESCE(ecosystem_slashed_amount, 0) as ecosystem_slashed_amount'),
          knex.raw('COALESCE(ecosystem_slashed_amount_repaid, 0) as ecosystem_slashed_amount_repaid'),
          knex.raw('COALESCE(network_slash_events, 0) as network_slash_events'),
          knex.raw('COALESCE(network_slashed_amount, 0) as network_slashed_amount'),
          knex.raw('COALESCE(network_slashed_amount_repaid, 0) as network_slashed_amount_repaid')
        )
      }

      const query = knex('participants').select(selectColumns)
      this.applyBaseListFiltersToQuery(
        query,
        normalizedParams,
        corporationIdFilter,
        modifiedAfterIso,
        whenIso,
        onlyValid,
        onlySlashed,
        onlyRepaid,
        now,
        undefined,
        'id'
      )
      const liveMetricPushdown = this.applyMetricFiltersToSql(query, normalizedParams, {
        participants: hasParticipantsColumn,
        participantRoles: hasParticipantRoleColumns,
        weight: hasWeightColumn,
        issued: hasIssuedColumn,
        verified: hasVerifiedColumn,
        slashStats: hasEcosystemSlashEventsColumn,
      })
      const liveParticipantStatePushdown = this.applyParticipantStateFilterToQuery(
        query,
        normalizedParams.participant_state,
        now
      )
      const liveNeedsPostEnrich =
        (!liveParticipantStatePushdown.pushedDown && !!normalizedParams.participant_state) ||
        liveMetricPushdown.requiresPostFilter
      const liveFetchLimit = liveNeedsPostEnrich ? Math.min(Math.max(limit * 10, 500), 5000) : limit
      const orderedQuery = query.orderBy('id', sortDirection)
      perfMarks.dbQueryStart = Date.now()
      const results = await orderedQuery.limit(liveFetchLimit)
      perfMarks.dbQueryEnd = Date.now()
      const normalizedResults = results.map((participant: any) => this.normalizeParticipantRow(participant))

      perfMarks.enrichStart = Date.now()
      const liveLightweightDerivedStats = lightweightDerivedStats || liveHasAllDerivedColumns
      const enrichedResults = await this.batchEnrichParticipants(normalizedResults, blockHeight, new Date(now), 300, {
        lightweightDerivedStats: liveLightweightDerivedStats,
      })
      perfMarks.enrichEnd = Date.now()

      let finalResults = enrichedResults
      if (!liveParticipantStatePushdown.pushedDown && normalizedParams.participant_state) {
        const requestedState = String(normalizedParams.participant_state).toUpperCase()
        finalResults = enrichedResults.filter((participant) => participant.participant_state === requestedState)
      }
      if (liveMetricPushdown.requiresPostFilter) {
        finalResults = this.applyMetricFiltersInMemory(finalResults, normalizedParams)
      }

      finalResults.sort((a: any, b: any) => compareById(a.id, b.id, sortDirection))
      finalResults = finalResults.slice(0, limit)
      const participantsWithTrustData =
        trustDataMode === 'none'
          ? finalResults
          : await this.enrichDidItemsWithTrustData(finalResults, trustDataMode, blockHeight)
      const responsePayload = { participants: participantsWithTrustData }
      return ApiResponder.success(ctx, responsePayload, 200)
    } catch (err: any) {
      const errMessage = err?.message || String(err)
      if (
        typeof errMessage === 'string' &&
        (errMessage.includes('invalid input value for enum participant_role') ||
          errMessage.includes('invalid input value for enum onboarding_state'))
      ) {
        return ApiResponder.error(ctx, `Invalid enum filter value: ${errMessage}`, 400)
      }
      this.logger.error('Error in listParticipants:', err)
      this.logger.error('Error details:', {
        message: err?.message,
        stack: err?.stack,
        code: err?.code,
      })
      return ApiResponder.error(ctx, `Failed to list participants: ${err?.message || String(err)}`, 500)
    } finally {
      const totalMs = Date.now() - requestStartedMs
      const dbMs =
        perfMarks.dbQueryStart && perfMarks.dbQueryEnd ? perfMarks.dbQueryEnd - perfMarks.dbQueryStart : undefined
      const enrichMs =
        perfMarks.enrichStart && perfMarks.enrichEnd ? perfMarks.enrichEnd - perfMarks.enrichStart : undefined

      const msg = `[listParticipants] duration=${totalMs}ms${dbMs !== undefined ? ` db=${dbMs}ms` : ''}${enrichMs !== undefined ? ` enrich=${enrichMs}ms` : ''} limit=${perfMeta.limit ?? '?'} schema_id=${perfMeta.schema_id ?? '-'} role=${perfMeta.role ?? '-'} did=${perfMeta.did ? 'yes' : 'no'} at_height=${perfMeta.blockHeight ?? 'live'}`

      if (totalMs >= ParticipantAPIService.LIST_PARTICIPANTS_SLOW_MS) {
        this.logger.warn(msg)
      } else {
        this.logger.debug(msg)
      }
    }
  }

  @Action()
  async getParams(ctx: Context) {
    return getModuleParamsAction(ctx, ModulesParamsNamesTypes.PP, MODULE_DISPLAY_NAMES.PARTICIPANT)
  }

  @Action({
    rest: 'GET get/:id',
    params: {
      id: { type: 'number', integer: true },
      trust_data: { type: 'string', optional: true },
    },
  })
  async getParticipant(ctx: Context<{ id: number; trust_data?: string }>) {
    try {
      const id = ctx.params.id
      const blockHeight = getBlockHeight(ctx)
      const trustDataModeParsed = parseTrustDataMode((ctx.params as any).trust_data)
      if (!trustDataModeParsed.ok) {
        return ApiResponder.error(ctx, trustDataModeParsed.message, 400)
      }
      const trustDataMode = trustDataModeParsed.mode
      const useHistoryQuery = this.shouldUseHistoryQuery(ctx, blockHeight)

      // If AtBlockHeight is provided, query historical state
      if (useHistoryQuery && blockHeight !== undefined) {
        const {
          hasIssuedColumn,
          hasVerifiedColumn,
          hasParticipantsColumn,
          hasWeightColumn,
          hasEcosystemSlashEventsColumn,
          hasExpireSoonColumn,
        } = await this.getMetricColumnAvailability('participant_history')
        const historyHasAllDerivedColumns =
          hasIssuedColumn &&
          hasVerifiedColumn &&
          hasParticipantsColumn &&
          hasWeightColumn &&
          hasEcosystemSlashEventsColumn

        const selectColumns: any[] = [
          'participant_id',
          'schema_id',
          'corporation_id',
          'did',
          'validator_participant_id',
          'role',
          'op_state',
          'revoked',
          'slashed',
          'repaid',
          'effective_from',
          'effective_until',
          'validation_fees',
          'issuance_fees',
          'verification_fees',
          'deposit',
          'slashed_deposit',
          'repaid_deposit',
          'op_last_state_change',
          'op_current_fees',
          'op_current_deposit',
          'op_summary_digest',
          'op_exp',
          'op_validator_deposit',
          'vs_operator',
          'adjusted',
          'vs_operator_authz_enabled',
          'vs_operator_authz_spend_limit',
          'vs_operator_authz_with_feegrant',
          'vs_operator_authz_fee_spend_limit',
          'vs_operator_authz_spend_period',
          'issuance_fee_discount',
          'verification_fee_discount',
          'created',
          'modified',
        ]
        if (hasExpireSoonColumn) selectColumns.push('expire_soon')
        if (hasIssuedColumn) selectColumns.push(knex.raw('COALESCE(issued, 0) as issued'))
        if (hasVerifiedColumn) selectColumns.push(knex.raw('COALESCE(verified, 0) as verified'))
        if (hasParticipantsColumn) selectColumns.push(knex.raw('COALESCE(participants, 0) as participants'))
        if (hasWeightColumn) selectColumns.push(knex.raw('COALESCE(weight, 0) as weight'))
        if (hasEcosystemSlashEventsColumn) {
          selectColumns.push(
            knex.raw('COALESCE(ecosystem_slash_events, 0) as ecosystem_slash_events'),
            knex.raw('COALESCE(ecosystem_slashed_amount, 0) as ecosystem_slashed_amount'),
            knex.raw('COALESCE(ecosystem_slashed_amount_repaid, 0) as ecosystem_slashed_amount_repaid'),
            knex.raw('COALESCE(network_slash_events, 0) as network_slash_events'),
            knex.raw('COALESCE(network_slashed_amount, 0) as network_slashed_amount'),
            knex.raw('COALESCE(network_slashed_amount_repaid, 0) as network_slashed_amount_repaid')
          )
        }

        const historyRecord = await knex('participant_history')
          .select(selectColumns)
          .where({ participant_id: Number(id) })
          .whereRaw('height <= ?', [Number(blockHeight)])
          .orderBy('height', 'desc')
          .orderBy('created_at', 'desc')
          .first()

        if (!historyRecord) {
          return ApiResponder.error(ctx, 'Participant not found', 404)
        }

        const historicalParticipant: any = {
          id: Number(historyRecord.participant_id),
          schema_id: Number(historyRecord.schema_id),
          corporation_id: Number(historyRecord.corporation_id ?? 0) || 0,
          did: historyRecord.did,
          validator_participant_id: historyRecord.validator_participant_id
            ? Number(historyRecord.validator_participant_id)
            : null,
          role:
            historyRecord.role !== undefined && historyRecord.role !== null
              ? mapParticipantType(historyRecord.role)
              : historyRecord.role,
          op_state: this.normalizeOpStateForResponse(historyRecord.op_state),
          revoked: historyRecord.revoked,
          slashed: historyRecord.slashed,
          repaid: historyRecord.repaid,
          effective_from: historyRecord.effective_from,
          effective_until: historyRecord.effective_until,
          validation_fees: historyRecord.validation_fees != null ? Number(historyRecord.validation_fees) : 0,
          issuance_fees: historyRecord.issuance_fees != null ? Number(historyRecord.issuance_fees) : 0,
          verification_fees: historyRecord.verification_fees != null ? Number(historyRecord.verification_fees) : 0,
          deposit: historyRecord.deposit != null ? Number(historyRecord.deposit) : 0,
          slashed_deposit: historyRecord.slashed_deposit != null ? Number(historyRecord.slashed_deposit) : 0,
          repaid_deposit: historyRecord.repaid_deposit != null ? Number(historyRecord.repaid_deposit) : 0,
          op_last_state_change: historyRecord.op_last_state_change,
          op_current_fees: historyRecord.op_current_fees != null ? Number(historyRecord.op_current_fees) : 0,
          op_current_deposit: historyRecord.op_current_deposit != null ? Number(historyRecord.op_current_deposit) : 0,
          op_summary_digest: historyRecord.op_summary_digest,
          op_exp: historyRecord.op_exp,
          op_validator_deposit:
            historyRecord.op_validator_deposit != null ? Number(historyRecord.op_validator_deposit) : 0,
          vs_operator: historyRecord.vs_operator ?? null,
          adjusted: historyRecord.adjusted ?? null,
          vs_operator_authz_enabled: historyRecord.vs_operator_authz_enabled ?? undefined,
          vs_operator_authz_spend_limit: this.normalizeDenomAmountArray(historyRecord.vs_operator_authz_spend_limit),
          vs_operator_authz_with_feegrant: historyRecord.vs_operator_authz_with_feegrant ?? undefined,
          vs_operator_authz_fee_spend_limit: this.normalizeDenomAmountArray(
            historyRecord.vs_operator_authz_fee_spend_limit
          ),
          vs_operator_authz_spend_period: historyRecord.vs_operator_authz_spend_period ?? undefined,
          created: historyRecord.created,
          modified: historyRecord.modified,
          issuance_fee_discount:
            historyRecord.issuance_fee_discount != null ? Number(historyRecord.issuance_fee_discount) : 0,
          verification_fee_discount:
            historyRecord.verification_fee_discount != null ? Number(historyRecord.verification_fee_discount) : 0,
        }

        if (hasIssuedColumn) {
          historicalParticipant.issued = Number(historyRecord.issued ?? 0)
        }
        if (hasVerifiedColumn) {
          historicalParticipant.verified = Number(historyRecord.verified ?? 0)
        }
        if (hasParticipantsColumn) {
          historicalParticipant.participants = Number(historyRecord.participants ?? 0)
        }
        if (hasWeightColumn) {
          historicalParticipant.weight = Number(historyRecord.weight ?? 0)
        }
        if (hasEcosystemSlashEventsColumn) {
          historicalParticipant.ecosystem_slash_events = Number(historyRecord.ecosystem_slash_events ?? 0)
          historicalParticipant.ecosystem_slashed_amount = Number(historyRecord.ecosystem_slashed_amount ?? 0)
          historicalParticipant.ecosystem_slashed_amount_repaid = Number(
            historyRecord.ecosystem_slashed_amount_repaid ?? 0
          )
          historicalParticipant.network_slash_events = Number(historyRecord.network_slash_events ?? 0)
          historicalParticipant.network_slashed_amount = Number(historyRecord.network_slashed_amount ?? 0)
          historicalParticipant.network_slashed_amount_repaid = Number(historyRecord.network_slashed_amount_repaid ?? 0)
        }
        if (hasExpireSoonColumn) {
          historicalParticipant.expire_soon = historyRecord.expire_soon ?? null
        }

        const enrichedParticipant = await this.enrichParticipantWithStateAndActions(
          historicalParticipant,
          blockHeight,
          new Date(),
          { lightweightDerivedStats: historyHasAllDerivedColumns }
        )
        const [participantWithTrustData] = await this.enrichDidItemsWithTrustData(
          [enrichedParticipant],
          trustDataMode,
          blockHeight
        )
        return ApiResponder.success(ctx, { participant: participantWithTrustData }, 200)
      }

      const participant = await knex('participants').where('id', Number(id)).first()
      if (!participant) {
        return ApiResponder.error(ctx, 'Participant not found', 404)
      }
      const normalizedParticipant = this.normalizeParticipantRow(participant)
      const liveHasAllDerivedColumns =
        participant.issued !== undefined &&
        participant.verified !== undefined &&
        participant.participants !== undefined &&
        participant.weight !== undefined &&
        participant.ecosystem_slash_events !== undefined

      const enrichedParticipant = await this.enrichParticipantWithStateAndActions(
        normalizedParticipant,
        blockHeight,
        new Date(),
        { lightweightDerivedStats: liveHasAllDerivedColumns }
      )

      const [participantWithTrustData] = await this.enrichDidItemsWithTrustData(
        [enrichedParticipant],
        trustDataMode,
        blockHeight
      )
      return ApiResponder.success(ctx, { participant: participantWithTrustData }, 200)
    } catch (err: any) {
      this.logger.error('Error in getParticipant:', err)
      return ApiResponder.error(ctx, 'Failed to get participant', 500)
    }
  }

  @Action({
    rest: 'GET history/:id',
    params: {
      id: { type: 'number', integer: true },
      response_max_size: { type: 'number', optional: true, default: 64 },
      transaction_timestamp_older_than: { type: 'string', optional: true },
    },
  })
  async getParticipantHistory(
    ctx: Context<{ id: number; response_max_size?: number; transaction_timestamp_older_than?: string }>
  ) {
    try {
      const {
        id,
        response_max_size: responseMaxSize = 64,
        transaction_timestamp_older_than: transactionTimestampOlderThan,
      } = ctx.params
      const atBlockHeight =
        (ctx.meta as any)?.$headers?.['at-block-height'] || (ctx.meta as any)?.$headers?.['At-Block-Height']

      const participantExists = await knex('participants').where({ id }).first()
      if (!participantExists) {
        return ApiResponder.error(ctx, `Participant with id=${id} not found`, 404)
      }

      const activity = await buildActivityTimeline(
        {
          entityType: 'Participant',
          historyTable: 'participant_history',
          idField: 'participant_id',
          entityId: id,
          msgTypePrefixes: ['/verana.pp.v1'],
        },
        {
          responseMaxSize,
          transactionTimestampOlderThan,
          atBlockHeight,
        }
      )

      const result = {
        entity_type: 'Participant',
        entity_id: String(id),
        activity: activity || [],
      }

      return ApiResponder.success(ctx, result, 200)
    } catch (err: any) {
      this.logger.error('Error in getParticipantHistory:', err)
      this.logger.error('Error stack:', err?.stack)
      this.logger.error('Error details:', {
        message: err?.message,
        code: err?.code,
        name: err?.name,
      })
      return ApiResponder.error(ctx, `Failed to get participant history: ${err?.message || 'Unknown error'}`, 500)
    }
  }

  @Action({
    rest: 'GET beneficiaries',
    params: {
      issuer_participant_id: { type: 'number', integer: true },
      verifier_participant_id: { type: 'number', integer: true },
    },
  })
  async findBeneficiaries(ctx: Context<{ issuer_participant_id: number; verifier_participant_id: number }>) {
    const { issuer_participant_id: issuerParticipantId, verifier_participant_id: verifierParticipantId } = ctx.params
    const blockHeight = getBlockHeight(ctx)
    const useHistoryQuery = this.shouldUseHistoryQuery(ctx, blockHeight)

    if (!issuerParticipantId && !verifierParticipantId) {
      return ApiResponder.error(ctx, 'issuer_participant_id and verifier_participant_id must be set', 400)
    }

    try {
      const rootIds = [issuerParticipantId, verifierParticipantId]
        .filter((id): id is number => id !== undefined && id !== null)
        .map((id) => Number(id))

      const initialMap = await this.getParticipantsByIdsMap(rootIds, useHistoryQuery ? blockHeight : undefined)
      const missingRootIds = rootIds.filter((rootId) => !initialMap.has(rootId))
      if (missingRootIds.length > 0) {
        return ApiResponder.error(ctx, `Participant not found for id(s): ${missingRootIds.join(', ')}`, 404)
      }

      const foundParticipantMap = new Map<number, any>()
      const collectAncestors = async (startParticipantId: number) => {
        const visited = new Set<number>([startParticipantId])
        let frontier: number[] = [startParticipantId]

        while (frontier.length > 0) {
          const currentMap = await this.getParticipantsByIdsMap(frontier, useHistoryQuery ? blockHeight : undefined)
          const parentIds: number[] = []
          const nextFrontier: number[] = []
          for (const participantId of frontier) {
            const participant = currentMap.get(participantId)
            if (!participant) {
              continue
            }
            const parentId = participant.validator_participant_id ? Number(participant.validator_participant_id) : null
            if (!parentId || visited.has(parentId)) {
              continue
            }
            visited.add(parentId)
            parentIds.push(parentId)
            nextFrontier.push(parentId)
          }
          const parentMap =
            parentIds.length > 0
              ? await this.getParticipantsByIdsMap(parentIds, useHistoryQuery ? blockHeight : undefined)
              : new Map<number, any>()
          for (const parentId of parentIds) {
            const parent = parentMap.get(parentId)
            if (!parent) continue
            if (!parent.revoked && !parent.slashed) foundParticipantMap.set(Number(parent.id), parent)
          }
          frontier = nextFrontier
        }
      }

      if (issuerParticipantId) {
        if (!verifierParticipantId) {
          await collectAncestors(Number(issuerParticipantId))
        }
      }

      if (verifierParticipantId) {
        if (issuerParticipantId) {
          const issuerParticipant = initialMap.get(Number(issuerParticipantId))
          if (issuerParticipant) foundParticipantMap.set(Number(issuerParticipant.id), issuerParticipant)
        }
        await collectAncestors(Number(verifierParticipantId))
      }

      const enrichedParticipants = await this.batchEnrichParticipants(
        Array.from(foundParticipantMap.values()),
        useHistoryQuery ? blockHeight : undefined,
        new Date(),
        100
      )

      return ApiResponder.success(ctx, { participants: enrichedParticipants }, 200)
    } catch (err: any) {
      this.logger.error('Error in findBeneficiaries:', err)
      return ApiResponder.error(ctx, 'Failed to find beneficiaries', 500)
    }
  }

  @Action({
    rest: 'GET participant-session/:id',
    params: {
      id: { type: 'string', pattern: /^[0-9a-fA-F-]+$/ },
    },
  })
  async getParticipantSession(ctx: Context<{ id: string }>) {
    try {
      const { id } = ctx.params
      const blockHeight = getBlockHeight(ctx)
      const useHistoryQuery = this.shouldUseHistoryQuery(ctx, blockHeight)

      // If AtBlockHeight is provided, query historical state
      if (useHistoryQuery && blockHeight !== undefined) {
        const historyRecord = await knex('participant_session_history')
          .where({ session_id: id })
          .whereRaw('height <= ?', [Number(blockHeight)])
          .orderBy('height', 'desc')
          .orderBy('created_at', 'desc')
          .first()

        if (!historyRecord) {
          return ApiResponder.error(ctx, 'ParticipantSession not found', 404)
        }

        const historicalSession = this.normalizeParticipantSessionRow(historyRecord)
        return ApiResponder.success(ctx, { session: historicalSession }, 200)
      }

      // Otherwise, return latest state
      const session = await knex('participant_sessions').where('id', id).first()
      if (!session) {
        return ApiResponder.error(ctx, 'ParticipantSession not found', 404)
      }
      const normalized = this.normalizeParticipantSessionRow(session)
      return ApiResponder.success(ctx, { session: normalized }, 200)
    } catch (err: any) {
      this.logger.error('Error in getParticipantSession:', err)
      return ApiResponder.error(ctx, 'Failed to get ParticipantSession', 500)
    }
  }

  @Action({
    rest: 'GET participant-session-history/:id',
    params: {
      id: { type: 'string', pattern: /^[0-9a-fA-F-]+$/ },
      response_max_size: { type: 'number', optional: true, default: 64 },
      transaction_timestamp_older_than: { type: 'string', optional: true },
    },
  })
  async getParticipantSessionHistory(
    ctx: Context<{ id: string; response_max_size?: number; transaction_timestamp_older_than?: string }>
  ) {
    try {
      const {
        id,
        response_max_size: responseMaxSize = 64,
        transaction_timestamp_older_than: transactionTimestampOlderThan,
      } = ctx.params

      if (transactionTimestampOlderThan) {
        if (!isValidISO8601UTC(transactionTimestampOlderThan)) {
          return ApiResponder.error(
            ctx,
            "Invalid transaction_timestamp_older_than format. Must be ISO 8601 UTC format (e.g., '2026-01-18T10:00:00Z' or '2026-01-18T10:00:00.000Z')",
            400
          )
        }
        const timestampDate = new Date(transactionTimestampOlderThan)
        if (Number.isNaN(timestampDate.getTime())) {
          return ApiResponder.error(ctx, 'Invalid transaction_timestamp_older_than format', 400)
        }
      }

      const atBlockHeight =
        (ctx.meta as any)?.$headers?.['at-block-height'] || (ctx.meta as any)?.$headers?.['At-Block-Height']

      const [currentSession, historySession] = await Promise.all([
        knex('participant_sessions').where({ id }).first(),
        knex('participant_session_history').where({ session_id: id }).first(),
      ])
      if (!currentSession && !historySession) {
        return ApiResponder.error(ctx, `ParticipantSession ${id} not found`, 404)
      }

      const activity = await buildActivityTimeline(
        {
          entityType: 'ParticipantSession',
          historyTable: 'participant_session_history',
          idField: 'session_id',
          entityId: id,
          msgTypePrefixes: ['/verana.pp.v1'],
        },
        {
          responseMaxSize,
          transactionTimestampOlderThan,
          atBlockHeight,
        }
      )

      const result = {
        entity_type: 'ParticipantSession',
        entity_id: id,
        activity: activity || [],
      }

      return ApiResponder.success(ctx, result, 200)
    } catch (err: any) {
      this.logger.error('Error in getParticipantSessionHistory:', err)
      this.logger.error('Error stack:', err?.stack)
      this.logger.error('Error details:', {
        message: err?.message,
        code: err?.code,
        name: err?.name,
      })
      return ApiResponder.error(
        ctx,
        `Failed to get ParticipantSession history: ${err?.message || 'Unknown error'}`,
        500
      )
    }
  }

  @Action({
    rest: 'GET pending/flat',
    params: {
      account: { type: 'string' },
      limit: { type: 'number', optional: true, default: 64 },
      trust_data: { type: 'string', optional: true },
    },
  })
  async pendingFlat(
    ctx: Context<{
      account: string
      limit?: number
      trust_data?: string
    }>
  ) {
    try {
      const p = ctx.params as any
      const trustDataModeParsed = parseTrustDataMode(p.trust_data)
      if (!trustDataModeParsed.ok) {
        return ApiResponder.error(ctx, trustDataModeParsed.message, 400)
      }
      const trustDataMode = trustDataModeParsed.mode
      const accountRaw = typeof p.account === 'string' && p.account.trim() !== '' ? p.account : undefined
      const accountValidation = validateRequiredAccountParam(accountRaw, 'account')
      if (!accountValidation.valid) {
        return ApiResponder.error(ctx, accountValidation.error, 400)
      }
      const account = accountValidation.value
      // VPR v4: validator ownership is corporation-based; resolve the account once.
      const accountCorpId = await resolveCorporationIdByAddress(account)
      if (accountCorpId === null) {
        return ApiResponder.success(ctx, { ecosystems: [] }, 200)
      }

      const limit = Math.min(Math.max(p.limit || 64, 1), 1024)
      const now = new Date()

      const blockHeight = getBlockHeight(ctx)
      const useHistory = this.shouldUseHistoryQuery(ctx, blockHeight)

      const parentIdSet = new Set<number>()

      const validatorParentTypeList = [...PENDING_FLAT_VALIDATOR_PARENT_TYPES]

      /** At-height: latest row per participant where account holds a grantor/validator parent role. */
      let parentIdsAtHeightSubquery: ReturnType<typeof knex> | null = null
      if (useHistory) {
        const rankedParentAtHeight = knex('participant_history')
          .select('participant_id')
          .select(
            knex.raw(
              `ROW_NUMBER() OVER (PARTITION BY participant_id ORDER BY height DESC, created_at DESC, id DESC) as rn`
            )
          )
          .whereRaw('height <= ?', [Number(blockHeight)])
          .andWhere('corporation_id', accountCorpId)
          .whereIn('role', validatorParentTypeList)
          .as('ranked_parent')

        parentIdsAtHeightSubquery = knex.from(rankedParentAtHeight).where('rn', 1).select('participant_id')

        const parentIdNums = await parentIdsAtHeightSubquery.then((rows: any[]) =>
          rows.map((r: any) => Number(r.participant_id))
        )
        for (const id of parentIdNums) parentIdSet.add(id)
      }

      const validatorParentIdsSubquery = knex('participants')
        .select('id')
        .where('corporation_id', accountCorpId)
        .whereIn('role', validatorParentTypeList)

      if (!useHistory) {
        const parentRows = await validatorParentIdsSubquery.clone()
        if (Array.isArray(parentRows)) {
          for (const r of parentRows) parentIdSet.add(Number(r.id))
        }
      }

      const baseColumns = [
        'id',
        'schema_id',
        'role',
        'did',
        'corporation_id',
        'created',
        'modified',
        'slashed',
        'repaid',
        'effective_from',
        'effective_until',
        'revoked',
        'validation_fees',
        'issuance_fees',
        'verification_fees',
        'deposit',
        'slashed_deposit',
        'repaid_deposit',
        'validator_participant_id',
        'op_state',
        'op_last_state_change',
        'op_current_fees',
        'op_current_deposit',
        'op_summary_digest',
        'op_exp',
        'op_validator_deposit',
        'vs_operator',
        'adjusted',
        'vs_operator_authz_enabled',
        'vs_operator_authz_spend_limit',
        'vs_operator_authz_with_feegrant',
        'vs_operator_authz_fee_spend_limit',
        'vs_operator_authz_spend_period',
        'issuance_fee_discount',
        'verification_fee_discount',
      ]

      let participantsAtHeight: any[] = []
      if (useHistory) {
        const latestSub = knex('participant_history')
          .select('participant_id')
          .select(
            knex.raw(
              `ROW_NUMBER() OVER (PARTITION BY participant_id ORDER BY height DESC, created_at DESC, id DESC) as rn`
            )
          )
          .whereRaw('height <= ?', [Number(blockHeight)])
          .where((qb) => {
            qb.where('corporation_id', accountCorpId)
            if (parentIdsAtHeightSubquery) {
              qb.orWhereIn('validator_participant_id', parentIdsAtHeightSubquery.clone())
            }
          })
          .as('ranked')

        const joined = await knex
          .from(latestSub)
          .join('participant_history as ph', (join) => {
            join.on('ranked.participant_id', '=', 'ph.participant_id').andOn('ranked.rn', '=', knex.raw('1'))
          })
          .select(
            'ph.participant_id',
            'ph.schema_id',
            'ph.corporation_id',
            'ph.did',
            'ph.validator_participant_id',
            'ph.role',
            'ph.op_state',
            'ph.revoked',
            'ph.slashed',
            'ph.repaid',
            'ph.effective_from',
            'ph.effective_until',
            'ph.validation_fees',
            'ph.issuance_fees',
            'ph.verification_fees',
            'ph.deposit',
            'ph.slashed_deposit',
            'ph.repaid_deposit',
            'ph.op_last_state_change',
            'ph.op_current_fees',
            'ph.op_current_deposit',
            'ph.op_summary_digest',
            'ph.op_exp',
            'ph.op_validator_deposit',
            'ph.vs_operator',
            'ph.adjusted',
            'ph.vs_operator_authz_enabled',
            'ph.vs_operator_authz_spend_limit',
            'ph.vs_operator_authz_with_feegrant',
            'ph.vs_operator_authz_fee_spend_limit',
            'ph.vs_operator_authz_spend_period',
            'ph.issuance_fee_discount',
            'ph.verification_fee_discount',
            'ph.created',
            'ph.modified'
          )
          .orderBy('ph.participant_id', 'asc')

        if (!Array.isArray(joined) || joined.length === 0) {
          return ApiResponder.success(ctx, { ecosystems: [] }, 200)
        }

        participantsAtHeight = Array.isArray(joined)
          ? joined.map((historyRecord: any) => ({
              ...historyRecord,
              id: Number(historyRecord.participant_id),
              schema_id: Number(historyRecord.schema_id),
              validator_participant_id: historyRecord.validator_participant_id
                ? Number(historyRecord.validator_participant_id)
                : null,
            }))
          : []
      } else {
        const fetchLimit = Math.min(Math.max(limit * 10, 500), 50_000)
        const rows = await knex('participants')
          .select(baseColumns)
          .where((qb) => {
            qb.where('corporation_id', accountCorpId)
            qb.orWhereIn('validator_participant_id', validatorParentIdsSubquery.clone())
          })
          .limit(fetchLimit)
        participantsAtHeight = Array.isArray(rows)
          ? rows.map((participant: any) => ({
              ...participant,
              id: participant.id,
              schema_id: participant.schema_id,
              validator_participant_id: participant.validator_participant_id || null,
            }))
          : []
      }

      const enriched = await this.batchEnrichParticipants(
        participantsAtHeight,
        useHistory ? blockHeight : undefined,
        now,
        50
      )
      const filtered = enriched.filter((participant: any) => {
        if (Number(participant.corporation_id ?? 0) === accountCorpId) {
          if (pendingFlatMatchesOpPendingWithEligibleParticipantState(participant)) return true
          if (participant.participant_state === 'SLASHED') return true
          if (participant.participant_state === 'ACTIVE' && participant.expire_soon === true) return true
        }
        if (participant.validator_participant_id && parentIdSet.has(Number(participant.validator_participant_id))) {
          if (pendingFlatMatchesOpPendingWithEligibleParticipantState(participant)) return true
          if (participant.participant_state === 'SLASHED') return true
        }
        return false
      })
      const sortedFiltered = [...filtered].sort((a: any, b: any) => compareById(a.id, b.id, 'desc'))
      const participantsWithTrustData =
        trustDataMode === 'none'
          ? sortedFiltered
          : await this.enrichDidItemsWithTrustData(sortedFiltered, trustDataMode, useHistory ? blockHeight : undefined)
      const schemaIds = Array.from(new Set(sortedFiltered.map((r: any) => Number(r.schema_id))))
      const schemas =
        schemaIds.length > 0
          ? await knex('credential_schemas')
              .whereIn('id', schemaIds)
              .select('id', 'ecosystem_id', 'json_schema', 'title', 'description', 'participants')
          : []
      const schemaMap = new Map<number, any>()
      for (const s of schemas) {
        const js = s.json_schema
        let schemaObj: any = null
        if (js) {
          if (typeof js === 'string') {
            try {
              schemaObj = JSON.parse(js)
            } catch {
              schemaObj = null
            }
          } else {
            schemaObj = js
          }
        }
        const title =
          (schemaObj && typeof schemaObj === 'object' && typeof schemaObj.title === 'string'
            ? schemaObj.title
            : null) ??
          s.title ??
          undefined
        const description =
          (schemaObj && typeof schemaObj === 'object' && typeof schemaObj.description === 'string'
            ? schemaObj.description
            : null) ??
          s.description ??
          undefined
        schemaMap.set(s.id, {
          id: s.id,
          ecosystem_id: s.ecosystem_id || null,
          title,
          description,
          participants: s.participants ?? 0,
        })
      }

      if (useHistory && schemaMap.size > 0) {
        const schemaIdList = Array.from(schemaMap.keys())
        try {
          let latestSchemaRows: any[] = []
          if (IS_PG_CLIENT) {
            latestSchemaRows = await knex('credential_schema_history as csh')
              .distinctOn('csh.credential_schema_id')
              .select('csh.credential_schema_id', knex.raw('COALESCE(csh.participants, 0) as participants'))
              .whereIn('csh.credential_schema_id', schemaIdList)
              .where('csh.height', '<=', Number(blockHeight))
              .orderBy('csh.credential_schema_id', 'asc')
              .orderBy('csh.height', 'desc')
              .orderBy('csh.created_at', 'desc')
              .orderBy('csh.id', 'desc')
          } else {
            const rankedSchemas = knex('credential_schema_history as csh')
              .select(
                'csh.credential_schema_id',
                knex.raw('COALESCE(csh.participants, 0) as participants'),
                knex.raw(
                  'ROW_NUMBER() OVER (PARTITION BY csh.credential_schema_id ORDER BY csh.height DESC, csh.created_at DESC, csh.id DESC) as rn'
                )
              )
              .whereIn('csh.credential_schema_id', schemaIdList)
              .where('csh.height', '<=', Number(blockHeight))
              .as('ranked')
            latestSchemaRows = await knex
              .from(rankedSchemas)
              .select('credential_schema_id', 'participants')
              .where('rn', 1)
          }
          for (const row of latestSchemaRows) {
            const schemaId = Number(row.credential_schema_id)
            const cs = schemaMap.get(schemaId)
            if (cs) cs.participants = Number(row.participants || 0)
          }
        } catch {
          // Old deployments may not have stats columns in history tables.
        }
      }

      const ecosystemIds = Array.from(
        new Set(
          Array.from(schemaMap.values())
            .map((s: any) => s.ecosystem_id)
            .filter((x: any) => x !== null)
        )
      )
      const trs =
        ecosystemIds.length > 0
          ? await knex('ecosystem').whereIn('id', ecosystemIds).select('id', 'did', 'aka', 'participants')
          : []
      const trMap = new Map<number | string, any>()
      for (const ec of trs) {
        trMap.set(Number(ec.id), {
          id: Number(ec.id),
          did: ec.did,
          aka: ec.aka,
          credential_schemas: [],
          pending_tasks: 0,
          participants: ec.participants ?? 0,
        })
      }
      const csMap = new Map<number, any>()
      for (const participant of participantsWithTrustData) {
        const schemaId = participant.schema_id
        const csInfo = schemaMap.get(schemaId) || { ecosystem_id: null, title: undefined, description: undefined }
        if (!csMap.has(schemaId)) {
          csMap.set(schemaId, {
            id: schemaId,
            title: csInfo.title,
            description: csInfo.description,
            pending_tasks: 0,
            participants: csInfo.participants ?? 0,
            pending_participants: [],
          })
        }
        const entry = csMap.get(schemaId)
        entry.pending_participants.push({ ...participant })
        entry.pending_tasks++
      }

      for (const [schemaId, csEntry] of csMap.entries()) {
        const csInfo = schemaMap.get(schemaId) || { ecosystem_id: null }
        const ecosystemId = csInfo.ecosystem_id != null ? Number(csInfo.ecosystem_id) : null
        if (ecosystemId !== null && trMap.has(ecosystemId)) {
          const trEntry = trMap.get(ecosystemId)
          trEntry.credential_schemas.push(csEntry)
          trEntry.pending_tasks += csEntry.pending_tasks
        } else {
          const nullTrKey = 'null'
          if (!trMap.has(nullTrKey)) {
            trMap.set(nullTrKey, {
              id: null,
              did: null,
              aka: null,
              credential_schemas: [],
              pending_tasks: 0,
              participants: 0,
            })
          }
          const trEntry = trMap.get(nullTrKey)
          trEntry.credential_schemas.push(csEntry)
          trEntry.pending_tasks += csEntry.pending_tasks
        }
      }
      if (useHistory && trMap.size > 0) {
        const ecosystemIdList = Array.from(trMap.keys())
          .filter((ecosystemId) => ecosystemId !== 'null')
          .map((ecosystemId) => Number(ecosystemId))
          .filter((ecosystemId) => Number.isFinite(ecosystemId) && ecosystemId > 0)
        if (ecosystemIdList.length > 0) {
          try {
            let latestTrRows: any[] = []
            if (IS_PG_CLIENT) {
              latestTrRows = await knex('ecosystem_history as trh')
                .distinctOn('trh.ecosystem_id')
                .select('trh.ecosystem_id', knex.raw('COALESCE(trh.participants, 0) as participants'))
                .whereIn('trh.ecosystem_id', ecosystemIdList)
                .where('trh.height', '<=', Number(blockHeight))
                .orderBy('trh.ecosystem_id', 'asc')
                .orderBy('trh.height', 'desc')
                .orderBy('trh.created_at', 'desc')
                .orderBy('trh.id', 'desc')
            } else {
              const rankedTrs = knex('ecosystem_history as trh')
                .select(
                  'trh.ecosystem_id',
                  knex.raw('COALESCE(trh.participants, 0) as participants'),
                  knex.raw(
                    'ROW_NUMBER() OVER (PARTITION BY trh.ecosystem_id ORDER BY trh.height DESC, trh.created_at DESC, trh.id DESC) as rn'
                  )
                )
                .whereIn('trh.ecosystem_id', ecosystemIdList)
                .where('trh.height', '<=', Number(blockHeight))
                .as('ranked')
              latestTrRows = await knex.from(rankedTrs).select('ecosystem_id', 'participants').where('rn', 1)
            }
            for (const row of latestTrRows) {
              const ecosystemId = Number(row.ecosystem_id)
              const trEntry = trMap.get(ecosystemId)
              if (trEntry) trEntry.participants = Number(row.participants || 0)
            }
          } catch {
            // Fallback to live participants if historical stats are unavailable.
          }
        }
      }

      for (const trEntry of trMap.values()) {
        trEntry.credential_schemas.sort((a: any, b: any) => (b.participants || 0) - (a.participants || 0))
      }

      const ecosystems = Array.from(trMap.values())
        .map((ec: any) => ({
          id: ec.id,
          did: ec.did,
          pending_tasks: ec.pending_tasks,
          participants: ec.participants || 0,
          schemas: ec.credential_schemas,
        }))
        .sort((a: any, b: any) => (b.participants || 0) - (a.participants || 0))
      const ecosystemsWithTrustData = await this.enrichDidItemsWithTrustData(
        ecosystems.slice(0, limit),
        trustDataMode,
        useHistory ? blockHeight : undefined
      )
      return ApiResponder.success(ctx, { ecosystems: ecosystemsWithTrustData }, 200)
    } catch (err: any) {
      this.logger.error('Error in pendingFlat:', err)
      return ApiResponder.error(ctx, `Failed to get pending tasks: ${err?.message || err}`, 500)
    }
  }
}
