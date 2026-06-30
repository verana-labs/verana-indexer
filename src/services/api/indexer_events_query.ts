import knex from '../../common/utils/db_connection'
import { extractController } from '../../common/utils/extract_controller'
import {
  VeranaCorporationMessageTypes,
  VeranaCredentialSchemaMessageTypes,
  VeranaDelegationMessageTypes,
  VeranaDiMessageTypes,
  VeranaEcosystemMessageTypes,
  VeranaParticipantMessageTypes,
} from '../../common/verana-message-types'
import { applyBlockHeightFilter, toIsoSeconds } from './api_shared'
import {
  collectDidsDeep,
  firstNormalizedDid,
  normalizeDid,
  readFirstPositiveInteger,
  toProtoModule,
  toShortMessageType,
  toSnakeCaseAction,
  uniqueNormalizedDids,
} from './indexer_event_utils'

export type IndexerTxEvent = {
  type: 'transaction-executed'
  module: 'ecosystem' | 'credential-schema' | 'participant' | 'digital-identity' | 'delegation' | 'corporation'
  action: string
  messageType: string
  blockHeight: number
  txHash: string
  txIndex: number
  messageIndex: number
  sender: string
  did: string
  relatedDids: string[]
  entityType?: string
  entityId?: string
  ecosystemId?: string
  schemaId?: string
  participantId?: string
  corporationId?: number
  relatedCorporationIds: number[]
  timestamp: string
}

export type IndexerEventRecord = {
  type: 'indexer-event'
  event_type: string
  did: string
  block_height: number
  tx_hash: string
  timestamp: string
  payload: {
    module: IndexerTxEvent['module']
    action: string
    message_type: string
    tx_index: number
    message_index: number
    sender: string
    related_dids: string[]
    entity_type?: string
    entity_id?: string
    ecosystem_id?: string
    schema_id?: string
    participant_id?: string
    corporation_id?: number
    related_corporation_ids?: number[]
  }
}

type EventRow = {
  message_id: number
  tx_id: number
  message_index: number
  message_type: string
  sender: string
  content: unknown
  block_height: number
  tx_hash: string
  tx_index: number
  timestamp: Date | string
}

type EventMeta = {
  module: IndexerTxEvent['module']
  action: string
  entityType?: string
}

const EVENT_META: Record<string, EventMeta> = {
  [VeranaEcosystemMessageTypes.CreateEcosystem]: {
    module: 'ecosystem',
    action: 'CreateNewEcosystem',
    entityType: 'Ecosystem',
  },
  [VeranaEcosystemMessageTypes.UpdateEcosystem]: {
    module: 'ecosystem',
    action: 'UpdateEcosystem',
    entityType: 'Ecosystem',
  },
  [VeranaEcosystemMessageTypes.ArchiveEcosystem]: {
    module: 'ecosystem',
    action: 'ArchiveEcosystem',
    entityType: 'Ecosystem',
  },
  [VeranaEcosystemMessageTypes.AddGovernanceFrameworkDoc]: {
    module: 'ecosystem',
    action: 'AddGovernanceFrameworkDocument',
    entityType: 'GovernanceFrameworkDocument',
  },
  [VeranaEcosystemMessageTypes.IncreaseGovernanceFrameworkVersion]: {
    module: 'ecosystem',
    action: 'IncreaseActiveGFVersion',
    entityType: 'GovernanceFrameworkVersion',
  },
  [VeranaCredentialSchemaMessageTypes.CreateCredentialSchema]: {
    module: 'credential-schema',
    action: 'CreateNewCredentialSchema',
    entityType: 'CredentialSchema',
  },
  [VeranaCredentialSchemaMessageTypes.UpdateCredentialSchema]: {
    module: 'credential-schema',
    action: 'UpdateCredentialSchema',
    entityType: 'CredentialSchema',
  },
  [VeranaCredentialSchemaMessageTypes.ArchiveCredentialSchema]: {
    module: 'credential-schema',
    action: 'ArchiveCredentialSchema',
    entityType: 'CredentialSchema',
  },
  [VeranaParticipantMessageTypes.StartParticipantOP]: {
    module: 'participant',
    action: 'StartParticipantOP',
    entityType: 'Participant',
  },
  [VeranaParticipantMessageTypes.CreateRootParticipant]: {
    module: 'participant',
    action: 'CreateRootParticipant',
    entityType: 'Participant',
  },
  [VeranaParticipantMessageTypes.SelfCreateParticipant]: {
    module: 'participant',
    action: 'SelfCreateParticipant',
    entityType: 'Participant',
  },
  [VeranaParticipantMessageTypes.RenewParticipantOP]: {
    module: 'participant',
    action: 'RenewParticipantOP',
    entityType: 'Participant',
  },
  [VeranaParticipantMessageTypes.SetParticipantOPToValidated]: {
    module: 'participant',
    action: 'SetParticipantOPToValidated',
    entityType: 'Participant',
  },
  [VeranaParticipantMessageTypes.SetParticipantEffectiveUntil]: {
    module: 'participant',
    action: 'SetParticipantEffectiveUntil',
    entityType: 'Participant',
  },
  [VeranaParticipantMessageTypes.RevokeParticipant]: {
    module: 'participant',
    action: 'RevokeParticipant',
    entityType: 'Participant',
  },
  [VeranaParticipantMessageTypes.SlashParticipantTrustDeposit]: {
    module: 'participant',
    action: 'SlashParticipantTrustDeposit',
    entityType: 'Participant',
  },
  [VeranaParticipantMessageTypes.RepayParticipantSlashedTrustDeposit]: {
    module: 'participant',
    action: 'RepayParticipantSlashedTrustDeposit',
    entityType: 'Participant',
  },
  [VeranaParticipantMessageTypes.CancelParticipantOPLastRequest]: {
    module: 'participant',
    action: 'CancelParticipantOPLastRequest',
    entityType: 'Participant',
  },
  [VeranaParticipantMessageTypes.CreateOrUpdateParticipantSession]: {
    module: 'participant',
    action: 'CreateOrUpdateParticipantSession',
    entityType: 'ParticipantSession',
  },
  [VeranaDiMessageTypes.StoreDigest]: {
    module: 'digital-identity',
    action: 'StoreDigest',
    entityType: 'DigitalIdentityDigest',
  },
  [VeranaDelegationMessageTypes.GrantOperatorAuthorization]: {
    module: 'delegation',
    action: 'GrantOperatorAuthorization',
    entityType: 'OperatorAuthorization',
  },
  [VeranaDelegationMessageTypes.RevokeOperatorAuthorization]: {
    module: 'delegation',
    action: 'RevokeOperatorAuthorization',
    entityType: 'OperatorAuthorization',
  },
  [VeranaCorporationMessageTypes.CreateCorporation]: {
    module: 'corporation',
    action: 'CreateNewCorporation',
    entityType: 'Corporation',
  },
  [VeranaCorporationMessageTypes.UpdateCorporation]: {
    module: 'corporation',
    action: 'UpdateCorporation',
    entityType: 'Corporation',
  },
}

const WATCHED_MESSAGE_TYPES = Object.keys(EVENT_META)

function readNumber(content: unknown, keys: readonly string[]): number | null {
  return readFirstPositiveInteger(content, keys)
}

const ID_ALIASES = {
  ecosystem: ['ecosystem_id', 'ecosystemId'],
  credentialSchema: ['schema_id', 'schemaId', 'credential_schema_id', 'credentialSchemaId'],
  participant: ['participant_id', 'participantId'],
  validatorParticipant: ['validator_participant_id', 'validatorParticipantId'],
  governanceFramework: ['gfv_id', 'gfvId', 'gfd_id', 'gfdId'],
} as const

async function getEntityId(row: EventRow, meta: EventMeta): Promise<string | undefined> {
  if (meta.module === 'participant') {
    const participantId = readNumber(row.content, ['id', ...ID_ALIASES.participant])
    return participantId ? String(participantId) : resolveEntityIdFromDomain(row, meta)
  }

  if (meta.module === 'credential-schema') {
    const schemaId = readNumber(row.content, ['id', ...ID_ALIASES.credentialSchema])
    return schemaId ? String(schemaId) : resolveEntityIdFromDomain(row, meta)
  }

  const ecosystemId =
    readNumber(row.content, ['id', ...ID_ALIASES.ecosystem]) ?? readNumber(row.content, ID_ALIASES.governanceFramework)
  return ecosystemId ? String(ecosystemId) : resolveEntityIdFromDomain(row, meta)
}

async function resolveEntityIdFromDomain(row: EventRow, meta: EventMeta): Promise<string | undefined> {
  const content = row.content && typeof row.content === 'object' ? (row.content as Record<string, unknown>) : {}
  const height = Number(row.block_height)

  if (meta.module === 'ecosystem') {
    const did = normalizeDid(content.did)
    if (!did) return undefined
    const ec = await knex('ecosystem').select('id').where({ did }).first()
    return ec?.id != null ? String(ec.id) : undefined
  }

  if (meta.module === 'credential-schema') {
    const ecosystemId = readNumber(content, ID_ALIASES.ecosystem)
    const query = knex('credential_schema_history').select('credential_schema_id').where({ height })
    if (ecosystemId) query.andWhere({ ecosystem_id: ecosystemId })
    const cs = await query.orderBy('credential_schema_id', 'desc').first()
    return cs?.credential_schema_id != null ? String(cs.credential_schema_id) : undefined
  }

  if (meta.module === 'participant') {
    const did = normalizeDid(content.did)
    const query = knex('participant_history').select('participant_id').where({ height })
    if (did) query.andWhere({ did })
    const pp = await query.orderBy('participant_id', 'desc').first()
    return pp?.participant_id != null ? String(pp.participant_id) : undefined
  }

  return undefined
}

function toCorporationId(value: unknown): number | undefined {
  const n = Number(value)
  return Number.isInteger(n) && n > 0 ? n : undefined
}

async function loadEcosystem(ecosystemId: number | null | undefined): Promise<{
  did?: string
  corporationId?: number
}> {
  if (!ecosystemId) return {}
  const row = await knex('ecosystem').select('did', 'corporation_id').where({ id: ecosystemId }).first()
  return { did: normalizeDid(row?.did), corporationId: toCorporationId(row?.corporation_id) }
}

async function loadCorporation(opts: { did?: string; address?: string }): Promise<{
  did?: string
  corporationId?: number
}> {
  const query = knex('corporation').select('id', 'did')
  if (opts.address) query.where({ corporation: opts.address })
  else if (opts.did) query.where({ did: opts.did })
  else return {}
  const row = await query.first()
  if (!row) return {}
  return { did: normalizeDid(row.did), corporationId: toCorporationId(row.id) }
}

async function loadSchemaRelation(schemaId: number | null | undefined): Promise<{
  schemaId?: string
  ecosystemId?: string
  ecosystemDid?: string
  corporationId?: number
}> {
  if (!schemaId) return {}
  const schema = await knex('credential_schemas as cs')
    .leftJoin('ecosystem as ec', 'ec.id', 'cs.ecosystem_id')
    .where('cs.id', schemaId)
    .select('cs.id as schema_id', 'cs.ecosystem_id', 'ec.did as tr_did', 'ec.corporation_id as corporation_id')
    .first()
  if (!schema) return { schemaId: String(schemaId) }
  return {
    schemaId: String(schema.schema_id ?? schemaId),
    ecosystemId: schema.ecosystem_id != null ? String(schema.ecosystem_id) : undefined,
    ecosystemDid: normalizeDid(schema.tr_did),
    corporationId: toCorporationId(schema.corporation_id),
  }
}

async function loadParticipantRelation(participantId: number | null | undefined): Promise<{
  participantId?: string
  participantDid?: string
  schemaId?: string
  ecosystemId?: string
  ecosystemDid?: string
  validatorParticipantDid?: string
  corporationId?: number
  validatorCorporationId?: number
}> {
  if (!participantId) return {}
  const participant = await knex('participants as p')
    .leftJoin('credential_schemas as cs', 'cs.id', 'p.schema_id')
    .leftJoin('ecosystem as ec', 'ec.id', 'cs.ecosystem_id')
    .leftJoin('participants as validator', 'validator.id', 'p.validator_participant_id')
    .where('p.id', participantId)
    .select(
      'p.id as participant_id',
      'p.did as participant_did',
      'p.schema_id',
      'p.corporation_id as corporation_id',
      'cs.ecosystem_id',
      'ec.did as tr_did',
      'validator.did as validator_participant_did',
      'validator.corporation_id as validator_corporation_id'
    )
    .first()
  if (!participant) return { participantId: String(participantId) }
  return {
    participantId: String(participant.participant_id ?? participantId),
    participantDid: normalizeDid(participant.participant_did),
    schemaId: participant.schema_id != null ? String(participant.schema_id) : undefined,
    ecosystemId: participant.ecosystem_id != null ? String(participant.ecosystem_id) : undefined,
    ecosystemDid: normalizeDid(participant.tr_did),
    validatorParticipantDid: normalizeDid(participant.validator_participant_did),
    corporationId: toCorporationId(participant.corporation_id),
    validatorCorporationId: toCorporationId(participant.validator_corporation_id),
  }
}

async function toIndexerEvent(row: EventRow): Promise<IndexerTxEvent | null> {
  const meta = EVENT_META[row.message_type]
  if (!meta) return null

  const entityId = await getEntityId(row, meta)
  const content = row.content && typeof row.content === 'object' ? (row.content as Record<string, unknown>) : {}
  const collected = collectDidsDeep([row.sender, row.content])
  let ecosystemId: string | undefined
  let schemaId: string | undefined
  let participantId: string | undefined
  let corporationId: number | undefined
  const relatedCorporationIds = new Set<number>()
  const explicitPrimaryDid = firstNormalizedDid([
    content.did,
    content.ecosystem_did,
    content.ecosystemDid,
    content.participant_did,
    content.participantDid,
    content.sender,
    row.sender,
  ])

  if (meta.module === 'ecosystem') {
    const rawEcosystemId = readNumber(row.content, [...ID_ALIASES.ecosystem, 'id'])
    ecosystemId = rawEcosystemId ? String(rawEcosystemId) : entityId
    const ecosystem = await loadEcosystem(rawEcosystemId)
    if (ecosystem.did) collected.add(ecosystem.did)
    corporationId = ecosystem.corporationId
  }

  if (meta.module === 'credential-schema') {
    const rawSchemaId = readNumber(row.content, [...ID_ALIASES.credentialSchema, 'id'])
    const rawEcosystemId = readNumber(row.content, ID_ALIASES.ecosystem)
    const relation = await loadSchemaRelation(rawSchemaId)
    schemaId = relation.schemaId ?? (rawSchemaId ? String(rawSchemaId) : entityId)
    ecosystemId = relation.ecosystemId ?? (rawEcosystemId ? String(rawEcosystemId) : undefined)
    corporationId = relation.corporationId
    const ecosystem = relation.ecosystemDid ? { did: relation.ecosystemDid } : await loadEcosystem(rawEcosystemId)
    if (ecosystem.did) collected.add(ecosystem.did)
    if (corporationId === undefined && 'corporationId' in ecosystem) {
      corporationId = (ecosystem as { corporationId?: number }).corporationId
    }
  }

  if (meta.module === 'participant') {
    const rawParticipantId = readNumber(row.content, [...ID_ALIASES.participant, 'id'])
    const rawSchemaId = readNumber(row.content, ID_ALIASES.credentialSchema)
    const rawValidatorParticipantId = readNumber(row.content, ID_ALIASES.validatorParticipant)
    const relation = await loadParticipantRelation(rawParticipantId)
    participantId = relation.participantId ?? (rawParticipantId ? String(rawParticipantId) : entityId)
    schemaId = relation.schemaId ?? (rawSchemaId ? String(rawSchemaId) : undefined)
    ecosystemId = relation.ecosystemId
    corporationId = relation.corporationId
    if (relation.validatorCorporationId !== undefined) relatedCorporationIds.add(relation.validatorCorporationId)
    ;[relation.participantDid, relation.ecosystemDid, relation.validatorParticipantDid].forEach((did) => {
      if (did) collected.add(did)
    })
    if (rawSchemaId && !relation.ecosystemDid) {
      const schemaRelation = await loadSchemaRelation(rawSchemaId)
      schemaId = schemaId ?? schemaRelation.schemaId
      ecosystemId = ecosystemId ?? schemaRelation.ecosystemId
      if (corporationId === undefined) corporationId = schemaRelation.corporationId
      if (schemaRelation.ecosystemDid) collected.add(schemaRelation.ecosystemDid)
    }
    if (rawValidatorParticipantId) {
      const validatorRelation = await loadParticipantRelation(rawValidatorParticipantId)
      if (validatorRelation.corporationId !== undefined) relatedCorporationIds.add(validatorRelation.corporationId)
      ;[validatorRelation.participantDid, validatorRelation.ecosystemDid].forEach((did) => {
        if (did) collected.add(did)
      })
    }
  }

  if (meta.module === 'corporation') {
    const relation = await loadCorporation({
      did: firstNormalizedDid([content.did]),
      address: extractController(content),
    })
    corporationId = relation.corporationId
    if (relation.did) collected.add(relation.did)
  }

  const relatedDids = uniqueNormalizedDids(collected)
  const primaryDid =
    explicitPrimaryDid ??
    (meta.module === 'participant' ? firstNormalizedDid(relatedDids) : undefined) ??
    firstNormalizedDid(relatedDids)
  if (!primaryDid) return null

  return {
    type: 'transaction-executed',
    module: meta.module,
    action: meta.action,
    messageType: row.message_type,
    blockHeight: Number(row.block_height),
    txHash: row.tx_hash,
    txIndex: Number(row.tx_index),
    messageIndex: Number(row.message_index),
    sender: row.sender,
    did: primaryDid,
    relatedDids,
    entityType: meta.entityType,
    entityId,
    ecosystemId,
    schemaId,
    participantId,
    corporationId,
    relatedCorporationIds: [...relatedCorporationIds],
    timestamp: toIsoSeconds(row.timestamp),
  }
}

function toEventRow(event: IndexerTxEvent): Record<string, unknown> {
  const module = toProtoModule(event.module)
  return {
    event_type: event.action,
    did: event.did,
    block_height: event.blockHeight,
    tx_hash: event.txHash,
    tx_index: event.txIndex,
    message_index: event.messageIndex,
    message_type: event.messageType,
    module,
    entity_type: event.entityType ?? null,
    entity_id: event.entityId ?? null,
    timestamp: event.timestamp,
    payload: {
      module,
      action: toSnakeCaseAction(event.action),
      message_type: toShortMessageType(event.messageType),
      tx_index: event.txIndex,
      message_index: event.messageIndex,
      sender: event.sender,
      related_dids: event.relatedDids,
      entity_type: event.entityType,
      entity_id: event.entityId,
      ecosystem_id: event.ecosystemId,
      schema_id: event.schemaId,
      participant_id: event.participantId,
      corporation_id: event.corporationId,
      related_corporation_ids: event.relatedCorporationIds,
    },
  }
}

function fromStoredRow(row: Record<string, any>): IndexerEventRecord {
  return {
    type: 'indexer-event',
    event_type: String(row.event_type),
    did: String(row.did),
    block_height: Number(row.block_height),
    tx_hash: String(row.tx_hash),
    timestamp: toIsoSeconds(row.timestamp),
    payload: {
      module: row.payload?.module ?? row.module,
      action: row.payload?.action ?? row.event_type,
      // Backward compatible: accept old camelCase payload keys.
      message_type: row.payload?.message_type ?? row.payload?.messageType ?? row.message_type,
      tx_index: Number(row.payload?.tx_index ?? row.payload?.txIndex ?? row.tx_index ?? 0),
      message_index: Number(row.payload?.message_index ?? row.payload?.messageIndex ?? row.message_index ?? 0),
      sender: String(row.payload?.sender ?? ''),
      related_dids: Array.isArray(row.payload?.related_dids)
        ? row.payload.related_dids
        : Array.isArray(row.payload?.relatedDids)
          ? row.payload.relatedDids
          : [String(row.did)],
      entity_type: row.payload?.entity_type ?? row.payload?.entityType ?? row.entity_type ?? undefined,
      entity_id: row.payload?.entity_id ?? row.payload?.entityId ?? row.entity_id ?? undefined,
      ecosystem_id: row.payload?.ecosystem_id ?? row.payload?.ecosystemId ?? undefined,
      schema_id: row.payload?.schema_id ?? row.payload?.schemaId ?? undefined,
      participant_id: row.payload?.participant_id ?? row.payload?.participantId ?? undefined,
      corporation_id: toCorporationId(row.payload?.corporation_id ?? row.payload?.corporationId),
      related_corporation_ids: Array.isArray(row.payload?.related_corporation_ids)
        ? row.payload.related_corporation_ids
            .map((v: unknown) => Number(v))
            .filter((n: number) => Number.isInteger(n) && n > 0)
        : Array.isArray(row.payload?.relatedCorporationIds)
          ? row.payload.relatedCorporationIds
              .map((v: unknown) => Number(v))
              .filter((n: number) => Number.isInteger(n) && n > 0)
          : undefined,
    },
  }
}

async function buildIndexerTxEvents(args: {
  afterBlockHeight?: number
  blockHeight?: number
  limit?: number
  offset?: number
}): Promise<IndexerTxEvent[]> {
  const limit = Math.max(1, Math.min(500, Math.floor(Number(args.limit ?? 100))))
  const query = knex('transaction_message as tm')
    .innerJoin('transaction as tx', 'tx.id', 'tm.tx_id')
    .whereIn('tm.type', WATCHED_MESSAGE_TYPES)
    .andWhere('tx.code', 0)
    .select(
      'tm.id as message_id',
      'tm.tx_id',
      'tm.index as message_index',
      'tm.type as message_type',
      'tm.sender',
      'tm.content',
      'tx.height as block_height',
      'tx.hash as tx_hash',
      'tx.index as tx_index',
      'tx.timestamp'
    )
    .orderBy('tx.height', 'asc')
    .orderBy('tx.index', 'asc')
    .orderBy('tm.index', 'asc')
    .limit(limit)

  applyBlockHeightFilter(query, args, 'tx.height')
  if (Number.isInteger(args.offset) && Number(args.offset) > 0) {
    query.offset(Number(args.offset))
  }

  const rows = (await query) as EventRow[]
  return (await Promise.all(rows.map((row) => toIndexerEvent(row)))).filter(Boolean) as IndexerTxEvent[]
}

export async function persistIndexerEventsForBlock(blockHeight: number): Promise<IndexerEventRecord[]> {
  const rows: Array<Record<string, unknown>> = []
  const pageSize = 500
  let offset = 0
  while (true) {
    const txEvents = await buildIndexerTxEvents({ blockHeight, limit: pageSize, offset })
    rows.push(...txEvents.map(toEventRow))
    if (txEvents.length < pageSize) break
    offset += pageSize
  }
  let insertedIds: number[] = []

  if (rows.length > 0) {
    const inserted = await knex('indexer_events')
      .insert(rows)
      .onConflict(knex.raw("(tx_hash, tx_index, message_index, event_type, entity_type, COALESCE(entity_id, ''))"))
      .ignore()
      .returning('id')
    insertedIds = inserted
      .map((row: number | string | { id?: number | string }) => Number(typeof row === 'object' ? row.id : row))
      .filter((id): id is number => Number.isInteger(id))
    if (insertedIds.length === 0) {
      console.info(
        `[IndexerEvents] skipped duplicate event batch for block_height=${blockHeight}, candidates=${rows.length}`
      )
    } else {
      console.info(
        `[IndexerEvents] saved ${insertedIds.length}/${rows.length} event(s) for block_height=${blockHeight}`
      )
    }
  } else {
    console.info(`[IndexerEvents] no DID found or no watched messages for block_height=${blockHeight}`)
  }

  if (insertedIds.length === 0) return []

  const results: IndexerEventRecord[] = []
  const chunkSize = 500
  for (let i = 0; i < insertedIds.length; i += chunkSize) {
    const chunk = insertedIds.slice(i, i + chunkSize)
    const rows = await listIndexerEvents({ ids: chunk, limit: chunk.length })
    results.push(...rows)
  }
  return results
}

export async function listIndexerEvents(args: {
  afterBlockHeight?: number
  blockHeight?: number
  did?: string
  dids?: string[]
  corporationId?: number
  ids?: number[]
  limit?: number
}): Promise<IndexerEventRecord[]> {
  const limit = Math.max(1, Math.min(500, Math.floor(Number(args.limit ?? 100))))
  const normalizedDids = uniqueNormalizedDids([...(args.dids ?? []), ...(args.did != null ? [args.did] : [])])
  const didFilterRequested = (args.dids?.length ?? 0) > 0 || args.did != null
  const corporationId = toCorporationId(args.corporationId)
  const query = knex('indexer_events as ie')
    .select(
      'ie.id',
      'ie.event_type',
      'ie.did',
      'ie.block_height',
      'ie.tx_hash',
      'ie.tx_index',
      'ie.message_index',
      'ie.message_type',
      'ie.module',
      'ie.entity_type',
      'ie.entity_id',
      'ie.timestamp',
      'ie.payload'
    )
    .orderBy('ie.block_height', 'asc')
    .orderBy('ie.tx_index', 'asc')
    .orderBy('ie.message_index', 'asc')
    .orderBy('ie.id', 'asc')
    .limit(limit)

  if (args.ids) {
    query.whereIn('ie.id', args.ids)
  } else {
    if (didFilterRequested && normalizedDids.length === 0) return []
    if (normalizedDids.length > 0) {
      query.andWhere(function () {
        this.whereIn('ie.did', normalizedDids)
        for (const requestedDid of normalizedDids) {
          this.orWhereRaw("(ie.payload -> 'related_dids') \\? ?", [requestedDid]).orWhereRaw(
            "(ie.payload -> 'relatedDids') \\? ?",
            [requestedDid]
          )
        }
      })
    }
    if (corporationId !== undefined) {
      query.andWhere(function () {
        this.whereRaw("(ie.payload ->> 'corporation_id') = ?", [String(corporationId)])
          .orWhereRaw("(ie.payload ->> 'corporationId') = ?", [String(corporationId)])
          .orWhereRaw("(ie.payload -> 'related_corporation_ids') @> ?::jsonb", [JSON.stringify([corporationId])])
          .orWhereRaw("(ie.payload -> 'relatedCorporationIds') @> ?::jsonb", [JSON.stringify([corporationId])])
      })
    }
  }
  applyBlockHeightFilter(query, args, 'ie.block_height')

  const rows = (await query) as Array<Record<string, any>>
  return rows.map(fromStoredRow)
}
