import _ from 'lodash'
import knex from '../../common/utils/db_connection'
import { extractController } from '../../common/utils/extract_controller'
import Utils from '../../common/utils/utils'
import {
  VeranaCorporationMessageTypes,
  VeranaCredentialSchemaMessageTypes,
  VeranaDelegationMessageTypes,
  VeranaDiMessageTypes,
  VeranaEcosystemMessageTypes,
  VeranaExchangeRateMessageTypes,
  VeranaGovernanceFrameworkMessageTypes,
  VeranaParticipantMessageTypes,
  VeranaTrustDepositMessageTypes,
} from '../../common/verana-message-types'
import {
  anyTypeUrl,
  decodeProposalMessages,
  GROUP_ADMIN_MESSAGE_TYPES,
  GROUP_EVENT_TYPES,
  GROUP_ROUTED_MESSAGE_TYPES,
  parseEventAttrValue,
  shortExecutorResult,
} from '../crawl-group/group_helpers'
import { AUTHZ_EXEC_TYPE } from '../crawl-tx/nested_messages'
import { applyBlockHeightFilter, toIsoSeconds } from './api_shared'
import { fetchCorporationById, fetchParticipantDid } from './indexer_event_chain_lookup'
import {
  collectDidsDeep,
  firstNormalizedDid,
  normalizeDid,
  readFirstPositiveInteger,
  readPositiveInteger,
  toProtoModule,
  toShortMessageType,
  uniqueNormalizedDids,
} from './indexer_event_utils'

export type IndexerTxEvent = {
  type: 'transaction-executed'
  module:
    | 'ecosystem'
    | 'credential-schema'
    | 'participant'
    | 'digital-identity'
    | 'delegation'
    | 'corporation'
    | 'group'
    | 'exchange-rate'
    | 'trust-deposit'
    | 'governance-framework'
  action: string
  messageType: string
  blockHeight: number
  txHash: string
  txIndex: number
  messageIndex: number
  sender: string
  did: string | null
  relatedDids: string[]
  entityType?: string
  entityId?: string
  ecosystemId?: string
  schemaId?: string
  participantId?: string
  corporationId?: number
  relatedCorporationIds: number[]
  grantee?: string
  vsOperator?: string
  withFeegrant?: boolean
  proposalId?: string
  proposalMessageIndex?: number
  timestamp: string
}

export type IndexerEventRecord = {
  type: 'indexer-event'
  event_type: string
  did: string | null
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
    grantee?: string
    vs_operator?: string
    with_feegrant?: boolean
    proposal_id?: string
    proposal_message_index?: number
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
  // Position among same-type Msgs sharing this tx message, set for Msgs wrapped in a proposal.
  occurrence?: number
}

type EventMeta = {
  module: IndexerTxEvent['module']
  action: string
  entityType?: string
}

const MODULE_PARAMS_ENTITY_TYPE = 'ModuleParams'

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
  [VeranaGovernanceFrameworkMessageTypes.AddGovernanceFrameworkDocument]: {
    module: 'corporation',
    action: 'AddGovernanceFrameworkDocument',
    entityType: 'GovernanceFrameworkDocument',
  },
  [VeranaGovernanceFrameworkMessageTypes.IncreaseActiveGovernanceFrameworkVersion]: {
    module: 'corporation',
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
  [VeranaParticipantMessageTypes.TriggerResolver]: {
    module: 'participant',
    action: 'TriggerResolver',
    entityType: 'Participant',
  },
  [VeranaDiMessageTypes.StoreDigest]: {
    module: 'digital-identity',
    action: 'StoreDigest',
    entityType: 'DigitalIdentityDigest',
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
  [VeranaTrustDepositMessageTypes.ReclaimYield]: {
    module: 'trust-deposit',
    action: 'ReclaimTrustDepositYield',
    entityType: 'TrustDeposit',
  },
  [VeranaTrustDepositMessageTypes.SlashTrustDeposit]: {
    module: 'trust-deposit',
    action: 'SlashTrustDeposit',
    entityType: 'TrustDeposit',
  },
  [VeranaTrustDepositMessageTypes.RepaySlashed]: {
    module: 'trust-deposit',
    action: 'RepaySlashedTrustDeposit',
    entityType: 'TrustDeposit',
  },
  [VeranaExchangeRateMessageTypes.CreateExchangeRate]: {
    module: 'exchange-rate',
    action: 'CreateExchangeRate',
    entityType: 'ExchangeRate',
  },
  [VeranaExchangeRateMessageTypes.UpdateExchangeRate]: {
    module: 'exchange-rate',
    action: 'UpdateExchangeRate',
    entityType: 'ExchangeRate',
  },
  [VeranaExchangeRateMessageTypes.SetExchangeRateState]: {
    module: 'exchange-rate',
    action: 'SetExchangeRateState',
    entityType: 'ExchangeRate',
  },
  [VeranaExchangeRateMessageTypes.GrantExchangeRateAuthorization]: {
    module: 'exchange-rate',
    action: 'GrantExchangeRateAuthorization',
    entityType: 'ExchangeRateAuthorization',
  },
  [VeranaExchangeRateMessageTypes.RevokeExchangeRateAuthorization]: {
    module: 'exchange-rate',
    action: 'RevokeExchangeRateAuthorization',
    entityType: 'ExchangeRateAuthorization',
  },
  [VeranaEcosystemMessageTypes.UpdateParams]: {
    module: 'ecosystem',
    action: 'UpdateParams',
    entityType: MODULE_PARAMS_ENTITY_TYPE,
  },
  [VeranaCredentialSchemaMessageTypes.UpdateParams]: {
    module: 'credential-schema',
    action: 'UpdateParams',
    entityType: MODULE_PARAMS_ENTITY_TYPE,
  },
  [VeranaParticipantMessageTypes.UpdateParams]: {
    module: 'participant',
    action: 'UpdateParams',
    entityType: MODULE_PARAMS_ENTITY_TYPE,
  },
  [VeranaCorporationMessageTypes.UpdateParams]: {
    module: 'corporation',
    action: 'UpdateParams',
    entityType: MODULE_PARAMS_ENTITY_TYPE,
  },
  [VeranaGovernanceFrameworkMessageTypes.UpdateParams]: {
    module: 'governance-framework',
    action: 'UpdateParams',
    entityType: MODULE_PARAMS_ENTITY_TYPE,
  },
  [VeranaDiMessageTypes.UpdateParams]: {
    module: 'digital-identity',
    action: 'UpdateParams',
    entityType: MODULE_PARAMS_ENTITY_TYPE,
  },
  [VeranaDelegationMessageTypes.UpdateParams]: {
    module: 'delegation',
    action: 'UpdateParams',
    entityType: MODULE_PARAMS_ENTITY_TYPE,
  },
  [VeranaTrustDepositMessageTypes.UpdateParams]: {
    module: 'trust-deposit',
    action: 'UpdateParams',
    entityType: MODULE_PARAMS_ENTITY_TYPE,
  },
  [VeranaExchangeRateMessageTypes.UpdateParams]: {
    module: 'exchange-rate',
    action: 'UpdateParams',
    entityType: MODULE_PARAMS_ENTITY_TYPE,
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
  issuerParticipant: ['issuer_participant_id', 'issuerParticipantId'],
  verifierParticipant: ['verifier_participant_id', 'verifierParticipantId'],
  governanceFramework: ['gfv_id', 'gfvId', 'gfd_id', 'gfdId'],
  exchangeRate: ['xr_id', 'xrId'],
} as const

async function getEntityId(row: EventRow, meta: EventMeta): Promise<string | undefined> {
  if (meta.entityType === MODULE_PARAMS_ENTITY_TYPE) return undefined

  if (meta.module === 'exchange-rate') {
    const exchangeRateId = readNumber(row.content, ['id', ...ID_ALIASES.exchangeRate])
    return exchangeRateId ? String(exchangeRateId) : undefined
  }

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
    const createdId = await loadCreatedEntityId(row)
    if (createdId) return createdId
    const did = normalizeDid(content.did)
    if (!did) return undefined
    const ec = await knex('ecosystem').select('id').where({ did }).first()
    return ec?.id != null ? String(ec.id) : undefined
  }

  if (meta.module === 'credential-schema') {
    const createdId = await loadCreatedEntityId(row)
    if (createdId) return createdId
    const ecosystemId = readNumber(content, ID_ALIASES.ecosystem)
    const query = knex('credential_schema_history').select('credential_schema_id').where({ height })
    if (ecosystemId) query.andWhere({ ecosystem_id: ecosystemId })
    const cs = await query.orderBy('credential_schema_id', 'desc').first()
    return cs?.credential_schema_id != null ? String(cs.credential_schema_id) : undefined
  }

  if (meta.module === 'participant') {
    const createdId = await loadCreatedEntityId(row)
    if (createdId) return createdId
    const did = normalizeDid(content.did)
    const query = knex('participant_history').select('participant_id').where({ height })
    if (did) query.andWhere({ did })
    const pp = await query.orderBy('participant_id', 'desc').first()
    return pp?.participant_id != null ? String(pp.participant_id) : undefined
  }

  return undefined
}

const CREATED_ENTITY_EVENTS: Record<string, { eventType: string; idKey: string }> = {
  [VeranaCredentialSchemaMessageTypes.CreateCredentialSchema]: {
    eventType: 'create_credential_schema',
    idKey: 'credential_schema_id',
  },
  [VeranaEcosystemMessageTypes.CreateEcosystem]: { eventType: 'create_ecosystem', idKey: 'ecosystem_id' },
  [VeranaParticipantMessageTypes.CreateRootParticipant]: {
    eventType: 'create_root_participant',
    idKey: 'root_participant_id',
  },
  [VeranaParticipantMessageTypes.SelfCreateParticipant]: { eventType: 'create_participant', idKey: 'participant_id' },
  [VeranaParticipantMessageTypes.StartParticipantOP]: { eventType: 'start_participant_op', idKey: 'participant_id' },
}

// A create Msg carries no id; the chain reports it on the create event, one event per create in Msg order.
async function loadCreatedEntityId(row: EventRow): Promise<string | undefined> {
  const created = CREATED_ENTITY_EVENTS[row.message_type]
  if (!created) return undefined
  const attribute = await knex('event as e')
    .innerJoin('event_attribute as ea', 'ea.event_id', 'e.id')
    .where('e.tx_id', row.tx_id)
    .where('e.type', created.eventType)
    .whereRaw('COALESCE(e.tx_msg_index, 0) = ?', [Number(row.message_index)])
    .where('ea.key', created.idKey)
    .select('ea.value')
    .orderBy('e.id', 'asc')
    .offset(row.occurrence ?? 0)
    .first()
  const entityId = readPositiveInteger(parseEventAttrValue(attribute?.value))
  return entityId ? String(entityId) : undefined
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
  if (opts.address) {
    query.where(function () {
      this.where('corporation', opts.address).orWhere('policy_address', opts.address)
    })
  } else if (opts.did) query.where({ did: opts.did })
  else return {}
  const row = await query.first()
  if (!row) return {}
  return { did: normalizeDid(row.did), corporationId: toCorporationId(row.id) }
}

async function loadCorporationById(corporationId: number | null | undefined): Promise<{
  did?: string
  corporationId?: number
}> {
  if (!corporationId) return {}
  const row = await knex('corporation').select('id', 'did').where({ id: corporationId }).first()
  if (!row) return {}
  return { did: normalizeDid(row.did), corporationId: toCorporationId(row.id) }
}

async function loadParticipantsByAccount(account: string | null | undefined): Promise<{
  dids: string[]
  corporationIds: number[]
}> {
  if (!account || typeof account !== 'string' || !account.trim()) return { dids: [], corporationIds: [] }
  const rows = await knex('participants')
    .select('id', 'did', 'corporation_id')
    .where({ vs_operator: account })
    .orderBy('id', 'asc')

  const dids: string[] = []
  const corporationIds: number[] = []
  for (const row of rows) {
    const did = normalizeDid(row.did)
    if (did && !dids.includes(did)) dids.push(did)
    const corporationId = toCorporationId(row.corporation_id)
    if (corporationId !== undefined && !corporationIds.includes(corporationId)) corporationIds.push(corporationId)
  }
  return { dids, corporationIds }
}

function readAddress(value: unknown): string | undefined {
  if (typeof value !== 'string') return undefined
  const trimmed = value.trim()
  return trimmed ? trimmed : undefined
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

  let entityId = await getEntityId(row, meta)
  const content = row.content && typeof row.content === 'object' ? (row.content as Record<string, unknown>) : {}
  const collected = collectDidsDeep([row.sender, row.content])
  let ecosystemId: string | undefined
  let schemaId: string | undefined
  let participantId: string | undefined
  let corporationId: number | undefined
  let participantResourceDid: string | undefined
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
    const ecosystem = await loadEcosystem(rawEcosystemId ?? readPositiveInteger(entityId))
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
    // Start-class messages have no participant id; reuse the did-scoped one from getEntityId.
    const entityParticipantId =
      normalizeDid(content.did) && entityId && /^\d+$/.test(entityId) ? Number(entityId) : null
    const relation = await loadParticipantRelation(rawParticipantId ?? entityParticipantId)
    participantResourceDid = relation.participantDid
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
    const rawIssuerParticipantId = readNumber(row.content, ID_ALIASES.issuerParticipant)
    const rawVerifierParticipantId = readNumber(row.content, ID_ALIASES.verifierParticipant)
    for (const sessionParticipantId of [rawIssuerParticipantId, rawVerifierParticipantId]) {
      if (!sessionParticipantId) continue
      const sessionRelation = await loadParticipantRelation(sessionParticipantId)
      participantId = participantId ?? sessionRelation.participantId
      participantResourceDid = participantResourceDid ?? sessionRelation.participantDid
      ecosystemId = ecosystemId ?? sessionRelation.ecosystemId
      schemaId = schemaId ?? sessionRelation.schemaId
      if (sessionRelation.corporationId !== undefined) {
        if (corporationId === undefined) corporationId = sessionRelation.corporationId
        else if (sessionRelation.corporationId !== corporationId)
          relatedCorporationIds.add(sessionRelation.corporationId)
      }
      ;[sessionRelation.participantDid, sessionRelation.ecosystemDid].forEach((did) => {
        if (did) collected.add(did)
      })
    }
  }

  if (meta.module === 'trust-deposit') {
    const slashedCorporationId = readNumber(content, ['corporation_id', 'corporationId'])
    const relation = slashedCorporationId
      ? await loadCorporationById(slashedCorporationId)
      : await loadCorporation({ address: extractController(content, row.sender) })
    corporationId = relation.corporationId ?? slashedCorporationId ?? undefined
    if (relation.did) collected.add(relation.did)
    entityId = corporationId !== undefined ? String(corporationId) : entityId
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
  // Participant events are labeled with Participant.did, never a sorted related DID.
  const participantPrimaryDid =
    meta.module === 'participant'
      ? (firstNormalizedDid([content.did, content.participant_did, content.participantDid]) ?? participantResourceDid)
      : undefined
  const primaryDid = participantPrimaryDid ?? explicitPrimaryDid ?? firstNormalizedDid(relatedDids) ?? null

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
      action: _.snakeCase(event.action),
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
      grantee: event.grantee,
      vs_operator: event.vsOperator,
      with_feegrant: event.withFeegrant,
      proposal_id: event.proposalId,
      proposal_message_index: event.proposalMessageIndex,
    },
  }
}

function fromStoredRow(row: Record<string, any>): IndexerEventRecord {
  return {
    type: 'indexer-event',
    event_type: String(row.event_type),
    did: row.did != null ? String(row.did) : null,
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
          : row.did != null
            ? [String(row.did)]
            : [],
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
      grantee: row.payload?.grantee ?? undefined,
      vs_operator: row.payload?.vs_operator ?? row.payload?.vsOperator ?? undefined,
      with_feegrant:
        typeof row.payload?.with_feegrant === 'boolean'
          ? row.payload.with_feegrant
          : typeof row.payload?.withFeegrant === 'boolean'
            ? row.payload.withFeegrant
            : undefined,
      proposal_id: row.payload?.proposal_id ?? undefined,
      proposal_message_index:
        typeof row.payload?.proposal_message_index === 'number' ? row.payload.proposal_message_index : undefined,
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

const DELEGATION_EVENT_META: Record<string, { action: string; entityType: string }> = {
  grant_operator_authorization: {
    action: 'GrantOperatorAuthorization',
    entityType: 'OperatorAuthorization',
  },
  revoke_operator_authorization: {
    action: 'RevokeOperatorAuthorization',
    entityType: 'OperatorAuthorization',
  },
  grant_vs_operator_authorization: {
    action: 'GrantVSOperatorAuthorization',
    entityType: 'VSOperatorAuthorization',
  },
  revoke_vs_operator_authorization: {
    action: 'RevokeVSOperatorAuthorization',
    entityType: 'VSOperatorAuthorization',
  },
  update_vs_operator_authorization: {
    action: 'UpdateVSOperatorAuthorization',
    entityType: 'VSOperatorAuthorization',
  },
}

const DELEGATION_EVENT_TYPES = Object.keys(DELEGATION_EVENT_META)

const DELEGATION_ATTRIBUTE_KEYS = [
  'authz_id',
  'vsoa_id',
  'corporation_id',
  'participant_id',
  'grantee',
  'vs_operator',
  'with_feegrant',
]

type DelegationEventRow = {
  event_id: number
  type: string
  tx_msg_index: number | null
  tx_hash: string
  tx_index: number
  block_height: number
  sender: string | null
  timestamp: Date | string
}

async function resolveCorporationDid(corporationId: number, blockHeight: number): Promise<string | undefined> {
  const corporation = await loadCorporationById(corporationId)
  if (corporation.did) return corporation.did
  return (await fetchCorporationById(corporationId, blockHeight)).did
}

async function buildDelegationEvents(blockHeight: number): Promise<IndexerTxEvent[]> {
  const eventRows = (await knex('event as e')
    .innerJoin('transaction as tx', 'tx.id', 'e.tx_id')
    .leftJoin('transaction_message as tm', function () {
      this.on('tm.tx_id', '=', 'e.tx_id').andOn(knex.raw('tm.index = COALESCE(e.tx_msg_index, 0)'))
    })
    .where('e.block_height', blockHeight)
    .andWhere('tx.code', 0)
    .whereIn('e.type', DELEGATION_EVENT_TYPES)
    .select(
      'e.id as event_id',
      'e.type',
      'e.tx_msg_index',
      'tx.hash as tx_hash',
      'tx.index as tx_index',
      'tx.height as block_height',
      'tm.sender as sender',
      'tx.timestamp'
    )
    .orderBy('tx.index', 'asc')
    .orderBy('e.id', 'asc')) as DelegationEventRow[]

  if (eventRows.length === 0) return []

  const eventIds = eventRows.map((row) => row.event_id)
  const attributeRows = (await knex('event_attribute')
    .where('block_height', blockHeight)
    .whereIn('event_id', eventIds)
    .whereIn('key', DELEGATION_ATTRIBUTE_KEYS)
    .select('event_id', 'key', 'value')) as Array<{ event_id: number; key: string; value: string }>

  const attributesByEvent = new Map<number, Record<string, string>>()
  for (const attribute of attributeRows) {
    const bucket = attributesByEvent.get(attribute.event_id) ?? {}
    bucket[attribute.key] = attribute.value
    attributesByEvent.set(attribute.event_id, bucket)
  }

  const events: IndexerTxEvent[] = []
  for (const row of eventRows) {
    const meta = DELEGATION_EVENT_META[row.type]
    const attributes = attributesByEvent.get(row.event_id) ?? {}
    const entityId = attributes.authz_id ?? attributes.vsoa_id
    const grantee = readAddress(attributes.grantee)
    const vsOperator = readAddress(attributes.vs_operator)
    const rawParticipantId = readPositiveInteger(attributes.participant_id)
    const corporationId = toCorporationId(attributes.corporation_id)
    const withFeegrant = attributes.with_feegrant === undefined ? undefined : attributes.with_feegrant === 'true'

    const collected = new Set<string>()
    const relatedCorporationIds = new Set<number>()
    let did: string | undefined

    if (rawParticipantId) {
      const participant = await loadParticipantRelation(rawParticipantId)
      if (participant.participantDid) collected.add(participant.participantDid)
      if (participant.ecosystemDid) collected.add(participant.ecosystemDid)
      did = participant.participantDid ?? (await fetchParticipantDid(rawParticipantId, blockHeight))
      if (did) collected.add(did)
    }

    if (corporationId) {
      const corporationDid = await resolveCorporationDid(corporationId, blockHeight)
      if (corporationDid) collected.add(corporationDid)
      did = did ?? corporationDid
    }

    for (const account of [grantee, vsOperator]) {
      const participants = await loadParticipantsByAccount(account)
      for (const participantDid of participants.dids) collected.add(participantDid)
      for (const participantCorporationId of participants.corporationIds) {
        if (participantCorporationId !== corporationId) relatedCorporationIds.add(participantCorporationId)
      }
      did = did ?? participants.dids[0]
    }

    if (!did) {
      console.warn(
        `[IndexerEvents] ${meta.action} at block_height=${blockHeight} tx_hash=${row.tx_hash} has no resolvable DID; emitting with did=null`
      )
    }

    events.push({
      type: 'transaction-executed',
      module: 'delegation',
      action: meta.action,
      messageType: row.type,
      blockHeight: Number(row.block_height),
      txHash: row.tx_hash,
      txIndex: Number(row.tx_index),
      messageIndex: Number(row.tx_msg_index ?? 0),
      sender: row.sender ?? '',
      did: did ?? null,
      relatedDids: uniqueNormalizedDids(collected),
      entityType: meta.entityType,
      entityId,
      participantId: rawParticipantId ? String(rawParticipantId) : undefined,
      corporationId,
      relatedCorporationIds: [...relatedCorporationIds],
      grantee,
      vsOperator,
      withFeegrant,
      timestamp: toIsoSeconds(row.timestamp),
    })
  }
  return events
}

// Anchors on the corporation table itself, so routing does not wait for the (separately checkpointed)
// group processor to fill corporation_group.
async function loadCorporationByPolicyAddress(policyAddress: string): Promise<{
  corporationId?: number
  did?: string
}> {
  const row = await knex('corporation')
    .where(function () {
      this.where('policy_address', policyAddress).orWhere('corporation', policyAddress)
    })
    .select('id', 'did')
    .first()
  if (!row) return {}
  return { corporationId: toCorporationId(row.id), did: normalizeDid(row.did) }
}

// Falls back to the create events so routing never waits for the group processor's checkpoint.
async function loadCorporationByGroupId(groupId: number): Promise<{ corporationId?: number; did?: string }> {
  const row = await knex('corporation_group as cg')
    .innerJoin('corporation as co', 'co.id', 'cg.corporation_id')
    .where('cg.group_id', groupId)
    .select('cg.corporation_id', 'co.did')
    .first()
  if (row) return { corporationId: toCorporationId(row.corporation_id), did: normalizeDid(row.did) }

  const createGroup = await knex('event as e')
    .innerJoin('event_attribute as ea', 'ea.event_id', 'e.id')
    .where('e.type', 'cosmos.group.v1.EventCreateGroup')
    .where('ea.key', 'group_id')
    .where('ea.value', JSON.stringify(String(groupId)))
    .select('e.id', 'e.tx_id', 'e.tx_msg_index')
    .orderBy('e.id', 'asc')
    .first()
  if (!createGroup?.tx_id) return {}

  // Same tx and message index: one tx can create several corporations.
  const policyAddress = await knex('event as e')
    .innerJoin('event_attribute as ea', 'ea.event_id', 'e.id')
    .where('e.type', 'cosmos.group.v1.EventCreateGroupPolicy')
    .where('e.tx_id', createGroup.tx_id)
    .whereRaw('COALESCE(e.tx_msg_index, 0) = ?', [Number(createGroup.tx_msg_index ?? 0)])
    .where('ea.key', 'address')
    .where('e.id', '>', Number(createGroup.id))
    .select('ea.value')
    .orderBy('e.id', 'asc')
    .first()
  const address = String(policyAddress?.value ?? '').replace(/^"|"$/g, '')
  return address ? loadCorporationByPolicyAddress(address) : {}
}

async function loadSubmitProposalContent(proposalId: number): Promise<Record<string, unknown> | undefined> {
  const submitAttr = await knex('event_attribute as ea')
    .innerJoin('event as e', 'e.id', 'ea.event_id')
    .where('e.type', 'cosmos.group.v1.EventSubmitProposal')
    .where('ea.key', 'proposal_id')
    .where('ea.value', JSON.stringify(String(proposalId)))
    .select('e.tx_id', 'e.tx_msg_index')
    .first()
  if (!submitAttr?.tx_id) return undefined
  // Match the submitting message by index: one tx can batch several MsgSubmitProposal for different policies.
  const submitMessage = await knex('transaction_message')
    .where('tx_id', submitAttr.tx_id)
    .where('type', '/cosmos.group.v1.MsgSubmitProposal')
    .where('index', Number(submitAttr.tx_msg_index ?? 0))
    .select('content')
    .first()
  return submitMessage?.content as Record<string, unknown> | undefined
}

type LoadedProposal = { policyAddress?: string; messages: Array<Record<string, unknown>> }
type ProposalCache = Map<number, Promise<LoadedProposal | undefined>>

// Submit tx first: group_proposal lags this block's events; it is the fallback when the submit tx is not indexed.
export async function readProposal(proposalId: number): Promise<LoadedProposal | undefined> {
  const content = await loadSubmitProposalContent(proposalId)
  if (content) {
    return {
      policyAddress: readAddress(content.group_policy_address),
      messages: decodeProposalMessages(content.messages),
    }
  }
  const proposal = await knex('group_proposal')
    .select('group_policy_address', 'messages')
    .where('id', proposalId)
    .first()
  if (!proposal) return undefined
  return {
    policyAddress: readAddress(proposal.group_policy_address),
    messages: Array.isArray(proposal.messages) ? proposal.messages : [],
  }
}

type ExecEventRow = {
  event_id: number
  tx_id: number
  tx_msg_index: number | null
  tx_hash: string
  tx_index: number
  block_height: number
  timestamp: Date | string
}

// One cache per block: several Exec and Vote events in a block often resolve the same proposal.
function loadProposal(proposalId: number, cache: ProposalCache): Promise<LoadedProposal | undefined> {
  let proposal = cache.get(proposalId)
  if (!proposal) {
    proposal = readProposal(proposalId)
    cache.set(proposalId, proposal)
  }
  return proposal
}

// Msgs an authz MsgExec runs have no transaction_message row of their own when the block's events are built.
async function buildAuthzExecEvents(blockHeight: number): Promise<IndexerTxEvent[]> {
  const rows = (await knex('transaction_message as tm')
    .innerJoin('transaction as tx', 'tx.id', 'tm.tx_id')
    .where('tx.height', blockHeight)
    .andWhere('tx.code', 0)
    .where('tm.type', AUTHZ_EXEC_TYPE)
    .whereNull('tm.parent_id')
    .select(
      'tm.id as message_id',
      'tm.tx_id',
      'tm.index as message_index',
      'tm.sender',
      'tm.content',
      'tx.height as block_height',
      'tx.hash as tx_hash',
      'tx.index as tx_index',
      'tx.timestamp'
    )
    .orderBy('tx.index', 'asc')
    .orderBy('tm.index', 'asc')) as EventRow[]

  const events: IndexerTxEvent[] = []
  for (const row of rows) {
    const inner = decodeProposalMessages((row.content as { msgs?: unknown } | null)?.msgs)
    for (const [index, message] of inner.entries()) {
      const messageType = anyTypeUrl(message)
      if (!messageType || !EVENT_META[messageType]) continue
      const occurrence = inner.slice(0, index).filter((earlier) => anyTypeUrl(earlier) === messageType).length
      const event = await toIndexerEvent({ ...row, message_type: messageType, content: message, occurrence })
      if (event) events.push(event)
    }
  }
  return events
}

// Msgs a proposal executes (MsgExec, or MsgSubmitProposal/MsgVote with EXEC_TRY) never get a transaction_message row.
async function buildProposalExecEvents(
  blockHeight: number,
  proposals: ProposalCache = new Map()
): Promise<IndexerTxEvent[]> {
  const execRows = (await knex('event as e')
    .innerJoin('transaction as tx', 'tx.id', 'e.tx_id')
    .where('e.block_height', blockHeight)
    .andWhere('tx.code', 0)
    .where('e.type', GROUP_EVENT_TYPES.Exec)
    .select(
      'e.id as event_id',
      'e.tx_id',
      'e.tx_msg_index',
      'tx.hash as tx_hash',
      'tx.index as tx_index',
      'tx.height as block_height',
      'tx.timestamp'
    )
    .orderBy('tx.index', 'asc')
    .orderBy('e.id', 'asc')) as ExecEventRow[]
  if (execRows.length === 0) return []

  const attributeRows = (await knex('event_attribute')
    .where('block_height', blockHeight)
    .whereIn(
      'event_id',
      execRows.map((row) => row.event_id)
    )
    .whereIn('key', ['proposal_id', 'result'])
    .select('event_id', 'key', 'value')) as Array<{ event_id: number; key: string; value: string }>
  const attributesByEvent = new Map<number, Record<string, string>>()
  for (const attribute of attributeRows) {
    const bucket = attributesByEvent.get(attribute.event_id) ?? {}
    bucket[attribute.key] = attribute.value
    attributesByEvent.set(attribute.event_id, bucket)
  }

  const events: IndexerTxEvent[] = []
  for (const row of execRows) {
    const attributes = attributesByEvent.get(row.event_id) ?? {}
    // FAILURE rolls the wrapped Msgs back; the proposal stays executable and a later SUCCESS emits its own EventExec.
    if (shortExecutorResult(parseEventAttrValue(attributes.result)) !== 'SUCCESS') continue
    const proposalId = readPositiveInteger(parseEventAttrValue(attributes.proposal_id))
    if (!proposalId) continue
    const proposal = await loadProposal(proposalId, proposals)
    if (!proposal) continue

    for (const [index, message] of proposal.messages.entries()) {
      const messageType = anyTypeUrl(message)
      if (!messageType) continue
      const isGroupAdmin = GROUP_ADMIN_MESSAGE_TYPES.includes(messageType)
      if (!EVENT_META[messageType] && !isGroupAdmin) continue
      const occurrence = proposal.messages
        .slice(0, index)
        .filter((earlier) => anyTypeUrl(earlier) === messageType).length
      const eventRow: EventRow = {
        message_id: row.event_id,
        tx_id: row.tx_id,
        message_index: Number(row.tx_msg_index ?? 0),
        message_type: messageType,
        sender: proposal.policyAddress ?? '',
        content: Utils.camelizeKeys(message),
        block_height: row.block_height,
        tx_hash: row.tx_hash,
        tx_index: row.tx_index,
        timestamp: row.timestamp,
        occurrence,
      }
      const event = isGroupAdmin ? await toGroupEvent(eventRow, proposals) : await toIndexerEvent(eventRow)
      if (event) events.push({ ...event, proposalId: String(proposalId), proposalMessageIndex: index })
    }
  }
  return events
}

// MsgSubmitProposal carries no proposal id (assigned on-chain) — read it from the tx's own event.
async function resolveSubmittedProposalId(txHash: string, messageIndex: number): Promise<string | undefined> {
  const row = await knex('event as e')
    .innerJoin('transaction as tx', 'tx.id', 'e.tx_id')
    .innerJoin('event_attribute as ea', 'ea.event_id', 'e.id')
    .where('tx.hash', txHash)
    .where('e.type', 'cosmos.group.v1.EventSubmitProposal')
    .where('ea.key', 'proposal_id')
    .select('ea.value', 'e.tx_msg_index')
    .orderBy('e.id', 'asc')
  const match = row.find((entry) => Number(entry.tx_msg_index ?? 0) === messageIndex) ?? row[0]
  if (!match) return undefined
  const value = String(match.value ?? '').replace(/^"|"$/g, '')
  return /^\d+$/.test(value) ? value : undefined
}

async function buildGroupEvents(blockHeight: number, proposals: ProposalCache = new Map()): Promise<IndexerTxEvent[]> {
  const rows = (await knex('transaction_message as tm')
    .innerJoin('transaction as tx', 'tx.id', 'tm.tx_id')
    .where('tx.height', blockHeight)
    .andWhere('tx.code', 0)
    .whereIn('tm.type', GROUP_ROUTED_MESSAGE_TYPES)
    .select(
      'tm.index as message_index',
      'tm.type as message_type',
      'tm.sender',
      'tm.content',
      'tx.height as block_height',
      'tx.hash as tx_hash',
      'tx.index as tx_index',
      'tx.timestamp'
    )
    .orderBy('tx.index', 'asc')
    .orderBy('tm.index', 'asc')) as EventRow[]
  const events: IndexerTxEvent[] = []
  for (const row of rows) events.push(await toGroupEvent(row, proposals))
  return events
}

async function toGroupEvent(row: EventRow, proposals: ProposalCache): Promise<IndexerTxEvent> {
  const content = row.content && typeof row.content === 'object' ? (row.content as Record<string, unknown>) : {}
  const action = toShortMessageType(row.message_type).replace(/^Msg/, '')

  let anchor: { corporationId?: number; did?: string } = {}
  let entityType = 'CorporationGroup'
  let entityId: string | undefined
  const policyAddress = readAddress(content.group_policy_address)
  const groupId = readFirstPositiveInteger(content, ['group_id', 'groupId'])
  const proposalId = readFirstPositiveInteger(content, ['proposal_id', 'proposalId'])
  if (policyAddress) {
    anchor = await loadCorporationByPolicyAddress(policyAddress)
  } else if (groupId) {
    anchor = await loadCorporationByGroupId(groupId)
  } else if (proposalId) {
    const resolvedAddress = (await loadProposal(proposalId, proposals))?.policyAddress
    if (resolvedAddress) anchor = await loadCorporationByPolicyAddress(resolvedAddress)
    entityType = 'GroupProposal'
    entityId = String(proposalId)
  }
  if (row.message_type === '/cosmos.group.v1.MsgSubmitProposal') {
    entityType = 'GroupProposal'
    entityId = await resolveSubmittedProposalId(row.tx_hash, Number(row.message_index))
  }
  // CorporationGroup entities are keyed by corporation_id; group_id lives in a different id space.
  if (entityType === 'CorporationGroup' && anchor.corporationId) entityId = String(anchor.corporationId)

  // Group events anchoring no Corporation stay wildcard-only (did null, no corporation scope).
  return {
    type: 'transaction-executed',
    module: 'group',
    action,
    messageType: row.message_type,
    blockHeight: Number(row.block_height),
    txHash: row.tx_hash,
    txIndex: Number(row.tx_index),
    messageIndex: Number(row.message_index),
    sender: row.sender ?? '',
    did: anchor.did ?? null,
    relatedDids: anchor.did ? [anchor.did] : [],
    entityType,
    entityId,
    corporationId: anchor.corporationId,
    relatedCorporationIds: [],
    timestamp: toIsoSeconds(row.timestamp),
  }
}

export const buildGroupEventsForTest = buildGroupEvents
export const buildProposalExecEventsForTest = buildProposalExecEvents

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
  const delegationEvents = await buildDelegationEvents(blockHeight)
  rows.push(...delegationEvents.map(toEventRow))
  const proposals: ProposalCache = new Map()
  const groupEvents = await buildGroupEvents(blockHeight, proposals)
  rows.push(...groupEvents.map(toEventRow))
  const proposalExecEvents = await buildProposalExecEvents(blockHeight, proposals)
  rows.push(...proposalExecEvents.map(toEventRow))
  const authzExecEvents = await buildAuthzExecEvents(blockHeight)
  rows.push(...authzExecEvents.map(toEventRow))
  let insertedIds: number[] = []

  if (rows.length > 0) {
    const inserted = await knex('indexer_events')
      .insert(rows)
      .onConflict(
        knex.raw(
          "(tx_hash, tx_index, message_index, event_type, entity_type, COALESCE(entity_id, ''), COALESCE(payload ->> 'proposal_id', ''), COALESCE(payload ->> 'proposal_message_index', ''))"
        )
      )
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

  return listIndexerEventsByIds(insertedIds)
}

export async function listIndexerEventsForBlock(blockHeight: number): Promise<IndexerEventRecord[]> {
  const rows = (await knex('indexer_events').select('id').where('block_height', blockHeight).orderBy('id')) as Array<{
    id: number | string
  }>
  return listIndexerEventsByIds(rows.map((row) => Number(row.id)))
}

async function listIndexerEventsByIds(ids: number[]): Promise<IndexerEventRecord[]> {
  const results: IndexerEventRecord[] = []
  const chunkSize = 500
  for (let i = 0; i < ids.length; i += chunkSize) {
    const chunk = ids.slice(i, i + chunkSize)
    results.push(...(await listIndexerEvents({ ids: chunk, limit: chunk.length })))
  }
  return results.sort(
    (a, b) => a.payload.tx_index - b.payload.tx_index || a.payload.message_index - b.payload.message_index
  )
}

export async function listIndexerEvents(args: {
  afterBlockHeight?: number
  blockHeight?: number
  dids?: string[]
  corporationId?: number
  ids?: number[]
  limit?: number
}): Promise<IndexerEventRecord[]> {
  const limit = Math.max(1, Math.min(500, Math.floor(Number(args.limit ?? 100))))
  const normalizedDids = uniqueNormalizedDids(args.dids ?? [])
  const didFilterRequested = (args.dids?.length ?? 0) > 0
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
  await resolveMissingDids(rows)
  return rows.map(fromStoredRow)
}

async function resolveMissingDids(rows: Array<Record<string, any>>): Promise<void> {
  for (const row of rows) {
    if (row.did != null) continue

    const participantId = readPositiveInteger(row.payload?.participant_id ?? row.payload?.participantId)
    const corporationId = toCorporationId(row.payload?.corporation_id ?? row.payload?.corporationId)

    let did: string | undefined
    if (participantId) did = (await loadParticipantRelation(participantId)).participantDid
    if (!did && corporationId) did = (await loadCorporationById(corporationId)).did
    if (!did) continue

    row.did = did
    await knex('indexer_events').where({ id: row.id }).update({ did })
  }
}
