import { Action, Service } from '@ourparentcenter/moleculer-decorators-extended'
import fs from 'fs'
import type { Knex } from 'knex'
import { Context, type LoggerInstance, ServiceBroker } from 'moleculer'
import path from 'path'
import BaseService from '../../base/base.service'
import { SERVICE } from '../../common'
import ApiResponder from '../../common/utils/apiResponse'
import { getLatestIndexedBlockTime } from '../../common/utils/block_time'
import { parseDatetimeParam, toDate } from '../../common/utils/date_utils'
import knex from '../../common/utils/db_connection'
import { Network } from '../../network'
import { calculateParticipantState, type ParticipantData, type ParticipantType } from '../crawl-pp/pp_state_utils'

const TRQP_PROFILE_PATH = path.join(process.cwd(), 'docs', 'api', 'schemas', 'v4', 'trqp', 'profile.json')

let trqpProfileBody: string | undefined

function readTrqpProfileBody(): string {
  if (trqpProfileBody === undefined) {
    trqpProfileBody = fs.readFileSync(TRQP_PROFILE_PATH, 'utf8')
  }
  return trqpProfileBody
}

const ACTION_ROLE_MAP: Record<string, ParticipantType> = {
  issue: 'ISSUER',
  verify: 'VERIFIER',
  grant_issue: 'ISSUER_GRANTOR',
  grant_verify: 'VERIFIER_GRANTOR',
  govern: 'ECOSYSTEM',
}

const RESOURCE_PATTERN = /^vpr:verana:([a-z0-9-]+):cs:([0-9]+)$/

interface TrqpQueryParams {
  authority_id: string
  entity_id: string
  action: string
  resource: string
  context?: { time?: string; session_id?: string }
}

interface MatchedParticipant {
  role: ParticipantType
  deposit: number | string | null
  modified: Date | string | null
  effective_from: Date | string | null
  effective_until: Date | string | null
  revoked: Date | string | null
  slashed: Date | string | null
  repaid: Date | string | null
}

function toIsoZ(value: Date | string | null | undefined): string | null {
  if (value === null || value === undefined) return null
  const date = value instanceof Date ? value : new Date(value)
  if (Number.isNaN(date.getTime())) return null
  return date.toISOString().replace(/\.\d{3}Z$/, 'Z')
}

function stateSince(row: MatchedParticipant, state: string): string | null {
  switch (state) {
    case 'REPAID':
      return toIsoZ(row.repaid)
    case 'SLASHED':
      return toIsoZ(row.slashed)
    case 'REVOKED':
      return toIsoZ(row.revoked)
    case 'EXPIRED':
      return toIsoZ(row.effective_until)
    case 'ACTIVE':
    case 'FUTURE':
      return toIsoZ(row.effective_from)
    default:
      return toIsoZ(row.modified)
  }
}

type ResolvedInstant = { ok: true; value: Date } | { ok: false; message: string }

// context.time omitted: the spec evaluates against the latest block, never the service clock.
async function resolveEvaluatedAt(
  time: string | undefined,
  logContext: string,
  logger: LoggerInstance
): Promise<ResolvedInstant> {
  if (!time) return { ok: true, value: await getLatestIndexedBlockTime({ logContext, logger }) }
  const parsed = parseDatetimeParam(time, 'context.time')
  return parsed.ok ? { ok: true, value: new Date(parsed.value) } : parsed
}

function activeAt<Q extends Knex.QueryBuilder>(query: Q, at: Date): Q {
  return query
    .where('created', '<=', at)
    .where((builder) => builder.whereNull('archived').orWhere('archived', '>', at)) as Q
}

async function sessionDenial(
  sessionId: string,
  corporationDid: string,
  requestedTime: Date | undefined
): Promise<string | undefined> {
  const corporation = await knex('corporation').select('id').where({ did: corporationDid }).first()
  const session = corporation
    ? await knex('participant_sessions')
        .select('created')
        .where({ id: sessionId, corporation_id: corporation.id })
        .first()
    : undefined
  if (!session) return 'session not found'
  // VPR sessions never close, so the window opens at creation and has no end.
  const created = toDate(session.created)
  if (requestedTime && created && requestedTime < created) return 'session out of window'
  return undefined
}

@Service({
  name: SERVICE.V1.TrqpApiService.key,
  version: 1,
})
export default class TrqpApiService extends BaseService {
  public constructor(public broker: ServiceBroker) {
    super(broker)
  }

  @Action({
    rest: 'GET profile',
  })
  async getProfile(ctx: Context) {
    ;(ctx.meta as any).$rawJsonResponse = true
    return readTrqpProfileBody()
  }

  @Action({
    rest: 'POST authorization',
    params: {
      authority_id: { type: 'string' },
      entity_id: { type: 'string' },
      action: { type: 'enum', values: ['issue', 'verify', 'grant_issue', 'grant_verify', 'govern'] },
      resource: { type: 'string' },
      context: { type: 'object', optional: true },
    },
  })
  async authorize(ctx: Context<TrqpQueryParams>) {
    try {
      const { authority_id: authorityId, entity_id: entityId, action, resource } = ctx.params
      const requestContext = ctx.params.context ?? {}

      const role = ACTION_ROLE_MAP[action]

      const resourceMatch = RESOURCE_PATTERN.exec(resource)
      if (!resourceMatch) {
        return ApiResponder.error(ctx, 'Invalid "resource": expected vpr:verana:<network>:cs:<id>', 400)
      }
      const resourceNetwork = resourceMatch[1]
      const schemaId = Number(resourceMatch[2])

      const instant = await resolveEvaluatedAt(requestContext.time, '[trqp:authorize]', this.logger)
      if (!instant.ok) {
        return ApiResponder.error(ctx, instant.message, 400)
      }
      const evaluatedAt = instant.value
      const timeEvaluated = toIsoZ(evaluatedAt)

      const baseResponse = {
        authority_id: authorityId,
        entity_id: entityId,
        action,
        resource,
        time_requested: timeEvaluated,
        time_evaluated: timeEvaluated,
      }

      if (requestContext.session_id) {
        const denial = await sessionDenial(
          requestContext.session_id,
          entityId,
          requestContext.time ? evaluatedAt : undefined
        )
        if (denial) {
          return ApiResponder.success(ctx, { ...baseResponse, authorized: false, message: denial }, 200)
        }
      }

      if (resourceNetwork !== Network.chainId) {
        return ApiResponder.success(ctx, { ...baseResponse, authorized: false }, 200)
      }

      const candidates = (await knex('participants as p')
        .join('credential_schemas as cs', 'p.schema_id', 'cs.id')
        .join('ecosystem as e', 'cs.ecosystem_id', 'e.id')
        .join('corporation as c', 'p.corporation_id', 'c.id')
        .where('e.did', authorityId)
        .where('c.did', entityId)
        .where('p.role', role)
        .where('cs.id', schemaId)
        .select(
          'p.role as role',
          'p.deposit as deposit',
          'p.modified as modified',
          'p.effective_from as effective_from',
          'p.effective_until as effective_until',
          'p.revoked as revoked',
          'p.slashed as slashed',
          'p.repaid as repaid'
        )) as MatchedParticipant[]

      if (candidates.length === 0) {
        return ApiResponder.success(ctx, { ...baseResponse, authorized: false }, 200)
      }

      const evaluated = candidates.map((row) => ({
        row,
        state: calculateParticipantState(row as unknown as ParticipantData, evaluatedAt),
      }))
      const grounding = evaluated.find((entry) => entry.state === 'ACTIVE') ?? evaluated[0]
      const authorized = grounding.state === 'ACTIVE'

      return ApiResponder.success(
        ctx,
        {
          ...baseResponse,
          authorized,
          verana: {
            participant_state: grounding.state,
            since: stateSince(grounding.row, grounding.state),
            deposit: `${grounding.row.deposit ?? 0}uvna`,
          },
        },
        200
      )
    } catch (err: any) {
      this.logger.error('Error in TRQP.authorize:', err)
      return ApiResponder.error(ctx, `Failed to evaluate TRQP authorization: ${err?.message || String(err)}`, 500)
    }
  }

  @Action({
    rest: 'POST recognition',
    params: {
      authority_id: { type: 'string' },
      entity_id: { type: 'string' },
      action: { type: 'enum', values: ['issue', 'verify', 'grant_issue', 'grant_verify', 'govern'] },
      resource: { type: 'string' },
      context: { type: 'object', optional: true },
    },
  })
  async recognize(ctx: Context<TrqpQueryParams>) {
    try {
      const { authority_id: authorityId, entity_id: entityId, action, resource } = ctx.params
      const requestContext = ctx.params.context ?? {}

      const resourceMatch = RESOURCE_PATTERN.exec(resource)
      if (!resourceMatch) {
        return ApiResponder.error(ctx, 'Invalid "resource": expected vpr:verana:<network>:cs:<id>', 400)
      }
      const resourceNetwork = resourceMatch[1]
      const schemaId = Number(resourceMatch[2])

      const instant = await resolveEvaluatedAt(requestContext.time, '[trqp:recognize]', this.logger)
      if (!instant.ok) {
        return ApiResponder.error(ctx, instant.message, 400)
      }
      const evaluatedAt = instant.value
      const timeEvaluated = toIsoZ(evaluatedAt)

      const baseResponse = {
        authority_id: authorityId,
        entity_id: entityId,
        action,
        resource,
        time_requested: timeEvaluated,
        time_evaluated: timeEvaluated,
      }

      if (requestContext.session_id) {
        const denial = await sessionDenial(
          requestContext.session_id,
          authorityId,
          requestContext.time ? evaluatedAt : undefined
        )
        if (denial) {
          return ApiResponder.success(ctx, { ...baseResponse, recognized: false, message: denial }, 200)
        }
      }

      const corporation = (await knex('corporation')
        .select('id')
        .where({ did: authorityId })
        .where('created', '<=', evaluatedAt)
        .first()) as { id: number } | undefined
      const ecosystem = (await activeAt(
        knex('ecosystem').select('id', 'corporation_id', 'created', 'active_version').where({ did: entityId }),
        evaluatedAt
      ).first()) as
        | { id: number; corporation_id: number; created: Date | string | null; active_version: number | null }
        | undefined
      if (!corporation || !ecosystem) {
        return ApiResponder.success(
          ctx,
          { ...baseResponse, recognized: false, message: 'out of v4 recognition scope (corporation → ecosystem only)' },
          200
        )
      }

      if (ecosystem.corporation_id !== corporation.id) {
        return ApiResponder.success(ctx, { ...baseResponse, recognized: false }, 200)
      }

      const schema =
        resourceNetwork === Network.chainId
          ? await activeAt(
              knex('credential_schemas').select('id').where({ id: schemaId, ecosystem_id: ecosystem.id }),
              evaluatedAt
            ).first()
          : undefined

      return ApiResponder.success(
        ctx,
        {
          ...baseResponse,
          recognized: !!schema,
          verana: {
            ecosystem_active_egf_version: ecosystem.active_version ?? 0,
            controlling_since: toIsoZ(ecosystem.created),
          },
        },
        200
      )
    } catch (err: any) {
      this.logger.error('Error in TRQP.recognize:', err)
      return ApiResponder.error(ctx, `Failed to evaluate TRQP recognition: ${err?.message || String(err)}`, 500)
    }
  }
}
