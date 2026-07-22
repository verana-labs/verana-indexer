import { Action, Service } from '@ourparentcenter/moleculer-decorators-extended'
import { Context, ServiceBroker } from 'moleculer'
import BaseService from '../../base/base.service'
import { SERVICE } from '../../common'
import ApiResponder from '../../common/utils/apiResponse'
import knex from '../../common/utils/db_connection'
import { Network } from '../../network'
import { calculateParticipantState, type ParticipantData, type ParticipantType } from '../crawl-pp/pp_state_utils'
import trqpProfileDescriptor from './trqp/profile.json' with { type: 'json' }

const TRQP_PROFILE_BODY = `${JSON.stringify(trqpProfileDescriptor, null, 2)}\n`

const ACTION_ROLE_MAP: Record<string, ParticipantType> = {
  issue: 'ISSUER',
  verify: 'VERIFIER',
  grant_issue: 'ISSUER_GRANTOR',
  grant_verify: 'VERIFIER_GRANTOR',
  govern: 'ECOSYSTEM',
}

const RESOURCE_PATTERN = /^vpr:verana:([a-z0-9-]+):cs:([0-9]+)$/

interface AuthorizeParams {
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
    return TRQP_PROFILE_BODY
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
  async authorize(ctx: Context<AuthorizeParams>) {
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

      let evaluatedAt = new Date()
      if (requestContext.time) {
        evaluatedAt = new Date(requestContext.time)
        if (Number.isNaN(evaluatedAt.getTime())) {
          return ApiResponder.error(ctx, 'Invalid "context.time" datetime format', 400)
        }
      }
      const timeRequested = requestContext.time ? toIsoZ(requestContext.time) : toIsoZ(evaluatedAt)
      const timeEvaluated = toIsoZ(evaluatedAt)

      const baseResponse = {
        authority_id: authorityId,
        entity_id: entityId,
        action,
        resource,
        time_requested: timeRequested,
        time_evaluated: timeEvaluated,
      }

      // session_id (precedence over time). No temporal window exists on-chain (ParticipantSession
      // carries no validity window), so only existence + corporation scope are enforced; the
      // "session out of window" sub-clause is a documented no-op until the node introduces a TTL.
      if (requestContext.session_id) {
        const entityCorporation = await knex('corporation').select('id').where({ did: entityId }).first()
        const session = entityCorporation
          ? await knex('participant_sessions')
              .select('id')
              .where({ id: requestContext.session_id, corporation_id: entityCorporation.id })
              .first()
          : undefined
        if (!session) {
          return ApiResponder.success(ctx, { ...baseResponse, authorized: false, message: 'session not found' }, 200)
        }
      }

      // A resource for another network cannot match a schema indexed here.
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
}
