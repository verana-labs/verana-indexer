import { Buffer } from 'node:buffer'
import type { ServiceBroker } from 'moleculer'
import { SERVICE } from '../../common'
import { Corporation } from '../../models/corporation'
import type { FeeAllowanceSnapshot } from './de_height_sync_helpers'
import {
  fetchCorporationPolicyAddress,
  fetchFeeAllowance,
  fetchFeeGrantAllowance,
  fetchOperatorAuthorization,
  fetchVSOperatorAuthorization,
  serializeLedgerOperatorAuthorization,
  serializeLedgerVSOperatorAuthorization,
} from './de_height_sync_helpers'

export const DE_EVENT_TYPES = {
  GRANT_OPERATOR_AUTHORIZATION: 'grant_operator_authorization',
  REVOKE_OPERATOR_AUTHORIZATION: 'revoke_operator_authorization',
  GRANT_VS_OPERATOR_AUTHORIZATION: 'grant_vs_operator_authorization',
  REVOKE_VS_OPERATOR_AUTHORIZATION: 'revoke_vs_operator_authorization',
  UPDATE_VS_OPERATOR_AUTHORIZATION: 'update_vs_operator_authorization',
  GRANT_FEE_ALLOWANCE: 'grant_fee_allowance',
  REVOKE_FEE_ALLOWANCE: 'revoke_fee_allowance',
  // SDK x/feegrant event: the only on-chain signal for fee draws (x/de emits none).
  USE_FEEGRANT: 'use_feegrant',
} as const

const DE_EVENT_TYPE_SET = new Set<string>(Object.values(DE_EVENT_TYPES))

const OPERATOR_AUTHORIZATION_EVENTS = new Set<string>([
  DE_EVENT_TYPES.GRANT_OPERATOR_AUTHORIZATION,
  DE_EVENT_TYPES.REVOKE_OPERATOR_AUTHORIZATION,
])

const VS_OPERATOR_AUTHORIZATION_EVENTS = new Set<string>([
  DE_EVENT_TYPES.GRANT_VS_OPERATOR_AUTHORIZATION,
  DE_EVENT_TYPES.REVOKE_VS_OPERATOR_AUTHORIZATION,
  DE_EVENT_TYPES.UPDATE_VS_OPERATOR_AUTHORIZATION,
])

const FEE_GRANT_EVENTS = new Set<string>([DE_EVENT_TYPES.GRANT_FEE_ALLOWANCE, DE_EVENT_TYPES.REVOKE_FEE_ALLOWANCE])

interface BlockEventAttribute {
  key?: string
  value?: string
}

interface BlockEvent {
  type?: string
  attributes?: BlockEventAttribute[]
}

function decodeBase64(value: string): string {
  return Buffer.from(value, 'base64').toString('utf8')
}

function getAttr(event: BlockEvent, key: string): string | undefined {
  for (const attr of event.attributes ?? []) {
    if (attr.key === undefined) continue
    if (attr.key === key) return attr.value
    if (decodeBase64(attr.key) === key) return attr.value === undefined ? undefined : decodeBase64(attr.value)
  }
  return undefined
}

function parseId(value: string | undefined): number | undefined {
  if (!value) return undefined
  const parsed = Number(value)
  return Number.isSafeInteger(parsed) && parsed > 0 ? parsed : undefined
}

export function hasDelegationEvents(events: BlockEvent[]): boolean {
  return events.some((event) => event.type !== undefined && DE_EVENT_TYPE_SET.has(event.type))
}

interface OperatorAuthorizationTouch {
  authzId: number
  corporationId: number
  grantee: string
  revoked: boolean
}

export function extractOperatorAuthorizationTouches(events: BlockEvent[]): OperatorAuthorizationTouch[] {
  const touches = new Map<number, OperatorAuthorizationTouch>()
  for (const event of events) {
    if (!event.type || !OPERATOR_AUTHORIZATION_EVENTS.has(event.type)) continue
    const authzId = parseId(getAttr(event, 'authz_id'))
    const corporationId = parseId(getAttr(event, 'corporation_id'))
    const grantee = getAttr(event, 'grantee')
    if (authzId === undefined || corporationId === undefined || !grantee) continue
    touches.set(authzId, {
      authzId,
      corporationId,
      grantee,
      revoked: event.type === DE_EVENT_TYPES.REVOKE_OPERATOR_AUTHORIZATION,
    })
  }
  return [...touches.values()]
}

export function extractVSOperatorAuthorizationIds(events: BlockEvent[]): number[] {
  const ids = new Set<number>()
  for (const event of events) {
    if (!event.type || !VS_OPERATOR_AUTHORIZATION_EVENTS.has(event.type)) continue
    const vsoaId = parseId(getAttr(event, 'vsoa_id'))
    if (vsoaId !== undefined) ids.add(vsoaId)
  }
  return [...ids]
}

interface FeeGrantTouch {
  grantorCorporationId: number
  grantee: string
  revoked: boolean
}

export function extractFeeGrantTouches(events: BlockEvent[]): FeeGrantTouch[] {
  const touches = new Map<string, FeeGrantTouch>()
  for (const event of events) {
    if (!event.type || !FEE_GRANT_EVENTS.has(event.type)) continue
    const grantorCorporationId = parseId(getAttr(event, 'corporation_id'))
    const grantee = getAttr(event, 'grantee')
    if (grantorCorporationId === undefined || !grantee) continue
    touches.set(`${grantorCorporationId}:${grantee}`, {
      grantorCorporationId,
      grantee,
      revoked: event.type === DE_EVENT_TYPES.REVOKE_FEE_ALLOWANCE,
    })
  }
  return [...touches.values()]
}

interface FeeGrantUse {
  granter: string
  grantee: string
}

export function extractFeeGrantUses(events: BlockEvent[]): FeeGrantUse[] {
  const uses = new Map<string, FeeGrantUse>()
  for (const event of events) {
    if (event.type !== DE_EVENT_TYPES.USE_FEEGRANT) continue
    const granter = getAttr(event, 'granter')
    const grantee = getAttr(event, 'grantee')
    if (!granter || !grantee) continue
    uses.set(`${granter}:${grantee}`, { granter, grantee })
  }
  return [...uses.values()]
}

async function resolveCorporationPolicyAddress(corporationId: number): Promise<string | undefined> {
  const corporation = await Corporation.query().findById(corporationId)
  return corporation?.policy_address ?? undefined
}

async function syncOperatorAuthorization(
  broker: ServiceBroker,
  touch: OperatorAuthorizationTouch,
  blockHeight: number
): Promise<void> {
  if (touch.revoked) {
    await broker.call(`${SERVICE.V1.DelegationDatabaseService.path}.revokeOperatorAuthorization`, {
      id: touch.authzId,
      corporationId: touch.corporationId,
      operator: touch.grantee,
      blockHeight,
    })
    return
  }

  const ledgerAuthorization = await fetchOperatorAuthorization(touch.authzId, blockHeight)
  if (!ledgerAuthorization) return

  const authorization = serializeLedgerOperatorAuthorization(ledgerAuthorization)

  let feeAllowance: FeeAllowanceSnapshot | undefined
  const policyAddress = await resolveCorporationPolicyAddress(authorization.corporation_id)
  if (policyAddress) {
    try {
      feeAllowance = await fetchFeeAllowance(policyAddress, authorization.operator, blockHeight)
    } catch (err: any) {
      broker.logger.warn(
        `[DE Height Sync] Failed to fetch fee allowance granter=${policyAddress} grantee=${authorization.operator} at block=${blockHeight}: ${err?.message || String(err)}`
      )
    }
  }

  await broker.call(`${SERVICE.V1.DelegationDatabaseService.path}.syncOperatorAuthorization`, {
    authorization,
    feeAllowance: feeAllowance ?? null,
    blockHeight,
  })
}

async function syncFeeGrant(broker: ServiceBroker, touch: FeeGrantTouch, blockHeight: number): Promise<void> {
  if (touch.revoked) {
    await broker.call(`${SERVICE.V1.DelegationDatabaseService.path}.revokeFeeGrant`, {
      grantorCorporationId: touch.grantorCorporationId,
      grantee: touch.grantee,
      blockHeight,
    })
    return
  }

  // Chain fallback covers reindex ordering, where the corporation pipeline may lag this one.
  const policyAddress =
    (await resolveCorporationPolicyAddress(touch.grantorCorporationId)) ??
    (await fetchCorporationPolicyAddress(touch.grantorCorporationId, blockHeight))
  if (!policyAddress) {
    broker.logger.warn(
      `[DE Height Sync] No policy address for corporation=${touch.grantorCorporationId}; skipping fee grant sync at block=${blockHeight}`
    )
    return
  }

  const snapshot = await fetchFeeGrantAllowance(policyAddress, touch.grantee, blockHeight)
  if (!snapshot) {
    // Allowance already pruned at end of block (re-grant fully drawn): zero an existing row.
    broker.logger.warn(
      `[DE Height Sync] No fee allowance found granter=${policyAddress} grantee=${touch.grantee} at block=${blockHeight}`
    )
    await broker.call(`${SERVICE.V1.DelegationDatabaseService.path}.refreshFeeGrantSpend`, {
      grantorCorporationId: touch.grantorCorporationId,
      grantee: touch.grantee,
      snapshot: null,
      blockHeight,
    })
    return
  }

  await broker.call(`${SERVICE.V1.DelegationDatabaseService.path}.syncFeeGrant`, {
    grantorCorporationId: touch.grantorCorporationId,
    grantee: touch.grantee,
    snapshot,
    blockHeight,
  })
}

async function refreshFeeGrantUse(
  broker: ServiceBroker,
  use: FeeGrantUse,
  blockHeight: number,
  touchedPairs: Set<string>
): Promise<void> {
  const corporation = await Corporation.query().where('policy_address', use.granter).first()
  if (!corporation?.id) return
  // A grant/revoke in this block already synced the pair's end-of-block state.
  if (touchedPairs.has(`${Number(corporation.id)}:${use.grantee}`)) return

  // Transport errors propagate to the caller's catch, so a failed fetch writes nothing.
  const snapshot = (await fetchFeeGrantAllowance(use.granter, use.grantee, blockHeight)) ?? null

  await broker.call(`${SERVICE.V1.DelegationDatabaseService.path}.refreshFeeGrantSpend`, {
    grantorCorporationId: Number(corporation.id),
    grantee: use.grantee,
    snapshot,
    blockHeight,
  })
}

async function syncVSOperatorAuthorization(broker: ServiceBroker, vsoaId: number, blockHeight: number): Promise<void> {
  const ledgerAuthorization = await fetchVSOperatorAuthorization(vsoaId, blockHeight)

  if (!ledgerAuthorization) {
    await broker.call(`${SERVICE.V1.DelegationDatabaseService.path}.deleteVSOperatorAuthorization`, {
      id: vsoaId,
      blockHeight,
    })
    return
  }

  await broker.call(`${SERVICE.V1.DelegationDatabaseService.path}.syncVSOperatorAuthorization`, {
    authorization: serializeLedgerVSOperatorAuthorization(ledgerAuthorization),
    blockHeight,
  })
}

export async function runHeightSyncDE(
  broker: ServiceBroker,
  payload: { events: BlockEvent[] },
  blockHeight: number
): Promise<void> {
  const events = payload.events ?? []
  if (!hasDelegationEvents(events) || typeof blockHeight !== 'number' || blockHeight <= 0) {
    return
  }

  for (const touch of extractOperatorAuthorizationTouches(events)) {
    try {
      await syncOperatorAuthorization(broker, touch, blockHeight)
    } catch (err: any) {
      broker.logger.warn(
        `[DE Height Sync] Sync failed operator_authorization=${touch.authzId} at block=${blockHeight}: ${err?.message || String(err)}`
      )
    }
  }

  for (const vsoaId of extractVSOperatorAuthorizationIds(events)) {
    try {
      await syncVSOperatorAuthorization(broker, vsoaId, blockHeight)
    } catch (err: any) {
      broker.logger.warn(
        `[DE Height Sync] Sync failed vs_operator_authorization=${vsoaId} at block=${blockHeight}: ${err?.message || String(err)}`
      )
    }
  }

  const feeGrantTouches = extractFeeGrantTouches(events)
  const touchedPairs = new Set(feeGrantTouches.map((touch) => `${touch.grantorCorporationId}:${touch.grantee}`))
  for (const touch of feeGrantTouches) {
    try {
      await syncFeeGrant(broker, touch, blockHeight)
    } catch (err: any) {
      broker.logger.warn(
        `[DE Height Sync] Sync failed fee_grant corporation=${touch.grantorCorporationId} grantee=${touch.grantee} at block=${blockHeight}: ${err?.message || String(err)}`
      )
    }
  }

  for (const use of extractFeeGrantUses(events)) {
    try {
      await refreshFeeGrantUse(broker, use, blockHeight, touchedPairs)
    } catch (err: any) {
      broker.logger.warn(
        `[DE Height Sync] Refresh failed fee_grant granter=${use.granter} grantee=${use.grantee} at block=${blockHeight}: ${err?.message || String(err)}`
      )
    }
  }
}
