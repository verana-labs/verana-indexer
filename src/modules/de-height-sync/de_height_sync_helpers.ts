import type { Coin } from '@verana-labs/verana-types/codec/cosmos/base/v1beta1/coin'
import type { Duration } from '@verana-labs/verana-types/codec/google/protobuf/duration'
import {
  QueryClientImpl as DeQueryClientImpl,
  QueryGetOperatorAuthorizationRequest,
  QueryGetVSOperatorAuthorizationRequest,
} from '@verana-labs/verana-types/codec/verana/de/v1/query'
import type {
  OperatorAuthorization as LedgerOperatorAuthorization,
  ParticipantAuthorizationRecord as LedgerParticipantAuthorizationRecord,
  VSOperatorAuthorization as LedgerVSOperatorAuthorization,
} from '@verana-labs/verana-types/codec/verana/de/v1/types'
import { AllowedMsgAllowance, BasicAllowance, PeriodicAllowance } from 'cosmjs-types/cosmos/feegrant/v1beta1/feegrant'
import {
  QueryClientImpl as FeegrantQueryClientImpl,
  QueryAllowanceRequest,
} from 'cosmjs-types/cosmos/feegrant/v1beta1/query'
import { dateToIsoOrNull } from '../../common/utils/date_utils'
import { withAbciQueryClient } from '../../common/utils/grpc_query'

const ALLOWED_MSG_ALLOWANCE_TYPE_URL = '/cosmos.feegrant.v1beta1.AllowedMsgAllowance'
const BASIC_ALLOWANCE_TYPE_URL = '/cosmos.feegrant.v1beta1.BasicAllowance'
const PERIODIC_ALLOWANCE_TYPE_URL = '/cosmos.feegrant.v1beta1.PeriodicAllowance'

export interface DenomAmount {
  denom: string
  amount: string
}

export interface OperatorAuthorizationRow {
  id: number
  corporation_id: number
  operator: string
  msg_types: string[]
  spend_limit: DenomAmount[] | null
  remaining_spend: DenomAmount[] | null
  expiration: string | null
  period: string | null
}

export interface ParticipantAuthorizationRecordRow {
  participant_id: number
  msg_types: string[]
  spend_limit: DenomAmount[] | null
  remaining_spend: DenomAmount[] | null
  fee_spend_limit: DenomAmount[] | null
  remaining_fee_spend: DenomAmount[] | null
  with_feegrant: boolean
  expiration: string | null
  period: string | null
}

export interface VSOperatorAuthorizationRow {
  id: number
  corporation_id: number
  vs_operator: string
  records: ParticipantAuthorizationRecordRow[]
}

export interface FeeAllowanceSnapshot {
  fee_spend_limit: DenomAmount[] | null
  remaining_fee_spend: DenomAmount[] | null
}

function serializeCoins(coins: Coin[] | undefined): DenomAmount[] | null {
  if (!coins || coins.length === 0) return null
  return coins.map((coin) => ({ denom: coin.denom, amount: String(coin.amount) }))
}

function serializeDuration(duration: Duration | undefined): string | null {
  if (!duration) return null
  const seconds = Number(duration.seconds ?? 0)
  const nanos = Number(duration.nanos ?? 0)
  if (seconds === 0 && nanos === 0) return null
  return `${seconds + nanos / 1e9}s`
}

export function serializeLedgerOperatorAuthorization(
  authorization: LedgerOperatorAuthorization
): OperatorAuthorizationRow {
  return {
    id: Number(authorization.id),
    corporation_id: Number(authorization.corporationId),
    operator: authorization.operator,
    msg_types: authorization.msgTypes ?? [],
    spend_limit: serializeCoins(authorization.spendLimit),
    remaining_spend: serializeCoins(authorization.remainingSpend),
    expiration: dateToIsoOrNull(authorization.expiration),
    period: serializeDuration(authorization.period),
  }
}

function serializeLedgerParticipantRecord(
  record: LedgerParticipantAuthorizationRecord
): ParticipantAuthorizationRecordRow {
  return {
    participant_id: Number(record.participantId),
    msg_types: record.msgTypes ?? [],
    spend_limit: serializeCoins(record.spendLimit),
    remaining_spend: serializeCoins(record.remainingSpend),
    fee_spend_limit: serializeCoins(record.feeSpendLimit),
    remaining_fee_spend: serializeCoins(record.remainingFeeSpend),
    with_feegrant: Boolean(record.withFeegrant),
    expiration: dateToIsoOrNull(record.expiration),
    period: serializeDuration(record.period),
  }
}

export function serializeLedgerVSOperatorAuthorization(
  authorization: LedgerVSOperatorAuthorization
): VSOperatorAuthorizationRow {
  return {
    id: Number(authorization.id),
    corporation_id: Number(authorization.corporationId),
    vs_operator: authorization.vsOperator,
    records: (authorization.records ?? []).map(serializeLedgerParticipantRecord),
  }
}

export async function fetchOperatorAuthorization(
  id: number,
  blockHeight: number | undefined
): Promise<LedgerOperatorAuthorization | undefined> {
  return withAbciQueryClient(blockHeight, async (rpc) => {
    const query = new DeQueryClientImpl(rpc)
    const res = await query.GetOperatorAuthorization(QueryGetOperatorAuthorizationRequest.fromPartial({ id }))
    return res?.operatorAuthorization ?? undefined
  })
}

export async function fetchVSOperatorAuthorization(
  id: number,
  blockHeight: number | undefined
): Promise<LedgerVSOperatorAuthorization | undefined> {
  return withAbciQueryClient(blockHeight, async (rpc) => {
    const query = new DeQueryClientImpl(rpc)
    const res = await query.GetVSOperatorAuthorization(QueryGetVSOperatorAuthorizationRequest.fromPartial({ id }))
    return res?.vsOperatorAuthorization ?? undefined
  })
}

function unwrapFeeAllowance(typeUrl: string, value: Uint8Array): FeeAllowanceSnapshot | undefined {
  if (typeUrl === PERIODIC_ALLOWANCE_TYPE_URL) {
    const periodic = PeriodicAllowance.decode(value)
    return {
      fee_spend_limit: serializeCoins(periodic.periodSpendLimit as Coin[]),
      remaining_fee_spend: serializeCoins(periodic.periodCanSpend as Coin[]),
    }
  }
  if (typeUrl === BASIC_ALLOWANCE_TYPE_URL) {
    const basic = BasicAllowance.decode(value)
    const limit = serializeCoins(basic.spendLimit as Coin[])
    return { fee_spend_limit: limit, remaining_fee_spend: limit }
  }
  return undefined
}

export async function fetchFeeAllowance(
  granter: string,
  grantee: string,
  blockHeight: number | undefined
): Promise<FeeAllowanceSnapshot | undefined> {
  const grant = await withAbciQueryClient(blockHeight, async (rpc) => {
    const query = new FeegrantQueryClientImpl(rpc)
    const res = await query.Allowance(QueryAllowanceRequest.fromPartial({ granter, grantee }))
    return res?.allowance ?? undefined
  })

  const allowance = grant?.allowance
  if (!allowance) return undefined

  if (allowance.typeUrl !== ALLOWED_MSG_ALLOWANCE_TYPE_URL) {
    return unwrapFeeAllowance(allowance.typeUrl, allowance.value)
  }

  const allowed = AllowedMsgAllowance.decode(allowance.value)
  if (!allowed.allowance) return undefined
  return unwrapFeeAllowance(allowed.allowance.typeUrl, allowed.allowance.value)
}
