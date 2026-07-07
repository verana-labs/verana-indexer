export enum ParticipantType {
  PARTICIPANT_TYPE_UNSPECIFIED = 0,
  PARTICIPANT_TYPE_ISSUER = 1,
  PARTICIPANT_TYPE_VERIFIER = 2,
  PARTICIPANT_TYPE_ISSUER_GRANTOR = 3,
  PARTICIPANT_TYPE_VERIFIER_GRANTOR = 4,
  /** PARTICIPANT_TYPE_ECOSYSTEM - Changed from PARTICIPANT_TYPE_ECOSYSTEM */
  PARTICIPANT_TYPE_ECOSYSTEM = 5,
  PARTICIPANT_TYPE_HOLDER = 6,
  UNRECOGNIZED = -1,
}
export enum ParticipantTypeNames {
  UNSPECIFIED = 0,
  ISSUER = 1,
  VERIFIER = 2,
  ISSUER_GRANTOR = 3,
  VERIFIER_GRANTOR = 4,
  ECOSYSTEM = 5,
  HOLDER = 6,
}
export function getParticipantTypeString(msg: MsgStartParticipantOP): string {
  const rawType =
    (msg as any).type ?? (msg as any).role ?? (msg as any).participant_type ?? (msg as any).participantType
  if (typeof rawType === 'string') {
    return rawType
  }
  if (typeof rawType === 'number') {
    return ParticipantTypeNames[rawType] ?? 'UNKNOWN'
  }
  return 'UNKNOWN'
}
export interface MsgCreateRootParticipant {
  creator?: string
  corporation?: string
  operator?: string
  schema_id: number
  schemaId?: number
  did: string
  effective_from?: Date | undefined
  effectiveFrom?: Date | undefined
  timestamp?: Date | undefined
  effective_until?: Date | undefined
  effectiveUntil?: Date | undefined
  participant_type?: number | string
  participantType?: number | string
  validation_fees: number
  validationFees?: number
  issuance_fees: number
  issuanceFees?: number
  verification_fees: number
  verificationFees?: number
  vs_operator?: string
  vsOperator?: string
}

export type DenomAmount = {
  denom: string
  amount: string
}
export interface MsgStartParticipantOP {
  creator?: string
  corporation?: string
  operator?: string
  timestamp?: Date | undefined
  type: number
  participantType?: number | string
  validatorParticipantId?: number
  did: string
  effectiveFrom?: Date | undefined
  effectiveUntil?: Date | undefined
  validationFees?: number
  issuanceFees?: number
  verificationFees?: number
  vsOperator?: string
  vsOperatorAuthzEnabled?: boolean
  vsOperatorAuthzSpendLimit?: DenomAmount[] | null
  vsOperatorAuthzWithFeegrant?: boolean
  vsOperatorAuthzFeeSpendLimit?: DenomAmount[] | null
  vsOperatorAuthzSpendPeriod?: string | null
  vsOperatorAuthzPeriod?: string | null
  vsOperatorAuthzMsgTypes?: string[] | null
}
export interface MsgSlashParticipantTrustDeposit {
  timestamp?: Date | undefined
  creator?: string
  corporation?: string
  operator?: string
  id: number
  amount: number
  reason?: string
}
export interface MsgSetParticipantOPToValidated {
  creator: string
  id: number
  effective_until?: Date | undefined
  timestamp?: Date | undefined
  validation_fees: number
  issuance_fees: number
  verification_fees: number
  op_summary_digest?: string
}

export interface MsgRevokeParticipant {
  timestamp: Date | string
  id: number
  creator: string
}
export interface MsgRepayParticipantSlashedTrustDeposit {
  timestamp: Date | string
  creator?: string
  corporation?: string
  operator?: string
  id: number
  amount?: number | string
}
export interface MsgRenewParticipantOP {
  creator?: string
  corporation?: string
  operator?: string
  timestamp: Date | string
  id: number
  participant_type?: number | string
  participantType?: number | string
}
export interface MsgSetParticipantEffectiveUntil {
  creator?: string
  corporation?: string
  authority?: string
  operator?: string
  id: number
  effective_until?: Date | undefined
  effectiveUntil?: Date | undefined
  timestamp?: Date | undefined
}
export interface MsgSelfCreateParticipant {
  creator?: string
  corporation?: string
  operator?: string
  schemaId?: number
  type: ParticipantType
  participantType?: number | string
  validatorParticipantId?: number
  did: string
  effectiveFrom?: Date | undefined
  timestamp?: Date | undefined
  effectiveUntil?: Date | undefined
  validationFees?: number
  issuanceFees?: number
  verificationFees?: number
  vsOperator?: string
  vsOperatorAuthzEnabled?: boolean
  vsOperatorAuthzSpendLimit?: DenomAmount[] | null
  vsOperatorAuthzWithFeegrant?: boolean
  vsOperatorAuthzFeeSpendLimit?: DenomAmount[] | null
  vsOperatorAuthzSpendPeriod?: string | null
  vsOperatorAuthzPeriod?: string | null
  vsOperatorAuthzMsgTypes?: string[] | null
}
export interface MsgCreateOrUpdateParticipantSession {
  timestamp?: Date | undefined
  creator?: string
  corporation?: string
  operator?: string
  id: string
  issuerParticipantId?: number
  issuer_participant_id?: number
  verifierParticipantId?: number
  verifier_participant_id?: number
  agentParticipantId?: number
  agent_participant_id?: number
  walletAgentParticipantId?: number
  wallet_agent_participant_id?: number
  digest?: string
}
export interface MsgCancelParticipantOPLastRequest {
  timestamp?: Date | undefined
  creator?: string
  corporation?: string
  operator?: string
  id: number
}
export interface MsgConfirmParticipantOPTermination {
  timestamp?: Date | undefined
  creator: string
  id: number
}
export interface MsgRequestParticipantOPTermination {
  timestamp?: Date | undefined
  creator: string
  id: number
}
