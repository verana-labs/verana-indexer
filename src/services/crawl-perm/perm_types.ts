export enum PermissionType {
  PERMISSION_TYPE_UNSPECIFIED = 0,
  PERMISSION_TYPE_ISSUER = 1,
  PERMISSION_TYPE_VERIFIER = 2,
  PERMISSION_TYPE_ISSUER_GRANTOR = 3,
  PERMISSION_TYPE_VERIFIER_GRANTOR = 4,
  /** PERMISSION_TYPE_ECOSYSTEM - Changed from PERMISSION_TYPE_TRUST_REGISTRY */
  PERMISSION_TYPE_ECOSYSTEM = 5,
  PERMISSION_TYPE_HOLDER = 6,
  UNRECOGNIZED = -1,
}
export enum PermissionTypeNames {
  UNSPECIFIED = 0,
  ISSUER = 1,
  VERIFIER = 2,
  ISSUER_GRANTOR = 3,
  VERIFIER_GRANTOR = 4,
  ECOSYSTEM = 5,
  HOLDER = 6,
}
export function getPermissionTypeString(msg: MsgStartPermissionVP): string {
  const rawType = (msg as any).type ?? (msg as any).permission_type ?? (msg as any).permissionType;
  if (typeof rawType === 'string') {
    return rawType;
  }
  if (typeof rawType === 'number') {
    return PermissionTypeNames[rawType] ?? "UNKNOWN";
  }
  return "UNKNOWN";
}
export interface MsgCreateRootPermission {
  creator?: string;
  corporation?: string;
  operator?: string;
  schema_id: number;
  schemaId?: number;
  did: string;
  effective_from?: Date | undefined;
  effectiveFrom?: Date | undefined;
  timestamp?: Date | undefined;
  effective_until?: Date | undefined;
  effectiveUntil?: Date | undefined;
  permission_type?: number | string;
  permissionType?: number | string;
  validation_fees: number;
  validationFees?: number;
  issuance_fees: number;
  issuanceFees?: number;
  verification_fees: number;
  verificationFees?: number;
  vs_operator?: string;
  vsOperator?: string;
}

export type DenomAmount = {
  denom: string;
  amount: string;
};
export interface MsgStartPermissionVP {
  creator?: string;
  corporation?: string;
  operator?: string;
  timestamp?: Date | undefined;
  type: number;
  permission_type?: number | string;
  permissionType?: number | string;
  validator_perm_id: number;
  validatorPermId?: number;
  did: string;
  effective_from: Date | undefined;
  effectiveFrom?: Date | undefined;
  effective_until: Date | undefined;
  effectiveUntil?: Date | undefined;
  validation_fees?: number;
  validationFees?: number;
  issuance_fees?: number;
  issuanceFees?: number;
  verification_fees?: number;
  verificationFees?: number;
  vs_operator?: string;
  vsOperator?: string;
  vs_operator_authz_enabled?: boolean;
  vsOperatorAuthzEnabled?: boolean;
  vs_operator_authz_spend_limit?: DenomAmount[] | null;
  vsOperatorAuthzSpendLimit?: DenomAmount[] | null;
  vs_operator_authz_with_feegrant?: boolean;
  vsOperatorAuthzWithFeegrant?: boolean;
  vs_operator_authz_fee_spend_limit?: DenomAmount[] | null;
  vsOperatorAuthzFeeSpendLimit?: DenomAmount[] | null;
  vs_operator_authz_spend_period?: string | null;
  vsOperatorAuthzSpendPeriod?: string | null;
}
export interface MsgSlashPermissionTrustDeposit {
  timestamp?: Date | undefined;
  creator?: string;
  corporation?: string;
  operator?: string;
  id: number;
  amount: number;
  reason?: string;
}
export interface MsgSetPermissionVPToValidated {
  creator: string;
  id: number;
  effective_until?: Date | undefined;
  timestamp?: Date | undefined;
  validation_fees: number;
  issuance_fees: number;
  verification_fees: number;
  vp_summary_digest?: string;
}

export interface MsgRevokePermission {
  timestamp: Date | string;
  id: number;
  creator: string;
}
export interface MsgRepayPermissionSlashedTrustDeposit {
  timestamp: Date | string;
  creator?: string;
  corporation?: string;
  operator?: string;
  id: number;
  amount?: number | string;
}
export interface MsgRenewPermissionVP {
  creator?: string;
  corporation?: string;
  operator?: string;
  timestamp: Date | string;
  id: number;
  permission_type?: number | string;
  permissionType?: number | string;
}
export interface MsgAdjustPermission {
  creator?: string;
  corporation?: string;
  authority?: string;
  operator?: string;
  id: number;
  effective_until?: Date | undefined;
  effectiveUntil?: Date | undefined;
  timestamp?: Date | undefined;
}
export interface MsgSelfCreatePermission {
  creator?: string;
  corporation?: string;
  operator?: string;
  schema_id?: number;
  schemaId?: number;
  type: PermissionType;
  permission_type?: number | string;
  permissionType?: number | string;
  validator_perm_id?: number;
  validatorPermId?: number;
  did: string;
  effective_from?: Date | undefined;
  effectiveFrom?: Date | undefined;
  timestamp?: Date | undefined;
  effective_until?: Date | undefined;
  effectiveUntil?: Date | undefined;
  validation_fees?: number;
  validationFees?: number;
  issuance_fees?: number;
  issuanceFees?: number;
  verification_fees: number;
  verificationFees?: number;
  vs_operator?: string;
  vsOperator?: string;
  vs_operator_authz_enabled?: boolean;
  vsOperatorAuthzEnabled?: boolean;
  vs_operator_authz_spend_limit?: DenomAmount[] | null;
  vsOperatorAuthzSpendLimit?: DenomAmount[] | null;
  vs_operator_authz_with_feegrant?: boolean;
  vsOperatorAuthzWithFeegrant?: boolean;
  vs_operator_authz_fee_spend_limit?: DenomAmount[] | null;
  vsOperatorAuthzFeeSpendLimit?: DenomAmount[] | null;
  vs_operator_authz_spend_period?: string | null;
  vsOperatorAuthzSpendPeriod?: string | null;
}
export interface MsgCreateOrUpdatePermissionSession {
  timestamp?: Date | undefined;
  creator?: string;
  corporation?: string;
  operator?: string;
  id: string;
  issuerPermId?: number;
  issuer_perm_id?: number;
  verifierPermId?: number;
  verifier_perm_id?: number;
  agentPermId?: number;
  agent_perm_id?: number;
  walletAgentPermId?: number;
  wallet_agent_perm_id?: number;
  digest?: string;
}
export interface MsgCancelPermissionVPLastRequest {
  timestamp?: Date | undefined;
  creator?: string;
  corporation?: string;
  operator?: string;
  id: number;
}
export interface MsgConfirmPermissionVPTermination {
  timestamp?: Date | undefined;
  creator: string;
  id: number;
}
export interface MsgRequestPermissionVPTermination {
  timestamp?: Date | undefined;
  creator: string;
  id: number;
}
