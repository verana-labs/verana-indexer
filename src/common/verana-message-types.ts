export enum VeranaCredentialSchemaMessageTypes {
  UpdateParams = '/verana.cs.v1.MsgUpdateParams',
  CreateCredentialSchema = '/verana.cs.v1.MsgCreateCredentialSchema',
  UpdateCredentialSchema = '/verana.cs.v1.MsgUpdateCredentialSchema',
  ArchiveCredentialSchema = '/verana.cs.v1.MsgArchiveCredentialSchema',
  CreateSchemaAuthorizationPolicy = '/verana.cs.v1.MsgCreateSchemaAuthorizationPolicy',
  IncreaseActiveSchemaAuthorizationPolicyVersion = '/verana.cs.v1.MsgIncreaseActiveSchemaAuthorizationPolicyVersion',
  RevokeSchemaAuthorizationPolicy = '/verana.cs.v1.MsgRevokeSchemaAuthorizationPolicy',
}

export enum VeranaParticipantMessageTypes {
  UpdateParams = '/verana.pp.v1.MsgUpdateParams',
  CreateRootParticipant = '/verana.pp.v1.MsgCreateRootParticipant',
  SelfCreateParticipant = '/verana.pp.v1.MsgSelfCreateParticipant',
  StartParticipantOP = '/verana.pp.v1.MsgStartParticipantOP',
  RenewParticipantOP = '/verana.pp.v1.MsgRenewParticipantOP',
  RevokeParticipant = '/verana.pp.v1.MsgRevokeParticipant',
  SetParticipantEffectiveUntil = '/verana.pp.v1.MsgSetParticipantEffectiveUntil',
  SetParticipantOPToValidated = '/verana.pp.v1.MsgSetParticipantOPToValidated',
  CreateOrUpdateParticipantSession = '/verana.pp.v1.MsgCreateOrUpdateParticipantSession',
  SlashParticipantTrustDeposit = '/verana.pp.v1.MsgSlashParticipantTrustDeposit',
  RepayParticipantSlashedTrustDeposit = '/verana.pp.v1.MsgRepayParticipantSlashedTrustDeposit',
  CancelParticipantOPLastRequest = '/verana.pp.v1.MsgCancelParticipantOPLastRequest',
}

export enum VeranaTrustDepositMessageTypes {
  UpdateParams = '/verana.td.v1.MsgUpdateParams',
  AdjustTrustDeposit = '/verana.td.v1.MsgAdjustTrustDeposit',
  ReclaimYield = '/verana.td.v1.MsgReclaimTrustDepositYield',
  RepaySlashed = '/verana.td.v1.MsgRepaySlashedTrustDeposit',
  SlashTrustDeposit = '/verana.td.v1.MsgSlashTrustDeposit',
  BurnEcosystemSlashedTrustDeposit = '/verana.td.v1.MsgBurnEcosystemSlashedTrustDeposit',
}

export enum VeranaEcosystemMessageTypes {
  UpdateParams = '/verana.ec.v1.MsgUpdateParams',
  CreateEcosystem = '/verana.ec.v1.MsgCreateEcosystem',
  UpdateEcosystem = '/verana.ec.v1.MsgUpdateEcosystem',
  ArchiveEcosystem = '/verana.ec.v1.MsgArchiveEcosystem',
  AddGovernanceFrameworkDoc = '/verana.ec.v1.MsgAddGovernanceFrameworkDocument',
  IncreaseGovernanceFrameworkVersion = '/verana.ec.v1.MsgIncreaseActiveGovernanceFrameworkVersion',
}

export enum VeranaCorporationMessageTypes {
  UpdateParams = '/verana.co.v1.MsgUpdateParams',
  CreateCorporation = '/verana.co.v1.MsgCreateCorporation',
  UpdateCorporation = '/verana.co.v1.MsgUpdateCorporation',
}

export enum VeranaGovernanceFrameworkMessageTypes {
  UpdateParams = '/verana.gf.v1.MsgUpdateParams',
  AddGovernanceFrameworkDocument = '/verana.gf.v1.MsgAddGovernanceFrameworkDocument',
  IncreaseActiveGovernanceFrameworkVersion = '/verana.gf.v1.MsgIncreaseActiveGovernanceFrameworkVersion',
}

export enum VeranaDiMessageTypes {
  UpdateParams = '/verana.di.v1.MsgUpdateParams',
  StoreDigest = '/verana.di.v1.MsgStoreDigest',
}

export enum VeranaDelegationMessageTypes {
  UpdateParams = '/verana.de.v1.MsgUpdateParams',
  GrantOperatorAuthorization = '/verana.de.v1.MsgGrantOperatorAuthorization',
  RevokeOperatorAuthorization = '/verana.de.v1.MsgRevokeOperatorAuthorization',
}

export enum VeranaExchangeRateMessageTypes {
  UpdateParams = '/verana.xr.v1.MsgUpdateParams',
  CreateExchangeRate = '/verana.xr.v1.MsgCreateExchangeRate',
  UpdateExchangeRate = '/verana.xr.v1.MsgUpdateExchangeRate',
  SetExchangeRateState = '/verana.xr.v1.MsgSetExchangeRateState',
}

export enum UpdateParamsMessageTypes {
  CREDENTIAL_SCHEMA = '/verana.cs.v1.MsgUpdateParams',
  PARTICIPANT = '/verana.pp.v1.MsgUpdateParams',
  TRUST_DEPOSIT = '/verana.td.v1.MsgUpdateParams',
  ECOSYSTEM = '/verana.ec.v1.MsgUpdateParams',
  DIGITAL_IDENTITY = '/verana.di.v1.MsgUpdateParams',
  DELEGATION = '/verana.de.v1.MsgUpdateParams',
  EXCHANGE_RATE = '/verana.xr.v1.MsgUpdateParams',
  CORPORATION = '/verana.co.v1.MsgUpdateParams',
  GOVERNANCE_FRAMEWORK = '/verana.gf.v1.MsgUpdateParams',
}

export enum CosmosStakingMessageTypes {
  CreateValidator = '/cosmos.staking.v1beta1.MsgCreateValidator',
  EditValidator = '/cosmos.staking.v1beta1.MsgEditValidator',
  Delegate = '/cosmos.staking.v1beta1.MsgDelegate',
  BeginRedelegate = '/cosmos.staking.v1beta1.MsgBeginRedelegate',
  Undelegate = '/cosmos.staking.v1beta1.MsgUndelegate',
  CancelUnbondingDelegation = '/cosmos.staking.v1beta1.MsgCancelUnbondingDelegation',
}

export enum CosmosSlashingMessageTypes {
  Unjail = '/cosmos.slashing.v1beta1.MsgUnjail',
}

export const ALL_KNOWN_VERANA_MESSAGE_TYPES = new Set<string>([
  ...Object.values(VeranaCredentialSchemaMessageTypes),
  ...Object.values(VeranaParticipantMessageTypes),
  ...Object.values(VeranaTrustDepositMessageTypes),
  ...Object.values(VeranaEcosystemMessageTypes),
  ...Object.values(VeranaCorporationMessageTypes),
  ...Object.values(VeranaGovernanceFrameworkMessageTypes),
  ...Object.values(VeranaDiMessageTypes),
  ...Object.values(VeranaDelegationMessageTypes),
  ...Object.values(VeranaExchangeRateMessageTypes),
  ...Object.values(CosmosStakingMessageTypes),
  ...Object.values(CosmosSlashingMessageTypes),
])

export function isKnownVeranaMessageType(messageType: string): boolean {
  return ALL_KNOWN_VERANA_MESSAGE_TYPES.has(messageType)
}

export function getAllKnownMessageTypes(): string[] {
  return Array.from(ALL_KNOWN_VERANA_MESSAGE_TYPES)
}

export function shouldSkipUnknownMessages(): boolean {
  return process.env.SKIP_UNKNOWN_MESSAGES === 'true' || process.env.SKIP_UNKNOWN_MESSAGES === '1'
}

export function isVeranaMessageType(messageType: string): boolean {
  return messageType.startsWith('/verana.')
}

export function isEcosystemMessageType(messageType: string): boolean {
  return Object.values(VeranaEcosystemMessageTypes).includes(messageType as VeranaEcosystemMessageTypes)
}

export function isCredentialSchemaMessageType(messageType: string): boolean {
  return Object.values(VeranaCredentialSchemaMessageTypes).includes(messageType as VeranaCredentialSchemaMessageTypes)
}

export function isParticipantMessageType(messageType: string): boolean {
  return Object.values(VeranaParticipantMessageTypes).includes(messageType as VeranaParticipantMessageTypes)
}

export function isTrustDepositMessageType(messageType: string): boolean {
  return Object.values(VeranaTrustDepositMessageTypes).includes(messageType as VeranaTrustDepositMessageTypes)
}

export function isDelegationMessageType(messageType: string): boolean {
  return Object.values(VeranaDelegationMessageTypes).includes(messageType as VeranaDelegationMessageTypes)
}

export function isDigitalIdentityMessageType(messageType: string): boolean {
  return Object.values(VeranaDiMessageTypes).includes(messageType as VeranaDiMessageTypes)
}

export function isDidMessageType(messageType: string): boolean {
  return isDigitalIdentityMessageType(messageType)
}

export function isExchangeRateMessageType(messageType: string): boolean {
  return Object.values(VeranaExchangeRateMessageTypes).includes(messageType as VeranaExchangeRateMessageTypes)
}

export function isCorporationMessageType(messageType: string): boolean {
  return Object.values(VeranaCorporationMessageTypes).includes(messageType as VeranaCorporationMessageTypes)
}

export function isGovernanceFrameworkMessageType(messageType: string): boolean {
  return Object.values(VeranaGovernanceFrameworkMessageTypes).includes(
    messageType as VeranaGovernanceFrameworkMessageTypes
  )
}

export function isUpdateParamsMessageType(messageType: string): boolean {
  return (
    messageType === VeranaCredentialSchemaMessageTypes.UpdateParams ||
    messageType === VeranaParticipantMessageTypes.UpdateParams ||
    messageType === VeranaTrustDepositMessageTypes.UpdateParams ||
    messageType === VeranaEcosystemMessageTypes.UpdateParams ||
    messageType === VeranaDiMessageTypes.UpdateParams ||
    messageType === VeranaDelegationMessageTypes.UpdateParams ||
    messageType === VeranaExchangeRateMessageTypes.UpdateParams ||
    messageType === VeranaCorporationMessageTypes.UpdateParams ||
    messageType === VeranaGovernanceFrameworkMessageTypes.UpdateParams
  )
}
