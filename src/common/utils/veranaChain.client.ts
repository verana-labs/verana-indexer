import { TsProtoGeneratedType } from '@cosmjs/proto-signing';
import {
  MsgArchiveCredentialSchema,
  MsgCreateCredentialSchema,
  MsgCreateSchemaAuthorizationPolicy,
  MsgIncreaseActiveSchemaAuthorizationPolicyVersion,
  MsgRevokeSchemaAuthorizationPolicy,
  MsgUpdateCredentialSchema,
  MsgUpdateParams as MsgUpdateParamsCS,
} from "@verana-labs/verana-types/codec/verana/cs/v1/tx";
import { MsgCreateCorporation, MsgUpdateCorporation, MsgUpdateParams as MsgUpdateParamsCO } from "@verana-labs/verana-types/codec/verana/co/v1/tx";
import { MsgAddGovernanceFrameworkDocument as MsgAddGovernanceFrameworkDocumentGF, MsgIncreaseActiveGovernanceFrameworkVersion as MsgIncreaseActiveGovernanceFrameworkVersionGF, MsgUpdateParams as MsgUpdateParamsGF } from "@verana-labs/verana-types/codec/verana/gf/v1/tx";
import { MsgStoreDigest, MsgUpdateParams as MsgUpdateParamsDI } from "@verana-labs/verana-types/codec/verana/di/v1/tx";
import { MsgGrantOperatorAuthorization, MsgRevokeOperatorAuthorization, MsgUpdateParams as MsgUpdateParamsDE } from "@verana-labs/verana-types/codec/verana/de/v1/tx";
import {
  MsgCancelParticipantOPLastRequest,
  MsgCreateOrUpdateParticipantSession,
  MsgSelfCreateParticipant,
  MsgCreateRootParticipant,
  MsgRenewParticipantOP,
  MsgRepayParticipantSlashedTrustDeposit,
  MsgRevokeParticipant,
  MsgSetParticipantOPToValidated,
  MsgSlashParticipantTrustDeposit,
  MsgStartParticipantOP,
  MsgUpdateParams as MsgUpdateParamsPP,
  MsgSetParticipantEffectiveUntil,
} from '@verana-labs/verana-types/codec/verana/pp/v1/tx';
import { MsgReclaimTrustDepositYield, MsgRepaySlashedTrustDeposit, MsgSlashTrustDeposit, MsgUpdateParams } from '@verana-labs/verana-types/codec/verana/td/v1/tx';
import { MsgArchiveEcosystem, MsgCreateEcosystem, MsgUpdateEcosystem, MsgUpdateParams as MsgUpdateParamsEC } from '@verana-labs/verana-types/codec/verana/ec/v1/tx';
import { MsgCreateExchangeRate, MsgSetExchangeRateState, MsgUpdateExchangeRate, MsgUpdateParams as MsgUpdateParamsXR } from "@verana-labs/verana-types/codec/verana/xr/v1/tx";
import {
  VeranaCorporationMessageTypes,
  VeranaCredentialSchemaMessageTypes,
  VeranaDiMessageTypes,
  VeranaDelegationMessageTypes,
  VeranaExchangeRateMessageTypes,
  VeranaGovernanceFrameworkMessageTypes,
  VeranaParticipantMessageTypes,
  VeranaTrustDepositMessageTypes,
  VeranaEcosystemMessageTypes,
} from '../verana-message-types';

export const veranaRegistry: readonly [string, TsProtoGeneratedType][] = [
   // verana.td.v1
    [VeranaTrustDepositMessageTypes.ReclaimYield, MsgReclaimTrustDepositYield],
    [VeranaTrustDepositMessageTypes.RepaySlashed, MsgRepaySlashedTrustDeposit],
    [VeranaTrustDepositMessageTypes.SlashTrustDeposit, MsgSlashTrustDeposit],
    [VeranaTrustDepositMessageTypes.UpdateParams, MsgUpdateParams],
    // verana.ec.v1
    [VeranaEcosystemMessageTypes.CreateEcosystem, MsgCreateEcosystem],
    [VeranaEcosystemMessageTypes.UpdateEcosystem, MsgUpdateEcosystem],
    [VeranaEcosystemMessageTypes.ArchiveEcosystem, MsgArchiveEcosystem],
    [VeranaEcosystemMessageTypes.UpdateParams, MsgUpdateParamsEC],
    // verana.cs.v1
    [VeranaCredentialSchemaMessageTypes.CreateCredentialSchema, MsgCreateCredentialSchema],
    [VeranaCredentialSchemaMessageTypes.UpdateCredentialSchema, MsgUpdateCredentialSchema],
    [VeranaCredentialSchemaMessageTypes.ArchiveCredentialSchema, MsgArchiveCredentialSchema],
    [VeranaCredentialSchemaMessageTypes.CreateSchemaAuthorizationPolicy, MsgCreateSchemaAuthorizationPolicy],
    [VeranaCredentialSchemaMessageTypes.IncreaseActiveSchemaAuthorizationPolicyVersion, MsgIncreaseActiveSchemaAuthorizationPolicyVersion],
    [VeranaCredentialSchemaMessageTypes.RevokeSchemaAuthorizationPolicy, MsgRevokeSchemaAuthorizationPolicy],
    [VeranaCredentialSchemaMessageTypes.UpdateParams, MsgUpdateParamsCS],
    [VeranaDiMessageTypes.StoreDigest, MsgStoreDigest],
    [VeranaDiMessageTypes.UpdateParams, MsgUpdateParamsDI],
    [VeranaDelegationMessageTypes.GrantOperatorAuthorization, MsgGrantOperatorAuthorization],
    [VeranaDelegationMessageTypes.RevokeOperatorAuthorization, MsgRevokeOperatorAuthorization],
    [VeranaDelegationMessageTypes.UpdateParams, MsgUpdateParamsDE],
    // verana.xr.v1
    [VeranaExchangeRateMessageTypes.CreateExchangeRate, MsgCreateExchangeRate],
    [VeranaExchangeRateMessageTypes.UpdateExchangeRate, MsgUpdateExchangeRate],
    [VeranaExchangeRateMessageTypes.SetExchangeRateState, MsgSetExchangeRateState],
    [VeranaExchangeRateMessageTypes.UpdateParams, MsgUpdateParamsXR],
    // verana.pp.v1
    [VeranaParticipantMessageTypes.StartParticipantOP, MsgStartParticipantOP],
    [VeranaParticipantMessageTypes.RenewParticipantOP, MsgRenewParticipantOP],
    [VeranaParticipantMessageTypes.SetParticipantOPToValidated, MsgSetParticipantOPToValidated],
    [VeranaParticipantMessageTypes.CancelParticipantOPLastRequest, MsgCancelParticipantOPLastRequest],
    [VeranaParticipantMessageTypes.CreateRootParticipant, MsgCreateRootParticipant],
    [VeranaParticipantMessageTypes.RevokeParticipant, MsgRevokeParticipant],
    [VeranaParticipantMessageTypes.CreateOrUpdateParticipantSession, MsgCreateOrUpdateParticipantSession],
    [VeranaParticipantMessageTypes.SlashParticipantTrustDeposit, MsgSlashParticipantTrustDeposit],
    [VeranaParticipantMessageTypes.RepayParticipantSlashedTrustDeposit, MsgRepayParticipantSlashedTrustDeposit],
    [VeranaParticipantMessageTypes.SelfCreateParticipant, MsgSelfCreateParticipant],
    [VeranaParticipantMessageTypes.UpdateParams, MsgUpdateParamsPP],
    [VeranaParticipantMessageTypes.SetParticipantEffectiveUntil, MsgSetParticipantEffectiveUntil],
    // verana.co.v1
    [VeranaCorporationMessageTypes.CreateCorporation, MsgCreateCorporation],
    [VeranaCorporationMessageTypes.UpdateCorporation, MsgUpdateCorporation],
    [VeranaCorporationMessageTypes.UpdateParams, MsgUpdateParamsCO],
    // verana.gf.v1
    [VeranaGovernanceFrameworkMessageTypes.AddGovernanceFrameworkDocument, MsgAddGovernanceFrameworkDocumentGF],
    [VeranaGovernanceFrameworkMessageTypes.IncreaseActiveGovernanceFrameworkVersion, MsgIncreaseActiveGovernanceFrameworkVersionGF],
    [VeranaGovernanceFrameworkMessageTypes.UpdateParams, MsgUpdateParamsGF],
];
