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
import { MsgStoreDigest, MsgUpdateParams as MsgUpdateParamsDI } from "@verana-labs/verana-types/codec/verana/di/v1/tx";
import { MsgGrantOperatorAuthorization, MsgRevokeOperatorAuthorization, MsgUpdateParams as MsgUpdateParamsDE } from "@verana-labs/verana-types/codec/verana/de/v1/tx";
import { MsgCancelPermissionVPLastRequest, MsgCreateOrUpdatePermissionSession, MsgSelfCreatePermission, MsgCreateRootPermission, MsgRenewPermissionVP, MsgRepayPermissionSlashedTrustDeposit as MsgRepayPermissionSlashedTrustDepositPerm, MsgRevokePermission, MsgSetPermissionVPToValidated, MsgSlashPermissionTrustDeposit, MsgStartPermissionVP, MsgUpdateParams as MsgUpdateParamsPerm ,MsgAdjustPermission} from '@verana-labs/verana-types/codec/verana/perm/v1/tx';
import { MsgReclaimTrustDepositYield, MsgRepaySlashedTrustDeposit, MsgSlashTrustDeposit, MsgUpdateParams } from '@verana-labs/verana-types/codec/verana/td/v1/tx';
import { MsgAddGovernanceFrameworkDocument, MsgArchiveTrustRegistry, MsgCreateTrustRegistry, MsgIncreaseActiveGovernanceFrameworkVersion, MsgUpdateTrustRegistry, MsgUpdateParams as MsgUpdateParamsTR } from '@verana-labs/verana-types/codec/verana/tr/v1/tx';
import { MsgCreateExchangeRate, MsgSetExchangeRateState, MsgUpdateExchangeRate, MsgUpdateParams as MsgUpdateParamsXR } from "@verana-labs/verana-types/codec/verana/xr/v1/tx";
import {
  VeranaCredentialSchemaMessageTypes,
  VeranaDiMessageTypes,
  VeranaDelegationMessageTypes,
  VeranaExchangeRateMessageTypes,
  VeranaPermissionMessageTypes,
  VeranaTrustDepositMessageTypes,
  VeranaTrustRegistryMessageTypes,
} from '../verana-message-types';

export const veranaRegistry: readonly [string, TsProtoGeneratedType][] = [
   // verana.td.v1
    [VeranaTrustDepositMessageTypes.ReclaimYield, MsgReclaimTrustDepositYield],
    [VeranaTrustDepositMessageTypes.RepaySlashed, MsgRepaySlashedTrustDeposit],
    [VeranaTrustDepositMessageTypes.SlashTrustDeposit, MsgSlashTrustDeposit],
    [VeranaTrustDepositMessageTypes.UpdateParams, MsgUpdateParams],
    // verana.tr.v1
    [VeranaTrustRegistryMessageTypes.CreateTrustRegistry, MsgCreateTrustRegistry],
    [VeranaTrustRegistryMessageTypes.UpdateTrustRegistry, MsgUpdateTrustRegistry],
    [VeranaTrustRegistryMessageTypes.ArchiveTrustRegistry, MsgArchiveTrustRegistry],
    [VeranaTrustRegistryMessageTypes.AddGovernanceFrameworkDoc, MsgAddGovernanceFrameworkDocument],
    [VeranaTrustRegistryMessageTypes.IncreaseGovernanceFrameworkVersion, MsgIncreaseActiveGovernanceFrameworkVersion],
    [VeranaTrustRegistryMessageTypes.UpdateParams, MsgUpdateParamsTR],
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
    // verana.perm.v1
    [VeranaPermissionMessageTypes.StartPermissionVP, MsgStartPermissionVP],
    [VeranaPermissionMessageTypes.RenewPermissionVP, MsgRenewPermissionVP],
    [VeranaPermissionMessageTypes.SetPermissionVPToValidated, MsgSetPermissionVPToValidated],
    [VeranaPermissionMessageTypes.CancelPermissionVPLastRequest, MsgCancelPermissionVPLastRequest],
    [VeranaPermissionMessageTypes.CreateRootPermission, MsgCreateRootPermission],
    [VeranaPermissionMessageTypes.RevokePermission, MsgRevokePermission],
    [VeranaPermissionMessageTypes.CreateOrUpdatePermissionSession, MsgCreateOrUpdatePermissionSession],
    [VeranaPermissionMessageTypes.SlashPermissionTrustDeposit, MsgSlashPermissionTrustDeposit],
    [VeranaPermissionMessageTypes.RepayPermissionSlashedTrustDeposit, MsgRepayPermissionSlashedTrustDepositPerm],
    [VeranaPermissionMessageTypes.SelfCreatePermission, MsgSelfCreatePermission],
    [VeranaPermissionMessageTypes.UpdateParams, MsgUpdateParamsPerm],
    [VeranaPermissionMessageTypes.AdjustPermission, MsgAdjustPermission]
];


