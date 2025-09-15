import { MsgArchiveCredentialSchema, MsgCreateCredentialSchema, MsgUpdateCredentialSchema } from "../../verana-proto/cs/v1/tx";
import { MsgAddDID, MsgRemoveDID, MsgRenewDID, MsgTouchDID } from '../../verana-proto/dd/v1/tx';
import { MsgReclaimTrustDeposit, MsgReclaimTrustDepositYield, MsgRepaySlashedTrustDeposit } from '../../verana-proto/td/v1/tx';
import { MsgAddGovernanceFrameworkDocument, MsgArchiveTrustRegistry, MsgCreateTrustRegistry, MsgIncreaseActiveGovernanceFrameworkVersion, MsgUpdateTrustRegistry } from '../../verana-proto/tr/v1/tx';


export const veranaRegistry = [
    // verana.dd.v1
    ["/verana.dd.v1.MsgAddDID", MsgAddDID],
    ["/verana.dd.v1.MsgRenewDID", MsgRenewDID],
    ["/verana.dd.v1.MsgTouchDID", MsgTouchDID],
    ["/verana.dd.v1.MsgRemoveDID", MsgRemoveDID],
    // verana.td.v1
    ["/verana.td.v1.MsgReclaimTrustDepositYield", MsgReclaimTrustDepositYield],
    ["/verana.td.v1.MsgReclaimTrustDeposit", MsgReclaimTrustDeposit],
    ["/verana.td.v1.MsgRepaySlashedTrustDeposit", MsgRepaySlashedTrustDeposit],
    // verana.tr.v1
    ["/verana.tr.v1.MsgCreateTrustRegistry", MsgCreateTrustRegistry],
    ["/verana.tr.v1.MsgUpdateTrustRegistry", MsgUpdateTrustRegistry],
    ["/verana.tr.v1.MsgArchiveTrustRegistry", MsgArchiveTrustRegistry],
    ["/verana.tr.v1.MsgAddGovernanceFrameworkDocument", MsgAddGovernanceFrameworkDocument],
    ["/verana.tr.v1.MsgIncreaseActiveGovernanceFrameworkVersion", MsgIncreaseActiveGovernanceFrameworkVersion],
    // verana.cs.v1
    ["/verana.cs.v1.MsgCreateCredentialSchema", MsgCreateCredentialSchema],
    ["/verana.cs.v1.MsgUpdateCredentialSchema", MsgUpdateCredentialSchema],
    ["/verana.cs.v1.MsgArchiveCredentialSchema", MsgArchiveCredentialSchema]
];
  

