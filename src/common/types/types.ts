/** Lifecycle state of a Participant / permission (VPR `participant_state`). */
export type ParticipantState =
  | "ACTIVE"
  | "FUTURE"
  | "INACTIVE"
  | "EXPIRED"
  | "REVOKED"
  | "SLASHED"
  | "REPAID";

/** Role a Participant plays in an Ecosystem (VPR `Participant.role`). */
export type ParticipantRole =
  | "HOLDER"
  | "ISSUER"
  | "VERIFIER"
  | "ISSUER_GRANTOR"
  | "VERIFIER_GRANTOR"
  | "ECOSYSTEM";

export const ALL_PARTICIPANT_STATES: ParticipantState[] = [
  "ACTIVE",
  "FUTURE",
  "INACTIVE",
  "EXPIRED",
  "REVOKED",
  "SLASHED",
  "REPAID",
];

export const ALL_PARTICIPANT_ROLES: ParticipantRole[] = [
  "HOLDER",
  "ISSUER",
  "VERIFIER",
  "ISSUER_GRANTOR",
  "VERIFIER_GRANTOR",
  "ECOSYSTEM",
];
