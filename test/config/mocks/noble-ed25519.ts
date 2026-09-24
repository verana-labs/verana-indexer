// Jest mock for @noble/curves/ed25519.js (ESM-only; jest does not transform node_modules).
export const ed25519 = {
  verify: (_signature: Uint8Array, _message: Uint8Array, _publicKey: Uint8Array): boolean => false,
}
