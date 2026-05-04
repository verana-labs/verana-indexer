// Jest unit-test mock for @verana-labs/verre (ESM-only dependency).
// Jest in this repo doesn't transform ESM under node_modules, so we replace verre entirely.

export type TrustResolution = any;
export type TrustResolutionCache = any;
export type VerifiablePublicRegistry = any;

export const PermissionType = { ISSUER: 'ISSUER', VERIFIER: 'VERIFIER' } as const;

export const TrustResolutionOutcome = {
  VERIFIED: 'verified',
  VERIFIED_TEST: 'verified-test',
  NOT_TRUSTED: 'not-trusted',
  INVALID: 'invalid',
} as const;

export class InMemoryCache {
  private store = new Map<string, any>();

  get(key: string) {
    return this.store.get(key);
  }

  set(key: string, value: any) {
    this.store.set(key, value);
  }

  has(key: string) {
    return this.store.has(key);
  }

  delete(key: string) {
    return this.store.delete(key);
  }
}

export const resolveDID = async () => ({
  verified: false,
  outcome: TrustResolutionOutcome.NOT_TRUSTED,
});

export const verifyPermissions = async () => ({
  verified: false,
  outcome: TrustResolutionOutcome.NOT_TRUSTED,
});

