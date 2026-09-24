import { ed25519 } from '@noble/curves/ed25519.js'
import {
  type DIDCache,
  type DIDResolutionResult,
  type DIDResolver,
  Resolver,
  type ResolverRegistry,
} from 'did-resolver'
import { resolveDID as resolveWebVh } from 'didwebvh-ts'
import { getResolver as getWebResolver } from 'web-did-resolver'

// Same registry verre builds internally (build/libraries/did-resolver.js); verre does not export it.
const webvh: DIDResolver = async (did) => {
  try {
    const result = await resolveWebVh(did, {
      verifier: { verify: async (signature, message, publicKey) => ed25519.verify(signature, message, publicKey) },
    })
    if (result.meta?.error || !result.doc) {
      return { didResolutionMetadata: { error: 'notFound' }, didDocument: null, didDocumentMetadata: result.meta ?? {} }
    }
    return { didResolutionMetadata: {}, didDocument: result.doc, didDocumentMetadata: result.meta ?? {} }
  } catch (e) {
    const message = e instanceof Error ? e.message : String(e)
    return { didResolutionMetadata: { error: 'internalError', message }, didDocument: null, didDocumentMetadata: {} }
  }
}

const registry: ResolverRegistry = { ...getWebResolver(), webvh }

// Successful fetches are shared for the lifetime of the resolver, failures are retried on next use; no TTL.
function scopedCache(): DIDCache {
  const inflight = new Map<string, Promise<DIDResolutionResult>>()
  return (parsed, resolve) => {
    let hit = inflight.get(parsed.did)
    if (!hit) {
      hit = resolve()
      inflight.set(parsed.did, hit)
      hit.then(
        (result) => {
          if (result.didResolutionMetadata?.error) inflight.delete(parsed.did)
        },
        () => inflight.delete(parsed.did)
      )
    }
    return hit
  }
}

export function createDidDocumentResolver(methods: ResolverRegistry = registry): Resolver {
  return new Resolver(methods, { cache: scopedCache() })
}
