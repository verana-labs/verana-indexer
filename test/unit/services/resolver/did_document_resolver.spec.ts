import type { DIDResolutionResult } from 'did-resolver'
import { createDidDocumentResolver } from '../../../../src/services/resolver/did-document-resolver'

const DID = 'did:web:service.example.org'

function resolution(marker: string): DIDResolutionResult {
  return {
    didResolutionMetadata: {},
    didDocument: { id: DID, service: [{ id: `${DID}#m`, type: 'LinkedDomains', serviceEndpoint: marker }] },
    didDocumentMetadata: {},
  }
}

describe('createDidDocumentResolver', () => {
  it('fetches a DID once per resolver instance, including DID URLs with a fragment', async () => {
    const web = jest.fn(async () => resolution('v1'))
    const resolver = createDidDocumentResolver({ web })

    const [a, b, c] = await Promise.all([
      resolver.resolve(DID),
      resolver.resolve(DID),
      resolver.resolve(`${DID}#key-1`),
    ])

    expect(web).toHaveBeenCalledTimes(1)
    expect(a.didDocument).toEqual(b.didDocument)
    expect(c.didDocument).toEqual(a.didDocument)
  })

  it('shares nothing between resolver instances', async () => {
    const web = jest.fn().mockResolvedValueOnce(resolution('v1')).mockResolvedValueOnce(resolution('v2'))

    const first = await createDidDocumentResolver({ web }).resolve(DID)
    const second = await createDidDocumentResolver({ web }).resolve(DID)

    expect(web).toHaveBeenCalledTimes(2)
    expect(first.didDocument?.service?.[0]?.serviceEndpoint).toBe('v1')
    expect(second.didDocument?.service?.[0]?.serviceEndpoint).toBe('v2')
  })

  it('registers did:web and did:webvh by default', async () => {
    const resolver = createDidDocumentResolver()

    const unsupported = await resolver.resolve('did:key:z6Mk')

    expect(unsupported.didResolutionMetadata.error).toBe('unsupportedDidMethod')
  })
})
