import type { DIDResolutionResult } from 'did-resolver'

const webMock = jest.fn()
const webVhMock = jest.fn()

jest.mock('web-did-resolver', () => ({
  __esModule: true,
  getResolver: () => ({ web: (...args: unknown[]) => webMock(...args) }),
}))

jest.mock('didwebvh-ts', () => ({
  __esModule: true,
  resolveDID: (...args: unknown[]) => webVhMock(...args),
}))

import { createDidDocumentResolver } from '../../../../src/services/resolver/did-document-resolver'

const DID = 'did:web:service.example.org'
const WEBVH = 'did:webvh:QmAgent:agent.example.org'

function resolution(marker: string): DIDResolutionResult {
  return {
    didResolutionMetadata: {},
    didDocument: { id: DID, service: [{ id: `${DID}#m`, type: 'LinkedDomains', serviceEndpoint: marker }] },
    didDocumentMetadata: {},
  }
}

function failure(error: string): DIDResolutionResult {
  return { didResolutionMetadata: { error }, didDocument: null, didDocumentMetadata: {} }
}

describe('createDidDocumentResolver', () => {
  beforeEach(() => {
    webMock.mockReset()
    webVhMock.mockReset()
  })

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

  it('retries a failed resolution on the next use instead of keeping it for the block', async () => {
    const web = jest.fn().mockResolvedValueOnce(failure('notFound')).mockResolvedValueOnce(resolution('v1'))
    const resolver = createDidDocumentResolver({ web })

    const first = await resolver.resolve(DID)
    const second = await resolver.resolve(DID)
    const third = await resolver.resolve(DID)

    expect(web).toHaveBeenCalledTimes(2)
    expect(first.didResolutionMetadata.error).toBe('notFound')
    expect(second.didDocument?.service?.[0]?.serviceEndpoint).toBe('v1')
    expect(third.didDocument).toEqual(second.didDocument)
  })

  it('routes did:web and did:webvh through their resolvers by default and rejects other methods', async () => {
    webMock.mockResolvedValue(resolution('web'))
    webVhMock.mockResolvedValue({ did: WEBVH, doc: { id: WEBVH }, meta: { versionId: '1-abc' } })
    const resolver = createDidDocumentResolver()

    const [web, webvh, other] = await Promise.all([
      resolver.resolve(DID),
      resolver.resolve(WEBVH),
      resolver.resolve('did:key:z6Mk'),
    ])

    expect(webMock).toHaveBeenCalledWith(DID, expect.anything(), expect.anything(), expect.anything())
    expect(webVhMock).toHaveBeenCalledWith(WEBVH, { verifier: { verify: expect.any(Function) } })
    expect(web.didDocument?.id).toBe(DID)
    expect(webvh).toEqual({
      didResolutionMetadata: {},
      didDocument: { id: WEBVH },
      didDocumentMetadata: { versionId: '1-abc' },
    })
    expect(other.didResolutionMetadata.error).toBe('unsupportedDidMethod')
  })

  it('maps a did:webvh failure to notFound and a thrown error to internalError', async () => {
    webVhMock
      .mockResolvedValueOnce({ did: WEBVH, doc: null, meta: { error: 'invalid log' } })
      .mockRejectedValueOnce(new Error('boom'))
    const resolver = createDidDocumentResolver()

    const notFound = await resolver.resolve(WEBVH)
    const internal = await resolver.resolve(WEBVH)

    expect(notFound.didResolutionMetadata).toEqual({ error: 'notFound' })
    expect(internal.didResolutionMetadata).toEqual({ error: 'internalError', message: 'boom' })
  })
})
