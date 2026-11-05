import { toBase64 } from '@cosmjs/encoding'
import { Registry } from '@cosmjs/proto-signing'
import { defaultRegistryTypes } from '@cosmjs/stargate'
import { decodeProposalMessages } from '../../../../src/services/crawl-group/group_helpers'

// decodeProposalMessages is the whole implementation of the spec's "messages[] JSON-decoded with their @type"
// contract; stored MsgSubmitProposal content carries Anys as { type_url, value: base64 }.
describe('decodeProposalMessages', () => {
  const decode = decodeProposalMessages

  it('returns an empty array for non-array input', () => {
    expect(decode(undefined)).toEqual([])
    expect(decode(null)).toEqual([])
    expect(decode({})).toEqual([])
  })

  it('decodes a base64 Any into JSON fields under @type', () => {
    const registry = new Registry([...defaultRegistryTypes] as ConstructorParameters<typeof Registry>[0])
    const encoded = registry.encode({
      typeUrl: '/cosmos.group.v1.MsgVote',
      value: { proposalId: 7, voter: 'verana1voter', option: 1, metadata: '', exec: 0 },
    })
    const value = toBase64(encoded)

    const [decoded] = decode([{ type_url: '/cosmos.group.v1.MsgVote', value }])
    expect(decoded['@type']).toBe('/cosmos.group.v1.MsgVote')
    expect(decoded.voter).toBe('verana1voter')
    expect(String(decoded.proposalId ?? decoded.proposal_id)).toBe('7')
  })

  it('normalizes an already-decoded message to the @type key', () => {
    const [decoded] = decode([{ type_url: '/verana.ec.v1.MsgCreateEcosystem', did: 'did:example:ec' }])
    expect(decoded).toEqual({ '@type': '/verana.ec.v1.MsgCreateEcosystem', did: 'did:example:ec' })
    expect('type_url' in decoded).toBe(false)
  })

  it('keeps the raw value when the type is not in the registry', () => {
    const [decoded] = decode([{ type_url: '/unknown.v1.MsgMystery', value: 'CgFh' }])
    expect(decoded).toEqual({ '@type': '/unknown.v1.MsgMystery', value: 'CgFh' })
  })

  it('passes through entries with no type url', () => {
    expect(decode([{ foo: 'bar' }])).toEqual([{ foo: 'bar' }])
  })
})
