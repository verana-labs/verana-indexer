import { ServiceBroker } from 'moleculer'
import FeegrantService from '../../../../src/services/feegrant/feegrant.service'

describe('FeegrantService getCreateFeegrantInfo', () => {
  const broker = new ServiceBroker({ logger: false })
  const service: any = broker.createService(FeegrantService)

  afterAll(async () => {
    await broker.stop()
  })

  it('parses a basic allowance grant', () => {
    const info = service.getCreateFeegrantInfo({
      '@type': '/cosmos.feegrant.v1beta1.MsgGrantAllowance',
      allowance: {
        '@type': '/cosmos.feegrant.v1beta1.BasicAllowance',
        spend_limit: [{ amount: '1000', denom: 'uvna' }],
        expiration: '2027-01-01T00:00:00Z',
      },
    })
    expect(info.type).toBe('/cosmos.feegrant.v1beta1.BasicAllowance')
    expect(info.spend_limit).toBe('1000')
    expect(info.denom).toBe('uvna')
  })

  it('parses an AllowedMsgAllowance wrapping a basic allowance', () => {
    const info = service.getCreateFeegrantInfo({
      '@type': '/cosmos.feegrant.v1beta1.MsgGrantAllowance',
      allowance: {
        '@type': '/cosmos.feegrant.v1beta1.AllowedMsgAllowance',
        allowance: {
          '@type': '/cosmos.feegrant.v1beta1.BasicAllowance',
          spend_limit: [{ amount: '500', denom: 'uvna' }],
        },
        allowed_messages: ['/verana.de.v1.MsgSomething'],
      },
    })
    expect(info.type).toBe('/cosmos.feegrant.v1beta1.BasicAllowance')
    expect(info.spend_limit).toBe('500')
  })

  it('returns type null for a non-grant message (group MsgVote, devnet block 70407)', () => {
    const info = service.getCreateFeegrantInfo({
      '@type': '/cosmos.group.v1.MsgVote',
      proposal_id: '1',
      voter: 'verana1c3hjgq0u0lgtyeh55yuyxqq952p8z08tqjpdq7',
      option: 'VOTE_OPTION_YES',
      exec: 'EXEC_TRY',
    })
    expect(info.type).toBeNull()
    expect(info.spend_limit).toBeUndefined()
    expect(info.denom).toBeUndefined()
    expect(info.expiration).toBeUndefined()
  })

  it('still throws for a grant message whose allowance type cannot be resolved', () => {
    expect(() =>
      service.getCreateFeegrantInfo({
        '@type': '/cosmos.feegrant.v1beta1.MsgGrantAllowance',
        allowance: { '@type': '/some.future.v9.MysteryAllowance' },
      })
    ).toThrow('Cannot detect feegrant type')
  })
})
