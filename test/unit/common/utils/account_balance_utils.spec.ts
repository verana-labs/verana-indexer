import { Buffer } from 'node:buffer'
import { extractAccountAddressesFromEvents } from '../../../../src/common/utils/account_balance_utils'

const ACCOUNT = 'verana1evvrzxw9yg5staqdvumd6fupy3jhaxfflla7st'
const b64 = (s: string) => Buffer.from(s).toString('base64')

describe('extractAccountAddressesFromEvents', () => {
  it('reads plain-text attributes', () => {
    const events = [{ type: 'adjust_trust_deposit', attributes: [{ key: 'account', value: ACCOUNT }] }]
    expect(extractAccountAddressesFromEvents(events)).toEqual([ACCOUNT])
  })

  it('decodes base64 attributes', () => {
    const events = [{ type: 'adjust_trust_deposit', attributes: [{ key: b64('account'), value: b64(ACCOUNT) }] }]
    expect(extractAccountAddressesFromEvents(events)).toEqual([ACCOUNT])
  })

  it('ignores non-account keys and invalid values', () => {
    const events = [
      {
        type: 'adjust_trust_deposit',
        attributes: [
          { key: 'amount', value: ACCOUNT },
          { key: 'account', value: '1000' },
          { key: b64('amount'), value: b64(ACCOUNT) },
        ],
      },
    ]
    expect(extractAccountAddressesFromEvents(events)).toEqual([])
  })
})
