import { Buffer } from 'node:buffer'
import { buildEventTypeResolverFromEvents } from '../../../../src/modules/xr-height-sync/xr_height_sync_service'

const b64 = (value: string) => Buffer.from(value, 'utf8').toString('base64')

describe('buildEventTypeResolverFromEvents', () => {
  it('reads plain ids that also look like base64', () => {
    const resolve = buildEventTypeResolverFromEvents([
      { type: 'update_exchange_rate', attributes: [{ key: 'id', value: '1400' }] },
    ])
    expect(resolve(1400)).toBe('update_exchange_rate')
  })

  it('decodes attributes whose key is base64-encoded', () => {
    const resolve = buildEventTypeResolverFromEvents([
      { type: 'create_exchange_rate', attributes: [{ key: b64('id'), value: b64('7') }] },
    ])
    expect(resolve(7)).toBe('create_exchange_rate')
    expect(resolve(8)).toBe('SYNC_LEDGER')
  })
})
