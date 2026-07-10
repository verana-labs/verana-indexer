import { Buffer } from 'node:buffer'
import {
  extractOperatorAuthorizationTouches,
  extractVSOperatorAuthorizationIds,
  hasDelegationEvents,
} from '../../../../src/modules/de-height-sync/de_height_sync_service'

const b64 = (value: string) => Buffer.from(value, 'utf8').toString('base64')

function event(type: string, attributes: Record<string, string>, encoded = false) {
  return {
    type,
    attributes: Object.entries(attributes).map(([key, value]) =>
      encoded ? { key: b64(key), value: b64(value) } : { key, value }
    ),
  }
}

describe('de height-sync event extraction', () => {
  it('detects delegation events among unrelated ones', () => {
    expect(hasDelegationEvents([event('coin_spent', { amount: '1uvna' })])).toBe(false)
    expect(hasDelegationEvents([event('grant_operator_authorization', { authz_id: '1' })])).toBe(true)
  })

  it('extracts an operator-authorization grant from a plain event', () => {
    const events = [
      event('grant_operator_authorization', {
        authz_id: '42',
        corporation_id: '7',
        grantee: 'verana1operator',
        with_feegrant: 'false',
      }),
    ]

    expect(extractOperatorAuthorizationTouches(events)).toEqual([
      { authzId: 42, corporationId: 7, grantee: 'verana1operator', revoked: false },
    ])
  })

  it('decodes base64-encoded attributes as emitted in block_result', () => {
    const events = [
      event('grant_operator_authorization', { authz_id: '5', corporation_id: '3', grantee: 'verana1abc' }, true),
    ]

    expect(extractOperatorAuthorizationTouches(events)).toEqual([
      { authzId: 5, corporationId: 3, grantee: 'verana1abc', revoked: false },
    ])
  })

  it('marks revoke events so the row is deleted, and last touch per id wins', () => {
    const events = [
      event('grant_operator_authorization', { authz_id: '9', corporation_id: '3', grantee: 'verana1abc' }),
      event('revoke_operator_authorization', { authz_id: '9', corporation_id: '3', grantee: 'verana1abc' }),
    ]

    expect(extractOperatorAuthorizationTouches(events)).toEqual([
      { authzId: 9, corporationId: 3, grantee: 'verana1abc', revoked: true },
    ])
  })

  it('ignores touches with missing or invalid ids', () => {
    const events = [
      event('grant_operator_authorization', { corporation_id: '3', grantee: 'verana1abc' }),
      event('grant_operator_authorization', { authz_id: '0', corporation_id: '3', grantee: 'verana1abc' }),
    ]

    expect(extractOperatorAuthorizationTouches(events)).toEqual([])
  })

  it('collects unique VS-operator authorization ids across grant/revoke/update', () => {
    const events = [
      event('grant_vs_operator_authorization', { vsoa_id: '1', participant_id: '10' }),
      event('update_vs_operator_authorization', { vsoa_id: '1', participant_id: '11' }),
      event('revoke_vs_operator_authorization', { vsoa_id: '2', participant_id: '12' }),
    ]

    expect(extractVSOperatorAuthorizationIds(events).sort()).toEqual([1, 2])
  })
})
