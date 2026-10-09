import { mapParticipantApiFields, normalizeVprFeeDiscountRatio } from '../../../src/common/vpr-v4-mapping'

describe('normalizeVprFeeDiscountRatio', () => {
  it('converts the smallest non-zero chain value to 0.0001 instead of a full discount', () => {
    expect(normalizeVprFeeDiscountRatio(1)).toBe(0.0001)
  })

  it('converts the full chain scale to 1', () => {
    expect(normalizeVprFeeDiscountRatio(10000)).toBe(1)
  })

  it('keeps 0 as 0', () => {
    expect(normalizeVprFeeDiscountRatio(0)).toBe(0)
  })

  it('converts intermediate chain values to their decimal ratio', () => {
    expect(normalizeVprFeeDiscountRatio(2500)).toBe(0.25)
    expect(normalizeVprFeeDiscountRatio('500')).toBe(0.05)
  })

  it('clamps values above the chain scale to 1', () => {
    expect(normalizeVprFeeDiscountRatio(10001)).toBe(1)
    expect(normalizeVprFeeDiscountRatio(99999)).toBe(1)
  })

  it('clamps negative values to 0', () => {
    expect(normalizeVprFeeDiscountRatio(-5000)).toBe(0)
  })

  it('returns 0 for non-finite input', () => {
    expect(normalizeVprFeeDiscountRatio(null)).toBe(0)
    expect(normalizeVprFeeDiscountRatio(undefined)).toBe(0)
    expect(normalizeVprFeeDiscountRatio('abc')).toBe(0)
  })
})

describe('mapParticipantApiFields', () => {
  it('exposes both fee discounts as decimals between 0 and 1', () => {
    const mapped = mapParticipantApiFields({
      corporation_id: 3,
      issuance_fee_discount: 2500,
      verification_fee_discount: 1,
    })

    expect(mapped.issuance_fee_discount).toBe(0.25)
    expect(mapped.verification_fee_discount).toBe(0.0001)
  })

  it('drops the columns that are not part of the Participant response', () => {
    const mapped = mapParticipantApiFields({
      corporation_id: 3,
      corporation: 'verana1corp',
      vs_operator_authz_enabled: true,
      vs_operator_authz_spend_limit: [],
      vs_operator_authz_with_feegrant: false,
      vs_operator_authz_fee_spend_limit: [],
      vs_operator_authz_spend_period: '86400s',
    })

    expect(mapped).toEqual({ corporation_id: 3 })
  })

  it('leaves absent discount fields absent', () => {
    expect(mapParticipantApiFields({ corporation_id: 3 })).not.toHaveProperty('issuance_fee_discount')
  })
})
