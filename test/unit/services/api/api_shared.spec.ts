import {
  parseAtBlockHeightHeader,
  parseListLimit,
  validateAtBlockHeightCeiling,
} from '../../../../src/services/api/api_shared'

describe('parseAtBlockHeightHeader', () => {
  it('rejects height 0 because the spec requires a positive integer', () => {
    expect(parseAtBlockHeightHeader('0')).toEqual({
      ok: false,
      message: 'At-Block-Height must be a positive integer',
      errorType: 'AT_BLOCK_HEIGHT_INVALID',
    })
  })

  it('rejects a negative height', () => {
    expect(parseAtBlockHeightHeader('-1')).toMatchObject({ ok: false, errorType: 'AT_BLOCK_HEIGHT_INVALID' })
  })

  it('rejects a non-numeric header', () => {
    expect(parseAtBlockHeightHeader('abc')).toMatchObject({ ok: false, errorType: 'AT_BLOCK_HEIGHT_INVALID' })
  })

  it('rejects an empty header', () => {
    expect(parseAtBlockHeightHeader('')).toMatchObject({ ok: false, errorType: 'AT_BLOCK_HEIGHT_INVALID' })
  })

  it('rejects a fractional height', () => {
    expect(parseAtBlockHeightHeader('10.5')).toMatchObject({ ok: false, errorType: 'AT_BLOCK_HEIGHT_INVALID' })
  })

  it('accepts the lowest valid height and any higher integer', () => {
    expect(parseAtBlockHeightHeader('1')).toEqual({ ok: true, height: 1 })
    expect(parseAtBlockHeightHeader('1500004')).toEqual({ ok: true, height: 1500004 })
  })
})

describe('validateAtBlockHeightCeiling', () => {
  it('accepts a height equal to the indexed height', () => {
    expect(validateAtBlockHeightCeiling(42, 42)).toEqual({ ok: true, height: 42 })
  })

  it('rejects a height one above the indexed height', () => {
    expect(validateAtBlockHeightCeiling(43, 42)).toEqual({
      ok: false,
      message: 'Requested height 43 exceeds indexed height 42',
      errorType: 'AT_BLOCK_HEIGHT_AHEAD',
    })
  })

  it('rejects every height while the indexer reports height 0', () => {
    expect(validateAtBlockHeightCeiling(1, 0)).toMatchObject({ ok: false, errorType: 'AT_BLOCK_HEIGHT_AHEAD' })
    expect(validateAtBlockHeightCeiling(1500004, 0)).toMatchObject({ ok: false, errorType: 'AT_BLOCK_HEIGHT_AHEAD' })
  })
})

describe('parseListLimit', () => {
  it('falls back to 64 when the limit is absent, null or empty', () => {
    expect(parseListLimit(undefined)).toEqual({ ok: true, limit: 64 })
    expect(parseListLimit(null)).toEqual({ ok: true, limit: 64 })
    expect(parseListLimit('')).toEqual({ ok: true, limit: 64 })
  })

  it('rejects a limit outside 1..1024 instead of clamping it', () => {
    expect(parseListLimit('0')).toEqual({ ok: false, message: '"limit" must be an integer between 1 and 1024' })
    expect(parseListLimit('1025')).toEqual({ ok: false, message: '"limit" must be an integer between 1 and 1024' })
    expect(parseListLimit(-5)).toMatchObject({ ok: false })
  })

  it('rejects a non-integer limit', () => {
    expect(parseListLimit('abc')).toMatchObject({ ok: false })
    expect(parseListLimit(1.5)).toMatchObject({ ok: false })
  })

  it('accepts the boundary limits 1 and 1024', () => {
    expect(parseListLimit('1')).toEqual({ ok: true, limit: 1 })
    expect(parseListLimit(1024)).toEqual({ ok: true, limit: 1024 })
  })

  it('honours custom bounds, as the verifiable-trust changes method needs', () => {
    const bounds = { max: 1000, fallback: 100 }
    expect(parseListLimit(undefined, bounds)).toEqual({ ok: true, limit: 100 })
    expect(parseListLimit('1000', bounds)).toEqual({ ok: true, limit: 1000 })
    expect(parseListLimit('1001', bounds)).toEqual({
      ok: false,
      message: '"limit" must be an integer between 1 and 1000',
    })
  })
})
