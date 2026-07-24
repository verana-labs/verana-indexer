import { schemaIdFromUrl } from '../../../../src/services/resolver/verre-registry-adapter'

describe('schemaIdFromUrl', () => {
  it.each([
    ['vpr:verana:vna-testnet-1:cs:2', 2],
    ['verana:cs:1', 1],
    ['vpr:verana:vna-testnet-1/cs/v1/js/137', 137],
    ['https://reg.example/cs/v1/js/9', 9],
    ['42', 42],
  ])('parses %s -> %s', (input, expected) => {
    expect(schemaIdFromUrl(input)).toBe(expected)
  })

  it.each([
    ['https://agent.example/vt/cs/v1/js/ecs-service'],
    ['vpr:verana:vna-testnet-1:cs:'],
    ['not-a-ref'],
    [''],
  ])('returns null for %s', (input) => {
    expect(schemaIdFromUrl(input)).toBeNull()
  })
})
