import knex from '../../../src/common/utils/db_connection'
import Participant from '../../../src/models/participant'

// The chain checks DID syntax only, so a did column with a length limit can stop the indexer.
describe('did columns', () => {
  afterAll(async () => {
    await knex.destroy()
  })

  it('have no length limit', async () => {
    const limited = await knex
      .withSchema('information_schema')
      .from('columns')
      .where({ table_schema: 'public', column_name: 'did' })
      .whereNotNull('character_maximum_length')
      .orderBy('table_name')
      .pluck('table_name')

    expect(limited).toEqual([])
  })

  it('have no length limit in the participant model', () => {
    const did = `did:example:${'c'.repeat(288)}`
    const now = new Date().toISOString()

    const participant = Participant.fromJson({
      id: 2,
      schema_id: 2,
      role: 'ECOSYSTEM',
      corporation_id: 1,
      created: now,
      modified: now,
      did,
    })

    expect(participant.did).toBe(did)
  })
})
