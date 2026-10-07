import { ServiceBroker } from 'moleculer'
import { ModulesParamsNamesTypes } from '../../../../src/common'
import knex from '../../../../src/common/utils/db_connection'
import { VeranaGovernanceFrameworkMessageTypes } from '../../../../src/common/verana-message-types'
import EcosystemHistoryService from '../../../../src/services/crawl-ec/ec_history.service'
import EcosystemMessageProcessorService from '../../../../src/services/crawl-ec/ec_processor.service'

const ECO = 1
const OPERATOR = 'verana1op'
const CORP = 'verana1pol'
const ADD_HEIGHT = 60193
const INCREASE_HEIGHT = 60200
const addTimestamp = new Date('2026-10-02T13:18:16.000Z')
const increaseTimestamp = new Date('2026-10-02T13:20:00.000Z')

async function insertTx(id: number, height: number, timestamp: Date, type: string, content: Record<string, unknown>) {
  await knex('transaction').insert({
    id,
    height,
    hash: `HASH${id}`,
    codespace: '',
    code: 0,
    gas_used: '0',
    gas_wanted: '0',
    gas_limit: '0',
    fee: JSON.stringify({}),
    timestamp,
    index: 0,
    data: JSON.stringify({}),
  })
  await knex('transaction_message').insert({
    tx_id: id,
    index: 0,
    type,
    sender: OPERATOR,
    content: JSON.stringify(content),
  })
}

describe('ecosystem history labels for EGF changes made through the gf module', () => {
  const broker = new ServiceBroker({ logger: false })
  const processor: any = broker.createService(EcosystemMessageProcessorService)
  const history: any = broker.createService(EcosystemHistoryService)
  const previousHeightSync = process.env.USE_HEIGHT_SYNC_TR
  let activity: any[]

  beforeAll(async () => {
    process.env.USE_HEIGHT_SYNC_TR = 'false'
    for (const table of [
      'governance_framework_document_history',
      'governance_framework_version_history',
      'ecosystem_history',
      'governance_framework_document',
      'governance_framework_version',
      'ecosystem',
      'transaction_message',
      'transaction',
      'module_params',
    ]) {
      await knex(table)
        .del()
        .catch(() => {})
    }
    await knex('module_params').insert({ module: ModulesParamsNamesTypes.EC, params: JSON.stringify({ params: {} }) })
    await knex('ecosystem').insert({
      id: ECO,
      did: 'did:web:eco-one.example',
      corporation_id: 1,
      created: addTimestamp,
      modified: addTimestamp,
      language: 'en',
      height: 10,
      active_version: 1,
    })
    await knex('governance_framework_version').insert({
      ecosystem_id: ECO,
      version: 1,
      created: addTimestamp,
      active_since: addTimestamp,
      gfv_id: 2,
    })

    const addContent = {
      corporation: CORP,
      operator: OPERATOR,
      ecosystem_id: String(ECO),
      doc_language: 'en',
      doc_url: 'https://example.com/egf-v2-en.json',
      doc_digest_sri: 'sha384-v2-en',
      version: 2,
    }
    const increaseContent = { corporation: CORP, operator: OPERATOR, ecosystem_id: String(ECO) }
    await insertTx(
      100,
      ADD_HEIGHT,
      addTimestamp,
      VeranaGovernanceFrameworkMessageTypes.AddGovernanceFrameworkDocument,
      addContent
    )
    await insertTx(
      101,
      INCREASE_HEIGHT,
      increaseTimestamp,
      VeranaGovernanceFrameworkMessageTypes.IncreaseActiveGovernanceFrameworkVersion,
      increaseContent
    )

    await processor.handleEcosystemMessages({
      params: {
        ecosystemList: [
          {
            type: VeranaGovernanceFrameworkMessageTypes.AddGovernanceFrameworkDocument,
            height: ADD_HEIGHT,
            timestamp: addTimestamp,
            txEvents: [],
            content: addContent,
          },
          {
            type: VeranaGovernanceFrameworkMessageTypes.IncreaseActiveGovernanceFrameworkVersion,
            height: INCREASE_HEIGHT,
            timestamp: increaseTimestamp,
            txEvents: [],
            content: increaseContent,
          },
        ],
      },
    })

    const res = await history.getTRHistory({ params: { ecosystem_id: ECO }, meta: {} })
    activity = res.activity
  })

  afterAll(async () => {
    process.env.USE_HEIGHT_SYNC_TR = previousHeightSync
    await broker.stop()
    await knex.destroy()
  })

  it('labels the added version with the gf method name and the operator as account', () => {
    const item = activity.find(
      (a) => a.entity_type === 'GovernanceFrameworkVersion' && String(a.block_height) === String(ADD_HEIGHT)
    )
    expect(item).toBeDefined()
    expect(item.msg).toBe('AddGovernanceFrameworkDocument')
    expect(item.account).toBe(OPERATOR)
  })

  it('labels the activation with the gf method name and the operator as account', () => {
    const item = activity.find(
      (a) => a.entity_type === 'Ecosystem' && String(a.block_height) === String(INCREASE_HEIGHT)
    )
    expect(item).toBeDefined()
    expect(item.msg).toBe('IncreaseActiveGovernanceFrameworkVersion')
    expect(item.account).toBe(OPERATOR)
  })
})
