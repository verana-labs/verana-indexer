import { Knex } from 'knex'

import { toProtoModule, toShortMessageType, toSnakeCaseAction } from '../services/api/indexer_event_utils'

const BATCH_SIZE = 1000

export async function up(knex: Knex): Promise<void> {
  if (!(await knex.schema.hasTable('indexer_events'))) return

  let lastId = 0
  for (;;) {
    const rows = await knex('indexer_events')
      .select('id', 'module', 'payload')
      .where('id', '>', lastId)
      .orderBy('id', 'asc')
      .limit(BATCH_SIZE)
    if (rows.length === 0) break

    for (const row of rows) {
      const payload = (row.payload && typeof row.payload === 'object' ? row.payload : {}) as Record<string, unknown>
      const updatedPayload = {
        ...payload,
        module: toProtoModule(String(payload.module ?? row.module)),
        action: typeof payload.action === 'string' ? toSnakeCaseAction(payload.action) : payload.action,
        message_type:
          typeof payload.message_type === 'string' ? toShortMessageType(payload.message_type) : payload.message_type,
      }
      await knex('indexer_events')
        .where('id', row.id)
        .update({ module: toProtoModule(String(row.module)), payload: updatedPayload })
    }

    lastId = Number(rows[rows.length - 1].id)
  }
}

export async function down(): Promise<void> {}
