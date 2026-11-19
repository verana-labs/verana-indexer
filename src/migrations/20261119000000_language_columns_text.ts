import type { Knex } from 'knex'

const PREVIOUS_WIDTHS: Record<string, number> = {
  ecosystem: 2,
  ecosystem_history: 2,
  ecosystem_snapshot: 2,
  ecosystem_document: 2,
  governance_framework_document: 2,
  governance_framework_document_history: 2,
  corporation: 8,
  corporation_history: 8,
  co_governance_framework_document: 8,
}

// The chain accepts any BCP 47 language tag and puts no length limit on a governance framework document tag.
export async function up(knex: Knex): Promise<void> {
  for (const table of Object.keys(PREVIOUS_WIDTHS)) {
    if (await knex.schema.hasColumn(table, 'language')) {
      await knex.raw('ALTER TABLE ?? ALTER COLUMN language TYPE text', [table])
    }
  }
}

export async function down(knex: Knex): Promise<void> {
  for (const [table, width] of Object.entries(PREVIOUS_WIDTHS)) {
    if (await knex.schema.hasColumn(table, 'language')) {
      await knex.raw(`ALTER TABLE ?? ALTER COLUMN language TYPE varchar(${width}) USING left(language, ${width})`, [
        table,
      ])
    }
  }
}
