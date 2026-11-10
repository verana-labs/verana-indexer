import { Knex } from 'knex'

export async function up(knex: Knex): Promise<void> {
  const exists = await knex.schema.hasTable('ecosystem')
  if (!exists) return

  await knex.raw(`
    DO $$
    DECLARE c record;
    BEGIN
      FOR c IN
        SELECT conname FROM pg_constraint
        WHERE conrelid = 'public.ecosystem'::regclass AND contype = 'u' AND pg_get_constraintdef(oid) = 'UNIQUE (height)'
      LOOP
        EXECUTE format('ALTER TABLE public.ecosystem DROP CONSTRAINT %I', c.conname);
      END LOOP;
    END $$;
  `)
}

export async function down(knex: Knex): Promise<void> {
  const exists = await knex.schema.hasTable('ecosystem')
  if (!exists) return

  await knex.schema.alterTable('ecosystem', (table) => {
    table.unique(['height'])
  })
}
