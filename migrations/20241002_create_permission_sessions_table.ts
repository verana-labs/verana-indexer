import { Knex } from 'knex';

export async function up(knex: Knex): Promise<void> {
    // Create permission_sessions table safely
    const hasPermissionSessions = await knex.schema.hasTable('permission_sessions');
    if (!hasPermissionSessions) {
        await knex.schema.createTable('permission_sessions', (table) => {
            table.bigIncrements('id').primary();
            table.string('controller', 255).notNullable().index();
            table.string('agent_perm_id', 50).notNullable();
            table.string('wallet_agent_perm_id', 50).notNullable();
            table.json('authz').notNullable(); // Array of authorization entries
            table.timestamp('created').notNullable().defaultTo(knex.fn.now());
            table.timestamp('modified').notNullable().defaultTo(knex.fn.now());

            // Foreign key constraints
            table.foreign('agent_perm_id').references('id').inTable('permissions');
            table.foreign('wallet_agent_perm_id').references('id').inTable('permissions');

            // Indexes
            table.index(['controller', 'modified']);
            table.index(['agent_perm_id']);
            table.index(['wallet_agent_perm_id']);
        });
    }

    // Create permission_events table safely
    const hasPermissionEvents = await knex.schema.hasTable('permission_events');
    if (!hasPermissionEvents) {
        await knex.schema.createTable('permission_events', (table) => {
            table.bigIncrements('id').primary();
            table.string('perm_id', 50).notNullable();
            table.enum('action', [
                'CREATE', 'UPDATE', 'VALIDATE', 'REVOKE', 'EXTEND',
                'SLASH', 'REPAY', 'CANCEL_VP', 'START_VP', 'RENEW_VP', 'SET_VP_VALIDATED'
            ]).notNullable();
            table.string('actor', 255).notNullable();
            table.json('data').nullable(); // Event-specific data
            table.timestamp('created').notNullable().defaultTo(knex.fn.now());

            // Foreign key constraint
            table.foreign('perm_id').references('id').inTable('permissions');

            // Indexes
            table.index(['perm_id', 'created']);
            table.index(['actor', 'created']);
            table.index(['action']);
        });
    }
}

export async function down(knex: Knex): Promise<void> {
    await knex.schema.dropTableIfExists('permission_events');
    await knex.schema.dropTableIfExists('permission_sessions');
}
