import { ServiceBroker } from "moleculer";
import CredentialSchemaDatabaseService from "../../../../src/services/crawl-cs/cs_database.service";
import knex from "../../../../src/common/utils/db_connection";
import { SERVICE } from "../../../../src/common";
describe("CredentialSchemaDatabaseService Tests", () => {
  const broker = new ServiceBroker({ logger: false });
  const serviceKey = SERVICE.V1.CredentialSchemaDatabaseService.path;
  let schema: any;

  beforeAll(async () => {
    broker.createService(CredentialSchemaDatabaseService);
    await broker.start();

    await knex("credential_schemas").del();
    await knex("credential_schema_history").del();
  });

  afterAll(async () => {
    await broker.stop();
    await knex.destroy();
  });

  it("should insert a credential schema", async () => {
    const payload = {
      tr_id: "6",
      json_schema: JSON.stringify({
        $id: "/vpr/v1/cs/js/1",
        type: "object",
        $schema: "https://json-schema.org/draft/2020-12/schema",
        properties: {
          credentialSubject: {
            type: "object",
            required: [
              "id",
              "name",
              "logo",
              "registryId",
              "registryUrl",
              "address",
              "type",
              "countryCode",
            ],
            properties: {
              id: { type: "string", format: "uri" },
              logo: {
                type: "string",
                contentEncoding: "base64",
                contentMediaType: "image/png",
              },
              name: { type: "string", maxLength: 256, minLength: 0 },
              type: {
                type: "string",
                enum: ["PUBLIC", "PRIVATE", "FOUNDATION"],
              },
              address: { type: "string", maxLength: 1024, minLength: 0 },
              registryId: { type: "string", maxLength: 256, minLength: 0 },
              countryCode: { type: "string", maxLength: 2, minLength: 2 },
              registryUrl: { type: "string", maxLength: 256, minLength: 0 },
            },
          },
        },
      }),
      deposit: "10000000",
      isActive: false,
      issuer_grantor_validation_validity_period: 365,
      verifier_grantor_validation_validity_period: 365,
      issuer_validation_validity_period: 180,
      verifier_validation_validity_period: 180,
      holder_validation_validity_period: 180,
      issuer_perm_management_mode: 2,
      verifier_perm_management_mode: 2,
      created: new Date().toISOString(),
      archived: null,
      modified: new Date().toISOString(),
    };

    const upsertRes = await broker.call(`${serviceKey}.upsert`, { payload });
    schema = upsertRes.data?.result || upsertRes.result || upsertRes;

    expect(schema.tr_id).toBe("6");
    expect(schema.id).toBeDefined();
  });
  it("should update a credential schema", async () => {
    const payload = {
      id: schema?.id,
      issuer_grantor_validation_validity_period: 1000,
      verifier_grantor_validation_validity_period: 1000,
      issuer_validation_validity_period: 1000,
      verifier_validation_validity_period: 1000,
      holder_validation_validity_period: 1000,
      issuer_perm_management_mode: 1000,
      verifier_perm_management_mode: 1000,
    };

    const upsertRes = await broker.call(`${serviceKey}.update`, { payload });
    schema = upsertRes.data?.updated || upsertRes.updated || upsertRes;
    expect(schema.tr_id).toBe("6");
    expect(schema.id).toBeDefined();
  });

  it("should archive a credential schema", async () => {
    const archiveRes = await broker.call(`${serviceKey}.archive`, {
      payload: {
        id: schema.id,
        archive: true,
        modified: new Date().toISOString(),
      },
    });

    const archiveSuccess = archiveRes.data?.success ?? archiveRes.success;
    expect(archiveSuccess).toBe(true);
  });
  it("should reflect the archive status in the database", async () => {
    const dbSchema = await knex("credential_schemas")
      .where({ id: schema.id })
      .first();
    const unarchiveSuccess = dbSchema ?? dbSchema;

    expect(unarchiveSuccess.isActive).toBe(true);
  });
  it("should unarchive a credential schema", async () => {
    const unarchiveRes = await broker.call(`${serviceKey}.archive`, {
      payload: {
        id: schema.id,
        archive: false,
        modified: new Date().toISOString(),
      },
    });

    const unarchiveSuccess = unarchiveRes.data?.success ?? unarchiveRes.success;
    expect(unarchiveSuccess).toBe(true);
  });
});
