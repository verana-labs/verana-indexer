/**
 * Per-table knex mock. `__rows` maps a table name to the rows its query builder
 * resolves to. Every builder method returns the same chainable object, which is
 * thenable (resolves to the configured rows for the table it was created with).
 */
const tableRows: Record<string, any[]> = {};

jest.mock("../../../../src/common/utils/db_connection", () => {
  function makeChain(table: string) {
    const chain: any = {};
    const passthrough = () => chain;
    for (const m of ["select", "where", "whereIn", "whereNull", "andWhere", "orderBy", "first"]) {
      chain[m] = jest.fn(passthrough);
    }
    // `.first()` resolves to a single row; everything else resolves to the array.
    chain.first = jest.fn(() => Promise.resolve((tableRows[table] ?? [])[0]));
    chain.then = (resolve: any, reject: any) =>
      Promise.resolve(tableRows[table] ?? []).then(resolve, reject);
    return chain;
  }
  const knexMock: any = jest.fn((table: string) => makeChain(table));
  knexMock.client = { config: { client: "pg" } };
  return knexMock;
});

import {
  deriveParticipantState,
  buildParticipations,
  buildServices,
  buildPresentations,
  buildEcsCredentials,
} from "../../../../src/services/resolver/trust-resolve-v4.builders";

const NOW = new Date("2026-06-02T00:00:00Z");
const past = "2026-01-01T00:00:00Z";
const future = "2027-01-01T00:00:00Z";

describe("deriveParticipantState", () => {
  it("1. REPAID when slashed and repaid >= slashed", () => {
    expect(deriveParticipantState({ slashed: past, repaid: future }, NOW)).toBe("REPAID");
  });
  it("2. SLASHED when slashed and not repaid (or repaid < slashed)", () => {
    expect(deriveParticipantState({ slashed: past }, NOW)).toBe("SLASHED");
    expect(deriveParticipantState({ slashed: future, repaid: past }, NOW)).toBe("SLASHED");
  });
  it("3. REVOKED when revoked <= now", () => {
    expect(deriveParticipantState({ revoked: past }, NOW)).toBe("REVOKED");
  });
  it("4. EXPIRED when effective_until <= now", () => {
    expect(deriveParticipantState({ effective_until: past }, NOW)).toBe("EXPIRED");
  });
  it("5. FUTURE when effective_from > now", () => {
    expect(deriveParticipantState({ effective_from: future }, NOW)).toBe("FUTURE");
  });
  it("6. ACTIVE within the effective window", () => {
    expect(deriveParticipantState({ effective_from: past, effective_until: future }, NOW)).toBe("ACTIVE");
    expect(deriveParticipantState({}, NOW)).toBe("ACTIVE");
  });
  it("priority: SLASHED outranks REVOKED/EXPIRED", () => {
    expect(
      deriveParticipantState({ slashed: past, revoked: past, effective_until: past }, NOW)
    ).toBe("SLASHED");
  });
  it("revoked in the future does not count as REVOKED", () => {
    expect(deriveParticipantState({ revoked: future, effective_from: past }, NOW)).toBe("ACTIVE");
  });
});

describe("buildParticipations", () => {
  beforeEach(() => {
    for (const k of Object.keys(tableRows)) delete tableRows[k];
  });

  it("maps participant rows to Participant entries and filters by state", async () => {
    tableRows.participants = [
      {
        id: 501,
        schema_id: 1234,
        type: "ISSUER",
        did: "did:example:1",
        vs_operator: "verana1op",
        weight: 10000000,
        validator_participant_id: 401,
        issued: 2345,
        verified: 0,
        participants_holder: 75,
        effective_from: past,
        effective_until: future,
      },
      {
        id: 502,
        schema_id: 1234,
        type: "VERIFIER",
        did: "did:example:1",
        weight: 5000000,
        validator_participant_id: 402,
        revoked: past, // -> REVOKED, filtered out when only ACTIVE requested
      },
    ];
    tableRows.credential_schemas = [{ id: 1234, ecosystem_id: 9876 }];

    const active = await buildParticipations("did:example:1", NOW, ["ACTIVE"]);
    expect(active).toHaveLength(1);
    expect(active[0]).toMatchObject({
      id: 501,
      role: "ISSUER",
      state: "ACTIVE",
      credentialSchemaId: 1234,
      ecosystemId: 9876,
      weight: "10000000uvna",
      vsOperator: "verana1op",
      validatorParticipantId: 401,
      issuedCredentials: 2345,
      participants: { HOLDER: 75 },
    });

    const both = await buildParticipations("did:example:1", NOW, ["ACTIVE", "REVOKED"]);
    expect(both.map((p) => p.state).sort()).toEqual(["ACTIVE", "REVOKED"]);
  });

  it("emits validatorParticipantId null only for ECOSYSTEM role", async () => {
    tableRows.participants = [
      { id: 1, schema_id: 7, type: "ECOSYSTEM", did: "did:example:eco", validator_participant_id: null },
    ];
    tableRows.credential_schemas = [{ id: 7, ecosystem_id: 70 }];
    const out = await buildParticipations("did:example:eco", NOW, ["ACTIVE"]);
    expect(out[0].role).toBe("ECOSYSTEM");
    expect(out[0].validatorParticipantId).toBeNull();
  });
});

describe("buildServices", () => {
  const mcp = { id: "did:example:1#mcp", type: "MCP", serviceEndpoint: "https://x/mcp" };
  const didcomm = {
    id: "did:example:1#did-communication",
    type: "did-communication",
    serviceEndpoint: "wss://x/didcomm",
    accept: ["didcomm/v2"],
  };
  const linkedVp = {
    id: "did:example:1#vt-vp1",
    type: "LinkedVerifiablePresentation",
    serviceEndpoint: "https://x/vp1.json",
  };

  it("returns the non-LinkedVerifiablePresentation service entries verbatim", () => {
    const out = buildServices({ didDocument: { service: [mcp, linkedVp, didcomm] } });
    expect(out).toEqual([mcp, didcomm]);
  });

  it("filters LinkedVerifiablePresentation when type is an array", () => {
    const arrayTyped = { ...linkedVp, type: ["LinkedVerifiablePresentation"] };
    const out = buildServices({ didDocument: { service: [mcp, arrayTyped] } });
    expect(out).toEqual([mcp]);
  });

  it("returns [] when the resolution has no DID Document or services", () => {
    expect(buildServices(undefined)).toEqual([]);
    expect(buildServices({ error: true })).toEqual([]);
    expect(buildServices({ didDocument: {} })).toEqual([]);
    expect(buildServices({ didDocument: { service: null } })).toEqual([]);
  });
});

describe("buildPresentations", () => {
  const noFlags = { unresolvableCredentialIds: false, invalidCredentialIds: false };
  const resolution = {
    didDocument: {
      id: "did:example:x",
      service: [
        { id: "did:example:x#whois", type: "LinkedVerifiablePresentation", serviceEndpoint: "https://x/vp1.json" },
        { id: "#files", type: "relativeRef", serviceEndpoint: "https://x" },
        { id: "#vp2", type: "LinkedVerifiablePresentation", serviceEndpoint: "https://x/vp2.json" },
      ],
    },
  };

  it("maps LinkedVerifiablePresentation entries, resolving relative service ids", () => {
    expect(buildPresentations(resolution, noFlags)).toEqual([
      { id: "https://x/vp1.json", serviceId: "did:example:x#whois", vtcCredentials: [] },
      { id: "https://x/vp2.json", serviceId: "did:example:x#vp2", vtcCredentials: [] },
    ]);
  });

  it("includes the empty sub-lists only when their flags are set", () => {
    const [first] = buildPresentations(resolution, {
      unresolvableCredentialIds: true,
      invalidCredentialIds: true,
    });
    expect(first).toMatchObject({ unresolvableCredentialIds: [], invalidCredentialIds: [] });
  });

  it("dedupes by resolved serviceId and returns [] without a DID Document", () => {
    const dup = {
      didDocument: {
        id: "did:example:x",
        service: [
          { id: "#vp", type: "LinkedVerifiablePresentation", serviceEndpoint: "https://x/a.json" },
          { id: "did:example:x#vp", type: "LinkedVerifiablePresentation", serviceEndpoint: "https://x/b.json" },
        ],
      },
    };
    expect(buildPresentations(dup, noFlags)).toHaveLength(1);
    expect(buildPresentations({ error: true }, noFlags)).toEqual([]);
  });
});

describe("buildEcsCredentials", () => {
  beforeEach(() => {
    for (const k of Object.keys(tableRows)) delete tableRows[k];
  });

  const service = {
    schemaType: "ecs-service",
    id: "did:example:sub",
    issuer: "did:example:org",
    name: "Gov ID issuer",
    type: "VerifiableService",
  };

  it("surfaces the subject and resolves stable ids from participants/schemas", async () => {
    tableRows.participants = [{ id: 501, schema_id: 1, did: "did:example:sub", type: "HOLDER" }];
    tableRows.credential_schemas = [
      { id: 1, ecosystem_id: 9, json_schema: { title: "ServiceCredential", $id: "vpr:verana:net/cs/v1/js/1" } },
    ];

    const out = await buildEcsCredentials({ service });
    expect(out).toHaveLength(1);
    expect(out[0]).toMatchObject({
      ecsSchema: "ServiceCredential",
      ecsSchemaVersion: "v1",
      credentialSchemaId: 1,
      ecosystemId: 9,
      participantId: 501,
      credentialSubject: { id: "did:example:sub", name: "Gov ID issuer", type: "VerifiableService" },
    });
    expect(out[0].credentialSubject).not.toHaveProperty("schemaType");
    expect(out[0].credentialSubject).not.toHaveProperty("issuer");
  });

  it("still surfaces the credential with 0 ids when no participant is indexed", async () => {
    const out = await buildEcsCredentials({ service });
    expect(out[0]).toMatchObject({
      ecsSchema: "ServiceCredential",
      ecsSchemaVersion: "",
      credentialSchemaId: 0,
      issuerParticipantId: 0,
      ecosystemId: 0,
      participantId: 0,
      credentialSubject: { id: "did:example:sub" },
    });
  });

  it("ignores non-ECS resolutions", async () => {
    expect(await buildEcsCredentials({ service: { schemaType: "unknown", id: "did:x" } })).toEqual([]);
    expect(await buildEcsCredentials({ error: true })).toEqual([]);
  });
});
