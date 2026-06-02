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

  it("maps permission rows to Participant entries and filters by state", async () => {
    tableRows.permissions = [
      {
        id: 501,
        schema_id: 1234,
        type: "ISSUER",
        did: "did:example:1",
        vs_operator: "verana1op",
        weight: 10000000,
        validator_perm_id: 401,
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
        validator_perm_id: 402,
        revoked: past, // -> REVOKED, filtered out when only ACTIVE requested
      },
    ];
    tableRows.credential_schemas = [{ id: 1234, tr_id: 9876 }];

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
    tableRows.permissions = [
      { id: 1, schema_id: 7, type: "ECOSYSTEM", did: "did:example:eco", validator_perm_id: null },
    ];
    tableRows.credential_schemas = [{ id: 7, tr_id: 70 }];
    const out = await buildParticipations("did:example:eco", NOW, ["ACTIVE"]);
    expect(out[0].role).toBe("ECOSYSTEM");
    expect(out[0].validatorParticipantId).toBeNull();
  });
});
