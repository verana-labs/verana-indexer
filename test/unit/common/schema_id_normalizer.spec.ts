import { overrideSchemaIdInString } from "../../../src/common/utils/schema_id_normalizer";

describe("overrideSchemaIdInString", () => {
  const actualId = 42;
  const expectedId = "vpr:verana:vna-testnet-1/cs/v1/js/42";

  it("overrides $id when it exists (placeholder)", () => {
    const input = `{"$id":"vpr:verana:VPR_CHAIN_ID/cs/v1/js/VPR_CREDENTIAL_SCHEMA_ID","type":"object"}`;
    const out = overrideSchemaIdInString(input, actualId);
    expect(out).toContain(expectedId);
    expect(out).toContain('"type":"object"');
    expect(out).not.toContain("VPR_CREDENTIAL_SCHEMA_ID");
  });

  it("overrides $id when it is empty string", () => {
    const input = `{"$id": "", "type": "object"}`;
    const out = overrideSchemaIdInString(input, actualId);
    expect(out).toContain(`"$id": "${expectedId}"`);
    expect(out).toContain('"type": "object"');
  });

  it("overrides $id when it is malformed (e.g. 'url' or '2345')", () => {
    const input = `{"$id": "url", "title": "Test"}`;
    const out = overrideSchemaIdInString(input, actualId);
    expect(out).toContain(`"$id": "${expectedId}"`);
    expect(out).toContain('"title": "Test"');
  });

  it("does not change any other content", () => {
    const input = `{"$id":"old","type":"object","properties":{"name":{"type":"string"}},"required":["name"]}`;
    const out = overrideSchemaIdInString(input, actualId);
    expect(out).toContain(expectedId);
    expect(out).toContain('"type":"object"');
    expect(out).toContain('"properties":{"name":{"type":"string"}}');
    expect(out).toContain('"required":["name"]');
    expect(out).not.toContain('"old"');
  });

  it("preserves indentation and newlines exactly", () => {
    const input = `{\n  "$id": "placeholder",\n  "type": "object",\n  "properties": {\n    "foo": { "type": "string" }\n  }\n}`;
    const out = overrideSchemaIdInString(input, actualId);
    expect(out).toBe(`{\n  "$id": "${expectedId}",\n  "type": "object",\n  "properties": {\n    "foo": { "type": "string" }\n  }\n}`);
    expect(out.split("\n").length).toBe(input.split("\n").length);
  });

  it("does not inject $id when it does not exist", () => {
    const input = `{"type":"object","title":"NoId"}`;
    const out = overrideSchemaIdInString(input, actualId);
    expect(out).toBe(input);
    expect(out).not.toContain("$id");
  });

  it("returns unchanged string when $id is absent", () => {
    const input = '{"type":"object"}';
    const out = overrideSchemaIdInString(input, actualId);
    expect(out).toBe(input);
  });
});
