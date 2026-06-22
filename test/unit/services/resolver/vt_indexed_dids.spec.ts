import {
  INDEXED_DIDS_DEFAULT_LIMIT,
  INDEXED_DIDS_MAX_LIMIT,
  decodeOffsetCursor,
  encodeOffsetCursor,
  parseIndexedDidsLimit,
} from "../../../../src/services/resolver/vt_indexed_dids";

describe("vt_indexed_dids helpers", () => {
  describe("parseIndexedDidsLimit", () => {
    it("defaults when missing", () => {
      for (const raw of [undefined, null, ""]) {
        const r = parseIndexedDidsLimit(raw);
        expect(r).toEqual({ ok: true, value: INDEXED_DIDS_DEFAULT_LIMIT });
      }
    });

    it("accepts in-range integers (string or number)", () => {
      expect(parseIndexedDidsLimit("500")).toEqual({ ok: true, value: 500 });
      expect(parseIndexedDidsLimit(1)).toEqual({ ok: true, value: 1 });
      expect(parseIndexedDidsLimit(INDEXED_DIDS_MAX_LIMIT)).toEqual({
        ok: true,
        value: INDEXED_DIDS_MAX_LIMIT,
      });
    });

    it("rejects out-of-range, zero, negative and non-integers", () => {
      for (const raw of [0, -1, 1.5, INDEXED_DIDS_MAX_LIMIT + 1, "abc"]) {
        expect(parseIndexedDidsLimit(raw).ok).toBe(false);
      }
    });
  });

  describe("offset cursor round-trip", () => {
    it("matches the spec example payload", () => {
      expect(encodeOffsetCursor(1000)).toBe("eyJvZmZzZXQiOjEwMDB9");
      expect(decodeOffsetCursor("eyJvZmZzZXQiOjEwMDB9")).toEqual({ ok: true, value: 1000 });
    });

    it("treats missing cursor as offset 0", () => {
      for (const raw of [undefined, null, ""]) {
        expect(decodeOffsetCursor(raw)).toEqual({ ok: true, value: 0 });
      }
    });

    it("rejects malformed cursors", () => {
      expect(decodeOffsetCursor("not-base64-json").ok).toBe(false);
      expect(decodeOffsetCursor(123).ok).toBe(false);
      const negative = Buffer.from(JSON.stringify({ offset: -1 }), "utf-8").toString("base64");
      expect(decodeOffsetCursor(negative).ok).toBe(false);
      const noOffset = Buffer.from(JSON.stringify({ foo: 1 }), "utf-8").toString("base64");
      expect(decodeOffsetCursor(noOffset).ok).toBe(false);
    });
  });
});
