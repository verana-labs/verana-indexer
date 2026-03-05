import { parseSortParameter, sortByStandardAttributes } from "../../../../src/common/utils/query_ordering";

describe("query_ordering role participant support", () => {
  it("accepts new participant role sort attributes", () => {
    const parsed = parseSortParameter("-participants_ecosystem,+participants_holder");
    expect(parsed).toEqual([
      { attribute: "participants_ecosystem", direction: "desc" },
      { attribute: "participants_holder", direction: "asc" },
    ]);
  });

  it("sorts by participants_holder", () => {
    const rows = [
      { id: 1, participants_holder: 2, modified: "2026-01-01T00:00:00Z" },
      { id: 2, participants_holder: 5, modified: "2026-01-01T00:00:00Z" },
      { id: 3, participants_holder: 1, modified: "2026-01-01T00:00:00Z" },
    ];
    const sorted = sortByStandardAttributes(rows, "-participants_holder", {
      getId: (r) => r.id,
      getModified: (r) => r.modified,
      getParticipantsHolder: (r) => r.participants_holder,
    });

    expect(sorted.map((r) => r.id)).toEqual([2, 1, 3]);
  });

  it("rejects unknown sort attributes", () => {
    expect(() => parseSortParameter("participants_unknown")).toThrow(/Invalid sort attribute/);
  });
});
