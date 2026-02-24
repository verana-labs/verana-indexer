function normalizeJsonSchema(js: unknown): object | null {
  if (js == null) return null;
  if (typeof js === "object") return js as object;
  if (typeof js === "string") {
    try {
      const parsed = JSON.parse(js);
      return typeof parsed === "object" && parsed !== null ? parsed : null;
    } catch {
      return null;
    }
  }
  return null;
}

export function extractTitleDescriptionFromJsonSchema(
  jsonSchema: unknown
): { title: string | null; description: string | null } {
  const obj = normalizeJsonSchema(jsonSchema);
  if (!obj || typeof obj !== "object") return { title: null, description: null };
  const o = obj as Record<string, unknown>;
  const title = typeof o.title === "string" ? o.title : null;
  const description = typeof o.description === "string" ? o.description : null;
  return { title, description };
}
