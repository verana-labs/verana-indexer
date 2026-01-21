import { Knex } from "knex";

export const ALLOWED_SORT_ATTRIBUTES = ["id", "modified", "created"] as const;

export type SortAttribute = typeof ALLOWED_SORT_ATTRIBUTES[number];

export interface SortOrder {
  attribute: SortAttribute;
  direction: "asc" | "desc";
}

interface OrderableQueryBuilder {
  orderBy(column: string, direction?: "asc" | "desc"): any;
}

export function parseSortParameter(sortParam?: string): SortOrder[] {
  if (!sortParam || typeof sortParam !== "string") {
    return [];
  }

  const sortOrders: SortOrder[] = [];
  const attributes = sortParam.split(",").map((attr) => attr.trim()).filter(Boolean);

  for (const attr of attributes) {
    let attribute: string;
    let direction: "asc" | "desc";

    if (attr.startsWith("-")) {
      attribute = attr.substring(1);
      direction = "desc";
    } else if (attr.startsWith("+")) {
      attribute = attr.substring(1);
      direction = "asc";
    } else {
      attribute = attr;
      direction = "asc";
    }

    if (!ALLOWED_SORT_ATTRIBUTES.includes(attribute as SortAttribute)) {
      throw new Error(
        `Invalid sort attribute: "${attribute}". Allowed attributes are: ${ALLOWED_SORT_ATTRIBUTES.join(", ")}`
      );
    }

    sortOrders.push({
      attribute: attribute as SortAttribute,
      direction,
    });
  }

  return sortOrders;
}

export function applyOrdering<T extends OrderableQueryBuilder>(
  queryBuilder: T,
  sortParam?: string,
  tablePrefix: string = ""
): T {
  const sortOrders = parseSortParameter(sortParam);
  let hasIdInSort = false;
  let resultQuery = queryBuilder;

  for (const { attribute, direction } of sortOrders) {
    const columnName = `${tablePrefix}${attribute}`;
    resultQuery = resultQuery.orderBy(columnName, direction) as T;
    
    if (attribute === "id") {
      hasIdInSort = true;
    }
  }

  if (!hasIdInSort) {
    const idColumnName = `${tablePrefix}id`;
    resultQuery = resultQuery.orderBy(idColumnName, "desc") as T;
  }

  return resultQuery;
}

export function sortByStandardAttributes<T>(
  items: T[],
  sortParam: string | undefined,
  opts: {
    getId: (item: T) => string | number;
    getCreated?: (item: T) => string | Date | undefined | null;
    getModified?: (item: T) => string | Date | undefined | null;
    defaultAttribute?: SortAttribute;
    defaultDirection?: "asc" | "desc";
  }
): T[] {
  const sortOrders = sortParam ? parseSortParameter(sortParam) : [];
  const hasCustomSort = sortOrders.length > 0;

  const effectiveDefaultAttr: SortAttribute = opts.defaultAttribute || "modified";
  const effectiveDefaultDir: "asc" | "desc" = opts.defaultDirection || "asc";

  return items.sort((a, b) => {
    const getDateMs = (v: string | Date | undefined | null): number => {
      if (!v) return 0;
      if (v instanceof Date) return v.getTime();
      const d = new Date(v);
      return Number.isNaN(d.getTime()) ? 0 : d.getTime();
    };

    const applyOne = (attribute: SortAttribute, direction: "asc" | "desc"): number => {
      let av: number | string = 0;
      let bv: number | string = 0;

      if (attribute === "id") {
        av = String(opts.getId(a));
        bv = String(opts.getId(b));
      } else if (attribute === "created") {
        av = getDateMs(opts.getCreated ? opts.getCreated(a) : undefined);
        bv = getDateMs(opts.getCreated ? opts.getCreated(b) : undefined);
      } else if (attribute === "modified") {
        av = getDateMs(opts.getModified ? opts.getModified(a) : undefined);
        bv = getDateMs(opts.getModified ? opts.getModified(b) : undefined);
      }

      let cmp = 0;
      if (typeof av === "number" && typeof bv === "number") {
        cmp = av - bv;
      } else {
        cmp = String(av).localeCompare(String(bv));
      }

      if (cmp === 0) return 0;
      return direction === "asc" ? cmp : -cmp;
    };

    for (const { attribute, direction } of sortOrders) {
      const c = applyOne(attribute, direction);
      if (c !== 0) return c;
    }

    if (!hasCustomSort) {
      const c = applyOne(effectiveDefaultAttr, effectiveDefaultDir);
      if (c !== 0) return c;
    }

    return applyOne("id", "desc");
  });
}

export function validateSortParameter(sortParam?: string): boolean {
  if (!sortParam) {
    return true;
  }

  parseSortParameter(sortParam);
  return true;
}
