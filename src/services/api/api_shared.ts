export type LoggerLike = {
  info?: (...args: any[]) => void;
  warn?: (...args: any[]) => void;
  error?: (...args: any[]) => void;
};

export function createLogger(logger?: LoggerLike) {
  const base = logger ?? console;
  return {
    info: (...args: any[]) => (base.info ? base.info(...args) : console.log(...args)),
    warn: (...args: any[]) => (base.warn ? base.warn(...args) : console.warn(...args)),
    error: (...args: any[]) => (base.error ? base.error(...args) : console.error(...args)),
  };
}

export function isUnknownMessageError(errorMessage: string): boolean {
  if (!errorMessage) return false;
  return (
    errorMessage.includes("Unknown Verana message types") ||
    errorMessage.includes("UNKNOWN VERANA MESSAGE TYPES")
  );
}

export function isValidDid(value: unknown): value is string {
  return typeof value === "string" && /^did:[a-z0-9]+:.+/i.test(value.trim());
}

export function applyBlockHeightFilter(
  query: { andWhere: (...args: any[]) => any },
  args: { blockHeight?: unknown; afterBlockHeight?: unknown },
  column: string
) {
  if (Number.isInteger(args.blockHeight)) {
    query.andWhere(column, Number(args.blockHeight));
  } else if (Number.isInteger(args.afterBlockHeight)) {
    query.andWhere(column, ">", Number(args.afterBlockHeight));
  }
  return query;
}

export function toIsoSeconds(value: Date | string = new Date()): string {
  const date = value instanceof Date ? value : new Date(value);
  return date.toISOString().replace(/\.\d{3}Z$/, "Z");
}

