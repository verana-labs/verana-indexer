import fs from "fs";
import path from "path";
import pino from "pino";
let loadEnvFiles;
try {
  ({ loadEnvFiles } = await import("../common/utils/loadEnv.ts"));
} catch {
  ({ loadEnvFiles } = await import("../common/utils/loadEnv.js"));
}

loadEnvFiles();

const isTrue = (val) =>
  ["1", "true", "yes"].includes(String(val || "").toLowerCase());
const isFalse = (val) =>
  ["0", "false", "no"].includes(String(val || "").toLowerCase());
const getBool = (val, defaultValue) => {
  if (isTrue(val)) return true;
  if (isFalse(val)) return false;
  return defaultValue;
};

const shouldLogToFile = isTrue(process.env.LOG_TO_FILE);
if (!shouldLogToFile) {
} else {
  const LOG_DIR = process.env.LOG_DIR || path.join(process.cwd(), "logs");
  const rawFilePath =
    process.env.ERROR_LOG_FILE || process.env.LOG_FILE_PATH || path.join("logs", "errors.log");

  const hasPathInFile = rawFilePath.includes("/") || rawFilePath.includes("\\");
  const errorFilePath = path.isAbsolute(rawFilePath)
    ? rawFilePath
    : hasPathInFile
      ? path.resolve(process.cwd(), rawFilePath)
      : path.join(LOG_DIR, rawFilePath);
  const errorDir = path.dirname(errorFilePath);

  if (!fs.existsSync(errorDir)) {
    fs.mkdirSync(errorDir, { recursive: true });
  }
  try {
    fs.closeSync(fs.openSync(errorFilePath, "a"));
  } catch (_) {}


  const errorDestination = pino.destination({
    dest: errorFilePath,
    sync: false,
  });

  const errorLogger = pino(
    {
      level: "error",
      timestamp: pino.stdTimeFunctions.isoTime,
    },
    errorDestination
  );


function stripAnsi(str) {
  return typeof str === "string"
    ? str.replace(/\u001b\[[0-9;]*m/g, "")
    : str;
}

function safeStringify(arg) {
  if (arg instanceof Error) {
    return stripAnsi(arg.stack || arg.message);
  }
  if (typeof arg === "string") {
    return stripAnsi(arg);
  }
  try {
    return stripAnsi(JSON.stringify(arg));
  } catch {
    return String(arg);
  }
}

  const originalConsole = {
    error: console.error,
    warn: console.warn,
    log: console.log,
    info: console.info,
  };

  console.error = (...args) => {
    try {
      const message = args.map(safeStringify).join(" ");
      errorLogger.error(message);
    } catch (_) {}

    originalConsole.error.apply(console, args);
  };

  console.warn = (...args) => {
    originalConsole.warn.apply(console, args);
  };

  console.log = (...args) => {
    originalConsole.log.apply(console, args);
  };

  console.info = (...args) => {
    originalConsole.info.apply(console, args);
  };


  process.on("uncaughtException", (err) => {
    try {
      const finalLogger = pino.final(errorLogger, (_err, final) => {
        try {
          final.fatal(
            {
              message: err && err.message,
              stack: err && err.stack,
            },
            "uncaughtException"
          );
        } catch (_) {
          // swallow
        }
      });
      finalLogger(err);
    } catch (_) {}

    originalConsole.error(err);

    setTimeout(() => process.exit(1), 1500);
  });

  process.on("unhandledRejection", (reason) => {
    try {
      if (reason instanceof Error) {
        errorLogger.error(
          {
            message: reason.message,
            stack: reason.stack,
          },
          "unhandledRejection"
        );
      } else {
        errorLogger.error(
          {
            reason: safeStringify(reason),
          },
          "unhandledRejection"
        );
      }

      const finalLogger = pino.final(errorLogger, (_err, final) => {
        try {
          final.error(
            {
              reason: reason instanceof Error ? reason.message : safeStringify(reason),
            },
            "unhandledRejection - final flush"
          );
        } catch (_) {}
      });
      finalLogger(reason instanceof Error ? reason : new Error(String(reason)));
    } catch (_) {}

    originalConsole.error("Unhandled Promise Rejection:", reason);
  });

  process.on("warning", (warning) => {
    try {
      errorLogger.warn({
        name: warning.name,
        message: warning.message,
        stack: warning.stack,
      });
    } catch (_) {}

    originalConsole.warn(warning);
  });


  process.on("SIGTERM", () => {
    setTimeout(() => process.exit(0), 500);
  });

  process.on("SIGINT", () => {
    setTimeout(() => process.exit(0), 500);
  });
}
