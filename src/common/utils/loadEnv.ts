import fs from 'fs';
import path from 'path';
import { fileURLToPath } from 'url';
import * as dotenv from 'dotenv';

const resolveModuleDir = () => {
  if (typeof __dirname === 'string') {
    return __dirname;
  }

  try {
    // eslint-disable-next-line no-eval
    const meta = (0, eval)('import.meta') as { url?: string };
    if (meta && meta.url) {
      return path.dirname(fileURLToPath(meta.url));
    }
  } catch {
    // ignore, will fall back below
  }

  return process.cwd();
};

const discoverSearchRoots = () => {
  const moduleDir = resolveModuleDir();

  return Array.from(
    new Set([
      process.cwd(),
      path.resolve(process.cwd(), '..'),
      path.resolve(process.cwd(), '../..'),
      moduleDir,
      path.resolve(moduleDir, '..'),
      path.resolve(moduleDir, '../..'),
      path.resolve(moduleDir, '../../..'),
    ]),
  );
};

export const loadEnvFiles = () => {
  const searchRoots = discoverSearchRoots();
  const defaultFiles = ['.env', '.env.example', 'docker.env'];
  const candidates = searchRoots.flatMap((rootDir) =>
    defaultFiles.map((relativePath) => path.resolve(rootDir, relativePath)),
  );

  candidates.forEach((filePath) => {
    if (!fs.existsSync(filePath)) {
      return;
    }

    const normalized = fs
      .readFileSync(filePath, { encoding: 'utf8' })
      .split(/\r?\n/)
      .map((line) => line.trimStart())
      .join('\n');

    const parsed = dotenv.parse(normalized);
    Object.entries(parsed).forEach(([key, value]) => {
      if (process.env[key] === undefined) {
        process.env[key] = value;
      }
    });
  });
};

