import { readFileSync } from 'fs';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';
import { execSync } from 'child_process';

let cachedVersion: string | null = null;

export function getIndexerVersion(): string {
  if (cachedVersion) {
    return cachedVersion;
  }

  try {
    try {
      const gitTag = execSync('git describe --tags --abbrev=0', { 
        encoding: 'utf-8',
        stdio: ['pipe', 'pipe', 'pipe']
      }).trim();
      
      if (gitTag) {
        cachedVersion = gitTag.startsWith('v') ? gitTag : `v${gitTag}`;
        console.log(`[Version] Using Git tag: ${cachedVersion}`);
        return cachedVersion;
      }
    } catch (gitError) {
      console.log('[Version] Git tag not available, falling back to package.json');
    }

    const __filename = fileURLToPath(import.meta.url);
    const __dirname = dirname(__filename);
    const packageJsonPath = join(__dirname, '../../../package.json');
    const packageJson = JSON.parse(readFileSync(packageJsonPath, 'utf-8'));
    
    let version = packageJson.version;
    
    const buildNumber = process.env.GITHUB_RUN_NUMBER || 
                       process.env.BUILD_NUMBER || 
                       process.env.CI_PIPELINE_IID;
    
    if (buildNumber && !version.includes('-')) {
      version = `${version}-dev.${buildNumber}`;
    }
    
    const formattedVersion = version.startsWith('v') ? version : `v${version}`;
    cachedVersion = formattedVersion;
    console.log(`[Version] Using package.json version: ${cachedVersion}`);
    return formattedVersion;
  } catch (error) {
    console.error('[Version] Failed to read version:', error);
    cachedVersion = 'v1.0.0';
    return cachedVersion;
  }
}

export function clearVersionCache(): void {
  cachedVersion = null;
}

