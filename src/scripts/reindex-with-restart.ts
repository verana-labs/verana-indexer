import { spawn } from 'child_process';
import * as path from 'path';
import * as fs from 'fs';
import { fileURLToPath } from 'url';

const MAX_RESTARTS = 10;
const RESTART_DELAY = 5000;
const CHECKPOINT_FILE = path.join(process.cwd(), '.reindex-checkpoint.json');

interface ReindexCheckpoint {
    lastCompletedStep?: string;
    completedSteps: string[];
    attemptCount: number;
    lastError?: string;
    timestamp: string;
}

function log(message: string): void {
    const timestamp = new Date().toISOString();
    console.log(`[${timestamp}] ${message}`);
}

function loadCheckpoint(): ReindexCheckpoint | null {
    try {
        if (fs.existsSync(CHECKPOINT_FILE)) {
            const content = fs.readFileSync(CHECKPOINT_FILE, 'utf-8');
            return JSON.parse(content);
        }
    } catch (error) {
        log(`⚠️  Could not load checkpoint: ${error}`);
    }
    return null;
}

function saveCheckpoint(checkpoint: ReindexCheckpoint): void {
    try {
        fs.writeFileSync(CHECKPOINT_FILE, JSON.stringify(checkpoint, null, 2));
    } catch (error) {
        log(`⚠️  Could not save checkpoint: ${error}`);
    }
}

function clearCheckpoint(): void {
    try {
        if (fs.existsSync(CHECKPOINT_FILE)) {
            fs.unlinkSync(CHECKPOINT_FILE);
            log('✅ Checkpoint file cleared');
        }
    } catch (error) {
        log(`⚠️  Could not clear checkpoint: ${error}`);
    }
}

function runReindex(checkpoint: ReindexCheckpoint | null): Promise<number> {
    return new Promise((resolve, reject) => {
        const attemptCount = (checkpoint?.attemptCount || 0) + 1;
        log(`Starting reindex process (attempt ${attemptCount}/${MAX_RESTARTS + 1})...`);

        if (checkpoint && checkpoint.completedSteps.length > 0) {
            log(`📌 Resuming from checkpoint. Completed steps: ${checkpoint.completedSteps.join(', ')}`);
            log(`📌 Last completed step: ${checkpoint.lastCompletedStep || 'none'}`);
        }

        const currentFile = fileURLToPath(import.meta.url);
        const currentDir = path.dirname(currentFile);
        const isProduction = process.env.NODE_ENV === 'production' ||
            (process.env.NODE_OPTIONS && !process.env.NODE_OPTIONS.includes('tsx')) ||
            currentFile.endsWith('.js');
        const scriptPath = isProduction
            ? path.join(currentDir, 'reindex-modules.js')
            : path.join(currentDir, 'reindex-modules.ts');

        const baseOptions = (process.env.NODE_OPTIONS || '').trim();
        const baseParts = baseOptions ? baseOptions.split(/\s+/).filter(Boolean) : [];

        const hasTsx = baseParts.some(opt => opt.includes('tsx'));
        const hasMemoryLimit = baseParts.some(opt => opt.includes('--max-old-space-size'));
        const hasExposeGc = baseParts.some(opt => opt.includes('--expose-gc'));

        const nodeOptionsParts: string[] = [];

        if (!isProduction && !hasTsx) {
            nodeOptionsParts.push('--import=tsx');
        }

        if (!hasMemoryLimit) {
            nodeOptionsParts.push('--max-old-space-size=4096');
        }

        // Match pnpm start's semi-space size — critical for GC during reindex.
        // Without this, V8's default 16MB semi-space promotes short-lived objects
        // (query results, model instances, decoded txs) to old generation too fast,
        // causing monotonic heap growth that leads to OOM.
        const hasSemiSpace = baseParts.some(opt => opt.includes('--max-semi-space-size'));
        if (!hasSemiSpace) {
            nodeOptionsParts.push('--max-semi-space-size=64');
        }

        if (!hasExposeGc) {
            nodeOptionsParts.push('--expose-gc');
        }

        baseParts.forEach(opt => {
            const isMemoryOpt = opt.includes('--max-old-space-size');
            const isExposeGcOpt = opt.includes('--expose-gc');
            const isTsxOpt = opt.includes('tsx');

            if (isTsxOpt && !isProduction) {
                nodeOptionsParts.push(opt);
            } else if (!isMemoryOpt && !isExposeGcOpt && !isTsxOpt) {
                nodeOptionsParts.push(opt);
            }
        });

        const nodeOptions = nodeOptionsParts.join(' ');

        log(`🔧 Node options: ${nodeOptions}`);
        log(`📁 Script path: ${scriptPath}`);

        const child = spawn('node', [
            ...nodeOptionsParts,
            scriptPath
        ], {
            stdio: 'inherit',
            shell: true,
            env: {
                ...process.env,
                NODE_OPTIONS: nodeOptions,
                REINDEX_CHECKPOINT: checkpoint ? JSON.stringify(checkpoint) : undefined
            }
        });

        let hasExited = false;

        child.on('exit', (code, signal) => {
            if (hasExited) return;
            hasExited = true;

            if (code === 0) {
                log('✅ Reindex completed successfully!');
                clearCheckpoint();
                resolve(0);
            } else if (signal === 'SIGTERM' || signal === 'SIGINT') {
                log('⚠️  Reindex interrupted by user');
                const newCheckpoint: ReindexCheckpoint = {
                    ...checkpoint,
                    attemptCount,
                    completedSteps: [],
                    timestamp: new Date().toISOString()
                };
                saveCheckpoint(newCheckpoint);
                reject(new Error('Interrupted'));
            } else {
                const errorType = code === 134 ? 'JavaScript heap out of memory' :
                    code === null ? 'Unknown error' :
                        `Exit code ${code}`;
                log(`❌ Reindex failed: ${errorType}${signal ? ` (signal: ${signal})` : ''}`);

                if (code === 134) {
                    log(`💡 Memory limit reached. The process will restart with checkpoint recovery.`);
                }

                const newCheckpoint: ReindexCheckpoint = {
                    ...checkpoint || { completedSteps: [] },
                    attemptCount,
                    lastError: `${errorType}${signal ? `, signal: ${signal}` : ''}`,
                    timestamp: new Date().toISOString()
                };
                saveCheckpoint(newCheckpoint);

                if (attemptCount <= MAX_RESTARTS) {
                    log(`🔄 Will restart in ${RESTART_DELAY / 1000} seconds... (attempt ${attemptCount + 1}/${MAX_RESTARTS + 1})`);
                    setTimeout(() => {
                        runReindex(newCheckpoint)
                            .then(resolve)
                            .catch(reject);
                    }, RESTART_DELAY);
                } else {
                    log(`❌ Maximum restart attempts (${MAX_RESTARTS}) reached. Giving up.`);
                    log(`📌 Checkpoint saved at: ${CHECKPOINT_FILE}`);
                    log(`   You can manually resume by running: pnpm reindex:dev`);
                    reject(new Error(`Failed after ${MAX_RESTARTS + 1} attempts`));
                }
            }
        });

        child.on('error', (error) => {
            if (hasExited) return;
            hasExited = true;
            log(`❌ Failed to start reindex process: ${error.message}`);
            const newCheckpoint: ReindexCheckpoint = {
                ...checkpoint || { completedSteps: [] },
                attemptCount,
                lastError: error.message,
                timestamp: new Date().toISOString()
            };
            saveCheckpoint(newCheckpoint);
            reject(error);
        });

        const cleanup = () => {
            log('⚠️  Received termination signal, saving checkpoint...');
            const newCheckpoint: ReindexCheckpoint = {
                ...checkpoint || { completedSteps: [] },
                attemptCount,
                timestamp: new Date().toISOString()
            };
            saveCheckpoint(newCheckpoint);
            child.kill('SIGTERM');
        };

        process.on('SIGINT', cleanup);
        process.on('SIGTERM', cleanup);
    });
}

const MAX_SERVICE_RESTARTS = 20;
const SERVICE_RESTART_DELAY = 5000;
const MEMORY_EXIT_CODES = new Set([3, 134]); // 3 = our graceful exit, 134 = OOM signal

function startServicesWithRestart(dockerCommand: string): Promise<void> {
    let restartCount = 0;

    const launch = (): Promise<void> => {
        return new Promise((resolve, reject) => {
            restartCount++;
            log(`🚀 Starting services (${dockerCommand})... ${restartCount > 1 ? `[restart #${restartCount - 1}]` : ''}`);

            const svcProcess = spawn('pnpm', ['run', dockerCommand], {
                stdio: 'inherit',
                shell: true
            });

            svcProcess.on('exit', (code, signal) => {
                const exitCode = code ?? (signal === 'SIGKILL' ? 137 : 1);

                if (exitCode === 0) {
                    resolve();
                    return;
                }

                if (MEMORY_EXIT_CODES.has(exitCode) && restartCount <= MAX_SERVICE_RESTARTS) {
                    log(`⚠️  Services exited with code ${exitCode} (memory). Restarting in ${SERVICE_RESTART_DELAY / 1000}s... (${restartCount}/${MAX_SERVICE_RESTARTS})`);
                    log(`   Checkpoints are preserved in the database — the fresh process will continue where it left off.`);
                    setTimeout(() => {
                        launch().then(resolve).catch(reject);
                    }, SERVICE_RESTART_DELAY);
                } else if (MEMORY_EXIT_CODES.has(exitCode)) {
                    log(`❌ Services exceeded max restarts (${MAX_SERVICE_RESTARTS}) due to memory. Exiting.`);
                    log(`   Run 'pnpm start' to continue from checkpoints.`);
                    process.exit(exitCode);
                } else {
                    log(`❌ Services exited with code ${exitCode}${signal ? ` (signal: ${signal})` : ''}`);
                    process.exit(exitCode);
                }
            });

            svcProcess.on('error', (error) => {
                log(`❌ Failed to start services: ${error.message}`);
                reject(error);
            });
        });
    };

    return launch();
}

async function main(): Promise<void> {
    try {
        const checkpoint = loadCheckpoint();

        if (checkpoint && checkpoint.attemptCount > 0) {
            log(`📌 Found existing checkpoint from ${checkpoint.timestamp}`);
            log(`   Attempt count: ${checkpoint.attemptCount}`);
            if (checkpoint.lastError) {
                log(`   Last error: ${checkpoint.lastError}`);
            }
        }

        const exitCode = await runReindex(checkpoint);

        if (exitCode === 0) {
            const isProduction = process.env.NODE_ENV === 'production';
            const dockerCommand = isProduction ? 'docker' : 'docker:dev';
            await startServicesWithRestart(dockerCommand);
        } else {
            process.exit(exitCode);
        }
    } catch (error: unknown) {
        const err = error as Error;
        log(`❌ Fatal error: ${err.message}`);
        process.exit(1);
    }
}

const isMainModule = (): boolean => {
    try {
        if (!process.argv[1]) {
            return false;
        }
        const fileUrl = import.meta.url;
        const scriptPath = path.resolve(process.argv[1]);
        let filePath = fileUrl.replace(/^file:\/\//, '');
        if (process.platform === 'win32' && filePath.startsWith('/')) {
            filePath = filePath.substring(1);
        }
        const resolvedFilePath = path.resolve(filePath);
        return resolvedFilePath === scriptPath;
    } catch {
        return true;
    }
};

if (isMainModule()) {
    main();
}

export { runReindex, loadCheckpoint, saveCheckpoint, clearCheckpoint };
