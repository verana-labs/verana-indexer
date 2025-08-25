FROM node:22-slim

# Working directory
WORKDIR /app

# Install dependencies
COPY package.json pnpm-lock.yaml ./
RUN pnpm install

    # Copy package files
    COPY pnpm-lock.yaml ./
    RUN pnpm install

# Rebuild the source code only when needed
FROM base AS builder
WORKDIR /app
COPY --from=deps /app/node_modules ./node_modules
COPY . .

# Build and cleanup
ENV NODE_ENV=production
RUN pnpm run build

# Production image, copy all the files and run the app
FROM node:22-slim AS runner
WORKDIR /app

    # Create non-root user
    RUN groupadd --system --gid 1001 nodejs || true
    RUN useradd --system --uid 1001 -g nodejs nodejs || true

# Install pnpm
RUN corepack enable && corepack prepare pnpm@10.14.0 --activate

    # Copy built application
    COPY --from=builder --chown=nodejs:nodejs /app/dist ./dist
COPY --from=builder --chown=nodejs:nodejs /app/pnpm-lock.yaml ./  # ✅ Add this line
    
    # Install only production dependencies
RUN pnpm install --prod --frozen-lockfile --ignore-scripts

# Change to non-root user
USER nodejs

# Expose port (optional)
EXPOSE 3000

# Start the application (no healthcheck to avoid false negatives)
CMD ["node", "--es-module-specifier-resolution=node", "./node_modules/moleculer/bin/moleculer-runner.mjs", "--env", "--config", "./dist/src/moleculer.config.js"]
