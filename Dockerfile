FROM node:22-slim

# Working directory
WORKDIR /app

# Install pnpm
RUN corepack enable && corepack prepare pnpm@10.14.0 --activate

# Copy package files first (for better caching)
COPY package.json ./

# Install dependencies without lock file (fallback)
RUN pnpm install

# Copy source code
COPY . .

RUN cp docker.env .env

# Build the application
RUN pnpm run build

# Create non-root user
RUN groupadd --system --gid 1001 nodejs || true
RUN useradd --system --uid 1001 -g nodejs nodejs || true

# Change ownership of the app directory
RUN chown -R nodejs:nodejs /app

# Change to non-root user
USER nodejs

# Expose port
EXPOSE 3000

# Start the application
CMD ["node", "--es-module-specifier-resolution=node", "./node_modules/moleculer/bin/moleculer-runner.mjs", "--env", "--config", "./dist/src/moleculer.config.js"]
