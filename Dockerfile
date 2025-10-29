FROM node:22-slim

# Working directory
WORKDIR /app

# Create non-root user
RUN groupadd --system --gid 1001 nodejs || true
RUN useradd --system --uid 1001 -g nodejs nodejs || true

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

# Remove TypeScript source code
RUN rm -rf ./src

RUN rm *.ts

RUN rm -rf ./migrations

RUN rm ./dist/migrations/*.ts

# Change ownership of the app directory
RUN chown -R nodejs:nodejs /app

# Prepare pnpm cache directory and fix permissions
RUN mkdir -p /home/nodejs/.cache/node/corepack/v1 && chown -R nodejs:nodejs /home/nodejs

# Change to non-root user
USER nodejs

# Expose port
EXPOSE 3001

# Run migrations, then start the app
CMD ["sh", "-c", "pnpm run db:migrate:latest && pnpm start"]
