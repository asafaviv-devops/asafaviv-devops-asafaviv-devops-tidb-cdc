# Simple Dockerfile - No package-lock.json needed
FROM node:22-alpine

# Install wget for health checks
RUN apk add --no-cache wget

# Create app directory
WORKDIR /app

# Copy package.json and install
COPY package.json ./
RUN npm install --production --no-package-lock && \
    npm cache clean --force

# Copy application code
COPY index.js ./
COPY healthcheck.js ./

# Create non-root user
RUN addgroup -g 1001 -S nodejs && \
    adduser -S nodejs -u 1001 && \
    chown -R nodejs:nodejs /app

USER nodejs

# Expose ports
EXPOSE 3000 9090

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:9090/health || exit 1

ENV NODE_ENV=production

CMD ["node", "index.js"]
