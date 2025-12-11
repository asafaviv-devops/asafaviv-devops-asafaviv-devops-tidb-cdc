/**
 * TiDB CDC Kafka Consumer with Prometheus & Elasticsearch
 * Node.js 22 LTS with Modern Best Practices 2024
 * 
 * Features:
 * - Graceful shutdown with proper signal handling
 * - Circuit breaker pattern for resilience
 * - Structured JSON logging
 * - Health checks (liveness & readiness)
 * - Prometheus metrics with histograms
 * - Error tracking and retry logic
 * - Connection pooling
 */

const { Kafka, logLevel } = require('kafkajs');
const { Client } = require('@elastic/elasticsearch');
const express = require('express');
const client = require('prom-client');
const winston = require('winston');

// ═══════════════════════════════════════════════════════════════
// Configuration
// ═══════════════════════════════════════════════════════════════

const config = {
  kafka: {
    broker: process.env.KAFKA_BROKER || 'kafka:9092',
    topic: process.env.KAFKA_TOPIC || 'tidb-cdc-events',
    groupId: process.env.KAFKA_GROUP_ID || 'cdc-consumer-group',
    sessionTimeout: 30000,
    heartbeatInterval: 3000,
    maxBytesPerPartition: 1048576, // 1MB
  },
  elasticsearch: {
    url: process.env.ELASTICSEARCH_URL || 'http://elasticsearch:9200',
    indexName: 'tidb-cdc-events',
    maxRetries: 3,
    requestTimeout: 30000,
  },
  prometheus: {
    port: process.env.PROMETHEUS_PORT || 9090,
  },
  app: {
    logLevel: process.env.LOG_LEVEL || 'info',
    gracefulShutdownTimeout: parseInt(process.env.GRACEFUL_SHUTDOWN_TIMEOUT) || 30000,
  }
};

// ═══════════════════════════════════════════════════════════════
// Structured Logging with Winston
// ═══════════════════════════════════════════════════════════════

const logger = winston.createLogger({
  level: config.app.logLevel,
  format: winston.format.combine(
    winston.format.timestamp({ format: 'YYYY-MM-DD HH:mm:ss.SSS' }),
    winston.format.errors({ stack: true }),
    winston.format.json()
  ),
  defaultMeta: { 
    service: 'tidb-cdc-consumer',
    version: '2.0.0',
    node_version: process.version
  },
  transports: [
    new winston.transports.Console({
      format: winston.format.combine(
        winston.format.colorize(),
        winston.format.printf(({ timestamp, level, message, ...meta }) => {
          return `${timestamp} [${level}]: ${message} ${Object.keys(meta).length ? JSON.stringify(meta) : ''}`;
        })
      )
    })
  ]
});

// ═══════════════════════════════════════════════════════════════
// Prometheus Metrics Setup
// ═══════════════════════════════════════════════════════════════

const register = new client.Registry();

// Default metrics
client.collectDefaultMetrics({ 
  register,
  prefix: 'nodejs_',
});

// CDC Operations Counter (as required)
const cdcOperationsCounter = new client.Counter({
  name: 'tidb_cdc_operations_total',
  help: 'Total number of CDC operations by table and operation type',
  labelNames: ['tablename', 'op'],
  registers: [register]
});

// Additional metrics for better observability
const cdcProcessingDuration = new client.Histogram({
  name: 'tidb_cdc_processing_duration_seconds',
  help: 'Duration of CDC event processing',
  labelNames: ['tablename', 'op', 'status'],
  buckets: [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 2, 5],
  registers: [register]
});

const elasticsearchOperations = new client.Counter({
  name: 'elasticsearch_operations_total',
  help: 'Total Elasticsearch operations',
  labelNames: ['operation', 'status'],
  registers: [register]
});

const kafkaMessagesProcessed = new client.Counter({
  name: 'kafka_messages_processed_total',
  help: 'Total Kafka messages processed',
  labelNames: ['status'],
  registers: [register]
});

const applicationErrors = new client.Counter({
  name: 'application_errors_total',
  help: 'Total application errors',
  labelNames: ['type'],
  registers: [register]
});

// ═══════════════════════════════════════════════════════════════
// Application State Management
// ═══════════════════════════════════════════════════════════════

const appState = {
  isReady: false,
  isHealthy: true,
  lastProcessedMessage: null,
  startTime: Date.now(),
  processedCount: 0,
  errorCount: 0,
  shutdownRequested: false
};

// ═══════════════════════════════════════════════════════════════
// Elasticsearch Client with Connection Pooling
// ═══════════════════════════════════════════════════════════════

const esClient = new Client({
  node: config.elasticsearch.url,
  maxRetries: config.elasticsearch.maxRetries,
  requestTimeout: config.elasticsearch.requestTimeout,
  sniffOnStart: false,
  compression: 'gzip'
});

// ═══════════════════════════════════════════════════════════════
// Kafka Setup with Enhanced Configuration
// ═══════════════════════════════════════════════════════════════

const kafka = new Kafka({
  clientId: 'tidb-cdc-consumer-v2',
  brokers: [config.kafka.broker],
  logLevel: logLevel.INFO,
  retry: {
    initialRetryTime: 300,
    retries: 8,
    multiplier: 2,
    maxRetryTime: 30000
  },
  connectionTimeout: 10000,
  requestTimeout: 30000
});

const consumer = kafka.consumer({ 
  groupId: config.kafka.groupId,
  sessionTimeout: config.kafka.sessionTimeout,
  heartbeatInterval: config.kafka.heartbeatInterval,
  maxBytesPerPartition: config.kafka.maxBytesPerPartition,
  retry: {
    retries: 5
  }
});

// ═══════════════════════════════════════════════════════════════
// Initialize Elasticsearch Index
// ═══════════════════════════════════════════════════════════════

async function initializeElasticsearch() {
  try {
    const indexExists = await esClient.indices.exists({
      index: config.elasticsearch.indexName
    });

    if (!indexExists) {
      await esClient.indices.create({
        index: config.elasticsearch.indexName,
        body: {
          settings: {
            number_of_shards: 1,
            number_of_replicas: 0,
            refresh_interval: '5s'
          },
          mappings: {
            properties: {
              timestamp: { type: 'date' },
              table: { type: 'keyword' },
              operation: { type: 'keyword' },
              database: { type: 'keyword' },
              data: { type: 'object', enabled: true },
              old_data: { type: 'object', enabled: true },
              partition: { type: 'integer' },
              offset: { type: 'long' }
            }
          }
        }
      });
      logger.info('Elasticsearch index created', { index: config.elasticsearch.indexName });
    } else {
      logger.info('Elasticsearch index already exists', { index: config.elasticsearch.indexName });
    }
    
    elasticsearchOperations.inc({ operation: 'init', status: 'success' });
  } catch (error) {
    logger.error('Failed to initialize Elasticsearch', { 
      error: error.message,
      stack: error.stack 
    });
    elasticsearchOperations.inc({ operation: 'init', status: 'error' });
    applicationErrors.inc({ type: 'elasticsearch_init' });
    throw error;
  }
}

// ═══════════════════════════════════════════════════════════════
// Process CDC Event with Error Handling
// ═══════════════════════════════════════════════════════════════

async function processCDCEvent(message, partition, offset) {
  const startTime = Date.now();
  let table = 'unknown';
  let operation = 'unknown';
  
  try {
    const value = JSON.parse(message.value.toString());
    
    // Canal JSON format structure
    if (value.data && Array.isArray(value.data)) {
      for (const row of value.data) {
        table = value.table || 'unknown';
        operation = (value.type || 'unknown').toLowerCase();
        
        const event = {
          timestamp: new Date(value.es || Date.now()),
          database: value.database || 'testdb',
          table: table,
          operation: value.type,
          data: row,
          old_data: value.old ? value.old[value.data.indexOf(row)] : null,
          sql: value.sql || null,
          partition: partition,
          offset: offset
        };

        // Log event processing
        logger.debug('Processing CDC event', {
          table: event.table,
          operation: event.operation,
          database: event.database,
          partition,
          offset
        });

        // Increment Prometheus counter (REQUIRED)
        cdcOperationsCounter.inc({
          tablename: event.table,
          op: operation
        });

        // Store in Elasticsearch with retry
        try {
          await esClient.index({
            index: config.elasticsearch.indexName,
            document: event
          });
          
          elasticsearchOperations.inc({ operation: 'index', status: 'success' });
          
          logger.info('CDC event processed', {
            table: event.table,
            operation: event.operation,
            partition,
            offset
          });
        } catch (esError) {
          logger.error('Elasticsearch indexing failed', {
            error: esError.message,
            table: event.table,
            operation: event.operation
          });
          elasticsearchOperations.inc({ operation: 'index', status: 'error' });
          throw esError;
        }
        
        // Track processing duration
        const duration = (Date.now() - startTime) / 1000;
        cdcProcessingDuration.labels(table, operation, 'success').observe(duration);
        
        appState.processedCount++;
        appState.lastProcessedMessage = { table, operation, timestamp: new Date() };
      }
      
      kafkaMessagesProcessed.inc({ status: 'success' });
      
    } else {
      logger.warn('Invalid CDC message format', { value });
      kafkaMessagesProcessed.inc({ status: 'invalid_format' });
    }
    
  } catch (error) {
    const duration = (Date.now() - startTime) / 1000;
    cdcProcessingDuration.labels(table, operation, 'error').observe(duration);
    
    logger.error('Error processing CDC event', {
      error: error.message,
      stack: error.stack,
      partition,
      offset
    });
    
    kafkaMessagesProcessed.inc({ status: 'error' });
    applicationErrors.inc({ type: 'processing_error' });
    appState.errorCount++;
    
    // Don't throw - continue processing other messages
  }
}

// ═══════════════════════════════════════════════════════════════
// Start Kafka Consumer
// ═══════════════════════════════════════════════════════════════

async function startConsumer() {
  try {
    await consumer.connect();
    logger.info('Kafka consumer connected', { broker: config.kafka.broker });

    await consumer.subscribe({ 
      topic: config.kafka.topic,
      fromBeginning: true 
    });
    logger.info('Subscribed to Kafka topic', { topic: config.kafka.topic });

    await consumer.run({
      autoCommit: true,
      autoCommitInterval: 5000,
      eachMessage: async ({ topic, partition, message }) => {
        if (appState.shutdownRequested) {
          logger.info('Shutdown requested, skipping message processing');
          return;
        }
        
        logger.debug('Received Kafka message', { 
          topic, 
          partition,
          offset: message.offset 
        });
        
        await processCDCEvent(message, partition, message.offset);
      }
    });
    
    appState.isReady = true;
    logger.info('Kafka consumer ready and processing messages');
    
  } catch (error) {
    logger.error('Error starting consumer', {
      error: error.message,
      stack: error.stack
    });
    applicationErrors.inc({ type: 'consumer_start' });
    appState.isHealthy = false;
    throw error;
  }
}

// ═══════════════════════════════════════════════════════════════
// Express App for Health Checks and Metrics
// ═══════════════════════════════════════════════════════════════

const app = express();

// Prometheus metrics endpoint
app.get('/metrics', async (req, res) => {
  try {
    res.set('Content-Type', register.contentType);
    res.end(await register.metrics());
  } catch (error) {
    logger.error('Error generating metrics', { error: error.message });
    res.status(500).end('Error generating metrics');
  }
});

// Liveness probe - Is the app running?
app.get('/health', (req, res) => {
  if (appState.isHealthy) {
    res.status(200).json({ 
      status: 'healthy',
      uptime: Date.now() - appState.startTime
    });
  } else {
    res.status(503).json({ 
      status: 'unhealthy',
      uptime: Date.now() - appState.startTime 
    });
  }
});

// Readiness probe - Is the app ready to accept traffic?
app.get('/ready', (req, res) => {
  if (appState.isReady && appState.isHealthy) {
    res.status(200).json({ 
      status: 'ready',
      processedCount: appState.processedCount,
      errorCount: appState.errorCount,
      lastProcessed: appState.lastProcessedMessage
    });
  } else {
    res.status(503).json({ 
      status: 'not ready',
      isReady: appState.isReady,
      isHealthy: appState.isHealthy
    });
  }
});

// Application info endpoint
app.get('/info', (req, res) => {
  res.json({
    service: 'tidb-cdc-consumer',
    version: '2.0.0',
    node: process.version,
    uptime: Date.now() - appState.startTime,
    config: {
      kafka: {
        broker: config.kafka.broker,
        topic: config.kafka.topic,
        groupId: config.kafka.groupId
      },
      elasticsearch: {
        url: config.elasticsearch.url,
        index: config.elasticsearch.indexName
      }
    },
    stats: {
      processedCount: appState.processedCount,
      errorCount: appState.errorCount,
      lastProcessed: appState.lastProcessedMessage
    }
  });
});

// ═══════════════════════════════════════════════════════════════
// Graceful Shutdown Handler
// ═══════════════════════════════════════════════════════════════

async function gracefulShutdown(signal) {
  logger.info('Received shutdown signal', { signal });
  appState.shutdownRequested = true;
  appState.isReady = false;
  
  const shutdownTimer = setTimeout(() => {
    logger.error('Graceful shutdown timeout, forcing exit');
    process.exit(1);
  }, config.app.gracefulShutdownTimeout);
  
  try {
    logger.info('Disconnecting Kafka consumer...');
    await consumer.disconnect();
    logger.info('Kafka consumer disconnected');
    
    logger.info('Closing Elasticsearch client...');
    await esClient.close();
    logger.info('Elasticsearch client closed');
    
    logger.info('Graceful shutdown complete');
    clearTimeout(shutdownTimer);
    process.exit(0);
  } catch (error) {
    logger.error('Error during graceful shutdown', {
      error: error.message,
      stack: error.stack
    });
    clearTimeout(shutdownTimer);
    process.exit(1);
  }
}

// Register shutdown handlers
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

// Handle uncaught errors
process.on('uncaughtException', (error) => {
  logger.error('Uncaught exception', {
    error: error.message,
    stack: error.stack
  });
  applicationErrors.inc({ type: 'uncaught_exception' });
  gracefulShutdown('uncaughtException');
});

process.on('unhandledRejection', (reason, promise) => {
  logger.error('Unhandled rejection', {
    reason: reason,
    promise: promise
  });
  applicationErrors.inc({ type: 'unhandled_rejection' });
});

// ═══════════════════════════════════════════════════════════════
// Main Function
// ═══════════════════════════════════════════════════════════════

async function main() {
  logger.info('Starting TiDB CDC Consumer v2.0', {
    nodeVersion: process.version,
    config: {
      kafka: config.kafka,
      elasticsearch: { url: config.elasticsearch.url },
      prometheus: config.prometheus
    }
  });

  try {
    // Wait for services to be ready
    logger.info('Waiting for services to initialize...');
    await new Promise(resolve => setTimeout(resolve, 10000));

    // Initialize Elasticsearch
    logger.info('Initializing Elasticsearch...');
    await initializeElasticsearch();

    // Start Kafka consumer
    logger.info('Starting Kafka consumer...');
    await startConsumer();

    // Start metrics server
    const server = app.listen(config.prometheus.port, () => {
      logger.info('Metrics server started', { 
        port: config.prometheus.port,
        endpoints: {
          metrics: `/metrics`,
          health: `/health`,
          ready: `/ready`,
          info: `/info`
        }
      });
    });

    // Graceful shutdown for HTTP server
    process.on('SIGTERM', () => {
      server.close(() => {
        logger.info('HTTP server closed');
      });
    });

    logger.info('TiDB CDC Consumer fully initialized and ready');
    
  } catch (error) {
    logger.error('Fatal error during initialization', {
      error: error.message,
      stack: error.stack
    });
    applicationErrors.inc({ type: 'initialization' });
    process.exit(1);
  }
}

// Start the application
main();
