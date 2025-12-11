#!/usr/bin/env node

/**
 * Health Check Script for Docker Container
 * Checks if the application is responding on the health endpoint
 */

const http = require('http');

const options = {
  host: 'localhost',
  port: process.env.PROMETHEUS_PORT || 9090,
  path: '/health',
  timeout: 2000,
  method: 'GET'
};

const request = http.request(options, (res) => {
  if (res.statusCode === 200) {
    process.exit(0);
  } else {
    console.error(`Health check failed with status: ${res.statusCode}`);
    process.exit(1);
  }
});

request.on('error', (err) => {
  console.error('Health check error:', err.message);
  process.exit(1);
});

request.on('timeout', () => {
  console.error('Health check timeout');
  request.destroy();
  process.exit(1);
});

request.end();
