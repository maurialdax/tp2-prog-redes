/**
 * worker.js — Proceso Worker del Clúster
 */

'use strict';

const http          = require('http');
const { Worker }    = require('worker_threads');
const path          = require('path');
const logger        = require('./logger');

const PORT = 8080;

// Memoria compartida: 4 bytes (Int32) para el contador global
const sharedBuffer  = new SharedArrayBuffer(4);
const sharedCounter = new Int32Array(sharedBuffer);

// Worker Thread fijo que realizará los cálculos pesados
const processorThread = new Worker(
  path.join(__dirname, 'processor.js'),
  { workerData: { sharedBuffer } }
);

// Cola de callbacks pendientes (id de petición → resolve de Promise)
const pendingRequests = new Map();
let   requestSeq      = 0;

processorThread.on('message', ({ reqId, result, totalIngested }) => {
  const resolve = pendingRequests.get(reqId);
  if (resolve) {
    pendingRequests.delete(reqId);
    resolve({ result, totalIngested });
  }
});

processorThread.on('error', (err) => {
  logger.error(`[Worker ${process.pid}] Error en ProcessorThread: ${err.message}`);
});

// Servidor HTTP
const server = http.createServer((req, res) => {
  const url = new URL(req.url, `http://localhost`);

  // /health — responde inmediatamente sin tocar el hilo secundario
  if (url.pathname === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ status: 'ok', pid: process.pid }));
    return;
  }

  // /counter — lee el valor actual del SharedArrayBuffer (para verificación post-prueba)
  if (url.pathname === '/counter') {
    const total = Atomics.load(sharedCounter, 0);
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ total, pid: process.pid }));
    return;
  }

  // /ingest?id=<número> — delega el cálculo al Worker Thread
  if (url.pathname === '/ingest') {
    const id = parseInt(url.searchParams.get('id'), 10);

    if (isNaN(id)) {
      res.writeHead(400, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'El parámetro "id" debe ser un número entero.' }));
      return;
    }

    const reqId = requestSeq++;
    const result = new Promise((resolve) => pendingRequests.set(reqId, resolve));

    processorThread.postMessage({ reqId, id });

    result.then(({ result: computedResult, totalIngested }) => {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        status:        'ingested',
        pid:           process.pid,
        id,
        result:        computedResult,
        totalIngested,
      }));
    }).catch((err) => {
      logger.error(`[Worker ${process.pid}] Error procesando /ingest: ${err.message}`);
      res.writeHead(500, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'Error interno al procesar la ingesta.' }));
    });

    return;
  }

  // Ruta desconocida
  res.writeHead(404, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify({ error: 'Ruta no encontrada.' }));
});

server.listen(PORT, () => {
  logger.info(`[Worker ${process.pid}] Servidor HTTP escuchando en puerto ${PORT}`);
});

server.on('error', (err) => {
  logger.error(`[Worker ${process.pid}] Error en servidor HTTP: ${err.message}`);
  process.exit(1);
});