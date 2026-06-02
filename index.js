/**
 * THE GUARDIAN — Micro-Orquestador de Ingesta y Monitoreo Reactivo
 * index.js — Proceso Master (Cluster)
 *
 * Parte 1: Levanta un cluster con la mitad de los núcleos disponibles.
 * Implementa Self-Healing: si un Worker muere, hace fork() inmediato.
 */

'use strict';

const cluster = require('cluster');
const os      = require('os');
const logger  = require('./logger');

// ── Cantidad de Workers: mitad de los núcleos lógicos (mínimo 1) ──────────────
const TOTAL_CORES   = os.cpus().length;
const WORKER_COUNT  = Math.max(1, Math.floor(TOTAL_CORES / 2));

// ── MASTER ─────────────────────────────────────────────────────────────────────
if (cluster.isPrimary) {
  logger.info(`[Master] PID ${process.pid} | Núcleos totales: ${TOTAL_CORES} | Workers a lanzar: ${WORKER_COUNT}`);

  // Lanzar Workers iniciales
  for (let i = 0; i < WORKER_COUNT; i++) {
    spawnWorker();
  }

  // Self-Healing: detectar muerte de un Worker y reemplazarlo inmediatamente
  cluster.on('exit', (worker, code, signal) => {
    const reason = signal || `código ${code}`;
    logger.warn(`[Master] Worker ${worker.process.pid} murió (${reason}). Iniciando reemplazo...`);
    spawnWorker();
  });

  cluster.on('online', (worker) => {
    logger.info(`[Master] Worker ${worker.process.pid} en línea.`);
  });

// ── WORKER ─────────────────────────────────────────────────────────────────────
} else {
  require('./worker');
}

// ── Helpers ───────────────────────────────────────────────────────────────────
function spawnWorker() {
  const w = cluster.fork();
  logger.info(`[Master] Fork realizado → PID esperado: ${w.process.pid}`);
  return w;
}
