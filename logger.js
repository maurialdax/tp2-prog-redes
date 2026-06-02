/**
 * logger.js — Módulo de logging centralizado
 *
 * Proporciona niveles de log con timestamp y color en consola.
 * Reemplaza console.log dispersos por una interfaz uniforme.
 */

'use strict';

const LEVELS = {
  info:  '\x1b[36m[INFO] \x1b[0m',   // Cyan
  warn:  '\x1b[33m[WARN] \x1b[0m',   // Amarillo
  error: '\x1b[31m[ERROR]\x1b[0m',   // Rojo
  debug: '\x1b[90m[DEBUG]\x1b[0m',   // Gris
};

function timestamp() {
  return new Date().toISOString();
}

function log(level, message) {
  const prefix = LEVELS[level] || LEVELS.info;
  process.stdout.write(`${timestamp()} ${prefix} ${message}\n`);
}

module.exports = {
  info:  (msg) => log('info',  msg),
  warn:  (msg) => log('warn',  msg),
  error: (msg) => log('error', msg),
  debug: (msg) => log('debug', msg),
};
