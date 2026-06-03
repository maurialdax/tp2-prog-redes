/**
 * processor.js — Worker Thread de cómputo
 *
 * Parte 3: Recibe tareas del Worker principal vía postMessage(),
 *          realiza un cálculo "pesado" simulado, incrementa el contador
 *          global en el SharedArrayBuffer usando Atomics.add() (exclusión
 *          mutua por hardware, sin condiciones de carrera) y devuelve el
 *          resultado al Worker principal.
 */

'use strict';

const { parentPort, workerData } = require('worker_threads');

// Referencia al buffer compartido con el Worker del clúster
const sharedCounter = new Int32Array(workerData.sharedBuffer);

/**
 * Cálculo "pesado" simulado: suma de cuadrados hasta N.
 * Representa una operación CPU-bound que NO debe bloquear el Event Loop principal.
 * @param {number} n
 * @returns {number}
 */
function heavyComputation(n) {
  let sum = 0;
  // Limitamos las iteraciones para que no sea infinito, pero sí notable
  const limit = Math.abs(n) % 100_000 + 1;
  for (let i = 1; i <= limit; i++) {
    sum += i * i;
  }
  return sum;
}

// Escuchar tareas enviadas desde el Worker principal
parentPort.on('message', ({ reqId, id }) => {
  try {
    // 1. Realizar el cálculo pesado en este hilo (sin bloquear el Event Loop del Worker)
    const result = heavyComputation(id);

    // 2. Incrementar el contador atómicamente — sin condiciones de carrera
    //    Atomics.add() devuelve el valor ANTERIOR al incremento, por eso sumamos 1
    const prevValue = Atomics.add(sharedCounter, 0, 1);
    const totalIngested = prevValue + 1;

    // 3. Devolver el resultado al Worker principal (con el contador ya confirmado)
    parentPort.postMessage({ reqId, result, totalIngested });
  } catch (err) {
    // En caso de error, devolvemos null para que el Worker principal lo maneje
    parentPort.postMessage({ reqId, result: null, error: err.message });
  }
});