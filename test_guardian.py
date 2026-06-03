#!/usr/bin/env python3
"""
test_guardian.py — Script de evaluación para "The Guardian"

Envía simultáneamente:
  1. Una ráfaga de 500 peticiones concurrentes a /ingest
  2. Consultas en paralelo a /health

Al finalizar, consulta /counter para leer el valor exacto del SharedArrayBuffer.
"""

import asyncio
import aiohttp
import time
import sys

BASE_URL  = "http://localhost:8080"
INGEST_N  = 500
HEALTH_N  = 20
TIMEOUT_S = 60

GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

def ok(msg):   print(f"{GREEN}  {msg}{RESET}")
def warn(msg): print(f"{YELLOW}  {msg}{RESET}")
def fail(msg): print(f"{RED}   {msg}{RESET}")
def info(msg): print(f"{CYAN}   {msg}{RESET}")


async def send_ingest(session, sem, req_id):
    url = f"{BASE_URL}/ingest?id={req_id}"
    async with sem:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=TIMEOUT_S)) as resp:
                data = await resp.json()
                return {"ok": True, "data": data}
        except Exception as e:
            return {"ok": False, "error": str(e)}


async def send_health(session, index):
    url = f"{BASE_URL}/health"
    t0  = time.perf_counter()
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
            data    = await resp.json()
            latency = (time.perf_counter() - t0) * 1000
            return {"ok": True, "latency_ms": latency, "data": data}
    except Exception as e:
        return {"ok": False, "error": str(e)}


async def read_counter(session):
    """Lee el valor final del SharedArrayBuffer desde /counter."""
    try:
        async with session.get(f"{BASE_URL}/counter", timeout=aiohttp.ClientTimeout(total=5)) as resp:
            data = await resp.json()
            return data.get("total", None)
    except Exception as e:
        return None


async def main():

    # Semáforo: controla cuántas peticiones van simultáneamente
    sem = asyncio.Semaphore(100)

    connector = aiohttp.TCPConnector(limit=0)
    async with aiohttp.ClientSession(connector=connector) as session:

        # Verificación previa
        info("Verificando que el servidor esté activo...")
        try:
            async with session.get(f"{BASE_URL}/health", timeout=aiohttp.ClientTimeout(total=5)) as r:
                pre = await r.json()
                ok(f"Servidor activo. PID={pre.get('pid')} Status={pre.get('status')}")
        except Exception as e:
            fail(f"No se puede conectar al servidor: {e}")
            fail("Asegurate de haber ejecutado:  node index.js")
            sys.exit(1)

        print()
        info(f"Lanzando {INGEST_N} peticiones a /ingest y {HEALTH_N} a /health en paralelo...\n")
        t_start = time.perf_counter()

        ingest_tasks = [send_ingest(session, sem, i + 1) for i in range(INGEST_N)]
        health_tasks = [send_health(session, i) for i in range(HEALTH_N)]

        # Esperar a que TODAS las peticiones terminen antes de leer el contador
        all_results = await asyncio.gather(*ingest_tasks, *health_tasks)

        t_total = time.perf_counter() - t_start

        ingest_results = all_results[:INGEST_N]
        health_results = all_results[INGEST_N:]

        # Pequeña pausa para que el Worker Thread termine de escribir el último Atomics.add
        await asyncio.sleep(0.3)

        # Leer el valor REAL del SharedArrayBuffer desde el servidor
        final_counter = await read_counter(session)

        # ── Análisis de /ingest ────────────────────────────────────────────────
        print(f"{BOLD}[1] Resultados de /ingest{RESET}")
        ingest_ok  = [r for r in ingest_results if r["ok"]]
        ingest_err = [r for r in ingest_results if not r["ok"]]

        ok(f"Peticiones exitosas:  {len(ingest_ok)} / {INGEST_N}")
        if ingest_err:
            warn(f"Peticiones fallidas:  {len(ingest_err)}")
            for e in ingest_err[:3]:
                print(f"     Error: {e['error']}")

        # if final_counter is not None:
        #     if final_counter == INGEST_N:
        #         ok(f"Contador final en SharedArrayBuffer: {final_counter}  (sin drift)")
        #     else:
        #         fail(f"Contador final en SharedArrayBuffer: {final_counter} (esperado {INGEST_N})")
        # else:
        #     warn("No se pudo leer el contador desde /counter.")

        # ── Análisis de /health ────────────────────────────────────────────────
        print(f"\n{BOLD}[2] Resultados de /health (latencia){RESET}")
        health_ok  = [r for r in health_results if r["ok"]]

        ok(f"Peticiones exitosas: {len(health_ok)} / {HEALTH_N}")

        if health_ok:
            latencies = [r["latency_ms"] for r in health_ok]
            avg_lat = sum(latencies) / len(latencies)
            max_lat = max(latencies)
            min_lat = min(latencies)

            print(f"     Latencia promedio : {avg_lat:.2f} ms")
            print(f"     Latencia mínima   : {min_lat:.2f} ms")
            print(f"     Latencia máxima   : {max_lat:.2f} ms")

            LATENCY_THRESHOLD_MS = 500
            if avg_lat < LATENCY_THRESHOLD_MS:
                ok(f"Latencia promedio ({avg_lat:.1f} ms) < umbral ({LATENCY_THRESHOLD_MS} ms) ")
            else:
                warn(f"Latencia promedio ({avg_lat:.1f} ms) supera el umbral.")

        # ── Resumen final ──────────────────────────────────────────────────────
        print(f"\n{BOLD}[3] Resumen{RESET}")
        print(f"     Tiempo total de la prueba : {t_total:.2f} s")
        print(f"     Throughput /ingest        : {INGEST_N / t_total:.1f} req/s")

        all_ok = (
            len(ingest_ok) == INGEST_N and
            final_counter == INGEST_N and
            len(health_ok) == HEALTH_N
        )
        # print()
        # if all_ok:
        #     print(f"{BOLD}{GREEN}   PRUEBA APROBADA — Todos los criterios cumplidos.{RESET}")
        # else:
        #     print(f"{BOLD}{RED}   PRUEBA FALLIDA — Revisar los ítems marcados con ✘.{RESET}")

        # print(f"\n{BOLD}{'='*60}{RESET}\n")


if __name__ == "__main__":
    asyncio.run(main())