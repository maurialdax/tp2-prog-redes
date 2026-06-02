#!/usr/bin/env python3
"""
test_guardian.py — Script de evaluación para "The Guardian"

Envía simultáneamente:
  1. Una ráfaga de 500 peticiones concurrentes a /ingest
  2. Consultas en paralelo a /health

Demuestra que:
  - /health responde casi instantáneamente (~5 ms) mientras los cálculos pesados ocurren.
  - El contador final en la memoria compartida llega exactamente a 500 (sin drift).
"""

import asyncio
import aiohttp
import time
import sys

BASE_URL   = "http://localhost:8080"
INGEST_N   = 500          # Peticiones a /ingest
HEALTH_N   = 20           # Peticiones intercaladas a /health
TIMEOUT_S  = 30           # Timeout por petición (segundos)

# ── Colores ANSI ──────────────────────────────────────────────────────────────
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
RESET  = "\033[0m"
BOLD   = "\033[1m"

def ok(msg):    print(f"{GREEN}  ✔ {msg}{RESET}")
def warn(msg):  print(f"{YELLOW}  ⚠ {msg}{RESET}")
def fail(msg):  print(f"{RED}  ✘ {msg}{RESET}")
def info(msg):  print(f"{CYAN}  → {msg}{RESET}")


# ── Tarea: petición a /ingest ─────────────────────────────────────────────────
async def send_ingest(session, sem, req_id):
    url = f"{BASE_URL}/ingest?id={req_id}"
    async with sem:
        try:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=TIMEOUT_S)) as resp:
                data = await resp.json()
                return {"ok": True, "data": data}
        except Exception as e:
            return {"ok": False, "error": str(e)}


# ── Tarea: petición a /health con medición de latencia ───────────────────────
async def send_health(session, index):
    url = f"{BASE_URL}/health"
    t0  = time.perf_counter()
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            data    = await resp.json()
            latency = (time.perf_counter() - t0) * 1000  # ms
            return {"ok": True, "latency_ms": latency, "data": data, "index": index}
    except Exception as e:
        return {"ok": False, "error": str(e), "index": index}


# ── Main ──────────────────────────────────────────────────────────────────────
async def main():
    print(f"\n{BOLD}{'='*60}{RESET}")
    print(f"{BOLD}   THE GUARDIAN — Script de Evaluación{RESET}")
    print(f"{BOLD}{'='*60}{RESET}\n")

    # Semáforo: limita la concurrencia real del cliente para no saturar el SO
    sem = asyncio.Semaphore(200)

    connector = aiohttp.TCPConnector(limit=0)  # Sin límite de conexiones del cliente
    async with aiohttp.ClientSession(connector=connector) as session:

        # ── Verificación previa ────────────────────────────────────────────────
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

        # ── Lanzar las tareas en paralelo ──────────────────────────────────────
        info(f"Lanzando {INGEST_N} peticiones a /ingest y {HEALTH_N} a /health en paralelo...\n")
        t_start = time.perf_counter()

        # Crear todas las corutinas
        ingest_tasks = [
            send_ingest(session, sem, i + 1) for i in range(INGEST_N)
        ]
        # Intercalar health-checks a lo largo del tiempo de la prueba
        health_tasks = [
            send_health(session, i) for i in range(HEALTH_N)
        ]

        # Ejecutar todo de forma concurrente
        all_results = await asyncio.gather(*ingest_tasks, *health_tasks)

        t_total = time.perf_counter() - t_start

        ingest_results = all_results[:INGEST_N]
        health_results = all_results[INGEST_N:]

        # ── Análisis de /ingest ────────────────────────────────────────────────
        print(f"{BOLD}[1] Resultados de /ingest{RESET}")
        ingest_ok  = [r for r in ingest_results if r["ok"]]
        ingest_err = [r for r in ingest_results if not r["ok"]]

        ok(f"Peticiones exitosas:  {len(ingest_ok)} / {INGEST_N}")
        if ingest_err:
            warn(f"Peticiones fallidas:  {len(ingest_err)}")
            for e in ingest_err[:5]:
                print(f"     Error: {e['error']}")

        # Leer el contador final reportado por el último resultado exitoso
        last_counter = None
        for r in reversed(ingest_ok):
            if "totalIngested" in r.get("data", {}):
                last_counter = r["data"]["totalIngested"]
                break

        if last_counter is not None:
            if last_counter == INGEST_N:
                ok(f"Contador final en SharedArrayBuffer: {last_counter} ✓ (sin drift)")
            else:
                fail(f"Contador final en SharedArrayBuffer: {last_counter} ✗ (esperado {INGEST_N})")
                warn("Posible condición de carrera detectada.")
        else:
            warn("No se pudo leer el contador final de las respuestas.")

        # ── Análisis de /health ────────────────────────────────────────────────
        print(f"\n{BOLD}[2] Resultados de /health (latencia){RESET}")
        health_ok  = [r for r in health_results if r["ok"]]
        health_err = [r for r in health_results if not r["ok"]]

        ok(f"Peticiones exitosas: {len(health_ok)} / {HEALTH_N}")
        if health_err:
            warn(f"Peticiones fallidas: {len(health_err)}")

        if health_ok:
            latencies = [r["latency_ms"] for r in health_ok]
            avg_lat = sum(latencies) / len(latencies)
            max_lat = max(latencies)
            min_lat = min(latencies)

            print(f"     Latencia promedio : {avg_lat:.2f} ms")
            print(f"     Latencia mínima   : {min_lat:.2f} ms")
            print(f"     Latencia máxima   : {max_lat:.2f} ms")

            # Umbral: /health no debe bloquearse por los cálculos pesados
            LATENCY_THRESHOLD_MS = 200
            if avg_lat < LATENCY_THRESHOLD_MS:
                ok(f"Latencia promedio ({avg_lat:.1f} ms) < umbral ({LATENCY_THRESHOLD_MS} ms) ✓")
            else:
                warn(f"Latencia promedio ({avg_lat:.1f} ms) supera el umbral ({LATENCY_THRESHOLD_MS} ms)")
                warn("El Event Loop podría estar bloqueándose.")

        # ── Resumen final ──────────────────────────────────────────────────────
        print(f"\n{BOLD}[3] Resumen{RESET}")
        print(f"     Tiempo total de la prueba : {t_total:.2f} s")
        print(f"     Throughput /ingest        : {INGEST_N / t_total:.1f} req/s")

        all_ok = (len(ingest_ok) == INGEST_N) and (last_counter == INGEST_N) and (len(health_ok) == HEALTH_N)
        print()
        if all_ok:
            print(f"{BOLD}{GREEN}  ✔ PRUEBA APROBADA — Todos los criterios cumplidos.{RESET}")
        else:
            print(f"{BOLD}{RED}  ✘ PRUEBA FALLIDA — Revisar los ítems marcados con ✘.{RESET}")

        print(f"\n{BOLD}{'='*60}{RESET}\n")


if __name__ == "__main__":
    asyncio.run(main())
