"""Roteiro de ponta a ponta: sobe o ambiente limpo, injeta cada falha e reconcilia.

Cenários, na ordem (o mesmo ambiente, o estado vai se acumulando):
1. normal: snapshot inicial + mudanças com o consumidor rodando junto; mede a latência
   (commit na origem → arquivo gravado);
2. queda do consumidor: o processo cai depois de gravar e antes de confirmar o offset;
   na volta o Kafka reentrega o lote;
3. queda do conector: o Debezium é morto (SIGKILL) com a origem recebendo escrita, e volta
   do último offset que tinha salvo — reenvia o que já tinha publicado;
4. mensagem repetida: cópias de mensagens já consumidas são publicadas de novo no tópico.

Depois de cada um: origem × destino por tabela (contagem, chaves, conteúdo e hash).
O resultado vai para reports/cenarios.json.
"""
from __future__ import annotations

import json
import random
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable
from dataclasses import asdict
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import duckdb
import psycopg
from confluent_kafka import Consumer, Producer

from cdc import gerador
from cdc.destino import estado_atual
from cdc.reconciliar import comparar

RAIZ = Path(__file__).resolve().parents[2]
CONNECT = "http://localhost:58083"
CONECTOR = "loja-postgres"
BOOTSTRAP = "localhost:59094"
COLUNAS = {"clientes": ["id", "nome", "cidade", "atualizado"],
           "pedidos": ["id", "cliente_id", "valor", "status", "atualizado"]}
_EPOCA = datetime(1970, 1, 1, tzinfo=UTC)


# ---------- ambiente ----------

def _compose(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], cwd=RAIZ, check=True,
                   stdout=subprocess.DEVNULL)


def _esperar(cond: Callable[[], bool], limite_s: float, oque: str) -> None:
    fim = time.monotonic() + limite_s
    while time.monotonic() < fim:
        try:
            if cond():
                return
        except Exception:   # noqa: BLE001 — serviço ainda subindo
            pass
        time.sleep(2)
    raise TimeoutError(f"esperei {limite_s:.0f}s por: {oque}")


def _http(metodo: str, url: str, corpo: dict | None = None) -> tuple[int, Any]:
    dados = None if corpo is None else json.dumps(corpo).encode()
    req = urllib.request.Request(url, data=dados, method=metodo,
                                 headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            texto = r.read().decode() or "null"
            return r.status, json.loads(texto)
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()


def _conector_rodando() -> bool:
    st, corpo = _http("GET", f"{CONNECT}/connectors/{CONECTOR}/status")
    return (st == 200 and corpo["connector"]["state"] == "RUNNING"
            and bool(corpo["tasks"]) and corpo["tasks"][0]["state"] == "RUNNING")


def subir() -> None:
    _compose("down", "-v", "--remove-orphans")
    _compose("up", "-d")
    _esperar(lambda: psycopg.connect(gerador.DSN).close() is None, 120, "PostgreSQL")
    _esperar(lambda: _http("GET", f"{CONNECT}/connectors")[0] == 200, 240, "Kafka Connect")


def registrar_conector() -> None:
    config = json.loads((RAIZ / "connector" / "postgres.json").read_text())["config"]
    st, corpo = _http("PUT", f"{CONNECT}/connectors/{CONECTOR}/config", config)
    if st not in (200, 201):
        raise RuntimeError(f"conector recusado ({st}): {corpo}")
    _esperar(_conector_rodando, 120, "conector RUNNING")


def derrubar_conector() -> None:
    _compose("kill", "connect")     # SIGKILL: sem desligamento limpo, sem salvar offset
    time.sleep(3)
    _compose("start", "connect")
    _esperar(_conector_rodando, 240, "conector RUNNING depois da queda")


# ---------- consumidor e mensagens ----------

def consumidor(pasta: Path, metricas: Path | None = None, falhar_apos: int | None = None,
               ocioso_s: float = 20) -> subprocess.Popen[bytes]:
    cmd = [sys.executable, "-m", "cdc.consumidor", "--pasta", str(pasta),
           "--ocioso", str(ocioso_s)]
    if metricas:
        cmd += ["--metricas", str(metricas)]
    if falhar_apos:
        cmd += ["--falhar-apos", str(falhar_apos)]
    return subprocess.Popen(cmd, cwd=RAIZ)


def repetir_mensagens(n: int, semente: int = 7) -> int:
    """Lê as n primeiras mensagens de pedidos e publica cópias, embaralhadas, no mesmo tópico."""
    c = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": f"repetidor-{time.time_ns()}",
                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
    c.subscribe(["loja.public.pedidos"])
    lidas: list[tuple[bytes | None, bytes | None]] = []
    fim = time.monotonic() + 60
    while len(lidas) < n and time.monotonic() < fim:
        for m in c.consume(num_messages=n - len(lidas), timeout=1.0):
            if m.error() is None:
                lidas.append((m.key(), m.value()))
    c.close()
    random.Random(semente).shuffle(lidas)
    p = Producer({"bootstrap.servers": BOOTSTRAP})
    for chave, valor in lidas:
        p.produce("loja.public.pedidos", key=chave, value=valor)
    p.flush(30)
    return len(lidas)


# ---------- reconciliação ----------

def _micros(v: Any) -> int:
    dt = datetime.fromisoformat(v) if isinstance(v, str) else v
    return (dt - _EPOCA) // timedelta(microseconds=1)


def _canonica(linha: dict[str, Any]) -> dict[str, Any]:
    """Mesma forma dos dois lados: data em microssegundos, decimal com 2 casas."""
    out = dict(linha)
    out["atualizado"] = _micros(linha["atualizado"])
    if "valor" in out:
        out["valor"] = str(Decimal(str(linha["valor"])).quantize(Decimal("0.01")))
    return out


def reconciliar(pasta: Path) -> dict[str, Any]:
    res: dict[str, Any] = {}
    con = duckdb.connect()
    try:
        with psycopg.connect(gerador.DSN) as pg, pg.cursor() as cur:
            for tabela, cols in COLUNAS.items():
                cur.execute(f"SELECT {', '.join(cols)} FROM {tabela} ORDER BY id")
                origem = [_canonica(dict(zip(cols, r, strict=True))) for r in cur.fetchall()]
                destino = [_canonica(r) for r in estado_atual(con, pasta, tabela)]
                r = comparar(tabela, origem, destino, cols)
                res[tabela] = {**asdict(r), "ok": r.ok}
        brutos, unicos = con.execute(f"""
            SELECT count(*), count(DISTINCT (tabela, chave, lsn))
            FROM read_parquet('{pasta}/*.parquet')""").fetchone()   # type: ignore[misc]
    finally:
        con.close()
    res["eventos_gravados"] = brutos
    res["eventos_unicos"] = unicos
    res["repetidos_absorvidos"] = brutos - unicos
    res["ok"] = all(res[t]["ok"] for t in COLUNAS)
    return res


def _percentil(valores: list[float], q: float) -> float:
    ordem = sorted(valores)
    return ordem[min(len(ordem) - 1, int(round(q * (len(ordem) - 1))))]


# ---------- roteiro ----------

def executar(semente: int = 42) -> dict[str, Any]:
    pasta = RAIZ / "dados" / "eventos"
    for velho in pasta.glob("*.parquet"):
        velho.unlink()
    lat_arq = RAIZ / "dados" / "latencia.jsonl"
    lat_arq.parent.mkdir(parents=True, exist_ok=True)
    lat_arq.write_text("")
    est = gerador.Estado(random.Random(semente))
    rel: dict[str, Any] = {"semente": semente, "cenarios": {}}

    subir()
    with psycopg.connect(gerador.DSN) as pg:
        gerador.carga_inicial(pg, est, n_clientes=200, n_pedidos=1000)
    registrar_conector()

    # 1. normal, com o consumidor rodando junto
    proc = consumidor(pasta, metricas=lat_arq)
    with psycopg.connect(gerador.DSN) as pg:
        feito = gerador.mudancas(pg, est, 3000)
    proc.wait()
    lat = [json.loads(linha)["ms"] for linha in lat_arq.read_text().splitlines()]
    rel["cenarios"]["1_normal"] = {"mudancas": feito, **reconciliar(pasta),
                                   "latencia_ms": {"n": len(lat), "p50": _percentil(lat, .5),
                                                   "p95": _percentil(lat, .95),
                                                   "max": max(lat)}}

    # 2. queda do consumidor entre gravar e confirmar
    with psycopg.connect(gerador.DSN) as pg:
        feito = gerador.mudancas(pg, est, 1000)
    caiu = consumidor(pasta, falhar_apos=1).wait()
    consumidor(pasta).wait()
    rel["cenarios"]["2_queda_consumidor"] = {"mudancas": feito, "codigo_da_queda": caiu,
                                             **reconciliar(pasta)}

    # 3. queda do conector com escrita na origem
    saida: dict[str, int] = {}

    def escrever() -> None:
        with psycopg.connect(gerador.DSN) as pg:
            saida.update(gerador.mudancas(pg, est, 1500, pausa_s=0.05))

    t = threading.Thread(target=escrever)
    t.start()
    time.sleep(1.5)
    derrubar_conector()
    t.join()
    consumidor(pasta, ocioso_s=30).wait()
    rel["cenarios"]["3_queda_conector"] = {"mudancas": saida, **reconciliar(pasta)}

    # 4. mensagens repetidas publicadas de novo no tópico
    n = repetir_mensagens(400)
    consumidor(pasta).wait()
    rel["cenarios"]["4_mensagem_repetida"] = {"copias_publicadas": n, **reconciliar(pasta)}

    rel["ok"] = all(c["ok"] for c in rel["cenarios"].values())
    destino = RAIZ / "reports" / "cenarios.json"
    destino.parent.mkdir(exist_ok=True)
    destino.write_text(json.dumps(rel, indent=2, ensure_ascii=False) + "\n")
    return rel


def main() -> None:
    rel = executar()
    for nome, c in rel["cenarios"].items():
        print(f"{nome:24} ok={c['ok']}  gravados={c['eventos_gravados']}  "
              f"repetidos_absorvidos={c['repetidos_absorvidos']}")
    if "latencia_ms" in rel["cenarios"]["1_normal"]:
        print("latência (ms):", rel["cenarios"]["1_normal"]["latencia_ms"])
    print("APROVADO" if rel["ok"] else "REPROVADO")
    sys.exit(0 if rel["ok"] else 1)


if __name__ == "__main__":
    main()
