"""Roteiro de ponta a ponta: sobe o ambiente limpo, injeta cada falha e reconcilia.

Cenários, na ordem (o mesmo ambiente, o estado vai se acumulando):
1. normal: snapshot inicial + mudanças com o consumidor rodando junto; mede a latência
   (commit na origem → arquivo gravado);
2. queda do consumidor: o processo cai depois de gravar e antes de confirmar o offset;
   na volta o Kafka reentrega o lote;
3. queda do conector: o Debezium é morto (SIGKILL) com a origem recebendo escrita, e volta
   do último offset que tinha salvo — reenvia o que já tinha publicado;
4. mensagem repetida: cópias de mensagens já consumidas são publicadas de novo no tópico.

Depois de cada um: origem × destino por tabela (contagem, chaves, conteúdo e hash), nos dois
destinos: o estado calculado do log em Parquet (DuckDB) e a tabela Delta Lake (MERGE).
O resultado vai para reports/cenarios.json.
"""
from __future__ import annotations

import json
import random
import shutil
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

from cdc import delta, gerador
from cdc.destino import colisoes_de_identidade, estado_atual
from cdc.reconciliar import comparar
from cdc.relatorio import escrever_md

RAIZ = Path(__file__).resolve().parents[2]
PASTA_DELTA = RAIZ / "dados" / "delta"
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


def _pg_pronto() -> bool:
    psycopg.connect(gerador.DSN).close()
    return True


def subir() -> None:
    _compose("down", "-v", "--remove-orphans")
    _compose("up", "-d")
    _esperar(_pg_pronto, 120, "PostgreSQL")
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
               ocioso_s: float = 20) -> subprocess.Popen[str]:
    cmd = [sys.executable, "-m", "cdc.consumidor", "--pasta", str(pasta),
           "--delta", str(PASTA_DELTA), "--ocioso", str(ocioso_s)]
    if metricas:
        cmd += ["--metricas", str(metricas)]
    if falhar_apos:
        cmd += ["--falhar-apos", str(falhar_apos)]
    return subprocess.Popen(cmd, cwd=RAIZ, stdout=subprocess.PIPE, text=True)


def fim_do_consumidor(proc: subprocess.Popen[str]) -> tuple[int, dict[str, Any]]:
    """Espera o consumidor e devolve (código de saída, contagens que ele imprimiu)."""
    saida, _ = proc.communicate()
    linhas = [linha for linha in saida.splitlines() if linha.startswith("{")]
    return proc.returncode, (json.loads(linhas[-1]) if linhas else {})


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
                no_delta = [_canonica(r) for r in delta.estado_atual(PASTA_DELTA, tabela)]
                rd = comparar(tabela, origem, no_delta, cols)
                res[tabela] = {**asdict(r), "ok": r.ok,
                               "delta": {**asdict(rd), "ok": rd.ok,
                                         "commits": delta.versoes(PASTA_DELTA, tabela)}}
        brutos, unicos = con.execute(f"""
            SELECT count(*), count(DISTINCT (tabela, chave, lsn))
            FROM read_parquet('{pasta}/*.parquet')""").fetchone()   # type: ignore[misc]
        colisoes = colisoes_de_identidade(con, pasta)
    finally:
        con.close()
    res["eventos_gravados"] = brutos
    res["eventos_unicos"] = unicos
    res["repetidos_absorvidos_total"] = brutos - unicos
    res["colisoes_de_identidade"] = colisoes
    res["delta_igual_origem"] = all(res[t]["delta"]["ok"] for t in COLUNAS)
    res["origem_igual_destino"] = (all(res[t]["ok"] for t in COLUNAS) and colisoes == 0
                                   and res["delta_igual_origem"])
    return res


def _percentil(valores: list[float], q: float) -> float:
    ordem = sorted(valores)
    return ordem[min(len(ordem) - 1, int(round(q * (len(ordem) - 1))))]


class _Registro:
    """Fecha cada cenário: repetidos do próprio cenário (não o acumulado) e a falha conferida."""

    def __init__(self, pasta: Path) -> None:
        self.pasta = pasta
        self.repetidos_antes = 0
        self.cenarios: dict[str, dict[str, Any]] = {}

    def fechar(self, nome: str, dados: dict[str, Any],
               falha: Callable[[int], tuple[bool, str]] | None = None) -> None:
        r = reconciliar(self.pasta)
        novos = r["repetidos_absorvidos_total"] - self.repetidos_antes
        self.repetidos_antes = r["repetidos_absorvidos_total"]
        c = {**dados, **r, "repetidos_absorvidos_no_cenario": novos}
        if falha is None:
            c["falha_exercitada"], c["prova_da_falha"] = True, "sem falha injetada"
        else:
            c["falha_exercitada"], c["prova_da_falha"] = falha(novos)
        c["ok"] = c["origem_igual_destino"] and c["falha_exercitada"]
        self.cenarios[nome] = c


# ---------- roteiro ----------

def executar(semente: int = 42) -> dict[str, Any]:
    pasta = RAIZ / "dados" / "eventos"
    for velho in pasta.glob("*.parquet"):
        velho.unlink()
    shutil.rmtree(PASTA_DELTA, ignore_errors=True)
    lat_arq = RAIZ / "dados" / "latencia.jsonl"
    lat_arq.parent.mkdir(parents=True, exist_ok=True)
    lat_arq.write_text("")
    est = gerador.Estado(random.Random(semente))
    rel: dict[str, Any] = {"semente": semente,
                           "executado_em": datetime.now(UTC).isoformat(timespec="seconds"),
                           "cenarios": {}}
    reg = _Registro(pasta)

    subir()
    with psycopg.connect(gerador.DSN) as pg:
        gerador.carga_inicial(pg, est, n_clientes=200, n_pedidos=1000)
    registrar_conector()

    # 1. normal, com o consumidor rodando junto
    proc = consumidor(pasta, metricas=lat_arq)
    with psycopg.connect(gerador.DSN) as pg:
        feito = gerador.mudancas(pg, est, 3000)
        bordas = gerador.mesma_linha_na_mesma_transacao(pg, est, 40)
    fim_do_consumidor(proc)
    medidas = [json.loads(linha) for linha in lat_arq.read_text().splitlines()]

    def _lat(campo: str) -> dict[str, float]:
        v = [m[campo] for m in medidas]
        return {"n": len(v), "p50": _percentil(v, .5), "p95": _percentil(v, .95), "max": max(v)}

    reg.fechar("1_normal", {"mudancas": feito, "casos_de_borda": bordas,
                            "latencia_ms": _lat("ms"), "latencia_delta_ms": _lat("ms_delta")})

    # 2. queda do consumidor entre gravar e confirmar
    with psycopg.connect(gerador.DSN) as pg:
        feito = gerador.mudancas(pg, est, 1000)
    caiu, antes = fim_do_consumidor(consumidor(pasta, falhar_apos=1))
    _, depois = fim_do_consumidor(consumidor(pasta))
    gravadas = int(antes.get("mensagens", 0))
    mesma_faixa = int(depois.get("reentregues_mesma_faixa", 0))
    reg.fechar("2_queda_consumidor",
               {"mudancas": feito, "codigo_da_queda": caiu,
                "gravadas_antes_da_queda": gravadas,
                "reentregues_na_mesma_faixa": mesma_faixa},
               lambda novos: (caiu == 3 and gravadas > 0 and mesma_faixa + novos >= gravadas,
                              f"o consumidor saiu com o código {caiu} (3 = queda simulada) "
                              f"depois de gravar {gravadas} mensagens sem confirmar; na volta a "
                              f"fila reentregou {mesma_faixa} na mesma faixa de offsets (arquivo "
                              f"regravado igual) e {novos} em outra faixa (descartadas na "
                              f"leitura)"))

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
    fim_do_consumidor(consumidor(pasta, ocioso_s=30))
    reg.fechar("3_queda_conector", {"mudancas": saida},
               lambda novos: (novos > 0,
                              f"depois do SIGKILL o Debezium voltou do último offset salvo e "
                              f"publicou de novo {novos} eventos"))

    # 4. mensagens repetidas publicadas de novo no tópico
    n = repetir_mensagens(400)
    fim_do_consumidor(consumidor(pasta))
    reg.fechar("4_mensagem_repetida", {"copias_publicadas": n},
               lambda novos: (n > 0 and novos == n,
                              f"{n} cópias publicadas no tópico, {novos} descartadas no destino"))

    rel["cenarios"] = reg.cenarios
    rel["ok"] = all(c["ok"] for c in rel["cenarios"].values())
    destino = RAIZ / "reports" / "cenarios.json"
    destino.parent.mkdir(exist_ok=True)
    destino.write_text(json.dumps(rel, indent=2, ensure_ascii=False) + "\n")
    escrever_md(rel, RAIZ / "reports" / "cenarios.md")
    return rel


def main() -> None:
    rel = executar()
    for nome, c in rel["cenarios"].items():
        print(f"{nome:24} ok={c['ok']}  delta={c['delta_igual_origem']}  "
              f"gravados={c['eventos_gravados']}  "
              f"repetidos_no_cenario={c['repetidos_absorvidos_no_cenario']}  "
              f"colisoes={c['colisoes_de_identidade']}")
    if "latencia_ms" in rel["cenarios"]["1_normal"]:
        print("latência até o Parquet (ms):", rel["cenarios"]["1_normal"]["latencia_ms"])
        print("latência até o Delta (ms):", rel["cenarios"]["1_normal"].get("latencia_delta_ms"))
    print("APROVADO" if rel["ok"] else "REPROVADO")
    sys.exit(0 if rel["ok"] else 1)


if __name__ == "__main__":
    main()
