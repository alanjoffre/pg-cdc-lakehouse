"""Consumidor Kafka → Parquet: grava o lote e SÓ DEPOIS confirma o offset.

Se o processo cair entre a gravação e a confirmação, o Kafka reentrega o lote na volta
(pelo menos uma vez). O destino absorve a repetição: a leitura descarta repetidos pela
identidade (tabela, chave, lsn), ver `cdc.destino`.

`--falhar-apos N` derruba o processo de propósito depois de gravar o N-ésimo lote e antes
de confirmar: é a falha que os cenários injetam.
"""
from __future__ import annotations

import argparse
import json
import os
import time
from collections import defaultdict
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, Message, TopicPartition

from cdc.destino import gravar_lote
from cdc.eventos import de_debezium

TOPICOS = "^loja\\.public\\..*"


def _config(bootstrap: str, grupo: str) -> dict[str, object]:
    return {
        "bootstrap.servers": bootstrap,
        "group.id": grupo,
        "enable.auto.commit": False,          # confirmação manual, depois de gravar
        "auto.offset.reset": "earliest",
        "enable.partition.eof": False,
        "topic.metadata.refresh.interval.ms": 2000,   # acha tópico criado depois do subscribe
    }


def consumir(pasta: Path, bootstrap: str = "localhost:59094", grupo: str = "cdc-lakehouse",
             ocioso_s: float = 15.0, lote_max: int = 500, falhar_apos: int | None = None,
             metricas: Path | None = None) -> dict[str, int]:
    """Consome até ficar `ocioso_s` segundos sem mensagem nova. Devolve contagens."""
    c = Consumer(_config(bootstrap, grupo))
    c.subscribe([TOPICOS])
    lotes = mensagens = 0
    ultimo = time.monotonic()
    saida = metricas.open("a", encoding="utf-8") if metricas else None
    try:
        while time.monotonic() - ultimo < ocioso_s:
            msgs = c.consume(num_messages=lote_max, timeout=1.0)
            boas: list[Message] = []
            for m in msgs:
                erro = m.error()
                if erro is None:
                    boas.append(m)
                elif erro.code() != KafkaError._PARTITION_EOF:
                    raise RuntimeError(f"erro do Kafka: {erro}")
            if not boas:
                continue
            ultimo = time.monotonic()
            por_particao: dict[tuple[str, int], list[Message]] = defaultdict(list)
            for m in boas:
                por_particao[(m.topic(), m.partition())].append(m)   # type: ignore[arg-type]
            confirmar = []
            for (topico, particao), grupo_msgs in por_particao.items():
                eventos = [e for m in grupo_msgs
                           if (e := de_debezium(m.key(), m.value())) is not None]
                ini, fim = grupo_msgs[0].offset(), grupo_msgs[-1].offset()
                gravar_lote(pasta, eventos, topico, particao, ini, fim)   # type: ignore[arg-type]
                gravado_ms = time.time() * 1000
                if saida:
                    for e in eventos:
                        if e.op != "r":   # snapshot não tem commit recente: fica fora da latência
                            saida.write(json.dumps({"ms": round(gravado_ms - e.ts_ms, 1)}) + "\n")
                confirmar.append(TopicPartition(topico, particao, fim + 1))  # type: ignore[operator]
                mensagens += len(grupo_msgs)
            lotes += 1
            if falhar_apos is not None and lotes >= falhar_apos:
                if saida:
                    saida.flush()
                os._exit(3)   # queda simulada: gravou, não confirmou
            c.commit(offsets=confirmar, asynchronous=False)
    finally:
        if saida:
            saida.close()
        c.close()
    return {"lotes": lotes, "mensagens": mensagens}


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--pasta", type=Path, default=Path("dados/eventos"))
    ap.add_argument("--ocioso", type=float, default=15.0)
    ap.add_argument("--falhar-apos", type=int)
    ap.add_argument("--metricas", type=Path)
    a = ap.parse_args()
    print(json.dumps(consumir(a.pasta, ocioso_s=a.ocioso, falhar_apos=a.falhar_apos,
                              metricas=a.metricas)))


if __name__ == "__main__":
    main()
