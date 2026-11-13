"""Relatório legível dos cenários: `reports/cenarios.json` → `reports/cenarios.md`.

O JSON é a prova completa; o Markdown é o resumo que se lê sem abrir código: uma linha por
falha injetada, com a origem e o destino lado a lado.

    python -m cdc.relatorio            # regenera o .md a partir do .json já gravado
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

RAIZ = Path(__file__).resolve().parents[2]

TITULOS = {
    "1_normal": "Operação normal (snapshot + mudanças com o consumidor rodando)",
    "2_queda_consumidor": "Queda do consumidor entre gravar e confirmar o offset",
    "3_queda_conector": "Queda do Debezium (SIGKILL) com a origem recebendo escrita",
    "4_mensagem_repetida": "Mensagens repetidas publicadas de novo no tópico",
}
TABELAS = ("clientes", "pedidos")


def _sim(v: bool) -> str:
    return "sim" if v else "**NÃO**"


def _linhas(c: dict[str, Any]) -> str:
    return " · ".join(f"{t} {c[t]['linhas_origem']} × {c[t]['linhas_destino']}" for t in TABELAS)


def _falhas(c: dict[str, Any]) -> int:
    return sum(len(c[t][k]) for t in TABELAS for k in ("faltando", "sobrando", "divergentes"))


def _conferida(c: dict[str, Any]) -> str:
    if c.get("prova_da_falha") == "sem falha injetada":
        return "n/a"
    return _sim(c["falha_exercitada"])


def _delta(c: dict[str, Any]) -> str:
    """Origem × tabela Delta: "sim" só se contagem, chaves, conteúdo e hash baterem."""
    if not all("delta" in c[t] for t in TABELAS):
        return "—"
    return _sim(all(c[t]["delta"]["ok"] for t in TABELAS))


def gerar_md(rel: dict[str, Any]) -> str:
    cen = rel["cenarios"]
    out = [
        "# Resultado dos cenários",
        "",
        f"Execução: {rel.get('executado_em', '—')} · commit {rel.get('commit', '—')} · "
        f"semente {rel['semente']} · "
        f"resultado geral: **{'APROVADO' if rel['ok'] else 'REPROVADO'}**",
        "",
        "Gerado por `python -m cdc.cenarios` (prova completa em `cenarios.json`). Dado sintético,",
        "uma máquina, um broker. Cada cenário roda sobre o estado deixado pelo anterior.",
        "Dois destinos conferidos contra a origem: o estado calculado do log em Parquet (DuckDB)",
        "e a tabela Delta Lake mantida por MERGE.",
        "",
        "| Cenário | Linhas origem × destino | Faltando, sobrando ou divergentes | Hash igual "
        "| Delta igual à origem | Repetidos descartados no cenário | Falha conferida "
        "| Resultado |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for nome, c in cen.items():
        hash_ok = all(c[t]["assinatura_igual"] for t in TABELAS)
        out.append(
            f"| {TITULOS.get(nome, nome)} | {_linhas(c)} | {_falhas(c)} | {_sim(hash_ok)} "
            f"| {_delta(c)} "
            f"| {c['repetidos_absorvidos_no_cenario']} | {_conferida(c)} "
            f"| {'ok' if c['ok'] else '**falhou**'} |")
    out += ["", "## Como cada falha foi conferida", ""]
    for nome, c in cen.items():
        out.append(f"- **{TITULOS.get(nome, nome)}:** {c['prova_da_falha']}.")
    normal = cen.get("1_normal", {})
    if "casos_de_borda" in normal:
        b = normal["casos_de_borda"]
        out += ["", "## Identidade de cada mudança", "",
                f"Na mesma transação: {b['dois_updates']} vezes dois updates da mesma linha e "
                f"{b['apaga_e_reinsere']} vezes apagar e inserir de novo o mesmo id.",
                "Colisões de identidade (o mesmo `(tabela, chave, lsn)` com conteúdo diferente), "
                "por cenário: "
                + ", ".join(f"{n.split('_', 1)[0]}: {c['colisoes_de_identidade']}"
                            for n, c in cen.items()) + "."]
    etapas = [("latencia_kafka_ms", "no tópico do Kafka"),
              ("latencia_ms", "no arquivo Parquet gravado"),
              ("latencia_delta_ms", "na tabela Delta (depois do MERGE)")]
    medidas = [(rot, normal[k]) for k, rot in etapas if k in normal]
    if medidas:
        out += ["", "## Latência (cenário 1)", "",
                f"Da mudança na origem (`source.ts_ms`) até cada etapa, {medidas[0][1]['n']} "
                "mudanças, medidas depois de o consumidor gravar o snapshot:", "",
                "| Até | p50 | p95 | máximo |", "|---|---|---|---|",
                *(f"| {rot} | {m['p50']:.0f} ms | {m['p95']:.0f} ms | {m['max']:.0f} ms |"
                  for rot, m in medidas),
                "",
                "Medida numa máquina local com Docker e um broker: serve para comparar "
                "execuções deste projeto, não é benchmark."]
    return "\n".join(out) + "\n"


def escrever_md(rel: dict[str, Any], destino: Path) -> None:
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_text(gerar_md(rel), encoding="utf-8")


def main() -> None:
    rel = json.loads((RAIZ / "reports" / "cenarios.json").read_text(encoding="utf-8"))
    escrever_md(rel, RAIZ / "reports" / "cenarios.md")


if __name__ == "__main__":
    main()
