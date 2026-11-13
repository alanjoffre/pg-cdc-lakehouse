"""O relatório legível diz o que o JSON diz, inclusive quando algo falha."""
from cdc.relatorio import gerar_md


def _tabela(origem, destino, faltando=()):
    return {"linhas_origem": origem, "linhas_destino": destino, "faltando": list(faltando),
            "sobrando": [], "divergentes": [], "assinatura_igual": not faltando}


def _cenario(ok=True, **extra):
    return {"clientes": _tabela(2, 2), "pedidos": _tabela(3, 3 if ok else 2, () if ok else [9]),
            "repetidos_absorvidos_no_cenario": 4, "falha_exercitada": True,
            "prova_da_falha": "x", "colisoes_de_identidade": 0, "ok": ok, **extra}


def test_aprovado_mostra_numeros_e_latencia():
    rel = {"semente": 42, "ok": True, "cenarios": {"1_normal": _cenario(
        casos_de_borda={"dois_updates": 20, "apaga_e_reinsere": 20},
        latencia_ms={"n": 10, "p50": 100.4, "p95": 250.0, "max": 300.0})}}
    md = gerar_md(rel)
    assert "**APROVADO**" in md
    assert "clientes 2 × 2 · pedidos 3 × 3" in md
    assert "p95 250 ms" in md and "não é benchmark" in md
    assert "20 vezes dois updates" in md


def test_reprovado_aparece_em_destaque():
    md = gerar_md({"semente": 1, "ok": False, "cenarios": {"2_queda_consumidor": _cenario(False)}})
    assert "**REPROVADO**" in md and "**falhou**" in md and "**NÃO**" in md
