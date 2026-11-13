# Histórico das execuções

Saída bruta de cada execução de `python -m cdc.cenarios`, na ordem em que rodaram. A 1ª linha
de cada arquivo diz de que código ela veio. As rodadas reprovadas ficam aqui de propósito: são
a origem dos achados do README.

| Rodada | Código | Resultado | O que mostrou |
|---|---|---|---|
| 1 | 00acb6c + alterações locais | reprovada | o consumidor que volta depois da queda ficava parado até a sessão do grupo vencer (45 s) |
| 2 | 00acb6c + alterações locais | reprovada | origem e destino bateram, mas a prova da queda contava 0 reentregas (a mesma faixa de offsets regrava o mesmo arquivo) |
| 3 | 00acb6c + alterações locais | aprovada | 1ª aprovada, só com o log em Parquet |
| 4 | c99dc78 + Delta Lake (depois 697d366) | aprovada | a tabela Delta igual à origem nos 4 cenários |
| 5 | ver `rodada5.log` | ver `../cenarios.md` | latência medida por etapa, depois de o consumidor gravar o snapshot |
