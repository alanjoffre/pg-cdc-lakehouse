# Registro de decisões

Cada decisão de desenho do case, com o motivo e o que fica de fora. Formato curto de ADR
(*Architecture Decision Record*): contexto, decisão, consequência.

## D1. Entrega pelo menos uma vez, com estado final idempotente

- **Contexto:** a garantia padrão do Debezium é entregar cada mudança pelo menos uma vez
  (*at-least-once*). Depois de uma queda, a fila ou o conector reenviam o que já tinham enviado.
  O Kafka Connect tem um modo de entrega exatamente uma vez (KIP-618), e a própria documentação
  do Debezium avisa que a correção das transações do Kafka ainda não está plenamente
  estabelecida.
- **Decisão:** aceitar a repetição na entrega e tornar o **estado final** imune a ela. O
  consumidor só confirma o offset depois de gravar o arquivo, e a leitura descarta repetidos
  pela identidade de cada mudança (D2).
- **Consequência:** o log em Parquet tem cópias por desenho. O que se promete, e o que os
  cenários conferem, é o estado atual igual à origem. O texto do projeto nunca diz "exatamente
  uma vez".

## D2. Identidade de cada mudança: (tabela, chave, lsn)

- **Contexto:** para descartar uma repetição é preciso reconhecer que é a mesma mudança. O
  Debezium informa no bloco `source` o LSN (posição no WAL do PostgreSQL) de cada mudança.
- **Decisão:** a identidade é `(tabela, chave primária, lsn)`. Na leitura fica um evento por
  identidade e, para cada chave, o de maior LSN.
- **Prova (não suposição):** se duas mudanças diferentes da mesma linha chegassem com o mesmo
  LSN, o descarte as fundiria em silêncio. Por isso:
  - o gerador faz, na mesma transação, dois updates da mesma linha e também apaga e reinsere o
    mesmo id;
  - o relatório conta as **colisões de identidade**, ou seja, a mesma identidade com conteúdo
    diferente. Esse número tem de ser 0 em todos os cenários.
- **Limite:** ordenar a mesma chave pelo LSN do registro vale porque o PostgreSQL trava a linha
  até o commit, e duas transações não mudam a mesma linha ao mesmo tempo. Em produção, o campo
  `source.sequence` do Debezium (LSN do commit + LSN do registro) é o desempate mais robusto.

## D3. REPLICA IDENTITY FULL nas tabelas de origem

- **Decisão:** `ALTER TABLE … REPLICA IDENTITY FULL`, para o delete chegar com a linha inteira
  em `before`.
- **Consequência:** mais bytes no WAL por update e delete. Em tabela grande de produção, avaliar
  `DEFAULT` (só a chave), que basta para este destino.

## D4. Sem tombstone e sem DDL pelo CDC

- **Decisão:** `tombstones.on.delete=false`. O destino trata o delete pelo próprio evento `op=d`
  (o código ainda tolera tombstone, se vier).
- **Limite:** o pgoutput não emite DDL. Uma coluna nova na origem exige um procedimento à parte
  (a coluna `linha` guarda JSON e aceita a coluna nova, mas a reconciliação usa uma lista fixa
  de colunas).

## D5. JSON sem schema no ambiente local

- **Decisão:** conversor JSON sem schema e `decimal.handling.mode=string` (o valor monetário não
  vira número de ponto flutuante).
- **Fora do case:** em produção, Avro com Schema Registry (no Debezium 3.x, o Apicurio) para o
  contrato e a evolução do esquema. Ficou de fora porque acrescenta um serviço e um cenário de
  evolução de esquema, o que diluiria a prova principal.

## D6. Falhas injetadas de verdade, e conferidas

- **Decisão:** cada falha é provocada no processo real, não simulada no código de teste:
  1. o consumidor sai com `os._exit(3)` entre gravar e confirmar;
  2. o contêiner do Kafka Connect recebe SIGKILL com a origem recebendo escrita;
  3. cópias de mensagens já consumidas são publicadas de novo, embaralhadas.
- **Conferência:** o cenário só passa se origem e destino baterem **e** a falha tiver
  acontecido: o código de saída 3, a reentrega medida na fonte (mensagens gravadas antes da queda
  × reentregues depois) e os repetidos contados no próprio cenário, não no acumulado.
- **Achado da 1ª rodada:** o consumidor que volta depois da queda ficava parado, porque o grupo
  ainda considerava o consumidor morto dono das partições (sessão padrão de 45 s). Agora a
  sessão é de 10 s, e o tempo ocioso só conta depois que o consumidor recebe as partições.

## D7. Reconciliação por contagem, chaves, conteúdo e hash

- **Decisão:** comparar origem e destino por tabela: contagem, chaves faltando ou sobrando,
  conteúdo divergente na mesma chave e um SHA-256 das linhas em ordem de chave e colunas fixas,
  em forma canônica (data em microssegundos, decimal com 2 casas).
- **Por quê:** contagem igual não prova nada, porque uma linha trocada por outra empata.

## D8. Ambiente pequeno e descartável

- **Decisão:** um broker em KRaft, sem ZooKeeper, e o conjunto todo limitado a cerca de 2 GB.
  Portas próprias (56432, 59094, 58083), para não disputar com outros bancos da máquina.
- **Consequência:** a latência medida serve para comparar execuções deste projeto. Não é
  benchmark.

## D9. Estado atual em Delta Lake, por MERGE que só avança

- **Contexto:** o log bruto em Parquet guarda tudo o que chegou, com repetidos, e o estado atual
  era calculado na leitura (DuckDB). Num lakehouse, quem consulta lê uma tabela já resolvida,
  com transação e histórico de versões.
- **Decisão:** uma tabela Delta por tabela de origem (delta-rs, sem Spark), atualizada pelo
  consumidor a cada lote, **antes** de confirmar o offset:
  1. dedup do lote: fica o evento de maior LSN de cada chave;
  2. `MERGE … WHEN MATCHED AND s.lsn > t.lsn THEN UPDATE`, `WHEN NOT MATCHED THEN INSERT`;
  3. delete lógico: o `op='d'` fica na tabela com `apagado = true` e o LSN dele.
- **Prova:** testes para reaplicar o mesmo lote, evento velho depois do delete, duas mudanças
  da mesma chave no lote, apagar e reinserir o mesmo id e lotes em ordem invertida. Sem o
  dedup do lote, uma tabela nova recebeu a mesma chave 3 vezes, sem erro (conferido tirando a
  regra). Nos 4 cenários, a tabela Delta é reconciliada contra a origem como o log.
- **Consequência:** a queda do consumidor do cenário 2 acontece depois do MERGE e antes da
  confirmação, então a reentrega reaplica lotes já aplicados, e o predicado do LSN os ignora.
  Cada lote é um commit no `_delta_log`: em produção, compactar (`optimize`) e limpar
  (`vacuum`) periodicamente. O MERGE acrescentou ~60 ms ao p50 da latência; o grosso dos ~4 s
  vem antes do consumidor (a investigar).
- **Equivalente no Databricks:** `AUTO CDC` (antes `APPLY CHANGES INTO`) com `KEYS (id)`,
  `SEQUENCE BY lsn` e `APPLY AS DELETE WHEN op = 'd'`.
- **Fora do case:** Iceberg (a mesma ideia com outro formato de tabela), Flink e Spark
  Structured Streaming. Ficam de fora para manter o ambiente pequeno (D8).
