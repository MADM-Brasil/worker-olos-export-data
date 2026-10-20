# Primeira aquisição AttemptsRawData — 19/10/2026

Evolução posterior: [entrada corporativa once/start](corporate-attempts-worker.md). Este relatório descreve o estado anterior, antes do emissor restrito de tentativa durável; as pré-condições operacionais não são aprovadas automaticamente.

**Estado: não liberada.** Não foi executada aquisição, conexão corporativa, SQL ou alteração do PostgreSQL nesta tarefa. A solicitação de operação única não comprova as pré-condições de recuperação. Não foi criado um permit que aceite flags de risco como substituto de evidência.

## O impedimento preciso

A janela crítica continua sendo: **a Olos avança seu controle, mas o worker cai antes de receber/selar a resposta**. A intenção local identifica nossa tentativa, mas não identifica, sozinha, todos os registros consumidos na origem. Nem certificado aprovado, lock PostgreSQL, spool parcial, ReturnValue=0 ou ausência conhecida de outro consumidor provam recuperação dessa janela.

Falta informação operacional verificável da Olos sobre:

1. Como relacionar uma tentativa deste usuário/worker à faixa registrada por LogReadData quando nenhuma PK chegou ao worker; quais dados identificam o consumidor e como coordenar outros consumidores. O histórico de quatro intervalos de 01/10/2026 não estabelece a atribuição de uma tentativa em 19/10/2026.
2. Retenção, inclusividade, disponibilidade e completude de ReReadData para essa faixa após falha de conexão/hard crash/lease vencida. Aprovar uma política de parada/revisão não comprova que a origem ainda poderá recuperar os registros.
3. A data/fuso atuais do SQL Server e a definição da partição CurrentDay. **ReadDataCurrentDay só recebe TableName; não recebe DateExport.** ExportDate local é metadado da intenção, não filtro remoto. Executar em outro dia não busca retrospectivamente 19/10/2026. StartDate da chamada também não deve ser equiparado à partição sem confirmação.
4. Layout atual completo, nulabilidade, collation e política de sucesso/outputs. Não promover attemptsFixturePlan, que contém hipóteses de ambiente sintético, a contrato corporativo.
5. PEM/fingerprint aprovados independentemente com propósito OLOS_ATTEMPTS_TDS_ACQUISITION e handshake autenticado comprovado. A observação anterior do certificado não é aprovação. Não foi lida configuração privada para inferir essa aprovação durante esta tarefa.

Além dessas evidências, a liberação precisa emitir o permit executável e conectar a entrada manual ao destino local aprovado; esse emissor **continua ausente**, deliberadamente. Não há um arquivo com approved=true ou recoveryValidated=true que faça isso. A condição principal não é uma senha ou uma simples alteração de variável.

Uma resposta pode ter até 200.000 registros. Uma única execução não comprova que retornou todas as chamadas daquele dia, inclusive registros já consumidos antes. Não implementar repetição automática para “completar o dia”. Se registros recebidos tiverem StartDate fora do dia solicitado, preservá-los no spool; não descartá-los silenciosamente para ajustar a contagem.

## Mudança entregue

A CLI existente ganhou um preflight específico e sanitizado da primeira aquisição e reconhece a intenção `read --mode corporate-local --export-date ...`. Enquanto faltarem as pré-condições, recusa **antes de acessar segredos, PostgreSQL, diretórios privados, spool ou rede**. Não cria claim remoto falso nem marca uma tentativa como enviada. A confirmação textual não ignora os bloqueios.

Arquivos alterados/criados:

- src/integration/attempts/contracts.ts: relatório das informações faltantes e erro estruturado de primeira aquisição bloqueada.
- adapters/attempts-integration.ts: exposição do relatório e entrada manual recusada antes de efeitos; fluxo de fixture preservado.
- test/attempts-first-acquisition.test.ts: três testes específicos.
- este documento.

Não foram alterados ExactConnection, pinning, LargeBatchCoordinator, LocalSpool, PostgresLargeBatchPersistence ou ProjectionDispatcher nesta entrega. Eles continuam sendo o caminho de aquisição/spool/envelopes/outbox/entrega já implementado e ensaiado. A política preserveExisting=true do pipeline local mantém as linhas preexistentes, sem inventar recibos/checkpoints para os 570 históricos.

## Comando e seu estado real

Pré-condições, sem rede ou SQL:

```bash
cd ~/worker-olos-export-data
SYNC_ENABLED=false npm run integration:attempts -- requirements --export-date 2026-10-19
```

Entrada concreta reservada à primeira aquisição (sem scheduler):

```bash
SYNC_ENABLED=false npm run integration:attempts -- read \
  --mode corporate-local \
  --export-date 2026-10-19 \
  --work-dir "$HOME/.local/state/olos-worker/attempts-20261019-once" \
  --source-config "$HOME/.config/olos-worker/attempts-corporate.json" \
  --confirm EXECUTE_ATTEMPTS_READ_ONCE
```

**Esse segundo comando ainda termina com ATTEMPTS_CORPORATE_CONTRACT_UNCONFIRMED e executionAllowed=false. Não o apresento como aquisição operacional.** Não cria nem lê os caminhos mostrados enquanto bloqueado. O carregamento da configuração corporativa e emissão de autorização precisam ser concluídos a partir das evidências listadas; não criar esse JSON usando o schema de fixture ou credenciais PostgreSQL adicionais. Nesta tarefa não executei o segundo comando contra a Olos.

Após as pré-condições: validar schema/pools das três contas locais existentes, tirar baseline agregado dos IDs históricos, reservar exclusão/claim durável de tentativa e intenção da origem, enviar apenas um RPC, receber/validar/sincronizar/selar o spool antes de persistir. Confirmar envelopes/checkpoint de ingestão e depois outbox/recibos de negócio. Checkpoint da ingestão sozinho não comprova entrega em raw.olos_calls. Não há transação distribuída.

Em erro/timeout/COMMIT sem ACK: preservar a tentativa; não apagar claim, não iniciar outro diretório para repetir a leitura e não usar ResetReadData. Spool selado segue recuperação exclusivamente local por recibo/digest. Spool parcial exige a recuperação da origem ainda pendente de validação acima.

## Acompanhar o spool depois de uma operação liberada

Não ler/imprimir records.part/records.ready, XMLs ou payloads. Para acompanhar somente metadados técnicos do caso privado:

```bash
export ATTEMPTS_CASE="$HOME/.local/state/olos-worker/attempts-20261019-once"
watch -n 2 'find "$ATTEMPTS_CASE/spool/batches" -name manifest.json -exec jq -c "{batchId:.identity.batchId,runId:.identity.runId,state,sealed,rows,bytes,sha256}" {} \;'
```

Não haverá manifesto novo enquanto a entrada estiver bloqueada. sealed=true comprova fechamento do spool; state=committed refere-se ao estágio do spool/envelope, não à confirmação de todas as entregas de negócio. Conferir a outbox separadamente.

## Consultas PostgreSQL para comprovação futura

Abaixo somente instruções: **não foram executadas nesta tarefa**. Utilizar conexão privada ao 127.0.0.1:5434/olos_worker, sem senha na linha de comando, mantendo cada login no seu papel e sem criar grants. Não utilizar o pool de diagnóstico para escrita.

Com olos_receiver_login, antes e depois da operação, contar chamadas com data civil de 19/10. TIMESTAMP(6) não deve receber AT TIME ZONE por suposição:

```sql
BEGIN READ ONLY;
SELECT count(*) AS chamadas_19_10,
       count(DISTINCT internal_id_olos) AS ids_unicos,
       count(cpf) AS com_cpf,
       count(email) AS com_email,
       count(nome_agente) AS com_agente
FROM raw.olos_calls
WHERE data_ligacao >= TIMESTAMP '2026-10-19 00:00:00'
  AND data_ligacao <  TIMESTAMP '2026-10-20 00:00:00';
COMMIT;
```

Uma contagem positiva prova a existência das linhas, **não a chegada nesta execução**: pode incluir o histórico. Comparar baseline e correlacionar com batchId/runId e recibos. Não exigir total fixo de 570 nem presumir que a contagem sempre aumentará num replay/overlap.

Com olos_ingest_login, conferir o batchId técnico mostrado no manifesto (substituir BATCH_UUID pelo UUID real; nada de payload/PII):

```sql
BEGIN READ ONLY;
SELECT b.id, b.run_id, s.export_date, b.state,
       b.received_count, b.persisted_count, b.duplicate_count,
       b.rejected_count, r.record_count AS spool_records,
       c.version AS checkpoint_version, c.last_batch_id
FROM olos_export.batches b
JOIN olos_export.streams s ON s.id=b.stream_id
JOIN olos_export.large_batch_receipts r ON r.batch_id=b.id
LEFT JOIN olos_export.checkpoints c ON c.stream_id=b.stream_id
WHERE b.id='BATCH_UUID'::uuid;
COMMIT;
```

Com olos_project_login, confirmar que as intenções do lote foram entregues, sem pendências/incertezas:

```sql
BEGIN READ ONLY;
SELECT state, count(*) AS entregas
FROM olos_export.projection_outbox
WHERE batch_id='BATCH_UUID'::uuid
GROUP BY state;
COMMIT;
```

Os recibos do destino devem coincidir com delivery_id/destination_id/digest das intenções desse lote. Um recibo para ID preexistente pode confirmar deduplicação preservando o histórico, não um INSERT novo. Para comprovar integridade dos 570 históricos, guardar previamente seu conjunto de IDs e hash das 16 colunas e recalcular somente sobre esse mesmo conjunto após a operação; hash do total da tabela muda naturalmente quando entram linhas novas. Não exibir seus campos individuais.

## Validação

Typecheck e build aprovados. **17 testes direcionados aprovados em três arquivos**: preflight/CLI, pinning no transporte preparado e decoder/contrato binário. O helper da assinatura oficial rejeita DateExport extra em ReadDataCurrentDay. A entrada corporativa é testada com caminhos inexistentes e spies que falhariam se houvesse conexão TCP/PostgreSQL; a confirmação não produz rede nem ignora recuperação. Testes sintéticos, sem dados pessoais reais.

```bash
npm run typecheck
npm run build
npm test -- test/attempts-first-acquisition.test.ts \
  test/attempts-corporate-pinning.test.ts test/large-batch-wire.test.ts
```

Nenhum acesso/modificação a raw.olos_calls, aos 570 históricos, run-001/run-002, VPS, Dokploy, discadora ou banco oficial. Nenhuma migration, procedure real, commit, push ou deploy. SYNC_ENABLED permanece false. A aquisição solicitada está explicitamente **pendente**, e não foi declarada finalizada ou recuperável sem evidência.
