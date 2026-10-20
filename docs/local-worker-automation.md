# Worker local: automação, estado e recuperação

Entrega de 19/10/2026. Automação e HTTP integrados; **aquisição corporativa continua bloqueada**. Não há novas chamadas reais sendo coletadas. O PostgreSQL local foi verificado sem DML/DDL; as 570 linhas foram comparadas por hash antes/depois da inicialização segura.

## Alterações

| Arquivo | Função |
| --- | --- |
| `adapters/local-worker.ts` | Entrada local explícita; configuração privada, status autenticado, start/check/status; nunca cria fonte corporativa |
| `src/integration/attempts/pools.ts` | Pools limitados e separados para ingestão/projeção/recepção, com os três logins existentes |
| `src/integration/attempts/service.ts` | Lifecycle, preflight de schema/privilégios, exclusão entre instâncias, agregados e integração HTTP |
| `src/integration/attempts/pipeline.ts` | Composição do coordenador, spool, persistência/captura de outbox e entrega existentes; recuperação antes de consumo |
| `src/integration/attempts/runtime-control.ts` | Usa AttemptsCycleRunner; contagens, timestamps, abort, parada após erro e estado de conciliação |
| `src/app.ts`, `src/main.ts` | Injeção do runtime no mesmo servidor; readiness real e shutdown; main reutilizável sem segundo listener |
| `src/offline/tds-delivery.ts`, `tds-writer.ts` | Política explícita de preservar linhas preexistentes; rejeição dos quatro campos sem equivalência |
| `package.json` | `worker:local` |
| `test/local-automation.test.ts`, `test/app.test.ts`, `test/tds-end-to-end.test.ts` | Seis testes novos; expectativa de readiness e rejeição dos campos sem equivalência |
| README e este guia | Comandos e limites |

Não houve migration nova, alteração de grants/roles, aprovação automática de contratos ou DDL no boot. Os pools de `src/db/pools.ts` continuam somente leitura e não são usados para persistência. WireGuard, discadora, VPS, PostgreSQL corporativo e run-001/run-002 não foram modificados.

## Execução segura agora

Requer Node/npm instalados e o container local `olos-worker-postgres` previamente preparado, com `127.0.0.1:5434/olos_worker`. Nada neste comando recria container/volume/banco.

```bash
cd ~/worker-olos-export-data
npm run typecheck
npm run build
npm run worker:local -- check
# Conferir se já existe processo na porta; não matar outro serviço indiscriminadamente:
ss -ltnp '( sport = :3100 )'
npm run worker:local -- start
```

`start` permanece em primeiro plano. Ele lê somente os caminhos privados:

- `~/.config/olos-worker/postgres-credentials.json`: três senhas dos logins nomeados, não superusuário.
- `~/.config/olos-worker/worker-local.env`: SYNC_ENABLED=false, destino local fixo e configurações HTTP/intervalo.

Os dois arquivos originais não são reescritos. São exigidos arquivos 0600 pertencentes ao usuário, caminhos sem symlink e diretório sem escrita por terceiros. Não há argumentos com senha nem fallback de host/banco/usuário. Host/porta/banco PostgreSQL são fixados ao destino local aprovado. A conexão sem TLS é exclusiva do PostgreSQL em loopback; os flags genéricos de TLS inseguro do env antigo **não** são encaminhados à Olos. Nenhuma credencial Olos é utilizada por essa entrada.

O token de status é lido de STATUS_TOKEN, se configurado. Caso contrário, a entrada cria/reutiliza `~/.config/olos-worker/http-status-token`, 0600, sem imprimir seu conteúdo. Esse é o único arquivo de configuração auxiliar criado; não modifica os env/JSON originais ou históricos.

O comando genérico `npm start` conserva a entrada HTTP de diagnóstico. Para esta composição local usar **worker:local**, sem iniciar os dois processos na mesma porta. Erro de bind encerra somente o processo novo e fecha seus pools.

## Saúde e estatísticas

```bash
curl -s http://127.0.0.1:3100/health/live
curl -s http://127.0.0.1:3100/health/ready
npm run worker:local -- status
```

A CLI status lê o token privado e autentica GET /status sem mostrá-lo. Não há endpoint administrativo para habilitar aquisição. `/health/live` responde 200 enquanto o serviço vive. `/health/ready` permanece 503 com `reason=acquisition_blocked` e `syncEnabled=false` no modo atual. PostgreSQL saudável não significa autorização de consumo.

`/status.sync` distingue estado, sourceConfigured/sourceAuthorized/sourceKind, databaseState, schemaReady, enabled/armed, inFlight, intervalo, último início/conclusão/aquisição confirmada, falha sanitizada e recoveryPending. O modo local declara sourceKind=corporate-blocked; um IP conhecido não equivale a um adaptador configurado/autorizado.

Agregados: totalCalls, withCpf, withEmail, withAgentName, pendingDeliveries, confirmedDeliveries, checkpoints e ingestionTotals. Nenhum telefone/CPF/e-mail/nome individual ou payload é devolvido. Os contadores with* medem campos não nulos. Checkpoints mostram somente versão e referência técnica de lote, sem cursor arbitrário.

Contagens received/persisted/duplicates/rejected referem-se ao **último ciclo de ingestão completo**, conforme countsScope. ingestionTotals mostra os totais duráveis de ingestão; confirmedDeliveries mede a confirmação das intenções. Não equivalem à quantidade de novos INSERTs de negócio: um ID já existente pode ser preservado por deduplicação. statisticsCheckedAt/statisticsAvailable indicam idade e disponibilidade dos agregados. Monitoramento é periódico; não se promete uma leitura instantânea/transação única entre todos os pools a cada GET.

Pré-condições do runtime: autenticação pelos três logins esperados, ausência de superusuário, permissões de cada etapa (inclusive UPDATE por coluna do projector), 16 nomes/tipos/ordem, PK internal_id_olos e timestamp com precisão 6. Não amplia permissões para fazer o preflight passar.

## Pipeline preparado

`createAutomatedAttemptsService(pools, spool, limits, binding, intervalMs)` é a composição exercitada nos testes. Reutiliza:

1. AttemptsRuntimeControl → AttemptsCycleRunner → Scheduler, sem sobreposição; intervalo padrão 60.000 ms **após término** do ciclo anterior.
2. Lock de sessão PostgreSQL `(740219,1)` durante toda a vida do consumidor. Uma segunda instância não consome. Perda da conexão de posse aborta o ciclo, retira readiness e exige conciliação. Isso não prova ausência de consumidores externos no SQL Server.
3. LargeBatchCoordinator + LocalSpool: uma resposta completa, backpressure, limites de memória/disco, fim explícito e selo/fsync antes da persistência. Não inventa paginação Olos. Os limites existentes, inclusive até 200.000 registros, são preservados.
4. PostgresLargeBatchPersistence + ChunkedProjectionCapture: envelopes, recibo do lote, outbox e **checkpoint de ingestão** no mesmo commit PostgreSQL.
5. ProjectionDispatcher + PostgresDeliveryTarget + projectTdsAttempts/IsolatedTdsCallsWriter: transformação existente e recibo transacional do destino.
6. Só libera a próxima aquisição quando todas as entregas estiverem confirmadas. Checkpoint de envelope não é confirmação de raw.olos_calls. Não há transação distribuída entre origem, envelope e destino.

Spool ativo é recuperado localmente antes de criar qualquer fonte. Run pendente sem spool conhecido bloqueia. Entrega pendente é drenada em um ciclo exclusivamente local; esse ciclo não abre nova leitura de origem. Resultado incerto é conciliado pelo recibo/digest; recibo ausente/desconhecido não autoriza retry remoto. Rejeição, timeout, parcial, corrupção, falta de disco ou falha de entrega interrompem a recorrência em needs_reconciliation. Não há catch que simplesmente espere 60 s e consuma mais dados.

O pipeline atual aceita somente bindings **isolated-fixture**, sem parâmetro/env para disfarçar endpoint corporativo. O coordenador conserva suas verificações de capacidades reais. Os seis testes novos exercitam a automação real com gerador binário sintético e PostgreSQL descartável; não certificam SQL Server Olos. ReadonlyCallPoller/Source permanecem disponíveis, mas nenhum objeto SELECT foi presumido ou conectado: um adaptador read-only requer confirmação própria de objeto, chave, escopo, permissões e TLS.

## Histórico e contrato de negócio

O modelo de 16 colunas fornecido foi preservado. lead_id_origem, tma_origem, razao_desligamento e raw_olos_call_id são obrigatoriamente NULL nas novas intenções; não há cálculo de TMA nesta composição. Campanha/tabulação/agente dependem dos catálogos autorizados fornecidos ao binding; não há consulta automática Users ou filtro de operadores/campanhas.

A composição automática passa `preserveExisting=true` ao writer existente. INSERT parametrizado usa ON CONFLICT(internal_id_olos) DO NOTHING para linhas preexistentes, preservando **todos** os campos dos 570 históricos e nomes não pertencentes ao worker. Não exige nem fabrica seus recibos originais. Um recibo novo no processamento de um ID já existente confirma a política idempotente da nova intenção, não comprova a aquisição histórica original.

Atualização posterior de históricos/enriquecimento requer política explícita e evidência de vínculo; não é liberada por essa automação. Não há seed de checkpoint por maior ID, contagem 570, data da chamada ou histórico importado. O spool/envelope novo conserva valores exatos da aquisição nova; o JSON histórico não foi usado como fonte corrente.

## Primeira aquisição após liberação

**Não existe comando que habilite origem corporativa nesta release.** SYNC_ENABLED=true continua recusado; adicionar senha ou trocar kind para fixture não é autorização. Permanecem ATTEMPTS_CORPORATE_CONTRACT_UNCONFIRMED, CORPORATE_TLS_CHAIN_AND_NAME_NOT_VALIDATED e RECOVERY_AFTER_EXPIRED_LEASE_OR_HARD_CRASH_REQUIRES_REVIEW, além das pendências já registradas.

Sequência operacional necessária:

1. Resolver cadeia/identidade TLS sem trustServerCertificate ou downgrade. Aprovar layout atual/colações/nulabilidade, escopo da PK/data, compartilhamento do consumo e recuperação com LogReadData/ReReadData, inclusive hard crash e lease expirada. Não tratar o desconhecimento de outro consumidor como prova de exclusividade.
2. Emitir uma capacidade de fonte revisada no transporte existente, vinculada à aprovação/configuração/contrato. A fábrica desta release não emite essa capacidade corporativa; sua adição é uma mudança específica de liberação, não uma reescrita de scheduler/persistência/projeção.
3. Fazer uma chamada manual autorizada, única, com scheduler parado, destino local validado, spool privado novo e plano fixo. Não repetir automaticamente em erro/timeout. Conferir selo completo, recibo de ingestão, outbox, recibos de destino e IDs preservados antes de autorizar recorrência. ResetReadData permanece proibida.
4. Após esse aceite, fornecer o binding autorizado à composição já implementada e iniciar o **mesmo** runtime/HTTP com SYNC_INTERVAL_MS=60000 (ou intervalo revisado), uma única instância. Até existir essa capacidade emitida, os comandos start/check/status acima continuam estritamente seguros e bloqueados.

## Parada e recuperação

Ctrl+C no terminal do start solicita parada graciosa. Para execução direta com PID próprio conhecido, SIGTERM tem o mesmo efeito; não usar pkill ou matar outro serviço na porta. AbortController cancela/drena o ciclo antes de fechar pools; o watchdog existente limita shutdown.

Em needs_reconciliation: parar, preservar spool/claims/recibos/checkpoints e inspecionar os estados. Nunca apagar claim ou repetir incremental para substituir o lote. Spool selado pode ser validado e reapresentado localmente pelo coordenador; parcial ou lease inválida exige revisão. Outbox incerta exige receipt com o mesmo digest; ausência não prova rollback remoto. Um novo processo verifica essas pendências **antes** de criar fonte, e não libera mais consumo por apenas reiniciar o worker.

## Validação desta entrega

- Typecheck e build aprovados.
- Testes direcionados: 37 aprovados em 9 arquivos, incluindo seis novos testes de automação e regressões HTTP/configuração/runner/scheduler/projeção/entrega TDS.
- O teste preexistente de dois PostgreSQL excedeu o default de 5 s na primeira execução; repetido com `--testTimeout=30000`, todos passaram. Os dois containers descartáveis deixados por esse timeout foram identificados por ID/label/horário e removidos; nenhum container persistente foi removido.
- Inicialização local real sem aquisição: live 200, ready 503, state blocked, PostgreSQL up/schemaReady=true, syncEnabled/sourceAuthorized=false, saída 0 no SIGTERM.
- Antes/depois: hash das 16 colunas de todas as linhas idêntico; 570 IDs únicos, 218 CPF, 264 e-mails, 3 campanhas e 16 tabulações. Zero entregas confirmadas e checkpoints vazios refletidos corretamente.
- Nenhuma leitura corporativa, migration no PostgreSQL local persistente/corporativo, alteração dos históricos, commit, push, deploy ou mudança de serviços externos. Migrations dos testes foram aplicadas somente nas fixtures descartáveis.

Comando dos testes direcionados:

```bash
npm test -- --testTimeout=30000 \
  test/local-automation.test.ts test/app.test.ts test/config.test.ts \
  test/attempts-cycle-runner.test.ts test/attempts-recurring.test.ts \
  test/scheduler.test.ts test/tds-projection.test.ts \
  test/tds-end-to-end.test.ts test/no-automatic-migrations.test.ts
```
