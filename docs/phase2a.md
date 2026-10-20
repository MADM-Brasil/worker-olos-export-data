# Persistência e recuperação — Fase 2A

## Escopo e evidência

A arquitetura HTTP, pools de diagnóstico, scheduler desativado, TLS, logs e shutdown da Fase 1 foram preservados. O novo adaptador é injetável e **não está ligado ao boot ou a qualquer endpoint**. `SYNC_ENABLED=false` continua obrigatório; não há runner de migrations ou executor de procedures.

O projeto continha somente os nomes das seis tabelas e das sete procedures, sem documentação ExportDataV3, metadados de colunas ou índices. Em 15/10/2026, as buscas por `"ExportDataV3" Olos`, `"AttemptsRawData" Olos` e `"usp_ExportData_LogReadData"` também não localizaram uma documentação oficial pública utilizável. Não foram usadas fontes de terceiros para inventar layouts. A URL/cópia oficial e os metadados da versão instalada continuam necessários.

| Origem | Campos, tipos e nulabilidade | Chave e escopo de unicidade | Estado |
| --- | --- | --- | --- |
| AttemptsRawData | Não confirmados | Não confirmados | Bloqueado |
| InboundRawData | Não confirmados | Não confirmados | Bloqueado |
| PbxBillingData | Não confirmados | Não confirmados | Bloqueado |
| Users | Não confirmados; Password sempre proibido | Não confirmados | Bloqueado |
| Campaign | Não confirmados | Não confirmados | Bloqueado |
| Disposition | Não confirmados | Não confirmados | Bloqueado |

Não foi declarada nenhuma PK da Olos. As PKs/UNIQUE desta entrega são contratos **internos do worker**: UUID de execução/lote, stream por tabela+tipo de leitura+data civil, versão de checkpoint e chave canônica de envelope condicionada à evidência de unicidade. Os tipos de leitura (`current_day`, `previous_days`, `reread`, `reference_snapshot`, `investigation`) são rótulos internos, não parâmetros afirmados das procedures.

## Migrations e permissões preparadas

- `migrations/001_control.sql`: schema isolado, versões, contratos da origem, streams, execuções, locks com geração/lease, lotes, checkpoints, erros, casos/tentativas de recuperação e auditoria.
- `migrations/002_operational_envelopes.sql`: seis tabelas internas de envelope (`attempts_raw_data`, `inbound_raw_data`, `pbx_billing_data`, `users`, `campaign`, `disposition`), com chave canônica, payload exato, versão do contrato e referência ao lote. Não são réplicas relacionais do layout Olos.
- `permissions/worker.sql`: template administrativo separado que cria role de grupo sem login e concede somente USAGE, SELECT e INSERT/UPDATE nos objetos necessários, além de EXECUTE na função local de validação do payload. Sem DELETE, TRUNCATE, CREATE, ownership, superuser, aprovação de contratos ou alteração do schema public.

As migrations são transacionais e numeradas, com registro em `schema_versions`. Não são scripts de convergência: reaplicar uma versão já aplicada falha, em vez de esconder drift com `IF NOT EXISTS`. Não há down migration destrutiva ou limpeza automática. Os scripts não alteram `public`, não criam extensões, não provisionam senha e não pertencem à imagem runtime.

**Aplicação futura exclusivamente manual**, após revisão por DBA, plano de backup/restauração e verificação de conexão/identidade do destino. Revisar 001, depois 002, e então o template de permissões. Usar `psql -X -v ON_ERROR_STOP=1 -f <arquivo>` em sessão explicitamente autorizada. Não existe script npm que aplique esses arquivos, nem hook em start/install/build. As únicas execuções feitas nesta fase foram contra PostgreSQL 16 descartável criado pelo helper de testes.

O login dedicado deve receber o role de grupo e CONNECT somente no banco escolhido, com senha provisionada fora do código. A ausência de privilégios no schema dedicado é controlada pelos grants preparados; privilégios herdados de outros roles/PUBLIC no banco existente precisam de revisão administrativa. Não se alega que este template remove permissões preexistentes em public.

## Preservação dos dados e aprovação dos contratos

Todos os contratos reais nascem `approved=false`, versão `pending`, sem campos ou chave. O runtime não possui permissão de aprová-los. Antes de autorizar uma versão, registrar `layout_evidence` e `key_evidence`, allowlist completa de campos e componentes da chave. A evidência deve comprovar unicidade em todo o escopo armazenado, incluindo partição/data/tenant se necessário; não basta observar ausência de duplicatas em uma amostra. A evidência deve também confirmar representação canônica e collation: o envelope não emula comparações case-insensitive do SQL Server ou equivalência entre offsets/representações textuais distintas. Mudanças na composição ou representação de chaves exigem revisão e migração específica, sem habilitar duas convenções incompatíveis sobre os mesmos dados.

`source_key` usa serialização canônica dos nomes/componentes aprovados, com collation C. Não é hash de conteúdo, sequência local, ID convertido para number, nem suposição sobre uma coluna `Id`. O ID original permanece também no payload. Alterações na allowlist precisam ser revisadas com o mapeamento de tipos e a coleta mínima de dados pessoais.

O envelope JSONB aceita somente strings, booleanos e null; bytes chegam como Uint8Array e são codificados em base64. BIGINT e DECIMAL são strings, e números JavaScript são rejeitados em campos numéricos exatos. BIGINT é validado no intervalo signed int64. Valores fora desse intervalo não são silenciosamente arredondados. Null permanece null; string vazia permanece string vazia.

Timestamps da origem são preservados em strings, distinguindo `local_timestamp` (sem offset) e `offset_timestamp` (offset explícito ou Z), com até sete dígitos fracionários. Não há conversão automática para Date, UTC ou timezone local. Isso evita a perda do sétimo dígito de datetime2(7) em uma coluna PostgreSQL com precisão de microssegundos. Esses validadores são capacidades do envelope, **não confirmação de tipos de nenhuma coluna da Olos**. Timestamps internos de auditoria usam timestamptz; `export_date` usa date e é uma data civil de controle.

Campos desconhecidos, ausentes ou de tipo incompatível rejeitam o lote integral. Não há descarte silencioso de colunas. Campos cujo nome normalizado contém password/passwd/senha são proibidos no contrato e no payload de todas as tabelas. O banco possui CHECK adicional e rejeita objetos/arrays aninhados e números JSON. Portanto `Users.Password` não tem destino permitido. Não há logs de registros, IDs de clientes, telefones, e-mails, erros brutos, stacks ou parâmetros SQL na nova camada; `sync_errors` armazena somente códigos enumerados e contagens.

## Protocolo de persistência

O adaptador `PostgresPersistence` recebe explicitamente um pool `pg`. Não cria conexões sozinho e não reutiliza implicitamente o pool somente leitura de diagnóstico. A futura composição deve fornecer pool de escrita com credenciais exclusivas, TLS, limites e timeouts validados. Os parâmetros `PERSISTENCE_*` são validados por Zod e podem ser traduzidos por `configuredPersistenceLimits(config)`; nada disso habilita o consumo.

1. `startRun`: verifica o registro das versões 001/002 já aplicadas (sem executar DDL), valida contrato aprovado, adquire exclusão por tabela em transação com row lock NOWAIT, verifica ausência de recuperação aberta, incrementa geração BIGINT e grava `read_intent` antes de qualquer futura leitura remota. Cria ou recupera stream/checkpoint. A lease cobre todas as datas/tipos de leitura da mesma tabela entre processos que usam este protocolo.
2. `checkpoint`: retorna versão string e cursor anterior. O cursor é opaco; o protocolo não presume que ele seja uma PK monotônica. `pkRange`, quando informado, é metadata declarada pelo chamador como componentes string; seu significado/ordenação da origem não são inferidos. `renew` renova somente posse ainda válida.
3. `observeBatch`: após uma leitura futura, grava o recibo da leitura em uma transação separada, sem guardar payload bruto nessa área de controle. Registra UUID estável do lote, digest SHA-256 do conteúdo normalizado+metadados, contagem recebida e expectativa de checkpoint. Um lote pendente por execução evita pular trabalho não confirmado.
4. `persistBatch`: exige lote observado e conteúdo idêntico, valida lease/geração/run/stream, confere versão **e cursor anterior**, deduplica a chave aprovada e executa UPSERT em parcelas. Dados, estado committed do lote, checkpoint CAS, contagens e auditoria são confirmados na **mesma transação e no mesmo cliente pg**. Um erro em qualquer parcela ou no checkpoint faz rollback integral. O row lock de posse é mantido até o COMMIT.
5. `finishRun`: exige pelo menos um lote, todos committed e nenhum rejeitado. Grava sucesso/horário final e último sucesso da execução no stream, liberando a posse na mesma transação. Um lote vazio confirmado pode representar uma leitura vazia, conforme a futura semântica validada.

O limite de parcelas controla o tamanho dos comandos; todas as parcelas de um lote lógico compartilham a mesma transação. Não há commit por parcela, DELETE ou TRUNCATE no adaptador. Valores usam parâmetros. O único identificador SQL interpolado é escolhido de um mapa fixo de seis destinos, nunca de entrada externa.

Contagens de lote confirmado: `received = persisted + duplicates + rejected`, com rejected=0. `persisted` conta chaves efetivamente inseridas/alteradas; `duplicates` inclui repetições iguais dentro do lote e registros já idênticos no destino. Duplicatas com mesma chave e conteúdo divergente no mesmo lote rejeitam o lote inteiro. Rejeição de validação anterior à observação fica em `sync_errors` vinculada ao run, sem payload. Falhas de persistência conhecidamente revertidas registram rejected=received; retry do lote observado pode confirmar todos os dados e zerar a rejeição corrente do lote, mantendo o erro histórico. Totais da execução são recalculados sem duplicar os recibos de lotes. Rejeições sem recibo contam por tentativa, não como uma identidade de registro inferida.

Reenvio de lote já committed devolve o recibo original, mesmo depois de nova execução, sem alterar os dados, contagens, auditoria ou checkpoint atual. Um batch ID com conteúdo diferente é erro de protocolo. Alterar um registro existente com UPSERT exige novo lote e checkpoint válido; reordenar lotes ou repetir um antigo com expectativa de checkpoint antiga é bloqueado. Um novo lote com expectativa atual pode conter registros sobrepostos, contados como duplicados; uma releitura remota continua exigindo planejamento de recuperação.

## Falhas, reinicialização e limites da garantia

- Falha antes da leitura: intent pode existir sem leitura remota. Sem evidência não se presume o que aconteceu.
- Falha depois da leitura, antes de `observeBatch`: intent permanece. O payload em memória pode ter sido perdido; o destino sozinho não reconstrói o intervalo lido.
- Falha após `observeBatch`, antes de commit: metadados do lote permanecem pendentes. Se a aplicação reteve o mesmo payload e o rollback é conhecido, pode repetir apenas a persistência local. Após crash, não gerar novo lote ou repetir a procedure automaticamente.
- Falha de aquisição/conexão antes de COMMIT: nenhum COMMIT é enviado pelo adaptador; erros são sanitizados, sem retry automático. Se o registro do erro também falhar, o intent/lote durável fica como evidência para recuperação.
- Perda da resposta de COMMIT: resultado **unknown**, mesmo que rollback posterior responda. A conexão é descartada. Se um novo acesso comprovar o mesmo lote committed, seu recibo pode ser consultado sem reexecutar writes. Se não houver comprovação, abrir caso e bloquear avanço; não converter incerteza em sucesso ou falha definitiva.
- `recoverExpired`: somente leases expiradas e não bloqueadas por transações ativas são recuperadas (SKIP LOCKED). Invalida a geração antiga, marca execução/lotes pendentes como needs_reconciliation e cria caso durável. Leitura já committed não é desfeita. Casos abertos bloqueiam novos runs da tabela. Renovação ou callback de run A não pode alterar nem liberar a posse de B.
- `recordRecoveryAttempt`: registra ação, resultado enumerado e digest de evidência, sem detalhes pessoais. Não resolve casos automaticamente e não executa ações remotas.

`src/sync/recovery.ts` define portas para investigação por `usp_ExportData_LogReadData` e planejamento de `usp_ExportData_ReReadData`. Os modelos são internos: não afirmam os parâmetros SQL reais. Nenhuma implementação, comando EXECUTE, reset ou chamada de rede foi adicionada.

A exclusão e fencing protegem escritas PostgreSQL dos participantes deste protocolo. Não garantem fencing remoto da Olos, atomicidade distribuída, recuperação de um payload perdido ou exactly-once. Um escritor fora do protocolo ou mudança administrativa de contrato/chave pode quebrar as premissas. Aprovações de contrato devem ocorrer com o consumo interrompido. A disponibilidade e retenção de LogReadData/ReReadData, inclusive em falhas entre controle remoto e commit local, ainda precisam de documentação e testes autorizados.

## Testes e próximos passos

`npm test` mantém os testes da Fase 1 e cria um PostgreSQL 16 descartável via Docker para a nova suíte. É necessário Docker acessível e acesso à imagem `postgres:16-bookworm` na primeira execução. Sem Docker, a suíte de integração falha explicitamente; não pula a validação nem usa um banco configurado na máquina como fallback. Nenhuma URL PG de produção é aceita pelo helper.

O helper gera senha efêmera, publica somente porta efêmera em 127.0.0.1, usa armazenamento tmpfs e remove seu próprio container no teardown. As migrations e o template de permissões são aplicados **somente por esse helper**, dentro do banco `phase2a_fixture` criado por ele. Reset/truncamento entre cenários existe somente nos testes desse banco descartável. O adaptador usa o role de privilégios limitados nos testes, não o role administrativo que aplica os fixtures.

A suíte verifica idempotência, duplicatas, UPSERT, rollback após parcelas e checkpoint, bigint/decimal/null/timestamps, limites, indisponibilidade, concorrência, fencing A/B, leases, checkpoints incompatíveis, crash/restart, tentativas de recuperação, commit sem confirmação nas duas possibilidades, permissões mínimas e ausência de execução automática de migrations/procedures. Isso comprova comportamento local simulado, não a semântica da Olos.

Antes da integração real: obter documentação oficial e metadados autorizados; definir allowlists e chaves com escopo comprovado; revisar schema relacional/índices e representação temporal; revisar/aplicar manualmente migrations e permissões em ambiente autorizado; validar identidade/retomada dos lotes remotos, retenção e parâmetros das procedures; testar falhas entre leitura e commit; definir decisão humana e critérios de fechamento de conciliação; só depois conectar extração e recalcular readiness. `SYNC_ENABLED` permanece bloqueado até essa entrega posterior.

Referências primárias para as decisões PostgreSQL: [transações no mesmo cliente pg](https://node-postgres.com/features/transactions), [ON CONFLICT](https://www.postgresql.org/docs/16/sql-insert.html), [row locks](https://www.postgresql.org/docs/16/explicit-locking.html) , [tipos temporais PostgreSQL](https://www.postgresql.org/docs/16/datatype-datetime.html) e [datetime2 SQL Server](https://learn.microsoft.com/en-us/sql/t-sql/data-types/datetime2-transact-sql?view=sql-server-ver16). Nenhuma delas substitui o contrato ExportDataV3.

## Resultado desta entrega

Em 15/10/2026, `npm run typecheck`, `npm run build` e `npm test` passaram. Foram aprovados 65 testes em 9 arquivos, preservando os 25 da Fase 1. Os containers PostgreSQL com o label exclusivo da suíte foram removidos pelo teardown. `npm audit --omit=dev` continua reportando os três alertas moderados preexistentes da cadeia mssql/tedious/sprintf-js; nenhuma dependência foi alterada nesta fase.

Criados: `migrations/001_control.sql`, `migrations/002_operational_envelopes.sql`, `permissions/worker.sql`, `src/persistence/{types,validation,transaction,store}.ts`, `src/sync/recovery.ts`, `test/persistence.test.ts`, `test/persistence-config.test.ts`, `test/transaction.test.ts`, `test/no-automatic-migrations.test.ts`, `test/support/postgres.ts` e este documento. Modificados: `src/config/env.ts`, `src/sync/contracts.ts` (comentários de contrato), `.env.example` e `README.md`. `dist/` foi gerado pelo build e continua ignorado. O diretório permanece sem repositório Git inicializado.

Nenhuma conexão com SQL Server/Olos ou PostgreSQL da empresa foi realizada; nenhuma procedure real, migration em produção, alteração de public em produção, extração, commit, push, deploy ou alteração de WireGuard foi executada. As migrations e o template de permissões foram efetivamente executados somente no PostgreSQL descartável de testes.
