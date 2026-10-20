# Fase 2B — projeção e entrega durável

Data: 15/10/2026. Estado: implementação preparatória validada em ambiente isolado. **Integração com a Olos e escrita corporativa ainda bloqueadas; não pronta para produção.**

## 1. Arquivos criados e modificados

Criados:

| Arquivo | Responsabilidade |
| --- | --- |
| `src/projection/contract.ts` | Catálogo versionado `raw.olos_calls/v1`, união de 41 campos e diferenças R1/R2. |
| `src/projection/transform.ts` | Transformação pura, valores exatos, bindings explícitos, autorização de referências e pendências. |
| `src/projection/capture.ts` | Snapshot e outbox usando o cliente da transação de ingestão; verificação de replay. |
| `src/projection/outbox.ts` | Claim concorrente, geração/lease, autorização de envio, estados e conciliação. |
| `src/projection/request.ts` | Digest canônico da requisição, incluindo destino lógico e versões. |
| `src/projection/dispatcher.ts` | Processamento de uma entrega, retry seguro e consulta de recibo. |
| `src/projection/postgres-target.ts` | Receptor PostgreSQL: escrita e recibo na mesma transação, deduplicação no destino. |
| `src/projection/writer.ts` | Preparação de SQL parametrizado e writer raw com execução bloqueada. |
| `migrations/003_projection_control.sql` | Ledger separado, manifests, snapshots/outbox e eventos de projeção. |
| `migrations/004_delivery_receipts.sql` | Ledger e recibos no banco receptor, sem DDL de negócio. |
| `permissions/projection.sql` | Grants separados para ingestão e projetor; snapshots sem UPDATE para esses roles. |
| `permissions/receiver.sql` | Role de recibos sem privilégios raw, DELETE ou UPDATE de recibos. |
| `test/projection-transform.test.ts` | Contratos, valores, pendências, autorização e bloqueio de escrita raw. |
| `test/projection-delivery.test.ts` | Transações, concorrência e falhas entre dois PostgreSQL descartáveis. |
| `test/support/projection-plan.ts` | Plano exclusivamente sintético, sem mapeamento Olos real. |
| `docs/phase2b-implementation.md` | Este relatório. |

Modificados:

- `src/persistence/store.ts`: terceiro argumento opcional com configuração de captura, gravação atômica da intenção e validação de captura em replay committed.
- `src/persistence/types.ts`: código de erro `PROJECTION_CAPTURE_MISSING`.
- `README.md`: referência à Fase 2B e ao uso de bancos descartáveis nos testes.

Não foram alterados dependências, lockfile, configuração, boot HTTP, scheduler, pools de diagnóstico ou regras de TLS. Os arquivos de testes/suporte existentes e as migrations 001/002 foram preservados integralmente, conferidos por SHA-256. O build gerou `dist/` ignorado.

## 2. Contrato implementado e bloqueios

O catálogo registra os 23 campos e tipos declarados em R1 e os 33 nomes de R2, totalizando 41 nomes distintos. Cada campo contém evidência de presença nas referências, tipo R1 quando fornecido, tipo R2 desconhecido, ownership candidato e necessidade de autorização. A exceção `data_carga` fica marcada como presente somente no SELECT mencionado de R1, sem definição de tipo nesse modelo.

Não se inferem defaults, nulabilidade, FKs, tipo de coluna exclusivo de R2, geração de PK, equivalências de nomes ou normalizações. Não há UNIQUE criado para olos_call_id/internal_id_olos. Os três IDs permanecem distintos.

`physicalDdlConfirmed` e `callIdentityConfirmed` são false no catálogo congelado. Não há variável de ambiente, argumento do dispatcher ou `WriteApproval` sintético que libere esse gate. Os contratos de origem reais continuam `pending` nas migrations existentes. Não foi criado um mapeamento real aprovado da Olos.

### Função pura

`projectCall(source, references, plan)`:

- Exige plano versionado e evidência explícita; nenhum binding, join, lookup ou cálculo é implícito.
- Preserva o objeto original permitido em cópia congelada e não modifica a entrada. Aceita valores planos exatos string/boolean/null; rejeita Date, number, bigint JS, undefined como valor e objetos aninhados. IDs numéricos devem chegar como strings exatas.
- Diferencia atributo ausente, null, valor, campo protegido e referência não resolvida. Campos de origem não mapeados/explicitamente ignorados causam pendência, sem virar colunas SQL.
- Exige prova de identidade e granularidade, componentes da chave de origem presentes e raw_olos_call_id válido para produzir `row`. Não deriva esse ID de olos_call_id/internal_id_olos.
- BIGINT/INTEGER são validados em string e no intervalo conhecido de R1. TEXT mantém zeros, case e conteúdo; não há coerção numérica de IDs textuais.
- TIMESTAMP candidato aceita somente representação civil válida com até seis dígitos fracionários. Offset, fração superior ou formato inválido produzem pendência `TEMPORAL_POLICY_REQUIRED`; não há truncamento ou inferência de timezone. O snapshot original pode preservar sete dígitos, sem forçar sua projeção para TIMESTAMP.
- Não calcula CPF, telefone normalizado, match key, e-mail, TMA, vínculos ou IDs CRM. Valores locais calculados devem vir de referência com regra/evidência autorizada. Campos locais/protegidos exigem autorização de ownership; não existe normalizador oculto.
- Campos de senha são rejeitados antes de qualquer uso. Tipos desconhecidos de R2 não são inventados para preencher uma linha.

`status: ready` significa **candidato do mapeamento fornecido**, não prontidão física de raw. Pode haver campos não exigidos pelo plano ainda ausentes/protegidos, explicitados em `fields` e `issues`. `requiredFields` descreve a completude exigida pelo plano, não nulabilidade do banco. O writer mantém o gate físico independente. `pending`/`rejected` não incluem `row`.

As evidências são atestações explícitas da configuração autorizada do chamador, não uma validação automática da documentação da empresa. Não existem endpoints para fornecer/aprovar planos arbitrários.

## 3. Captura transacional durante ingestão

O modo compatível com a Fase 2A continua funcionando sem migrations de projeção e sem argumentos novos obrigatórios. Para composição futura da Fase 2B, o construtor recebe explicitamente um `CaptureConfig` com namespace estável da origem, destino lógico e versão de mapeamento. O boot não fornece essa configuração nem instancia a persistência.

Quando a captura está configurada, `persistBatch` grava na mesma transação e no mesmo cliente pg:

1. UPSERT dos envelopes existentes.
2. Manifest do lote e snapshot de cada chave única observada, inclusive registros iguais ao envelope já existente.
3. Intenção pendente com UUID de entrega persistido uma única vez.
4. Checkpoint CAS, recibo committed do lote, contagens e auditoria existentes.

Falha na captura reverte também envelopes e checkpoint. Não há chamada de rede ou de destino dentro dessa transação. A integração segue o requisito de [usar o mesmo cliente pg para a transação](https://node-postgres.com/features/transactions).

O manifest registra namespace/tabela/contrato de origem, definições de campos, destino lógico, versões, digest da configuração, digest do snapshot e contagem. A referência ao batch permite chegar a run/stream/data/tipo de leitura originais. Cada entrega contém chave e payload aprovado preservados. O snapshot fica separado do envelope mutável; alterações posteriores desse envelope não apagam a versão capturada.

Lote vazio também possui manifest. Duplicatas iguais no mesmo lote geram um snapshot por chave. Um novo lote pode voltar a observar a mesma chave, criando outra intenção: isso não é prova de nova chamada. Deduplicação da entidade de negócio depende da identidade aprovada, além da deduplicação de transporte por delivery ID.

Replay de batch já committed verifica o manifest e não recria entregas. Lote legado committed sem captura retorna `PROJECTION_CAPTURE_MISSING` no modo de projeção; não reconstrói silenciosamente o histórico a partir do último envelope. Mudança de destino/namespace/mapeamento no replay também não cria nova intenção. Backfill e reprojeção com nova versão exigem um procedimento futuro explícito.

O namespace adicional registra proveniência, mas não altera a chave dos envelopes da Fase 2A: ainda não autoriza misturar várias instâncias Olos com IDs potencialmente iguais no mesmo conjunto de envelopes.

## 4. Entrega, retry, concorrência e reinicialização

A outbox usa os estados `pending`, `confirmed`, `rejected` e `uncertain`, com eventos duráveis sem payload/erro bruto. Contagens/checkpoint da origem ficam independentes da confirmação do destino.

### Claim e fencing

`claim` escolhe uma entrega elegível por destino lógico com `FOR UPDATE SKIP LOCKED`, incrementa geração BIGINT e grava owner UUID, lease e tentativa. O mecanismo é voltado a uma fila, conforme a semântica de [SELECT no PostgreSQL](https://www.postgresql.org/docs/16/sql-select.html). Operações posteriores exigem ID/owner/geração e lease válida; uma instância antiga não libera nem autoriza o trabalho da nova.

Um claim expirado ainda `pending` pode ser retomado, pois nenhum envio foi autorizado. Antes de chamar o destino, `authorize` congela a requisição e grava `uncertain` com attempt ID e digest, confirmando essa autorização em transação local. Só após receber o ACK desse COMMIT o dispatcher pode chamar o destino.

Se o COMMIT da autorização não for confirmado, não há chamada ao destino. Se o banco tiver commitado, o estado durável já bloqueia outro envio. `uncertain` não é elegível para novo claim, mesmo quando a lease expira. Não existe timeout visual/lease que libere um transporte ainda pendente.

### Retry conhecido e resultado incerto

- Falha comprovadamente anterior ao commit do destino, classificada pelo adaptador SQL transacional, volta a pending com backoff configurável. Reusa o delivery ID e o snapshot da requisição, sem remapear referências alteradas.
- Falha genérica, pós-envio, COMMIT sem ACK ou falha de confirmação local permanece incerta. Não é convertida em “não enviado”.
- Recibo positivo compatível com delivery ID/digest permite confirmar/rejeitar a entrega após reinicialização, sem repetir escrita.
- Ausência de recibo ou indisponibilidade da consulta não prova que um envio antigo não possa terminar: a entrega continua uncertain, com evento/código de diagnóstico. Não há reset/retry automático de incerteza por ausência.
- Se a lease expirar antes de registrar o ACK, o callback antigo não força atualização local. A conciliação por recibo trata o resultado posteriormente.

Pending sem envio pode ser recuperado por outra instância após expiração. Pending com mapeamento/contrato bloqueado continua pendente, sem autorização de envio. Rejected é terminal nesta implementação. Não foi criada API que resolva manualmente ambiguidades por suposição ou reabra entregas incertas.

Os limites de lease/retry são validados nos construtores (defaults de 60s); a instância do dispatcher executa um item por `runOnce`. Não foi ligado um novo laço ao scheduler ou ao boot. A futura composição deve controlar cadência, monitoramento, shutdown e aprovações antes de qualquer consumo real.

## 5. Receptor PostgreSQL e proteção de raw

`PostgresDeliveryTarget` recebe pool explícito e um `TransactionalBusinessWriter`. O pool de diagnóstico somente leitura não é convertido em pool de escrita. Não há conexão de destino descoberta por ambiente/fallback nesta camada.

No receptor, o delivery ID e destino lógico identificam um recibo único, vinculado a digest e versões. O recibo e `writeInTransaction` são confirmados no mesmo COMMIT. Concorrência/replay do mesmo delivery ID não executa a escrita novamente; conteúdo/versões incompatíveis não substituem o recibo existente. Falha do writer faz rollback do recibo e da escrita. Perda do ACK do COMMIT retorna unknown, mesmo que um rollback posterior responda.

O business writer desse protocolo deve executar **somente SQL no cliente fornecido**, sem commit próprio ou efeitos externos. O protocolo não oferece transação distribuída: origem, receptor e ACK da outbox são transações separadas. Os testes usam dois servidores PostgreSQL diferentes para exercitar essa separação.

`RawCallsWriter` implementa esse writer com:

- SQL parametrizado com identificadores escolhidos da lista fixa de campos conhecidos, nunca de entrada SQL externa.
- Allowlist explícita por coluna, evidência de DDL/identidade, autorização de ownership e nulabilidade declarada pelo aprovador do plano.
- Proibição de preencher colunas R2 sem tipo conhecido, inclusive Kommo/data_carga, por inferência.
- Gate físico/identidade false verificado antes de qualquer `client.query` raw. Mesmo uma aprovação sintética de SQL não o libera.

`prepareRawStatement` pode produzir um plano SQL revisável sob atestações explícitas; isso não aprova o DDL da empresa nem executa o plano. A PK de conflito usada nesse plano é somente a declarada em R1, ainda condicionada ao gate físico. Não há conflito presumido em olos_call_id/internal_id_olos. Defaults, geração de IDs, regras de atualização e precedência de versões reais não foram implementados por adivinhação.

As gravações de negócio nos testes usam apenas `olos_export.fixture_calls`, com DDL sintético e writer próprio de teste. Nenhuma tabela raw.olos_calls, nem mesmo uma cópia supostamente definitiva em fixture, foi criada por esta entrega.

## 6. Migrations e permissões preparadas

| Arquivo | Aplicação futura e limites |
| --- | --- |
| 003_projection_control.sql | Manual no banco de ingestão após 001/002. Cria somente controles, snapshots e eventos em olos_export. Ledger `projection_schema_versions=[1]`; ledger antigo continua `[1,2]`. |
| 004_delivery_receipts.sql | Manual no receptor, com namespace olos_export já preparado pelo DBA. Não exige tabelas de negócio específicas, não cria schema raw e usa ledger próprio `delivery_schema_versions=[1]`. Pode coexistir no banco de ingestão, mas não pressupõe que os dois destinos sejam o mesmo database. |
| permissions/projection.sql | Ingestor insere manifests/snapshots; projetor consulta e altera somente colunas operacionais da entrega e insere eventos. Nenhum desses grants permite alterar snapshots já inseridos. |
| permissions/receiver.sql | Role de grupo sem login para SELECT/INSERT de recibos; privilégios de negócio devem ser aprovados separadamente. Sem raw, DELETE, UPDATE de recibos ou DDL. |

As constraints novas são contratos internos justificáveis por identidade de entrega/estado de processamento e referências ao batch existente; não são constraints inferidas dos 41 campos de negócio. Aprovação corporativa, login, CONNECT e secrets continuam manuais. Não há runner de migrations nem hooks de instalação/start/build para aplicá-las.

Hashes de preservação verificados contra a baseline anterior às alterações:

```text
001_control.sql
03269ebe6cb1cba994d08d54e85cf66a90b09fa841eaec898908bff9655acd37
002_operational_envelopes.sql
6358bf82b70423b4eabb93d27749af1aef2749a5628bafaa20a50980576b7fab
```

## 7. Testes e resultados

| Comando final | Resultado |
| --- | --- |
| `npm run typecheck` | Aprovado, exit 0. |
| `npm run build` | Aprovado, exit 0. |
| `npm test` | Aprovado: **120 testes em 11 arquivos**, exit 0. |

Os 65 testes anteriores foram mantidos sem alterações. Foram adicionados 55 testes/casos parametrizados, incluindo:

- União/diferenças dos 41 campos; tipos/nulabilidade desconhecidos; bindings e referências autorizadas; proteção de campos externos/calculados e Password.
- Ausente/null, BIGINT acima de MAX_SAFE_INTEGER, TEXT com zeros/case, extremos/valores inválidos e rejeição de Date/number.
- Timestamp civil exato; offset/sétimo dígito/formato inválido como pendência, sem perda do snapshot de origem.
- Identidade sem prova não produz row; nenhuma equivalência implícita dos IDs, leads ou medidas.
- Plano SQL parametrizado, colunas restritas e nenhum client.query raw mesmo com aprovação sintética.
- Atomicidade envelope/outbox/checkpoint, rollback na captura, lotes vazios/duplicados e preservação de versões anteriores.
- Replay committed sem novas entregas, lote legado sem captura bloqueado e crash simulado após ingestão.
- Destino bloqueado/indisponível, retry congelado, confirmação e rejeição, múltiplos projetores e fencing A→B.
- COMMIT sem ACK na ingestão, autorização, receptor e confirmação local, simulando commit efetuado e não efetuado.
- Transporte remoto ainda pendente após TTL, recibo ausente sem reenvio e conciliação por recibo após reinicialização simulada.
- Receptor concorrente/idempotente, rollback conjunto de escrita/recibo e grants mínimos dos novos roles.

As reinicializações são simuladas por novas instâncias dos componentes sobre estado PostgreSQL persistido; não representam ensaio de falha física de armazenamento ou de infraestrutura corporativa. Os testes de contrato/driver da Olos real continuam pendentes.

## 8. Evidências de isolamento e operações não realizadas

- A suíte usa o helper existente `test/support/postgres.ts`, preservado: host obrigatório 127.0.0.1, porta obtida do container criado pela própria chamada, banco fixo de fixture e senha efêmera gerada. Não aceita URL externa e não usa PG_HOST corporativo como fallback.
- Os novos testes abrem containers exclusivos para origem e receptor. O fixture legado dos 65 testes também é separado. Armazenamento tmpfs, porta publicada somente no loopback e remoção dos IDs próprios no teardown.
- 003/004 e templates foram executados apenas nesses bancos descartáveis, além das migrations originais usadas pelo helper. Grants de negócio adicionais são exclusivos da tabela sintética dos testes.
- Ao final da rodada, consulta somente de leitura aos containers com o label da suíte não encontrou containers restantes.
- Comparação por hashes confirmou que todos os arquivos anteriores de testes/suporte e 001/002 ficaram intactos. Nenhuma asserção antiga foi removida/enfraquecida para acomodar a implementação.
- `src/app.ts`, `src/main.ts`, `src/config/env.ts`, pools e scheduler não foram alterados. `SYNC_ENABLED` permanece false por padrão e true continua rejeitado. A readiness não anuncia consumo disponível.
- Nenhuma conexão Olos, procedure, extração real, INSERT corporativo, migration em banco real, modificação da discadora/WireGuard, commit, push ou deploy foi executado.

## 9. Pendências antes da conexão real

1. Obter layout oficial ExportDataV3, tipos/chaves/escopos e semântica das procedures, inclusive recuperação. Nenhum contrato de origem real foi aprovado aqui.
2. Validar aquisição exata no driver antes de Date/number/clone, especialmente DECIMAL e frações/offset temporais. O novo código rejeita dados inexatos; não reconstrói precisão já perdida.
3. Confirmar DDL, SELECTs e divergência de data_carga; granularidade de chamada; identidade/deduplicação e atribuição de raw_olos_call_id. Registrar contrato físico/versão aprovado, sem simplesmente trocar os flags false.
4. Aprovar bindings, fontes, ownership, normalização, matching, TMA, referências e política temporal/de correções. Definir nulabilidade, campos obrigatórios, defaults e estratégia de atualização sem tocar estado CRM externo.
5. Planejar ordem de aplicação por revisão de negócio: checkpoint da origem/ordem de chegada não prova revisão cronológica da chamada. As leases de ingestão não resolvem concorrência entre fontes projetando a mesma chamada.
6. Definir reprojeção/backfill de versões e tratamento operacional de uncertain sem recibo, preservando payload e identidade de entrega. Não preencher gaps legados usando somente o último envelope.
7. Revisar manualmente migrations/grants/pools TLS e coordenar migração futura entre bancos. Locks de um banco não cercam um worker no outro; cutover precisa parar o escritor antigo, transportar estado consistente e conciliar pendências.
8. Conectar a composição de captura/projeção a um lifecycle autorizado, com shutdown, métricas e critérios de readiness apropriados. O modo legado opcional da Fase 2A não deve ser escolhido por engano para um fluxo que exija entrega durável.

A camada está preparada e testada localmente para esses passos. **Não há autorização nem contrato suficiente para ativar a integração de produção.**
