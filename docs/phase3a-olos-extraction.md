# Fase 3A — integração preparatória com Olos ExportDataV3

Data: 15/10/2026. **Consumo real desabilitado; contratos reais ainda não aprovados. Não pronta para produção.**

## 1. Escopo e evidências disponíveis

Foram lidos os relatórios da Fase 2A, da auditoria e da implementação 2B, as quatro migrations, o protocolo de persistência e os testes existentes. A baseline de 120 testes passou antes da nova suíte. Não foram alterados seus arquivos ou as implementações anteriores.

A empresa informou os nomes das seis chaves abaixo como documentados pelo manual. Isso confirma esses nomes para o catálogo desta fase; **o manual completo, sua edição e as assinaturas SQL não estão disponíveis no projeto**. Não foram deduzidos tipos, nulabilidade, escopo global, monotonicidade, parâmetros, relacionamentos ou limites da origem.

A projeção `raw.olos_calls`, o banco definitivo e a ativação do scheduler estão fora desta entrega. Os contratos/proteções da Fase 2B permanecem intactos.

## 2. Arquivos

| Criado | Responsabilidade |
| --- | --- |
| `src/olos/contracts.ts` | Seis contratos individuais versionados, sete nomes de procedure, chaves informadas e pendências. |
| `src/olos/types.ts` | Parâmetros, datasets/metadados, respostas, transporte e erros sanitizados. |
| `src/olos/client.ts` | Preparação tipada mssql, cliente injetável, timeout/cancelamento com drenagem, limites, backoff e transporte nativo bloqueado. |
| `src/olos/mssql-collector.ts` | Coleta streaming por ordinal, múltiplos datasets, limites durante recepção e rejeição de tipos destrutivos. Não inicia requisições. |
| `src/olos/adapter.ts` | Validação de contrato/metadados e adaptação pura para os envelopes exatos da Fase 2A. |
| `src/olos/extraction.ts` | Coordenação de intent, leitura, observação, persistência e checkpoint; backpressure, exclusão e retomada local. |
| `src/olos/recovery.ts` | Investigação LogReadData sintética e planejamento ReReadData, sem executor de recuperação real. |
| `test/olos-client.test.ts` | Contratos, parâmetros, bloqueios, OUTPUTs, falhas, timeout, cancelamento, reconexão e volume simulado. |
| `test/olos-mssql-collector.test.ts` | Eventos do Request sem pool; metadados, precisão, erros, cancelamento e streaming limitado. |
| `test/olos-adapter.test.ts` | Seis tabelas, identidades, valores exatos, null, drift, duplicatas e limites. |
| `test/olos-extraction.test.ts` | Integração de respostas sintéticas com PostgreSQL 16 descartável e controles existentes. |
| `test/support/olos.ts` | Assinaturas, layouts, registros e semântica exclusivamente sintéticos. |
| `docs/phase3a-olos-extraction.md` | Este relatório. |

Modificado: `README.md`, somente para referência à Fase 3A.

Não foram modificados código de boot/configuração/pools de diagnóstico, scheduler, persistência, projeção, permissões, dependências, lockfile ou testes anteriores. `dist/` é saída ignorada de build.

## 3. Procedures e parâmetros

Todos os nomes são selecionados de uma allowlist fixa no schema dbo. Não há montagem de EXEC por concatenação, query SQL fornecida pelo chamador, SQL de descoberta ou inferência de assinatura.

| Procedure | Implementação preparatória | Execução real nesta fase |
| --- | --- | --- |
| `usp_ExportData_ReadData` | Chamada tipada pelo cliente/transporte; casos sintéticos. | Bloqueada. |
| `usp_ExportData_ReadDataCurrentDay` | Mesmo protocolo; casos sintéticos, sem semântica incremental presumida. | Bloqueada. |
| `usp_ExportData_ReadDataPreviousDays` | Mesmo protocolo; casos sintéticos. | Bloqueada. |
| `usp_ExportData_LogReadData` | Cliente e adaptador de investigação com classifier explicitamente contratado. | Bloqueada. |
| `usp_ExportData_ReReadData` | Cliente testado com mocks e planejamento de recuperação auditado. O planejador não invoca a procedure. | Bloqueada. |
| `usp_ExportData_ResetReadData` | Nome catalogado; preparação/invocação recusadas inclusive no cliente sintético. | Bloqueada. |
| `usp_ExportData_DataAvailable` | Nome catalogado; preparação/invocação recusadas inclusive no cliente sintético. | Bloqueada; semântica desconhecida. |

`ProcedureSignature` exige versão, evidência, nomes/direções/tipos explícitos e associação dos OUTPUTs Message/ErrorMessage. Não há assinatura real default. Os nomes `FixtureTable` e `FixtureAfter`, tamanhos de OUTPUT e returnValue=0 dos testes **não são documentação da Olos**.

O preparador usa `request.input(name, sqlType, value)` e `request.output(name, sqlType)`, sem inferência de tipo do driver. Suporta por enquanto NVARCHAR/VARCHAR com tamanho explícito, BIGINT, INT e BIT. BIGINT permanece string signed int64; somente INT validado em 32 bits é convertido para number. VARCHAR de entrada aceita apenas ASCII até confirmação da codepage. Outros tipos de parâmetro exigem codec/revisão específicos, principalmente DECIMAL e parâmetros temporais. Nomes, duplicatas sem distinção de case, extras, ausências e valores fora do tipo/intervalo são recusados antes da conexão.

`prepareMssqlRequest` apenas prepara um Request, sem executar. O caminho nativo preparado usa streaming, modo de linhas por ordinal e o método execute vinculado ao Request com o nome da allowlist. **`BlockedMssqlTransport.gate()` sempre lança antes da construção do pool e antes de qualquer RPC**; adicionalmente, `OlosProcedureClient` recusa transportes de tipo mssql antes de conectar. Não existe variável ou argumento de aprovação que libere esses gates.

O executor nativo permanece atrás desses bloqueios e não foi ensaiado com servidor SQL Server. O coletor e a preparação do Request são exercitados sem pool/socket. Sua presença não comprova extração real funcional. Uma composição futura deverá usar pool dedicado com as opções centralizadas existentes, TLS e timeouts revisados; o pool somente diagnóstico não é reaproveitado. node-mssql 11 não fornece timeout por Request na API instalada: o cancelamento do cliente deve coexistir com requestTimeout/connectionTimeout do pool futuro.

### Respostas e falhas

- Datasets, metadados e returnValue são capturados separadamente dos OUTPUTs. Message/ErrorMessage contratados ficam somente na resposta em memória; outputs adicionais não contratados não são expostos.
- O coordenador rejeita ErrorMessage não vazio e returnValue não incluído explicitamente no plano sintético/autorizado. Não interpreta texto de Message como sucesso ou como regra de paginação.
- O cliente captura múltiplos datasets. A ingestão desta versão admite **um dataset associado explicitamente ao contrato** e rejeita respostas extras; não descarta datasets desconhecidos. Mais associações exigem o manual.
- Erros de conexão/execução/timeout são códigos sanitizados, sem mensagens, parâmetros ou cause do driver. Nenhum logger foi adicionado a esta camada.
- Falha de conexão anterior ao envio é `not_sent`; erro/timeout/cancelamento após dispatch é `unknown`, sem tentativa automática da procedure. Esse outcome remoto é distinto de `not_committed` do PostgreSQL.
- Reconexão só ocorre em nova chamada explícita, após backoff mínimo configurável; não repete a chamada anterior. Close e nova chamada são recusados enquanto o transporte anterior estiver pendente.
- Timeout e AbortSignal pedem cancelamento, mas não fazem Promise.race liberar a exclusão. O cliente aguarda o término de invoke. O transporte deve respeitar também timeout/cancelamento nativos; se não terminar, permanece ocupado. Integração ao shutdown operacional ainda exige a composição futura.

## 4. Contratos das seis tabelas

| Contrato | Nome de chave informado como documentado | Tipos/layout reais | Aprovação |
| --- | --- | --- | --- |
| `AttemptsRawData/v1` | `AttemptsRawDataID` | Pendentes. | false |
| `InboundRawData/v1` | `InboundRawDataID` | Pendentes. | false |
| `PbxBillingData/v1` | `PbxBillingDataId` | Pendentes. | false |
| `Users/v1` | `UserId` | Pendentes; Password excluído. | false |
| `Campaign/v1` | `CampaignId` | Pendentes. | false |
| `Disposition/v1` | `DispositionId` | Pendentes. | false |

Cada contrato registra o nome informado e sqlType/nullable=null, versão própria, exclusões e lista de pendências. A evidência é identificada como informação da empresa na solicitação 3A; não se declara consulta independente ao manual. CallId/CallIdTelecom ficam catalogados como nomes de correlação, sem atribuição a tabela ou uso em chave.

Esses catálogos não aprovam os contratos persistidos em source_contracts e não modificam as migrations. Os contratos reais continuam pending/approved=false. `ValidatedTableContract` precisa fornecer layout completo, tipos, precisão/escala, aquisição exata e evidência de chave antes de aceitar um dataset. A chave informada deve fazer parte dos componentes aprovados; seu nome sozinho não comprova unicidade no escopo armazenado.

Campos FixtureText/FixtureAmount/FixtureLocal/FixtureOffset e os tipos das chaves nos testes são sintéticos. Não são campos oficialmente mapeados. Não foram implementados joins, equivalências entre PK e correlações ou normalizações.

### Preservação e fronteira do driver

A inspeção local do driver instalado confirmou os riscos já descritos na auditoria 2B: Tedious retorna BIGINT como string, mas realiza aritmética number em readNumeric; readDateTime2 cria Date e acrescenta nanosecondsDelta não enumerável; readDateTimeOffset não conserva o offset original no Date devolvido. Código inspecionado: `node_modules/mssql/node_modules/tedious/lib/value-parser.js`, além dos metadados/eventos de `node_modules/mssql/lib/tedious/request.js`. Não foram alterados dependências, handlers globais ou node_modules.

Decisões implementadas:

- BIGINT exige string exata no intervalo signed int64, inclusive acima de MAX_SAFE_INTEGER. Não passa por number; IDs TEXT conservam zeros, case e conteúdo segundo contrato explícito.
- DECIMAL exige string exata, validada contra precisão/escala declaradas e sem notação exponencial. O adaptador não faz String(number) para reconstruir um decimal.
- O coletor nativo recusa metadados DECIMAL/NUMERIC e temporais do parser padrão antes de aceitar suas linhas. Tipos desconhecidos, como FLOAT/MONEY/DATE/TIME nesta versão, também são recusados, em vez de convertidos por suposição.
- O adaptador só aceita decimal/temporal quando o transporte declara `exact-text/v1` com evidência de aquisição **anterior à conversão destrutiva**. Essa representação é testada sinteticamente; não existe ainda um transporte real certificado para produzi-la.
- Date, BIGINT numérico e decimal numérico são rejeitados antes de clone/persistência. INT/SMALLINT/TINYINT nativos são convertidos somente dentro dos intervalos inteiros comprovados desses tipos SQL.
- Timestamp civil mantém ausência de offset; timestamp com offset mantém o offset/Z e até sete dígitos, sem Date, ajuste UTC, inferência de timezone ou redução a microssegundos. Metadados e calendário são validados.
- Null, vazio e atributo ausente são distintos; ausente não vira null. Tipos, campos e metadados desconhecidos rejeitam o lote. Strings não são normalizadas.
- Bytes são copiados para Uint8Array e seguem o codec base64 já existente na persistência. O coletor usa ordinais para não mesclar nomes duplicados; duplicatas de nomes são recusadas.
- Password e variantes sensíveis são removidos dos metadados/linhas antes da resposta pública do cliente/coletor; o contrato e o envelope continuam proibindo esses campos. Não se afirma que uma procedure que os devolva deixará de transmiti-los pela rede: uma estratégia de exclusão na origem dependeria da assinatura real.

**A aquisição exata real de DECIMAL e tempo continua bloqueante.** Declarar um number/Date previamente arredondado como texto exato não recupera os dados. Não foi criado wrapper SQL, CAST em procedure ou patch de parser sem documentação/autorização.

## 5. Ingestão, exclusão e backpressure

`OlosExtraction` recebe separadamente o cliente de procedures e uma porta estrutural implementada por PostgresPersistence. O adaptador de origem não importa pg nem escreve SQL de destino; o coordenador não altera o protocolo transacional existente.

Sequência preparada/testada:

1. Validar contrato, versão e evidências do plano; obter exclusão local por tabela.
2. Confirmar startRun/read_intent no PostgreSQL e consultar checkpoint antes de qualquer leitura.
3. Renovar a lease válida e executar uma chamada pelo cliente injetado.
4. Verificar OUTPUTs/returnValue, associação do dataset, tipos, valores e limites.
5. Renovar a posse novamente. Lease expirada/cancelamento impedem persistência e novo consumo.
6. Gerar UUID de lote, observar o mesmo payload e persistir por observeBatch/persistBatch.
7. Confirmar versão+1 e cursor do checkpoint após commit, antes de qualquer nova chamada.
8. Encerrar o run somente quando o plano explícito indicar término e todos os lotes estiverem committed.

O lock/geração durável da Fase 2A cobre a tabela entre instâncias que usam o mesmo banco de controle, inclusive datas/tipos de leitura diferentes. Um Set local e a exclusão de transporte complementam esse controle. O cliente serializa suas chamadas; uma composição futura deve serializar o uso do cliente compartilhado ou fornecer clientes dedicados por fluxo. Não há paralelismo ou fila ligados ao boot.

Os limites de registros/bytes são configuráveis no cliente/adaptador/persistência. O coletor limita recepção streaming, pede cancelamento ao exceder o teto e ignora eventos de linhas posteriores até drenagem. Os defaults do caminho nativo preparado são conservadores, 1000 registros/4 MiB; compatibilização desses limites com pool e plano pertence à ativação revisada. A resposta remota deve caber integralmente no lote lógico: não se confirma checkpoint de uma fração por inferência. As parcelas SQL seguem na mesma transação da Fase 2A.

Sem repetition, há exatamente uma chamada: nem resposta vazia, nem tamanho do lote, nem maior PK são usados para reler ou declarar esgotamento da origem. Repetição exige evidência própria, função explícita de continuação, cursores sem ciclos e teto de chamadas. Contrato sem progresso ou teto atingido interrompe o run para investigação, sem consumir a próxima página. Lotes anteriores confirmados permanecem preservados; a resposta problemática pode ainda estar apenas em memória.

readTypeEvidence associa a chamada ao rótulo interno de leitura da Fase 2A. Não há associação oficial inferida por nome da procedure. Cursores são opacos; ordem numérica/monotonicidade/intervalo de PK precisam de evidência separada.

O fluxo é deliberadamente opt-in por composição de objetos e não foi conectado ao HTTP, scheduler ou configuração de ambiente. Falhas não chamam finishRun, não liberam silenciosamente a posse, não avançam checkpoint por conta própria e não repetem a leitura Olos.

## 6. Recuperação e limites das garantias

Não há transação distribuída SQL Server/PostgreSQL. A exclusão local não é fencing na Olos e não controla outros consumidores do ExportDataV3.

| Janela | Tratamento preparado |
| --- | --- |
| Antes da confirmação de startRun | Zero leitura; falha do controle impede o RPC. |
| Depois do intent, antes/durante leitura | Intent conservador permanece. Erro após dispatch é incerto. |
| Depois da leitura, antes de observeBatch | Payload pode se perder em crash; intent não reconstrói seus registros. Exige recuperação da origem validada. |
| Depois de observeBatch, antes do commit | Metadados do lote duráveis, dados podem estar apenas em memória. Retry local somente com mesmo lote/payload retido e posse válida. |
| COMMIT PostgreSQL sem ACK | Nenhuma nova leitura. Mesmo batch ID pode consultar recibo/repetir somente persistência idempotente; ausência de comprovação exige conciliação. |
| Reinicialização/lease expirada | recoverExpired marca needs_reconciliation, invalida geração e abre caso que bloqueia novos runs. Não relê nem resolve automaticamente. |
| Outro consumidor avançou o controle Olos | Não presumir autoria deste worker, exclusividade ou retomada por checkpoint local. Investigar evidência remota. |

`retryRetainedBatch` chama somente persistBatch com o UUID/digest original, sem qualquer chamada SQL Server. Não é recuperação de payload depois de crash. `recoverInterrupted` delega ao mecanismo existente de recuperação de intents expirados.

`OlosReconciliation` exige binding/classificador versionado explícito para LogReadData. Registra somente ação/resultado/digest nos controles existentes. Evidência é vinculada a caso, tabela, tipo, data, intervalo e versão do contrato; não é reaproveitada em outro pedido. `remote_read_observed` não significa “lido por este worker” nem “confirmado no PostgreSQL”.

O plano ReReadData exige evidência positiva compatível e referência de aprovação operacional. Planejar não invoca ReReadData, não altera checkpoint e não fecha caso. A assinatura, retenção, escopo, disponibilidade, limites e efeitos reais de ReReadData/LogReadData continuam pendentes. ResetReadData não faz parte da recuperação automática. Evidência resumida em digest não substitui os registros originais nem uma prova verificável de autoria remota.

Os envelopes existentes preservam os valores aceitos e o último payload por chave; não passam a ser um histórico imutável completo nesta fase. A captura histórica opcional da Fase 2B continua disponível por composição já existente, mas não foi ativada ou alterada aqui. Não se promete recuperação de todas as versões sobrescritas.

**Não há garantia demonstrada de ausência de perda, exatamente uma leitura ou recuperação completa.** A janela leitura remota → persistência continua dependendo da semântica e retenção reais da origem.

## 7. Migrations, segurança e isolamento

Nenhuma migration ou permissão nova foi criada; 001/002/003/004 e seus ledgers foram preservados. Nenhum contrato real foi aprovado. O teste de composição usa somente os envelopes/controles 001/002 em fixture isolada, sem exigir mudanças da Fase 2B.

SYNC_ENABLED continua false por padrão e true continua rejeitado. Não há CLI, endpoint de consumo/diagnóstico de catálogo ou script de migration novo. Readiness permanece 503; não anuncia um consumidor disponível. O transporte não busca credenciais ou hosts por conta própria; credenciais reais não foram lidas/inseridas nos testes. TLS e regras de segurança existentes não foram reduzidos.

Os novos testes PostgreSQL reutilizam o helper inalterado `test/support/postgres.ts`: container próprio, banco phase2a_fixture, senha efêmera, tmpfs e porta efêmera somente em 127.0.0.1. Nenhuma URL corporativa/fallback é aceita. DDL, TRUNCATE e fixtures nos testes atuam exclusivamente no container criado pelo helper; não são operações do runtime. Os testes anteriores de projeção continuam usando seus próprios servidores descartáveis.

## 8. Validação executada

| Comando | Resultado |
| --- | --- |
| `npm test` antes de acrescentar a nova suíte | 120 testes/11 arquivos aprovados. |
| `npx vitest run test/olos-client.test.ts test/olos-adapter.test.ts test/olos-extraction.test.ts` | 84 casos aprovados na primeira rodada desses três arquivos. |
| Rodada focada incluindo coletor mssql | 97 casos aprovados antes dos quatro casos complementares. |
| `npm run typecheck` final | Exit 0. |
| `npm run build` | Exit 0. |
| `npm test` completo | **221 testes em 15 arquivos aprovados**, exit 0: 120 anteriores + 101 novos. |

Cobertura adicionada: falha de conexão SQL Server; timeout por deadline/código ETIMEOUT; drenagem/cancelamento sem sobreposição; erros em OUTPUT/returnValue; datasets vazios/extras; 10 mil registros sintéticos; limites de registros/bytes; seis contratos; BIGINT/TEXT/DECIMAL/null e fração/offset; parser destrutivo; senha excluída; drift e duplicatas; reconexão explícita; intent antes de leitura; falha do PostgreSQL; interrupção leitura/persistência; replay local; ACK de COMMIT perdido; duas instâncias; lease expirada; paginação explicitamente contratada; cursor sem progresso; checkpoint inconsistente; reinicialização simulada; evidência de recuperação e bloqueios reais.

Os testes de COMMIT perdido da composição simulam commit efetivado sem ACK. As duas possibilidades (commit efetivado/não efetivado) continuam cobertas nos testes anteriores de persistência/projeção, preservados. Reinicialização é simulação por novas instâncias sobre o PostgreSQL da fixture, não ensaio físico de desastre.

Comparação SHA-256 contra baseline local confirmou preservação de todos os arquivos anteriores de src/test/migrations/permissions/docs, package.json e lockfile; README tem apenas a adição documental desta fase. O teste anterior de ausência de execução automática foi preservado e passou; os testes novos verificam adicionalmente os gates antes da fábrica do pool e das sete chamadas reais. Nenhuma asserção anterior foi removida ou enfraquecida.

Após a suíte, `docker ps -a --filter label=worker-olos-export-data.test=phase2a` não listou containers remanescentes. O build gerou apenas dist ignorado. O checkout já apresentava arquivos untracked antes desta tarefa; não houve staging/commit para mudar esse estado.

## 9. Pendências antes da primeira leitura real controlada

1. Receber manual oficial da edição instalada e assinaturas completas: parâmetros/direções/tipos/tamanhos/defaults, OUTPUTs, códigos de retorno, datasets e mensagens. Confirmar efeitos de ReadData e suas variantes, inclusive controle compartilhado.
2. Confirmar layout completo das seis tabelas, coleta mínima autorizada, tipos/precisão/escala/nulos, PKs e escopo (instância/partição/data), estabilidade e correlações. Não aprovar contratos só com os nomes de chave.
3. Validar em ambiente sintético de SQL Server a aquisição exata **anterior ao parser destrutivo**, especialmente DECIMAL e frações/offsets temporais. Decidir com evidência se será necessário transporte/serializador específico; não apenas remover a rejeição de Date/number.
4. Confirmar limites/retorno por chamada, paginação, tamanho de lotes, término, ordenação e significado dos cursores/intervalos. Validar distinção transacionais versus snapshots de referência; não usar DataAvailable como sondagem sem contrato.
5. Documentar retenção e semântica de LogReadData/ReReadData, autoria entre consumidores, recuperação de cada janela de falha e critérios humanos de fechamento de caso. Provar o que pode ser reobtido antes de prometer ausência de perda.
6. Revisar pool dedicado, TLS, timeouts, cancelamento/drenagem, renovação de lease para duração real, backpressure global por instância, shutdown e métricas sem PII. A infraestrutura VPN informada pela empresa não foi novamente testada por esta entrega.
7. Revisar contratos/permissões/migrations existentes manualmente em ambiente autorizado e fornecer pool de escrita explícito. Não utilizar o pool diagnóstico somente leitura, alterar public ou copiar credenciais para o código.
8. Obter autorização específica para uma etapa diagnóstica de catálogo, se necessária, e depois para uma primeira leitura controlada com escopo/volume/procedure escolhidos e plano de recuperação. Esta fase não concede essa autorização.
9. Só após essas evidências, revisar os bloqueios nativos por mudança de código/testes e a composição operacional; não há flag para liberá-los agora. Habilitar SYNC_ENABLED e rever readiness pertencem a uma entrega posterior autorizada.

raw.olos_calls e o banco definitivo continuam sujeitos aos contratos e gates da Fase 2B, independentemente do avanço da extração.

**Nenhuma conexão ou procedure real da Olos, extração real, consulta de dados pessoais reais, migration/INSERT corporativo, alteração de WireGuard/discadora, commit, push ou deploy foi executado.** SQL Server foi representado exclusivamente por mocks/eventos sintéticos; todo SQL PostgreSQL de teste foi executado nas fixtures descartáveis.
