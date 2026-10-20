# Fase 3C — aquisição de grandes lotes

## Escopo e estado operacional

Implementação offline de recepção, spool local durável e persistência de uma resposta completa, com testes sintéticos até 200.000 registros. O caminho novo não é instanciado pelo boot HTTP nem pelo scheduler. `SYNC_ENABLED=false` permanece obrigatório; a configuração existente rejeita sua habilitação. O gate do cliente mssql anterior permanece intacto e o coordenador novo rejeita `RawSource.kind='mssql'` antes de criar intent, acessar PostgreSQL ou chamar a origem.

A arquitetura de controle das fases 2A/3A foi reutilizada. A fase 2B permanece intacta e bloqueada; o novo caminho persiste somente os envelopes de origem, sem projetar nem produzir entregas para `raw.olos_calls`, Kommo ou HubSpot.

Referências: [contratos da Fase 3B](phase3b-olos-contracts.md), [controle da Fase 2A](phase2a.md), [extração da Fase 3A](phase3a-olos-extraction.md). O manual DBA-MN002 de 03/06/2020 permite respostas incrementais de até 200.000 registros; as assinaturas conhecidas não oferecem tamanho de página. O código não cria paginação, offset, cursor remoto ou repetição automática de procedure. `ReadData` estática não tem limite documental equivalente: acima do limite local ocorre falha explícita, nunca truncamento seguido de checkpoint.

**Ainda não existe transporte mssql certificado que forneça esses corpos binários antes do parser.** Foi implementada e testada a fronteira exata de aquisição e seu pipeline, sem conectar um produtor real. Não se afirma integração pronta para produção, leitura real validada nem ausência de perda entre bancos.

## Arquitetura e arquivos

| Arquivo | Responsabilidade |
|---|---|
| `src/large-batch/types.ts` | Portas de origem binária, spool, manifesto e persistência da resposta completa |
| `src/large-batch/config.ts` | Limites explícitos, validados por Zod e congelados |
| `src/large-batch/wire.ts` | Validação do plano e decodificação anterior ao parser destrutivo |
| `src/large-batch/coordinator.ts` | Intent, exclusão, backpressure, cancelamento, selo, entrega e recuperação |
| `adapters/local-spool.ts` | Adaptador opcional de filesystem POSIX, sem SQL e sem conexão de origem |
| `src/persistence/large-batch.ts` | Uma resposta/uma transação, deduplicação global em disco PostgreSQL e recibo idempotente |
| `src/persistence/types.ts`, `validation.ts` | Tipo adicional explícito `sql_datetime_ticks`; tipos antigos preservados |
| `migrations/005_large_batch_control.sql` | Recibos e staging de chaves por resposta, aplicação manual |
| `permissions/large-batch.sql` | Privilégios mínimos adicionais, aplicação manual |
| `scripts/build-spool-adapter.mjs`, `tsconfig.adapters.json` | Empacotamento e verificação do adaptador opcional |
| `package.json`, `tsconfig.json`, `Dockerfile` | Inclusão do adaptador no typecheck/build/imagem; sem dependências novas |
| `test/large-batch-{wire,storage,ingestion,load}.test.ts` | Precisão, armazenamento, falhas, concorrência e carga |
| `test/support/large-batch-{fixture,load}.ts` | Layout sintético documentado e produtor preguiçoso em processo separado |
| `README.md`, este relatório, `phase3c-load-results.json` | Orientação, limites e evidência de carga |

O filesystem fica em um adaptador injetado, fora do núcleo `src`: o núcleo continua sem carregar filesystem, executar SQL de migration ou importar o adaptador no boot. O teste anterior de ausência de migrations automáticas não foi alterado. O adaptador é compilado separadamente, com imports de tipos removidos, e distribuído como `dist/adapters/local-spool.js`. Seu carregamento não abre diretórios; é necessário chamar explicitamente `LocalSpool.open` com um caminho absoluto privado. Não há comando npm para aplicar migrations.

### Fluxo

1. Validar plano, contrato aprovado, evidências de aquisição/resposta/partição e limites. Recusar transporte real e resposta anterior pendente.
2. Fazer preflight do destino de envelopes: ledger, contrato e permissões individuais de SELECT/INSERT/UPDATE. Um pool de diagnóstico somente leitura é recusado.
3. Usar `startRun` existente para gravar intent e obter lease/geração e checkpoint esperado. O bloqueio PostgreSQL é por tabela.
4. Criar claim exclusivo no spool e manifesto de recepção com identidade completa **antes de chamar o produtor**.
5. Consumir um evento por vez de `AsyncIterable`, decodificar corpos binários e aguardar `append`. Não há fila ilimitada nem array de toda a resposta. O produtor deve respeitar esse contrato de pull; um adaptador de eventos futuro também precisará pausar o transporte e limitar suas filas.
6. Exigir um dataset, término explícito de sucesso, ausência de ErrorMessage e término efetivo do iterador. Linha extra, erro depois do evento terminal ou ausência de terminal impedem o selo. Sucesso remoto, metadados e partição devem ser comprovados antes de autorizar um produtor real.
7. Flush, fsync do arquivo, rename e fsync do diretório; publicar manifesto selado com contagem, bytes e SHA-256. Um arquivo existente ou renomeado sem manifesto selado não é resposta completa.
8. Ler o spool em parcelas limitadas, verificar integridade e persistir tudo em uma transação PostgreSQL. Confirmar o manifesto local somente depois do recibo de commit. Reter o arquivo; liberar apenas o claim ativo.

O limite de recepção é 200.000; a parcela de 500 registros é exclusivamente uma estratégia local de escrita SQL. Não é página nem confirmação parcial da Olos. Nenhum checkpoint avança enquanto a resposta inteira não for validada e persistida.

## Limites e comportamento de armazenamento

Configuração por objeto explícito em `spoolLimits`, ainda sem ligação com variáveis do boot:

| Parâmetro | Default | Finalidade |
|---|---:|---|
| `maxRows` | 200.000 | Recusar excesso, sem descartar linhas silenciosamente |
| `maxDiskBytes` | 512 MiB | Quota lógica do diretório, incluindo arquivos retidos |
| `minFreeBytes` | 64 MiB | Reserva de espaço livre, além de margem de metadados |
| `bufferBytes` | 64 KiB | Buffer de I/O para registros comuns |
| `maxRecordBytes` | 1 MiB | Limite de um registro serializado, máximo configurável 8 MiB |
| `syncEveryBytes` | 1 MiB | Fsync periódico; selo sempre executa fsync final |
| `chunkRows` / `chunkBytes` | 500 / 2 MiB | Parcela PostgreSQL, sem commit intermediário |
| `readTimeoutMs` | 300.000 ms | Cancelamento cooperativo da leitura |

A memória cresce com o maior registro, buffers e parcela SQL, não com a quantidade total de linhas. Um registro maior que `bufferBytes` pode ocupar sozinho até `maxRecordBytes`; há buffers transitórios de JSON/Unicode/hashing. Estes limites não são um teto universal de RSS. A carga foi executada também sob limite de heap V8, descrito adiante. Células MAX/PLP grandes ainda exigem validação do framing e eventualmente streaming por célula; exceder o limite atual interrompe a resposta com recuperação pendente.

Diretórios são privados (0700), arquivos 0600, root absoluto/canônico e sem symlinks. Claim exclusivo com O_EXCL/O_NOFOLLOW impede outra leitura no mesmo root. O PostgreSQL acrescenta exclusão por tabela entre roots/instâncias que compartilham o mesmo controle. Um root permite somente um lote não resolvido, mesmo de tabelas diferentes. Não existe expiração automática do claim local, nem exclusão automática de dados. Limite de inventário: 10.000 entradas por diretório e profundidade restrita; esgotamento exige revisão/arquivamento manual.

Antes de escrever, consultar quota e espaço livre, reservando margem para metadados. ENOSPC/EDQUOT em write, sync ou rename interrompe o lote, cancela o produtor e mantém o claim e a identidade. Não se tenta confirmar PostgreSQL/checkpoint. Se até a atualização do manifesto falhar, o intent anterior permanece evidência conservadora. A quota não é reserva física contra outros processos e não garante gravação de um erro quando o dispositivo já está cheio.

O timeout envia AbortSignal, mas não libera a leitura via Promise.race: aguarda o transporte terminar/drenar. Um produtor não cooperativo pode bloquear a instância; não autoriza nova leitura. Lease é renovada durante eventos e parcelas; uma pausa longa pode expirar a lease, causando falha conservadora na persistência.

## Precisão anterior a conversões destrutivas

`decodeWireRow` aceita somente `Uint8Array` ou null por campo; Date e number provenientes do driver são recusados. Nome/tipo/nullability/encoding/tamanho devem corresponder ao contrato explicitamente aprovado. Os layouts documentados não são aprovados automaticamente e o contrato usado nos testes é aprovado somente na fixture descartável.

| Tipo | Aquisição e representação |
|---|---|
| INT | 4 bytes signed little endian; string decimal exata |
| BIGINT | 8 bytes signed little endian via BigInt; string decimal, incluindo valores fora de MAX_SAFE_INTEGER |
| DECIMAL | Codec binário exato existente, com precisão/escala explícitas; sem passagem por number |
| DATETIME | Preservação dos 8 bytes originais, dias e ticks de 1/300 s; token `sql-datetime/v1:<16 dígitos hex>` |
| NVARCHAR | UTF-16LE estrito, comprimento em bytes par; preservar null, vazio e espaços |
| VARCHAR/CHAR | Encoding autorizado explícito (ASCII, UTF-8 ou Windows-1252), tamanho validado e sem trim |

O tipo novo `sql_datetime_ticks` valida o token e a faixa dos dias/ticks antes de aceitar o envelope. DATETIME não vira TIMESTAMPTZ, não recebe timezone e não é arredondado para milissegundos ou microssegundos. O token guarda a representação exata mesmo quando a fração de 1/300 s não cabe exatamente em microssegundos PostgreSQL. É necessária uma **nova versão de contrato** para usá-lo; não reinterpretar payloads ou contratos antigos. Todos os seis campos temporais documentados nas seis tabelas são DATETIME; não foi inferido suporte a DATETIME2/DATETIMEOFFSET neste caminho.

Textos inválidos, metadados desconhecidos e bytes de tamanho incorreto falham explicitamente. Campos de senha são excluídos antes do spool e recusados pelo adaptador e validação de persistência. Null e vazio permanecem distintos; campos inesperados não são silenciosamente aceitos. A PK documentada continua distinta de identificadores de correlação. A confiança em charset, nullability, unicidade e particionamento atuais continua pendente.

A investigação da fase anterior identificou perda no parser padrão Tedious/mssql: DATETIME passa por Date e arredondamento; DECIMAL passa por number. O codec novo não reconstrói valores já perdidos. O futuro transporte precisa interceptar os corpos antes desses parsers, validar token framing, collation, metadados e término RPC, além de demonstrar cancelamento/backpressure. Nenhuma opção do driver foi apresentada como solução já comprovada.

## Transação PostgreSQL e controles

`PostgresLargeBatchPersistence` recebe um iterador de arquivo selado. Staging de chaves (`large_batch_keys`) detecta duplicatas entre parcelas com memória limitada. Chave repetida com conteúdo diferente causa rollback integral. Chave/hash de conteúdo não substitui a identidade original documentada. Envelopes continuam usando a allowlist fixa de tabelas e parâmetros SQL.

Batches, recibo do manifesto, staging de chaves, UPSERTs, contagens, checkpoint, conclusão de run, auditoria e liberação da lease são uma única transação. SHA-256, contagem e tamanho são verificados até EOF, antes de checkpoint/commit; corrupção detectada no fim também faz rollback de todas as parcelas. O cursor opaco é conservado: o código não inventa cursor remoto nem intervalo de PKs. O checkpoint avança uma vez por resposta completa, inclusive vazia bem-sucedida.

Contagens seguem a semântica anterior: `persisted` são linhas inseridas/alteradas; `duplicates` inclui repetições e envelopes já idênticos; `rejected=0` em sucesso. Qualquer registro inválido rejeita a resposta inteira, sem commit parcial de rejeitados. Em falha não se grava contagem parcial como se fosse lote concluído; os controles existentes/manifesto conservam o caso pendente. `rows=0` no manifesto inicial **não prova** que zero registros foram lidos remotamente.

O recibo liga batch UUID estável ao digest do manifesto e identidade run/stream/geração. Replay com o mesmo recibo confirmado retorna contagens sem reescrever nem avançar checkpoint, mesmo se a lease original já expirou. Na ausência de recibo, lease/geração e checkpoint precisam continuar válidos. Não se adota silenciosamente uma execução vencida.

Migration **005 preparada**, sem alterar 001–004 nem o ledger principal [1,2]: `large_batch_schema_versions` [1], `large_batch_receipts`, `large_batch_keys`. Contém somente controles internos em `olos_export`; não altera `public`, layouts de origem ou destinos de negócio. Permissões adicionais são SELECT no ledger e SELECT/INSERT nos dois controles, sem DELETE/TRUNCATE/UPDATE/DDL. Nenhuma migration roda no boot/build. Nos testes, SQL foi aplicado explicitamente apenas nos containers PostgreSQL 16 descartáveis criados pela fixture.

## Recuperação e janelas de falha

| Falha | Evidência e comportamento |
|---|---|
| Após intent e antes de criar spool | Run/lease duráveis; recuperação existente abre caso quando expirar; nenhuma nova leitura da tabela |
| Durante leitura, timeout ou VPN indisponível | Cancelar/drenar produtor, manter identidade e arquivo parcial; sem selo, persistência ou checkpoint; investigar consumo remoto |
| Após leitura e antes de flush/fsync/selo | Mesmo tratamento conservador; bytes recebidos em RAM não provam aquisição durável |
| Após rename e antes do manifesto selado | Não promover arquivo por sua existência; requer conciliação |
| Após spool selado e antes do PostgreSQL | Reinício lê manifesto/arquivo, valida hash e tenta entregar mesma identidade, sem chamar Olos |
| Falha durante parcelas ou COMMIT | Transação falha/resultado desconhecido; manter spool ativo, nenhuma nova leitura |
| Commit efetivo sem ACK | Estado local incerto; consultar recibo idêntico por replay, sem duplicar registros/checkpoint |
| Commit não efetivado sem ACK | Ausência de recibo permite retry somente sob controles/lease válidos; caso contrário conciliar |
| Após commit e antes de liberar claim local | Recibo idempotente permite concluir manifesto e liberar claim, preservando arquivo |
| Reinício com lote parcial | `recoverExpired` abre caso quando lease vence; não persistir prefixo nem executar procedure |
| Reinício com lote selado e lease vencida, sem recibo | Recusar persistência automática e usar caso de recuperação existente; arquivo completo fica retido |
| Disco cheio/corrompido | Sem descarte, sem nova leitura e sem checkpoint; intervenção de armazenamento e conciliação |

`planRecovery` conecta identidade retida ao `OlosRecoveryPort` existente: inspeção de LogReadData e construção de plano de ReReadData. Somente mocks foram usados; não invoca ReReadData/Reset nem repete leitura automaticamente. A inspeção real continua bloqueada pelo cliente nativo. A identificação do caso, exportDate, intervalos e escopo devem ser validados com evidência atual; o controle remoto pode ser compartilhado com outros consumidores.

Um `transition.lock` abandonado ou claim incompleto falha fechado. Não se remove lock baseado apenas em tempo: exigir quiescência das instâncias, inspeção do run/recibo/arquivos e procedimento operacional aprovado. Recuperação automática de adoção de lease vencida, reparo de claims e retenção/arquivamento ainda não foram implementados.

**Spool local não fornece atomicidade SQL Server/PostgreSQL.** Uma interrupção antes do selo pode ocorrer depois de a Olos alterar seu controle. A recuperação depende da semântica real de logs e releitura, intervalo/exportDate e retenção da origem. Nem o hash, nem a lease local, nem o recibo PostgreSQL demonstram ausência de perda nessa janela.

## Validação e carga

Os 297 testes anteriores e seus arquivos de suporte foram preservados sem alteração. Foram adicionados testes de aquisição binária, null/Unicode/espaços, BIGINT/DECIMAL, ticks DATETIME, limites, corrupção, disco cheio em write/sync/rename, concorrência, timeout e cancelamento, preflight, resposta incompleta, rollback, commit sem ACK, restart e plano de recuperação simulado.

A carga gera preguiçosamente os 18 campos documentados de AttemptsRawData, sem dados pessoais reais. Um único evento terminal fecha uma única chamada sintética. O processo separado usa `--max-old-space-size=128 --max-semi-space-size=4`; recebe conexão somente da fixture PostgreSQL descartável via stdin, validando host loopback e nome fixo do banco. Não há fallback para credenciais corporativas.

Validação final em 15/10/2026:

| Comando/verificação | Resultado |
|---|---|
| `npm run typecheck` | Aprovado, exit 0 |
| `npm run build` | Aprovado, exit 0; núcleo e adaptador opcional |
| `npm test` | **333 testes aprovados em 23 arquivos**, 297 anteriores + 36 novos |
| `docker build -t worker-olos-export-data:phase3c-local .` | Aprovado; imagem local, sem deploy |
| Integridade dos arquivos anteriores | Testes/suportes antigos, migrations 001–004, controles e projeção preservados |

| Carga sintética completa | Medida |
|---|---:|
| Recebidos / persistidos | 200,000 / 200,000 |
| Duplicados / rejeitados | 0 / 0 |
| Chamadas sintéticas / versão final do checkpoint | 1 / 1 |
| Tempo do pipeline | 39.255 s |
| Arquivo selado | 88,088,895 bytes (84.01 MiB) |
| Heap inicial / pico amostrado | 22.89 / 35.97 MiB |
| RSS inicial / pico amostrado | 110.75 / 123.75 MiB |
| Limite total de heap informado pelo V8 | 140 MiB |
| Buffer máximo observado / configurado | 65,411 / 65,536 bytes |
| Parcela SQL | 500 registros / 2 MiB |
| Fsyncs / escritas medidas no adaptador | 88 / 1356 |

Resultados finais e métricas estão no arquivo [phase3c-load-results.json](phase3c-load-results.json). Pico de heap/RSS é amostrado, não medição de cada alocação; limite V8 limita heap, não buffers nativos/RSS. O teste demonstra comportamento para esse layout/volume no ambiente local, sem extrapolar para todas as distribuições de payload ou desempenho em produção.

## Limitações e primeiro teste real controlado

Antes de qualquer leitura real, exigir:

1. Aprovação operacional específica, mantendo bloqueados procedimentos não autorizados. Nunca usar ResetReadData como limpeza de teste.
2. Validar metadados atuais, tipos/nullability/collation, PK e escopo de unicidade por tabela; versionar/aprovar contratos de aquisição. Não usar a aprovação sintética de fixture.
3. Implementar e certificar o transporte pré-parser: DATETIME binário exato, DECIMAL/BIGINT, framing/PLP, metadados, datasets, outputs/return status, término RPC, cancelamento e filas limitadas. Não remover o gate somente porque os testes sintéticos passaram.
4. Confirmar semântica de consumo de cada procedure, partição de PreviousDays, efeito de desconexão, logs, releitura, retenção e convivência com outros consumidores. Não presumir exclusividade do controle incremental.
5. Revisar/aplicar controles e permissões manualmente apenas em ambiente autorizado; usar pool de ingestão separado do pool diagnóstico. Dimensionar WAL, espaço de staging/envelopes, tamanho/tempo da transação única e lease; o timeout diagnóstico de 5 s não foi reutilizado como configuração de escrita real.
6. Disponibilizar volume POSIX persistente privado com capacidade/monitoramento, fsync confiável e política de backup, retenção e proteção dos dados pessoais que futuramente poderão existir no spool. Arquivos não são criptografados pelo adaptador. Não usar filesystem efêmero do container como única evidência.
7. Definir intervenção após lease vencida/claim abandonado, perda do volume e corrupção; testar recuperação com restauração e evidência atual da origem antes de declarar garantias. Independência entre bancos/volumes exige plano de migração e exclusão global; o spool não coordena roots em bancos diferentes.
8. Executar primeiro em ambiente isolado com autorização e plano de recuperação; somente depois propor uma leitura corporativa limitada pelo escopo realmente suportado, sem inventar page size. Confrontar contagens/PKs/logs antes de autorizar outra execução.

Não foram executadas procedures Olos, extrações reais, consultas de dados pessoais reais, gravações em bancos corporativos ou migrations de produção. Não houve habilitação de SYNC_ENABLED, alteração de WireGuard/discadora/serviços existentes, commit, push ou deploy. Docker foi utilizado apenas para fixtures PostgreSQL isoladas e build local de imagem; não foi publicado ou iniciado container de aplicação para implantação.
