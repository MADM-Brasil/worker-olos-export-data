# AttemptsRawData — integração controlada e recuperação

## Resultado e bloqueio corporativo

A CLI isolada `npm run integration:attempts -- ...` reutiliza o transporte binário Tedious 18.6.2, parser, spool POSIX, controles PostgreSQL, fencing, transação integral, recibos e checkpoints existentes. O fluxo foi preparado e testado com SQL Server Developer e PostgreSQL descartáveis. **Não houve conexão à Olos, consumo de dados reais, mudança de WireGuard, deploy, commit ou push.** A integração Disposition, inclusive sua exceção TLS, permaneceu inalterada.

**Esta versão não libera leitura corporativa de AttemptsRawData.** O modo padrão é `corporate`, que falha incondicionalmente com `ATTEMPTS_CORPORATE_CONTRACT_UNCONFIRMED`, antes de ler arquivos, inspecionar containers ou abrir conexões. Nenhuma aprovação, variável de ambiente ou flag de TLS remove esse bloqueio. O único modo executável é `--mode isolated-fixture`, cujos schemas/identidades permitem exclusivamente os containers de teste e loopback. Não existe capacidade corporativa Attempts no transporte.

O comando offline mostra o bloqueio exato, sem SQL:

```bash
SYNC_ENABLED=false npm run integration:attempts -- requirements
```

| Bloqueio listado | Evidência ainda necessária |
|---|---|
| `CURRENT_ATTEMPTS_LAYOUT_NULLABILITY_COLLATION_UNVERIFIED` | Metadados atuais completos: ordinais, nomes, tipos, larguras, nulabilidade, collation/codepage e limites de células MAX |
| `PARTITION_DATE_TIMEZONE_AND_PK_SCOPE_UNVERIFIED` | Associação da resposta à data de exportação, timezone do dia corrente e escopo/estabilidade da PK entre partições/instalações |
| `LOG_LAYOUT_AND_SUCCESS_OUTPUT_POLICY_UNVERIFIED` | Layout atual do Log, nulabilidade e política de sucesso/retornos/outputs nas três procedures |
| `LOG_OWNERSHIP_OTHER_CONSUMERS_UNCOORDINATED` | Coordenação com outros consumidores e evidência independente da atribuição do intervalo ao lote interrompido |
| `REREAD_RANGE_INCLUSIVITY_RETENTION_AND_COMPLETENESS_UNVERIFIED` | Inclusividade, retenção/expurgo, completude e recuperação efetiva do intervalo, inclusive sob concorrência |
| `CORPORATE_TLS_CHAIN_AND_NAME_NOT_VALIDATED` | Certificado/cadeia e nome aprovados para TLS verificado; a exceção Disposition não é aceita aqui |
| `RECOVERY_AFTER_EXPIRED_LEASE_OR_HARD_CRASH_REQUIRES_REVIEW` | Procedimento aprovado para lease vencida/processo morto com resposta incerta; não há adoção automática de run vencido |
| `CORPORATE_ATTEMPTS_CAPABILITY_NOT_ISSUABLE_IN_THIS_RELEASE` | Revisão posterior de código e contratos para conceder uma capacidade corporativa específica, sem remover gates por conveniência |

## Arquivos

Novos:

- `src/integration/attempts/contracts.ts`: bloqueios, aprovação por operação, versão de contrato somente da fixture, 18 campos documentados e assinaturas oficiais.
- `src/integration/attempts/source.ts`: produtor de tentativa única sobre `IsolatedTdsSource`, metadados exatos, PKs únicas e validação integral do intervalo ReRead.
- `src/integration/attempts/recovery.ts`: decodificação exata do Log, seleção conservadora de intervalo e plano de ReRead vinculado ao caso/manifesto.
- `adapters/attempts-integration.ts`: CLI, PostgreSQL próprio, validação Docker, aprovação, claims, Log, ReRead, retomada cercada e replay local.
- `migrations/006_attempts_recovery_control.sql`: ledger separado e claims imutáveis de Log/ReRead por run/operação.
- `permissions/attempts-recovery.sql`: SELECT no ledger e SELECT/INSERT nos claims, sem DELETE/UPDATE/DDL.
- `test/attempts-integration.test.ts`: fixtures reais descartáveis e testes da CLI/pipeline.
- Este relatório.

Alterados:

- `src/large-batch/wire.ts`: permite plano de ReReadData apenas para AttemptsRawData com readType=reread, data e intervalo coerentes com os parâmetros; demais validações mantidas.
- `package.json`: adiciona `integration:attempts`, sem hooks automáticos.
- README: link deste procedimento.

Não há segundo transporte/parser ou novo motor de persistência. Os módulos de Disposition, exceção TLS, fonte TDS genérica, parser, pools gerais, boot, configuração, scheduler, persistência, spool e testes anteriores não foram modificados. Os helpers privados de arquivos da CLI Disposition são reutilizados por importação, sem executar seu entrypoint.

## Contratos e aquisição

O manual DBA-MN002 de 03/06/2020 continua sendo evidência documental, sem confirmação física atual. A versão global documentada permanece não aprovada; `controlled-attempts-fixture/v1` é aprovada somente no PostgreSQL criado para registros gerados no teste.

| Tipo documentado | Campos AttemptsRawData |
|---|---|
| INT | CallId, AgentId, CampaignId, DispositionId, PhoneTypeId, MailingRecordId, MailingPhoneNumberId, AttemptsRawDataID |
| DATETIME | StartDate, EndWrap, EndCall |
| VARCHAR(255) | TableName, Dnis, CustomerId |
| VARCHAR(16) | OriginalPhoneNumber, CallIdTelecom |
| VARCHAR(256) | Route |
| VARCHAR(MAX) | CampaignData |

Na fixture, somente AttemptsRawDataID é NOT NULL; todos os demais campos são nullable e os textos usam collation Latin1_General_100_CI_AS/Windows-1252. **Essa política não foi inferida para a Olos.** A PK do manual é AttemptsRawDataID, distinta de CallId e CallIdTelecom. O teste persiste três IDs de extração distintos com o mesmo CallId para conferir essa distinção.

INT/BIGINT continuam strings decimais exatas na persistência; DATETIME mantém os oito bytes, incluindo ticks de 1/300 s, como `sql-datetime/v1:<hex>`. NULL permanece NULL; não se recuperam valores arredondados de Date/number. Os metadados e bytes entram pelo parser existente, antes das conversões destrutivas do driver. A fixture recebe todos os 18 campos, sem selecionar relações ou projetar dados para CRM. Não foi introduzido destino de negócio.

As assinaturas usadas são as de sys.parameters anteriormente fornecidas pela empresa:

| Procedure | Inputs | Outputs |
|---|---|---|
| ReadDataCurrentDay | TableName VARCHAR(128) | Message/ErrorMessage VARCHAR(4000) |
| LogReadData | tablename NVARCHAR(128), DateExport DATE | Message/ErrorMessage VARCHAR(4000) |
| ReReadData | TableName VARCHAR(128), DateExport DATE, PKIDStart/PKIDEnd BIGINT | Message/ErrorMessage VARCHAR(4000) |

Case, comprimento em caracteres do NVARCHAR, DATE civil UTC de entrada e BIGINT dos parâmetros são tratados pelos codecs existentes. Não há cursor, TOP, page size ou data enviados a CurrentDay. A CLI inicial não expõe PreviousDays ou ReadData para Attempts, nem ResetReadData/DataAvailable. O cliente de fixture pode representar PreviousDays, mas isso não concede execução corporativa.

O layout documental de Log é TableName VARCHAR(128), PKColumnName VARCHAR(128), DateExport DATE, PKIDStart/PKIDEnd BIGINT, DateRead DATETIME e Quantity INT. A fixture declara todos NOT NULL. Todos os corpos binários do Log são primeiro preservados como hex em spool de evidência, sem checkpoint de ingestão. DATE é decodificado por dias civis inteiros; DateRead conserva ticks binários. BIGINT acima de MAX_SAFE_INTEGER é testado sem passagem por number. A evidência não é tratada como envelope de dados operacionais e não tem contrato aprovado para persistência de chamadas.

## Aprovações, tentativas e destino

Cada operação `read`, `log` e `reread` exige aprovação separada, com testId, operador, referência, emissão e expiração de até 15 minutos. `approve` exige `AUTHORIZE_ATTEMPTS_OPERATION_ONCE`; a execução exige respectivamente `EXECUTE_ATTEMPTS_READ_ONCE`, `EXECUTE_ATTEMPTS_LOG_ONCE` ou `EXECUTE_ATTEMPTS_REREAD_ONCE`.

O HMAC liga a aprovação ao estado do PostgreSQL próprio, configuração/credencial da fixture SQL, versão do contrato, operação, TLS verificado e, para recuperação, manifesto original e plano/evidência pertinentes. Alterar configuração invalida o binding. Aprovações/claims são privados, duráveis, exclusivos e não sobrescritos. Trata-se de controle local do operador do host, não assinatura de autorização externa.

`attempt-read.json`, `attempt-log.json` e `attempt-reread.json` são gravados e sincronizados antes das operações. Erro, timeout, resultado incerto ou reinício não apagam o claim e não causam retry. O cliente aceita exatamente uma chamada da assinatura/parâmetros vinculados; repetir a mesma instância é recusado. Não há loop incremental nem reconexão automática de procedure.

A identidade Docker do PostgreSQL inclui ID, label `worker-olos-export-data.attempts-test=<testId>`, imagem postgres:16-bookworm, banco fixo phase5_attempts_fixture, porta exclusiva em 127.0.0.1, estado Running e credencial de criação. SQL Server também precisa ser o Developer descartável, com imagem fixada, label de fixture, ID, porta loopback e credencial correspondente. Configuração externa/fallback e campos de TLS permissivo são recusados.

Os claims adicionais no PostgreSQL têm PK `(run_id, operation)`, não apenas exclusão por diretório. Antes de Log/ReRead, uma transação verifica run, tabela, contrato, posse, geração, lease e checkpoint; insere o claim e confirma antes do RPC. Diretórios distintos que compartilhem o mesmo controle não podem repetir essa operação. COMMIT do claim sem ACK é conservador: não permite RPC por inferência de rollback. Não existem permissões de apagar ou atualizar esses claims.

A migration 006 **não altera** 001–005, o ledger principal [1,2], public ou tabelas de negócio. É aplicada somente por `prepare --mode isolated-fixture`, no PostgreSQL novo que essa operação acabou de criar. Não há migrations no boot/build/test apontando a banco externo, nem aplicação em ambientes existentes. `/home/madm/olos-phase4-test/run-001` não foi lido, reutilizado ou alterado.

## Recuperação ensaiada e seus limites

O caminho testado é:

1. CurrentDay grava intent PostgreSQL e manifesto antes da chamada. Uma falha depois de receber parte da resposta encerra/drena o transporte e mantém spool parcial, sem checkpoint.
2. Só manifesto `recovery_required` não selado pode iniciar essa reconciliação. `receiving` não autoriza recuperação concorrente nem permite concluir que uma procedure acabou; crash abrupto exige revisão, sem promoção automática do manifesto.
3. Aprovação independente permite **uma** chamada LogReadData, para tabela/data explícitas. O spool binário de evidência é selado e validado por hash até EOF. Ele não prova autoria do consumo e não avança checkpoint.
4. O operador seleciona **um intervalo exato** e informa evidência de coordenação/atribuição e de completude/inclusividade. Intervalos sobrepostos/duplicados, data divergente, zero registros ou faixa incompatível não produzem plano. Não se mesclam logs nem se infere intervalo a partir de CallId, StartDate ou maior PK local.
5. Outra aprovação vincula ReRead ao caso (batch original), digest do manifesto e Log selado. A chamada usa os parâmetros oficiais. Valida IDs únicos dentro da faixa, extremos e quantidade exatamente iguais ao intervalo aprovado. Retorno 0 com só duas das três linhas é recusado, sem selo/checkpoint.
6. A resposta completa de ReRead é gravada em **outro spool**, mantendo o parcial original. O mesmo coordenador/persistência entrega o lote usando run/stream/checkpoint originais e **a geração original ainda válida**. Não se inicia outra leitura incremental para repor dados.
7. Envelopes, contagens, recibo, checkpoint e conclusão da run são confirmados pela transação existente. Replay usa o mesmo recibo. O spool é validado integralmente antes de replay/recuperação, inclusive se o banco já tiver um recibo idempotente, para não ocultar corrupção local.

Esse caminho suporta reinicialização dos comandos dentro da lease original de cinco minutos. Uma aprovação de 15 minutos não estende uma lease vencida. Mudança de checkpoint/geração, lease vencida, origem ainda `receiving`, perda do PostgreSQL ou ausência de prova exigem parar. Não há takeover, adoção de execução expirada ou fechamento automático de caso por mera existência de Log. `recoverExpired` continua abrindo o caso PostgreSQL e bloqueando novas runs até conciliação, conforme a arquitetura anterior.

Na recuperação bem-sucedida dentro da lease, o caso local é identificado pelo batch original; os arquivos de plano/claim/evidência registram a proveniência e `recovery-delivered.json` registra a entrega. Não se inventa uma linha resolved em recovery_cases quando essa run ainda não expirou. O manifesto parcial original é conservado como evidência histórica e não deve ser tratado como uma pendência a repetir remotamente. O manifesto de recovery-spool e o recibo são a referência de entrega recuperada.

Ausência de selo ou manifesto inicial com rows=0 não prova ausência de consumo remoto. Cancelar/drenar não prova rollback no SQL Server. Igualdade de quantidade/extremos não prova identidade/completude de registros na instalação real. Não há transação distribuída, exactly-once remoto ou garantia de ausência de perda. Retenção, compartilhamento do controle e recuperação de hard crash continuam pendências de liberação corporativa.

## Limites

Reutilizados os defaults da Fase 3C: até 200.000 registros, spool de 512 MiB, reserva livre de 64 MiB, células/registro limitados, buffer de 64 KiB, parcelas PostgreSQL de 500 linhas/2 MiB e timeout de leitura de 300 s. Não são parâmetros de paginação Olos. CampaignData é limitado localmente a 64 KiB na fixture. Os IDs do snapshot são mantidos num Set limitado a 200.000 para rejeitar duplicatas antes do selo. Log aceita até 10.000 intervalos, sem acumular registros operacionais em memória.

Estouro de quota, disco cheio, célula maior que o permitido, dataset extra, outputs divergentes, erro de execução ou resposta parcial interrompem conservadoramente. Os limites precisam ser revistos com o volume real antes de qualquer liberação: espaço local e spool não resolvem a janela de consumo da origem.

## Comandos e validação manual

Para reproduzir a implementação com os dois bancos descartáveis, sem selecionar hosts de ambiente:

```bash
npm run typecheck
npm run build
npx vitest run test/attempts-integration.test.ts
```

O teste cria SQL Server Developer/TLS próprio, procedures com **corpos sintéticos** e PostgreSQL próprio; prepara os contratos e migrations somente nessas instâncias; executa os comandos reais da CLI e remove apenas os containers que criou. Não se apresenta simulação como teste da Olos.

A sequência manual suportada em fixtures é `prepare → approve(operation=read) → read`, seguida de `replay`; para falha de destino com selo, `recover → replay`. Para falha parcial drenada, `approve(operation=log) → log → plan-reread → approve(operation=reread) → reread → replay --spool recovery-spool`. Todos exigem `--mode isolated-fixture`, diretório novo privado e configuração privada de um SQL Developer identificado. A fixture SQL/configuração é montada automaticamente pelo teste; `prepare` sozinho cria apenas o PostgreSQL.

### Primeiro teste corporativo futuro — NÃO liberado nesta versão

1. Resolver os bloqueios da tabela inicial mediante etapa diagnóstica/coordenação **separadamente autorizada**. Não executar incremental para descobrir layout. Aprovar versão do contrato, política de dados mínimos, TLS, data/partição, volume, sucesso/outputs, retenção e recuperação, com evidência específica da instalação atual.
2. Revisar e implementar a capacidade corporativa Attempts no mesmo transporte, preservando tentativa única e vínculo ao destino isolado. A simples edição de um gate, da fixture ou de approved=true não é liberação válida. Definir a resposta a hard crash/lease vencida e ensaiá-la antes do consumo incremental.
3. Preparar **outro** diretório/testId e PostgreSQL de ensaio específicos para Attempts, sem reaproveitar run-001/claims Disposition. Não executar migrations em banco corporativo. Configuração corporativa ainda não tem schema operacional aprovado nesta release.
4. Autorizar uma chamada CurrentDay de Attempts, sem supor tamanho/data remotos configuráveis. Revalidar janela, certificado, consumidores concorrentes e possibilidade de recuperação. Planejar Log/ReRead como operações independentes, sem rotina de polling.

As formas reservadas dos comandos são as seguintes; **atualmente ambas falham com o bloqueio corporativo, sem conectar**. Não executá-las esperando consumo nesta versão:

```bash
export SYNC_ENABLED=false
npm run integration:attempts -- requirements

# Somente depois de revisão/liberação futura, com caminhos e aprovação reais:
npm run integration:attempts -- approve \
  --mode corporate --work-dir /caminho/privado/novo-teste-attempts \
  --source-config /caminho/protegido/config-corporativa-ainda-nao-aprovada.json \
  --operation read --operator OPERADOR --reference REFERENCIA \
  --expires-at DATA_UTC_DENTRO_DE_15_MINUTOS \
  --confirm AUTHORIZE_ATTEMPTS_OPERATION_ONCE

npm run integration:attempts -- read \
  --mode corporate --work-dir /caminho/privado/novo-teste-attempts \
  --source-config /caminho/protegido/config-corporativa-ainda-nao-aprovada.json \
  --confirm EXECUTE_ATTEMPTS_READ_ONCE
```

Para uma recuperação futura autorizada, approve usa `--operation log` e log usa `EXECUTE_ATTEMPTS_LOG_ONCE`; plan-reread exige `--start`, `--end`, `--operator`, `--reference`, `--ownership-evidence` e `--completeness-evidence`; approve usa depois `--operation reread` e reread usa `EXECUTE_ATTEMPTS_REREAD_ONCE`. O modo corporate permanece bloqueado também nessas operações. Não introduzir senha em argumentos/arquivos públicos nem reutilizar a exceção TLS Disposition para torná-las executáveis.

**Parada:** qualquer erro/gate, tentativa anterior, timeout, incerteza remota, corrupção, alteração de metadados/contrato/quantidade, perda de lease, conflito de checkpoint ou identidade de container divergente. Preservar diretório, claims, spools e PostgreSQL. Não apagar claim, repetir CurrentDay, executar ResetReadData ou promover um prefixo. Só spool selado e validado permite recuperação exclusivamente local; ReRead é nova ação remota específica, nunca retry automático.

## Resultados

| Verificação | Resultado |
|---|---|
| `npm run typecheck` | Aprovado, exit 0 |
| `npm run build` | Aprovado, exit 0; núcleo compilado e adaptadores verificados |
| Teste focado | 12 testes novos aprovados com SQL Server/PostgreSQL efetivos isolados; a suíte final inclui a revisão adicional de replay/corrupção |
| `npm test` final | **442 testes aprovados em 30 arquivos**, exit 0; 430 anteriores + 12 novos; 134,61 s |
| `SYNC_ENABLED=false npm run integration:attempts -- requirements` | Aprovado, somente saída offline de bloqueios; nenhuma conexão |

A primeira rodada completa teve 441 aprovações e um timeout do harness de teste: quatro processos Node/tsx excederam o default de 5 s sob carga. Apenas esse teste novo recebeu 30 s de orçamento; nenhum timeout de RPC, autorização, lease, gate ou teste anterior foi relaxado. A rodada final acima passou integralmente, incluindo corrupção de spool após recibo confirmado.

Comparação SHA-256 com baseline local confirmou que Disposition, sua exceção TLS, fonte/parser TDS, persistência, coordenador, configuração, scheduler, migrations/permissões anteriores e todos os 38 arquivos anteriores de testes/suportes permaneceram idênticos. Somente os três arquivos anteriores listados foram alterados. Os arquivos novos de controle SQL foram aplicados exclusivamente aos PostgreSQLs descartáveis criados pelos testes; não a run-001 ou qualquer ambiente existente.

Cobertura adicional: 18 campos, correlação distinta da PK, precisão temporal, NULL, BIGINT do Log acima de MAX_SAFE_INTEGER, TLS validado, autorização/configuração específica, claims concorrentes locais/PostgreSQL, idempotência, corrupção após recibo, falha após selo, resposta parcial, Log exato, ambiguidade de intervalos, recuperação com ReRead completo, ReRead truncado com sucesso aparente, OUTPUT de erro, lease/checkpoint divergentes e gates corporativos. Os ensaios anteriores de grande volume e transporte permanecem na suíte.
