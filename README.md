# worker-olos-export-data

Serviço local integrado: [automação e operação segura](docs/local-worker-automation.md). `npm run worker:local -- check`, `start` e `status` usam os três logins do PostgreSQL local, preservam o histórico e mostram estatísticas reais. A composição automática de spool/ingestão/outbox/entrega foi testada com fixtures; aquisição corporativa continua explicitamente bloqueada.

O [catálogo global de agentes](docs/global-agent-catalog.md) prepara o relacionamento Users.AgentId → UserName e o enriquecimento histórico transacional, com snapshots locais sanitizados e sem aquisição corporativa Users. O ambiente de homologação já existente exige adoção manual revisada da migration; não é atualizado no boot.

Homologação local de arquivos históricos de AttemptsRawData: [guia offline](docs/offline-homologation.md). A CLI `homologation:offline` cria PostgreSQL próprio, preserva arquivo/spool, projeta 16 colunas por perfil explícito e oferece ciclo local de 60 segundos. Não consulta a Olos; `SYNC_ENABLED=false` e o bloqueio corporativo de Attempts permanecem obrigatórios. Os comandos de importação, replay, início, parada e transferência manual estão no guia.

Worker independente da discadora para sincronizar futuramente SQL Server Olos → PostgreSQL MADM. A Fase 2B acrescenta projeção e entrega durável à persistência da Fase 2A, mantendo **extração desativada e nenhuma migration automática**. `SYNC_ENABLED=false` é o padrão; `true` impede a inicialização. Documentação: [Fase 2A](docs/phase2a.md), [auditoria do contrato](docs/phase2b-contract-audit.md) e [implementação da Fase 2B](docs/phase2b-implementation.md). A escrita no banco definitivo em `raw.olos_calls` continua bloqueada; não há contrato físico ou identidade real aprovados para esse destino.

A [Fase 3A](docs/phase3a-olos-extraction.md) prepara cliente tipado Olos/mssql, contratos por tabela, aquisição exata, ingestão com backpressure e conciliação. Os testes usam respostas/eventos SQL Server sintéticos e PostgreSQL descartável. Todas as chamadas reais de procedures permanecem bloqueadas; layouts, assinaturas e aquisição exata de DECIMAL/tempo ainda exigem validação antes de qualquer consumo. A nova camada não está ligada ao boot ou scheduler e não altera a projeção da Fase 2B.

A [Fase 3B](docs/phase3b-olos-contracts.md) consolida os seis layouts do manual DBA-MN002 de 03/06/2020 e compara as assinaturas com os metadados fornecidos pela empresa. Acrescenta codecs DATE/MAX, testes com layouts documentados e investigação de precisão anterior ao parser. Documentação não aprova consumo: contratos operacionais, aquisição exata e compatibilidade com lotes de até 200 mil registros continuam bloqueantes; nenhuma procedure real foi liberada.

## Desenvolvimento

Requer Node.js 20.19+ e npm. A imagem usa Node 20 por requisito inicial; planejar atualização para LTS suportada antes da operação produtiva, repetindo os testes e validando TLS/drivers.

```bash
npm install
cp .env.example .env
npm run typecheck
npm test
npm run build
npm run dev
# Alternativamente, após build:
npm start
```

Sem credenciais, o HTTP inicia normalmente e nenhuma conexão com banco é tentada. `.env` é carregado por dotenv no entrypoint e fica fora do Git e da imagem. Variáveis já presentes no ambiente têm prioridade. Não colocar senhas em comandos, arquivos versionados ou logs. Não há dependência do checkout da discadora.

```bash
curl -i http://127.0.0.1:3000/health/live
curl -i http://127.0.0.1:3000/health/ready
```

| Endpoint | Comportamento nesta fase |
| --- | --- |
| `GET /health/live` | 200 com o processo ativo; 503 durante encerramento, enquanto HTTP estiver disponível. Não depende de rede ou banco. |
| `GET /health/ready` | Sempre 503: consumo ainda indisponível. Informa estado das dependências sem detalhes de conexão. Não usar como liveness nesta fase. |
| `GET /status` | Token Bearer obrigatório, comparação em tempo constante. Sem `STATUS_TOKEN`: 404. Token incorreto/ausente: 401. Retorna fase, sync desativada e último diagnóstico de cada dependência. |

`STATUS_TOKEN` deve ter 32 a 256 caracteres aleatórios, armazenado como secret. Enviar `Authorization: Bearer <token>` por cliente interno, sem registrar esse header. Mesmo autenticado, manter `/status` em rede interna, sem rota pública. Os endpoints não expõem hosts, usuários, senhas, registros ou erros brutos dos drivers. Logs Pino usam eventos e estados; logging automático das requisições está desligado para evitar URL/header/PII nos logs.

## Estrutura

```text
src/
  config/env.ts         validação centralizada Zod, sem valores secretos em erros
  db/dependency.ts      contratos de diagnóstico
  db/pools.ts           pools mssql/pg exclusivos do processo e probes somente SELECT 1
  health/monitor.ts     diagnóstico assíncrono e backoff por dependência
  sync/contracts.ts     contratos originais de extração e sincronização
  sync/recovery.ts      portas futuras LogReadData/ReReadData sem executor
  persistence/          transações, envelopes exatos e controle durável da Fase 2A
  sync/scheduler.ts     estrutura futura com exclusão local e cancelamento
  app.ts                fábrica Fastify e lifecycle HTTP
  logging.ts            Pino e redaction defensiva
  main.ts               boot, SIGTERM/SIGINT e prazo de encerramento
test/                   testes unitários e PostgreSQL 16 descartável via Docker
migrations/             SQL versionado, somente aplicação manual
permissions/            template do role exclusivo do worker
docs/phase2a.md         decisões, pendências e protocolo de recuperação
Dockerfile              build, dependências de produção e runtime sem root
.env.example            catálogo das variáveis sem segredos
```

## Configuração e conexões

Todos os parâmetros estão em `.env.example`. Booleanos aceitam somente `true` ou `false`; portas, tamanhos de pool e timeouts são limitados. `PG_SCHEMA` aceita somente `olos_export`; não é possível escolher `public`.

- Olos: defaults `10.196.234.11:1433`, `ExportDataV3`, `OEC`. Sem `OLOS_PASSWORD`, estado `unconfigured`. Servidor reportado previamente: `OLOSMADSPODTB01`; este projeto não confirmou a identidade ou acesso ao servidor. Não há alteração de WireGuard, WSL2 ou rotas. O futuro container precisa alcançar a VPN através da infraestrutura disponibilizada pela operação.
- PostgreSQL: host, usuário e senha devem ser fornecidos juntos. Banco default `hubspot_olos`, porta 5432. Não se presume PostgreSQL local, nem se usa fallback implícito para localhost. Sem o conjunto de credenciais, estado `unconfigured`.
- SQL Server: pool máximo 2, mínimo 0, connect/acquire e request de 5s, idle de 30s. Pool próprio, sem singleton global do driver. Falha no probe fecha o pool Olos antes de nova tentativa.
- PostgreSQL: pool máximo 3, mínimo 0, aquisição/conexão e query/statement de 5s, idle de 30s. Sessões de diagnóstico configuradas `default_transaction_read_only=on` e `search_path=olos_export`. Isto não cria schema e não fornece adaptador de escrita. `pg` substitui conexões quebradas; não há repetição automática de escritas.
- Ao abrir o listener, cada dependência configurada recebe apenas `SELECT 1 AS healthy`. Não há acesso às tabelas operacionais. Probes rodam em paralelo entre dependências, mas sem sobreposição dentro de uma mesma dependência; o HTTP não aguarda os probes. Sucesso: próximo diagnóstico após 60s. Falha: backoff exponencial com jitter, máximo de 300s. Healthchecks HTTP consultam memória, não disparam SQL.
- Estados: `unconfigured`, `unknown` (primeiro probe pendente), `up`, `down`, `stopped`; `checkedAt` e contador de falhas permitem avaliar o frescor. `up` atesta apenas conectividade no último probe, não permissões de escrita, schema, procedures, consistência dos dados ou capacidade de consumir.
- SIGTERM/SIGINT fecha HTTP, cancela novos probes, aguarda os pendentes e fecha pools. Encerramento é idempotente, com prazo global de 20s no entrypoint; prazo excedido ou falha no shutdown resulta em saída não zero.

### TLS

Defaults: `OLOS_ENCRYPT=true`, `OLOS_TRUST_SERVER_CERTIFICATE=false`, `OLOS_TLS_MIN_VERSION=TLSv1.2`. PostgreSQL usa `PG_SSL_MODE=verify-full` e valida certificado/hostname; `PG_SSL_CA` permite uma CA privada em PEM. Usar host compatível com o certificado.

Qualquer redução dessas proteções (incluindo `PG_SSL_MODE=disable`) exige `ALLOW_INSECURE_DB_TLS=true`, além do parâmetro específico. É exceção explícita, gera evento de advertência e deve ser avaliada individualmente pela operação. Não alterar globalmente `NODE_TLS_REJECT_UNAUTHORIZED`, opções OpenSSL ou nível de segurança para contornar falhas. `trustServerCertificate` não resolve incompatibilidade de protocolo. TLS legado pode continuar incompatível com OpenSSL do Node/imagem; testar em ambiente autorizado antes de definir a exceção. Nenhuma exceção é ativada no exemplo ou Dockerfile.

## Sincronização futura e limites atuais

O scheduler é testado mas **não é instanciado pela aplicação**. Aguarda 60s, executa uma tarefa por vez e agenda o próximo ciclo 60s após o término (fixed delay, sem backlog ou sobreposição). `stop()` aborta o sinal e aguarda a tarefa cooperativa. Exclusão local não substitui exclusão entre containers.

A camada `PostgresPersistence` implementa intent durável, exclusão por tabela com lease e geração, lotes observados, UPSERT, checkpoint e auditoria em transação única, além de recuperação conservadora para casos de conciliação. É injetável e não está conectada ao boot HTTP. O pool de diagnóstico continua somente leitura.

As migrations 001/002 e o template de permissões estão preparados para revisão manual. Criam controles internos e seis envelopes; **não declaram layouts ou PKs da Olos**. Os contratos reais começam bloqueados. Não foi localizada documentação oficial ExportDataV3 suficiente para aprová-los. BIGINT/DECIMAL e timestamps da origem são representações exatas em strings, com validação de null e distinção de timezone. Password não é aceito em nenhum envelope.

O protocolo, contagens, decisões de idempotência, limites da garantia e pendências para a integração real estão em [docs/phase2a.md](docs/phase2a.md). Não há atomicidade presumida entre SQL Server e PostgreSQL; commit sem confirmação permanece unknown e leitura interrompida exige investigação. Nenhuma procedure remota foi executada.

## Docker e futura implantação no Dokploy

```bash
docker build -t worker-olos-export-data:phase1 .
# Apenas desenvolvimento, porta vinculada ao loopback do host:
docker run --rm --name worker-olos-export-data --env-file .env \
  -p 127.0.0.1:3000:3000 worker-olos-export-data:phase1
```

O Dockerfile usa três estágios, `npm ci`, somente dependências de produção no runtime, usuário `node`, SIGTERM direto para Node e healthcheck de liveness. Não declara portas públicas ou instala ferramentas administrativas. `.dockerignore` exclui `.env`, chaves, logs e `credent.txt`. Não passar segredos por ARG/build: injetar em runtime.

No Dokploy futuramente: criar serviço independente, configurar os secrets protegidos, rede privada até PostgreSQL e Olos/VPN, `HTTP_HOST=0.0.0.0`, `SYNC_ENABLED=false`, restart policy, limites de recursos e período de parada maior que `SHUTDOWN_TIMEOUT_MS` (por exemplo 30s). Não publicar SQL Server/PostgreSQL nem expor `/status` via domínio público. A readiness 503 é intencional nesta fase; configurar a plataforma para usar `/health/live` como saúde do container. Não conectar volumes ou alterar containers da discadora. Nenhum deploy foi realizado.

Para atualizar Node, mudar `--build-arg NODE_VERSION=24` (ou LTS suportada escolhida), atualizar tipos/engines quando necessário e validar build, testes e conectividade TLS em ambiente autorizado.

## Verificação e dependências

`npm run typecheck` e `npm run build` não acessam bancos. `npm test` usa mocks e PostgreSQL 16 descartável via Docker. A suíte de projeção usa bancos separados de origem e receptor; cada container é criado/removido pelos helpers, com porta efêmera somente no loopback e senha efêmera. As migrations são aplicadas apenas nesse fixture. A suíte não aceita URL de banco externo e exige Docker disponível; não há fallback para produção. O smoke test sem credenciais não acessa bancos. Os testes não comprovam disponibilidade da VPN, compatibilidade TLS real, permissões de procedures ou acesso PostgreSQL em produção.

`package-lock.json` fixa a árvore. O override `light-my-request=6.6.0` mantém seu ramo compatível com Node 20 (6.8.0 traz `cookie@2` que exige Node 22). Vitest 4.1.12 evita os alertas críticos encontrados na primeira instalação com Vitest 3. O driver permanece `mssql@11`, ramo previamente validado com a Olos: `npm audit` ainda aponta alertas moderados na cadeia `tedious → sprintf-js`, sem correção compatível nesse ramo. Não aplicar `npm audit fix --force` automaticamente. Reavaliar driver e esses alertas junto da atualização de Node; o lock deve ser revisado antes da produção.

Referências utilizadas para os pools e lifecycle: [node-mssql](https://github.com/tediousjs/node-mssql), [node-postgres Pool](https://node-postgres.com/apis/pool), [Fastify Server](https://fastify.dev/docs/latest/Reference/Server/).

## Fase 3C — grandes lotes, somente offline

Recepção sintética de até 200.000 registros com backpressure, spool POSIX privado e persistência da resposta inteira em uma transação foi implementada. O adaptador de spool é opcional e não é carregado pelo boot. A migration 005 e permissões adicionais exigem revisão/aplicação manual; nenhuma migration é automática.

O transporte real permanece bloqueado: falta certificar a aquisição binária anterior ao parser do driver e os comportamentos remotos. `SYNC_ENABLED=false` permanece obrigatório. Arquitetura, resultados dos 333 testes, carga e requisitos de recuperação estão em [docs/phase3c-large-batch-ingestion.md](docs/phase3c-large-batch-ingestion.md).

## Fase 3D — transporte TDS validado em servidor isolado

A aquisição binária anterior ao parser foi implementada por uma extensão isolada de Tedious 18.6.2, sem patch global, e ensaiada em SQL Server Developer real e descartável. O caminho completo até o spool/PostgreSQL recebeu 200.000 registros gerados. O transporte disponível aceita somente loopback e banco fixo de fixture; a Olos corporativa e SYNC_ENABLED permanecem bloqueados.

A extensão usa API privada e requer nova validação ao atualizar o driver. Os procedimentos usados no ensaio têm assinaturas oficiais e corpos sintéticos; não certificam o comportamento incremental da Olos. Estratégia, evidências e pendências estão em [docs/phase3d-real-transport.md](docs/phase3d-real-transport.md).

## Fase 4 — teste controlado de Disposition

`npm run integration:disposition -- ...` prepara um PostgreSQL próprio, registra aprovação operacional e permite uma tentativa de ReadData('Disposition'), seguida de recuperação/replay exclusivamente locais. Exige SYNC_ENABLED=false, ambiente de credencial separado e diretório privado fora do repositório. Não faz parte do runtime Docker nem habilita consumo incremental.

A chamada corporativa **não foi executada nesta entrega**. Instruções da única tentativa autorizada, critérios de parada, proteção TLS e evidências estão em [docs/phase4-worker-integration-test.md](docs/phase4-worker-integration-test.md).

Há um modo excepcional de teste com TLS criptografado e identidade não validada, limitado à CLI Disposition, aprovação específica de até 15 minutos e tentativa única. O modo verificado permanece padrão. Riscos, gates e procedimento futuro estão em [docs/phase4-exceptional-tls-test.md](docs/phase4-exceptional-tls-test.md). Um claim anterior, inclusive de falha TLS, bloqueia esse modo.

## AttemptsRawData — preparo controlado

`npm run integration:attempts -- requirements`, com `SYNC_ENABLED=false`, lista os bloqueios atuais sem conectar. A CLI usa o pipeline existente e ensaia LogReadData/ReReadData em SQL Server Developer/PostgreSQL descartáveis; o modo corporativo permanece bloqueado. Contratos pendentes, comandos futuros e limites da recuperação estão em [docs/attempts-controlled-integration.md](docs/attempts-controlled-integration.md).

## Diagnóstico manual de certificado TLS

`npm run diagnose:olos-tls -- --help` descreve a CLI separada que negocia PRELOGIN/TLS sem autenticação SQL, consultas ou procedures. A conexão manual à Olos exige autorização explícita; o modo padrão verifica certificado e nome. O procedimento, a inspeção exclusivamente informativa e os critérios de parada estão em [docs/olos-tls-diagnostic.md](docs/olos-tls-diagnostic.md). Esse diagnóstico não altera os gates Disposition nem utiliza o PostgreSQL preparado para a Fase 4.

Execução corporativa controlada (primeiro `once`, depois `start`, com TLS/contratos aprovados): [worker Attempts e ciclos de 60 segundos](docs/corporate-attempts-worker.md). Não está habilitada por `.env` ou no boot padrão.
