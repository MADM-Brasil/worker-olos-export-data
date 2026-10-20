# Deploy isolado no Dokploy

Preparação para a VPS Hostinger `179.198.127.41`; nenhum acesso ou deploy remoto faz parte desta entrega. Este Compose inicia um worker HTTP bloqueado e PostgreSQL 16 dedicado. Não inicia o pipeline de aquisição/entrega. Node.js 22 é compatível com `package.json` e está no período de manutenção LTS: [calendário oficial](https://github.com/nodejs/Release#release-schedule).

## O que será executado

- `worker`: `node dist/main.js`, usuário `node` (UID/GID 1000), uma instância, filesystem raiz somente leitura, `/tmp` temporário, shutdown de 20s e grace period de 30s.
- `postgres`: target `database` do mesmo Dockerfile, baseado em PostgreSQL 16 Bookworm, banco novo `olos_worker`, administrador exclusivo `olos_admin`. O segredo do administrador é montado somente neste serviço.
- Rede Compose `isolated`, `internal: true`, para worker/postgres; bridge `olos_egress` somente no worker. Sem `ports`, `network_mode: host`, redes compartilhadas externas, Docker socket ou labels Traefik. Ver [saída WireGuard e teste TCP](dokploy-olos-egress.md). Aquisição continua desativada.
- Worker com `restart: "no"`: crash/reboot exige revisão e reinício manual. PostgreSQL usa `unless-stopped`. Nenhum comando `once`, `recover` ou `worker:attempts -- start` é executado no boot, healthcheck ou restart.

O monitor genérico de dependências não é um consumidor e está sem credenciais. Não configurar `OLOS_PASSWORD` nem `PG_*` neste deploy: o HTTP funciona sem eles. `/health/live` retorna 200; `/health/ready` retorna 503 com `syncEnabled=false`, porque a aquisição está bloqueada. `/status` retorna 404, pois `STATUS_TOKEN` fica vazio. Usar liveness, não readiness, para saúde do container.

## Configurar no painel

1. Criar **novo projeto/serviço Docker Compose** dedicado, por exemplo `olos-exportdata`, na VPS indicada. Não selecionar Docker Stack/Swarm: este arquivo usa `build`. Não reutilizar aplicação, banco, volumes ou redes da discadora/comissionamento/Dokploy.
2. Selecionar repositório GitHub e revisão revisada que contenha estes arquivos, contexto raiz e Compose Path `./docker-compose.yml`. As mudanças locais desta entrega precisam chegar ao repositório por um fluxo autorizado antes do deploy; nenhum push foi feito aqui. Desativar Auto Deploy e não criar Schedule Jobs/webhooks de execução.
3. Manter o comando do worker definido no Compose. Não adicionar domínio, porta publicada, escala adicional ou healthcheck externo. Se a versão do painel oferecer Isolated Deployments, habilitar e conferir o Compose final: postgres somente na rede interna e worker em `isolated` e `olos_egress`. Não permitir injeção da rede compartilhada `dokploy-network`.
4. Preparar os arquivos privados abaixo na área persistente `../files` deste serviço (fora do checkout Git), usando File Mounts/terminal administrativo protegido. Os caminhos são relativos ao diretório do Compose no servidor, não ao diretório pessoal do operador. O bind usa `create_host_path: false`: diretório ausente deve falhar, sem criação silenciosa com permissões erradas.
5. Manter o nome/projeto do Compose estável entre redeploys; ele determina os nomes dos volumes. Antes de pressionar Deploy, revisar o Compose renderizado e confirmar ausência de `ports`, redes externas, domínios e comandos de aquisição. Não executar o deploy enquanto esta revisão estiver pendente.

O Dokploy suporta [build em Compose, volumes nomeados e arquivos em `../files`](https://docs.dokploy.com/docs/core/docker-compose). Variáveis do painel são interpoladas pelo Compose; não são automaticamente todas injetadas nos containers. Não usar `env_file: .env`, que poderia carregar credenciais Olos inadvertidamente. Sem domínios adicionados, preservar a configuração de rede original conforme a [documentação de domínios](https://docs.dokploy.com/docs/core/docker-compose/domains).

## Variáveis e mounts

| Item | Valor/local | Operação |
| --- | --- | --- |
| `WORKER_LOG_LEVEL` | `info` (opcional no painel) | Única variável interpolada; usar `info`, `warn` ou `error` em produção. |
| `WORKER_PG_HOST` / `WORKER_PG_PORT` / `WORKER_PG_DATABASE` | `postgres` / `5432` / `olos_worker` no Compose | Destino dos três pools das CLIs de runtime; sem usuário/senha administrativos. Ver [integração de runtime](dokploy-runtime-postgres.md). |
| `SYNC_ENABLED` | `false` literal no Compose | Não habilitar; `true` é rejeitado pelo runtime. |
| `HTTP_HOST` / `HTTP_PORT` | `0.0.0.0` / `3000` internos | Sem publicação no host ou Traefik. |
| `STATUS_TOKEN` | vazio | Endpoint administrativo desabilitado nesta etapa. |
| `OLOS_PASSWORD`, `PG_HOST`, `PG_USER`, `PG_PASSWORD` | vazios | Worker não autentica em nenhum banco. |
| `POSTGRES_DB` / `POSTGRES_USER` | `olos_worker` / `olos_admin` | Somente PostgreSQL dedicado; administrador nunca entregue ao worker. |
| Segredo PostgreSQL | `../files/olos-secrets/postgres-admin-password` → `/run/secrets/postgres_admin_password` | Arquivo privado 0600, não variável de ambiente nem build arg. |
| Arquivos privados do worker | `../files/olos-private` → `/home/node/.config/olos-worker` (somente leitura) | Diretório 0700 UID/GID 1000, inicialmente vazio. Arquivos futuros 0600 UID 1000 e sem symlinks. |
| Volume `worker_state` | `/home/node/.local/state/olos-worker` | Spool binário/quarentena, replay-spool, intenções, claims e estado operacional; manter o caso inteiro junto. |
| Volume `postgres_data` | `/var/lib/postgresql/data` | Banco, checkpoints `olos_export.checkpoints`, recibos e outbox. Não existe volume de checkpoint separado do PostgreSQL. |

Exemplo de preparação **futura no servidor**, no diretório do Compose e com privilégios administrativos, somente para este serviço:

```bash
install -d -m 700 -o 1000 -g 1000 ../files/olos-private
install -d -m 700 ../files/olos-secrets
# Gerar uma única vez. Não sobrescrever o segredo de banco já inicializado.
(umask 077; set -C; openssl rand -base64 48 > ../files/olos-secrets/postgres-admin-password)
docker compose config --quiet
```

Se usar o editor File Mounts do painel, criar os mesmos arquivos/caminhos e ajustar ownership/permissões no host. Não colar segredos em issues/logs. Compose secrets com arquivo são mounts de arquivo, não um cofre criptografado; restringir acesso ao host/painel e fazer backup protegido. A senha só é aplicada pelo entrypoint PostgreSQL na inicialização de volume vazio; trocar o arquivo não rotaciona a senha existente. Não importar automaticamente `~/.config/olos-worker` local: contém credenciais, paths de homologação e autorizações de outro ambiente.

## Migrations e roles manuais

O Dockerfile inclui exatamente `migrations/001..006` e os templates `permissions/` em `/opt/olos`, **fora** de `/docker-entrypoint-initdb.d`. O boot não aplica SQL. PostgreSQL novo fica sem schema até a etapa administrativa manual. Os scripts não são idempotentes: antes de reaplicar, verificar os ledgers, revisão da imagem e backup. Não automatizar esta seção em Post Deploy Commands.

Após deploy futuro, somente no **terminal do serviço postgres**, confirmar identidade:

```bash
psql -X -v ON_ERROR_STOP=1 -U olos_admin -d olos_worker -c 'SELECT current_database(), current_user, version();'
```

Após revisão/backup, aplicar uma vez e parar no primeiro erro:

```bash
set -eu
for file in \
  /opt/olos/migrations/001_control.sql \
  /opt/olos/migrations/002_operational_envelopes.sql \
  /opt/olos/migrations/003_projection_control.sql \
  /opt/olos/migrations/004_delivery_receipts.sql \
  /opt/olos/migrations/005_large_batch_control.sql \
  /opt/olos/migrations/006_attempts_recovery_control.sql \
  /opt/olos/permissions/worker.sql \
  /opt/olos/permissions/projection.sql \
  /opt/olos/permissions/receiver.sql \
  /opt/olos/permissions/large-batch.sql \
  /opt/olos/permissions/attempts-recovery.sql
do
  psql -X -v ON_ERROR_STOP=1 -U olos_admin -d olos_worker -f "$file"
done
```

Isso reutiliza `olos_export_worker`, `olos_export_projector`, `olos_export_receiver` NOLOGIN e suas ACLs existentes; não cria outra arquitetura. Logins de runtime ficam para o provisionamento autorizado posterior: `olos_ingest_login` recebe somente `olos_export_worker`, `olos_project_login` somente `olos_export_projector`, `olos_receiver_login` somente `olos_export_receiver`, sempre sem superuser/DDL/ownership e com senhas distintas definidas por canal protegido (`\password` em sessão interativa, sem senha em SQL de log). Não há necessidade desses logins para o boot bloqueado atual.

Não executar scripts de `homologation/`, criar `raw.olos_calls` por inferência, aprovar contratos `pending` nem conceder escrita de negócio nesta etapa. O DDL físico do destino e seus grants precisam de revisão própria antes de ingestão/migração histórica. Não montar o PostgreSQL local ou reaproveitar seu volume.

## Verificações após deploy futuro

No painel, esperar `postgres` healthy e `worker` healthy. Confirmar uma réplica e nenhum bind de portas. No terminal do worker:

```bash
id
node --version
node -e 'for (const path of ["/health/live","/health/ready","/status"]) { const r=await fetch("http://127.0.0.1:3000"+path); console.log(path,r.status); }'
node -e 'const fs=await import("node:fs/promises"); const p="/home/node/.local/state/olos-worker/.mount-check"; await fs.writeFile(p,"mount-ok",{flag:"wx",mode:0o600}); console.log("state writable");'
```

Esperado: UID 1000, Node 22, HTTP `200 / 503 / 404`. Recriar **somente o worker deste serviço**, confirmar a existência do `.mount-check` e então removê-lo. Não usar `down -v`. Não apagar claims/spool em falhas. No terminal postgres, depois das migrations:

```sql
BEGIN READ ONLY;
SELECT version FROM olos_export.schema_versions ORDER BY version; -- 1,2
SELECT version FROM olos_export.projection_schema_versions; -- 1
SELECT version FROM olos_export.delivery_schema_versions; -- 1
SELECT version FROM olos_export.large_batch_schema_versions; -- 1
SELECT version FROM olos_export.attempts_recovery_schema_versions; -- 1
SELECT source_table, approved FROM olos_export.source_contracts; -- seis false
SELECT count(*) FROM olos_export.runs; -- 0
SELECT count(*) FROM olos_export.checkpoints; -- 0
SELECT count(*) FROM olos_export.projection_outbox; -- 0
SELECT to_regclass('raw.olos_calls'); -- NULL nesta etapa
SELECT rolname, rolcanlogin, rolsuper, rolcreatedb, rolcreaterole
FROM pg_roles WHERE rolname IN ('olos_export_worker','olos_export_projector','olos_export_receiver');
COMMIT;
```

No host, usando os IDs **deste projeto**: inspecionar `docker inspect <worker-id>` (RestartPolicy=no, ReadonlyRootfs=true, sem PortBindings) e `docker network inspect <rede-isolated>` (Internal=true, somente estes serviços). Não publicar endpoints para testar de fora. Logs não devem mostrar aquisição, datasets ou erro com payload. Validar backup/restauração dos dois volumes em projeto descartável, mantendo a captura desativada; snapshot bruto de PG em execução não substitui backup consistente. Medir espaço livre, CPU/memória da VPS antes de promover: limites atuais de 512 MiB/0,5 CPU por serviço são para o boot ocioso, não dimensionamento de lotes corporativos.

## Limite para ativação e resultado incerto

A imagem final contém o boot e as CLIs compiladas, sem depender de `tsx` em produção. `npm run worker:local -- check` verifica o PostgreSQL dedicado por três logins; `npm run worker:attempts -- status` continua sendo um diagnóstico de arquivos sem rede. Os pools aceitam `WORKER_PG_*` para `postgres:5432`, preservando o padrão local `127.0.0.1:5434`. Preparar os arquivos privados e seguir o [procedimento de redeploy somente do worker](dokploy-runtime-postgres.md); o comando de boot permanece `node dist/main.js`.

Uma ativação posterior precisa revisar contratos/grants reais, rede corporativa, TLS de aquisição aprovado, autorização/recuperação reais e mounts dos catálogos/perfil com caminhos válidos. Primeiro executar uma única leitura `observe` autorizada, confirmar `response.json`/digest/completude em quarentena, aprovar o contrato observado e só então autorizar replay/recorrência. A confirmação da primeira leitura/entrega e o contrato independente PreviousDays continuam exigidos pelo fluxo existente. Não executar `once`, `recover` ou `start` para descobrir se está configurado.

Se uma operação futura cair com resultado remoto incerto: parar, preservar volume/caso/claim/intenção, revisar recibos e conciliar pela etapa externa LogReadData/ReReadData autorizada. Não fazer retry remoto, apagar pendências, criar outro diretório para repetir, nem usar ResetReadData. O boot atual não contém nenhuma leitura remota; `restart: no`, ausência de cron/Auto Deploy e rede interna evitam reexecução automática por este deploy.

Rollback desta etapa: parar somente o worker deste projeto e voltar à imagem/revisão validada, preservando ambos os volumes. Não tentar rollback de schema apagando volume ou repetir migrations após falha parcial; consultar ledgers e backup.

## Validação local desta entrega — 20/10/2026

- `npm run typecheck`, `npm run build`, `docker compose config --quiet` e `git diff --check`: aprovados.
- `docker compose -p olos-deploy-validation build`: dois targets construídos; imagem de runtime executada com Node `v22.23.3`. Lockfile e versões de dependências preservados.
- Smoke local em projeto Compose exclusivo e descartável, com segredo aleatório e diretório privado vazio: ambos os serviços healthy, usuário node, rootfs readonly, restart do worker desativado, zero portas publicadas e rede `Internal=true` com apenas dois containers.
- HTTP interno confirmado: live 200, ready 503, status 404. Antes da aplicação manual, schemas `olos_export` e `raw` ausentes: nenhum SQL de inicialização da aplicação.
- As seis migrations e cinco templates de permissões foram aplicados manualmente somente no PostgreSQL descartável. Confirmados ledgers, três roles NOLOGIN sem superuser/createrole, seis contratos não aprovados, zero runs/checkpoints/outbox e ausência de `raw.olos_calls`.
- Recriação de ambos os containers preservou os contratos/migrations no PostgreSQL e um arquivo sentinela no volume de estado do worker. Recursos descartáveis foram removidos após o teste.
- Testes adicionais: 15 testes de configuração passaram; `test/no-automatic-migrations.test.ts` falhou na asserção antiga que proíbe imports de filesystem em todo `src/`. O código atual já usa filesystem nos módulos de certificado/spool. Nenhum arquivo de `src/` ou `test/` foi alterado nesta entrega; a falha é preexistente e não foi ocultada nem corrigida por ampliação de escopo. O smoke verificou diretamente a ausência de migrations no boot da imagem.

Não houve acesso à VPS, procedures Olos, alteração de banco persistente, cópia de históricos, commit, push ou deploy remoto. A compatibilidade TLS corporativa e a capacidade da VPS não foram validadas por estes testes locais.
