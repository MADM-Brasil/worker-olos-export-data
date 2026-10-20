# PostgreSQL de runtime no Dokploy

Esta atualização integra as CLIs ao banco dedicado já provisionado. Não aplica DDL, migrations ou grants e não habilita aquisição Olos. O Compose mantém o comando de boot `node dist/main.js`, `SYNC_ENABLED=false`, uma instância, rede interna, volumes existentes, `restart: no` no worker e nenhuma porta publicada. O boot genérico continua sem conexão PostgreSQL; a verificação de runtime é um comando explícito.

## Destino e credenciais

Os quatro caminhos corporativos (aquisição, recuperação local, replay observado e captura observe) e `worker:local` passam o mesmo destino aos três pools. A precedência é:

1. `WORKER_PG_HOST`, `WORKER_PG_PORT`, `WORKER_PG_DATABASE` no ambiente do processo.
2. `PG_HOST`, `PG_PORT`, `PG_DATABASE` no arquivo privado `worker-local.env`.
3. Padrão local `127.0.0.1:5434/olos_worker`.

O Compose define as três variáveis de destino como `postgres`, `5432`, `olos_worker`. São valores literais; não é preciso cadastrar senhas ou outras variáveis no painel. O banco continua restrito a `olos_worker`; host deve ser nome DNS/IPv4 simples, porta 1–65535. As conexões mantêm `ssl:false` existente, apropriado somente à rede Docker interna/loopback desta implantação. Não usar esta configuração para banco publicado ou rede externa sem uma entrega própria de TLS PostgreSQL. Os limites dos pools, timeouts, schema e separação de logins permanecem iguais.

Os nomes dos logins são fixos no código. `PG_USER`, `PG_PASSWORD`, `DATABASE_URL`, `.pgpass` e credenciais administrativas não selecionam login/senha nesses pools. Somente estes três campos são aceitos no arquivo privado `postgres-credentials.json`:

```json
{
  "olos_ingest_login": "<senha real deste login>",
  "olos_project_login": "<senha real deste login>",
  "olos_receiver_login": "<senha real deste login>"
}
```

Preencher por canal protegido, usando as senhas existentes na VPS. Campos extras (inclusive `olos_admin`), senhas vazias ou repetidas são rejeitados com erro sanitizado. O administrador não deve ser incluído nesse diretório, em variáveis do worker ou em build args. Seu secret continua montado apenas no serviço postgres; rede compartilhada permite comunicação com o servidor, mas não entrega a senha administrativa ao worker. Não copiar o secret administrativo para testar conexão.

## Mounts exigidos

Usar o bind existente, sem trocar volume ou nome de projeto:

| Host, relativo ao Compose do Dokploy | Container | Conteúdo |
| --- | --- | --- |
| `../files/olos-private/worker-local.env` | `/home/node/.config/olos-worker/worker-local.env` | Configuração privada abaixo. |
| `../files/olos-private/postgres-credentials.json` | `/home/node/.config/olos-worker/postgres-credentials.json` | Três senhas distintas de runtime. |
| `../files/olos-private/http-status-token` | `/home/node/.config/olos-worker/http-status-token` | Token aleatório de 32–256 caracteres, sem espaços internos. |

O diretório completo já é montado **somente leitura**. No host, diretório 0700 e arquivos 0600, todos UID/GID **1000:1000** (`node` na imagem), sem symlinks. Não deixar diretório 0700 root:root ou arquivos root:root: o leitor recusa ownership incorreto. Conteúdo mínimo de `worker-local.env`:

```dotenv
SYNC_ENABLED=false
HTTP_HOST=127.0.0.1
HTTP_PORT=3100
LOG_LEVEL=info
```

As variáveis `WORKER_PG_*` do Compose fornecem o destino; não colocar senhas nesse env. O HTTP local das CLIs fica no loopback interno: `worker:local status` consulta esse HTTP na porta 3100 **somente se** `worker:local start` tiver sido explicitamente iniciado. Não é o servidor de boot na porta 3000. Para validar o banco sem iniciar outro servidor, usar `check`.

Gerar o token uma vez por canal administrativo sem imprimi-lo, por exemplo, no diretório do Compose na VPS:

```bash
# Somente se o arquivo ainda não existir; não sobrescreve token existente.
(umask 077; set -C; openssl rand -hex 32 > ../files/olos-private/http-status-token)
chown 1000:1000 ../files/olos-private ../files/olos-private/worker-local.env ../files/olos-private/postgres-credentials.json ../files/olos-private/http-status-token
chmod 700 ../files/olos-private
chmod 600 ../files/olos-private/worker-local.env ../files/olos-private/postgres-credentials.json ../files/olos-private/http-status-token
```

Preparar `worker-local.env` e o JSON com editor/secret manager protegido antes desse ajuste. Não usar `cat` nos logs do painel nem senha em argumentos. Em produção, token ausente gera `PRIVATE_STATUS_TOKEN_REQUIRED`; a CLI não tenta criar arquivos no mount readonly. Fora de produção, a geração local anterior do token continua disponível. Também é possível fornecer `STATUS_TOKEN` dentro do env privado, mas o arquivo separado evita duplicação.

`worker_state` continua em `/home/node/.local/state/olos-worker` e `postgres_data` em `/var/lib/postgresql/data`. Não renomear projeto/volumes nem migrar os 570 históricos nesta tarefa. Não copiar a pasta privada local: possui senhas, autorizações e paths de outro ambiente.

## CLIs na imagem

`npm run build` emite o boot em `dist/` e a árvore de CLIs em `dist/cli/`. A árvore separada preserva imports relativos de adaptadores/shared code sem loader TypeScript. O Dockerfile ajusta somente os scripts do `package.json` da imagem:

```text
worker:local    → node dist/cli/adapters/local-worker.js
worker:attempts → node dist/cli/adapters/attempts-corporate.js
```

O checkout local conserva os scripts via `tsx`; não é necessário instalar dependências de desenvolvimento na imagem final. Nenhum script é executado ao compilar/copiar a CLI. O entrypoint HTTP, o parser, os gates corporativos e a política de resultado incerto não são alterados.

## Redeploy exato, somente do worker

Procedimento futuro, não executado nesta entrega:

1. Revisar e disponibilizar estes arquivos em uma revisão GitHub autorizada. Nenhum commit/push foi feito por esta tarefa. No serviço Compose **existente** do Dokploy, apontar a revisão/branch correta. Preservar nome do projeto, contexto raiz e `./docker-compose.yml`. Não criar outro serviço ou banco.
2. Preparar/verificar os três arquivos privados acima e os UID/modes. Manter Auto Deploy e Schedules desativados, nenhum domínio/porta pública, e `node dist/main.js` no boot. Não cadastrar senha administrativa no worker. Não executar migrations, criação de roles/tabelas ou comandos post-deploy: o ambiente já está provisionado.
3. Conferir o comando padrão exibido em Advanced no serviço Compose. Para esta atualização, usar comando customizado **somente para worker**, preservando o valor real de projeto `-p` e o Compose Path mostrados pelo próprio painel:

   ```text
   compose -p <PROJETO_COMPOSE_EXISTENTE> -f <COMPOSE_PATH_EXISTENTE> up -d --build --no-deps worker
   ```

   O Dokploy prefixa `docker`; não colocar `docker` duas vezes. Substituir os dois placeholders pelos valores reais do serviço, sem inventar ou alterar o projeto. Remover da variante padrão qualquer `--remove-orphans`, escala, comandos de aquisição ou `down`. A operação deve reconstruir/recriar apenas `worker`; `--no-deps` preserva o container postgres já existente. [Referência oficial do comando customizado](https://docs.dokploy.com/docs/core/docker-compose).
4. Antes de Deploy, conferir diff do Compose: apenas três novas variáveis `WORKER_PG_*`; mounts, nomes de volumes, serviço postgres, restart policies, rede e comando do worker devem coincidir com o ambiente atual. Verificar capacidade livre de disco para a nova imagem e backup existente dos volumes. Pressionar Deploy somente após essa revisão.
5. Esperar worker healthy. No terminal **do worker**, executar:

   ```bash
   node --version
   npm run worker:attempts -- status
   npm run worker:local -- check
   ```

   `attempts status` não conecta ao PostgreSQL/Olos e pode informar configuração/TLS ausentes. `local check` abre somente os três pools de runtime, valida identidades/permissões/schema e lê contagens; não grava dados nem executa procedures Olos. Esperado: `databaseState: "up"`, `schemaReady: true`, `statisticsAvailable: true`, `sourceAuthorized: false`, `enabled: false`. Não exigir contagem zero ou 570: produção tem seu próprio estado.
6. Confirmar que `/run/secrets/postgres_admin_password` não existe no worker, que a rede permanece interna e sem portas publicadas, e que o ID do container postgres e os nomes dos volumes não mudaram. O HTTP principal deve continuar live 200, ready 503, status 404. Não iniciar `once`, `recover`, `worker:attempts start` ou `worker:local start` nesta validação.

Se `check` falhar, revisar arquivos/permissões/identidade, destino e grants existentes com o responsável pelo banco; não corrigir automaticamente com DDL/GRANT nem testar credenciais administrativas no worker. Os erros públicos são sanitizados. Não apagar spool, claims ou checkpoints para contornar falha. Para rollback, reimplantar somente o worker na revisão anterior com os mesmos volumes; não reverter schema ou copiar históricos.

## Validação desta atualização

Em 20/10/2026: `npm run typecheck`, `npm run build`, `docker compose config --quiet` e `git diff --check` passaram. Foram aprovados **38 testes em quatro arquivos**: `runtime-postgres-configuration`, `attempts-corporate-diagnostics`, `attempts-corporate-runtime` e `local-automation`. Cobrem padrão local, override Docker para os três pools, permissões/ownership dos arquivos, token previamente provisionado em mount readonly, configurações/credenciais inválidas, rejeição de administrador e gates da aquisição/recuperação existentes.

A imagem de produção também foi construída e testada com o Compose em projeto local descartável, diretório privado readonly, três senhas sintéticas distintas e PostgreSQL 16 equivalente. As CLIs executaram sem `tsx`: `attempts status` ficou sem rede/autorização; `local check` retornou banco up/schemaReady true, sem habilitar Olos ou alterar runs/checkpoints/outbox/chamadas. Uma senha incorreta produziu falha sanitizada, sem senha na saída e sem fallback administrativo. Confirmada ausência do secret administrativo no worker. Recursos e segredos da fixture foram removidos após a validação.

Somente a fixture descartável recebeu SQL de preparação de testes. Nenhuma migration/role/tabela persistente foi alterada, nenhum acesso à VPS ou procedure corporativa foi realizado, nenhum histórico foi transferido e nenhum commit/push/deploy foi feito. A conectividade/grants reais da VPS devem ser confirmados pelo `check` após o redeploy autorizado.
