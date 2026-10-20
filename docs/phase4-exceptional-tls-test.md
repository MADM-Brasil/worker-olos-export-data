# Fase 4 — exceção TLS temporária para o teste Disposition

## Justificativa, evidência e limite

A empresa informou o resultado do diagnóstico da instalação Olos: Subject e Issuer `CN=SSL_Self_Signed_Fallback`, SAN ausente, cadeia `unverified`, nome `mismatch`, Tedious 18.6.2 e mssql 11.0.2. Essa evidência foi recebida nesta solicitação; **nenhuma conexão à Olos foi repetida nesta implementação**. O mssql permanece sem atualização de dependência; a aquisição binária utiliza diretamente o Tedious fixado em 18.6.2.

Foi adicionada à CLI `integration:disposition` uma exceção explícita, chamada `exceptional-unverified-test`. Ela permite **criptografia sem autenticação da identidade do servidor**, exclusivamente para a tentativa controlada de `dbo.usp_ExportData_ReadData`, `TableName='Disposition'`. Aceitar o certificado nessa condição permite interceptação por um atacante com acesso ao caminho de rede. A exceção não valida o emissor, o nome ou a legitimidade do endpoint e não deve ser promovida a configuração de produção.

O padrão continua `verified`, com `encrypt=true`, `trustServerCertificate=false` e CA/nome esperado. Não existe variável de runtime que habilite a exceção. Fastify, scheduler, pools gerais, CLI de diagnóstico, demais procedimentos e políticas gerais do worker não foram alterados para aceitá-la. `SYNC_ENABLED=false` permanece obrigatório.

`/home/madm/olos-phase4-test/run-001` e seu PostgreSQL **não foram lidos, recriados, removidos ou alterados pelo agente**. Os testes prepararam novos diretórios e containers descartáveis próprios. Não houve commit, push, deploy, alteração de WireGuard/discadora ou operação corporativa.

## Arquivos

| Arquivo | Alteração |
|---|---|
| `src/olos/tds/disposition-exception.ts` (novo) | Aprovação limitada a 15 minutos, capacidade por identidade de objeto, endpoint fixo, confirmação de risco e budgets de chamada/conexão/RPC; emissão corporativa restrita ao entrypoint da CLI |
| `adapters/disposition-exception-policy.ts` (novo) | Seleção explícita do modo, schema de configuração e HMAC que vincula origem, credencial e destino à aprovação |
| `adapters/disposition-integration.ts` | Branch excepcional somente em approve/read, aprovação separada, inspeção do destino, claim comum aos dois modos e metadados sanitizados |
| `src/olos/tds/disposition-access.ts` | Reconhecimento e encaminhamento da capacidade excepcional; autorização verificada anterior preservada |
| `src/olos/tds/connection.ts` | Validação da capacidade antes de construir a conexão; uma tentativa TCP, bloqueio de redirecionamento e de LOGIN7 antes de TLS negociado |
| `src/olos/tds/source.ts` | Seleção de trust somente pela capacidade; observação sanitizada do handshake; mesmo fluxo binário/backpressure de antes |
| `test/support/sqlserver.ts` | Opção adicional de certificado sintético autoassinado sem SAN; padrão anterior e administração verificada preservados |
| `test/phase4-exceptional-tls.test.ts` (novo) | Casos negativos, CLI e pipeline real com SQL Server Developer/PostgreSQL descartáveis |
| README e este documento | Referência operacional e evidências |

Não há migrations novas, alteração de hooks/npm scripts, Dockerfile, `.env.example`, configuração geral, persistência, spool, contratos, checkpoints, procedures de recuperação ou testes anteriores. O helper de teste recebeu uma opção adicional; nenhuma asserção dos 395 testes anteriores foi removida ou alterada.

## Autorização e isolamento

Approve **e** read exigem cumulativamente:

- `--tls-mode exceptional-unverified-test`;
- `--tls-risk-confirmation I_ACCEPT_ENCRYPTION_WITHOUT_SERVER_IDENTITY_VALIDATION`;
- as confirmações normais de Disposition: `AUTHORIZE_READ_DISPOSITION_ONCE` em approve e `READ_DISPOSITION_ONCE` em read;
- aprovação operacional identificada por `--operator` e `--reference`, UUID do testId, snapshotDate e instante de emissão;
- validade futura, limitada a **15 minutos desde a emissão**, sem renovar automaticamente;
- endpoint exato `10.196.234.11:1433`, banco `ExportDataV3`, usuário `OEC`; somente ReadData/Disposition;
- ambiente `SYNC_ENABLED=false` e ausência de `NODE_TLS_REJECT_UNAUTHORIZED=0`;
- mesmo destino isolado e configuração aprovados, sem claim anterior.

A aprovação fica em `exceptional-tls-approval.json`, separado de `approval.json` do modo seguro. A configuração não é inferida de uma aprovação anterior, do certificado ou de `.env`. O servidor não é acessado por approve. A capacidade corporativa só pode ser emitida no processo cujo entrypoint é `adapters/disposition-integration.ts`, depois dos gates e do claim durável; importá-la em Fastify, scheduler, boot ou outro script não concede autorização. Não há opção de CLI para o caminho de fixtures.

O HMAC-SHA-256 utiliza o segredo do PostgreSQL descartável, já protegido em state.json, para vincular a configuração de origem inteira, senha OEC corrente, collation opcional, política TLS, ação, testId, container, porta e snapshot. A senha OEC não é persistida nem impressa. Trocar a senha entre approve e read também invalida o binding. Isso é um controle local de integridade, **não assinatura digital ou aprovação externa**: o operador autorizado do host/arquivos/Docker continua dentro da fronteira de confiança. O identificador do operador é armazenado nos arquivos privados de auditoria, sem conteúdo operacional.

Antes da emissão e novamente após o claim, a CLI confere por Docker inspect: ID exato, label `worker-olos-export-data.integration=<testId>`, imagem `postgres:16-bookworm`, container em execução, porta única ligada a 127.0.0.1, banco `phase4_worker_fixture` e credencial correspondente ao state.json. A configuração PG não aceita URL/host/banco corporativo; não existe fallback. Antes de receber, continuam a preflight do contrato e a exigência de destino vazio. Configuração de origem é relida e comparada depois do claim, antes da capacidade de conexão.

O arquivo `source-attempt.json` é criado com exclusividade, fsync e sincronização do diretório, **antes de qualquer conexão à origem**. É compartilhado pelos dois modos: uma tentativa verificada anterior bloqueia a excepcional, e vice-versa. Falha após o claim nunca o apaga. A mesma regra limita duas instâncias concorrentes. Esta entrega não introduz renovação/remoção de aprovações ou claims.

## TLS e ausência de fallback

Para a capacidade excepcional: `encrypt=true`, `trustServerCertificate=true`, TLS mínimo 1.2, TDS 7.4, cifras padrão do Node/OpenSSL, `maxRetriesOnTransientErrors=0`. Endpoint e autenticação são conferidos antes da construção da conexão. Não há alteração global do Tedious, de node_modules ou de OpenSSL. O caminho normal permanece estrito.

Além da configuração, o gate de LOGIN7 exige que o handshake já esteja completo e que o protocolo observado seja TLS 1.2 ou 1.3. Isso impede envio de credenciais caso um peer responda ENCRYPT_OFF/NOT_SUP ao PRELOGIN. O Tedious 18.6.2 limita internamente sua negociação TDS 7.x a TLS 1.2. Não há redução de versão/cifras em caso de incompatibilidade. A sobreposição por instância de `connectOnPort` recusa uma segunda conexão, redirecionamento ou connector alternativo. A capacidade permite exatamente um RPC com o nome e os três parâmetros oficiais esperados.

Os arquivos privados de metadados são:

- `source-transport-policy.json`: modo excepcional, TLS mínimo, encrypt/trust explícitos, testId, referência, operador, digest e `serverIdentityValidated=false`, registrado antes da conexão;
- `source-transport-result.json`: se o handshake foi observado, protocolo e `identityValidated=false`, mesmo quando o driver teria autorizado uma cadeia conhecida;
- `source-metadata.json`: somente nomes, tipos, nulabilidade, larguras e collation das colunas; nunca valores de Disposition.

Se o processo cair antes de produzir o resultado, a ausência de observação não prova ausência de conexão. O claim e a política permanecem para revisão conservadora. Erros continuam sanitizados com `CONTROLLED_INTEGRATION_STOPPED` e `retrySource=false`, sem driver stacks, credenciais, descrições ou dados pessoais. O spool contém o snapshot original e mantém as mesmas restrições privadas anteriores.

## Evidências isoladas e validação

A fixture usa SQL Server 2022 Developer (16.x), com certificado RSA sintético `CN=SSL_Self_Signed_Fallback`, autoassinado e sem SAN. Sua administração tem CA explícita e nome correto, com `trustServerCertificate=false`. A conexão Disposition sob teste usa nome localhost e CA não correspondente: o modo padrão falha antes do RPC; somente a capacidade excepcional permite a leitura. Essa separação evita que o setup administrativo use a exceção.

O corpo sintético de ReadData registra `encrypt_option` de **sua própria sessão** em `sys.dm_exec_connections`, exclusivamente no SQL Server descartável. O ensaio verifica `TRUE`, juntamente com o evento secure/TLSv1.2 do transporte. Essa consulta de evidência não foi adicionada à CLI, à procedure Olos ou a qualquer execução corporativa. São três registros sintéticos, não dados pessoais e não uma suposição sobre a quantidade da instalação real.

Os testes cobrem padrão seguro, autorização/risco/operador ausentes, expiração, endpoint/banco/usuário/procedure/tabela incorretos, binding alterado, TLS inferior a 1.2, encrypt=false, cifra permissiva, retry/connector alternativo, destino/container divergente, impossibilidade de ativar no boot, concorrência de claims, um RPC, conexão recusada/interrompida sem retry, rejeição de ENCRYPT_OFF antes de LOGIN7, persistência interrompida, spool selado retido, recuperação após reinício e replay exclusivamente locais. A capacidade de fixture é limitada a loopback, banco phase3d_fixture e usuário sa; ela nunca autoriza endpoint corporativo, e não é exposta por flags/env.

| Comando | Resultado |
|---|---|
| `npm run typecheck` | Aprovado, exit 0 |
| `npm run build` | Aprovado, exit 0; núcleo compilado e adaptadores verificados |
| `npx vitest run test/phase4-exceptional-tls.test.ts` | 35 testes aprovados, incluindo SQL Server/PostgreSQL efetivos descartáveis |
| `npm test` | **430 testes aprovados / 29 arquivos**, exit 0, 134,57 s: 395 anteriores + 35 novos |

Ambiente efetivamente verificado: Node.js 20.20.2, Tedious 18.6.2, mssql 11.0.2. Comparação SHA-256 com baseline local confirmou integridade dos arquivos anteriores de persistência, large-batch, sync/recuperação, configuração, pools, migrations, permissões e todos os testes/suportes anteriores, com a única exceção declarada do helper SQL Server, que recebeu a opção de fixture autoassinada. Package/lockfile, Dockerfile e `.env.example` permaneceram idênticos. Os únicos seis arquivos anteriores modificados são os cinco de implementação/helper indicados na matriz e README; quatro arquivos novos completam esta entrega.

Esses testes não certificam o certificado, a rede, o comportamento ou a semântica da Olos corporativa. Não afirmam prontidão para produção.

## Uma execução manual futura

Os comandos abaixo **não foram executados pelo agente contra run-001 ou contra a Olos**. Exigem autorização humana para uma tentativa de ReadData/Disposition com identidade TLS não validada. Não executar os dois modos em sequência nem criar um loop. Não executar prepare, recriar containers, alterar a label ou reutilizar outro PostgreSQL para contornar um gate.

1. Com a equipe responsável, revisar o risco e a janela da autorização, a configuração, o testId/container já preparado e a disponibilidade da VPN. Não alterar infraestrutura. Verificar se run-001 já tem `source-attempt.json`: **se existir, inclusive após uma falha TLS anterior, parar**. Esta exceção não autoriza repetir uma tentativa existente, apagar o claim ou reutilizar o testId. Uma necessidade de outro ambiente/testId requer planejamento e aprovação separados.

2. Preparar por mecanismo protegido um arquivo JSON privado (diretório 0700, arquivo 0600, fora do Git) com o formato abaixo. O caminho é um exemplo novo e não substitui automaticamente arquivos existentes. A CA é opcional somente neste modo; serverName é um rótulo explícito, não validação de identidade.

```json
{
  "source": {
    "server": "10.196.234.11",
    "port": 1433,
    "database": "ExportDataV3",
    "user": "OEC",
    "serverName": "OLOSMADSPODTB01"
  }
}
```

3. Na sessão autorizada, disponibilizar `PHASE4_OLOS_PASSWORD` pelo mecanismo protegido existente, sem senha em argumentos, JSON, histórico ou logs. Manter o mesmo valor durante approve/read. Selecionar as variáveis locais e uma expiração de aproximadamente 10 minutos, explicitamente dentro da janela aprovada:

```bash
cd /home/madm/worker-olos-export-data
export SYNC_ENABLED=false
PHASE4_WORK_DIR=/home/madm/olos-phase4-test/run-001
PHASE4_EXCEPTION_CONFIG=/caminho/privado/olos-exceptional-source.json
PHASE4_TLS_OPERATOR=IDENTIFICADOR_OPERADOR
PHASE4_TLS_REFERENCE=REFERENCIA_APROVACAO
PHASE4_TLS_EXPIRES_AT="$(node -e 'process.stdout.write(new Date(Date.now()+10*60*1000).toISOString())')"
```

Substituir operador, referência e caminho por valores reais previamente aprovados. Não definir `NODE_TLS_REJECT_UNAUTHORIZED=0`. A flag explícita de risco é exigida em ambos os comandos abaixo:

```bash
npm run integration:disposition -- approve \
  --work-dir "$PHASE4_WORK_DIR" \
  --source-config "$PHASE4_EXCEPTION_CONFIG" \
  --reference "$PHASE4_TLS_REFERENCE" \
  --operator "$PHASE4_TLS_OPERATOR" \
  --expires-at "$PHASE4_TLS_EXPIRES_AT" \
  --confirm AUTHORIZE_READ_DISPOSITION_ONCE \
  --tls-mode exceptional-unverified-test \
  --tls-risk-confirmation I_ACCEPT_ENCRYPTION_WITHOUT_SERVER_IDENTITY_VALIDATION
```

Approve faz validações locais/Docker, grava a aprovação privada, mas não conecta à Olos. Se recusar, parar; não remover arquivos para obter aprovação. Uma aprovação expirada ou já existente não é sobrescrita automaticamente. Somente se a aprovação tiver sido aceita e a execução ainda estiver na janela operacional, invocar **uma vez**:

```bash
npm run integration:disposition -- read \
  --work-dir "$PHASE4_WORK_DIR" \
  --source-config "$PHASE4_EXCEPTION_CONFIG" \
  --confirm READ_DISPOSITION_ONCE \
  --tls-mode exceptional-unverified-test \
  --tls-risk-confirmation I_ACCEPT_ENCRYPTION_WITHOUT_SERVER_IDENTITY_VALIDATION
```

Para um ensaio futuro cujo objetivo inclua falha simulada de destino, acrescentar `--simulate-destination-failure true` **nessa mesma única chamada**, em vez de fazer uma segunda leitura. Decidir isso antes de executar. O total 126 não é critério de sucesso nem limite esperado; permanecem os limites locais e validações da Fase 4.

## Parada e recuperação

Qualquer gate, divergência, timeout, erro de TLS/rede, protocolo incompatível, erro da procedure, metadata drift, lote parcial, disco cheio, falha PostgreSQL ou COMMIT incerto exige parar e preservar diretório, container, spool e claim. Não repetir read, nem no modo verificado. Não reduzir TLS, mudar CA/nome ou alterar a aprovação para tentar novamente no mesmo testId.

Após spool selado, a recuperação e o replay continuam **sem cliente/credenciais SQL Server**. Remover a credencial do ambiente e usar os comandos locais existentes:

```bash
unset PHASE4_OLOS_PASSWORD
npm run integration:disposition -- status --work-dir "$PHASE4_WORK_DIR"
# Somente para spool selado pendente após falha de destino, dentro da lease existente:
npm run integration:disposition -- recover --work-dir "$PHASE4_WORK_DIR"
# Após persistência confirmada e conciliação:
npm run integration:disposition -- replay --work-dir "$PHASE4_WORK_DIR"
```

Recover não precisa que a aprovação de origem continue válida, pois não lê a Olos. Mantém a lease de cinco minutos, recibo, fencing, checkpoint e regras anteriores; não adota lote parcial ou lease vencida sem revisão. Commit sem ACK deve ser conciliado pela mesma identidade/recibo; não inferir rollback da origem. Não há atomicidade distribuída entre SQL Server, spool e PostgreSQL, nem recuperação remota nova. LogReadData/ReReadData/ResetReadData/DataAvailable e consumo incremental permanecem fora deste modo.

## Remoção da exceção

Após a Olos corrigir o certificado/cadeia/SAN, obter a CA e nome por canal confiável e validar por diagnóstico estrito sob autorização separada. Planejar um novo teste verificado com outro testId quando houver claim anterior. Não reutilizar aprovação excepcional nem promover a CA de uma sessão não validada a confiança automaticamente.

Remover o branch e flags excepcionais da CLI, o módulo de política/HMAC e a emissão de capacidades excepcionais; remover os hooks específicos de conexão/source mantendo a política verificada. Manter testes negativos contra trust genérico, downgrade, leitura duplicada e execução em runtime; arquivar metadados e aprovações expiradas conforme retenção, preservando claims e recibos. O responsável operacional deve acompanhar a correção e a retirada da funcionalidade: os 15 minutos limitam cada aprovação, não constituem uma data automática de remoção do código.
