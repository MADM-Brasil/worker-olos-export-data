# Ativação operacional do Worker Attempts

Este fluxo é opt-in pela configuração `olos-service/v2`, na CLI existente
`worker:attempts`. O boot genérico e a configuração v1 continuam compatíveis.
Nenhum comando deste documento foi executado contra a Olos durante o desenvolvimento.
Os exemplos com `<...>` são incompletos propositalmente: precisam de informações e
aprovações reais. Não copiar aprovações de fixtures ou de outra operação.

## Estados e armazenamento

- `BOOTSTRAP`: captura única em quarentena, revisão explícita e importação local.
  Não depende de `first-confirmed.json`, nem o fabrica. Cada procedure tem sua
  própria captura/revisão. Não inicia scheduler.
- `ACTIVE`: ativação persistente vinculada aos dois contratos observados, aos
  recibos de entrega e ao documento de recuperação externo. Um RPC por ciclo;
  próximo ciclo 120 segundos **após o término** do anterior, sem sobreposição.
- `RECOVERY_REQUIRED`: caso pendente, erro ou requisito revogado/inválido. Uma
  resposta completa pode ser importada localmente; parcial não provoca nova leitura.

Usar um diretório privado novo, direto sob
`~/.local/state/olos-worker/corporate-attempts`, por exemplo `service-run`.
Não reutilizar diretórios históricos. A raiz de serviço contém:

```text
activation.json                         # exclusivo; só depois dos requisitos reais
reviews/ReadDataCurrentDay.json
reviews/ReadDataPreviousDays.json
attempt-<novo-uuid>.json                 # intenção sincronizada antes da conexão
cases/<novo-uuid>/remote-state.json      # REMOTE_OUTCOME_UNKNOWN antes do socket
cases/<novo-uuid>/capture/               # LocalSpool: bytes TDS, digest e metadados
cases/<novo-uuid>/diagnostic.json        # fases/códigos sanitizados
cases/<novo-uuid>/contract-candidate.json
cases/<novo-uuid>/import/                # replay/ingestão local, outro LocalSpool
cases/<novo-uuid>/delivered.json         # só depois de recibos e checkpoint
cases/<novo-uuid>/remote-pending.completed.json
cycles/<instante>-<uuid>.json            # histórico sanitizado de ciclos
```

Diretórios `0700`, arquivos exclusivos `0600`, fsync de arquivos/diretórios.
`remote-pending.json` global impede outra aquisição inclusive por uma instância
antiga. O pending é **arquivado**, nunca apagado para liberar uma leitura incerta.
O lock consultivo PostgreSQL `(740219,1)` é o mesmo das rotas anteriores; o processo
mantém a conexão do lock durante a execução. A perda dessa conexão cancela o ciclo.
Todos os processos devem usar o mesmo estado privado persistente.

## Configuração e autorização persistente

Preparar privadamente `~/.config/olos-worker/attempts-service.json`:

```json
{
  "version": "olos-service/v2",
  "workerId": "<identidade estável desta instalação>",
  "endpoint": {
    "server": "10.196.234.11", "port": 1433, "database": "ExportDataV3",
    "user": "OEC", "password": "<credencial existente, somente no arquivo privado>"
  },
  "postgres": {"host": "<host atual>", "port": 5432, "database": "olos_worker"},
  "pinApprovalFile": "/caminho/privado/pin-aprovado.json",
  "approvalFile": "/caminho/privado/service-approval.json",
  "profile": "<objeto do perfil de 16 colunas aprovado já existente>",
  "campaignFile": "/caminho/privado/campaigns.json",
  "dispositionFile": "/caminho/privado/dispositions.json"
}
```

`profile` deve ser um objeto, não a string acima. Manter o perfil existente, com
`phoneMatchKey=normalized`, `tmaPolicy=disabled` e as 16 colunas na ordem de
`APPROVED_COLUMNS`. O destino deve coincidir exatamente com o retorno de
`localSettings` (`worker-local.env` e configuração PostgreSQL já existentes).
Não alterar credenciais, banco, roles, volumes ou rede para contornar uma rejeição.

Obter o digest local **antes** de emitir a autorização, sem conexão à origem:

```bash
npm run worker:attempts -- service-status \
  --config "$HOME/.config/olos-worker/attempts-service.json" \
  --work-dir "$HOME/.local/state/olos-worker/corporate-attempts/service-run"
```

O responsável prepara o arquivo indicado em `approvalFile`, `0600`, com valores
reais (a aplicação não cria, renova nem aprova esse documento):

```json
{
  "scope": "OLOS_ATTEMPTS_SERVICE",
  "approvalId": "<id de aprovação>",
  "workerId": "<mesma identidade>",
  "configurationDigest": "<digest retornado pelo status>",
  "procedures": ["ReadDataCurrentDay", "ReadDataPreviousDays"],
  "table": "AttemptsRawData",
  "destination": "olos_worker.raw.olos_calls",
  "operator": "<responsável>",
  "reference": "<aprovação operacional verdadeira>",
  "issuedAt": "<instante UTC real>",
  "reviewPolicy": "REVOKE_BY_PRIVATE_FILE",
  "revocationFile": "/caminho/privado/service-revoked.json",
  "bootstrapRecoveryEvidence": "<aprovação do procedimento externo para interrupção da primeira captura>"
}
```

Não há expiração administrativa de 12 horas no v2. A existência do arquivo de
revogação bloqueia o serviço. Configuração, catálogos e autorização são relidos
antes dos efeitos remotos; alteração de digest bloqueia. A ativação adicional
vincula as versões/digests dos contratos posteriormente observados. Os arquivos
de aprovação são autoridade administrativa externa; protegê-los contra escrita
por processos não autorizados (incluindo configuração/volumes privados existentes).
Não existe comando de autoemissão de autorização de serviço.

TLS continua obrigatório e independente. A aprovação de pin antiga mantém seu
limite de 7 dias. Uma aprovação TLS **explicitamente emitida** pode acrescentar
`"validityPolicy":"CERTIFICATE_LIFETIME"`, com `expiresAt` no máximo igual ao fim
da validade do certificado aprovado. Isso não prolonga nem altera o certificado,
não aceita um pin observado e não renova arquivos automaticamente. O certificado,
a aprovação e o peer são verificados; alteração/expiração bloqueia. O pin é
carregado novamente por captura. Não substituir a validação TLS por WireGuard.

## Inspeção anterior: reconciliação separada

O caso `193c7869-6ba0-480f-a7c8-3fcd6e178394` continua indeterminado. Nenhum comando
muda seu result.json, seus marcadores ou seus bytes. Zero bytes e ausência de TLS
confirmado não provam que o RPC não executou. A decisão existente
`PRESERVE_PENDING_EXTERNAL_RECONCILIATION` não libera consumo.

Para acrescentar uma decisão, fornecer um JSON privado:

```json
{
  "version":"olos-inspection-reconciliation/v1",
  "recordId":"<novo UUID da revisão, não o UUID da operação>",
  "caseId":"193c7869-6ba0-480f-a7c8-3fcd6e178394",
  "operator":"<responsável>", "reference":"<revisão real>",
  "issuedAt":"<UTC real>",
  "decision":"PRESERVE_PENDING_EXTERNAL_RECONCILIATION",
  "remoteOutcome":"INDETERMINATE"
}
```

```bash
npm run worker:attempts -- reconcile-inspection \
  --config "$HOME/.config/olos-worker/attempts-service.json" \
  --work-dir "$HOME/.local/state/olos-worker/corporate-attempts/service-run" \
  --decision-file /caminho/privado/decision.json \
  --confirm RECORD_OPERATIONAL_RECONCILIATION
```

`ADMINISTRATIVE_CLOSURE` também mantém `INDETERMINATE` e não libera consumo.
`TECHNICAL_CLEARANCE` exige `remoteOutcome=REMOTELY_ESTABLISHED` e um
`externalAuditFile` privado contendo:

```json
{
  "kind":"olos-remote-audit/v1", "caseId":"<UUID do caso>",
  "markerDigest":"<SHA-256 canônico do marcador original>",
  "operator":"<auditor externo>", "reference":"<auditoria real>",
  "conclusion":"NO_UNRESOLVED_INTERFERENCE",
  "rpcAuditEvidence":"<evidência de recebimento/não recebimento do RPC na janela exata>",
  "consumptionImpactEvidence":"<evidência de ausência de interferência incremental não resolvida>",
  "issuedAt":"<UTC real>"
}
```

O código não pode autenticar a conclusão do DBA apenas por ler esse JSON: a
procedência e suficiência da auditoria precisam ser aprovadas pelo responsável.
O procedimento salva novo registro em `inspection-reconciliations/<recordId>.json`,
com digests das evidências e do caso, e somente na decisão técnica cria
`log-inspection-clearance.json`. O guard revalida esse registro e os digests antes
do RPC; o marcador original permanece. Mudança nos arquivos invalida a liberação.
Não criar esse documento usando ausência de bytes como justificativa.

Falta, para liberar o caso atual, auditoria remota conclusiva da janela/login/
sessão do SQL Server, identificação do RPC e avaliação de efeitos/interferência
com os consumidores incrementais. Se essa informação não existir, preservar a
exclusão. Este código não consulta a origem para fabricar essa prova.

## Bootstrap, revisão e importação

Após autorização e reconciliação legítimas, executar uma captura única:

```bash
npm run worker:attempts -- bootstrap \
  --config "$HOME/.config/olos-worker/attempts-service.json" \
  --work-dir "$HOME/.local/state/olos-worker/corporate-attempts/service-run" \
  --procedure ReadDataCurrentDay --confirm AUTHORIZE_BOOTSTRAP_CAPTURE
```

O resumo retorna um **novo** attemptId. O retorno bruto é drenado para disco
antes da interpretação. Layout não suportado ou resposta inválida fica em
quarentena; não é aprovado nem convertido automaticamente. Pending parcial/incerto
bloqueia novas leituras. Não remover marcadores nem executar novamente para testar.

Gerar proposta privada a partir dos metadados realmente capturados:

```bash
npm run worker:attempts -- prepare-contract \
  --config "$HOME/.config/olos-worker/attempts-service.json" \
  --work-dir "$HOME/.local/state/olos-worker/corporate-attempts/service-run" \
  --capture-id <novo-attemptId>
```

A proposta contém `approved=false`, chave não aprovada e campos administrativos
vazios. Ela é rejeitada como aprovação até revisão real. O responsável deve:

1. Verificar o digest e TLS da captura e revisar a semântica da procedure.
2. Preencher identidade/referência/instante, versão nova de contrato, evidência de
   layout e PK; confirmar `AttemptsRawDataID` não nula e numérica, não CallId.
3. Manter os tipos/nulabilidade/collation/ordem realmente observados. Validar os
   campos de origem necessários à projeção de 16 colunas e ticks DATETIME.
4. Definir `sourceTimeZone`, `controlDayOffset` e `partitionSemanticsEvidence`.
   Esse offset é um **bucket de controle explicitamente aprovado**; não afirma
   que a procedure recebe data nem que todas as linhas pertencem àquele dia.
   PreviousDays precisa de política própria, não de copiar a data CurrentDay.
5. Aprovar `plan.contract` e preencher as evidências do plano após validar o
   contrato real. O DBA deve registrar exatamente esse contrato em
   `olos_export.source_contracts` pelo processo administrativo existente. O
   runtime não tem nem recebe permissão de aprovação de contrato/DDL.

Salvar a revisão completa em outro arquivo privado, preservando a proposta:

```bash
npm run worker:attempts -- approve-contract \
  --config "$HOME/.config/olos-worker/attempts-service.json" \
  --work-dir "$HOME/.local/state/olos-worker/corporate-attempts/service-run" \
  --review-file /caminho/privado/current-review.json \
  --confirm APPROVE_OBSERVED_CONTRACT

npm run worker:attempts -- import-capture \
  --config "$HOME/.config/olos-worker/attempts-service.json" \
  --work-dir "$HOME/.local/state/olos-worker/corporate-attempts/service-run"
```

A revisão verifica todos os bytes, metadados, chave, tipos e projeção das linhas;
a importação exige também a aprovação exata no PostgreSQL. `import-capture` é
local: não executa procedure. Repetir o fluxo de bootstrap/revisão/importação para
`--procedure ReadDataPreviousDays`, com captura/revisão/versão separadas. Só depois
da entrega confirmada o pending é arquivado. Nenhum first-confirmed fictício é usado.

## Promoção e execução contínua

Preparar um documento privado de recuperação com schema `recoveryApprovalSchema`
em `adapters/attempts-service.ts`: scope `OLOS_CONSUMPTIVE_RECOVERY`, digest da
configuração, `captureIds` CurrentDay/PreviousDays nesta ordem, responsável,
referência, `approvedAt`, evidências de ownership, retenção/completude e exercício
real de recuperação (`ownershipEvidence`, `retentionCompletenessEvidence`,
`recoveryExerciseEvidence`), `externalRunbookReference` e
`failurePolicy=STOP_PRESERVE_EXTERNAL_RECONCILIATION_NO_REMOTE_RETRY`.

A aprovação deve comprovar como atribuir uma faixa a este consumidor, o prazo e
completude de ReReadData, a reconciliação após leitura interrompida e quem fará a
recuperação externa. Capturas de bootstrap bem-sucedidas sozinhas não demonstram
isso. Não habilita execução automática de ReReadData ou ResetReadData.

Preparar privadamente a solicitação de ativação:

```json
{
  "version":"olos-activation/v1", "workerId":"<worker>",
  "configurationDigest":"<digest atual>", "approvalDigest":"<digest canônico da autorização>",
  "contractDigests":{"ReadDataCurrentDay":"<digest da revisão>","ReadDataPreviousDays":"<digest da revisão>"},
  "captureIds":{"ReadDataCurrentDay":"<captureId>","ReadDataPreviousDays":"<captureId>"},
  "recoveryEvidence":"/caminho/privado/recovery-approval.json",
  "operator":"<responsável>","reference":"<aprovação real>","issuedAt":"<UTC real>"
}
```

Os digests usam SHA-256 de JSON canônico (`digest` em
`src/persistence/validation.ts`), não o hash do arquivo formatado. O status fornece
o digest de configuração; approve-contract fornece o digest da revisão. A
ativação valida as duas capturas e seus recibos locais, além do documento externo.
Ela é exclusiva e persistente; mudanças de configuração/contratos exigem revisão
nova, não regravação silenciosa da aprovação anterior.

```bash
npm run worker:attempts -- activate \
  --config "$HOME/.config/olos-worker/attempts-service.json" \
  --work-dir "$HOME/.local/state/olos-worker/corporate-attempts/service-run" \
  --activation-file /caminho/privado/activation-request.json \
  --confirm APPROVE_SERVICE_ACTIVATION

npm run worker:attempts -- start \
  --config "$HOME/.config/olos-worker/attempts-service.json" \
  --work-dir "$HOME/.local/state/olos-worker/corporate-attempts/service-run" \
  --interval-ms 120000
```

Esse `start` deve ser o processo principal configurado no Dokploy em um deploy
posteriormente autorizado, por exemplo `node dist/cli/adapters/attempts-corporate.js
start ...` na imagem compilada. Não executá-lo como um segundo worker paralelo
via docker exec, cron ou shell em background. O Compose atual mantém o boot
genérico desabilitado até essa escolha operacional; este trabalho não altera
Compose, volumes, política de restart ou a VPS.

Depois de uma reinicialização, usar o mesmo comando e volume privado existente;
não repetir a ativação. O serviço importa primeiro uma captura pendente completa,
sem RPC. Um pending parcial/incerto suspende o ciclo. Se PostgreSQL estiver
indisponível ou ocorrer commit sem ACK, preservar tudo: `import-capture` ou uma
reinicialização com PostgreSQL acessível retoma apenas o lote local, reconciliando
recibos antes de qualquer nova leitura. Não há retry remoto por erro de conexão.

Uma falha nova com transporte encerrado e instrumentação comprovando que a
fronteira do driver RPC **não foi alcançada** gera `not-dispatched.json`, vinculado
ao diagnóstico. Após reiniciar ACTIVE, um ciclo local arquiva esse pending sem
RPC; uma leitura nova recebe outro UUID num ciclo posterior. Isso não se aplica
a SIGKILL, diagnóstico ausente, possível handoff, bytes recebidos ou casos antigos.
O caso LogReadData anterior jamais recebe esse certificado retroativamente.

## Checkpoints, métricas e limites

O modo v2 usa o protocolo existente de ingestão/transações/recibos/outbox, com
checkpoint adiado: a ingestão e seu recibo são duráveis primeiro, mas a versão do
checkpoint só muda após todos os ACKs da projeção/destino daquele lote. O avanço
é idempotente e comparado ao checkpoint esperado. O pending continua cercando
novas aquisições durante essa janela. Nenhum schema, role ou migration é criado.

O `/status` protegido por token contém estado, última procedure com entrega
confirmada, tentativa pendente, timestamps reais, erro sanitizado, validade da
aprovação TLS, estado PostgreSQL e próxima execução obtida do scheduler. O status
por arquivo não abre conexões: indica `NOT_PROBED` para PostgreSQL e próxima
execução nula. Não representa o estado vivo de um processo remoto.

`imported` no serviço conta registros do último lote com entrega confirmada;
`duplicates` são duplicatas identificadas na ingestão. Não são uma contagem de
INSERTs novos em raw.olos_calls: a política existente preserva PKs já presentes,
e não há recibo de contagem de INSERT por linha. `businessRowsInserted=null`
explicita essa limitação. Outras métricas indisponíveis ficam nulas, não zero.

A autorização persistente remove a renovação repetitiva; ela não comprova origem,
layout, semântica PreviousDays, independência de consumidores ou recuperação
remota. Antes de produção ainda são necessárias as evidências reais da Olos,
auditoria conclusiva da inspeção pendente e exercício operacional aprovado.
