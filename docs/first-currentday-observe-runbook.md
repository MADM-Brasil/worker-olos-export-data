# Primeira captura manual CurrentDay em observe

**Situação em 20/10/2026: preparada, ainda não liberada.** O estado da VPS foi informado pelo operador; não houve acesso remoto nesta auditoria. TCP, PostgreSQL, OEC e catálogos resolvem conectividade/preparação, mas não comprovam identidade TLS nem recuperação após consumo incerto. Não houve execução Olos, alteração de banco, scheduler, credenciais ou volumes.

## O que o caminho atual realmente faz

Auditado em [CLI corporativa](../adapters/attempts-corporate.ts), [spool](../adapters/local-spool.ts), [transporte](../src/olos/tds/source.ts), [binding](../src/integration/attempts/corporate.ts) e [pin](../src/security/olos-certificate-pin.ts):

1. `once` lê `~/.config/olos-worker/attempts-corporate.json`. O schema exige `pinApprovalFile` e `authorization`; se faltarem, retorna `CORPORATE_CONFIGURATION_INVALID` antes de abrir pools/socket. Não é falha de OEC ou da rota WireGuard.
2. `observe` dispensa `current`/`previous`, mas exige pin de aquisição vigente, autorização e mapeamento válidos. Carrega os catálogos já instalados, valida as três roles/schema PostgreSQL por probe somente leitura e obtém advisory lock. Pendência/caso anterior bloqueia outra captura.
3. Salva o snapshot local de mapeamento, `remote-pending.json`, intenção `attempt-UUID.json` e claims de spool com exclusividade/fsync **antes do permit/RPC**. Envia uma chamada CurrentDay, `TableName=AttemptsRawData`, sem DateExport e sem retry remoto. Timeout/cancelamento não provam que a origem não consumiu.
4. Grava bytes da resposta RPC antes do parser, com backpressure. Não grava LOGIN7/senha nesse arquivo. Metadados não suportados ou erro de armazenamento interrompem, conservando o caso.
5. Só sela após um dataset, DONEPROC final, ReturnStatus 0, ambos outputs presentes e null/vazios, contagem coerente e fsync/rename/digest. O evento é `corporate_response_quarantined`, com `persisted:0`, contrato não aprovado e checkpoint não avançado. Nenhum ACK/first-confirmed é emitido, nem a pendência é liberada por observar.

O limite padrão do spool é 200.000 linhas, 512 MiB e reserva livre de 64 MiB. O selo não é uma transação distribuída com a Olos: queda após consumo remoto e antes do selo ainda exige recuperação externa. Não aumentar limites ou repetir chamadas apenas para obter mais registros.

## Recuperação: comprovado versus pendente

| Evidência | O que permite afirmar | O que NÃO permite afirmar |
| --- | --- | --- |
| Manual local [DBA-MN002 / ExportDataV3](reference/olos-exportdata-v3.pdf), sintetizado em [contratos, seção operacional](phase3b-olos-contracts.md) | CurrentDay lê não lidos do dia corrente; LogReadData expõe tabela/data/faixa/quantidade; ReReadData relê por tabela/data/PK, até 200.000. O manual declara expurgo, sem prazo de retenção. | Comportamento/retenção atuais da instalação, autoria inequívoca de uma faixa, recuperação garantida após falha. O layout documentado do Log não tem identificador inequívoco deste worker. |
| Artefato local `run-002/archive/4ed44db3a4d08728f53f975345d929a1b4e2f28e7a9c7df20ea6fd3c99b553ca.json` em `/home/madm/olos-offline-homologation` | Hash revalidado; metadados registram ReReadData/AttemptsRawData, DateExport 2026-10-01, faixa 37150205–37150774, retorno 0, sem erro e um dataset. Recontados 570 registros/IDs únicos, cobrindo exatamente a faixa inclusiva nesse artefato. | Disponibilidade atual dessa faixa, semântica geral de limites ou de Quantity, completude de faixas futuras, autoria de tentativa nova ou tipos TDS/nulabilidade/collation: não há metadados SQL de colunas nesse JSON. |
| Intenção/spool/claims e replay local existentes | Captura selada e íntegra pode ser reprocessada sem nova chamada Olos, com contrato revisado. | Intenção local não identifica os registros consumidos se nenhuma PK chegou. Spool parcial não pode ser promovido a completo. |

O caso histórico foi conferido **somente no arquivo**, sem consultar/reexecutar ReReadData nem acessar os 570 registros no PostgreSQL. A presença de resposta arquivada não é prova independente de todos os efeitos remotos da execução histórica.

**Confirmação mínima da equipe Olos antes de consumir:** responsável e procedimento executável que relacionem a tentativa à faixa/data de LogReadData mesmo sem PK recebida; coordenação/exclusão de outros consumidores e como confirmar término remoto; fuso/partição e DateExport corretos; retenção, inclusividade e completude de ReReadData após queda. Registrar referência real e quem executará a conciliação/recuperação externa. As portas Log/ReRead da CLI atual continuam sem permit corporativo executável: não tratar `worker:attempts recover` como recuperação remota. Não basta preencher recoveryEvidence com “parar e revisar”. A solicitação mínima à Olos já está em [solicitação técnica](solicitacao-tecnica-olos-primeira-aquisicao.md), itens 2–4; o contrato TDS prévio pode ser observado, a recuperação não pode ser presumida.

## Duas pendências para liberar a única captura

1. **Identidade TLS:** receber PEM público completo e SHA-256 confirmados por canal administrativo independente. O certificado oficial ainda não recebido pode ser substituído apenas por aprovação independente real do certificado autoassinado efetivo, não por observação na mesma rede. O carregador atual aceita somente um certificado autoassinado válido; um certificado oficial assinado por CA não deve ser presumido compatível com esse carregador. Se vier outra cadeia, revisar essa compatibilidade antes de executar, sem contornar a validação. Usar propósito `OLOS_ATTEMPTS_TDS_ACQUISITION`, responsável/referência, approvedAt/expiresAt vigentes (máximo sete dias), arquivo 0600 UID 1000. A aprovação `OLOS_TDS_TLS_METADATA_ONLY`/trustServerCertificate do diagnóstico não serve. Ver [formato do pin](olos-certificate-pinning.md). Não é necessário novo endpoint/scheduler.
2. **Recuperação operacional estabelecida:** obter as confirmações acima e registrar autorização verdadeira para uma captura CurrentDay. Em `authorization`, informar `operator`, `reference`, `issuedAt`, `expiresAt` (máximo 12h), `sourceTimeZone` confirmado, `sharedConsumptionEvidence`, `recoveryEvidence`. Manter `partitionPolicy=ACQUISITION_DAY_CONTROL_BUCKET` e `failurePolicy=STOP_PRESERVE_CASE_RECONCILE_LOG_REREAD_NO_RESET`. São declarações apoiadas por evidências reais, não flags que criam a garantia.

Editar somente o arquivo privado **já instalado** `/home/node/.config/olos-worker/attempts-corporate.json` no Source do bind do Dokploy: adicionar `pinApprovalFile` real e `authorization` real. Preservar OEC, paths do perfil/catálogos e `contractMode: observe`; não acrescentar current/previous fictícios. Não copiar senha para logs, pacote, comandos ou Git. Diretório 0700 e arquivos 0600, UID/GID 1000:1000. Não recriar preparação, banco, roles ou volumes. O bind de diretório readonly reflete a edição administrativa no host; não precisa de deploy/restart.

## Janela operacional: executar somente depois das duas pendências

Escolher uma janela no dia/fuso confirmado, com operador e responsável pela recuperação disponíveis. Manter todas as recorrências desativadas. Conferir espaço do volume de estado: captura e replay preservam cópias distintas, portanto reservar mais que uma única resposta; limite do spool não é garantia de espaço real. Guardar baseline de IDs/hashes do destino em arquivo privado e backup consistente existente, sem importar históricos.

No terminal do worker:

```bash
npm run worker:local -- check
npm run worker:attempts -- status
```

Exigir banco up/schemaReady true e ausência de pendência/caso anterior. `status` é diagnóstico de arquivos; não comprova handshake nem a verdade das evidências. Se houver RECOVERY_REQUIRED ou tentativa anterior, parar e revisar o caso original, sem novo diretório ou limpeza de claims.

Baseline mínima, pelo login `olos_receiver_login`, em sessão `BEGIN READ ONLY`, exportada privadamente pelo cliente já disponível, sem imprimir linhas/campos no painel:

```sql
SELECT internal_id_olos::text AS id, md5(row_to_json(c)::text) AS row_hash
FROM raw.olos_calls AS c
ORDER BY internal_id_olos;
```

Guardar esse resultado como conjunto B. O hash cobre todas as 16 colunas e serve para comparar as mesmas linhas antes/depois; não é hash do arquivo de transporte. Não criar tabelas de baseline. O total existente pode ser zero ou outro; não assumir 570 na VPS. Se não houver ferramenta para guardar B privadamente com o login de runtime, resolver essa preparação antes da captura; não instalar componentes nem usar senha administrativa no worker por conveniência.

Após revisão final do pin, autorização, coordenação e espaço, executar **uma vez**:

```bash
npm run worker:attempts -- once
```

Não usar Scheduler do Dokploy, cron, `start`, loops ou retry do comando. A TLS autenticada ocorre no próprio transporte antes de LOGIN7/RPC. O processo permanecer parado após a execução manual; o HTTP de boot continua separado e bloqueado.

## Conferir a quarentena

Caso padrão no volume de estado:

```text
/home/node/.local/state/olos-worker/corporate-attempts/remote-pending.json
/home/node/.local/state/olos-worker/corporate-attempts/first-run/attempt-<attemptId>.json
/home/node/.local/state/olos-worker/corporate-attempts/first-run/local-delivery.json
/home/node/.local/state/olos-worker/corporate-attempts/first-run/spool/batches/<attemptId>/response.json
/home/node/.local/state/olos-worker/corporate-attempts/first-run/spool/batches/<attemptId>/response.ready
```

Conferir o descritor técnico: state=sealed, metadataState=observed, um dataset, retorno 0, outputs null/vazios, received, bytes, SHA-256 e transporte autenticado. Recalcular o SHA-256 de `response.ready` e comparar ao descritor sem exibir seu conteúdo. `response.ready` sozinho não comprova selo: crash entre rename e descritor pode deixá-lo sem confirmação. Confirmar também o evento de quarentena e a intenção correspondente. `contractApproved`, `businessDelivered` e `checkpointAdvanced` continuam false.

Não imprimir/copiar payload, XMLs ou registros de chamadas em logs/issues. Metadados de colunas não são aprovação. Uma resposta completa, inclusive zero linhas, não comprova que todas as chamadas do dia foram extraídas; não repetir CurrentDay para “completar o dia”.

## Se interromper, cair ou retornar erro

| Situação | Ação mínima |
| --- | --- |
| Configuração/pin recusados antes de qualquer intenção | Corrigir somente a causa comprovada; verificar status/caso antes de qualquer nova autorização. Não interpretar genericamente todo erro de conexão como NOT_SENT. |
| Há intenção/claim e resposta parcial, descriptor incomplete/ausente, timeout, VPN caiu, disco cheio, parser rejeitou ou processo morreu | Parar novas leituras. Conservar `response.partial` ou `.ready`, descritor, intenção e todos os claims; registrar attemptId/horário/código técnico. Acionar o responsável externo pelo procedimento Log/ReRead estabelecido, mesmo se não houver PK local. Cancelamento não desfaz consumo remoto. |
| Captura completa e íntegra, mas importação ainda não autorizada | Manter quarentena e pendência. Revisar contrato antes de recover. |
| Replay/commit/ACK local inconclusivo | Preservar `replay-spool`, `replay-started.json`, `replay-confirmed.json` se houver e recibos. Revisar resultado local; não chamar Olos nem apagar marcadores para tentar novamente. |

Se precisar interromper o processo manual, usar Ctrl+C no terminal que o executa ou SIGTERM no PID **desse comando**, não no HTTP de boot ou em outros serviços. Shutdown pede cancelamento/drenagem; não garante resposta completa. Não usar ResetReadData, não tratar a faixa histórica como faixa atual e não executar `recover` em spool parcial. Nenhum comando desta entrega elimina o bloqueio de recuperação remota.

## Após captura completa: revisão e replay local

1. Revisar `response.json`: ordem/nomes e tipos reais, nulabilidade, comprimentos SQL, collation/codepage e precisão/escala quando aplicáveis, conforme os campos efetivamente capturados/suportados. Preservar BIGINT/DECIMAL e tempo exatos, null e string vazia. Não converter nomes do manual/fixture em contrato atual. Confirmar unicidade no lote e evidência da identidade/escopo de AttemptsRawDataID; uma amostra não prova unicidade entre todas as partições.
2. Montar `current.fields` com os campos exatos observados e `current.plan` com contrato versionado/revisado, assinatura CurrentDay já existente, `parameters={TableName:AttemptsRawData}`, sucesso 0 e `run.exportDate` igual a **intent.controlDate original**. Não inventar DateExport por StartDate nem mudar a data para passar validação. Registrar aprovação real em `olos_export.source_contracts` pela administração autorizada, na estrutura existente e com conteúdo exatamente correspondente ao plano; nenhuma nova migration/role/grant é necessária ou proposta aqui. A role de ingestão não deve autoaprovar.
3. Retirar `contractMode` do arquivo privado para entrar no modo de contrato revisado; adicionar `current` e manter `previous` ausente. Preservar o perfil e catálogos originais: digest diferente de `local-delivery.json` bloqueia replay. Não modificar o payload/descritor original.
4. Depois da aprovação, executar explicitamente:

   ```bash
   npm run worker:attempts -- recover
   npm run worker:local -- check
   ```

   Esse recover reparsa `response.ready` local, verifica bytes/digest/contagem e usa o coordenador/persistência/outbox/writer existentes; **não chama SQL Server**. O replay usa `first-run/replay-spool`. O writer preserva preexistentes com ON CONFLICT DO NOTHING. O attemptId da captura não é o batchId/runId da ingestão: obtê-los dos manifests de `replay-spool/batches/<batchId>/manifest.json`.
5. Ao final, a CLI revalida pin/autorização e a data do contrato para salvar ACK/first-confirmed e retirar a pendência por último. Se pin expirou ou a data mudou, a entrega local pode já estar confirmada e o ACK continuar bloqueado. Não alterar `intent.controlDate`/plano nem reimportar para forçar liberação: preservar `replay-confirmed.json` e revisar recibos/condições da confirmação. Planejar a revisão/replay na mesma janela evita esse bloqueio conhecido; renovar só timestamps não altera a data original legitimamente.

## Confirmar IDs e recibos, sem concluir apenas pela contagem

Executar consultas somente leitura com as roles apropriadas ou uma sessão administrativa de auditoria **fora do worker**. A consulta cruzada final exige acesso de leitura a outbox/recibos/raw; não ampliar grants dos logins de runtime para rodá-la. Substituir `BATCH_UUID` pelo batchId real do replay.

```sql
BEGIN READ ONLY;
SELECT b.id, b.run_id, b.state, b.received_count, b.persisted_count,
       b.duplicate_count, b.rejected_count, r.record_count, c.last_batch_id
FROM olos_export.batches b
JOIN olos_export.large_batch_receipts r ON r.batch_id=b.id
LEFT JOIN olos_export.checkpoints c ON c.stream_id=b.stream_id
WHERE b.id='BATCH_UUID'::uuid;

SELECT o.state, count(*)
FROM olos_export.projection_outbox o
WHERE o.batch_id='BATCH_UUID'::uuid GROUP BY o.state;

SELECT count(*) AS intents,
       count(*) FILTER (WHERE o.state='confirmed' AND d.state='confirmed'
         AND d.request_digest=o.request_digest
         AND d.destination_version=o.destination_version
         AND d.mapping_version=o.mapping_version) AS receipts_matching,
       count(c.internal_id_olos) AS ids_present
FROM olos_export.projection_outbox o
LEFT JOIN olos_export.delivery_receipts d
  ON d.destination_id=o.destination_id AND d.delivery_id=o.delivery_id
LEFT JOIN raw.olos_calls c
  ON c.internal_id_olos=(o.request_payload->>'internal_id_olos')::bigint
WHERE o.batch_id='BATCH_UUID'::uuid;
COMMIT;
```

Exigir lotes/recibos/checkpoint consistentes, zero entregas pendentes/rejected/uncertain, `intents=receipts_matching=ids_present` para as intenções existentes e ausência de rejeições não revisadas. Comparar também received da captura, contagem do recibo de spool e resultados de duplicação do lote: outbox não precisa ter uma intenção por linha recebida quando há deduplicação.

Guardar privadamente o conjunto S de AttemptsRawDataID do lote completo validado pelo parser/replay existente, **incluindo duplicados**, e o conjunto C de `o.request_payload->>'internal_id_olos'` da outbox desse batch. Não extrair IDs por regex dos bytes TDS nem publicar os conjuntos em logs. Comparar com B: **novos IDs=S−B**, já existentes=S∩B. Confirmar presença de todos os IDs de S no destino e associar C aos recibos acima; C sozinho pode omitir linhas deduplicadas e não substitui S. Repetir o hash das 16 colunas sobre os IDs de B e exigir igualdade para provar preservação dos preexistentes. Se existirem outros escritores, coordenar a janela e não atribuir toda diferença global ao worker. Recibo confirmado pode corresponder a preservação/deduplicação, não necessariamente INSERT novo.

Concluir a execução só com evidência do lote e recibos mais reconciliação por IDs, nunca pelo total global ou checkpoint isolado. Guardar metadados, hashes e comparação privados no caso. Nenhuma confirmação desta captura autoriza scheduler/recorrência; não configurar PreviousDays nem iniciar outro processo nesta tarefa.

Validação desta auditoria: 31 testes direcionados passaram em quatro arquivos (`tds-response-observation`, `attempts-corporate-diagnostics`, `attempts-corporate-runtime`, `attempts-corporate-pinning`). São testes locais/sintéticos, não prova de TLS/recuperação reais da VPS. Só este runbook foi criado; nenhum código ou componente novo foi necessário.

## Complemento operacional: PEM capturado, identidade ainda pendente

Duas observações consecutivas do mesmo fingerprint demonstram estabilidade do certificado apresentado, **não identidade independente**. Não transformar esse fato em `approvedBy`, `approvalId` ou `recoveryEvidence` fictícios. Este complemento é documental: não houve conexão remota, execução corporativa, alteração de código ou banco. Não foram executados testes adicionais, pois não há alteração de código.

### Limite concreto dos componentes de recuperação

- `corporate-recovery.ts` prepara parâmetros de Log/ReRead e conecta a auditoria existente; `prepareCorporateAttemptsReRead` retorna `executionAllowed=false`, `remoteOwnershipProvenByLog=false` e escopo `corporate-prepared-blocked`. O emissor de permit em `attempts-access.ts` aceita somente CurrentDay/PreviousDays, não Log/ReRead.
- `OlosReconciliation` registra evidência ou falha e prepara plano; não executa ReReadData nem resolve checkpoint/caso. O log não comprova autoria do consumo.
- `reviewAttemptsRecovery` espera `SpoolManifest` de ingestão, não selado, `state=recovery_required`, com `identity.plan`. `response.json` de captura binária observe é outro formato, sem esse plano. Não fabricar um manifesto ou renomear `.partial` para fazer o helper aceitá-lo.
- `recoverObservedResponse` aceita apenas captura com descritor selado e hash/bytes/contagem válidos, após retirar observe e aprovar o contrato. Não retoma uma resposta TCP parcial nem recupera a origem.

Assim, a recuperação de uma primeira captura parcial exige procedimento **externo**, acordado com Olos/DBA e separado da CLI atual. Se essa etapa ainda não está operacionalmente disponível, a primeira captura consumptiva continua bloqueada, mesmo após aprovação TLS.

### Conciliação mínima antes e depois da tentativa

Antes: o responsável Olos confirma fuso/partição, retenção e como confirmar término remoto; coordena uma janela sem leitores concorrentes (ou fornece atribuição inequívoca alternativa); guarda snapshot técnico de LogReadData para AttemptsRawData e DateExport correto, com assinatura/layout atuais confirmados. Essa consulta deve ser autorizada e executada pela operação Olos fora do worker; não há comando habilitado da CLI para ela.

Em interrupção: parar novas leituras e preservar o caso/attemptId original; confirmar com Olos que a execução terminou; obter novo snapshot de LogReadData. Correlacionar tabela, PKColumnName, DateExport, limites, Quantity e DateRead com a janela e a evidência de exclusividade/atribuição. O diferencial de dois logs e a proximidade de horário, isoladamente, não provam autoria se houver concorrência. Se nenhuma faixa ou múltiplas faixas ambíguas aparecerem, manter o caso aberto; não inferir limites pelo maior ID parcial nem usar a faixa histórica.

Com faixa atribuída: o responsável verifica disponibilidade/retenção e autoriza ReReadData **daquela tabela/data/faixa exata**; preserva a resposta completa por ferramenta já aprovada que mantenha PKs, tipos/precisão, contagem, retorno/outputs e evidência de completude. Conferir Quantity, IDs únicos e limites; não presumir que Quantity=end−start+1 em qualquer faixa. O caso histórico comprovou essa igualdade apenas naquele resultado.

Antes da primeira captura, acordar também como o lote recuperado externamente será validado e persistido pelo caminho existente, vinculado à intenção original. A CLI atual não fornece comando para anexar um ReRead externo a uma captura observe parcial. Sem esse último passo definido/revisado, não declarar recuperação estabelecida. Não copiar bytes recuperados por cima de `response.partial`, criar selo/recibo manual ou retirar claims para abrir outra captura. Nenhum ResetReadData é utilizado.

### Comandos concretos de estado e aprovação, sem nova conexão TLS

No host da VPS, usando o container existente:

```bash
WORKER=olos-export-data-workerolosexportdata-ack5ed-worker-1
sudo docker exec "$WORKER" npm run worker:attempts -- status
sudo docker exec "$WORKER" npm run worker:local -- check
sudo docker exec "$WORKER" node -e 'const fs=require("node:fs"),{X509Certificate}=require("node:crypto"); const c=new X509Certificate(fs.readFileSync("/home/node/.local/state/olos-worker/tls-observed/olos-public.pem")); console.log(JSON.stringify({subject:c.subject,issuer:c.issuer,validFrom:c.validFrom,validTo:c.validTo,fingerprintSha256:c.fingerprint256}));'
```

O último comando lê somente o PEM já capturado; ajustar o caminho se ele foi salvo em outro local. Não estabelece uma terceira conexão. Entregar esse fingerprint e a identificação à equipe responsável por canal administrativo independente e obter confirmação explícita, referência, aprovador e validade.

**Somente depois de receber essa confirmação**, substituir TODOS os campos `<...>` abaixo pelos dados reais. Os placeholders são deliberadamente inválidos: o validador os recusa. O fingerprint deve ser o que foi **confirmado independentemente**, não preenchido automaticamente pelo PEM. Datas UTC ISO8601 com Z, aprovação vigente, prazo máximo sete dias e certificado dentro da validade. O pin atual aceita somente certificado autoassinado cuja assinatura possa ser verificada; certificado assinado por CA exige revisão de compatibilidade separada.

```bash
sudo docker exec -i "$WORKER" node --input-type=module <<'JS'
import fs from 'node:fs/promises';
import {constants} from 'node:fs';
import {parseOlosPinApproval} from '/app/dist/security/olos-certificate-pin.js';
const dir='/home/node/.local/state/olos-worker/tls-observed';
try {
  const s=await fs.lstat(dir);
  if(!s.isDirectory()||s.isSymbolicLink()||s.uid!==process.getuid()||(s.mode&0o077)||await fs.realpath(dir)!==dir)throw Error('PRIVATE_DIRECTORY_REQUIRED');
  const approval=parseOlosPinApproval({
    version:1,purpose:'OLOS_ATTEMPTS_TDS_ACQUISITION',
    host:'10.196.234.11',port:1433,database:'ExportDataV3',
    approvalId:'<REFERENCIA_REAL_DA_CONFIRMACAO_INDEPENDENTE>',
    approvedBy:'<RESPONSAVEL_REAL_QUE_CONFIRMOU_A_IDENTIDADE>',
    approvedAt:'<INSTANTE_REAL_UTC_ISO8601>',
    expiresAt:'<EXPIRACAO_REAL_UTC_ISO8601>',
    fingerprintSha256:'<SHA256_CONFIRMADO_INDEPENDENTEMENTE>',
    certificatePem:await fs.readFile(dir+'/olos-public.pem','utf8')
  });
  const file=await fs.open(dir+'/acquisition-pin-approval.json',constants.O_WRONLY|constants.O_CREAT|constants.O_EXCL|constants.O_NOFOLLOW,0o600);
  try {await file.chmod(0o600);await file.writeFile(JSON.stringify(approval,null,2)+'\n');await file.sync();}finally{await file.close();}
  const parent=await fs.open(dir,constants.O_RDONLY|constants.O_DIRECTORY|constants.O_NOFOLLOW);
  try {await parent.sync();}finally{await parent.close();}
  console.log('Aprovacao fornecida pelo operador validada e registrada; nenhuma conexao executada.');
} catch {
  process.stderr.write('Aprovacao nao registrada ou gravacao inconclusiva: revisar dados reais, validade, arquivo existente e permissoes.\n');
  process.exitCode=1;
}
JS
```

Esse comando valida criptografia/formato/validade, não autentica por si só a declaração do operador. O arquivo de política deve ficar protegido; não rodar com dados inventados. Não sobrescreve aprovação existente. Se a gravação falhar depois de abrir o arquivo, preservá-lo para inspeção administrativa em vez de repetir ou removê-lo automaticamente.

O caminho final fica no volume privado já existente, sem novo mount:

```text
/home/node/.local/state/olos-worker/tls-observed/acquisition-pin-approval.json
```

Depois, identificar o Source do bind e editar o `attempts-corporate.json` **existente**, fora do container readonly:

```bash
sudo docker inspect "$WORKER" --format '{{range .Mounts}}{{if eq .Destination "/home/node/.config/olos-worker"}}{{println .Source}}{{end}}{{end}}'
# Usar o caminho Source real impresso acima, sem presumir o diretório do Dokploy:
sudoedit <SOURCE_REAL>/attempts-corporate.json
```

Preservar endpoint/OEC/catálogos/perfil. Definir `pinApprovalFile` com o caminho acima; preencher a autorização real, com o procedimento de recuperação já estabelecido, no formato:

```json
{
  "operator": "<OPERADOR_REAL>",
  "reference": "<REFERENCIA_REAL_DA_JANELA_AUTORIZADA>",
  "issuedAt": "<INSTANTE_REAL_UTC_ISO8601>",
  "expiresAt": "<EXPIRACAO_REAL_UTC_ISO8601_ATE_12H>",
  "sourceTimeZone": "<FUSO_CONFIRMADO_PELA_OLOS>",
  "partitionPolicy": "ACQUISITION_DAY_CONTROL_BUCKET",
  "sharedConsumptionEvidence": "<REFERENCIA_REAL_DA_COORDENACAO_OU_ATRIBUICAO>",
  "recoveryEvidence": "<REFERENCIA_REAL_DO_PROCEDIMENTO_COMPLETO_LOG_REREAD_PERSISTENCIA>",
  "failurePolicy": "STOP_PRESERVE_CASE_RECONCILE_LOG_REREAD_NO_RESET"
}
```

Esse objeto é o valor de `authorization`, não substitui o arquivo inteiro. Manter `contractMode=observe`, sem `current`/`previous`. Manter arquivo 0600 UID/GID 1000:1000; não fazer chown/chmod recursivo, alterar credenciais ou reiniciar serviços. Conferir somente os metadados de permissão com `stat` no Source real.

Validar o pin registrado sem rede e conferir o estado:

```bash
sudo docker exec "$WORKER" node --input-type=module -e 'import {loadOlosPinApproval} from "/app/dist/security/olos-certificate-pin.js"; try {const p=await loadOlosPinApproval("/home/node/.local/state/olos-worker/tls-observed/acquisition-pin-approval.json");console.log(JSON.stringify({purpose:p.purpose,expiresAt:p.expiresAt,fingerprintSha256:p.fingerprintSha256}));}catch{console.error("PIN_APPROVAL_REJECTED");process.exitCode=1;}'
sudo docker exec "$WORKER" npm run worker:attempts -- status
sudo docker exec "$WORKER" npm run worker:local -- check
```

Só após autenticação **e** recuperação reais, ausência de pendência, baseline privado e espaço confirmados, executar uma única vez:

```bash
sudo docker exec "$WORKER" npm run worker:attempts -- once
```

O resultado deve ser quarentena completa, não persistência automática. Em parcial/erro com intenção, conservar a intenção original e acionar o procedimento externo acima, sem repetir. Em captura selada, seguir a revisão do contrato e aprovação descritas neste runbook; somente depois executar `sudo docker exec "$WORKER" npm run worker:attempts -- recover` para replay local, conferir IDs/recibos e preservar preexistentes. Não iniciar `start` ou qualquer scheduler.
