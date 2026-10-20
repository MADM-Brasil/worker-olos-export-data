# Worker corporativo Attempts: uma chamada manual e ciclos de 60 segundos

Implementação local de 19/10/2026. **Não está ativo e não houve consulta/procedure corporativa nesta entrega.** O novo caminho conecta a fonte corporativa ao pipeline e runtime existentes; execução depende de configuração privada aceita e confirmação manual inicial no PostgreSQL.

Simplificação operacional posterior: ver [PRD executivo](PRD-WORKER-OLOS-OPERACIONAL.md). Os comandos `npm run worker:attempts -- once|start|status|recover` usam o caso `first-run`, o arquivo privado padrão e intervalo de 60 s; não é necessário repetir `--config`, `--work-dir` ou `SYNC_ENABLED=false` quando a variável está ausente. Um `SYNC_ENABLED=true` explícito continua sendo recusado. A ação manual substitui apenas a frase duplicada `--confirm`: autorização privada vigente, aprovação TLS e todos os controles continuam obrigatórios. As flags antigas permanecem compatíveis; uma confirmação incorreta é recusada. Nenhum comando gera sua própria aprovação.

## O que mudou

- `adapters/attempts-corporate.ts` / `npm run worker:attempts`: once, start, status e recover; somente destino local já preparado.
- `src/integration/attempts/corporate.ts`: configuração aceita, binding registrado, alternância CurrentDay/PreviousDays e callbacks de intenção/confirmação duráveis.
- `attempts-access.ts`, `connection.ts`, `source.ts`: permit de uma única tentativa, conexão/socket/login/RPC sem repetição e validação TLS com o pin existente. Preparar endpoint continua não autorizando conexão.
- `pipeline.ts`: hooks na composição existente e recuperação exclusivamente local de spool/outbox. Reutiliza LargeBatchCoordinator, LocalSpool, PostgresLargeBatchPersistence, projectTdsAttempts, ProjectionDispatcher e PostgresDeliveryTarget.
- `service.ts`: identifica sourceKind=corporate-local quando esse binding foi explicitamente autorizado.
- `pools.ts`: ingestão limitada a três conexões (lock, transação e saúde); projeção/destino continuam com duas. Mesmos três logins, sem superusuário.
- `contracts.ts`: retirado somente o bloqueio de software “capacidade não emitível nesta release”, pois agora existe emissor restrito; pendências de configuração/evidência continuam impedindo execução.
- `test/attempts-corporate-runtime.test.ts`, package.json, README e este guia.
- `corporate-recovery.ts`: adaptação de tipo do guard; LogReadData/ReReadData continuam sem permit automático.

Não houve nova migration, DDL, alteração das 16 colunas, seed de checkpoint, alteração dos arquivos históricos ou uso do pool de diagnóstico para escrita. `worker:local start` mantém o comportamento seguro sem aquisição; `.env` sozinho não ativa o novo caminho.

## Configuração privada necessária

Criar/revisar, fora do repositório e com permissão 0600, por exemplo:

`~/.config/olos-worker/attempts-corporate.json`

O schema efetivo é `corporateConfigSchema` em `src/integration/attempts/corporate.ts`. Não foi criado nenhum arquivo real de aprovação/credencial nesta tarefa. Campos:

Também é possível usar `preparationFile` absoluto apontando para `~/.config/olos-worker/attempts-corporate.preparation.json`, com permissão privada. A CLI importa somente `available.version`, `endpoint`, `profileFile`, `campaignFile` e `dispositionFile`, evitando duplicar credenciais/perfil/catálogos no arquivo final. `pinApprovalFile` e `authorization` precisam de evidência real. Ingestão exige `current`; `previous` é obrigatório somente para `start`. O modo explícito `contractMode: "observe"` dispensa ambos e apenas captura a resposta em quarentena. Nenhum contrato, aprovação ou autorização é importado da preparação. O wrapper continua não executável.

`authorization.layoutEvidence` tornou-se opcional porque a evidência já é obrigatória em `plan.contract.layout_evidence` e comparada ao contrato PostgreSQL. Se fornecida, ainda precisa coincidir. As referências de recuperação/coordenação podem ser IDs curtos não vazios (não se exige um tamanho documental arbitrário). `partitionPolicy` e `failurePolicy` têm os mesmos valores fixos como defaults seguros; não são declarações de que a recuperação foi validada. Nenhum desses ajustes dispensa evidência real, TLS aprovado ou aprovação PostgreSQL.

| Campo | Conteúdo obrigatório |
| --- | --- |
| version | olos-corporate-local/v1 |
| endpoint | objeto estrito com server=10.196.234.11, port=1433, database=ExportDataV3, user=OEC, password privado |
| pinApprovalFile | caminho absoluto do PEM/fingerprint aprovados, propósito OLOS_ATTEMPTS_TDS_ACQUISITION |
| current | `{plan: LargePlan, fields: TdsField[]}` aceitos para ReadDataCurrentDay |
| previous | o mesmo formato, para ReadDataPreviousDays; opcional em once, obrigatório em start |
| profile ou profileFile | perfil local já homologado embutido, ou caminho absoluto privado 0600 para `~/olos-offline-homologation/profile-16-columns.json`; nunca ambos. Exatamente 16 colunas/ordem, phoneMatchKey=normalized e tmaPolicy=disabled |
| campaignFile/dispositionFile | caminhos privados de snapshots completos/autorizados dos catálogos locais, consumidos pelos parsers existentes |
| authorization | objeto de aceite operacional descrito abaixo |

Cada plano deve conter exclusivamente TableName=AttemptsRawData, PK AttemptsRawDataID, successReturnValues=[0], readType current_day ou previous_days respectivamente e pkRange=null. Os contratos das duas procedures são independentes; não presumir igualdade. fields inclui os metadados TDS efetivos, como tamanho SQL/collation, compatíveis com os WireFields. **Não usar attemptsFixturePlan nem inventar nulabilidade/codepage.** Versões fixture/synthetic/pending são recusadas. Para ingestão, o contrato precisa estar aprovado e compatível em `olos_export.source_contracts`; o worker não o aprova automaticamente.

`profileFile` é resolvido na CLI para o mesmo `profile` canônico existente antes de criar o binding/digest. Não é uma nova aprovação: reutiliza o conteúdo homologado, que continua validado. Perfil ausente/inválido retorna CORPORATE_PROFILE_MISSING/INVALID. Catálogos ausentes/inválidos retornam CAMPAIGN_CATALOG_MISSING/INVALID ou DISPOSITION_CATALOG_MISSING/INVALID. No preflight, SOURCE_CONTRACT_MISSING/NOT_APPROVED/MISMATCH/INVALID distinguem o contrato local; privilégios insuficientes e conexão somente leitura também têm códigos próprios. Esses diagnósticos não promovem o contrato nem concedem permissões.

authorization contém exatamente:

```json
{
  "operator": "RESPONSAVEL",
  "reference": "REFERENCIA_DO_ACEITE_OPERACIONAL",
  "issuedAt": "INSTANTE_UTC_ISO8601",
  "expiresAt": "INSTANTE_UTC_ISO8601",
  "sourceTimeZone": "FUSO_IANA_CONFIRMADO_DA_INSTALACAO",
  "partitionPolicy": "ACQUISITION_DAY_CONTROL_BUCKET",
  "layoutEvidence": "REFERENCIA_AOS_METADADOS_ATUAIS_ACEITOS",
  "sharedConsumptionEvidence": "REFERENCIA_A_COORDENACAO_DOS_CONSUMIDORES",
  "recoveryEvidence": "REFERENCIA_A_POLITICA_OPERACIONAL_DE_CONCILIACAO",
  "failurePolicy": "STOP_PRESERVE_CASE_RECONCILE_LOG_REREAD_NO_RESET"
}
```

É uma declaração operacional privada, não uma prova automática da semântica da Olos. O binding corporativo não marca tlsVerified/recoveryValidated como verdadeiros por configuração: exige sua capacidade registrada, autentica TLS antes do RPC e aplica a política de parada/conciliação, sem afirmar recuperação remota já certificada. Não preencher essas referências apenas para passar validação. A aprovação do certificado deve ser independente da observação anterior. Ver [formato/rotação do pin](olos-certificate-pinning.md). Uma aprovação de diagnóstico METADATA_ONLY não autoriza aquisição.

Janela operacional máxima de 12 horas, limitada também pela validade do pin e pelo dia configurado. A autorização não é renovada automaticamente. Os planos devem usar o dia corrente no fuso aprovado da instalação; a execução para se mudar o dia ou expirar a aprovação. Renovação ou mudança de configuração/catálogos exige novo aceite/caso e primeira chamada manual. Não é uma liberação permanente de produção.

**Semântica explícita de data:** neste modo, `run.exportDate` é o bucket técnico do dia de aquisição, diferenciado por readType, não um filtro enviado à Olos. ReadDataCurrentDay/PreviousDays não recebem DateExport. PreviousDays pode retornar registros de diferentes datas e todos devem ser preservados. A data real da chamada continua StartDate, convertida pelo codec exato para TIMESTAMP(6). Não usar o bucket técnico como DateExport de LogReadData/ReReadData: a data efetiva de exportação de uma faixa precisa ser determinada na conciliação. Se essa política não puder ser aceita, não emitir a configuração. Não se promete recuperar todo um dia em uma chamada ou selecionar retrospectivamente 19/10 por meio de CurrentDay.

## Diagnóstico local sem conexões

```bash
SYNC_ENABLED=false npm run worker:attempts -- status
```

Sem flags, inspeciona a configuração privada `~/.config/olos-worker/attempts-corporate.json` e o caso `~/.local/state/olos-worker/corporate-attempts/first-run`. Aceita `--config` e `--work-dir` para outros arquivos/casos permitidos. Não cria diretórios, não carrega credenciais PostgreSQL e não acessa bancos nem a Olos. Arquivos ausentes ou inválidos são reportados em JSON sanitizado, sem encerrar o diagnóstico com erro genérico.

O resultado informa `configuration`, `tlsApproval`, `remoteIntentPending`, `firstAcquisitionConfirmed`, `blockingPreconditions` e `unverifiedPreconditions`. `null` indica informação não verificável; sem configuração válida, não se presume o caminho da aprovação TLS. A validação da configuração é estrutural (`schema-only`); arquivos presentes e recibos locais não certificam o handshake, o destino, o binding atual ou a recuperação. `acquisitionAuthorized` permanece `false` nesse diagnóstico. Para `once`/`start`, configuração não informada/inexistente retorna `CORPORATE_CONFIGURATION_MISSING`; JSON, permissões ou estrutura inválidos retornam `CORPORATE_CONFIGURATION_INVALID`, preservando a exigência de confirmação explícita.

## Primeira chamada manual autorizada

Previamente: PostgreSQL local existente, três credenciais em `~/.config/olos-worker/postgres-credentials.json`, env local com SYNC_ENABLED=false, schema/roles/contrato aprovados, catálogos e aprovação TLS válidos. Não recriar banco/volume nem tocar em run-001/run-002. Tirar baseline dos históricos antes da primeira chamada, com consulta somente leitura e sem imprimir seus valores pessoais.

Escolher um novo caso sob o diretório abaixo; outros caminhos são recusados. Não usar outro diretório para substituir uma tentativa incerta.

```bash
cd ~/worker-olos-export-data
export ATTEMPTS_CASE="$HOME/.local/state/olos-worker/corporate-attempts/first-run"
export ATTEMPTS_CONFIG="$HOME/.config/olos-worker/attempts-corporate.json"

SYNC_ENABLED=false npm run worker:attempts -- once \
  --work-dir "$ATTEMPTS_CASE" \
  --config "$ATTEMPTS_CONFIG" \
  --confirm AUTHORIZE_ONE_ATTEMPTS_CURRENT_DAY
```

Envia no máximo um RPC ReadDataCurrentDay, sem scheduler ou retry. Uma falha de conexão também gasta a tentativa. O endpoint fixo, assinatura/parâmetros e configuração são conferidos de novo no transporte. Certificado divergente, criptografia incompatível, redirect/segunda conexão, erro/timeout/resposta parcial ou divergência de contrato impedem a confirmação.

A mensagem manual_cycle_completed só deve ser interpretada como aquisição se acquisitionPerformed=true. Um ciclo pode apenas drenar entregas locais preexistentes; nesse caso não cria a confirmação inicial nem habilita start. Uma resposta vazia completa não é falha e não autoriza presumir que todo o histórico foi exportado.

## Iniciar recorrência e consultar estado

Somente depois de `once` confirmar a aquisição e todas as entregas de negócio:

```bash
SYNC_ENABLED=false npm run worker:attempts -- start \
  --work-dir "$ATTEMPTS_CASE" \
  --config "$ATTEMPTS_CONFIG" \
  --interval-ms 60000 \
  --confirm AUTHORIZE_ATTEMPTS_RECURRING
```

Usa AttemptsService/AttemptsCycleRunner/Scheduler e o mesmo HTTP, em primeiro plano. Não iniciar simultaneamente outro serviço na porta 3100. A confirmação inicial é vinculada ao digest da configuração, pin e catálogos; não deriva dos 570 históricos nem de uma flag de ambiente.

Depois da aquisição manual CurrentDay, o primeiro ciclo automático é PreviousDays, seguido de CurrentDay, em alternância. Cada ciclo contém uma só procedure. O intervalo é contado **após concluir** o ciclo anterior, sem sobreposição: com alternância, cada procedure roda aproximadamente a cada 120 segundos mais o tempo de trabalho. Intervalo permitido na CLI: 60.000–3.600.000 ms.

```bash
npm run worker:local -- status
curl -s http://127.0.0.1:3100/health/live
curl -s http://127.0.0.1:3100/health/ready

SYNC_ENABLED=false npm run worker:attempts -- status --work-dir "$ATTEMPTS_CASE"
```

Status HTTP usa o token privado existente e só mostra agregados. O modo corporativo registra sourceKind=corporate-local; readiness/syncEnabled só passam após um ciclo deste processo concluir aquisição e entrega. Não há probe SQL de saúde na Olos. O status de arquivos também funciona com o processo parado e mostra apenas IDs técnicos, procedure/data de controle/confirmação. Não é prova de compatibilidade da instalação.

## Intenção, spool e confirmação

A exclusão de sessão PostgreSQL `(740219,1)` é compartilhada com o runtime local existente. Protege os processos deste worker; não controla consumidores externos na Olos.

Antes de cada tentativa: `remote-pending.json` exclusivo no diretório-base e `attempt-UUID.json` no caso, ambos fsync e diretório sincronizado. O emissor verifica e sincroniza novamente esses arquivos antes do permit; um callback/booleano em memória não basta. A intenção contém procedimento, digest, responsável, referência, pin e data técnica, sem senha ou payload pessoal.

O pipeline corporativo agora verifica o preflight local de ingestão antes de gravar essa intenção. O coordenador repete a verificação antes de startRun/leitura e a persistência revalida o contrato dentro da transação. Isso evita claims remotos desnecessários por falhas locais já conhecidas; não elimina possíveis mudanças/falhas posteriores nem autoriza apagar um claim conservador.

O coordenador existente registra run/checkpoint esperado e cria o spool durável antes da leitura. Backpressure e limites existentes permanecem (até 200.000 linhas, quota de spool). Só aceita uma resposta completa, metadados previstos, retorno 0, outputs nulos/vazios e integridade do spool. Depois de selo/fsync: envelopes, recibo/outbox e checkpoint de ingestão em transação PostgreSQL. Projeção usa os catálogos locais, projectTdsAttempts e parser XML/normalizadores existentes. CPF/e-mail válidos são extraídos de CampaignData; nenhuma PII vai para logs.

O writer recebe preserveExisting=true: ON CONFLICT(internal_id_olos) DO NOTHING, preservando todos os campos dos 570 históricos. Nas novas linhas, lead_id_origem, tma_origem, razao_desligamento e raw_olos_call_id ficam NULL. Não consulta Users nem usa SDK/HubSpot/discadora; agentes sem catálogo autorizado continuam NULL, sem filtrar suas chamadas.

Somente após todas as intenções de destino confirmarem recibo/commit, grava confirmed-UUID e a confirmação inicial e remove a pendência global com fsync. Checkpoint de ingestão não equivale a confirmação de negócio. Não se presume atomicidade SQL Server↔PostgreSQL↔filesystem; a janela entre consumo remoto e spool continua exigindo conciliação. Se a persistência ou ACK falhar, o caso global permanece e o runner para, sem nova chamada automática.

Não exibir records.ready/records.part ou payloads. Para acompanhar somente metadados:

```bash
watch -n 2 'find "$ATTEMPTS_CASE/spool/batches" -name manifest.json -exec jq -c "{batchId:.identity.batchId,runId:.identity.runId,state,sealed,rows,bytes}" {} \;'
```

## Parar e recuperar

Parar no terminal com **Ctrl+C**, ou enviar **SIGTERM apenas ao PID conhecido desse processo**. AbortController solicita cancelamento/drenagem antes de fechar os pools. Não usar pkill, apagar claims/spool ou matar outros serviços indiscriminadamente.

Recuperação local de spool selado e/ou entregas já persistidas, sem configuração/validade TLS e sem chamada à origem:

```bash
SYNC_ENABLED=false npm run worker:attempts -- recover \
  --work-dir "$ATTEMPTS_CASE" \
  --confirm LOCAL_RECOVERY_ONLY
```

Usa o snapshot privado de mapeamento/catálogos capturado antes da tentativa. Não depende do certificado ainda estar válido para recuperar o PostgreSQL local. Partial/corrupção/lease vencida ou recibo inconclusivo continuam bloqueados. Mesmo com recuperação local concluída, a pendência global não é apagada automaticamente: é necessária revisão do caso e dos recibos antes de liberar outro consumo. Não há comando nesta entrega que silencie essa revisão.

Quando não existir spool completo, conservar tudo e conciliar pela data real/faixa de LogReadData/ReReadData, com autorização/revisão próprias e sem presumir autoria da faixa. O novo emissor **não emite permits para Log/ReRead/Reset/DataAvailable/Users**. Não há reprocessamento remoto automático nem garantia declarada de ausência de perda sem validação da origem.

## Verificação PostgreSQL

Com as credenciais locais protegidas e papel apropriado, sem senha em argumentos/logs:

```sql
BEGIN READ ONLY;
SELECT data_ligacao::date AS dia,
       count(*) AS chamadas,
       count(DISTINCT internal_id_olos) AS ids_unicos,
       count(cpf) AS com_cpf,
       count(email) AS com_email
FROM raw.olos_calls
GROUP BY data_ligacao::date
ORDER BY dia DESC;
COMMIT;
```

Comparar baseline anterior/posterior e conferir batchId, recibo de ingestão/checkpoint e projection_outbox confirmada. Uma contagem positiva não prova inserção nova; overlaps e replay preservam IDs existentes. Para provar os históricos intactos, comparar hash das 16 colunas sobre o **mesmo conjunto original de 570 IDs**, não sobre a tabela inteira que receberá linhas novas. Consultas adicionais por papel estão em [primeira aquisição](first-attempts-2026-10-19.md).

## Validação e bloqueios reais

Typecheck/build e **40 testes direcionados em sete arquivos** aprovados. Cinco testes novos exercitam permit durável/uma chamada, fsync falho, alternância/pendência, validade/contrato/mapeamento e autorização CLI. Regressões incluem o handshake TDS/TLS sintético pelo hook real da Connection, parser/projeção e pipeline com PostgreSQL descartável/três roles/preservação histórica sintética. Não foi executado RPC de SQL Server real nesta tarefa; os testes não certificam a Olos.

```bash
npm run typecheck
npm run build
npm test -- --testTimeout=30000 test/attempts-corporate-runtime.test.ts \
  test/attempts-corporate-pinning.test.ts test/local-automation.test.ts \
  test/attempts-cycle-runner.test.ts test/attempts-recurring.test.ts \
  test/tds-projection.test.ts test/olos-certificate-pin.test.ts
```

Persistem como pré-condições reais: configuração privada aceita com metadados atuais e política de data/conciliação; certificado aprovado para aquisição e TLS compatível; coordenação do consumo da Olos; schema/contrato aprovado e permissões PostgreSQL locais; primeira chamada manual confirmada para habilitar recorrência. Não foram verificadas contra a instalação nesta tarefa. A presença de referências no JSON não transforma suposições em fatos: esses aceites precisam corresponder a evidências reais.

Nenhuma procedure corporativa, alteração no PostgreSQL local persistente ou oficial, mudança de VPS/Dokploy/discadora/WireGuard/run-001/run-002, migration em ambiente persistente, commit, push ou deploy. Nenhuma sincronização foi iniciada. Os processos/containers temporários dos testes foram restritos às fixtures descartáveis.

## Atualização — descoberta na sessão e replay local (19/10/2026)

Arquivos desta atualização: `adapters/attempts-corporate.ts`, `adapters/local-spool.ts`, `src/integration/attempts/corporate.ts`, `src/integration/attempts/pipeline.ts`, `src/olos/tds/connection.ts`, `src/olos/tds/raw-parser.ts`, `src/olos/tds/source.ts`, `test/attempts-corporate-runtime.test.ts`, `test/attempts-corporate-diagnostics.test.ts`, novo `test/tds-response-observation.test.ts`, `docs/PRD-WORKER-OLOS-OPERACIONAL.md` e este documento. Nenhuma migration, scheduler ou tabela de negócio foi alterada.

O bloqueio de metadados prévios pode ser substituído por **observação autorizada em quarentena**. Isso não transforma a primeira leitura em consulta não consumptiva nem resolve a perda possível antes de o spool estar completo. TLS autenticado, coordenação de consumo e procedimento externo de conciliação continuam necessários ANTES da leitura.

No arquivo privado padrão, pode-se reutilizar `preparationFile` e declarar `contractMode: "observe"`, `pinApprovalFile` e `authorization` reais. Nesse modo não fornecer `current`/`previous`. A autorização conserva operador, referência, issuedAt/expiresAt (até 12h), fuso, sharedConsumptionEvidence e recoveryEvidence; os dois últimos precisam apontar para evidência verdadeira. O pin precisa de PEM/fingerprint aprovados independentemente para `OLOS_ATTEMPTS_TDS_ACQUISITION`, aprovação vigente e certificado válido. Nenhum arquivo autorizado foi gerado nesta entrega, pois essas evidências não existem localmente.

Sequência operacional, após resolver essas condições:

```bash
cd ~/worker-olos-export-data
# Preparar o arquivo privado real, sem copiar fixtures ou inventar aceites:
umask 077
"${EDITOR:-vi}" "$HOME/.config/olos-worker/attempts-corporate.json"
chmod 600 "$HOME/.config/olos-worker/attempts-corporate.json"
SYNC_ENABLED=false npm run worker:attempts -- status
SYNC_ENABLED=false npm run worker:attempts -- once
```

Em `observe`, `once` envia somente CurrentDay/AttemptsRawData, depois de probe PostgreSQL local/roles/schema, advisory lock e intenção fsync. `ExactConnection` entrega os bytes da resposta RPC ao `LocalSpool` **antes do parser**, aguardando a escrita por chunk. LOGIN7/credenciais não entram nesse arquivo. O parser preserva BIGINT/DECIMAL, DATETIME em ticks, textos e nulos; metadados não suportados interrompem a execução. Não há retry remoto. Os limites de disco/memória e reserva de espaço existentes são reaproveitados.

O caso fica em `~/.local/state/olos-worker/corporate-attempts/first-run/`: intenção `attempt-UUID.json`; `spool/batches/UUID/response.partial` ou `response.ready`; descritor privado `response.json` com SHA-256, contagem, campos observados e segurança do transporte. Não imprimir o payload. O descritor distingue explicitamente `metadataState=observed`, `contractApproved=false`, `businessDelivered=false`, `checkpointAdvanced=false`. Só resposta com um dataset, final DONEPROC, ReturnStatus 0 e ambos OUTPUTs nulos/vazios pode ser selada. String vazia e null permanecem distintos no descritor.

**Observar não aprova nem importa.** O operador revisa os campos/identidade realmente capturados, monta `current` com o contrato comprovado e retira `contractMode`. O administrador local registra o contrato real nas estruturas existentes; a role de ingestão não recebe permissão de autoaprovação. O plano deve usar a data de controle da intenção original; não inventar DateExport de registros anteriores. `previous` pode continuar ausente durante esse replay. Catálogos/perfil têm que corresponder ao snapshot privado salvo antes da aquisição.

```bash
# Após revisão/aprovação reais; replay dos bytes selados, sem conexão SQL Server:
SYNC_ENABLED=false npm run worker:attempts -- recover
SYNC_ENABLED=false npm run worker:local -- check
```

`recover` usa o mesmo parser exato e `LargeBatchCoordinator` → `PostgresLargeBatchPersistence` → `ProjectionDispatcher`/writer 16 colunas, com ON CONFLICT DO NOTHING. O SHA-256 e a contagem são verificados antes do evento de completude; divergência impede selo/commit. O spool de ingestão do replay fica em `replay-spool`, separado do payload original preservado. `replay-started.json` impede criar outra ingestão após resultado local incerto; um spool ativo usa a recuperação existente. `replay-confirmed.json` torna a repetição concluída um no-op, sem novo checkpoint/RPC. Janela de crash após commit e antes desse marcador exige revisão de recibos; não é resolvida apagando arquivos.

Após entrega confirmada e revalidação da configuração/pin vigentes, são gravados ACK e primeira confirmação, vinculando a intenção original ao escopo revisado. Claims de captura são renomeados e mantidos como evidência; a pendência global é retirada por último, com fsync. Renovar a autorização ou incluir um contrato PreviousDays independente não invalida o recibo CurrentDay; alterar esse contrato, perfil ou catálogos invalida o escopo. Se o pin expirar depois da entrega, o replay confirmado permanece salvo e a liberação da próxima leitura espera a revalidação, sem reimportar.

Recorrência exige também metadados/contrato reais de PreviousDays. Não aprovar esse layout por ser semelhante ao CurrentDay. Com os dois contratos, TLS e recuperação estabelecidos e primeira entrega confirmada:

```bash
SYNC_ENABLED=false npm run worker:attempts -- start --interval-ms 60000
# Ctrl+C ou SIGTERM ao PID desse processo para parada limpa.
```

O scheduler existente alterna uma procedure por ciclo e espera 60s após conclusão. O controle de data/fuso e a autorização temporal existentes continuam vigentes: isto ainda não certifica execução ininterrupta entre dias nem recuperação remota automática. Em parcial, VPN indisponível, lease expirada ou COMMIT incerto: preservar o caso e parar novas leituras. LogReadData/ReReadData corporativos continuam sem permit executável nesta CLI; a conciliação exige a etapa externa autorizada. O histórico de 01/10/2026, faixa 37150205–37150774, comprova somente aquele caso, não propriedade/completude de qualquer faixa futura. Nunca usar ResetReadData.

Consulta agregada, executada com conexão local protegida (sem senha no comando):

```sql
SELECT count(*) AS total, count(DISTINCT internal_id_olos) AS ids_unicos,
       count(cpf) AS com_cpf, count(email) AS com_email
FROM raw.olos_calls;
SELECT state, count(*) FROM olos_export.projection_outbox GROUP BY state;
SELECT version, last_batch_id FROM olos_export.checkpoints;
```

Validação desta atualização: typecheck/build e 71 testes direcionados em sete arquivos. Incluem captura antes do parser real da Connection, replay binário fragmentado, BIGINT/DATETIME/null, disco cheio, corrupção/parcial, quarentena, gates TLS e regressões de persistência. Não houve integração real SQL Server nesta atualização. Consultas somente leitura confirmaram PostgreSQL up/schema pronto, 570 registros/570 IDs, 218 CPFs, 264 e-mails, nenhuma entrega/checkpoint e somente contrato AttemptsRawData `pending` não aprovado. A configuração preparada tem endpoint/perfil/catálogos; não há pin válido para aquisição na pasta privada inspecionada nem arquivo corporativo final. A CLI status permanece anterior a qualquer conexão. Nenhuma aquisição corporativa ou sincronização foi realizada.
