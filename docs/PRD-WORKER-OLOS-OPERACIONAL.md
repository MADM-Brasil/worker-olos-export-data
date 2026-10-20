# PRD executivo — Worker Olos operacional

## Resultado pretendido e limite desta entrega

Reutilizar a composição existente: TDS exato → spool completo/selado → envelopes/outbox/checkpoint de ingestão → projeção das 16 colunas → recibos de destino → próxima procedure. Uma chamada por ciclo, CurrentDay/PreviousDays alternados, espera de 60 s após conclusão. Preservar os 570 históricos por ON CONFLICT DO NOTHING.

Antes de alterar código, a inspeção confirmou que a composição, alternância, scheduler e recuperação local já existem. Os impedimentos atuais não são a porta HTTP nem a ausência de outro pipeline. São o contrato efetivo da resposta, TLS autenticado e uma política recuperável para consumo remoto. Não haverá aquisição corporativa durante mudanças/testes nem declaração de funcionamento real sem recibos. Não criar configuração/aprovação fictícia para eliminar CORPORATE_CONFIGURATION_MISSING.

## Classificação dos bloqueios

| Local / controle | Classe | Decisão |
| --- | --- | --- |
| CLI: repetir work-dir/config/intervalo/SYNC_ENABLED=false em toda execução | Excessivamente restritivo | Defaults apenas na CLI: caso first-run, configuração privada padrão, 60 s e SYNC_ENABLED=false quando ausente. SYNC_ENABLED=true continua recusado. Não inicia nada no boot. |
| CLI: frase --confirm além do comando manual once/start e da autorização privada obrigatória, vinculada ao digest | Redundante | A invocação explícita de once/start é o ato operacional; autorização privada vigente e todos os gates continuam obrigatórios. Aceitar as frases antigas quando fornecidas, recusando frases erradas. recover permanece exclusivamente local. |
| Configuração: copiar o perfil de 16 colunas para outro JSON | Redundante | Aceitar profileFile privado como alternativa ao profile embutido. Resolver pelo parser existente, vinculando seu conteúdo ao mesmo digest. Recusar ambos ao mesmo tempo. |
| corporate.ts: contrato completo antes da primeira resposta | Excessivo para observação | `contractMode: observe` permite capturar a resposta exata em quarentena, sem contrato fictício. Aprovação continua obrigatória para ingestão/projeção. |
| corporate.ts: PreviousDays obrigatório para CurrentDay once | Redundante | PreviousDays opcional em once, obrigatório em start; layouts são validados independentemente. Não inferir que são iguais. |
| primeira confirmação invalidada apenas por renovação administrativa ou inclusão de PreviousDays | Redundante | Preservar recibo original e conferir escopo de aquisição/mapeamento; mudanças no contrato CurrentDay ou catálogos continuam invalidando esse escopo. |
| corporate.ts: autorização temporal, fuso/data e identidade do plano | Necessário nesta liberação controlada | Manter; evitar automatizar renovações ou inventar a data de exportação de faixas de PreviousDays. Não é autorização permanente. |
| contracts.ts: requisitos de layout, controle compartilhado e recuperação | Necessário | Manter como dependências técnicas, não flags a silenciar. As referências de texto não comprovam a semântica da Olos. |
| attempts-access.ts: pin aprovado, endpoint/procedure fixos, intenção fsync, socket/login/RPC únicos | Necessário | Manter. Diagnóstico internal-homologation não autoriza aquisição. Sem Reset, retry remoto ou redirect. |
| CLI: first-confirmed + intenção/recibo compatíveis antes de start | Necessário | Manter como primeira validação ponta a ponta; não derivar aprovação dos 570 históricos. |
| large-batch.ts: aprovação e igualdade do contrato PostgreSQL, permissões, hashes, lease/fencing e recibos | Necessário | Manter. Aprovação de mapeamento local e contrato de aquisição têm finalidades distintas. Não autoaprovar nem migrar no boot. |
| pipeline: validações locais detectáveis apenas depois de gravar intenção remota | Excessivamente restritivo | Fazer preflight local antes de emitir o permit/intenção remota; repetir no coordenador protege contra mudanças entre verificação e consumo. Uma falha local não deve parecer uma aquisição remota incerta. |
| lease expirada, parcial, COMMIT sem ACK, pendência global | Necessário | Manter bloqueio e conciliação; nenhuma limpeza automática para liberar novo consumo. |
| Revalidação no transporte, coordenador e transação | Necessário | Manter: fecha janelas entre preparo, leitura e commit, não é mera duplicação burocrática. |

## Menor implementação

1. Defaults da CLI e remoção da confirmação textual duplicada, preservando autorização privada, TLS e capacidades de tentativa única.
2. Referência ao perfil existente, sem copiar metadados de fixtures ou criar nova versão de configuração.
3. Preflight local anterior ao claim remoto e erros sanitizados específicos para contrato/permissão, sem alterar o protocolo transacional.
4. Testes direcionados dos defaults, do perfil e da ordem preflight/intenção, além de regressões de alternância/recuperação. Nenhum novo scheduler, transporte ou serviço.

## Dependências externas e sequência para liberação

- Metadados atuais das respostas: obter do DBA ou, após TLS autenticado e recuperação estabelecida, capturar uma resposta consumptiva autorizada em quarentena. O parser observa tipos, ordem, nulos, tamanhos e collation antes de interpretar valores. Observação não aprova contrato nem entrega registros. Ver procedimento atualizado em `corporate-attempts-worker.md`.
- PEM/fingerprint aprovados independentemente para OLOS_ATTEMPTS_TDS_ACQUISITION. A conexão diagnóstica criptografada sem autenticar o servidor não basta.
- Confirmar fuso e coordenação de consumidores. Estabelecer como atribuir intervalos do Log ao caso, DateExport real, inclusividade, retenção e completude de ReRead. O Log não prova autoria. A capacidade corporativa de recuperação remota existente é preparada/bloqueada: sua execução exige liberação separada; não é recuperador operacional certificado.
- Aprovar o contrato comprovado no PostgreSQL local por administrador; manter as roles sem permissão de autoaprovação. Última verificação local nesta conversa encontrou apenas AttemptsRawData/pending, não aprovado. Não criar checkpoints retroativos para os 570 históricos.
- Preencher configuração privada 0600 por `preparationFile`, sem repetir credenciais/perfil/catálogos. `observe` dispensa planos prévios; ingestão exige CurrentDay aprovado; recorrência exige também PreviousDays aprovado. TLS e autorização/recuperação reais continuam indispensáveis.

## Recuperação mínima e riscos

Antes do RPC: intenção exclusiva/fsync e run/spool identificados. Durante leitura: resposta parcial nunca avança checkpoint. Depois do selo: recuperar spool/outbox localmente, sem nova consulta Olos. Commit sem ACK: verificar recibos/digests, sem repetir consumo. Spool ausente/parcial, lease vencida ou hard crash: parar, preservar evidências e conciliar externamente com LogReadData/ReReadData, mediante autorização própria. Não apagar remote-pending.json nem presumir posse de faixas. Sem mecanismo/evidência operacional de conciliação, não autorizar consumo.

Não existe transação distribuída SQL Server/PostgreSQL/filesystem. A intenção local identifica a tentativa, mas não recupera sozinha registros consumidos antes do selo. Defaults e retirada de uma frase duplicada não resolvem esse risco. Autorização expirada, alteração da configuração/catálogos ou mudança do dia continuam exigindo revisão conforme o contrato atual.

## Operação depois da liberação

Com configuração privada legítima já aprovada (sem SYNC_ENABLED=true):

```bash
npm run worker:attempts -- once
npm run worker:attempts -- start
npm run worker:attempts -- status
npm run worker:attempts -- recover
```

once executa uma tentativa CurrentDay sem scheduler/retry. start exige confirmação integral de once na mesma configuração e alterna PreviousDays/CurrentDay. Cada procedure roda aproximadamente a cada 120 s mais processamento. Parar com Ctrl+C/SIGTERM ao PID conhecido. recover não apaga a necessidade de revisão do caso nem chama Log/ReRead. Não executar start simultaneamente com outro HTTP na porta configurada.

Status local de arquivos não comprova conexão/ingestão. Com start autorizado, usar worker:local status para agregados HTTP privados e /health/live, /health/ready em loopback. Readiness não vira sucesso por simples presença de arquivos.

## Critérios de aceite

| Critério | Evidência necessária / situação antes da implementação |
| --- | --- |
| TLS corporativo autenticado e login | Pendente: diagnóstico temporário não valida identidade. |
| CurrentDay real e resposta completa | Pendente; proibido executar durante implementação. |
| Spool, transformação, recibos e checkpoint | Implementados/testados isoladamente; primeira execução corporativa pendente. |
| Preservar os 570 históricos | Writer existente DO NOTHING; última contagem local confirmada nesta conversa 570 IDs únicos, 218 CPF, 264 e-mail. |
| Confirmar novos IDs e recibos | Pendente; contagem por data sozinha não comprova chegada nova. |
| Alternância sem sobreposição e parada diante de incerteza | Código existente e testes; execução corporativa recorrente pendente. |
| Recuperação | Local implementada; sem selo exige conciliação real ainda não comprovada. |

Validação desta entrega: typecheck, build, testes direcionados e status sem aquisição. Nenhuma chamada corporativa, DDL, alteração de históricos, commit/push/deploy.

## Implementação e resultados efetivos (Fases B/C)

Criados este PRD e `test/attempts-preflight-diagnostics.test.ts`. Alterados `adapters/attempts-corporate.ts`, `src/integration/attempts/pipeline.ts`, `src/persistence/large-batch.ts`, os testes corporativos de runtime/diagnóstico e `docs/corporate-attempts-worker.md`. Não houve migration, nova arquitetura, novo serviço ou alteração do scheduler/transporte.

- Typecheck e build: aprovados.
- 21 testes direcionados de CLI, binding, preflight e ciclo: aprovados.
- 33 regressões de pipeline/persistência em PostgreSQL descartável e gates TDS: aprovadas. Total: **54 testes, sete arquivos**. Nenhum SQL Server corporativo foi consultado.
- `npm run worker:attempts -- status`: executado sem flags e sem erro de argumentos; reportou configuração ausente, nenhuma intenção pendente, nenhuma primeira aquisição e `acquisitionAuthorized=false`. Não iniciou HTTP, scheduler ou conexão à origem.
- Perfil privado indicado no PRD: existe, 0600, exatamente as 16 colunas, telefone normalizado e TMA desabilitado. Catálogos de 51 campanhas/126 tabulações previamente validados permanecem disponíveis; não foram copiados ou modificados.
- Verificação local, com credenciais privadas e `BEGIN READ ONLY`: `olos_worker` conserva **570 linhas/570 IDs únicos, 218 CPF, 264 e-mail**. Contrato de AttemptsRawData continua somente `pending`, não aprovado, sem campos/chaves; nenhuma run foi criada. Nenhuma aprovação/JSON corporativo foi fabricado.

Situação final dos critérios: infraestrutura de execução/ciclo/recuperação local validada; primeira aquisição corporativa, autenticação TLS de aquisição, confirmação de novos IDs/recibos e recorrência corporativa **pendentes**. A simplificação não torna as dependências externas burocráticas nem autoriza utilizar TLS não autenticado para consumo. O worker não está declarado operacionalmente concluído.

## Acompanhamento e comprovação da primeira execução futura

Depois de fechar as dependências externas e preencher legitimamente a configuração privada:

```bash
cd ~/worker-olos-export-data
npm run worker:attempts -- once
# Só após acquisitionPerformed=true e confirmação de todas as entregas:
npm run worker:attempts -- start
# Em outro terminal:
npm run worker:attempts -- status
npm run worker:local -- status
curl -s http://127.0.0.1:3100/health/live
curl -s http://127.0.0.1:3100/health/ready
```

Não iniciar start se once falhar ou o caso ficar pendente. Parar no terminal do processo com Ctrl+C; usar `npm run worker:attempts -- recover` somente para spool/outbox locais. Recuperação concluída não autoriza apagar manualmente a pendência global sem conciliação/revisão.

Consultas futuras com as credenciais protegidas, nunca senha no comando. Com `olos_receiver_login`, antes/depois:

```sql
BEGIN READ ONLY;
SELECT count(*) AS chamadas, count(DISTINCT internal_id_olos) AS ids_unicos,
       count(cpf) AS com_cpf, count(email) AS com_email
FROM raw.olos_calls;
SELECT data_ligacao::date AS dia, count(*) AS chamadas
FROM raw.olos_calls GROUP BY data_ligacao::date ORDER BY dia DESC;
COMMIT;
```

Com `olos_ingest_login`, substituir `BATCH_UUID` pelo batchId técnico do manifesto/log, sem imprimir records.ready ou payload:

```sql
BEGIN READ ONLY;
SELECT b.id, b.run_id, b.state, b.received_count, b.persisted_count,
       b.duplicate_count, b.rejected_count, r.record_count,
       c.version AS checkpoint_version, c.last_batch_id
FROM olos_export.batches b
JOIN olos_export.large_batch_receipts r ON r.batch_id=b.id
JOIN olos_export.checkpoints c ON c.stream_id=b.stream_id
WHERE b.id='BATCH_UUID'::uuid;
COMMIT;
```

Com `olos_project_login`, recuperar apenas IDs técnicos projetados/estado/recibo esperado:

```sql
BEGIN READ ONLY;
SELECT delivery_id, state, request_digest,
       request_payload->>'internal_id_olos' AS internal_id_olos
FROM olos_export.projection_outbox
WHERE batch_id='BATCH_UUID'::uuid
  AND destination_id='local.olos_worker.raw.olos_calls'
ORDER BY delivery_id;
COMMIT;
```

Com `olos_receiver_login`, conferir cada delivery_id/digest obtido acima em `olos_export.delivery_receipts`, filtrando pelo mesmo destination_id e estado `confirmed`, e conferir a presença dos IDs em raw.olos_calls. Comparar os IDs com o conjunto original capturado antes de once; contagem por dia/maior ID ou recibo de entrega sozinhos não distinguem inserção nova de ON CONFLICT DO NOTHING. Preservar o baseline privado das 16 colunas dos 570 IDs para comparação de integridade após a primeira aquisição. Nenhuma destas consultas futuras equivale a um teste corporativo já executado.
