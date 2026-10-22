# Auditoria local do pipeline Attempts — 22/10/2026

Escopo: código atual + arquivo histórico privado preservado + PostgreSQL de teste
criado exclusivamente pelo helper `isolatedPostgres`. Nenhuma chamada à Olos,
ativação, alteração da reconciliação, DML em banco existente, commit ou deploy.
Os documentos preexistentes não rastreados foram preservados.

## Evidência e limites

Entrada: `/home/madm/olos-offline-homologation/attempts.json`, SHA-256
`4ed44db3a4d08728f53f975345d929a1b4e2f28e7a9c7df20ea6fd3c99b553ca`.
570 registros, 570 IDs únicos, intervalo inclusivo 37150205–37150774, 18 campos.
O arquivo e os catálogos foram apenas lidos; os dados não estão no repositório.

A execução manual de ReReadData, returnValue=0 e ausência de erro foram informadas
pelo operador; não foram repetidas nem autenticadas por este teste. O JSON não é
uma captura TDS original. O teste reconstrói tokens **somente em memória**, com
metadados de fixture do layout documentado, collation Windows-1252 de teste e
DATETIME reconstruído por arredondamento ao tick de 1/300 segundo. Não promove
esses metadados a um contrato real nem fabrica evidência TLS. `Z` no JSON é
tratado como representação civil da exportação para esse ensaio, sem atestar o
fuso da Olos. Fidelidade ao tick original e à collation original permanece pendente.

O plano identifica `reread` e faixa histórica; a assinatura é apenas validada
localmente. O único source é `replayTdsResponse`, sem conexão/RPC. Nenhuma
aprovação/intent de CurrentDay/PreviousDays ou evidência TLS fictícia é criada.

## Caminho auditado

1. `createApprovedCaptureReplayBinding`: aceita somente source de replay registrado,
   plano válido, PK AttemptsRawDataID e perfil aprovado de 16 colunas. O vínculo é
   registrado no WeakMap existente; o teste não cria outro importador.
2. `createAttemptsPipeline`: preflight do contrato no banco isolado, intenção/run,
   captura do lote em LocalSpool, recibo de ingestão e outbox transacionais.
3. `isolatedTdsDelivery`: lê o envelope integral de 18 campos; projeta por
   `projectTdsAttempts`/`mapOffline`; entrega pelo dispatcher e receiver existentes.
4. `PostgresDeliveryTarget`: recibo e escrita de negócio na mesma transação.
   Commit sem ACK é reconciliado pelo recibo, sem refazer leitura na Olos.
5. `IsolatedTdsCallsWriter`: INSERT com parâmetros, PK real e `ON CONFLICT DO NOTHING`
   no serviço V2. Não sobrescreve registros existentes.
6. `confirmDeliveryCheckpoint`: exige recibo de lote e ACKs; avanço posterior à
   entrega, comparando versão/cursor. Repetição do finalizador é idempotente.

## Mapeamento aprovado de 16 colunas

| Destino | Origem/política atual |
|---|---|
| internal_id_olos | AttemptsRawDataID, inteiro exato; PK do destino |
| olos_call_id | CallId como texto, sem usá-lo como PK |
| data_ligacao | StartDate; ticks DATETIME → timestamp civil com 6 casas |
| telefone | OriginalPhoneNumber preservado |
| telefone_match_key | Telefone normalizado conforme phonePolicy |
| nome_agente | AgentId válido → snapshot sanitizado de agentes; ausente → NULL |
| id_campanha_olos | CampaignId como texto |
| nome_campanha_olos | Catálogo CampaignId → Description; ausente → NULL |
| id_tabulacao_olos | DispositionId como texto |
| nome_tabulacao_olos | Catálogo DispositionId → Description; ausente → NULL |
| cpf | CampaignData XML, nomes configurados, validação de dígitos verificadores |
| email | CampaignData XML, nomes configurados, validação sintática |
| lead_id_origem | NULL; sem equivalência aprovada com CustomerId/MailingRecordId |
| tma_origem | NULL; cálculo desabilitado |
| razao_desligamento | NULL; sem equivalência aprovada com DispositionId |
| raw_olos_call_id | NULL; sem equivalência aprovada com CallIdTelecom/CallId |

EndWrap, TableName, EndCall, Dnis, PhoneTypeId, Route, CustomerId, MailingRecordId,
MailingPhoneNumberId e CallIdTelecom permanecem nos envelopes/spool de origem;
não são copiados para colunas sem equivalência aprovada. AgentId e CampaignData
são usados na projeção e também preservados no envelope.

Nulos reais: 7 MailingRecordId e 7 MailingPhoneNumberId; nenhum nulo nos outros
16 campos deste arquivo. Nenhum default substitui esses nulos no envelope.
Contatos encontrados: 218 CPFs válidos e 264 emails; nenhum XML inválido/ambíguo,
nenhuma campanha/tabulação não encontrada nos catálogos preservados. Há 21 registros com AgentId positivo/válido. Não foi
localizado um snapshot histórico de agentes: os nomes ficam NULL, sem inferência
por email, Login, CallId ou outros campos. Campos adicionais de Campaign/Users
não são incorporados ao envelope de chamadas.

XML usa parser com limites e rejeita DTD; contatos ambíguos não são escolhidos
arbitrariamente. O fluxo operacional atual não bloqueia o lote por XML inválido:
CPF/email ficam NULL e o original é preservado. Os contadores XML pertencem à
projeção/homologação; não se deve afirmar que o status V2 já os publica.

## Problemas reproduzidos e correções mínimas

- Preflight aceitava `tma_origem NOT NULL`, incompatível com todas as entregas
  aprovadas. Teste falhou antes da correção. A mesma checagem física foi extraída
  para `assertAttemptsDestinationSchema`, usada pelo serviço, e agora valida
  nulabilidade, geração/identity, tipos/ordem, precisão temporal e PK.
- V2 construía sempre `agents:new Map()`, sem caminho para um snapshot legítimo.
  Teste de carregamento falhou antes da correção. `agentFile` privado e opcional
  reutiliza `parseAgentCatalog`/`agentMap`, somente AgentId/UserName. O conteúdo
  entra no digest e é revalidado antes de efeitos remotos. Ausência mantém a
  política NULL e o digest anterior. Não lê Users, não atualiza linhas existentes
  nem modifica configurações privadas. Configurar esse campo exige aprovação
  operacional da configuração nova; não se altera uma ativação existente em silêncio.

## Estrutura física observada

Consulta limitada a metadados/contagens, em READ ONLY, no alvo **local configurado**
`127.0.0.1:5434/olos_worker`: 16 colunas na ordem aprovada; internal_id_olos e
raw_olos_call_id bigint; data_ligacao timestamp(6) sem fuso; tma_origem integer;
demais text. Só a PK é NOT NULL; demais colunas aceitam NULL. PK exclusivamente
internal_id_olos; nenhuma coluna identity/generated, RLS desabilitado, nenhum
trigger de usuário e nenhum constraint adicional além da PK.

`AttemptsService.probe` passou com os três logins existentes e validações de
privilégios. Ledgers: schema_versions=[1,2], large_batch_schema_versions=[1],
projection_schema_versions=[1], delivery_schema_versions=[1].

**Esse banco local possui 570 linhas**, divergindo das zero linhas informadas
para a VPS. Não foi acessada a VPS nem seu banco: não se apresenta esta observação
como validação física da produção. `attempts-destination-readonly.sql` fornece a
consulta sanitizada para o alvo real. RLS/triggers/constraints novos ou alterações
após o preflight podem mudar a semântica; o código não prova ausência de mudanças
concorrentes nem autentica um documento externo de auditoria.

## Testes e repetição

```bash
OLOS_HISTORICAL_AUDIT_DIR=/home/madm/olos-offline-homologation \
  npm test -- --maxWorkers=2 --testTimeout=30000 --hookTimeout=120000
npm run typecheck
npm run build
git diff --check
```

Resultados desta auditoria: typecheck, build e diff-check aprovados; suíte
completa com a entrada histórica: **670 testes / 53 arquivos, zero falhas**;
verificação direcionada final com logs protegidos: **25 testes / 3 arquivos,
zero falhas**. Um ensaio adicional simultâneo à suíte excedeu o timeout de 30s
e afetou os casos seguintes; a repetição sem concorrência extra passou, sem
alterar o timeout nem o código de entrega. O digest do arquivo original foi
conferido novamente ao final e permanece igual.

Sem a variável, os três testes históricos ficam explicitamente skipped; não
substituem os dados por fixtures sintéticas. Com a variável, digest/quantidade/IDs
são conferidos antes de usar o lote. Erros SQL potencialmente sensíveis são
sanitizados e comparações de linhas retornam somente booleanos. O PostgreSQL
de teste inicia com logs de erros/statement/parâmetros desabilitados abaixo de
PANIC, para que falhas SQL não registrem linhas privadas no servidor. Os artefatos de
teste privados ficam em diretórios temporários 0700 e são removidos ao final.

Casos: 570 entregas e comparação das 16 colunas; nova ingestão dos mesmos IDs
produz 570 duplicatas e mantém 570 linhas; falha SQL real na persistência reverte
o lote e preserva spool selado; reconstrução do pipeline recupera localmente;
commit do receiver sem ACK preserva entregas pendentes/checkpoint e é reconciliado
por recibo; checkpoint avança somente após ACK e finalização repetida não avança
de novo. Asserções adicionais rejeitam NOT NULL incompatível, timestamp(3), PK
inexistente e snapshot de agentes alterado.

Scheduler: relógio controlado comprova que um ciclo de 480 segundos não sobrepõe
outro e que o próximo começa somente 120 segundos após terminar. Incerteza no
ciclo interrompe o runner; sem retry remoto. São testes do código local, não uma
medição do processo instalado na VPS.

## Critérios para a primeira importação real

- Preservar RECOVERY_REQUIRED e a inspeção 193c7869-6ba0-480f-a7c8-3fcd6e178394;
  resolver exclusão pelo procedimento auditável com evidência externa conclusiva.
- Validar no alvo da VPS tipos, ordem, nulabilidade, PK, precisão, logins, grants,
  ledgers, RLS/triggers/constraints e ausência de pendências concorrentes.
- Para a primeira importação, obter captura original completa/selada e metadados
  reais da procedure correspondente; revisar layout, collation, nulabilidade,
  timezone e escopo da PK e aprovar esse contrato no PostgreSQL. ACTIVE exige
  CurrentDay e PreviousDays separadamente. ReRead histórico não aprova esses
  contratos nem gera intent/TLS retroativo.
- Autorizar perfil e catálogos; aceitar expressamente nomes de agentes NULL ou
  fornecer snapshot sanitizado legítimo. Os quatro campos sem equivalência
  permanecem NULL; não preencher por suposição.
- Confirmar atribuição de faixas, coordenação de consumidores, retenção e
  completude/exercício da recuperação: o sucesso da faixa histórica não comprova
  o comportamento depois de uma leitura incremental interrompida.
- Usar importação local existente somente para captura aceita pelo seu contrato;
  este teste não cria comando de produção para converter o JSON histórico em TDS.
  Confirmar recibos/outbox, PKs e checkpoint antes de autorizar ACTIVE.

A sincronização e a primeira importação na VPS **não foram liberadas** por esta auditoria.
