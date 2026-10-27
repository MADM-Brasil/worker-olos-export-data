# Fechamento transacional sem despacho RPC

Correção local sobre `main`, sem execução na VPS/OLOS ou alteração de dados de produção.

## Causa e limite das evidências do incidente

O antigo `close-not-dispatched` validava arquivos e gravava
`not-dispatched-closed.json`, depois arquivava `remote-pending.json`. Não havia
transação que encerrasse um run. O scheduler V2 ignora capturas com aquele marcador,
mas verifica separadamente runs `read_intent`, `persisting` e
`needs_reconciliation`. Um run órfão continua bloqueando a inicialização; lease
expirada não é autorização para takeover.

Incidente informado: captura `2837dbae-c448-461f-86f3-d8197562ac69`, run
`4aca20bf-ea7d-470e-8b6a-7201c2619076`, geração 8, `read_intent`, sem batches,
sync errors ou recovery cases; `CONNECTION_FAILED` em `CONNECTION_INIT_STARTED`.
Os arquivos reais não foram acessados nesta entrega. Esses dados e a existência
dos cinco arquivos não bastam para executar o fechamento.

**Na main auditada, `observeServiceCapture` não cria run PostgreSQL.** O run é
criado pelo coordenador de importação local. Portanto, não há associação
automática captura/run no diagnóstico, nem é seguro escolher o único run aberto
pela tabela, data ou geração. A correção aceita o vínculo durável existente em
`cases/<captureId>/import/batches/<batchId>/manifest.json`, confirmado por
`import/active.json`, plano da intenção e identidade PostgreSQL. Sem esse vínculo,
um run pendente causa recusa. Não criar, copiar ou editar um manifest para satisfazer
essa validação. Se o incidente não possuir esse artefato, é necessária investigação
da revisão efetivamente executada e de sua proveniência antes de adaptar o protocolo;
esta entrega não afirma que o incidente real já pode ser liberado.

## Protocolo

### Compatibilidade do diagnóstico e fechamentos históricos

Antes de `e53ad7b`, `InspectionDiagnostics.snapshot()` emitia a versão
`olos-log-inspection-diagnostic/v1` sem o campo `transport`. Esse commit adicionou
o campo sem alterar a versão. A validação introduzida em `f360ccc` passou a exigir
um array para todo v1, rejeitando inclusive fechamentos antigos íntegros antes
de chegar à transação. Essa incompatibilidade é reproduzida nos testes de
inicialização da fila; não depende da presença da migration 009.

O produtor agora emite **`olos-log-inspection-diagnostic/v2`**, sempre com
`transport`. O contrato dos leitores é explícito:

| Formato | Regra |
| --- | --- |
| v1 sem propriedade `transport` | Apenas revalidação de fechamento local histórico já concluído, nas condições abaixo. |
| v1 com `transport` | Mantém todas as validações existentes do array e de suas entradas. |
| v2 | `transport` obrigatório; ausência, `null` e valores inválidos são recusados. |
| Versão desconhecida | Recusa. |

O reconhecimento histórico usa o conjunto exato de campos, fases, estágios e
códigos de erro do produtor anterior a `e53ad7b`, congelado em
`src/olos/tds/historical-not-dispatched-diagnostic.ts`. A sequência deve começar
em autorização e manter a ordem dos estágios anteriores ao despacho, sem
duplicações. Campos novos, estágios/códigos introduzidos posteriormente e versões
novas incompletas não são interpretados como legado. Datas, prefixos de IDs e
mtime não são critérios de identificação.

A exceção só opera em `verifyOnly=true`, com marker já arquivado, proof original
vinculado ao digest do diagnóstico, `not-dispatched-closed.json` correspondente
e **nenhum diretório de importação**. Recusa marker pendente para a mesma captura,
resposta pronta, bytes recebidos, captura enfileirada/entregue ou qualquer
incompatibilidade das demais provas. Fase/erro são reconstruídos, as identidades
são conferidas e os arquivos continuam privados, sem symlinks e sob o mesmo UID.
O arquivo de fechamento histórico também integra as evidências revalidadas
dentro da transação, inclusive antes do commit.

Não se adiciona `transport: []`, não se normaliza o diagnóstico e não se regrava
nenhum arquivo/proof/digest histórico. A comparação usa o JSON original. O ramo
sem manifest continua recusando contradição com ledger existente e não muda
runs, source locks, fencing ou checkpoints. Os bloqueios globais de inicialização
para outros runs/capturas continuam valendo. A exceção não cria fechamentos novos
e não permite executar fechamento transacional a partir de um diagnóstico legado.

**Limite de proveniência:** o v1 original não possui assinatura, versão do build
ou carimbo de tempo confiável. Não existe distinção criptográfica entre um
histórico legítimo e uma substituição coordenada de diagnóstico, proof, fechamento
e demais artefatos por alguém com controle do volume. O reconhecimento exige o
formato histórico completo e sua cadeia de evidências preservada, sob a mesma
premissa de custódia do protocolo existente. Não promete autenticação que os
artefatos não oferecem. Alterações isoladas, digests divergentes, formatos
incompatíveis e tentativas de usar a exceção em fechamento novo são recusadas.

Mudanças futuras em campos obrigatórios ou semântica exigem nova versão do
diagnóstico e política explícita de leitura; o vocabulário histórico congelado
não deve acompanhar adições ao produtor. Os testes cobrem a fila real em bases
descartáveis anteriores/posteriores à 009, múltiplos históricos, v1 com transporte,
v2, reinicialização, imutabilidade dos arquivos e preservação de runs alheios.

Validação local em 27/10/2026: `npm run typecheck` e `npm run build` aprovados;
`npm test -- --maxWorkers=2 --testTimeout=60000` com **1.022 aprovados e 5 opcionais
ignorados**, 63 arquivos aprovados e 3 ignorados (371,82 s na execução final).
Os 90 casos novos também passaram isoladamente, incluindo remoção concorrente do
fechamento e criação concorrente de importação durante a transação. A preparação
SQL ocorreu somente nas fixtures PostgreSQL descartáveis da suíte. Não houve
aplicação operacional de migrations, acesso à VPS/OLOS ou RPC real. Essa evidência
local não declara o worker pronto para produção nem resolve o run independente
com 200.000 registros selados.

### Fechamento transacional

1. A CLI mantém a autorização privada, o probe de roles/schema/destino e o advisory
   lock de instância `(740219,1)`. Não inicia scheduler, recovery ou conexão OLOS.
2. Valida intenção e marker atual/arquivado, UUID, diretório e configuration digest;
   proof estrito com digest do diagnóstico; versão, estados `NOT_OBSERVED`, erro,
   fase, estágios e observações de transporte consistentes. Qualquer estágio RPC,
   captura/validação de resposta, estado desconhecido ou prova incompleta recusa.
3. Exige `remote-state.json` do mesmo attempt com `retryRemote=false` e
   `REMOTE_OUTCOME_UNKNOWN`. Esse arquivo é o registro conservador anterior à
   execução; **não é**, isoladamente, prova de não execução. Exige descritor
   incompleto com zero bytes, SHA-256 do vazio e flags de efeitos falsas, além de
   `response.partial` realmente vazio e ausência de `response.ready`.
4. Havendo spool de importação, exige um único manifest não selado e sem linhas,
   bytes ou hash de lote, plano idêntico à intenção, claim com a mesma identidade
   e `records.partial` vazio. Arquivos privados, ownership e ausência de symlinks
   são revalidados. Captura enfileirada/entregue recusa.
5. Na transação existente: source row `FOR UPDATE NOWAIT`, run `FOR UPDATE NOWAIT`,
   checkpoint `FOR UPDATE NOWAIT`. Confere run `read_intent`, contadores zero,
   ausência de término, stream/contrato/plano/token exatos, owner exato e lease
   expirada segundo o relógio PostgreSQL. Outros runs ativos/casos abertos recusam.
6. Recusa batches, erros, casos, eventos de auditoria, recibos, chaves, envelopes,
   projeções/outbox, reconciliações ou descartes do run/lote/captura. As FKs dos
   efeitos e o lock exclusivo do run impedem inserção concorrente de sua raiz de
   proveniência; writers normais também obedecem ao source lock.
7. Insere auditoria append-only em `not_dispatched_closures`, muda apenas o run
   exato para `failed`, registra término, limpa owner/lease e incrementa geração.
   Exemplo: 8 → 9; um futuro `startRun` obterá 10. A geração do run antigo permanece 8.
   Não cria recovery case, não modifica checkpoints, deduplicação, recibos ou outbox.
8. Revalida arquivos/autorização/sinal antes das escritas e imediatamente antes do
   commit. Só após confirmação PostgreSQL grava/reconhece o fechamento local e
   arquiva o marker, com fsync dos diretórios. A evidência original permanece.

Capturas de bootstrap sem spool e sem run/owner pendente conservam o fechamento
local. Não se presume que um run sem vínculo pertença a elas. A inicialização V2
confere novamente provas e ledger das capturas encerradas, inclusive a existência
das intenções locais correspondentes aos registros PostgreSQL. Inicialização não
converte silenciosamente um fechamento antigo em autorização administrativa.

## Falhas e repetição

Falha antes do commit reverte auditoria, estado e source lock juntos. Falha de ACK
do commit retorna `COMMIT_UNKNOWN`; os arquivos ainda não são encerrados nessa
tentativa. Não executar RPC, `recover`, `recoverExpired`, reset nem SQL corretivo.
Repetir **o mesmo** `close-not-dispatched`, com as mesmas evidências, verifica o
ledger e o run terminal, ou executa a transação se ela não confirmou. Nenhum erro
de commit é gravado artificialmente como erro de aquisição do run.

Se o banco confirmou e houve falha no filesystem, a repetição termina o arquivo
local. Se a CLI antiga já havia arquivado tudo, a mesma chamada reconcilia o banco
sem reabrir captura. O resultado distingue `alreadyClosed` (filesystem) de
`runClosure.alreadyReconciled` (PostgreSQL). A repetição de A após um novo owner B
não escreve no source lock nem no run B; se B já tem um marker remoto pendente,
a CLI recusa fechar A nessa chamada.

Hashes e ACLs detectam inconsistências; não oferecem autenticidade contra um
administrador que substitua coordenadamente todas as evidências. O protocolo
continua dependendo da custódia do volume privado e da revisão que produziu a prova.

## Aplicação posterior no Dokploy

Este procedimento é para uma janela operacional futura; nenhum passo foi executado
remotamente. O Compose atual inicia `start --interval-ms 120000` automaticamente.
`SYNC_ENABLED=false` não impede esse scheduler explícito. Não fazer redeploy com
esse comando ativo antes da reconciliação.

1. Revisar o diff e publicar a revisão por fluxo autorizado separado. Parar o worker,
   desativar restart/Auto Deploy durante a janela, manter PostgreSQL disponível e
   preservar os mesmos volumes. Fazer backup consistente do banco e do estado
   privado completo. Não executar `down -v`.
2. Conferir captura, manifest/claim e run do incidente, revisão produtora, geração,
   estado, lease e ausência de efeitos. Não reconstruir provas ausentes. Se não
   houver vínculo durável suportado, manter bloqueado e investigar.
3. Construir a imagem revisada. Para disponibilizar o terminal administrativo do
   worker durante a janela, usar temporariamente `node dist/main.js` como comando,
   mantendo `SYNC_ENABLED=false` e as variáveis genéricas OLOS/PG vazias conforme
   Compose. Esse comando não inicia a CLI de aquisição. Preservar usuário `node`,
   mounts, roles de runtime, redes e uma única réplica.
4. Após conferir backups, banco/destino e migrations anteriores, aplicar manualmente
   como administrador **somente** `migrations/009_not_dispatched_closures.sql` e
   `permissions/not-dispatched-closures.sql` nesta atualização. Os arquivos estão
   em `/opt/olos` no target database de uma imagem reconstruída. Se o container
   PostgreSQL ainda usa imagem anterior, disponibilizar os dois arquivos revisados
   por canal administrativo; não presumir que redeploy do worker atualiza o banco.
   Não reaplicar scripts já aplicados. Não há migration automática no boot nem
   mudança em `schema_versions` `[1,2]`.

   ```bash
   psql -X -v ON_ERROR_STOP=1 -U olos_admin -d olos_worker -f /opt/olos/migrations/009_not_dispatched_closures.sql
   psql -X -v ON_ERROR_STOP=1 -U olos_admin -d olos_worker -f /opt/olos/permissions/not-dispatched-closures.sql
   ```

5. No terminal do worker revisado, como usuário `node`, executar apenas:

   ```bash
   node dist/cli/adapters/attempts-corporate.js close-not-dispatched \
     --config /home/node/.config/olos-worker/attempts-service.json \
     --work-dir /home/node/.local/state/olos-worker/corporate-attempts/service-run \
     --attempt-id 2837dbae-c448-461f-86f3-d8197562ac69 \
     --confirm LOCAL_RECOVERY_ONLY
   ```

   Para o incidente, sucesso no PostgreSQL precisa incluir o run exato em
   `runClosure`, `runState=failed` e `releasedGeneration=9`. `alreadyClosed=true`
   sozinho não comprova essa transição. Qualquer recusa preserva o bloqueio; não
   contornar por SQL, novos manifests ou remoção de evidências.
6. Conferir em leitura o registro de auditoria, run, source lock, ausência de novo
   recovery case e conservação dos checkpoints/outbox. Repetir a mesma CLI para
   confirmar idempotência. Não restaurar banco e volume em momentos incompatíveis.
7. Somente após revisão operacional, restaurar o comando `start` do Compose e o
   restart desejado. Observar inicialização, fila anterior, entregas/recibos e o
   próximo ciclo de 120 segundos. Essa etapa habilita aquisição real e exige a
   validação operacional que está fora dos testes locais desta entrega.

`HTTP_PORT: "3100"` foi preservado. O HEALTHCHECK herdado calcula a URL usando
`process.env.HTTP_PORT`; não existe porta 3000 fixa nem override no Compose.
O comando exato do Dockerfile foi exercitado contra fixture HTTP local na porta
3100. Isso verifica a compatibilidade do comando, não a saúde do deploy real.

Rollback de código não deve apagar 009 nem seus registros. Uma revisão antiga
não valida esse ledger na inicialização; manter aquisição parada até revisar a
compatibilidade de banco, arquivos e fila. Não restaurar um run para `read_intent`
nem reduzir geração/checkpoint como forma de rollback.

## Validação local

Testes PostgreSQL isolados cobrem prova válida, ausente/adulterada, estágios RPC,
lease ativa, divergências de identidade/geração, efeitos, concorrência em locks,
rollback, commit desconhecido com/sem confirmação, fechamento antigo, repetição
sem interferir no sucessor, perda de lock, revalidação e inicialização da fila.
Um scheduler real com relógio de teste verifica o intervalo de 120 segundos e
callback local. A entrega anterior usa recibos/outbox reais no PostgreSQL descartável.
As CLIs administrativas têm spies que falham se conexão OLOS, RPC ou start do
serviço forem chamados. O probe de roles da CLI é substituído nesses testes; isso
não prova permissões/configuração da instalação real. Nenhum resultado local
declara o worker pronto para produção.

Execução local de 2026-10-27:

- `npm run typecheck`: passou.
- `npm run build`: passou.
- `npm test -- --maxWorkers=2 --testTimeout=60000`: 911 passaram, 5 ignorados,
  duração de 263,95 segundos.
- Verificação adicional do `AttemptsService.start()` real com fila reinicializada,
  intervalo 120000 e callback local: passou. O relógio foi acelerado no teste;
  o probe da instalação produtiva não foi exercitado.
- `git diff --check`: passou.
- Healthcheck herdado com fixture HTTP local em `HTTP_PORT=3100`: passou.

O cenário com run vinculado é uma fixture sintética explícita. Não reproduz uma
criação natural de run pela captura V2 desta main, pois essa criação não existe
no caminho auditado. A associação do incidente informado permanece a confirmar
com seus artefatos originais, sem acesso remoto nesta entrega.
