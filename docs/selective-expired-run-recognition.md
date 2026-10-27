# Reconhecimento seletivo de lease expirada

Operação administrativa `recognize-expired-run` para o serviço Attempts V2.
Não importa dados, não chama a OLOS, não inicia scheduler e não autoriza nova
aquisição. A revisão implantada `91d5748` não contém este comando; sua publicação
e aplicação operacional dependem de um fluxo posterior autorizado.

## Contrato e transação

Entradas obrigatórias: UUID completo do run, `--source AttemptsRawData`, geração
esperada como inteiro decimal positivo e confirmação
`AUTHORIZE_EXPIRED_RUN_RECOGNITION`. A autorização privada do serviço continua
obrigatória, inclusive revogação e vínculo da configuração/destino PostgreSQL.
Outras fontes, parâmetros de scheduler e identificadores parciais são recusados.

A CLI reutiliza o lock exclusivo de instância `(740219,1)` e o probe padrão.
A transação usa o mesmo mecanismo de `recoverExpired`, com seleção explícita:

1. Source lock exato `FOR UPDATE NOWAIT`, run exato `FOR UPDATE NOWAIT` e seu
   recovery case, se existente, `FOR UPDATE NOWAIT`. Não varre runs/capturas para
   escolher um candidato, nem abre diretórios ou manifests de captura.
2. Confere source, UUID, token do run e geração esperada; exige `read_intent`,
   ausência de término/caso/auditoria prévia de reconciliação, owner igual ao run
   e lease expirada pelo relógio PostgreSQL. Não presume ausência de efeitos a
   partir dos contadores e não usa expiração como prova de não execução remota.
3. Muda somente o run selecionado para `needs_reconciliation`, cria o caso aberto
   `expired_read_intent` e o evento durável `reconciliation_required`.
4. Libera owner/lease e incrementa a geração, com comparação de owner/geração e
   expiração no UPDATE. O token original do run permanece intacto.
5. Revalida autorização/cancelamento antes das escritas e antes do commit. Qualquer
   falha antes do commit reverte a transação inteira.

O núcleo da transição é compartilhado com `recoverExpired`. O caminho global
conserva sua varredura e a marcação de batches pendentes. **O caminho seletivo não
altera nenhum batch**, confirmado ou pendente, nem contadores, checkpoints,
envelopes, outbox, recibos ou arquivos. A entrega assíncrona prévia é preservada.
Um caso com batches pendentes pode continuar exigindo análise na reconciliação;
reconhecer a expiração não afirma que o replay já foi autorizado ou é admissível.

Não há migration/grant novo: são usados `runs`, `source_locks`, `recovery_cases`
e `audit_events` e os privilégios existentes do runtime. A migration 009 de
not-dispatched não é requisito desta operação. A prontidão das demais tabelas
continua sujeita ao preflight normal da CLI.

## Idempotência e falhas

A repetição com **o mesmo Run ID, source e geração original** retorna o mesmo
`recoveryCaseId` e `alreadyRecognized=true`, sem novas escritas, somente se:

- Run continua `needs_reconciliation`, sem término e com o token original.
- Caso continua aberto, sem resolução, com motivo `expired_read_intent`.
- Existe exatamente um evento de reconciliação compatível, sem batch e com
  contagens zero, conforme o protocolo de `recoverExpired`.
- Source lock está livre, sem lease, na geração original + 1.

Um resultado equivalente criado pelo `recoverExpired` existente também é
reconhecido. Mudança de geração, novo owner, run finalizado, caso resolvido,
motivo divergente ou auditoria ausente/duplicada provoca recusa. Uma repetição
não rouba a posse de um sucessor nem reabre um caso já reconciliado.

Concorrência transacional retorna `BUSY`; outro processo com o lock de instância
retorna `INSTANCE_BUSY`. Não há retry automático. Em `COMMIT_UNKNOWN`, conservar
as evidências e repetir somente o mesmo comando quando a execução anterior
tiver terminado. Se o commit ocorreu, o resultado durável é reconhecido; caso
contrário, a mesma transição pode ser feita uma vez. Não mudar a geração esperada
para tentar contornar uma recusa.

## Execução controlada posterior — incidente informado

Nenhum passo abaixo foi executado na VPS. Manter o worker e seu reinício automático
parados durante a recuperação. `SYNC_ENABLED=false` não impede um comando `start`
explícito de iniciar o scheduler; não usar `start` nesta etapa.

1. Disponibilizar a revisão contendo esta correção mediante autorização operacional
   separada. Preservar volumes e fazer backup consistente do estado/banco. Conferir
   os caminhos reais de configuração e work-dir e usar o mesmo usuário do worker.
2. Confirmar que run `4aca20bf-ea7d-470e-8b6a-7201c2619076` é da fonte
   `AttemptsRawData`, permanece `read_intent`, token 8, source lock geração 8,
   owner desse run e lease expirada. A operação revalida isso na transação.
3. Executar **somente** o reconhecimento, substituindo os caminhos pelos da
   instalação (os exemplos devem permanecer sob o diretório privado do usuário):

   ```bash
   SYNC_ENABLED=false node dist/cli/adapters/attempts-corporate.js recognize-expired-run \
     --config /home/node/.config/olos-worker/attempts-service.json \
     --work-dir /home/node/.local/state/olos-worker/corporate-attempts/service-run \
     --run-id 4aca20bf-ea7d-470e-8b6a-7201c2619076 \
     --source AttemptsRawData \
     --expected-generation 8 \
     --confirm AUTHORIZE_EXPIRED_RUN_RECOGNITION
   ```

4. Registrar a saída: `event=expired_run_recognized`,
   `runState=needs_reconciliation`, `recoveryReason=expired_read_intent`,
   `recoveryState=open`, `releasedGeneration="9"` e o `recoveryCaseId` retornado.
   `imported=0`, `acquisitionPerformed=false`, `schedulerStarted=false` e
   `retryRemote=false`. Confirmar que owner/lease ficaram nulos e que o spool
   selado da captura `8538fc2b-6963-4d8e-aa1f-a64ff5bcbfec` está intacto.
5. Em etapa separada, preparar a autorização já prevista em
   [reconcile-ingestion](service-activation.md),
   usando o case ID retornado, o manifest original e `expectedGeneration="9"`
   **somente se essa ainda for a geração real**. O manifest/token original 8
   não deve ser reescrito. Esse mecanismo existente validará os 200.000 registros,
   recibos, efeitos anteriores, deduplicação e checkpoint antes do replay local.
6. Somente após a reconciliação e as demais pré-condições reais estarem satisfeitas,
   validar a fila/entrega pelo procedimento existente e autorizar o retorno do
   mesmo `start --interval-ms 120000`. Não criar outra instância ou scheduler.

O reconhecimento isolado **não libera o scheduler**: o caso aberto mantém a
barreira até a reconciliação. Nenhuma conclusão de prontidão de produção pode
ser baseada apenas nos testes locais.

## Validação local — 27/10/2026

- `npm run typecheck`: aprovado.
- `npm run build`: aprovado, incluindo a CLI compilada.
- `npm test -- --maxWorkers=2 --testTimeout=60000`: **1.065 aprovados, 5 opcionais
  ignorados**, 64 arquivos aprovados e 3 ignorados; 386,23 s.
- `git diff --check`: aprovado.
- 43 testes novos: CLI/autorização/lock de instância, seleção exata, fonte alheia
  preservada, lease ativa, estados inesperados, fencing, idempotência após restart,
  concorrência seletiva/global, rollback em cada escrita, `COMMIT_UNKNOWN` antes
  e depois do commit, revogação/cancelamento, retenção de batches/checkpoint/outbox
  e proibição de abertura/replay de spool, conexão OLOS e início de scheduler.

Somente fixtures PostgreSQL locais descartáveis foram preparadas pela suíte.
Não houve acesso à VPS/OLOS, migration operacional, commit, push ou deploy.
