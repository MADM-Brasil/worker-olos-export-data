# Inspeção manual de LogReadData

A implementação usa exclusivamente o endpoint corporativo fixo e o transporte
PinnedMessageIO → ExactConnection → IsolatedTdsSource. Não usa PostgreSQL,
catálogos, aquisição, recuperação remota ou scheduler. `executionAllowed: false`
da recuperação permanece vigente. Não executar a primeira consulta sem aprovação
operacional explícita; testes locais não comprovam a semântica da Olos.

## Autorização privada

Preparar, por processo operacional aprovado, um JSON privado (`0600`, proprietário
igual ao usuário do worker, caminho absoluto sem symlinks). Usar `--config` para
selecioná-lo. O arquivo padrão continua sendo `~/.config/olos-worker/attempts-corporate.json`;
uma configuração de aquisição não é autorização de inspeção e será rejeitada.
Não substituir a configuração de aquisição para fazer esta consulta.

Estrutura (os valores entre `<...>` precisam de aprovação real; não são defaults):

```json
{
  "version": "olos-log-inspection-config/v1",
  "endpoint": {
    "server": "10.196.234.11",
    "port": 1433,
    "database": "ExportDataV3",
    "user": "OEC",
    "password": "<credencial privada existente>"
  },
  "pinApprovalFile": "/caminho/privado/pin-aprovado.json",
  "authorization": {
    "scope": "OLOS_LOGREADDATA_INSPECTION_ONLY",
    "operationId": "<UUID exclusivo aprovado para esta operação>",
    "operator": "<responsável>",
    "reference": "<ticket ou aprovação operacional específica>",
    "procedure": "LogReadData",
    "tableName": "AttemptsRawData",
    "dateExport": "2026-10-22",
    "issuedAt": "<instante UTC ISO8601>",
    "expiresAt": "<instante UTC ISO8601, no máximo 12h após issuedAt>",
    "pinApprovalId": "<approvalId do pin aprovado>",
    "fingerprintSha256": "<fingerprintSha256 exato do pin aprovado>"
  }
}
```

O pin usa a política TLS corporativa existente (`OLOS_ATTEMPTS_TDS_ACQUISITION`),
com aprovação independente válida e certificado aprovado vigente. Esse propósito
TLS não concede autorização para consumir chamadas. Não há relaxamento TLS,
aceitação automática de certificado observado, SQL livre ou parâmetro NULL.
O digest da configuração privada (incluindo a autorização) é ligado à intenção.
A intenção, o pending e a captura são preparados antes da permissão; intenção e
pending são relidos nos guards de conexão, login e RPC. O processo precisa manter
os arquivos privados íntegros durante a operação.

## Comando na VPS após aprovação

Executar como o usuário de serviço do worker, na instalação do repositório:

```bash
npm run worker:attempts -- inspect-log \
  --config "$HOME/.config/olos-worker/attempts-log-inspection.json" \
  --confirm AUTHORIZE_LOG_INSPECTION \
  --date-export 2026-10-22
```

A data deve coincidir exatamente com a autorização. Não repetir o comando após
falha. Mesmo uma falha antes de receber bytes deixa um caso para revisão; ausência
de resposta não prova ausência de execução. O resumo `RESPONSE_SEALED` significa
resposta transportada e validada, não aprovação do layout nem atribuição de faixa.
`REVIEW_REQUIRED` inclui erro SQL, resposta interpretável com código de retorno
não zero/ErrorMessage preenchida e layout não interpretável. Uma interrupção ou
resposta parcial mantém `REMOTE_OUTCOME_UNKNOWN`. Todo resultado exige revisão.
O comando usa exit code 1 para REVIEW_REQUIRED e REMOTE_OUTCOME_UNKNOWN.
Mesmo exit code 0 exige revisão humana e não confirma atribuição de faixa remota.

## Evidências e exclusão

Estado privado em:

```text
~/.local/state/olos-worker/corporate-attempts/
  log-inspection-active.json
  log-inspections/<operationId>/
    intent.json
    pending.json
    execution-permit.json
    diagnostic-start.json
    diagnostic.json
    remote-outcome-unknown.json
    spool/active.json
    spool/active-capture.json
    spool/batches/<operationId>/response.partial OU response.ready
    spool/batches/<operationId>/response.json
    result.json
```

Diretórios são `0700`; arquivos `0600`, exclusivos, com sincronização de arquivos
e diretórios. O spool usa os limites de quota/espaço livre do LocalSpool. A área
`first-run/spool` não é usada. O ID de operação vem da autorização privada, e sua
pasta exclusiva bloqueia reuso. O marcador conservador é sincronizado antes do
socket; se não houver result.json após encerramento, preservar tudo e tratar como
incerto. Falhas de disco podem impedir o resumo/selo; nunca reparar fabricando-os.

`log-inspection-active.json` preserva o histórico e impede reuso da inspeção;
não bloqueia permits nem aquisições futuras do V2. O serviço informa a pendência
em `historicalInspection`, com `blocksAcquisition=false`. O caso indeterminado
permanece `REMOTE_OUTCOME_UNKNOWN`; não é reconciliado por essa separação.

Um pending de aquisição continua bloqueando novas leituras e a própria inspeção.
A recuperação de aquisições consumptivas incertas continua externa. Não apagar,
arquivar para liberar, trocar IDs ou fabricar evidências/clearance. Intenções,
marcadores, bytes e decisões existentes devem permanecer intactos.

## Revisão sem exposição

1. Consultar `result.json` pelo canal privado: contém apenas resumo, sem linhas ou
   mensagens SQL. Comparar operação/data com a autorização e a intenção.
2. Conferir SHA-256 e tamanho de `response.ready` contra `response.json` e o resumo,
   em ambiente restrito (`sha256sum <arquivo>` e `stat <arquivo>`). Para parcial,
   conferir `response.partial` contra o descritor incompleto, quando disponível.
   Não enviar os bytes ou descritores privados a logs públicos.
3. Revisar `response.json` privadamente: datasets/colunas efetivamente observados,
   validação TLS, retorno e presença de ErrorMessage. Metadados de coluna também
   podem ser sensíveis. A captura contém payload TDS da resposta ao RPC, sem
   LOGIN7/credenciais; não é um pcap nem inclui o tráfego de autenticação.
4. Somente pessoal autorizado pode examinar o payload com ferramentas locais,
   mantendo linhas e mensagens SQL no ambiente restrito. Layout não suportado,
   vários datasets ou mensagem incompleta exigem análise manual. Quando o parser
   falha, contagens e retorno podem ser desconhecidos; os bytes completos da
   mensagem recebida continuam em quarentena. Não existe layout de resultado
   aprovado embutido nem importação automática desta captura.
5. Registrar `REMOTE_RANGE_ATTRIBUTION_NOT_ESTABLISHED`: a captura não prova que
   uma faixa pertence à nossa execução. Não inferir PKIDStart/PKIDEnd nem liberar
   ReadDataCurrentDay/ReReadData sem evidência e aprovação independentes.

A assinatura executada é a reportada pelo servidor: `tablename nvarchar(128)`,
`DateExport date`, `Message varchar(4000) OUTPUT`, `ErrorMessage varchar(4000)
OUTPUT`, nessa ordem. Ela diverge do manual; o manual não autoriza alterar a
assinatura nem habilitar DateExport=NULL. A semântica operacional, layout real,
login/TLS na VPS, suficiência da evidência para atribuição e comportamento de
falhas reais continuam pendentes de validação corporativa aprovada.


## Diagnóstico de futuras operações

O resumo do terminal e `result.json` incluem `diagnostic`, com o mesmo conteúdo
sanitizado de `diagnostic.json`. Esse arquivo é exclusivo, privado (`0600`) e
sincronizado, junto ao diretório, após o encerramento da captura. Não contém
mensagem SQL, stack, credenciais, parâmetros, linhas, payload ou dados do operador.
Apenas UUID, vocabulário fixo de fases/estágios e códigos técnicos permitidos.
Erros desconhecidos viram `EXECUTION_FAILED` (ou o fallback técnico da fronteira),
sem copiar o texto. O primeiro erro observado prevalece sobre cancelamento,
fechamento e wrappers subsequentes do driver.

Consultar **somente uma nova operação autorizada**, substituindo o placeholder:

```bash
cat "$HOME/.local/state/olos-worker/corporate-attempts/log-inspections/<novo-uuid>/diagnostic.json"
```

| Fase | Evidência local observável |
| --- | --- |
| `AUTHORIZATION` | Validação/solicitação da capacidade iniciou após preparo exclusivo. |
| `PERMIT_ISSUED` | A função de autorização retornou com a capacidade emitida; não prova conexão ou RPC. |
| `CONNECTION_INIT` | Construção/início do transporte. Apenas `SOCKET_CONNECTED` indica entrega de um socket TCP conectado ao driver. |
| `TLS_HANDSHAKE` | `TLS_HANDSHAKE_STARTED` indica entrada na negociação; `TLS_VALIDATED` exige validação do pin/certificado e TLS 1.2 pelo IO corporativo. |
| `SQL_LOGIN` | `SQL_LOGIN_PREPARATION` é entrada no guard; `SQL_LOGIN_STARTED` é entrega ao método LOGIN7; `SQL_LOGIN_ACCEPTED` é sucesso do callback connect do Tedious, incluindo sua inicialização de sessão. |
| `RPC_DISPATCH` | `RPC_DISPATCH_STARTED` antecede os guards. `RPC_HANDOFF_STARTED` ocorre após os guards, antes de chamar o driver; `RPC_HANDOFF_RETURNED` indica retorno dessa chamada. |
| `RESPONSE_CAPTURE` | `RESPONSE_CAPTURE_STARTED` indica recebimento de bytes no sink, antes da escrita; `CAPTURE_FINALIZATION_STARTED` indica início do fechamento/selo. |
| `RESPONSE_VALIDATION` | Início do replay local da mensagem completa; `RESPONSE_VALIDATED` indica resposta integral interpretada com os controles de término/OUTPUT/retorno. |

`phase` e `lastObservedStage` fixam a fronteira do primeiro erro; `observedStages`
lista os marcos observados. `rpcDispatchState` permanece `NOT_OBSERVED` até a
entrega efetiva ao método do driver, independentemente de socket, TLS ou login.
Mesmo `DRIVER_HANDOFF_RETURNED` **não prova envio integral, recebimento no servidor
ou execução concluída**: o driver pode enfileirar bytes. Não existe estado `SENT`.

`remoteExecutionOutcome` é separado: `NOT_OBSERVED` significa ausência dessa
evidência; `UNKNOWN` significa possível execução sem resposta validada;
`RESPONSE_VALIDATED` comprova apenas a resposta interpretada. Erro SQL/OUTPUT
continua exigindo revisão e nenhuma dessas observações atribui faixa de leitura.
O estado conservador `REMOTE_OUTCOME_UNKNOWN` e todos os marcadores permanecem;
`NOT_OBSERVED` nunca autoriza retry, reaproveitamento de UUID ou liberação.

`diagnostic-start.json` registra somente a entrada inicial em `AUTHORIZATION`,
antes da solicitação de capacidade. Se ocorrer SIGKILL, perda de energia ou falha
de disco que impeça `diagnostic.json`, esse arquivo inicial não descreve a última
fase alcançada: o restante continua indeterminado. Não há backfill automático.
Rejeições anteriores ao preparo exclusivo (configuração/pin inválidos, diretório
ou UUID já existente) mantêm a rejeição sanitizada da CLI e não escrevem em casos
existentes. Se o armazenamento falhar, o diagnóstico final também pode faltar.

A operação `193c7869-6ba0-480f-a7c8-3fcd6e178394` não recebe diagnóstico retroativo.
Zero bytes e `transportSecurity: []` não permitem atribuir sua causa a autorização,
TCP, TLS, login ou RPC. Seus arquivos e seu resultado devem permanecer integrais;
a investigação e reconciliação desse caso continuam externas a este comando.
