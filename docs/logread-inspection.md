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

`log-inspection-active.json` e o pending de aquisição se excluem nos guards de
permissão. A inspeção mantém seu marcador inclusive após sucesso para impedir uma
nova chamada sem revisão; não remove nem confirma o pending de aquisição. Se já
existir pending de aquisição, a inspeção também é bloqueada nesta versão. Isso
não oferece inspeção de recuperação de aquisições pendentes: tal reconciliação
continua externa. A exclusão pressupõe processos usando esta versão, o mesmo
estado privado e o mesmo usuário. Outros consumidores/versões não são cercados.

Não apagar ou trocar IDs para contornar um caso. A liberação pós-revisão é manual,
fora deste comando: exige evidência remota independente e decisão registrada do
responsável. Preservar os arquivos existentes, inclusive o marcador de exclusão
como arquivo arquivado exclusivo, antes de qualquer liberação autorizada. Nenhum
fluxo automático de liberação ou nova aquisição foi implementado.

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
