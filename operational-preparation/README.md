# Primeira aquisição observe — bloqueada em 20/10/2026

`attempts-corporate.observe.pending.json` reutiliza a preparação privada existente por `preparationFile` e seleciona `contractMode: "observe"`. Não exige nem inclui `current` ou `previous`, não copia a senha e não cria aprovações. Está deliberadamente incompleto: não é configuração definitiva válida enquanto faltarem `pinApprovalFile` e `authorization` reais. O arquivo foi criado com permissão 0600 no projeto; a configuração privada padrão não foi criada ou substituída.

Verificação local executada:

```bash
SYNC_ENABLED=false npm run worker:attempts -- status --config /home/madm/worker-olos-export-data/operational-preparation/attempts-corporate.observe.pending.json
```

O status retornou `configuration.exists=true`, `configuration.valid=false`, `CORPORATE_CONFIGURATION_INVALID`, `TLS_APPROVAL_NOT_CHECKED_WITHOUT_VALID_CONFIGURATION`, `acquisitionAuthorized=false`, `networkAccessed=false`, `remoteIntentPending=false`, `firstAcquisitionConfirmed=false`, `attempts=[]`. O exit code 0 indica execução do diagnóstico, não aprovação da configuração. A inspeção independente confirmou que os diretórios privados `~/.config/olos-worker/certs/` e `tls/` estão vazios. A pasta de estado corporativo também está vazia. Nenhum handshake foi realizado.

Dependências para concluir a configuração:

1. **TLS de aquisição:** obter certificado público PEM e SHA-256 confirmados por canal administrativo independente. Um responsável real deve emitir aprovação vigente para `OLOS_ATTEMPTS_TDS_ACQUISITION`, vinculada a `10.196.234.11:1433 / ExportDataV3`, com referência, aprovador e datas. O formato aceito está em `src/security/olos-certificate-pin.ts`; o guia está em `docs/olos-certificate-pinning.md`. Usar o propósito de aquisição, não o propósito de diagnóstico mostrado no exemplo do guia. Arquivo privado 0600, sem symlinks; aprovação de no máximo sete dias, certificado válido. Informar seu caminho absoluto em `pinApprovalFile`. Um certificado apenas observado não satisfaz a aprovação; o TLS autenticado ainda precisa ser comprovado.
2. **Autorização operacional:** responsável, referência verificável, `issuedAt`/`expiresAt` vigentes (janela máxima de 12 horas), fuso confirmado da origem em `sourceTimeZone` e evidência real de coordenação dos consumidores em `sharedConsumptionEvidence`. A solicitação desta tarefa não comprova coordenação externa nem o fuso da origem.
3. **Recuperação de leitura interrompida:** evidência aprovada em `recoveryEvidence`, cobrindo atribuição inequívoca da tentativa à faixa de LogReadData mesmo se nenhuma PK chegar, coordenação de outros consumidores, término remoto, DateExport/fuso corretos e retenção, inclusividade, disponibilidade e completude de ReReadData. Registrar responsáveis e como executar a conciliação externa autorizada. A CLI atual não emite permit corporativo para Log/ReRead; não presumir que o comando local `recover` resolva perda antes do selo. O histórico de 570 registros de 01/10 não comprova recuperação de uma leitura futura. Ver `docs/solicitacao-tecnica-olos-primeira-aquisicao.md`.

Não foram executados `once`, `recover` ou `start`, nem procedures, consultas PostgreSQL, alterações de parser/scheduler, commit, push ou deploy. Os 570 históricos não foram acessados ou alterados nesta tarefa.

**Captura:** não existe resposta capturada ou metadado novo. Após aquisição autorizada futura, o caminho esperado dos metadados é `~/.local/state/olos-worker/corporate-attempts/first-run/spool/batches/<UUID>/response.json`; a resposta deve permanecer em quarentena. Confirmar o arquivo real e revisar/aprovar o contrato observado antes de qualquer `recover` ou `start`. Não interpretar este caminho esperado como evidência de captura.
