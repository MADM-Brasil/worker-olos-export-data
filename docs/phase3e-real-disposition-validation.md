# Fase 3E — primeira validação real de Disposition

## Evidência registrada

Este documento registra o resultado comprovado **fornecido pela empresa** de uma execução manual na VPS. A execução não foi repetida durante esta tarefa. Não foram fornecidos o horário da execução, o script utilizado, as versões dos drivers, os parâmetros TLS ou um artefato bruto; esses detalhes não são inferidos.

| Item | Resultado informado |
|---|---|
| Servidor | SQL Server 2022 Standard |
| Banco | ExportDataV3 |
| Procedure | `dbo.usp_ExportData_ReadData` |
| Entrada | `TableName='Disposition'` |
| Registros recebidos | 126 |
| Datasets | 1 |
| ReturnValue | 0 |
| Message | null |
| ErrorMessage | null |
| Código de saída do processo | 0 |

Metadados do dataset recebido:

| Campo | Tipo | Nulabilidade informada | Comprimento |
|---|---|---|---|
| DispositionId | INT | NOT NULL | Não informado nesta evidência |
| Description | NVARCHAR(200) | NOT NULL | 400 bytes no metadado mssql |

Os 400 bytes correspondem às 200 unidades UTF-16 da declaração NVARCHAR(200), não a NVARCHAR(400). O comprimento declarado não informa o tamanho efetivo de cada descrição. A nulabilidade acima descreve o metadado de retorno observado; não constitui inspeção integral do DDL físico.

Não são incluídos credenciais, registros individuais ou conteúdo das descrições.

## Escopo validado

A evidência demonstra uma chamada efetiva da procedure oficial, na instalação corporativa informada, a partir da VPS, para a referência Disposition. Nessa execução foram recebidos 126 registros em um dataset, com ReturnValue 0, outputs nulos e encerramento do processo com código 0.

Os nomes e tipos recebidos coincidem com o layout documental de Disposition registrado na [Fase 3B](phase3b-olos-contracts.md): DispositionId INT e Description NVARCHAR(200). Esta evidência acrescenta a nulabilidade observada no retorno dessa chamada. O total de 126 é uma observação daquela execução, não uma cardinalidade permanente ou limite configurável.

O resultado complementa a [Fase 3D](phase3d-real-transport.md), que testou transporte binário em SQL Server Developer isolado. São evidências distintas: o ensaio isolado não se transforma em validação da instalação Olos por associação com esta chamada manual. Os relatórios anteriores permanecem como registros históricos do conhecimento disponível em cada fase.

## Limitações e pendências

- **Persistência PostgreSQL não certificada:** este teste não demonstra ingestão, transação, UPSERT, deduplicação, checkpoint, recibo, auditoria ou confirmação durável no PostgreSQL.
- **Transporte binário não certificado:** a presença de metadados mssql não comprova uso do adaptador TDS da Fase 3D nem aquisição anterior ao parser. Não foram exercitados DATETIME/ticks de 1/300 segundo, BIGINT ou DECIMAL por esse layout. Não há certificação de preservação binária, collation ou fidelidade Unicode integral por comparação de conteúdo.
- **Completude e identidade ainda delimitadas:** receber 126 registros não prova, por conciliação independente, que correspondam à totalidade existente na origem naquele instante. Não comprova PK/UNIQUE, estabilidade dos IDs, ausência de duplicatas ou constraints físicas além dos metadados relatados.
- **Resultado de sucesso localizado:** ReturnValue 0 e outputs nulos são o resultado observado; não definem uma regra universal de sucesso para todas as procedures, tabelas ou situações.
- **Sem validação incremental:** não foram validados ReadDataCurrentDay, ReadDataPreviousDays, partições diárias, limites de 200.000 registros, paginação, controle compartilhado de consumo ou efeitos de concorrência. Não se infere ausência de efeitos remotos apenas dos outputs desta chamada.
- **Sem validação de recuperação:** não há ensaio corporativo de interrupção, timeout, perda da VPN, backpressure, disco cheio, spool, cancelamento, commit sem ACK ou reinicialização. LogReadData, ReReadData, ResetReadData e DataAvailable não são validadas por esta evidência.
- **Conectividade pontual:** o sucesso demonstra acesso naquela execução, não disponibilidade contínua da VPN nem certificação da política TLS. Versão exata do servidor, configuração de conexão e permissões efetivamente utilizadas não foram detalhadas neste registro.

Uma futura etapa precisa vincular script/versões/configuração sanitizada à execução, confirmar identidade e contrato operacional, testar explicitamente o transporte escolhido e conciliar dados antes de aprovar persistência e consumo contínuo. A autorização desta tarefa limita-se ao registro documental; não autoriza essas operações futuras.

## Estado do projeto e ações desta tarefa

Foi criado somente este documento. Não foram alterados código, contratos executáveis, aprovações, testes, migrations, permissões ou bloqueios operacionais. Não foi executada a suíte nesta tarefa documental; os 351 testes aprovados na Fase 3D são evidência histórica, não uma nova execução.

`SYNC_ENABLED=false` permanece no exemplo e no Dockerfile, e a configuração continua rejeitando sua habilitação. A chamada manual relatada não habilita o worker nem remove o bloqueio de endpoint corporativo.

**Nenhuma procedure adicional, migration, leitura incremental ou operação corporativa foi executada nesta tarefa. Não houve alteração de infraestrutura, commit, push ou deploy.**
