# Fase 4 — integração controlada do worker: Disposition

## Escopo e evidência disponível

Foi preparado um modo separado do runtime de produção para **uma tentativa autorizada de `dbo.usp_ExportData_ReadData`, com `TableName='Disposition'`**, usando o transporte binário, o coordenador, o spool e a persistência do worker. Nenhuma chamada à Olos foi executada nesta tarefa.

Evidência informada pela empresa, sem repetir as operações:

| Leitura manual | Evidência | Alcance |
|---|---|---|
| ReadData('Disposition') | 126 registros, um dataset, ReturnValue 0, Message/ErrorMessage null; DispositionId INT NOT NULL; Description NVARCHAR(200) NOT NULL, 400 bytes | Registrada em [Fase 3E](phase3e-real-disposition-validation.md); não certifica persistência nem transporte binário |
| ReadData('Campaign') | 51 registros, um dataset, ReturnValue 0, outputs null | Relato fornecido nesta solicitação; Campaign não foi habilitada neste modo |
| LogReadData('AttemptsRawData') | Histórico de quatro intervalos em 01/10/2026 | Relato fornecido; intervalos/PKs não fornecidos, sem inferência de autoria, retenção ou recuperação |

O total 126 é histórico, **não uma asserção de teste real nem limite de origem**. Os contratos anteriores permanecem não aprovados para produção. Nenhum fluxo incremental, ReReadData, ResetReadData, DataAvailable, Campaign ou LogReadData foi liberado para a instalação corporativa.

## Arquitetura implementada

| Arquivo | Responsabilidade |
|---|---|
| `src/olos/tds/disposition-access.ts` | Configuração Zod, aprovação expirável vinculada ao teste, capacidade por identidade e orçamento de uma tentativa/RPC |
| `src/olos/tds/source.ts` | Reutilização da sessão TDS da Fase 3D, preservando seu caminho de fixtures; metadados observados sem conteúdo |
| `src/olos/tds/connection.ts` | Exceção restrita por capacidade para o endpoint Olos, com TLS verificado e gate do RPC exato |
| `src/olos/tds/raw-parser.ts` | Política explícita de captura da collation de NVARCHAR, mantendo UTF-16LE exato e demais validações |
| `src/integration/disposition.ts` | Plano fixo, produtor controlado, detecção de IDs repetidos e replay local sem porta de origem |
| `src/large-batch/types.ts`, `coordinator.ts` | Identificação e admissão da instância controlada; mesmos intent, selo, transação e checkpoints anteriores |
| `adapters/disposition-integration.ts` | Entrada operacional isolada: preparo do PostgreSQL próprio, aprovação, tentativa durável, execução, recuperação e conciliação |
| `package.json` | Comando explícito `integration:disposition`, sem hooks automáticos |
| `test/phase4-disposition.test.ts` | Ensaios com SQL Server e PostgreSQL descartáveis; sem endpoint corporativo nos RPCs de teste |

A entrada é `npm run integration:disposition -- <comando> ...`, executada com tsx e dependências de desenvolvimento. Não está no CMD Docker nem é importada pelo HTTP, probes ou scheduler. O build compila os módulos centrais e verifica o adaptador; o launcher é usado pelo comando npm no checkout, não pelo runtime de produção. Não há execução automática de migrations no boot/build.

`SYNC_ENABLED=false` é exigido também pela CLI; true e ausência da variável são recusados. O runtime normal continua rejeitando habilitação. Gates genéricos de mssql e do cliente anterior permanecem bloqueados.

### Restrições de origem e aprovação

- Endpoint fixo: 10.196.234.11:1433, ExportDataV3, usuário OEC. TLS: encrypt=true, trustServerCertificate=false, mínimo 1.2, CA e nome de certificado fornecidos explicitamente. Não há flag de TLS inseguro ou fallback para driver/conversão diferente.
- A aprovação registra testId, ação literal, referência operacional, data de observação e expiração, no máximo 12 horas à frente. O arquivo é vinculado ao fingerprint da configuração pública de origem/TLS/metadados. A senha não entra no fingerprint.
- O gate é validado antes da conexão e o RPC é novamente limitado no dispatch: apenas ReadData, TableName Disposition e os dois OUTPUTs oficiais. Uma capacidade não pode emitir outro RPC.
- A CLI grava `source-attempt.json` com criação exclusiva e fsync antes de entrar no coordenador. O arquivo nunca é apagado, mesmo após falha ou reinício. Duas instâncias não conseguem usar o mesmo diretório para novas leituras. Uma nova execução com outro diretório exige novo testId e nova aprovação explícita.
- Não existe retry/reconexão automática da procedure; retries transitórios de login do Tedious são explicitamente desativados (`maxRetriesOnTransientErrors=0`). Falha de conexão também consome a tentativa operacional. O claim não prova que o servidor recebeu a chamada; registra uma tentativa conservadora.
- Aprovação é um controle operacional local baseado no operador do sistema e em arquivos privados; não é assinatura digital nem substitui o processo de aprovação da empresa. Usuários privilegiados do host/Docker continuam fora dessa fronteira de proteção.

### Contrato e metadados

O plano é fixo: DispositionId INT NOT NULL e Description NVARCHAR(200) NOT NULL. Valida quantidade, ordinais, nomes, tipos, larguras, nulabilidade, dataset único, sucesso do RPC e outputs nulos. Qualquer divergência interrompe sem selo/checkpoint. O retorno normal é 0 exclusivamente para este plano, com base na leitura manual informada.

NVARCHAR é adquirido em UTF-16LE pelo codec binário. Os bytes de collation são capturados em `source-metadata.json`, sem executar consulta extra de catálogo ou outra procedure. Essa política não afirma semântica de comparação/ordenação da collation; não há joins nem normalização. Se a empresa fornecer previamente `collationHex`, a igualdade desses cinco bytes passa a ser obrigatória. Os demais contratos do parser continuam exigindo suas políticas anteriores, especialmente para CHAR/VARCHAR.

IDs são preservados como strings inteiras exatas no envelope; texto mantém Unicode, vazio e espaços. NULL é recusado nos dois campos. O modo rejeita qualquer ID repetido no snapshot, inclusive com conteúdo idêntico. A chave utilizada é **condicional à unicidade comprovada nessa resposta e ao destino vazio deste teste**; não se declara PK física ou unicidade global da Olos. O contrato aprovado é inserido somente no PostgreSQL próprio, sem alterar o catálogo corporativo ou os contratos documentais anteriores.

Limites conservadores locais: 10.000 registros, 64 MiB de spool, reserva livre de disco de 64 MiB, timeout de leitura de 30 s, demais buffers/parcelas da Fase 3C. Não são page size remoto. Exceder um limite interrompe, sem confirmar um prefixo. Zero registros é representável; exige revisão operacional, sem retry automático.

A data `--snapshot-date` é um rótulo explícito do snapshot de referência no controle local. Não é inferida de uma coluna nem enviada como parâmetro inexistente a ReadData.

### Destino e durabilidade

`prepare` cria seu próprio PostgreSQL 16 Docker, banco fixo `phase4_worker_fixture`, porta efêmera somente em 127.0.0.1, label vinculada ao testId e senha aleatória. Não há opção PG_URL/host/banco externo nem leitura de configuração PG corporativa. Antes de conectar, verificar ID, label, imagem, porta, banco e credencial de criação por docker inspect. Não reutilizar containers existentes. Antes da leitura, também é exigida ausência de runs e de envelopes Disposition no banco de teste; nada é apagado para torná-lo vazio.

Somente `prepare`, explicitamente invocado, aplica 001/002/005 e permissões worker/large-batch nesse container novo. Não há migrations novas, aplicação em public corporativo ou inclusão de estruturas de negócio. A conta da fase de ingestão assume o role limitado; a conta administrativa é usada apenas no preparo.

Intent/checkpoint e manifesto precedem a leitura; resposta completa e fim do transporte precedem o selo fsync/hash; selo precede a persistência. Envelopes, recibo, contagens, auditoria e checkpoint são transacionais, sem commits por parcela. O diretório privado retém arquivos e manifests após confirmação. O PostgreSQL usa armazenamento próprio do container/volume Docker, sem tmpfs; mantê-lo até concluir a conciliação.

A conciliação compara todo o conteúdo do spool e dos envelopes em memória, valida chaves derivadas, recibo de lote confirmado, contagens e checkpoint. Imprime somente contagem, digest agregado, versão de checkpoint e UUID de lote, sem IDs/descrições da Olos ou credenciais.

`replay` valida o arquivo completo e usa a identidade original. Exige conciliação antes/depois e verifica que checkpoint/conteúdo não mudaram. O recibo idempotente devolve as contagens originais; isso **não** significa que essas linhas tenham sido inseridas novamente. Replay não tem acesso a um cliente SQL Server.

`recover` opera apenas em spool completo pendente. Uma falha simulada de destino ocorre depois do selo e antes da escrita; o reinício utiliza a mesma identidade para concluir a transação. Lease configurada em cinco minutos: na ausência de recibo confirmado, recuperação após expiração falha conservadoramente, sem adoção automática. Lote parcial não pode ser entregue como completo.

Spool e transação PostgreSQL não fornecem atomicidade distribuída com a Olos. Cancelar/fechar conexão não prova rollback remoto. Não há garantia de ausência de perda, de ausência de efeitos remotos ou de recuperação incremental nesta entrega.

## Instruções para uma única execução manual futura

**Não executadas nesta tarefa.** Somente um operador autorizado deve seguir a etapa de aprovação e leitura. Revisar código, contratos, autorização, janela de operação, CA/certificado e retenção antes de prosseguir. Não usar este modo para outras tabelas ou para testar a senha repetindo chamadas.

### 1. Preparar o ambiente separado

Usar checkout revisado, Node compatível, npm/dependências de desenvolvimento e Docker local. O usuário precisa poder criar um PostgreSQL próprio. A imagem `postgres:16-bookworm` deve estar disponível. WireGuard/VPN existente não deve ser alterada.

```bash
cd /home/madm/worker-olos-export-data
export SYNC_ENABLED=false
install -d -m 700 /home/madm/worker-integration-runs
export PHASE4_WORK_DIR=/home/madm/worker-integration-runs/disposition-20261016-01
npm run integration:disposition -- prepare --work-dir "$PHASE4_WORK_DIR" --snapshot-date 2026-10-16
```

Escolher diretório **novo**, absoluto, canônico e fora do repositório, e a data real de observação autorizada. A preparação cria o diretório com modo 0700; arquivos são 0600 e devem pertencer ao operador. O caminho não pode conter symlinks. `state.json` contém a senha do PostgreSQL de teste: não copiar para Git, logs ou relatório público. Se o preparo falhar, não apagar arquivos/containers para tentar novamente sem revisar o estado registrado.

### 2. Fornecer configuração pública e segredo protegido

Criar por provisionamento seguro um arquivo externo, por exemplo `/home/madm/worker-integration-secrets/disposition-source.json`, privado 0600, pertencente ao operador. Formato, substituindo os placeholders pela CA e identidade realmente aprovadas:

```json
{
  "source": {
    "server": "10.196.234.11",
    "port": 1433,
    "database": "ExportDataV3",
    "user": "OEC",
    "ca": "-----BEGIN CERTIFICATE-----\nCONTEUDO_DA_CA_APROVADA\n-----END CERTIFICATE-----",
    "serverName": "NOME_EXATO_VALIDADO_NO_CERTIFICADO"
  }
}
```

Não inserir `password` no JSON: é recusado. A senha vem **exclusivamente** de `PHASE4_OLOS_PASSWORD`, por ambiente protegido separado de OLOS_PASSWORD/PG_* do runtime. Para uma sessão manual, com tracing desabilitado:

```bash
set +x
read -r -s -p 'Senha OEC para o teste autorizado: ' PHASE4_OLOS_PASSWORD
export PHASE4_OLOS_PASSWORD
export PHASE4_SOURCE_CONFIG=/home/madm/worker-integration-secrets/disposition-source.json
```

Não colocar a senha na linha de comando, histórico ou arquivo versionado. Não foi inventado nome de certificado ou certificado confiável para a instalação real. Se esses dados não estiverem disponíveis, **parar antes de aprovar/ler**, sem reduzir TLS. Pode-se adicionar `"collationHex":"<10 digitos hex confirmados>"` na raiz do JSON para exigir collation previamente comprovada; não preencher com valor inferido de outra instalação.

### 3. Aprovação operacional explícita — sem conexão à origem

Depois de obter aprovação pelo processo da empresa, usar sua referência real e uma expiração curta. O comando abaixo valida e grava a aprovação local; não executa procedure nem diagnóstico remoto.

```bash
export PHASE4_EXPIRES_AT="$(node -p 'new Date(Date.now()+60*60*1000).toISOString()')"
npm run integration:disposition -- approve \
  --work-dir "$PHASE4_WORK_DIR" \
  --source-config "$PHASE4_SOURCE_CONFIG" \
  --reference CHANGE-APROVADA \
  --expires-at "$PHASE4_EXPIRES_AT" \
  --confirm AUTHORIZE_READ_DISPOSITION_ONCE
```

Trocar `CHANGE-APROVADA` pela referência válida, sem dados pessoais. A aprovação é exclusiva desse testId/data/configuração e não pode ser sobrescrita automaticamente. O arquivo approval.json é evidência local dessa decisão, não concessão de autorização por este relatório.

### 4. Executar exatamente uma tentativa

**Escolher somente uma alternativa**, após aprovação:

Fluxo normal:

```bash
npm run integration:disposition -- read \
  --work-dir "$PHASE4_WORK_DIR" \
  --source-config "$PHASE4_SOURCE_CONFIG" \
  --confirm READ_DISPOSITION_ONCE
```

Ou, para ensaiar falha do destino na mesma e única leitura autorizada:

```bash
npm run integration:disposition -- read \
  --work-dir "$PHASE4_WORK_DIR" \
  --source-config "$PHASE4_SOURCE_CONFIG" \
  --confirm READ_DISPOSITION_ONCE \
  --simulate-destination-failure true
```

A segunda alternativa termina com saída 1 de propósito depois do spool completo, sem escrita operacional no destino. Não executar a primeira em seguida: o claim bloqueia nova leitura, e a ação correta é recuperação local. **O sinalizador de falha não altera a chamada à Olos, autorização ou proteções.**

### 5. Recuperar/reprocessar somente localmente

Após sucesso normal, usar replay. Após falha simulada de destino com spool selado, usar recover imediatamente, antes de vencer a lease, e depois replay. Os cinco minutos contam da última renovação, não necessariamente da saída do comando. Não fornecer configuração/credenciais de origem a esses comandos:

```bash
unset PHASE4_OLOS_PASSWORD
npm run integration:disposition -- status --work-dir "$PHASE4_WORK_DIR"
# Somente se houver spool completo pendente por falha de destino:
npm run integration:disposition -- recover --work-dir "$PHASE4_WORK_DIR"
# Após confirmação de persistência:
npm run integration:disposition -- replay --work-dir "$PHASE4_WORK_DIR"
```

`recover` após um sucesso já confirmado não é necessário e não substitui replay. Fonte interrompida, selo ausente, lease vencida sem recibo, corrupção ou PostgreSQL indisponível exigem parada/revisão; nenhum desses comandos fará uma nova chamada para preencher dados ausentes. Commit sem ACK deve ser investigado pelo recibo da mesma identidade; nunca iniciar outro read por inferência.

## Critérios de parada e aceite

Parar imediatamente, preservar diretório e container e não repetir a origem diante de:

- aprovação ausente, expirada, configuração alterada ou claim anterior;
- TLS/certificado incompatível, timeout, perda de rede ou cancelamento;
- metadata drift, dataset extra, status diferente de 0 ou outputs não nulos;
- ID repetido, NULL indevido, texto inválido, excesso de linhas/bytes, disco cheio ou hash divergente;
- falha de destino/COMMIT incerto, checkpoint inesperado, lease vencida ou divergência de conteúdo;
- quantidade inesperada, inclusive zero, mesmo quando o protocolo terminar com sucesso: revisar evidência, sem retry automático.

Aceite futuro exige: uma única chamada da origem; dois campos corretos; conteúdo integral conciliado sem expô-lo; spool completo preservado; contagens coerentes; checkpoint uma vez; replay sem alteração/nova chamada; recuperação de destino comprovada quando escolhida; nenhuma escrita fora do PostgreSQL próprio. Receber exatamente 126 não é critério de aceite.

Após revisão e retenção das evidências, o operador pode parar/remover **apenas o container identificado em state.json, conferindo antes sua label com o testId**. Não usar docker prune, nomes de containers existentes, nem apagar automaticamente spool/claim. A CLI não executa limpeza destrutiva. Decidir explicitamente sobre retenção/remoção do volume PostgreSQL de teste e do segredo no ambiente; aprovações e arquivos privados devem seguir a política da empresa.

## Validação desta entrega e limitações

Foram usados somente SQL Server Developer e PostgreSQL descartáveis. A fixture tem os mesmos dois campos e assinatura, mas corpo SQL sintético; testa três registros com Unicode, vazio e espaços para comprovar independência da contagem 126. Os testes também exercitam dados duplicados, metadata drift, aprovação expirada/ação incorreta, outra tabela/procedure recusadas, concorrência de claims, timeout sem retry, falha simulada de destino, reinício, replay e arquivos de segredo inseguros. A aprovação da CLI é exercitada com credencial sintética em ambiente, sem conexão corporativa.

Resultados finais após a revisão do destino vazio e a desativação dos retries transitórios de login:

| Verificação | Resultado |
|---|---|
| `npm run typecheck` | Exit 0 |
| `npm run build` | Exit 0; núcleo compilado e adaptador verificado |
| `npx vitest run test/phase4-disposition.test.ts` | 8 testes novos aprovados na rodada focada |
| `npm test` final | **359 testes / 26 arquivos aprovados**, exit 0; 351 anteriores + 8 novos; duração 137,20 s |
| Gate RPC/TLS/budget do código compilado | Verificação adicional offline aprovada: RPC único; trustServerCertificate=true e retry de login >0 recusados; nenhuma conexão criada |
| Integridade dos arquivos anteriores | Testes/suportes anteriores, migrations, permissões, persistência, projeção, boot e configuração preservados por comparação SHA-256 com baseline local |
| Limpeza das fixtures | Nenhum container remanescente nos labels exclusivos de fase4, phase3d e phase2a após a suíte |

Não foi solicitado nem executado Docker build nesta tarefa; os containers usados foram somente de teste. Nenhuma aprovação para uma chamada corporativa foi emitida/executada pelo agente; as aprovações exercitadas nos testes usaram configuração e credencial sintéticas, sem conexão à Olos.
 A suíte anterior foi preservada; nenhuma asserção ou bloqueio foi removido para passar os ensaios. A política nova de captura NVARCHAR é explícita e fica distinta da política de comparação de collation dos contratos anteriores.

A instalação real ainda precisa demonstrar compatibilidade com Tedious 18.6.2/TDS 7.4, TLS/certificado e contrato binário desse modo. Os relatos manuais não certificam esses pontos nem PostgreSQL. A extensão usa API privada por instância da Fase 3D, sem patch global ou edição de node_modules; atualização do driver requer nova validação. Não há reconciliação com LogReadData/ReReadData nem prova de completude independente do snapshot da origem.

**Nenhuma chamada à Olos, consumo incremental, migration/INSERT corporativo, alteração de WireGuard/discadora/serviços existentes, commit, push ou deploy foi executado nesta tarefa. SYNC_ENABLED permanece false.**
