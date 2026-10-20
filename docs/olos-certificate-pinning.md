# Pinning TLS manual da Olos

Implementação de 19/10/2026. Nenhuma conexão Olos executada. A aquisição AttemptsRawData e seus gates não foram alterados. SYNC_ENABLED=false permanece obrigatório na CLI. Não houve acesso ao PostgreSQL, aos históricos, VPS ou run-001/run-002.

Evolução posterior: [preparação do transporte Attempts](attempts-corporate-pinned-transport.md), ainda sem autorização de conexão/consumo corporativo.

## Viabilidade e escopo

O Tedious instalado (18.6.2, utilizado pelo mssql 11) cria `tls.connect` em `lib/message-io.js`, mas não encaminha um callback `checkServerIdentity`. `cryptoCredentialsDetails` alimenta `createSecureContext`, que não instala esse callback. Portanto, **não é possível implementar pinning apenas acrescentando uma opção à configuração pública atual do driver**.

Foi implementada uma subclasse isolada de MessageIO, apenas no diagnóstico PRELOGIN/TLS. Ela reutiliza framing/streams do Tedious, substituindo somente a negociação desta instância. Não altera prototypes, node_modules, TLS global ou ExactConnection. Não possui caminho de autenticação SQL ou execução de comandos. A versão 18.6.2 é exigida; atualização exige revisão/testes e não ativa fallback.

A negociação usa TLS 1.2 exclusivamente (compatibilidade TDS 7.x existente), `rejectUnauthorized=true`, âncora de confiança restrita ao certificado autoassinado explicitamente aprovado e `checkServerIdentity` com SHA-256 do certificado DER completo e `timingSafeEqual`. Há nova comparação após secureConnect, verificação de validade e aprovação ainda vigente; renegociação é desabilitada. Não há cache de sessão, retry, downgrade ou confiança genérica em autoassinados.

A identidade **por certificado aprovado** substitui a identidade por DNS neste modo específico. SAN ausente ou nome incompatível continuam reportados em `name`; não são apresentados como DNS validado. `chain=valid` significa cadeia validada contra a âncora privada aprovada, não contra as CAs públicas do sistema. A saída distingue `identity=certificate-sha256-pin` e `code=PIN_VERIFIED`.

Referência: [Node.js 20 — tls.connect/checkServerIdentity](https://nodejs.org/docs/latest-v20.x/api/tls.html#tlscheckserveridentityhostname-cert). O callback não substitui a validação de confiança/validade pelo OpenSSL. Por isso **um fingerprint sozinho não basta** nesta implementação: o PEM público aprovado também é obrigatório. Certificados que o OpenSSL rejeitar continuam bloqueados.

## Arquivos

- `src/security/olos-certificate-pin.ts`: contrato estrito de aprovação privada, leitura segura, validade e comparação criptográfica.
- `src/diagnostics/pinned-message-io.ts`: negociação TDS/TLS isolada, sem LOGIN7/RPC.
- `src/diagnostics/tds-tls.ts`: modo opt-in verify-pinned, endpoint corporativo fixo e resultados sanitizados.
- `adapters/olos-tls-diagnostic.ts`: flag de arquivo privado; verify permanece padrão.
- `test/olos-certificate-pin.test.ts`: testes sintéticos locais.
- `test/support/tls-diagnostic-fixture.ts`: disponibiliza a chave da CA **sintética** à fixture local; nunca recebe material corporativo.
- Este guia.

Não houve mudança em runtime, env geral, pools, transporte de aquisição, gates, scheduler, migrations ou dependências.

## Cadastrar uma aprovação

O fingerprint informado anteriormente é **somente observado**. Ele não foi incorporado ao código, allowlist ou configuração privada. Não existe aprovação criada por esta entrega.

Antes do cadastro, obter o certificado público PEM completo e confirmar seu SHA-256 por um canal administrativo independente e autenticado com a equipe responsável pela Olos. A inspeção anterior pela mesma rede não prova a identidade. Nunca solicitar/exportar a chave privada do servidor.

Criar o arquivo fora do repositório, por exemplo `~/.config/olos-worker/tls/olos-pin-approval.json`, com diretório 0700 e arquivo 0600, pertencentes ao usuário, sem symlinks. O arquivo tem exatamente estes campos (substituir placeholders e representar as quebras do PEM com `\n`):

```json
{
  "version": 1,
  "purpose": "OLOS_TDS_TLS_METADATA_ONLY",
  "host": "10.196.234.11",
  "port": 1433,
  "database": "ExportDataV3",
  "approvalId": "REFERENCIA_DA_APROVACAO_OPERACIONAL",
  "approvedBy": "RESPONSAVEL_QUE_CONFIRMOU_A_IDENTIDADE",
  "approvedAt": "INSTANTE_UTC_ISO8601",
  "expiresAt": "INSTANTE_UTC_ISO8601",
  "fingerprintSha256": "SHA256_APROVADO_COM_32_BYTES_HEXADECIMAIS_SEPARADOS_POR_DOIS_PONTOS",
  "certificatePem": "-----BEGIN CERTIFICATE-----\nBASE64_DO_CERTIFICADO_PUBLICO_APROVADO\n-----END CERTIFICATE-----\n"
}
```

Usar datas UTC terminadas em Z, aprovação já vigente e prazo preferencial de 24 horas, **máximo de sete dias**. A validade de décadas do certificado não prolonga essa autorização. Ausência/expiração, campos extras, PEM contendo chave ou múltiplos certificados, fingerprint incompatível, endpoint/banco/propósito diferentes e certificado fora de validade são recusados. Apenas um certificado autoassinado por aprovação. A referência/nome do aprovador são uma declaração operacional protegida por permissões de arquivo, não assinatura digital nem sistema de aprovação externo; quem puder alterar este arquivo pode substituir a confiança.

Não utilizar .env, argumento de fingerprint, certificado observado como cadastro automático, diretório versionado ou CA global. O arquivo pode ser preparado com editor local seguro; não colar seu conteúdo em logs/issues. O PEM/fingerprint são públicos, mas o arquivo é política de confiança e deve ser protegido contra alteração.

## Verificação manual futura, sem SQL

**Não foi executada nesta tarefa.** Somente após validação independente do certificado, aprovação e autorização da conexão:

```bash
cd ~/worker-olos-export-data
SYNC_ENABLED=false npm run diagnose:olos-tls -- \
  --authorize TDS_TLS_METADATA_ONLY \
  --mode verify-pinned \
  --server-name 10.196.234.11 \
  --pin-approval-file "$HOME/.config/olos-worker/tls/olos-pin-approval.json"
```

Uma tentativa TCP, PRELOGIN com criptografia exigida, TLS e encerramento; timeout total 10 s e limite de bytes existente. Nunca envia credenciais, LOGIN7, SQL, USE ou procedures. `database=ExportDataV3` vincula a política pretendida, mas não comprova acesso ao banco: não há login. O endpoint efetivo da CLI é sempre 10.196.234.11:1433. A configuração do diagnóstico não aceita destino alternativo em modo pinning; a classe de baixo nível também é exercitada diretamente em loopback pelos testes.

Exit 0 somente para PIN_VERIFIED; 64 para configuração/aprovação recusada; 1 para falha de negociação/pin/rede/protocolo. Inspeção sem validação continua separada, exit 2, e jamais cria aprovação. Pinning não aceita --ca-file simultâneo. O modo padrão verify continua exigindo cadeia e nome DNS normalmente válidos.

Não prosseguir em PIN_REJECTED, certificado diferente, expiração, TLS incompatível, timeout ou falha de rede. Não repetir automaticamente, não reduzir TLS e não trocar para trustServerCertificate. A saída contém apenas metadados públicos e estados; não retorna PEM/chave/aprovação completa.

## Validação e limites

- Typecheck e build: aprovados.
- Testes direcionados: `npm test -- test/olos-certificate-pin.test.ts test/tls-diagnostic.test.ts`.
- 49 testes aprovados em dois arquivos: handshake TDS/TLS sintético criptografado, self-signed sem SAN, troca de folha na mesma cadeia, fingerprint/arquivo/aprovação inválidos, expiração da aprovação, configuração insegura recusada e regressões do diagnóstico existente.
- Em todos os handshakes novos: apenas pacotes PRELOGIN, zero bytes de aplicação/LOGIN7/SQL e uma conexão por execução.
- Não há certificação do SQL Server corporativo nem novo teste com SQL Server real. A validade/compatibilidade do certificado fallback concreto depende da execução manual autorizada e do PEM aprovado, não disponível nesta tarefa.
- Acesso a usuários/controles incrementais/recuperação de consumo não foi autorizado por pinning. Todos os bloqueios de Attempts continuam presentes. Este modo **não está conectado ao runtime de aquisição**, ao boot ou à CLI Attempts. Sua promoção futura requer revisão separada de transporte/gates e validação da recuperação.
- Expiração da aprovação é verificada no início e durante o handshake; o diagnóstico fecha logo após autenticar. Não há conexão persistente coberta por esta aprovação.
- Pin completo exige nova aprovação para qualquer reemissão, inclusive mesma chave. Comprometimento da chave privada/arquivo de confiança, aprovação por canal não autenticado e mudança silenciosa de certificado são riscos operacionais. Revogação OCSP/CRL não foi implementada.

## Rotação e retirada

1. Planejar a troca com a equipe Olos e confirmar o novo certificado por canal independente.
2. Preparar outro arquivo privado com novo PEM/fingerprint, responsável, referência e prazo curto; não aceitar automaticamente o certificado que aparecer na rede.
3. Executar uma verificação manual autorizada usando explicitamente o novo arquivo. Mismatch interrompe a operação; não existe fallback ao pin antigo ou CA pública.
4. Revogar/remover a aprovação antiga de forma controlada. Não habilitar aquisição por sucesso desse diagnóstico.
5. Quando a Olos fornecer cadeia confiável e identidade DNS correta, validar primeiro no modo verify e retirar o uso de verify-pinned. Manter os gates de aquisição até aprovação independente dos contratos e da recuperação.
