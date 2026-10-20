# Saída do worker para Olos via WireGuard

O Compose preserva `isolated` (`internal: true`) para worker ↔ postgres e adiciona `olos_egress`, bridge não interna, **somente ao worker**. PostgreSQL não recebe gateway externo por essa rede. Não há alteração de subnet/IPAM da rede existente (172.24.0.0/16 na VPS), volumes, comando, credenciais, portas ou domínios. A nova subnet é alocada pelo Docker: conferir ausência de sobreposição com redes/rotas existentes antes de usá-la.

Uma bridge não interna dá saída ao serviço; ela não implementa ACL exclusiva para `10.196.234.11:1433`. Esta alteração limita a associação da nova rede ao worker. As exceções opcionais abaixo liberam **somente esse destino/porta**, sem ampliar regras para outras aplicações. Um requisito adicional de bloquear toda outra saída do worker exigiria política de firewall própria, fora deste diff mínimo.

O Docker deve escolher a única rede não interna como saída. Não se utiliza `network_mode: host`, rede da discadora, IP fixo, gateway manual ou `gw_priority` dependente de versão recente do Compose. Referências: [redes internas e externas](https://docs.docker.com/compose/how-tos/networking/), [firewall Docker/iptables](https://docs.docker.com/engine/network/firewall-iptables/).

## Redeploy somente do worker

Após disponibilizar o diff em uma revisão GitHub autorizada, no serviço Compose **existente** do Dokploy manter projeto e Compose Path. Em Advanced → comando customizado, usar os valores reais exibidos pelo painel:

```text
compose -p <PROJETO_EXISTENTE> -f <COMPOSE_PATH_EXISTENTE> up -d --no-deps worker
```

O painel prefixa `docker`. Não adicionar `--remove-orphans`, `down`, migrations ou comandos de aquisição. A imagem não mudou nesta tarefa; não é necessário rebuild. Compose cria a rede nova e recria somente worker. PostgreSQL deve conservar ID e volume. Conferir que o painel não injeta domínios, portas ou redes compartilhadas. Manter `SYNC_ENABLED=false`, boot `node dist/main.js`, Auto Deploy/Schedules desativados.

Depois, no terminal do **worker**:

```bash
npm run worker:local -- check
node -e 'const net=require("node:net"); let finished=false; const finish=(code,result)=>{if(finished)return;finished=true;console.log(JSON.stringify({target:"10.196.234.11:1433",result,applicationBytesSent:0}));socket.destroy();process.exitCode=code;}; const socket=net.createConnection({host:"10.196.234.11",port:1433}); socket.setTimeout(5000); socket.once("connect",()=>finish(0,"TCP_CONNECTED")); socket.once("timeout",()=>finish(1,"TCP_TIMEOUT")); socket.once("error",e=>finish(1,e.code||"TCP_ERROR"));'
```

O segundo comando realiza **uma** conexão, timeout 5s, sem PRELOGIN, TLS, autenticação, SQL ou retries. Exit 0 significa apenas TCP conectado; não comprova certificado/contrato/recuperação nem autoriza aquisição. Não executar `once`, `recover` ou `start`. `check` deve continuar com banco up/schemaReady true e aquisição desabilitada.

## Diagnóstico do host, antes de propor regras

As instruções seguintes são para execução administrativa futura na VPS; **não foram executadas nesta entrega**. Usar somente se o teste de dentro do worker falhar. O TCP do host já confirmado não comprova tráfego encaminhado de containers.

Identificar o worker e a nova rede pelo projeto existente, sem supor nomes de interfaces:

```bash
docker compose -p <PROJETO_EXISTENTE> -f <COMPOSE_PATH_EXISTENTE> ps -q worker
docker network ls --filter label=com.docker.compose.project=<PROJETO_EXISTENTE> --filter label=com.docker.compose.network=olos_egress
docker inspect <ID_WORKER> --format '{{json .NetworkSettings.Networks}}'
docker network inspect <NOME_REDE_OLOS_EGRESS>
ip -4 route get 10.196.234.11
sysctl net.ipv4.ip_forward
iptables -S FORWARD
iptables -S DOCKER-USER
iptables -t nat -S POSTROUTING
```

Conferir: worker em duas redes, postgres somente em isolated, nova rede `Internal=false` e apenas um membro. A rota a `.11` deve apontar à interface WireGuard real. `ip_forward` deve ser 1; se for 0, parar para revisão de infraestrutura, sem alterar sysctl global nesta tarefa. O bridge pode vir de `Options.com.docker.network.bridge.name`; sem essa opção, usualmente é `br-` + os primeiros 12 caracteres do ID da rede. Confirmar com `ip link show`, não adivinhar `br-*` ou `wg0`.

Se há policy routing, conferir também a seleção da rota do tráfego encaminhado com `ip -4 route get 10.196.234.11 from <IP_WORKER_EGRESS> iif <BRIDGE_IF>`. Não adicionar tabela/regra global para resolver uma hipótese. A rota host `/32` já confirmada pode ser suficiente.

Docker normalmente já fornece forwarding e MASQUERADE para bridges não internas. **Se o teste funcionar, não adicionar regras.** Inspecionar contadores e políticas existentes antes de qualquer exceção. Se `DOCKER-USER` não existir porque o daemon usa backend nftables nativo ou outro gerenciador de firewall, parar: estes comandos iptables não se aplicam; revisar o backend real sem trocar backend/reiniciar Docker. `iptables-nft` com cadeia DOCKER-USER existente é diferente de backend Docker nftables nativo.

## Exceções condicionais, mínimas e reversíveis

Somente após confirmar bloqueio no forwarding pelo backend iptables, preencher em **Bash** com os valores efetivamente inspecionados. `WORKER_IP` é o IPv4 do worker **na nova rede**, sem máscara; não é o IP em isolated. Não usar a subnet 172.24.0.0/16, a subnet inteira da nova bridge, interface genérica ou `0.0.0.0/0`.

```bash
WORKER_IP='<IPv4_WORKER_NA_OLOS_EGRESS>'
BRIDGE_IF='<INTERFACE_BRIDGE_CONFIRMADA>'
WG_IF='<INTERFACE_WIREGUARD_CONFIRMADA>'
ip link show "$BRIDGE_IF"
ip link show "$WG_IF"
```

Aplicar **uma vez**, se necessárias, somente as regras ausentes. Não duplicar regras já instaladas:

```bash
iptables -I DOCKER-USER 1 -i "$BRIDGE_IF" -o "$WG_IF" -s "$WORKER_IP/32" -d 10.196.234.11/32 -p tcp --dport 1433 -m conntrack --ctstate NEW,ESTABLISHED -m comment --comment olos-worker-tds-out -j ACCEPT
iptables -I DOCKER-USER 1 -i "$WG_IF" -o "$BRIDGE_IF" -s 10.196.234.11/32 -d "$WORKER_IP/32" -p tcp --sport 1433 -m conntrack --ctstate ESTABLISHED -m comment --comment olos-worker-tds-in -j ACCEPT
```

Não adicionar uma liberação genérica de ESTABLISHED, mudar policy FORWARD para ACCEPT, fazer flush ou alterar DOCKER-FORWARD. As regras acima são exceções específicas, não uma política de bloqueio de outros destinos. Uma política administrativa deliberada deve ser revisada antes de inserir ACCEPT no topo.

Se o pacote já sai pelo WireGuard e o retorno da nova subnet não existe no lado corporativo, conferir se a regra de MASQUERADE padrão Docker já traduz a origem para o IP do WireGuard. Só na ausência/inadequação dessa tradução e após confirmar a necessidade, adicionar:

```bash
iptables -t nat -I POSTROUTING 1 -s "$WORKER_IP/32" -d 10.196.234.11/32 -o "$WG_IF" -p tcp --dport 1433 -m comment --comment olos-worker-tds-nat -j MASQUERADE
```

O retorno usa conntrack; não requer outra regra NAT. Este MASQUERADE específico evita pedir rota corporativa para toda a bridge. Não alterar AllowedIPs, rotas, peer, PostUp/PostDown ou NAT genérico do WireGuard utilizado pela discadora para `10.196.234.4`.

Verificar contadores das regras e repetir manualmente o teste TCP apenas após corrigir a causa; não criar loop de tentativas. Regras por `/32` precisam de revisão se o worker for recriado e receber outro IP. Não persistir IP dinâmico em configuração global de VPN; para manter exceções após reboot/redeploy, combinar sua reaplicação restrita e identificação do novo IP com o responsável pelo firewall.

Rollback, **somente das regras desta tarefa que foram efetivamente inseridas**, usando os mesmos valores originais:

```bash
iptables -D DOCKER-USER -i "$BRIDGE_IF" -o "$WG_IF" -s "$WORKER_IP/32" -d 10.196.234.11/32 -p tcp --dport 1433 -m conntrack --ctstate NEW,ESTABLISHED -m comment --comment olos-worker-tds-out -j ACCEPT
iptables -D DOCKER-USER -i "$WG_IF" -o "$BRIDGE_IF" -s 10.196.234.11/32 -d "$WORKER_IP/32" -p tcp --sport 1433 -m conntrack --ctstate ESTABLISHED -m comment --comment olos-worker-tds-in -j ACCEPT
iptables -t nat -D POSTROUTING -s "$WORKER_IP/32" -d 10.196.234.11/32 -o "$WG_IF" -p tcp --dport 1433 -m comment --comment olos-worker-tds-nat -j MASQUERADE
```

Não executar a remoção NAT se ela não foi inserida. Para rollback do Compose, voltar ao worker apenas em `isolated` e repetir o redeploy `--no-deps worker`; não usar `down`, não remover isolated/volumes. A rede de saída vazia pode permanecer até limpeza explícita pelo nome confirmado, sem afetar outros projetos.

## Evidência local — 20/10/2026

`docker compose config --quiet` e 23 testes direcionados (`runtime-postgres-configuration`, `attempts-corporate-diagnostics`) passaram. Comparação do Compose renderizado contra HEAD confirmou que **somente** a associação do worker e a nova definição de rede mudaram; todo o restante é idêntico.

Teste real executado em container Docker **local descartável**, na imagem `olos-runtime-validation-worker`, rede Docker bridge com saída, sem volumes ou credenciais da aplicação. Uma conexão a `10.196.234.11:1433` retornou:

```json
{"target":"10.196.234.11:1433","result":"TCP_CONNECTED","applicationBytesSent":0}
```

O comando foi o mesmo Node TCP acima, executado via `docker run --rm --network bridge --read-only --cap-drop ALL --security-opt no-new-privileges:true ... node -e ...`. Nenhum byte de aplicação foi enviado. A rota do host local era via eth0, não uma comprovação da bridge/WireGuard da VPS. O container foi removido automaticamente. Não houve acesso, deploy, teste TCP dentro do worker **da VPS** ou alteração de firewall remoto. O teste no worker implantado permanece como verificação pós-redeploy do operador.
