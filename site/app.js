const orderedDays = ["dia-1", "dia-2", "dia-3", "dia-4"];
const repoUrl = "https://github.com/ferfaoliver-ferreira/Semana_do_Desenvolvedor_EDN";
const readmeBaseUrl = "https://github.com/ferfaoliver-ferreira/Semana_do_Desenvolvedor_EDN/blob/main";

const dayData = {
  "dia-1": {
    number: "Dia 1",
    title: "API e EventBridge",
    slug: "dia-1.html",
    readme: `${readmeBaseUrl}/dia-1-api-eventbridge/README.md`,
    image: "assets/arquitetura-dia-1.png",
    duration: "Base da ingestão em tempo real",
    intro:
      "O primeiro laboratório cria a fundação da solução com entrada via API REST, pré-validação, enfileiramento em FIFO e publicação de eventos em um custom event bus.",
    summary:
      "É aqui que o projeto deixa de ser apenas uma ideia arquitetural e passa a ter um fluxo funcional de pedidos entrando pela API, sendo tratados em etapas desacopladas e ficando prontos para evolução.",
    focus: [
      "Entrada síncrona com API Gateway e desacoplamento com Amazon SQS FIFO",
      "Duas funções Lambda para pré-validação e validação final do payload",
      "Publicação do evento NovoPedidoValidado no Amazon EventBridge"
    ],
    services: ["API Gateway", "AWS Lambda", "Amazon SQS FIFO", "DLQ", "Amazon EventBridge"],
    concepts: ["Ingestão", "Pré-validação", "Fila FIFO", "Custom event bus"],
    steps: [
      "Criar as roles IAM e as inline policies para envio e consumo das mensagens.",
      "Provisionar a fila principal FIFO e a DLQ para isolamento de falhas recorrentes.",
      "Configurar a API REST, integrar com a Lambda de pré-validação e testar o endpoint.",
      "Consumir a fila com a Lambda de validação e publicar o evento no custom event bus."
    ],
    outcomes: [
      "Pedido recebido via POST /pedidos",
      "Mensagens ordenadas por fila FIFO",
      "Logs e rastreabilidade no CloudWatch",
      "Evento pronto para os próximos dias da trilha"
    ],
    architectureNotes: [
      "A entrada do pedido acontece por um endpoint simples, enquanto a lógica principal já nasce desacoplada.",
      "A fila FIFO preserva ordem e reduz o acoplamento entre o recebimento e o processamento.",
      "O EventBridge prepara a arquitetura para novos consumidores sem alterar a origem."
    ],
    deliverables: [
      "API funcional para receber pedidos",
      "Lambda de pré-validação e Lambda de validação",
      "Fila principal com DLQ",
      "Evento de domínio publicado no barramento"
    ]
  },
  "dia-2": {
    number: "Dia 2",
    title: "S3 e Integração",
    slug: "dia-2.html",
    readme: `${readmeBaseUrl}/dia-2-s3-integracao/README.md`,
    image: "assets/arquitetura-dia-2.png",
    duration: "Entrada em lote e rastreamento",
    intro:
      "No segundo dia, o projeto ganha uma segunda porta de entrada: arquivos JSON enviados ao Amazon S3, validados por Lambda e integrados ao mesmo pipeline principal de pedidos.",
    summary:
      "A solução passa a aceitar pedidos em lote sem duplicar a lógica central. Em vez de criar um caminho paralelo, o laboratório reaproveita o fluxo do Dia 1 e adiciona rastreabilidade dos arquivos processados.",
    focus: [
      "Ingestão em lote por bucket Amazon S3 com notifications para Amazon SQS Standard",
      "Validação do arquivo, rastreamento em DynamoDB e alertas com Amazon SNS",
      "Envio dos pedidos válidos para a fila FIFO principal criada no Dia 1"
    ],
    services: ["Amazon S3", "Amazon SQS Standard", "AWS Lambda", "Amazon DynamoDB", "Amazon SNS"],
    concepts: ["Ingestão em lote", "Notifications", "Rastreabilidade", "Alertas"],
    steps: [
      "Criar a fila padrão, a DLQ e a tabela de histórico de arquivos.",
      "Criar a Lambda de validação para baixar, interpretar e validar os JSONs.",
      "Configurar notifications do bucket para a fila e ajustar as policies.",
      "Testar com arquivos válidos e inválidos para validar o fluxo e os alertas."
    ],
    outcomes: [
      "Entrada por lotes integrada ao mesmo pipeline",
      "Histórico de validação persistido em DynamoDB",
      "Notificações de erro via SNS",
      "Reaproveitamento do fluxo principal de pedidos"
    ],
    architectureNotes: [
      "O bucket S3 vira a porta de entrada de pedidos em lote.",
      "A fila Standard desacopla o início do processamento do arquivo.",
      "Os pedidos válidos seguem para a mesma fila FIFO principal já usada pela API."
    ],
    deliverables: [
      "Bucket para arquivos JSON de pedidos",
      "Lambda de extração e validação do arquivo",
      "Tabela de histórico de arquivos",
      "Canal SNS para tratamento de erros"
    ]
  },
  "dia-3": {
    number: "Dia 3",
    title: "Processamento de Pedidos",
    slug: "dia-3.html",
    readme: `${readmeBaseUrl}/dia-3-processamento-pedidos/README.md`,
    image: "assets/arquitetura-dia-3.png",
    duration: "Camada central de negócio",
    intro:
      "O terceiro laboratório concentra a lógica central do pedido: eventos validados são roteados para uma fila de processamento e persistidos em uma tabela principal no DynamoDB.",
    summary:
      "A partir daqui, a arquitetura deixa de apenas receber e validar dados e passa a manter o estado principal do pedido. É o núcleo do processamento da solução.",
    focus: [
      "Regra do EventBridge consumindo eventos NovoPedidoValidado",
      "Fila de pedidos pendentes desacoplando a etapa de processamento central",
      "Persistência do estado final do pedido no Amazon DynamoDB"
    ],
    services: ["Amazon EventBridge", "Amazon SQS", "AWS Lambda", "Amazon DynamoDB"],
    concepts: ["Roteamento", "Processamento central", "Persistência", "Estado final"],
    steps: [
      "Criar a role de processamento com permissões para SQS e DynamoDB.",
      "Provisionar a fila principal de pedidos pendentes com sua DLQ.",
      "Configurar a Lambda que recebe o detail do evento e grava o pedido processado.",
      "Criar a regra do EventBridge e conectar a fila como destino."
    ],
    outcomes: [
      "Processamento principal desacoplado",
      "Persistência do status do pedido",
      "Fluxo compatível com entradas vindas da API e do S3",
      "Base pronta para fluxos adicionais"
    ],
    architectureNotes: [
      "O EventBridge passa a distribuir os pedidos validados para o fluxo central.",
      "A fila de pendentes absorve picos e protege a lógica principal.",
      "O DynamoDB se torna a fonte de verdade do estado do pedido."
    ],
    deliverables: [
      "Fila principal de pedidos pendentes",
      "Lambda de processamento central",
      "Tabela principal de pedidos",
      "Regra do EventBridge apontando para a fila"
    ]
  },
  "dia-4": {
    number: "Dia 4",
    title: "Fluxos e DLQ",
    slug: "dia-4.html",
    readme: `${readmeBaseUrl}/dia-4-fluxos-dlq/README.md`,
    image: "assets/arquitetura-dia-4.png",
    duration: "Ciclo de vida e resiliência",
    intro:
      "A etapa final expande a arquitetura para cancelamento, alteração de pedidos e teste prático de DLQs, consolidando uma solução orientada a eventos mais completa.",
    summary:
      "O quarto dia traz o realismo operacional do sistema: novas regras de negócio, filas específicas por fluxo e observação prática do comportamento das dead-letter queues.",
    focus: [
      "Novos eventos de negócio para CancelarPedido e AlterarPedido",
      "Filas e Lambdas especializadas para cada operação",
      "Teste intencional de falhas para observar o comportamento das DLQs"
    ],
    services: ["Amazon EventBridge", "Amazon SQS Standard", "AWS Lambda", "Amazon DynamoDB", "DLQ"],
    concepts: ["Cancelamento", "Alteração", "DLQ", "Resiliência operacional"],
    steps: [
      "Criar as filas de cancelamento e alteração, cada uma com sua própria DLQ.",
      "Implementar as Lambdas para atualização do pedido no DynamoDB.",
      "Criar regras do EventBridge separadas por tipo de evento.",
      "Forçar um erro controlado para validar o redrive e a chegada na DLQ."
    ],
    outcomes: [
      "Fluxos adicionais de ciclo de vida do pedido",
      "Tratamento de falhas mais resiliente",
      "Arquitetura final consolidada",
      "Compreensão prática do papel das DLQs"
    ],
    architectureNotes: [
      "Cada operação passa a ter seu próprio caminho, com regras e filas dedicadas.",
      "As DLQs deixam de ser só conceito e entram em teste prático dentro do laboratório.",
      "A arquitetura final fecha a semana com uma visão mais próxima de um sistema real."
    ],
    deliverables: [
      "Fluxo de cancelamento",
      "Fluxo de alteração",
      "Regras específicas no EventBridge",
      "Validação prática de DLQ e redrive"
    ]
  }
};

const globalResources = [
  {
    title: "README da trilha",
    text: "Resumo geral da Semana do Desenvolvedor EDN e visão consolidada da arquitetura.",
    href: `${readmeBaseUrl}/README.md`
  },
  {
    title: "Arquitetura completa",
    text: "Imagem central do sistema integrado ao longo dos quatro dias do laboratório.",
    href: "assets/arquitetura-completa.png"
  },
  {
    title: "Documentação original",
    text: "Cada dia continua com seu README completo, evidências visuais e passo a passo detalhado.",
    href: `${readmeBaseUrl}/dia-1-api-eventbridge/README.md`
  },
  {
    title: "Repositório no GitHub",
    text: "Código, documentação e frontend publicados no repositório independente da trilha.",
    href: repoUrl
  }
];

function navMarkup(current) {
  const items = [
    ["index.html", "Início", "home"],
    ["dia-1.html", "Dia 1", "dia-1"],
    ["dia-2.html", "Dia 2", "dia-2"],
    ["dia-3.html", "Dia 3", "dia-3"],
    ["dia-4.html", "Dia 4", "dia-4"]
  ];

  return `
    <header class="site-header">
      <div class="header-inner">
        <a class="brand" href="index.html" aria-label="Semana do Desenvolvedor EDN">
          <span class="brand-mark">
            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
              <path d="M7 18a4 4 0 0 1 0-8 5.5 5.5 0 0 1 10.5-1.5A4.5 4.5 0 1 1 18 18H7z"></path>
            </svg>
          </span>
          <span class="brand-text">
            <strong>Semana do Desenvolvedor EDN</strong>
            <span>Arquitetura orientada a eventos na AWS</span>
          </span>
        </a>
        <button class="menu-toggle" type="button" aria-expanded="false" aria-controls="site-nav-list" aria-label="Abrir navegação">
          <span></span>
          <span></span>
          <span></span>
        </button>
        <nav class="site-nav" id="site-nav-list" aria-label="Navegação principal">
          ${items
            .map(
              ([href, label, id]) =>
                `<a href="${href}" ${current === id ? 'aria-current="page"' : ""}>${label}</a>`
            )
            .join("")}
        </nav>
        <div class="header-actions">
          <a class="ghost-button" href="${readmeBaseUrl}/README.md" target="_blank" rel="noreferrer">Ver README</a>
          <a class="button" href="${repoUrl}" target="_blank" rel="noreferrer">Ver no GitHub</a>
        </div>
      </div>
    </header>
  `;
}

function footerMarkup() {
  return `
    <footer class="footer">
      <div class="footer-inner">
        <div class="footer-panel">
          <div class="footer-grid">
            <div>
              <div class="brand">
                <span class="brand-mark">
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round">
                    <path d="M7 18a4 4 0 0 1 0-8 5.5 5.5 0 0 1 10.5-1.5A4.5 4.5 0 1 1 18 18H7z"></path>
                  </svg>
                </span>
                <span class="brand-text">
                  <strong>Semana do Desenvolvedor EDN</strong>
                  <span>Trilha prática da Escola da Nuvem</span>
                </span>
              </div>
              <p>Conteúdo prático, arquiteturas reais e documentação organizada para estudar a evolução do projeto ao longo dos quatro dias.</p>
            </div>
            <div>
              <h4>Navegação</h4>
              <ul>
                <li><a href="index.html">Página inicial</a></li>
                <li><a href="dia-1.html">Dia 1</a></li>
                <li><a href="dia-2.html">Dia 2</a></li>
                <li><a href="dia-3.html">Dia 3</a></li>
                <li><a href="dia-4.html">Dia 4</a></li>
              </ul>
            </div>
            <div>
              <h4>Recursos</h4>
              <ul>
                <li><a href="${readmeBaseUrl}/README.md" target="_blank" rel="noreferrer">README da trilha</a></li>
                <li><a href="assets/arquitetura-completa.png">Arquitetura completa</a></li>
                <li><a href="${repoUrl}" target="_blank" rel="noreferrer">Repositório no GitHub</a></li>
              </ul>
            </div>
            <div>
              <h4>Sobre</h4>
              <p>Frontend documental pensado para apresentar a jornada da Semana do Desenvolvedor EDN com foco visual, navegação clara e reutilização do material já produzido.</p>
            </div>
          </div>
          <div class="footer-bottom">
            <span>Escola da Nuvem • Semana do Desenvolvedor EDN</span>
            <span>Feito para documentar os 4 dias da trilha AWS Developer</span>
          </div>
        </div>
      </div>
    </footer>
  `;
}

function iconMarkup(symbol) {
  const icons = {
    calendar:
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><rect x="3" y="5" width="18" height="16" rx="2"></rect><path d="M16 3v4M8 3v4M3 10h18"></path></svg>',
    cloud:
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M7 18a4 4 0 0 1 0-8 5.5 5.5 0 0 1 10.5-1.5A4.5 4.5 0 1 1 18 18H7z"></path></svg>',
    shield:
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3l7 3v5c0 5-3 8-7 10-4-2-7-5-7-10V6l7-3z"></path></svg>',
    chart:
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M4 20V10M10 20V4M16 20v-6M22 20H2"></path></svg>',
    layers:
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><path d="M12 3l9 5-9 5-9-5 9-5z"></path><path d="M3 12l9 5 9-5"></path><path d="M3 16l9 5 9-5"></path></svg>',
    route:
      '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"><circle cx="6" cy="19" r="2"></circle><circle cx="18" cy="5" r="2"></circle><path d="M8 19h4a4 4 0 0 0 4-4V7"></path><path d="M16 9V5h4"></path></svg>'
  };

  return icons[symbol] || icons.cloud;
}

function renderChrome(current) {
  document.body.insertAdjacentHTML("afterbegin", navMarkup(current));
  document.body.insertAdjacentHTML("beforeend", footerMarkup());
  bindInteractiveChrome();
}

function cardIcon(index) {
  return ["calendar", "cloud", "shield", "chart", "layers", "route"][index % 6];
}

function renderHome() {
  renderChrome("home");
  const root = document.querySelector("#app");

  root.innerHTML = `
    <main class="site-shell">
      <section class="hero">
        <div class="section-inner hero-grid">
          <div class="hero-copy">
            <div class="eyeline">Trilha prática • 4 dias intensivos</div>
            <h1>Semana do <span>Desenvolvedor EDN</span></h1>
            <p>Quatro laboratórios conectados para construir, na prática, uma arquitetura orientada a eventos na AWS, cobrindo ingestão, rastreamento, processamento, persistência e tratamento de falhas.</p>
            <div class="hero-metrics">
              <article class="metric-card">
                <span class="icon-chip">${iconMarkup("calendar")}</span>
                <strong>4 dias</strong>
                <span>Uma jornada prática, progressiva e integrada.</span>
              </article>
              <article class="metric-card">
                <span class="icon-chip">${iconMarkup("cloud")}</span>
                <strong>AWS na prática</strong>
                <span>Serviços reais conectados em um fluxo único.</span>
              </article>
              <article class="metric-card">
                <span class="icon-chip">${iconMarkup("shield")}</span>
                <strong>Resiliência</strong>
                <span>DLQs, filas e desacoplamento orientado a eventos.</span>
              </article>
              <article class="metric-card">
                <span class="icon-chip">${iconMarkup("chart")}</span>
                <strong>Escalabilidade</strong>
                <span>Entrada múltipla e processamento assíncrono.</span>
              </article>
            </div>
            <div class="hero-actions">
              <a class="button" href="#trilha">Explorar trilha</a>
              <a class="ghost-button" href="#arquitetura">Ver arquitetura</a>
              <a class="ghost-button" href="${repoUrl}" target="_blank" rel="noreferrer">Abrir GitHub</a>
            </div>
          </div>
          <div class="hero-visual">
            <div class="hero-orbit">
              <div class="cloud-badge">
                <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round">
                  <path d="M7 18a4 4 0 0 1 0-8 5.5 5.5 0 0 1 10.5-1.5A4.5 4.5 0 1 1 18 18H7z"></path>
                </svg>
              </div>
              <div class="node node--pink node-a"><small>API</small></div>
              <div class="node node--orange node-b"><small>Lambda</small></div>
              <div class="node node--violet node-c"><small>Event</small></div>
              <div class="node node--green node-d"><small>S3</small></div>
              <div class="node node--pink node-e"><small>DB</small></div>
            </div>
          </div>
        </div>
      </section>

      <section class="section" id="arquitetura">
        <div class="section-inner">
          <div class="section-title">
            <div>
              <div class="eyeline">Visão consolidada do sistema</div>
              <h2>Arquitetura completa da semana</h2>
            </div>
            <p>Ao final da trilha, a solução reúne API REST, ingestão por S3, processamento assíncrono com SQS, roteamento via EventBridge, notificações com SNS e persistência em DynamoDB.</p>
          </div>
          <div class="architecture-frame">
            <img src="assets/arquitetura-completa.png" alt="Arquitetura completa da Semana do Desenvolvedor EDN">
            <div class="architecture-actions">
              <span>Fluxo unificado construído ao longo dos quatro dias do laboratório.</span>
              <a class="link-arrow" href="${readmeBaseUrl}/README.md" target="_blank" rel="noreferrer">Ler documentação principal →</a>
            </div>
          </div>
        </div>
      </section>

      <section class="section" id="trilha">
        <div class="section-inner">
          <div class="section-title">
            <div>
              <div class="eyeline">Roteiro diário</div>
              <h2>Nossa trilha de 4 dias</h2>
            </div>
            <p>Cada etapa acrescenta uma nova capacidade à arquitetura, sem criar fluxos paralelos desconectados. O resultado é uma visão progressiva, coerente e reutilizável.</p>
          </div>
          <div class="timeline">
            ${orderedDays
              .map((key, index) => {
                const item = dayData[key];
                return `
                  <article class="timeline-card" data-day="${index + 1}">
                    <span class="icon-chip">${iconMarkup(cardIcon(index))}</span>
                    <h3>${item.number}<br>${item.title}</h3>
                    <p>${item.summary}</p>
                    <ul>
                      ${item.concepts.map((concept) => `<li>${concept}</li>`).join("")}
                    </ul>
                    <a class="link-arrow" href="${item.slug}">Ver detalhes →</a>
                  </article>
                `;
              })
              .join("")}
          </div>
        </div>
      </section>

      <section class="section">
        <div class="section-inner">
          <div class="section-title">
            <div>
              <div class="eyeline">Leituras da jornada</div>
              <h2>Como a arquitetura evolui</h2>
            </div>
            <p>A trilha foi pensada para crescer em camadas: primeiro a entrada, depois a integração de lotes, em seguida o processamento central e, por fim, os fluxos adicionais com reforço de resiliência.</p>
          </div>
          <div class="readme-grid">
            ${orderedDays
              .map((key, index) => {
                const item = dayData[key];
                return `
                  <article class="readme-card">
                    <span class="icon-chip">${iconMarkup(cardIcon(index + 1))}</span>
                    <h3>${item.number}</h3>
                    <p>${item.duration}</p>
                    <a class="link-arrow" href="${item.slug}">Abrir página do dia →</a>
                  </article>
                `;
              })
              .join("")}
          </div>
        </div>
      </section>

      <section class="section">
        <div class="section-inner">
          <div class="section-title">
            <div>
              <div class="eyeline">Ecossistema da solução</div>
              <h2>Serviços AWS utilizados</h2>
            </div>
            <p>Os laboratórios usam serviços integrados para ingestão, persistência, notificação e orquestração de eventos, sempre com foco em desacoplamento e observabilidade.</p>
          </div>
          <div class="services-grid">
            ${[
              ["API Gateway", "Exposição de endpoint REST para entrada de pedidos."],
              ["AWS Lambda", "Lógica de validação, processamento e integração entre etapas."],
              ["Amazon SQS", "Filas FIFO e Standard para desacoplamento e resiliência."],
              ["Amazon EventBridge", "Barramento e regras para roteamento orientado a eventos."],
              ["Amazon S3", "Recebimento de arquivos JSON com pedidos em lote."],
              ["Amazon DynamoDB", "Persistência dos pedidos e histórico de validação."],
              ["Amazon SNS", "Alertas e notificações de erro para o fluxo de arquivos."],
              ["CloudWatch", "Logs, monitoramento e apoio à validação do laboratório."]
            ]
              .map(
                ([title, text], index) => `
                  <article class="service-card">
                    <span class="icon-chip">${iconMarkup(cardIcon(index + 2))}</span>
                    <h3>${title}</h3>
                    <p>${text}</p>
                  </article>
                `
              )
              .join("")}
          </div>
        </div>
      </section>

      <section class="section">
        <div class="section-inner">
          <div class="section-title">
            <div>
              <div class="eyeline">Acesso rápido</div>
              <h2>Recursos da trilha</h2>
            </div>
            <p>Se você quiser aprofundar, pode navegar pelos READMEs originais, consultar a arquitetura consolidada ou abrir a documentação principal da trilha.</p>
          </div>
          <div class="resources-grid">
            ${globalResources
              .map(
                (item, index) => `
                  <article class="resource-card">
                    <span class="icon-chip">${iconMarkup(cardIcon(index + 3))}</span>
                    <h3>${item.title}</h3>
                    <p>${item.text}</p>
                    <a class="link-arrow" href="${item.href}" ${item.href.startsWith("http") ? 'target="_blank" rel="noreferrer"' : ""}>Abrir recurso →</a>
                  </article>
                `
              )
              .join("")}
          </div>
        </div>
      </section>
    </main>
  `;
}

function dayPager(key) {
  const index = orderedDays.indexOf(key);
  const prevKey = orderedDays[index - 1];
  const nextKey = orderedDays[index + 1];

  return `
    <div class="pager">
      ${
        prevKey
          ? `<a class="pager-card" href="${dayData[prevKey].slug}">
              <span>← Etapa anterior</span>
              <strong>${dayData[prevKey].number} • ${dayData[prevKey].title}</strong>
            </a>`
          : `<div class="pager-card is-muted">
              <span>Início da trilha</span>
              <strong>${dayData[key].number} abre a arquitetura</strong>
            </div>`
      }
      ${
        nextKey
          ? `<a class="pager-card" href="${dayData[nextKey].slug}">
              <span>Próxima etapa →</span>
              <strong>${dayData[nextKey].number} • ${dayData[nextKey].title}</strong>
            </a>`
          : `<div class="pager-card is-muted">
              <span>Fechamento da trilha</span>
              <strong>${dayData[key].number} consolida a solução</strong>
            </div>`
      }
    </div>
  `;
}

function renderDayPage(key) {
  const item = dayData[key];
  renderChrome(key);
  const root = document.querySelector("#app");

  root.innerHTML = `
    <main class="site-shell">
      <section class="page-hero">
        <div class="section-inner page-hero-grid">
          <div class="copy-block">
            <div class="eyeline">${item.number} • Semana do Desenvolvedor EDN</div>
            <h1>${item.number}<br><span>${item.title}</span></h1>
            <p>${item.intro}</p>
            <div class="tag-row">
              ${item.services.map((service) => `<span class="tag">${service}</span>`).join("")}
            </div>
            <div class="facts-grid">
              <article class="fact-card">
                <h3>Foco técnico</h3>
                <p>${item.focus[0]}</p>
              </article>
              <article class="fact-card">
                <h3>Integração</h3>
                <p>${item.focus[1]}</p>
              </article>
              <article class="fact-card">
                <h3>Entrega do dia</h3>
                <p>${item.focus[2]}</p>
              </article>
            </div>
            <div class="hero-actions">
              <a class="button" href="${item.readme}">Abrir README</a>
              <a class="ghost-button" href="index.html">Voltar à trilha</a>
              <a class="ghost-button" href="${repoUrl}" target="_blank" rel="noreferrer">Voltar ao GitHub</a>
            </div>
          </div>
          <div class="page-visual">
            <img src="${item.image}" alt="Arquitetura do ${item.number}">
          </div>
        </div>
      </section>

      <section class="section">
        <div class="section-inner detail-layout">
          <div class="detail-stack">
            <article class="mini-panel" id="visao-geral">
              <h2>Visão geral da etapa</h2>
              <p>${item.summary}</p>
              <div class="tag-row" style="margin-top:18px;">
                ${item.concepts.map((concept) => `<span class="tag">${concept}</span>`).join("")}
              </div>
            </article>

            <article class="mini-panel" id="arquitetura-dia">
              <h2>Arquitetura da etapa</h2>
              <p>Cada dia adiciona uma camada concreta à solução. Aqui, a arquitetura destaca os componentes centrais da etapa e o papel de cada serviço dentro do fluxo maior.</p>
              <img src="${item.image}" alt="Diagrama de arquitetura do ${item.number}" style="margin-top:18px;border-radius:18px;border:1px solid rgba(255,255,255,0.08);">
              <ul class="detail-list" style="margin-top:20px;">
                ${item.architectureNotes.map((note) => `<li>${note}</li>`).join("")}
              </ul>
            </article>

            <article class="mini-panel" id="objetivos">
              <h2>Objetivos do laboratório</h2>
              <ul class="detail-list">
                ${item.focus.map((entry) => `<li>${entry}</li>`).join("")}
              </ul>
            </article>

            <article class="mini-panel" id="fluxo">
              <h2>Fluxo principal da implementação</h2>
              <div class="steps-grid">
                ${item.steps
                  .map(
                    (step, index) => `
                      <article class="step-card">
                        <strong>${index + 1}</strong>
                        <p>${step}</p>
                      </article>
                    `
                  )
                  .join("")}
              </div>
            </article>

            <article class="mini-panel" id="resultado">
              <h2>O que esse dia entrega</h2>
              <div class="readme-grid">
                ${item.outcomes
                  .map(
                    (outcome, index) => `
                      <article class="readme-card">
                        <span class="icon-chip">${iconMarkup(cardIcon(index + 1))}</span>
                        <h3>${outcome}</h3>
                        <p>Capacidade consolidada dentro da arquitetura evolutiva da trilha.</p>
                      </article>
                    `
                  )
                  .join("")}
              </div>
            </article>

            <article class="mini-panel" id="entregaveis">
              <h2>Entregáveis da etapa</h2>
              <ul class="detail-list">
                ${item.deliverables.map((entry) => `<li>${entry}</li>`).join("")}
              </ul>
            </article>

            ${dayPager(key)}
          </div>

          <aside class="sticky-panel">
            <article class="mini-panel">
              <h3>Navegação da página</h3>
              <ul class="toc">
                <li><a href="#visao-geral">Visão geral</a></li>
                <li><a href="#arquitetura-dia">Arquitetura da etapa</a></li>
                <li><a href="#objetivos">Objetivos do laboratório</a></li>
                <li><a href="#fluxo">Fluxo principal</a></li>
                <li><a href="#resultado">Resultado do dia</a></li>
                <li><a href="#entregaveis">Entregáveis</a></li>
              </ul>
              <div class="divider"></div>
              <h3 style="margin-top:22px;">Documentação original</h3>
              <p>O README completo continua sendo a referência detalhada do passo a passo, dos recursos criados e das evidências visuais.</p>
              <a class="link-arrow" href="${item.readme}">Abrir README →</a>
              <a class="link-arrow" href="${repoUrl}" target="_blank" rel="noreferrer">Voltar ao GitHub →</a>
            </article>
          </aside>
        </div>
      </section>
    </main>
  `;
}

const page = document.body.dataset.page;

if (page === "home") {
  renderHome();
} else if (dayData[page]) {
  renderDayPage(page);
}

function bindInteractiveChrome() {
  const toggle = document.querySelector(".menu-toggle");
  const nav = document.querySelector(".site-nav");

  if (toggle && nav) {
    toggle.addEventListener("click", () => {
      const isOpen = toggle.getAttribute("aria-expanded") === "true";
      toggle.setAttribute("aria-expanded", String(!isOpen));
      document.body.classList.toggle("nav-open", !isOpen);
    });

    nav.querySelectorAll("a").forEach((link) => {
      link.addEventListener("click", () => {
        toggle.setAttribute("aria-expanded", "false");
        document.body.classList.remove("nav-open");
      });
    });
  }

  document.querySelectorAll(".timeline-card, .resource-card, .readme-card, .pager-card").forEach((card) => {
    const primaryLink = card.querySelector("a[href]");
    if (!primaryLink) return;

    card.classList.add("card-link");
    card.setAttribute("tabindex", "0");
    card.setAttribute("role", "link");

    card.addEventListener("click", (event) => {
      if (event.target.closest("a")) return;
      primaryLink.click();
    });

    card.addEventListener("keydown", (event) => {
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        primaryLink.click();
      }
    });
  });
}
