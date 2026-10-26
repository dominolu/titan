/* Presentation prototype. All fixtures below are fictional; there are no network or trading calls. */
const paths = {
  overview: '<rect x="3" y="3" width="7" height="7" rx="1.5"/><rect x="14" y="3" width="7" height="7" rx="1.5"/><rect x="3" y="14" width="7" height="7" rx="1.5"/><rect x="14" y="14" width="7" height="7" rx="1.5"/>',
  strategy: '<path d="m12 3 9 5-9 5-9-5 9-5Zm-9 9 9 5 9-5M3 16l9 5 9-5"/>',
  box: '<path d="m12 3 9 5v9l-9 5-9-5V8l9-5Zm0 10v9M3 8l9 5 9-5M7 5.8l9 5"/>',
  chart: '<path d="M4 3v17h17M7 14l4-5 4 3 5-7"/>',
  shield: '<path d="m12 3 8 3v6c0 5-8 9-8 9s-8-4-8-9V6l8-3Z"/><path d="m8 12 3 3 5-6"/>',
  arrow: '<path d="M5 12h14m-5-5 5 5-5 5"/>',
  back: '<path d="M19 12H5m5-5-5 5 5 5"/>',
  chevron: '<path d="m9 6 6 6-6 6"/>',
  down: '<path d="m6 9 6 6 6-6"/>',
  bell: '<path d="M18 8a6 6 0 0 0-12 0c0 7-3 7-3 9h18c0-2-3-2-3-9M10 21h4"/>',
  info: '<circle cx="12" cy="12" r="9"/><path d="M12 11v6m0-10v.1"/>',
  search: '<circle cx="10.5" cy="10.5" r="6.5"/><path d="m16 16 5 5"/>',
  clock: '<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>',
  check: '<circle cx="12" cy="12" r="9"/><path d="m8 12 3 3 5-6"/>',
  pause: '<path d="M8 5v14M16 5v14"/>',
  stop: '<rect x="5" y="5" width="14" height="14" rx="2"/>',
  play: '<path d="m8 4 12 8-12 8V4Z"/>',
  close: '<path d="m6 6 12 12M18 6 6 18"/>',
  more: '<circle cx="5" cy="12" r="1"/><circle cx="12" cy="12" r="1"/><circle cx="19" cy="12" r="1"/>',
  alert: '<path d="m12 3 10 17H2L12 3Z"/><path d="M12 9v5m0 3v.1"/>',
  server: '<rect x="3" y="3" width="18" height="7" rx="2"/><rect x="3" y="14" width="18" height="7" rx="2"/><path d="M7 6.5h.1M7 17.5h.1M11 6.5h6M11 17.5h6"/>',
  wallet: '<path d="M20 8V5a2 2 0 0 0-2-2H5a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h15V8H5a3 3 0 0 1 0-5"/><path d="M20 11h-5v6h5M17 14h.1"/>',
  orders: '<path d="M7 3h10v3H7zM5 5H3v16h18V5h-2M7 11h10M7 15h10"/>',
  sync: '<path d="M20 8A8 8 0 0 0 6 5L3 8m0-5v5h5M4 16a8 8 0 0 0 14 3l3-3m0 5v-5h-5"/>',
};
const icon = name => `<svg class="icon" viewBox="0 0 24 24" aria-hidden="true">${paths[name] || paths.info}</svg>`;
const esc = value => String(value).replace(/[&<>"']/g, char => ({'&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'}[char]));
const app = document.getElementById('app');
const overlays = document.getElementById('overlay-root');
const ui = {
  scenario: new URLSearchParams(location.search).get('state') || 'normal',
  tab: 'summary', period: '24h', filter: 'all', query: '', orderTab: 'orders',
  receipt: null, readOnly: false,
};
if (!['normal', 'stale', 'recovery'].includes(ui.scenario)) ui.scenario = 'normal';
const isDetail = () => location.hash.startsWith('#/strategy');
const isUncertain = () => ui.scenario !== 'normal';
const effectiveStatus = run => run.id === 'btc' && isUncertain() ? 'recovery' : run.status;
// Fixture capabilities only. Production must use the engine's allowed_actions.
const canControl = kind => kind === 'pause'
  ? ui.scenario === 'normal' && !ui.receipt
  : ui.scenario !== 'recovery' && ui.receipt?.kind !== 'stop';
const runData = [
  {id: 'btc', name: 'BTC 稳健网格', symbol: 'BTC / USDT', native: 'GATE_FUTURE_BTC_USDT', code: 'grid-btc-01', coin: '₿', status: 'running', quantity: '0.00060', exposure: '64.80', gain: '+1.28', type: 'NET · 线性合约'},
  {id: 'eth', name: 'ETH 区间网格', symbol: 'ETH / USDT', native: 'GATE_FUTURE_ETH_USDT', code: 'grid-eth-02', coin: '◆', status: 'paused', quantity: '0.00000', exposure: '0.00', gain: '+0.42', type: 'NET · 线性合约'},
  {id: 'sol', name: 'SOL 低频网格', symbol: 'SOL / USDT', native: 'GATE_FUTURE_SOL_USDT', code: 'grid-sol-03', coin: '≋', status: 'recovery', quantity: '—', exposure: '—', gain: '—', type: 'NET · 线性合约'},
];
const orders = [
  {side: 'buy', price: '80,900.0', quantity: '0.00010', id: '…8f21', reserve: '新增开仓'},
  {side: 'buy', price: '80,800.0', quantity: '0.00010', id: '…8f22', reserve: '新增开仓'},
  {side: 'sell', price: '81,100.0', quantity: '0.00010', id: '…8f23', reserve: '只减仓'},
  {side: 'sell', price: '81,200.0', quantity: '0.00010', id: '…8f24', reserve: '只减仓'},
];
const events = [
  ['10:42:18', '账户快照已同步', '订单、独立成交与持仓投影一致；当前运行代次 2。'],
  ['10:41:56', '一轮网格配对完成', '买入与卖出成交已确认，配对毛收益 +0.16 USDT；未计费用与资金费。'],
  ['10:40:12', '限价委托已确认', '买入 0.00010 BTC @ 80,900.0 USDT，交易所已确认挂单。'],
  ['10:38:06', 'Checkpoint 已保存', '运行代次 2，策略业务状态已持久化，恢复仍需平台核验。'],
  ['08:30:04', '恢复校验通过', '制品、绑定和账户证据已核对；引擎确认交易入口开放。'],
];
function statusBadge(kind, label) {
  const labels = {running: '运行中', paused: '已暂停', recovery: '待核对', stale: '观测过期'};
  const style = kind === 'running' ? '' : kind === 'paused' ? 'paused' : 'alert';
  return `<span class="status ${style}"><span class="dot"></span>${label || labels[kind]}</span>`;
}
function primaryStatus() {
  if (ui.receipt) return statusBadge('paused', '最后确认运行中') + ' ' + statusBadge('recovery', '操作待确认');
  if (ui.scenario === 'stale') return statusBadge('stale', '最后确认运行中');
  if (ui.scenario === 'recovery') return statusBadge('recovery', '恢复待核对');
  return statusBadge('running');
}
function liveChip() {
  return isUncertain()
    ? `<span class="live-chip warning"><span class="dot"></span>${ui.scenario === 'stale' ? '观测过期' : '恢复待核对'}</span>`
    : '<span class="live-chip"><span class="dot"></span>快照已同步</span>';
}
function button(label, action, style = '', glyph = '', disabled = false) {
  return `<button type="button" class="button ${style}" data-action="${action}" ${disabled ? 'disabled' : ''}>${glyph ? icon(glyph) : ''}${label}</button>`;
}
function metric(label, value, unit, note, extra = '', glyph = 'info') {
  return `<article class="metric ${extra}"><div class="metric-label"><span>${label}</span>${icon(glyph)}</div><div class="metric-value num">${value}<span class="unit">${unit}</span></div><div class="metric-note">${note}</div></article>`;
}
function pageFooter() {
  return '<footer class="page-footer"><span>示例截面 2026-10-26 10:42:18 · UTC+8</span><span>Titan 管理工作台 / 视觉提案 01</span></footer>';
}
function shell(content) {
  const detail = isDetail();
  return `<div class="prototype-bar">
    <div class="prototype-label"><strong>DESIGN PREVIEW / 01</strong><span>示例数据 · 未连接实盘</span></div>
    <div class="scenario"><span>状态预览</span><select id="scenario" aria-label="状态预览"><option value="normal" ${ui.scenario === 'normal' ? 'selected' : ''}>正常观测</option><option value="stale" ${ui.scenario === 'stale' ? 'selected' : ''}>观测过期</option><option value="recovery" ${ui.scenario === 'recovery' ? 'selected' : ''}>恢复待核对</option></select><button class="prototype-reset" data-action="reset">重置</button></div>
  </div>
  <aside class="sidebar" aria-label="主要导航">
    <a class="brand" href="#/overview" aria-label="Titan 运行总览"><svg class="brand-mark" viewBox="0 0 32 36" aria-hidden="true"><path d="M2 5 17 0l13 5-13 5Z" fill="#b9d9cd"/><path d="M13 12 21 9v22l-8 5Z" fill="#73a891"/><path d="M2 8v6l8 3v-6Z" fill="#4c6d60"/></svg><span class="brand-word">TITAN<span class="brand-sub">EXECUTION CONSOLE</span></span></a>
    <div class="workspace"><span>${icon('strategy')}</span><div>Titan 工作空间<span class="muted">管理工作台 · 设计预览</span></div></div>
    <p class="nav-label">工作台</p>
    <nav><a class="nav-item ${detail ? '' : 'active'}" href="#/overview" ${detail ? '' : 'aria-current="page"'}>${icon('overview')}<span>运行总览</span></a>
      <button class="nav-item" disabled title="待后续页面实施">${icon('strategy')}<span>策略与参数</span></button>
      <button class="nav-item" disabled title="待后续页面实施">${icon('box')}<span>构建制品</span></button>
      <button class="nav-item" disabled title="待后续页面实施">${icon('chart')}<span>历史回测</span></button>
      <button class="nav-item" disabled title="待后续页面实施">${icon('shield')}<span>账户风险</span></button>
    </nav>
    <div class="recent-nav"><p class="nav-label">最近查看</p><a href="#/strategy/grid-btc-01" class="recent-run ${detail ? 'active' : ''}" ${detail ? 'aria-current="page"' : ''}><span class="dot positive"></span>BTC 稳健网格</a></div>
    <div class="sidebar-bottom"><strong><span class="dot"></span>节点可达 · 示例</strong><p>live-01 / Gate CrossEx</p><div class="foot"><span>TITAN ENGINE</span><span>v0.1</span></div></div>
  </aside>
  <div class="shell"><header class="topbar"><div class="breadcrumb"><a href="#/overview">工作空间</a>${icon('chevron')}<a href="#/overview" class="${detail ? '' : 'current'}">运行总览</a>${detail ? `${icon('chevron')}<span class="current">策略详情</span>` : ''}</div><div class="topbar-right"><time>2026.10.26 <span class="small">UTC+8</span></time><button class="icon-button" data-action="events-drawer" aria-label="查看待处理事项">${icon('bell')}<span class="notification-dot"></span></button><div class="profile"><span class="avatar">DL</span><span class="profile-name">操作员</span></div></div></header><main id="main" tabindex="-1">${content}</main></div>`;
}
let chartSequence = 0;
const chartSets = {
  '24h': {low: 1020, high: 1026, ticks: ['10:00', '14:00', '18:00', '22:00', '02:00', '06:00', '10:42'], label: '近 24 小时', start: '昨日 10:42', values: [1022.22,1022.15,1022.45,1022.32,1022.61,1022.49,1022.77,1023.05,1022.92,1023.21,1023.09,1023.28,1023.16,1023.51,1023.38,1023.7,1023.6,1023.94,1023.68,1023.2,1022.55,1023.01,1023.28,1023.75,1023.52,1023.86,1023.74,1024.08,1023.95,1024.21,1024.07,1024.39,1024.2,1024.49,1024.36,1024.68]},
  '1h': {low: 1023, high: 1025, ticks: ['09:42', '09:52', '10:02', '10:12', '10:22', '10:32', '10:42'], label: '近 1 小时', start: '09:42', values: [1023.94,1024.08,1024.02,1024.15,1024.1,1024.24,1024.35,1024.27,1024.2,1024.4,1024.31,1024.48,1024.38,1024.57,1024.47,1024.68]},
  '7d': {low: 995, high: 1030, ticks: ['10/20', '10/21', '10/22', '10/23', '10/24', '10/25', '10/26'], label: '近 7 天', start: '10/20 10:42', values: [1000,1002.11,1001.82,1005.3,1007.24,1006.82,1009.16,1007.88,1006.55,1010.46,1011.53,1013.2,1012.92,1015.61,1013.77,1016.84,1017.91,1018.12,1020.66,1019.71,1021.48,1022.32,1021.11,1024.68]},
};
function equityChart(detail = false) {
  const config = chartSets[ui.period];
  const id = `equity-${++chartSequence}`;
  const narrow = window.innerWidth <= 760;
  const width = narrow ? Math.max(260, window.innerWidth - 60) : 724;
  const x0 = 10, x1 = width - (narrow ? 52 : 72), y0 = 15, y1 = 164;
  const points = config.values.map((value, i) => [x0 + (i / (config.values.length - 1)) * (x1 - x0), y1 - (value - config.low) / (config.high - config.low) * (y1 - y0)]);
  const line = points.map(([x,y], i) => `${i ? 'L' : 'M'}${x.toFixed(2)},${y.toFixed(2)}`).join(' ');
  const grids = [0,1,2,3].map(i => {
    const y = y0 + (y1 - y0) * i / 3;
    return `<path class="chart-grid" d="M${x0} ${y}H${x1}"/><text class="chart-label" x="${width-3}" y="${y+3}" text-anchor="end">${(config.high - (config.high-config.low)*i/3).toFixed(2)}</text>`;
  }).join('');
  return `<article class="card"><div class="card-header"><div><h2 class="card-title">账户净权益 <span class="outline-tag">账户级</span></h2><p class="card-subtitle">gate · ID 1 / USDT 计价${isUncertain() ? ' · 最后有效记录' : ''}</p></div><div class="segments" role="group" aria-label="权益图时间范围">${['1h','24h','7d'].map(p => `<button data-action="period" data-value="${p}" aria-pressed="${ui.period === p}" class="${ui.period === p ? 'selected' : ''}">${p}</button>`).join('')}</div></div><div class="chart-stats"><strong class="num">1,024.68</strong><span class="muted small">USDT</span><span class="change">${isUncertain() ? '最后确认值' : '账户权益快照'}</span></div><div class="chart-wrap"><svg class="equity-chart" viewBox="0 0 ${width} 197" role="img" aria-label="${config.label}账户净权益示例折线图，末值 1024.68 USDT；不代表策略净收益"><defs><linearGradient id="${id}" x1="0" y1="0" x2="0" y2="1"><stop offset="0%" stop-color="#229b7c" stop-opacity=".13"/><stop offset="100%" stop-color="#229b7c" stop-opacity=".005"/></linearGradient></defs>${grids}<path d="${line}L${x1} ${y1}H${x0}Z" fill="url(#${id})"/><path d="${line}" fill="none" stroke="#248d74" stroke-width="2.2" stroke-linejoin="round" stroke-linecap="round"/><circle cx="${x1}" cy="${points.at(-1)[1]}" r="3.3" fill="#248d74" stroke="white" stroke-width="1.5"/>${config.ticks.map((label,i) => narrow && i % 3 !== 0 ? '' : `<text class="chart-label" x="${x0 + (x1-x0)*i/6}" y="188" text-anchor="${i === 0 ? 'start' : i === 6 ? 'end' : 'middle'}">${label}</text>`).join('')}<g class="chart-cursor" visibility="hidden"><path class="crosshair" stroke="#9ec0b0" stroke-dasharray="3 3"/><circle class="cursor-dot" r="3.5" fill="#248d74" stroke="white" stroke-width="2"/></g><rect x="10" y="10" width="${x1-x0}" height="160" fill="transparent" data-chart-hit="true"/></svg><div class="chart-tooltip" hidden></div></div><div class="chart-footer"><span class="legend"><i></i>净权益 / USDT</span><span>${detail ? '账户事实，不归因为策略净收益' : '来源：账户权益记录 · ' + config.label}</span></div></article>`;
}
function riskCard(compact = false) {
  const unknown = isUncertain();
  return `<article class="card ${compact ? 'risk-compact' : 'risk-card'}"><div class="risk-heading"><h2 class="card-title">${compact ? '风险与完整性' : 'BTC 风险上界'}</h2>${icon('shield')}</div><div class="risk-amount num"><strong>${unknown ? '—' : '64.80'}</strong><small>/ 100 <span class="small">USDT</span></small></div><p class="risk-caption">${unknown ? '当前不可确认 · 最后确认上界 64.80 USDT' : '含在途委托 · 使用上限的 64.8%'}</p><div class="risk-bar" ${unknown ? 'style="opacity:.28"' : ''} role="img" aria-label="${unknown ? '历史风险分解，当前不可确认' : '实际敞口 48.60，在途风险预占 16.20，上限 100 USDT'}"><i class="actual"></i><i class="pending"></i><i class="peak" title="采样观测最大值 72.90 USDT"></i></div><div class="risk-scale"><span>0</span><span>上限 100</span></div><div class="risk-breakdown"><div><span class="square"></span><span class="muted">${unknown ? '最后确认实际敞口' : '实际持仓敞口'}</span><strong class="num">48.60 <span class="muted small">USDT</span></strong></div><div><span class="square pale"></span><span class="muted">${unknown ? '最后确认在途预占' : '在途风险预占'}</span><strong class="num">16.20 <span class="muted small">USDT</span></strong></div></div>${compact ? `<div class="completeness">${icon('info')}<span>策略净收益未确认：费用归属证据不完整。<br>缺失值保留为「—」。</span></div>` : `<div class="risk-foot"><span>采样观测最大值</span><strong class="num">72.90 USDT</strong></div>`}</article>`;
}
function overview() {
  return `<section class="page-header"><div><h1>运行总览</h1><p>Gate CrossEx <span class="muted">/</span> gate · ID 1 <span class="muted">/</span> 3 个策略实例</p></div><div class="header-actions">${liveChip()}${button('操作记录', 'operations', '', 'clock')}${button('查看策略', 'open-btc', 'primary', 'arrow')}</div></section>
    ${isUncertain() ? `<div class="data-note warning">${ui.scenario === 'stale' ? '观测已过期：以下保留最后确认的账户与委托记录，当前敞口不可确认。' : 'BTC 恢复待核对：交易入口关闭，账户与订单证据齐备后由引擎确认；当前敞口不可确认。'}</div>` : ''}
    <section class="metrics" aria-label="运行摘要">${metric('账户净权益', '1,024.68', 'USDT', `${isUncertain() ? '最后有效快照' : '账户级'} · 不代表策略收益`, '', 'wallet')}${metric('已确认运行实例', isUncertain() ? '—' : '1 <span class="slash">/ 3</span>', '个', isUncertain() ? '当前状态需要重新确认' : '1 个已暂停 · 1 个待核对', '', 'strategy')}${metric(isUncertain() ? 'BTC 最后确认委托' : 'BTC 活动委托', '4', '笔', '买入 2 笔 · 卖出 2 笔', '', 'orders')}${metric('待处理事项', isUncertain() ? '2' : '1', '项', '<span class="warning-text">恢复核验需要关注</span>', 'note-amber', 'alert')}</section>
    <div class="split">${equityChart()}${riskCard()}</div>
    <section class="card" aria-label="策略实例"><div class="section-heading"><h2>策略实例 <span class="count">3</span></h2><label class="search">${icon('search')}<input id="run-search" type="search" placeholder="搜索名称或品种" aria-label="搜索策略实例" value="${esc(ui.query)}" /></label></div><div class="table-tools"><div class="filter-tabs" role="group" aria-label="实例状态筛选">${[['all','全部',3],['running','运行中',runData.filter(r => effectiveStatus(r) === 'running').length],['paused','已暂停',1],['recovery',isUncertain() ? '待确认' : '待核对',runData.filter(r => effectiveStatus(r) === 'recovery').length]].map(([id,label,n]) => `<button data-action="filter" data-value="${id}" class="${ui.filter === id ? 'selected' : ''}" aria-pressed="${ui.filter === id}">${label}<span>${n}</span></button>`).join('')}</div><span class="small muted">金额单位 USDT · 当前账户 gate</span></div><div class="table-scroll"><table><thead><tr><th>策略 / 实例</th><th>运行状态</th><th>持仓数量</th><th>风险上界 / USDT</th><th>配对毛收益 / USDT</th><th class="align-right">操作</th></tr></thead><tbody id="run-rows">${runRows()}</tbody></table></div><div class="table-footer"><span id="run-count">共 ${filteredRuns().length} 个实例 · 每个实例独立品种绑定</span><span>01 / 01</span></div></section>
    <section class="attention"><span class="attention-icon">${icon('alert')}</span><div class="attention-copy"><strong>SOL 低频网格 · 恢复校验未完成</strong><p>当前订单终态证据不足，交易入口保持关闭，敞口暂不可确认。</p></div><button class="text-button" data-action="recovery-drawer">查看原因 ${icon('arrow')}</button></section>${pageFooter()}`;
}
function filteredRuns() {
  return runData.filter(run => (ui.filter === 'all' || effectiveStatus(run) === ui.filter) && `${run.name} ${run.symbol} ${run.code}`.toLowerCase().includes(ui.query.toLowerCase()));
}
function runRows() {
  return filteredRuns().map(run => `<tr><td><div class="run-name"><span class="coin ${run.id}">${run.coin}</span><div><button class="run-title" data-action="open-run" data-value="${run.id}">${run.name}</button><span class="secondary mono">${run.code}</span></div></div></td><td>${run.id === 'btc' ? primaryStatus() : statusBadge(run.status)}</td><td class="num">${run.quantity}<span class="secondary">${run.symbol.split(' / ')[0]} · NET${isUncertain() && run.id === 'btc' ? ' · 最后确认' : ''}</span></td><td class="num">${run.id === 'btc' && isUncertain() ? '—' : run.exposure}${run.id === 'btc' && !isUncertain() ? '<span class="mini-meter" aria-hidden="true"><i style="width:64.8%"></i></span>' : ''}<span class="secondary">${run.id === 'sol' ? '待核对，不能按零处理' : run.id === 'btc' && isUncertain() ? '当前不可确认' : '含在途委托'}</span></td><td class="num ${run.gain.startsWith('+') ? 'positive' : 'muted'}">${run.gain}<span class="secondary">${run.gain === '—' ? '归属证据不完整' : '未计费用与资金费'}</span></td><td class="align-right"><button class="text-button" data-action="open-run" data-value="${run.id}">查看 ${icon('chevron')}</button></td></tr>`).join('') || '<tr><td colspan="6"><div class="empty-result">没有匹配的实例，请调整搜索或筛选。</div></td></tr>';
}
function stateLine() {
  const uncertain = isUncertain();
  let title = '运行正常', description = '交易入口已开放 · 成交与持仓已对齐';
  if (ui.scenario === 'stale') { title = '观测已过期'; description = '当前仓位与交易入口不可确认 · 保留最后有效快照'; }
  if (ui.scenario === 'recovery') { title = '恢复待核对 · 交易入口关闭'; description = '等待订单终态证据，不自动重派原操作'; }
  if (ui.receipt) { title = '操作已受理 · 等待引擎确认'; description = '页面保留最后确认的运行状态，受理不等于执行完成'; }
  return `<div class="state-line ${uncertain || ui.receipt ? 'warning' : ''}"><div><strong>${icon(uncertain || ui.receipt ? 'alert' : 'check')}${title}</strong><span>${description}</span></div><span>${uncertain ? '最后确认 10:42:18' : '采样 10:42:18'}</span></div>`;
}
function detail() {
  return `<a class="back-link" href="#/overview">${icon('back')}返回运行总览</a><section class="page-header detail-header"><div><div class="detail-title"><span class="coin">₿</span><h1>BTC 稳健网格</h1>${primaryStatus()}<span class="outline-tag">NET</span></div><div class="detail-meta"><span class="mono">grid-btc-01</span><span>Gate CrossEx · BTC / USDT</span><span>运行代次 2</span><span>Numba Grid 1.2.1</span></div></div><div class="header-actions">${button('观测与诊断', 'diagnostics', '', 'info')}${button('暂停新增', 'pause', '', 'pause', !canControl('pause'))}${button('停止策略', 'stop', 'danger', 'stop', !canControl('stop'))}</div></section>${stateLine()}
    <nav class="detail-tabs" aria-label="策略详情内容">${[['summary','运行概览'],['orders','订单与成交'],['parameters','参数与版本'],['events','事件记录']].map(([id,label]) => `<button class="${ui.tab === id ? 'selected' : ''}" data-action="tab" data-value="${id}" aria-current="${ui.tab === id ? 'page' : 'false'}">${label}${id === 'orders' ? '<span class="tab-count">4</span>' : ''}</button>`).join('')}</nav><section id="detail-content">${detailContent()}</section>${pageFooter()}`;
}
function detailContent() {
  if (ui.tab === 'orders') return ordersPage();
  if (ui.tab === 'parameters') return parametersPage();
  if (ui.tab === 'events') return eventsPage();
  return `<section class="metrics detail-kpis" aria-label="策略指标">${metric('网格配对毛收益', '<span class="positive" title="原始值：1.280000 USDT">+1.28</span>', 'USDT', '已配对 8 轮 · 未计费用与资金费')}${metric('策略净收益', '<span class="muted">—</span>', 'USDT', '<span class="warning-text">费用归属不完整，暂不可确认</span>')}${metric('账户最大回撤', '0.34', '%', '本次运行期 · 账户权益采样值', '', 'chart')}${metric(isUncertain() ? '最后确认持仓' : '当前持仓', '0.00060', 'BTC', 'NET · 均价 80,640.0 USDT', '', 'wallet')}</section><div class="split detail-main">${equityChart(true)}${gridCard()}</div><div class="detail-bottom">${ordersCard(true)}${riskCard(true)}</div>`;
}
function gridCard() {
  const ladder = [
    ['sell', '卖', '81,200.0', '60%'], ['sell', '卖', '81,100.0', '60%'],
    ['current', '现价', '81,000.0', '0%'], ['buy', '买', '80,900.0', '60%'], ['buy', '买', '80,800.0', '60%'],
  ];
  return `<article class="card grid-card"><div class="risk-heading"><h2 class="card-title">网格状态</h2><span class="status ${isUncertain() ? 'alert' : ''}">${isUncertain() ? '最后确认' : '双向挂单'}</span></div><div class="grid-sub">每格 100.0 USDT <span>·</span> 0.00010 BTC / 笔</div><div class="price-ladder" aria-label="网格价格阶梯">${ladder.map(([kind, label, price, width]) => `<div class="price-row ${kind}"><span class="${kind === 'sell' ? 'negative' : kind === 'buy' ? 'positive' : ''}">${label}</span><span class="line"><i style="width:${width}"></i></span><span class="price num">${price}</span></div>`).join('')}</div><div class="grid-footer"><span>锚点 <strong class="num">81,000.0</strong></span><span>买 2 / 卖 2</span></div></article>`;
}
function orderRows(full = false) {
  return orders.map(row => `<tr><td><span class="status ${row.side}">${row.side === 'buy' ? '买入' : '卖出'}</span></td><td class="num">${row.price}</td><td class="num">${row.quantity}</td>${full ? '<td class="num">0.00000</td>' : ''}<td><span class="muted">${isUncertain() ? '最后确认挂单' : '已挂单'}</span></td>${full ? `<td>${row.reserve}</td><td class="mono muted">${row.id}</td>` : ''}</tr>`).join('');
}
function ordersCard(compact = false) {
  return `<article class="card"><div class="section-heading"><h2>${isUncertain() ? '最后确认委托' : '活动委托'} <span class="count">4</span></h2>${compact ? '<button class="text-button" data-action="tab" data-value="orders">全部订单 ' + icon('arrow') + '</button>' : '<span class="small muted">基础币数量 · BTC</span>'}</div><div class="table-scroll"><table><thead><tr><th>方向</th><th>委托价 / USDT</th><th>数量 / BTC</th>${compact ? '' : '<th>累计成交 / BTC</th>'}<th>状态</th>${compact ? '' : '<th>用途</th><th>订单标识</th>'}</tr></thead><tbody>${orderRows(!compact)}</tbody></table></div>${compact ? '<div class="inline-note">卖出委托使用只减仓。暂停新增后仍接收成交、持仓与撤单确认。</div>' : ''}</article>`;
}
function ordersPage() {
  return `${isUncertain() ? '<div class="data-note warning">以下是最后确认的委托记录，不能据此确认当前挂单或仓位。</div>' : ''}<div class="table-tools" style="padding-left:0;padding-right:0"><div class="filter-tabs" role="group" aria-label="订单或成交记录"><button data-action="order-tab" data-value="orders" aria-pressed="${ui.orderTab === 'orders'}" class="${ui.orderTab === 'orders' ? 'selected' : ''}">活动委托 <span>4</span></button><button data-action="order-tab" data-value="fills" aria-pressed="${ui.orderTab === 'fills'}" class="${ui.orderTab === 'fills' ? 'selected' : ''}">最近成交 <span>2</span></button></div><span class="small muted">策略 grid-btc-01 · 基础币 BTC</span></div>${ui.orderTab === 'orders' ? ordersCard() : `<section class="card"><div class="section-heading"><h2>最近独立成交</h2><span class="small muted">配对记录 · 示例</span></div><div class="table-scroll"><table><thead><tr><th>时间</th><th>方向</th><th>成交价 / USDT</th><th>数量 / BTC</th><th>费用归属</th><th>来源</th></tr></thead><tbody><tr><td class="num">10:41:56</td><td><span class="status sell">卖出</span></td><td class="num">81,100.0</td><td class="num">0.00010</td><td class="warning-text">尚未完整确认</td><td class="muted">独立成交事实</td></tr><tr><td class="num">10:39:32</td><td><span class="status buy">买入</span></td><td class="num">80,900.0</td><td class="num">0.00010</td><td class="warning-text">尚未完整确认</td><td class="muted">独立成交事实</td></tr></tbody></table></div><div class="inline-note">两笔成交仅展示近期独立事实，不代表上方已完成配对的全部成交。</div></section>`}<div class="data-note" style="margin-top:16px">本页查看委托与独立成交。策略停止沿用引擎既有停止流程，由执行回执与持仓事实确认结果。</div>`;
}
function parameterRows(rows) {
  return rows.map(([label,value]) => `<div class="parameter"><span>${label}</span><strong class="num">${value}</strong></div>`).join('');
}
function parametersPage() {
  return `<div class="data-note">运行实例引用已冻结的参数和制品。修改参数需要创建新版本，当前页面提供只读检查。</div><section class="card"><div class="section-heading"><h2>运行参数</h2><span class="outline-tag">冻结版本 · p-008</span></div><div class="parameters">${parameterRows([['品种','BTC / USDT · Gate Futures'],['持仓模式','NET · 线性合约'],['单笔数量','0.00010 BTC'],['网格间距','100.0 USDT'],['风险敞口上限','100.00 USDT'],['停止方式','撤单并平仓'],['账户绑定','gate · ID 1'],['运行节点','live-01'],['运行代次','2'],['参数生效方式','新实例加载']])}</div></section><section class="card" style="margin-top:16px"><div class="section-heading"><h2>制品与恢复来源</h2><span class="status">身份已核验 · 示例</span></div><div class="parameters">${parameterRows([['策略版本','Numba Grid 1.2.1'],['状态结构','Schema 3'],['制品摘要','<span class="mono" title="示例摘要，非实际制品身份">sha256: a4f9…e821</span>'],['加载来源','已签名原生制品'],['最近 Checkpoint','10:38:06 · 代次 2'],['恢复方式','平台核对账户与订单事实']])}</div></section>`;
}
function timeline(items = events) {
  return `<div class="timeline">${items.map(([time,title,description]) => `<div class="timeline-item"><time class="num">${time}</time><span class="timeline-point"></span><div>${title}<p>${description}</p></div></div>`).join('')}</div>`;
}
function eventsPage() {
  const additions = ui.receipt ? [['10:42:19', `${ui.receipt.label}已受理 · 结果待确认`, '模拟回执：保留同一个操作标识进行查询，运行状态尚未变更。']] : [];
  return `<section class="card"><div class="section-heading"><h2>运行事件</h2><button class="text-button" data-action="operations">操作记录 ${icon('arrow')}</button></div>${timeline([...additions, ...events])}</section>`;
}
function render() {
  chartSequence = 0;
  app.innerHTML = shell(isDetail() ? detail() : overview());
  document.title = `${isDetail() ? 'BTC 稳健网格 · 策略详情' : '运行总览'} | Titan 设计预览`;
}
function navigateDetail() {
  ui.tab = 'summary';
  if (isDetail()) render();
  else location.hash = '/strategy/grid-btc-01';
}
let priorFocus = null;
function closeOverlay() {
  overlays.innerHTML = '';
  document.body.style.overflow = '';
  if (priorFocus?.isConnected) priorFocus.focus();
}
function showOverlay(markup, dialog = false) {
  if (!overlays.firstElementChild) priorFocus = document.activeElement;
  document.body.style.overflow = 'hidden';
  overlays.innerHTML = `<div class="scrim ${dialog ? 'modal-wrap' : ''}" data-backdrop="true">${markup}</div>`;
  (overlays.querySelector('button') || overlays.querySelector('[tabindex]'))?.focus();
}
function drawer(title, content) {
  showOverlay(`<aside class="drawer" role="dialog" aria-modal="true" aria-labelledby="drawer-title"><div class="drawer-header"><div><p class="eyebrow">TITAN / DETAILS</p><h2 id="drawer-title">${title}</h2></div><button class="icon-button" data-action="close" aria-label="关闭详情">${icon('close')}</button></div>${content}</aside>`);
}
function diagnostics() {
  const source = {fixture: true, run: 'grid-btc-01', account: 'gate / 1', generation: 2, observed_at: '2026-10-26T10:42:18+08:00', observation: ui.scenario, scope: 'account', strategy_net_performance: null, missing: ['strategy_fee_attribution'], risk_upper_usdt: isUncertain() ? null : '64.800000', last_confirmed_risk_upper_usdt: '64.800000'};
  drawer('观测与诊断', `${isUncertain() ? '<div class="data-note warning">当前事实不可完整确认，历史值不代表当前账户状态。</div>' : '<div class="data-note">账户快照可用；策略净收益仍缺少完整的费用归属证据。</div>'}<dl><div><dt>观测来源</dt><dd>live-01 / 账户事实 · 示例</dd></div><div><dt>采样时间</dt><dd class="num">10:42:18 · UTC+8</dd></div><div><dt>数据归属</dt><dd>gate · ID 1 / 账户级</dd></div><div><dt>交易入口</dt><dd>${ui.scenario === 'stale' ? '当前未知' : ui.scenario === 'recovery' ? '关闭 · 等待核对' : '引擎确认已开放'}</dd></div><div><dt>策略净收益</dt><dd class="warning-text">不可确认</dd></div><div><dt>风险最大值口径</dt><dd>持久化采样观测最大值</dd></div></dl><h3>完整性提示</h3><p>账户权益与策略收益分别展示。缺少费用归属、行情估值或恢复证据时，相关字段保持缺失，页面不补零、不重新估算。</p><details><summary>查看原始诊断 · 示例</summary><pre>${esc(JSON.stringify(source, null, 2))}</pre></details>`);
}
function recoveryDrawer(id = 'sol') {
  const run = runData.find(row => row.id === id) || runData[2];
  drawer('恢复校验未完成', `<span class="status alert">交易入口关闭</span><h3>${run.name}</h3><p>原挂单的终态证据尚未完整确认，当前持仓与含在途风险上界不可确认。等待平台按原操作与账户接口核对。</p><dl><div><dt>实例</dt><dd class="mono">${run.code}</dd></div><div><dt>绑定品种</dt><dd>${run.symbol}</dd></div><div><dt>当前风险上界</dt><dd>— / 不可确认</dd></div><div><dt>账户占用</dt><dd>保留</dd></div><div><dt>恢复操作</dt><dd>沿用原操作查询</dd></div></dl><div class="data-note warning">知悉告警不代表恢复成功。恢复结论由引擎确认，页面不会清空订单或解除账户占用。</div>${button('查看相关操作', 'operations', '', 'clock')}`);
}
function operationsDrawer() {
  drawer('操作记录', `<p>这里区分管理受理、执行进度与引擎确认。</p>${ui.receipt ? `<div class="data-note warning">${ui.receipt.label}已受理 · 等待引擎确认<br><span class="mono">op-preview-001 · 目标代次 2</span></div>${timeline([['10:42:19','管理服务已受理','模拟回执：沿用同一个操作 ID 查询，未确认策略状态变化。'],['等待中','执行回执待确认','不会因超时创建第二个相同动作。']])}` : `<div class="data-note">当前 BTC 实例没有未完成的控制操作。</div>${timeline([['08:30:04','Checkpoint 恢复已完成','引擎确认代次 2 的恢复校验通过。'],['08:30:02','节点已接收操作','操作尚在执行，此时未显示恢复完成。'],['08:30:01','管理服务已受理','固定制品、账户绑定和目标代次。']])}`}`);
}
function confirmControl(kind) {
  if (!canControl(kind)) return;
  const stop = kind === 'stop';
  const title = stop ? '停止 BTC 稳健网格？' : '暂停新增开仓？';
  showOverlay(`<section class="modal" role="dialog" aria-modal="true" aria-labelledby="modal-title"><span class="modal-icon">${icon(stop ? 'stop' : 'pause')}</span><h2 id="modal-title">${title}</h2><p>${stop ? '按当前制品的停止方式撤销委托并处理持仓。只有撤单、成交与持仓确认完成后，引擎才会确认停止。' : '暂停后不再新增开仓，继续接收账户事实、处理撤单与保护性减仓，已有持仓不会自动清零。'}</p>${ui.scenario === 'stale' ? '<div class="data-note warning">观测已过期，下方为最后确认值。停止请求按实例与代次提交，执行端核对目标并确认结果。</div>' : ''}<div class="parameters">${parameterRows([['目标实例','grid-btc-01 · 代次 2'],['账户与品种','gate / BTC USDT'],[stop ? '停止方式' : '暂停范围',stop ? '撤单并平仓' : '禁止新增开仓'],['最后确认状态','4 笔委托 / 0.00060 BTC']])}</div><div class="data-note">这是设计稿中的模拟操作，不会连接实盘。</div><div class="modal-actions">${button('返回', 'close')}${button(stop ? '模拟提交停止' : '模拟提交暂停', `submit-${kind}`, stop ? 'danger' : 'primary')}</div></section>`, true);
}
let toastTimer;
function toast(message) {
  const el = document.getElementById('toast');
  el.textContent = message; el.classList.add('show');
  clearTimeout(toastTimer); toastTimer = setTimeout(() => el.classList.remove('show'), 4200);
}
document.addEventListener('click', event => {
  const target = event.target.closest('[data-action]');
  if (!target) {
    if (event.target.matches('[data-backdrop]')) closeOverlay();
    return;
  }
  const action = target.dataset.action, value = target.dataset.value;
  if (action === 'skip') { event.preventDefault(); document.getElementById('main').focus(); return; }
  if (action === 'close') return closeOverlay();
  if (action === 'open-btc') return navigateDetail();
  if (action === 'open-run') {
    if (value === 'btc') return navigateDetail();
    if (value === 'sol') return recoveryDrawer();
    return drawer('ETH 区间网格', `<span class="status paused">已暂停</span><p>实例 grid-eth-02 的最后确认状态。暂停禁止新增开仓，事实接收与保护动作继续。</p><dl><div><dt>持仓</dt><dd class="num">0.00000 ETH</dd></div><div><dt>活动委托</dt><dd>0 笔</dd></div><div><dt>含在途风险上界</dt><dd>0.00 USDT · 已确认</dd></div><div><dt>配对毛收益</dt><dd class="positive num">+0.42 USDT</dd></div><div><dt>策略净收益</dt><dd>— / 未确认</dd></div></dl><p>本次视觉提案的完整策略详情以 BTC 实例为样例。</p>${button('打开 BTC 详情', 'drawer-open-btc', 'primary', 'arrow')}`);
  }
  if (action === 'drawer-open-btc') { closeOverlay(); return navigateDetail(); }
  if (action === 'filter') { ui.filter = value; render(); return; }
  if (action === 'tab') { ui.tab = value; render(); return; }
  if (action === 'period') { ui.period = value; render(); return; }
  if (action === 'order-tab') { ui.orderTab = value; render(); return; }
  if (action === 'diagnostics') return diagnostics();
  if (action === 'operations') return operationsDrawer();
  if (action === 'events-drawer') return drawer('待处理事项', `<div class="data-note warning">SOL 低频网格：恢复校验未完成，交易入口保持关闭。</div>${button('查看恢复原因', 'recovery-drawer', '', 'arrow')}${isUncertain() ? `<h3>BTC 稳健网格</h3><p>${ui.scenario === 'stale' ? '观测已过期，当前敞口不可确认。' : '恢复待核对，交易入口保持关闭。'}</p>${button('查看观测来源', 'diagnostics', '', 'info')}` : ''}`);
  if (action === 'recovery-drawer') return recoveryDrawer();
  if (action === 'pause' || action === 'stop') return confirmControl(action);
  if (action === 'submit-pause' || action === 'submit-stop') {
    const kind = action === 'submit-stop' ? 'stop' : 'pause';
    if (!canControl(kind)) return;
    ui.receipt = {kind, label: kind === 'stop' ? '停止操作' : '暂停操作'};
    closeOverlay(); render(); operationsDrawer();
    return toast('模拟请求已受理；运行状态等待引擎确认。');
  }
  if (action === 'reset') {
    clearTimeout(toastTimer); document.getElementById('toast').classList.remove('show');
    closeOverlay(); ui.scenario = 'normal'; ui.receipt = null; ui.tab = 'summary'; ui.period = '24h'; ui.filter = 'all'; ui.query = ''; ui.orderTab = 'orders';
    history.replaceState(null, '', `${location.pathname}${location.hash || '#/overview'}`); render();
  }
});
document.addEventListener('change', event => {
  if (event.target.id !== 'scenario') return;
  ui.scenario = event.target.value; ui.receipt = null;
  const query = ui.scenario === 'normal' ? '' : `?state=${ui.scenario}`;
  history.replaceState(null, '', `${location.pathname}${query}${location.hash || '#/overview'}`);
  closeOverlay(); render(); document.getElementById('scenario').focus();
});
document.addEventListener('input', event => {
  if (event.target.id !== 'run-search') return;
  ui.query = event.target.value;
  document.getElementById('run-rows').innerHTML = runRows();
  document.getElementById('run-count').textContent = `共 ${filteredRuns().length} 个实例 · 每个实例独立品种绑定`;
});
document.addEventListener('keydown', event => {
  if (event.key === 'Escape') return closeOverlay();
  if (event.key !== 'Tab' || !overlays.firstElementChild) return;
  const controls = [...overlays.querySelectorAll('button:not(:disabled), a[href], input, select, summary, [tabindex="0"]')].filter(el => el.getClientRects().length);
  if (!controls.length) return;
  if (event.shiftKey && document.activeElement === controls[0]) { event.preventDefault(); controls.at(-1).focus(); }
  if (!event.shiftKey && document.activeElement === controls.at(-1)) { event.preventDefault(); controls[0].focus(); }
});
document.addEventListener('pointermove', event => {
  if (!event.target.matches('[data-chart-hit]')) return;
  const svg = event.target.closest('svg'), wrap = svg.parentElement;
  const point = new DOMPoint(event.clientX, event.clientY).matrixTransform(svg.getScreenCTM().inverse());
  const config = chartSets[ui.period];
  const plotWidth = Number(event.target.getAttribute('width'));
  const index = Math.max(0, Math.min(config.values.length - 1, Math.round((point.x - 10) / plotWidth * (config.values.length - 1))));
  const x = 10 + index / (config.values.length - 1) * plotWidth;
  const y = 164 - (config.values[index] - config.low) / (config.high - config.low) * 149;
  const cursor = svg.querySelector('.chart-cursor'); cursor.setAttribute('visibility', 'visible');
  cursor.querySelector('path').setAttribute('d', `M${x} 15V164`);
  cursor.querySelector('circle').setAttribute('cx', x); cursor.querySelector('circle').setAttribute('cy', y);
  const tip = wrap.querySelector('.chart-tooltip'); tip.hidden = false;
  tip.innerHTML = `${config.label} · 示例采样 ${String(index+1).padStart(2,'0')}<strong class="num">${config.values[index].toLocaleString('en-US', {minimumFractionDigits:2, maximumFractionDigits:2})} <span class="small">USDT</span></strong>`;
  tip.style.left = `${Math.max(6, Math.min(wrap.clientWidth - 160, (x / svg.viewBox.baseVal.width * svg.clientWidth) - 60))}px`;
});
document.addEventListener('pointerout', event => {
  if (!event.target.matches('[data-chart-hit]')) return;
  const svg = event.target.closest('svg'); svg.querySelector('.chart-cursor').setAttribute('visibility','hidden'); svg.parentElement.querySelector('.chart-tooltip').hidden = true;
});
window.addEventListener('hashchange', () => { closeOverlay(); render(); window.scrollTo(0,0); });
let resizeTimer;
window.addEventListener('resize', () => { clearTimeout(resizeTimer); resizeTimer = setTimeout(render, 120); });
render();
