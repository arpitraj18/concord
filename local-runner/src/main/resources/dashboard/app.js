/* Concord dashboard: React 18 + htm, no build step. Polls the local runner's read-only JSON API. */
(function () {
  const { useState, useEffect, useCallback } = React;
  const html = htm.bind(React.createElement);

  const money = (minor, cur) => minor == null ? "–" : (minor / 100).toLocaleString(undefined, { minimumFractionDigits: 2 }) + " " + (cur || "");
  const ago = (iso) => {
    if (!iso) return "–";
    const s = Math.max(0, (Date.now() - Date.parse(iso)) / 1000);
    if (s < 60) return Math.round(s) + "s";
    if (s < 3600) return Math.round(s / 60) + "m";
    if (s < 86400) return (s / 3600).toFixed(1) + "h";
    return (s / 86400).toFixed(1) + "d";
  };
  const label = (t) => (t || "").replace(/([a-z])([A-Z])/g, "$1 $2").toLowerCase().replaceAll("_", " ");

  function usePoll(url, ms) {
    const [data, setData] = useState(null);
    const [ok, setOk] = useState(true);
    const load = useCallback(() => fetch(url).then(r => r.ok ? r.json() : Promise.reject(r.status))
      .then(d => { setData(d); setOk(true); }).catch(() => setOk(false)), [url]);
    useEffect(() => { load(); const t = setInterval(load, ms); return () => clearInterval(t); }, [load, ms]);
    return [data, ok];
  }

  function Kpi({ label, value, sub }) {
    return html`<div class="kpi"><div class="label">${label}</div><div class="value">${value}</div>${sub ? html`<div class="sub">${sub}</div>` : null}</div>`;
  }

  function Incidents({ items, onOpen }) {
    if (!items) return html`<div class="empty">Loading…</div>`;
    if (items.length === 0) return html`<div class="empty">No open incidents. Gateway, Concord and merchant agree.</div>`;
    return html`<table>
      <thead><tr><th>Severity</th><th>Divergence</th><th>Payment</th><th>Order</th><th class="num">Amount</th><th class="num">Age</th></tr></thead>
      <tbody>${items.map(i => html`
        <tr class="click" key=${i.gateway + i.paymentId + i.type} onClick=${() => onOpen(i.gateway, i.paymentId)}>
          <td><span class=${"badge sev-" + i.severity}>${i.severity}</span></td>
          <td><strong>${label(i.type)}</strong><p class="explanation">${i.explanation}</p></td>
          <td class="mono">${i.gateway}·${i.paymentId}</td>
          <td class="mono">${i.orderId || "–"}<div class="muted">${i.merchantStatus || ""}</div></td>
          <td class="num">${money(i.amountMinor, i.currency)}</td>
          <td class="num">${ago(i.firstSeenAt)}</td>
        </tr>`)}</tbody></table>`;
  }

  function StateBars({ byState }) {
    if (!byState) return null;
    const entries = Object.entries(byState);
    const max = Math.max(1, ...entries.map(([, n]) => n));
    return html`<div class="bars">${entries.map(([s, n]) => html`
      <div class="row" key=${s}><span>${label(s)}</span><div class="track"><div class="fill" style=${{ width: (100 * n / max) + "%" }}></div></div><span class="num">${n}</span></div>`)}</div>`;
  }

  function Counters({ title, values }) {
    const entries = Object.entries(values || {});
    if (entries.length === 0) return null;
    return html`<div><h3 class="muted" style=${{ fontSize: "12px", margin: "14px 0 4px" }}>${title}</h3><div class="counters">${
      entries.map(([k, v]) => html`<span class="k" key=${k}>${label(k)}</span><span class="num">${v}</span>`)}</div></div>`;
  }

  function Payments({ items, onOpen }) {
    if (!items) return html`<div class="empty">Loading…</div>`;
    if (items.length === 0) return html`<div class="empty">No payments yet. Run a scenario on a gateway simulator.</div>`;
    return html`<table>
      <thead><tr><th>Payment</th><th>State</th><th>Order</th><th class="num">Amount</th><th class="num">Version</th><th class="num">Updated</th></tr></thead>
      <tbody>${items.slice(0, 50).map(p => html`
        <tr class="click" key=${p.gateway + p.paymentId} onClick=${() => onOpen(p.gateway, p.paymentId)}>
          <td class="mono">${p.gateway}·${p.paymentId}</td>
          <td><span class="badge">${p.state}</span> ${p.needsVerification ? html`<span class="flag" title=${p.verificationReason}>flagged</span>` : null}</td>
          <td class="mono">${p.orderId || "–"}</td>
          <td class="num">${money(p.amountMinor, p.currency)}</td>
          <td class="num">${p.version}</td>
          <td class="num">${ago(p.updatedAt)}</td>
        </tr>`)}</tbody></table>`;
  }

  function Drawer({ target, onClose }) {
    const [d, setD] = useState(null);
    useEffect(() => {
      setD(null);
      fetch(`/api/payments/${target.gateway.toLowerCase()}/${encodeURIComponent(target.paymentId)}`).then(r => r.json()).then(setD);
    }, [target]);
    return html`<aside class="drawer" role="dialog" aria-label="Payment detail">
      <button class="close" onClick=${onClose}>Close</button>
      <h2 class="mono" style=${{ margin: "0 0 8px" }}>${target.gateway}·${target.paymentId}</h2>
      ${!d ? html`<div class="empty">Loading…</div>` : d.error ? html`<div class="empty">${d.error}</div>` : html`
        <div class="facts">
          <span class="k">State</span><span><span class="badge">${d.payment.state}</span> v${d.payment.version}</span>
          <span class="k">Amount</span><span>${money(d.payment.amountMinor, d.payment.currency)}</span>
          <span class="k">Order</span><span class="mono">${d.payment.orderId || "–"}</span>
          <span class="k">Flag</span><span>${d.payment.needsVerification ? html`<span class="flag">${d.payment.verificationReason}</span>` : "none"}</span>
          <span class="k">Resolver</span><span>${d.resolver ? (d.resolver.parked ? "parked: " + d.resolver.parkedReason : "last check " + ago(d.resolver.lastCheckedAt) + " ago: " + (d.resolver.lastResult || "–")) : "not checked"}</span>
        </div>
        <h3>Incidents</h3>
        ${d.incidents.length === 0 ? html`<div class="muted">None.</div>` : d.incidents.map(i => html`
          <div key=${i.type} style=${{ margin: "6px 0" }}><span class=${"badge sev-" + i.severity}>${i.severity}</span> <strong>${label(i.type)}</strong> <span class="muted">${i.status.toLowerCase()}</span>
          <p class="explanation">${i.explanation}${i.resolution ? " Resolved: " + i.resolution : ""}</p></div>`)}
        <h3>History (every event, applied or not)</h3>
        <ul class="timeline">${d.history.map(h => html`
          <li key=${h.dedupKey} class=${h.outcome}>
            <strong>${h.outcome.toLowerCase()}</strong> <span class="muted">${h.source.toLowerCase().replace("_", " ")} · ${h.eventType}</span>
            <div class="mono muted">${h.stateBefore} → ${h.stateAfter}${h.path && h.path.length > 2 ? "  (inferred " + h.path.slice(1, -1).join(", ") + ")" : ""} · ${ago(h.receivedAt)} ago</div>
          </li>`)}</ul>
        <h3>Sent to the merchant (outbox)</h3>
        ${d.outbox.length === 0 ? html`<div class="muted">Nothing yet.</div>` : html`<table><tbody>${d.outbox.map(o => html`
          <tr key=${o.version}><td class="num">v${o.version}</td><td>${o.fromState} → <strong>${o.toState}</strong></td><td>${o.kind === "CORRECTION" ? html`<span class="flag">correction</span>` : "transition"}</td><td class="mono muted">${o.idempotencyKey}</td></tr>`)}</tbody></table>`}
      `}
    </aside>`;
  }

  function App() {
    const [summary, summaryOk] = usePoll("/api/summary", 3000);
    const [incidents] = usePoll("/api/incidents", 3000);
    const [payments] = usePoll("/api/payments", 3000);
    const [open, setOpen] = useState(null);
    const s = summary || {};
    const sev = s.openBySeverity || {};
    return html`
      <header>
        <h1>Concord</h1><span class="tagline">Every system agrees on what happened to the money.</span>
        <span class=${"live" + (summaryOk ? "" : " stale")}>${summaryOk ? "live" : "not reachable"}</span>
      </header>
      <main>
        <section class="kpis">
          <${Kpi} label="Payments" value=${s.payments ?? "–"} sub=${(s.eventsProcessed ?? 0) + " events processed"} />
          <${Kpi} label="Open incidents" value=${s.openIncidents ?? "–"} sub=${["CRITICAL", "HIGH", "MEDIUM", "LOW"].map(k => (sev[k] || 0) + " " + k.toLowerCase()).join(" · ")} />
          <${Kpi} label="Flagged" value=${s.flagged ?? "–"} sub="contradictions awaiting the resolver" />
          <${Kpi} label="Parked" value=${s.parked ?? "–"} sub="waiting for a human" />
          <${Kpi} label="Delivered" value=${s.delivery?.delivered ?? "–"} sub=${s.delivery ? s.delivery.pending + " pending · " + s.delivery.deadLettered + " dead-lettered" : "delivery off"} />
        </section>
        <section class="grid">
          <div class="panel"><h2>Open incidents <span class="count">${incidents ? incidents.length : ""}</span></h2>
            <div class="body"><${Incidents} items=${incidents} onOpen=${(g, p) => setOpen({ gateway: g, paymentId: p })} /></div></div>
          <div class="panel"><h2>Payments by state</h2><div class="body">
            <${StateBars} byState=${s.byState} />
            <${Counters} title="Resolver" values=${s.resolver} />
            <${Counters} title="Divergence detector" values=${s.detector} />
          </div></div>
        </section>
        <div class="panel"><h2>Recent payments</h2><div class="body"><${Payments} items=${payments} onOpen=${(g, p) => setOpen({ gateway: g, paymentId: p })} /></div></div>
      </main>
      ${open ? html`<${Drawer} target=${open} onClose=${() => setOpen(null)} />` : null}`;
  }

  ReactDOM.createRoot(document.getElementById("root")).render(html`<${App} />`);
})();
