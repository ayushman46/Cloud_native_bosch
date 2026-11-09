import React, {useEffect, useMemo, useState} from 'react';
import {createRoot} from 'react-dom/client';
import './style.css';
import {DependencyMap, ProbeChart, ScoreCard, display} from './widgets.jsx';

const API = import.meta.env.VITE_API_URL || '';
const TERMINAL = ['COMPLETED', 'FAILED', 'ABORTED', 'REJECTED', 'RECOVERY_FAILED'];
const SCENARIOS = {
  crash_api: {label: 'Crash the OpenMeter API', fault: 'application_crash', target: 'openmeter', goal: 'Test whether the OpenMeter API recovers after a crash'},
  failed_dependency: {label: 'Fail a dependency (Kafka)', fault: 'application_crash', target: 'kafka', goal: 'Test what happens to usage events when Kafka goes down'},
  slow_network: {label: 'Add network latency', fault: 'network_latency', target: 'openmeter', goal: 'Test how OpenMeter behaves when responses are slow'},
  restart_worker: {label: 'Restart the sink worker', fault: 'service_restart', target: 'sink-worker', goal: 'Test whether usage events survive a sink worker restart'},
  traffic_spike: {label: 'Send a traffic spike', fault: 'traffic_spike', target: 'openmeter', goal: 'Test OpenMeter ingestion under elevated request traffic'},
};
const STAGES = ['PLANNING', 'VALIDATING', 'BASELINING', 'INJECTING', 'ACTIVE', 'ROLLING_BACK', 'VERIFYING_RECOVERY', 'COMPLETED'];

async function api(path, options = {}) {
  const token = sessionStorage.getItem('chaosai_token') || '';
  const response = await fetch(API + path, {...options, headers: {'Content-Type': 'application/json', ...(token ? {Authorization: `Bearer ${token}`} : {})}});
  const data = await response.json();
  if (!response.ok) {
    const detail = Array.isArray(data.detail) ? data.detail.map(x => x.msg).join('; ') : data.detail;
    throw new Error(detail || response.statusText);
  }
  return data;
}

const scoreOf = e => e.report?.metrics?.resilience?.score ?? null;

function App() {
  const [health, setHealth] = useState(null), [caps, setCaps] = useState(null), [config, setConfig] = useState(null), [depmap, setDepmap] = useState(null);
  const [items, setItems] = useState([]), [selected, setSelected] = useState(null);
  const [token, setToken] = useState(sessionStorage.getItem('chaosai_token') || '');
  const [scenario, setScenario] = useState('restart_worker'), [target, setTarget] = useState('sink-worker');
  const [goal, setGoal] = useState(SCENARIOS.restart_worker.goal);
  const fault = SCENARIOS[scenario].fault;
  const [preview, setPreview] = useState(null), [error, setError] = useState(''), [connectionError, setConnectionError] = useState(''), [busy, setBusy] = useState(false);
  const active = items.some(e => !TERMINAL.includes(e.status)) || (selected && !TERMINAL.includes(selected.status));

  async function refresh() {
    try {
      const [h, c, x] = await Promise.all([api('/api/health'), api('/api/capabilities'), api('/api/experiments')]);
      setHealth(h); setCaps(c); setItems(x); setConnectionError('');
      setSelected(previous => previous ? (x.find(i => i.experiment_id === previous.experiment_id) || previous) : (x[0] || null));
      if (!config) setConfig(await api('/api/config'));
      if (!depmap) setDepmap(await api('/api/dependency-map'));
    } catch (e) { setConnectionError(e.message); }
  }
  useEffect(() => { refresh(); const id = setInterval(refresh, 1500); return () => clearInterval(id); }, []);
  useEffect(() => setPreview(null), [goal, scenario, target]);

  function pickScenario(value) {
    setScenario(value);
    setTarget(SCENARIOS[value].target);
    setGoal(SCENARIOS[value].goal);
  }
  async function action(kind) {
    setError(''); setBusy(true);
    const request = {goal, preferred_fault: fault, preferred_target: target};
    try {
      if (kind === 'plan') setPreview(await api('/api/experiments/plan', {method: 'POST', body: JSON.stringify(request)}));
      else {
        const experiment = await api('/api/experiments', {method: 'POST', body: JSON.stringify({...request, ...(preview ? {approved_plan: preview} : {})})});
        setSelected(experiment); setItems(previous => [experiment, ...previous]); setPreview(null);
      }
    } catch (e) { setError(e.message); } finally { setBusy(false); }
  }
  async function abort() {
    setBusy(true); setError('');
    try { setSelected(await api(`/api/experiments/${selected.experiment_id}/abort`, {method: 'POST'})); }
    catch (e) { setError(e.message); } finally { setBusy(false); }
  }
  function downloadReport() {
    const blob = new Blob([JSON.stringify(selected, null, 2)], {type: 'application/json'});
    const url = URL.createObjectURL(blob), link = document.createElement('a');
    link.href = url; link.download = `${selected.experiment_id}.json`; link.click(); URL.revokeObjectURL(url);
  }

  const ranked = useMemo(() => items.filter(e => scoreOf(e) != null).sort((a, b) => scoreOf(a) - scoreOf(b)), [items]);
  const real = health?.mode === 'real';
  const runBlocked = busy || active || goal.length < 8 || !health || (real && (!health.controller_authenticated || !health.openmeter_reachable));
  const resilience = selected?.report?.metrics?.resilience;
  const faultActive = selected && ['ACTIVE', 'ARMED', 'INJECTING'].includes(selected.status);
  const mapTarget = selected?.plan?.target_service || target;
  const auto = config?.auto_stop;

  return <main>
    <div className="top">
      <div><span className="eyebrow">BOSCH AI-DRIVEN CHAOS ENGINEERING · OPENMETER SANDBOX</span><h1>ChaosAI</h1>
        <p className="muted">Plan a bounded fault, watch what it breaks, and verify the system recovers.</p></div>
      <div className={'badge ' + (health?.mode === 'mock' ? 'mock' : 'real')}>{!health ? 'CONNECTING' : health.mode === 'mock' ? 'SIMULATION' : 'REAL MODE'}</div>
    </div>

    <div className="authbar">
      <label>ChaosAI API key<input type="password" autoComplete="off" value={token} onChange={e => { setToken(e.target.value); sessionStorage.setItem('chaosai_token', e.target.value); }} placeholder="Needed in real mode"/></label>
      <button onClick={refresh}>Connect</button>
    </div>

    <section className="strip" aria-label="System status">
      <div><strong>Controller</strong><span className={health?.controller_authenticated ? 'ok' : 'bad'}>{health?.controller_authenticated ? (health.mode === 'mock' ? 'Simulated' : 'Connected') : 'Unavailable'}</span></div>
      <div><strong>OpenMeter</strong><span className={health?.mode === 'mock' || health?.openmeter_reachable ? 'ok' : 'bad'}>{health?.mode === 'mock' ? 'Simulated' : health?.openmeter_reachable ? 'Reachable' : 'Unreachable'}</span></div>
      <div><strong>AI planner</strong><span className={config?.nvidia_configured ? 'ok' : 'warn'}>{config?.nvidia_configured ? (health?.mode === 'mock' ? 'Key set (used in real mode)' : 'NVIDIA LLM') : 'Deterministic fallback'}</span></div>
      <div><strong>Fault limits</strong><span>{config ? `${config.limits.duration_seconds}s · ${config.limits.latency_ms}ms · ${config.limits.requests_per_second}rps` : '—'}</span></div>
      <div><strong>Auto-stop</strong><span className={auto?.enabled ? 'ok' : 'warn'}>{auto ? (auto.enabled ? `at ${Math.round(auto.error_rate * 100)}% errors or ${auto.latency_ms}ms` : 'Disabled') : '—'}</span></div>
    </section>

    {(error || connectionError) && <div role="alert" className="alert">{error || connectionError}</div>}
    {health?.errors?.length > 0 && <div className="alert">{health.errors.join(' · ')}</div>}
    {real && config && !config.real_injections_enabled && <div className="notice">Real injections are switched off. Previews work; set REAL_INJECTIONS_ENABLED=true in .env to run a real fault in your sandbox.</div>}

    <div className="grid">
      <section className="card"><h2>New experiment</h2>
        <div className="fields">
          <label>Scenario<select value={scenario} onChange={e => pickScenario(e.target.value)}>{Object.entries(SCENARIOS).map(([k, v]) => <option key={k} value={k}>{v.label}</option>)}</select></label>
          <label>Target<select value={target} onChange={e => setTarget(e.target.value)}>{(caps?.targets || ['openmeter', 'sink-worker']).map(v => <option key={v}>{v}</option>)}</select></label>
        </div>
        <label style={{marginTop: 12}}>Testing objective<textarea value={goal} onChange={e => setGoal(e.target.value)}/></label>
        <div className="actions">
          <button disabled={busy || goal.length < 8 || !caps} onClick={() => action('plan')}>{busy ? 'Working…' : 'Preview plan'}</button>
          <button className="primary" disabled={runBlocked} onClick={() => action('run')}>Run experiment</button>
        </div>
        {preview && <div className="preview"><h3 style={{marginTop: 0}}>Proposed plan</h3><p>{preview.reason}</p>
          <dl><dt>Fault</dt><dd>{preview.failure_type}</dd><dt>Target</dt><dd>{preview.target_service}</dd><dt>Duration</dt><dd>{preview.parameters.duration_seconds}s</dd>
            {preview.parameters.latency_ms && <><dt>Latency</dt><dd>{preview.parameters.latency_ms} ms</dd></>}
            {preview.parameters.requests_per_second && <><dt>Rate</dt><dd>{preview.parameters.requests_per_second} req/s</dd></>}</dl>
          <p className="small">Expected impact: {preview.expected_impact}</p></div>}
      </section>

      <section className="card"><h2>Live experiment</h2>{selected ? <>
        <div className="topline"><span className="stage" data-testid="experiment-status">{selected.status.replaceAll('_', ' ')}</span><code title={selected.experiment_id}>{selected.experiment_id.slice(0, 18)}…</code></div>
        <p className="small">{selected.goal}</p>
        <div className="timeline">{STAGES.map(s => <span key={s} className={selected.status === s ? 'current' : selected.history?.some(x => x.status === s) ? 'visited' : ''}>{s.replaceAll('_', ' ').toLowerCase()}</span>)}</div>
        {selected.controller && <div className="facts">
          <div>Watchdog armed<b>{selected.controller.watchdog_armed ? 'Yes' : 'No'}</b></div>
          <div>Fault verified<b>{selected.controller.injection_verified ? selected.controller.activation_evidence?.evidence_type : 'Pending'}</b></div>
          <div>Rollback verified<b>{selected.controller.rollback_verified ? 'Yes' : 'Pending'}</b></div>
          {selected.controller.rollback_evidence?.details?.sent != null && <div>Load generated<b>{selected.controller.rollback_evidence.details.sent} requests, {selected.controller.rollback_evidence.details.errors} errors</b></div>}
          {selected.events?.length > 0 && <div>Usage metered<b>{selected.metered_count ?? 'Pending'} of {selected.events.filter(e => e.accepted).length} accepted</b></div>}
        </div>}
        {selected.safety_stop && <div className="notice"><b>Stopped by the safety controller.</b> {selected.safety_stop.reason}. The fault was rolled back early and recovery was still verified.</div>}
        {selected.recovery_note && <div className="notice"><b>Recovered late.</b> {selected.recovery_note}</div>}
        {selected.error && <div className="alert">{selected.error}</div>}
        {(!TERMINAL.includes(selected.status) || selected.status === 'RECOVERY_FAILED') && <button className="danger" disabled={busy} onClick={abort}>Emergency abort: stop the fault now</button>}
      </> : <p className="muted">Run an experiment to watch it here.</p>}</section>
    </div>

    <div className="grid">
      <section className="card"><h2>Resilience score</h2><ScoreCard resilience={resilience}/></section>
      <section className="card"><h2>What depends on what</h2><DependencyMap map={depmap} target={mapTarget} active={faultActive}/></section>
    </div>

    <div className="grid">
      <section className="card"><h2>Measurements</h2>
        <ProbeChart probes={selected?.probes}/>
        {selected?.simulated && selected.probes?.length > 0 && <p className="small">Simulated probes. Nothing here measures a real system.</p>}
      </section>
      <section className="card"><h2>Analysis and recommendations</h2>
        {selected?.report ? <>
          <p>{selected.report.diagnosis}</p>
          <h3>Evidence</h3><ul>{selected.report.evidence.map((x, i) => <li key={i}>{x}</li>)}</ul>
          <h3>Recommendations</h3><ul>{selected.report.recommendations.map((x, i) => <li key={i}>{x}</li>)}</ul>
          <p className="small">Source: {selected.report.analysis_source}. {selected.report.limitations.join(' ')}</p>
          <button onClick={downloadReport}>Download report</button>
        </> : <p className="muted">The report appears after recovery is verified.</p>}
      </section>
    </div>

    {selected?.diagnosis && <section className="card" style={{margin: '16px 0'}}><h2>Diagnosis from measurements (LLM, every claim tied to evidence)</h2>
      {selected.diagnosis.report ? <>
        <p><b>{selected.diagnosis.report.diagnosis.suspected_failure}</b> in <b>{selected.diagnosis.report.diagnosis.suspected_component}</b>. Model confidence {display(selected.diagnosis.report.diagnosis.confidence)}, grounding rate {display(selected.diagnosis.report.grounding.grounding_rate)}.</p>
        <ul>{selected.diagnosis.report.diagnosis.evidence.map((x, i) => <li key={i}>{x}</li>)}</ul>
        {selected.diagnosis.report.recommendations.length > 0 && <><h3>Recommended fixes</h3><ul>{selected.diagnosis.report.recommendations.map((x, i) => <li key={i}>{x}</li>)}</ul></>}
      </> : <p className="muted">Diagnosis {selected.diagnosis.status || 'unavailable'}{selected.diagnosis.error ? `: ${selected.diagnosis.error}` : ''}. The measurements above are unaffected.</p>}
    </section>}

    {selected?.report && <section className="card" style={{margin: '16px 0'}}><h2>Baseline, fault and recovery</h2>
      <div className="table-scroll"><table><thead><tr><th>Window</th><th>Probes</th><th>Availability</th><th>Error rate</th><th>P95 latency (ms)</th></tr></thead>
        <tbody>{['baseline', 'fault', 'recovery'].map(phase => {
          const sim = selected.simulated, m = selected.report.metrics[phase];
          const own = selected.probes.filter(p => p.phase === phase);
          const avail = own.length ? own.filter(p => p.reachable).length / own.length : null;
          const lat = own.map(p => p.latency_ms).filter(v => v != null).sort((a, b) => a - b);
          const p95 = lat.length ? lat[Math.ceil(.95 * lat.length) - 1] : null;
          return <tr key={phase}><td>{phase}{sim ? ' (simulated)' : ''}</td><td>{sim ? own.length : (m?.samples ?? 0)}</td><td>{display(sim ? avail : m?.availability)}</td><td>{display(sim ? (avail == null ? null : 1 - avail) : m?.error_rate)}</td><td>{display(sim ? p95 : m?.p95_latency_ms)}</td></tr>;
        })}</tbody></table></div>
    </section>}

    <section className="card"><h2>Experiments, weakest first</h2>
      <div className="history">
        {[...ranked, ...items.filter(e => scoreOf(e) == null)].map(e => <button key={e.experiment_id} onClick={() => setSelected(e)} aria-pressed={selected?.experiment_id === e.experiment_id}>
          <span>{e.plan ? `${e.plan.failure_type.replaceAll('_', ' ')} on ${e.plan.target_service}` : e.goal}</span>
          <b>{e.status.replaceAll('_', ' ')}</b>
          <span className="sc">{scoreOf(e) != null ? `${scoreOf(e)} / 100` : '—'}</span>
        </button>)}
        {!items.length && <p className="muted">No experiments yet. Run one above.</p>}
      </div>
    </section>
    <footer>Sandbox only. Faults go through the restricted Chaos Control service, which checks every rollback.</footer>
  </main>;
}
createRoot(document.getElementById('root')).render(<App/>);
