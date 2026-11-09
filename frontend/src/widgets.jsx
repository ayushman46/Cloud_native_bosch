import React from 'react';

export const display = value => value == null ? 'Unavailable' : typeof value === 'number' ? Number(value.toFixed(3)) : String(value);

const GRADE_COLOR = {A: '#7ce2b5', B: '#a9e08a', C: '#ffd995', D: '#ffb27a', F: '#ff958e'};
const COMPONENT_LABEL = {
  availability_during_fault: 'Availability under fault',
  latency_impact: 'Latency impact',
  recovery_speed: 'Recovery speed',
  data_integrity: 'Usage metered correctly',
  rollback_verified: 'Rollback verified',
};

export function ScoreCard({resilience}) {
  if (!resilience || resilience.score == null) return <p className="muted">A resilience score appears after the experiment finishes.</p>;
  const {score, grade, basis, coverage, components, unmeasured} = resilience;
  const r = 56, c = 2 * Math.PI * r, color = GRADE_COLOR[grade] || '#72d6cb';
  return <div>
    <div className="score">
      <div className="dial" role="img" aria-label={`Resilience score ${score} out of 100, grade ${grade}`}>
        <svg viewBox="0 0 132 132"><circle cx="66" cy="66" r={r} fill="none" stroke="#243645" strokeWidth="12"/>
          <circle cx="66" cy="66" r={r} fill="none" stroke={color} strokeWidth="12" strokeLinecap="round" strokeDasharray={`${c * score / 100} ${c}`} transform="rotate(-90 66 66)"/></svg>
        <div className="num"><b style={{color}}>{score}</b><span>Grade {grade}</span></div>
      </div>
      <div className="bars">
        {Object.entries(components).map(([key, v]) => <div key={key} className={'bar' + (v.value == null ? ' none' : '')}>
          <span>{COMPONENT_LABEL[key] || key}</span>
          <i><s style={{width: `${(v.value ?? 0) * 100}%`}}/></i>
          <span>{v.value == null ? 'n/a' : `${v.points}/${v.weight}`}</span>
        </div>)}
      </div>
    </div>
    <p className="small">{basis === 'simulated' ? 'Simulated run: this score illustrates the method and is not a measurement of OpenMeter. ' : 'Measured from live probes. '}
      Coverage {Math.round(coverage * 100)}% of the score weight{unmeasured.length ? `; not measured: ${unmeasured.map(k => COMPONENT_LABEL[k] || k).join(', ')}` : ''}. Unmeasured parts are left out, never scored as zero.</p>
  </div>;
}

const PHASE_FILL = {baseline: '#1d3a4a', activation: '#33291a', fault: '#4a2328', recovery: '#1d4a3a'};

export function ProbeChart({probes}) {
  if (!probes?.length) return <p className="muted">Probe measurements appear once baseline monitoring starts.</p>;
  const pts = probes.map((p, i) => ({...p, i}));
  const W = 640, H = 220, L = 44, R = 10, T = 12, B = 26;
  const maxMs = Math.max(50, ...pts.map(p => p.latency_ms || 0)) * 1.1;
  const x = i => L + (pts.length === 1 ? 0 : i / (pts.length - 1)) * (W - L - R);
  const y = ms => T + (1 - Math.min(ms, maxMs) / maxMs) * (H - T - B);
  const runs = [];
  pts.forEach(p => { const last = runs[runs.length - 1]; if (last && last.phase === p.phase) last.end = p.i; else runs.push({phase: p.phase, start: p.i, end: p.i}); });
  const line = pts.filter(p => p.latency_ms != null).map((p, k) => `${k ? 'L' : 'M'}${x(p.i).toFixed(1)},${y(p.latency_ms).toFixed(1)}`).join(' ');
  return <div>
    <svg className="chart" viewBox={`0 0 ${W} ${H}`} role="img" aria-label="Probe latency over time, with failed probes marked">
      {runs.map((r, k) => <rect key={k} x={x(r.start) - (r.start === 0 ? 0 : 3)} y={T} width={Math.max(6, x(r.end) - x(r.start) + 6)} height={H - T - B} fill={PHASE_FILL[r.phase] || '#1d2b38'} opacity=".85"/>)}
      {[0, .5, 1].map(f => <g key={f}><line x1={L} x2={W - R} y1={y(maxMs * f)} y2={y(maxMs * f)} stroke="#2a3b4a" strokeWidth="1"/><text x={L - 6} y={y(maxMs * f) + 4} textAnchor="end" fontSize="11" fill="#93a6b6">{Math.round(maxMs * f)}</text></g>)}
      <path d={line} fill="none" stroke="#72d6cb" strokeWidth="2"/>
      {pts.filter(p => !p.reachable).map(p => <g key={p.i}><circle cx={x(p.i)} cy={H - B - 6} r="4.5" fill="#ff958e"/></g>)}
      <text x={L} y={H - 6} fontSize="11" fill="#93a6b6">earlier</text><text x={W - R} y={H - 6} fontSize="11" fill="#93a6b6" textAnchor="end">later</text>
      <text x="4" y="12" fontSize="11" fill="#93a6b6">ms</text>
    </svg>
    <div className="legend">
      {Object.entries(PHASE_FILL).filter(([k]) => runs.some(r => r.phase === k)).map(([k, c]) => <span key={k}><i style={{background: c, border: '1px solid #3d5566'}}/>{k}</span>)}
      <span><i style={{background: '#72d6cb'}}/>latency</span><span><i style={{background: '#ff958e', borderRadius: 5}}/>failed probe</span>
    </div>
  </div>;
}

const NODE_POS = {
  client: [70, 110], 'openmeter-api': [230, 110], kafka: [400, 50], postgres: [400, 170],
  'sink-worker': [570, 30], 'balance-worker': [570, 110], clickhouse: [740, 70], redis: [740, 170],
};

export function DependencyMap({map, target, active}) {
  if (!map?.edges?.length) return <p className="muted">Dependency map unavailable.</p>;
  const alias = map.target_aliases || {};
  const hit = alias[target] || target;
  const nodes = [...new Set(map.edges.flatMap(e => [e.source, e.target]))].filter(n => NODE_POS[n]);
  // services that depend (directly or transitively) on the target are inside the blast radius
  const radius = new Set();
  if (hit) { let frontier = [hit]; while (frontier.length) { const next = []; map.edges.forEach(e => { if (frontier.includes(e.target) && !radius.has(e.source) && e.source !== hit) { radius.add(e.source); next.push(e.source); } }); frontier = next; } }
  const fill = n => n === hit ? (active ? '#7a2a2a' : '#4a3a1a') : radius.has(n) ? '#4a3a1a' : '#1d2b38';
  const stroke = n => n === hit ? (active ? '#ff958e' : '#ffd995') : radius.has(n) ? '#ffd995' : '#3d5566';
  return <div>
    <svg className="depmap" viewBox="0 0 820 220" role="img" aria-label="OpenMeter service dependency map">
      <defs><marker id="arr" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0L10 5L0 10z" fill="#5f7a8c"/></marker></defs>
      {map.edges.filter(e => NODE_POS[e.source] && NODE_POS[e.target]).map((e, i) => {
        const [x1, y1] = NODE_POS[e.source], [x2, y2] = NODE_POS[e.target];
        const dx = x2 - x1, dy = y2 - y1, len = Math.hypot(dx, dy) || 1, off = 40;
        return <line key={i} x1={x1 + dx / len * off} y1={y1 + dy / len * 18} x2={x2 - dx / len * off} y2={y2 - dy / len * 18} stroke={e.origin === 'derived' ? '#8aa9bd' : '#5f7a8c'} strokeDasharray={e.origin === 'derived' ? '5 4' : undefined} strokeWidth="1.4" markerEnd="url(#arr)"><title>{e.source} {e.relation.replaceAll('_', ' ')} {e.target}</title></line>;
      })}
      {nodes.map(n => <g key={n}><rect x={NODE_POS[n][0] - 46} y={NODE_POS[n][1] - 18} width="92" height="36" rx="8" fill={fill(n)} stroke={stroke(n)} strokeWidth="1.6"/>
        <text x={NODE_POS[n][0]} y={NODE_POS[n][1] + 4} textAnchor="middle" fontSize="12" fill="#e8eef3">{n}</text></g>)}
    </svg>
    <p className="small">{map.source}</p>
    <div className="legend"><span><i style={{background: '#7a2a2a', border: '1px solid #ff958e'}}/>fault target</span><span><i style={{background: '#4a3a1a', border: '1px solid #ffd995'}}/>blast radius (depends on target)</span><span>Solid: documented data flow. Dashed: found in the running containers.</span></div>
  </div>;
}
