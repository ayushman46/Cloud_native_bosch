import React, {useEffect, useState} from 'react';
import {createRoot} from 'react-dom/client';
import './style.css';

const API = import.meta.env.VITE_API_URL || '';
const TERMINAL = ['COMPLETED','FAILED','ABORTED','REJECTED','RECOVERY_FAILED'];
const FAULTS = ['application_crash','network_latency','service_restart','traffic_spike'];
const STAGES = ['PLANNING','PLANNED','VALIDATING','APPROVED','BASELINING','INJECTING','ARMED','ACTIVE','ROLLING_BACK','VERIFYING_RECOVERY','COMPLETED'];
async function api(path, options={}) {
  const token = sessionStorage.getItem('chaosai_token') || '';
  const response = await fetch(API + path, {...options,headers:{'Content-Type':'application/json',...(token?{Authorization:`Bearer ${token}`}:{})}});
  const data = await response.json();
  if (!response.ok) {
    const detail = Array.isArray(data.detail) ? data.detail.map(x=>x.msg).join('; ') : data.detail;
    throw new Error(detail || response.statusText);
  }
  return data;
}
const display = value => value == null ? 'Unavailable' : typeof value === 'number' ? Number(value.toFixed(3)) : String(value);

function App(){
  const [health,setHealth]=useState(null), [caps,setCaps]=useState(null), [items,setItems]=useState([]), [selected,setSelected]=useState(null);
  const [token,setToken]=useState(sessionStorage.getItem('chaosai_token')||'');
  const [goal,setGoal]=useState('Test whether usage events survive a sink worker restart');
  const [fault,setFault]=useState('service_restart'), [target,setTarget]=useState('sink-worker');
  const [preview,setPreview]=useState(null), [error,setError]=useState(''), [connectionError,setConnectionError]=useState(''), [busy,setBusy]=useState(false);
  const active=items.some(e=>!TERMINAL.includes(e.status)) || (selected && !TERMINAL.includes(selected.status));
  async function refresh(){
    try {
      const [h,c,x]=await Promise.all([api('/api/health'),api('/api/capabilities'),api('/api/experiments')]);
      setHealth(h);setCaps(c);setItems(x);setConnectionError('');
      setSelected(previous=>previous?(x.find(i=>i.experiment_id===previous.experiment_id)||previous):null);
    } catch(e) {setConnectionError(e.message);}
  }
  useEffect(()=>{refresh();const id=setInterval(refresh,1500);return()=>clearInterval(id)},[]);
  useEffect(()=>setPreview(null),[goal,fault,target]);
  async function action(kind){
    setError('');setBusy(true);
    const request={goal,preferred_fault:fault,preferred_target:target};
    try {
      if(kind==='plan') setPreview(await api('/api/experiments/plan',{method:'POST',body:JSON.stringify(request)}));
      else {
        const experiment=await api('/api/experiments',{method:'POST',body:JSON.stringify({...request,...(preview?{approved_plan:preview}:{})})});
        setSelected(experiment);setItems(previous=>[experiment,...previous]);setPreview(null);
      }
    } catch(e) {setError(e.message);} finally {setBusy(false);}
  }
  async function abort(){
    setBusy(true);setError('');
    try {setSelected(await api(`/api/experiments/${selected.experiment_id}/abort`,{method:'POST'}));}
    catch(e){setError(e.message);} finally {setBusy(false);}
  }
  function downloadReport(){
    const blob=new Blob([JSON.stringify(selected,null,2)],{type:'application/json'});
    const url=URL.createObjectURL(blob), link=document.createElement('a');
    link.href=url;link.download=`${selected.experiment_id}.json`;link.click();URL.revokeObjectURL(url);
  }
  return <main>
    <div className="authbar"><label>ChaosAI API key<input type="password" autoComplete="off" value={token} onChange={e=>{setToken(e.target.value);sessionStorage.setItem('chaosai_token',e.target.value)}} placeholder="Required in remote mode"/></label><button onClick={refresh}>Connect</button></div>
    <header><div><span className="eyebrow">BOSCH HACKATHON · OPENMETER SANDBOX</span><h1>ChaosAI</h1><p>Plan a bounded fault, observe its effect, and verify recovery.</p></div><div className={'mode '+(health?.mode==='mock'?'mock':'real')}>{!health?'CONNECTING':health.mode==='mock'?'SIMULATION':'REMOTE MODE'}</div></header>
    <section className="status">
      <div><strong>Controller</strong><span className={health?.controller_authenticated?'ok':'bad'}>{health?.controller_authenticated?'Connected & authenticated':'Unavailable'}</span></div>
      <div><strong>OpenMeter</strong><span className={health?.openmeter_reachable?'ok':'bad'}>{health?.mode==='mock'?'Simulated':health?.openmeter_reachable?'Reachable':'Unreachable'}</span></div>
      <div><strong>Monitoring</strong><span className={health?.monitoring_reachable?'ok':'bad'}>{health?.mode==='mock'?'Simulated':health?.monitoring_reachable?'Reachable':'Unavailable'}</span></div>
      <div><strong>Targets</strong><span>{caps?.targets?.join(', ')||'—'}</span></div>
    </section>
    {(error||connectionError)&&<div role="alert" className="alert">{error||connectionError}</div>}
    {health?.errors?.length>0&&<div className="alert">{health.errors.join(' · ')}</div>}
    <div className="grid">
      <section className="card"><h2>New experiment</h2>
        <label>Testing objective<textarea value={goal} onChange={e=>setGoal(e.target.value)}/></label>
        <div className="fields"><label>Fault type<select value={fault} onChange={e=>{setFault(e.target.value);setTarget(e.target.value==='service_restart'?'sink-worker':'openmeter')}}>{FAULTS.map(v=><option key={v}>{v}</option>)}</select></label><label>Target<select value={target} onChange={e=>setTarget(e.target.value)}>{(caps?.targets||['openmeter','sink-worker']).map(v=><option key={v}>{v}</option>)}</select></label></div>
        <div className="actions"><button disabled={busy||goal.length<8||!caps} onClick={()=>action('plan')}>{busy?'Working…':'Preview plan'}</button><button className="primary" disabled={busy||active||goal.length<8||!health||health.mode==='real'&&(!health.controller_authenticated||!health.openmeter_reachable)} onClick={()=>action('run')}>Run experiment</button></div>
        {preview&&<div className="preview"><h3>Proposed plan</h3><p>{preview.reason}</p><dl><dt>Fault</dt><dd>{preview.failure_type}</dd><dt>Target</dt><dd>{preview.target_service}</dd><dt>Duration</dt><dd>{preview.parameters.duration_seconds}s</dd></dl><p className="small">{preview.expected_impact}</p></div>}
      </section>
      <section className="card"><h2>Experiment timeline</h2>{selected?<>
        <div className="topline"><strong data-testid="experiment-status">{selected.status}</strong><code title={selected.experiment_id}>{selected.experiment_id.slice(0,20)}…</code></div><p>{selected.goal}</p>
        <div className="timeline">{STAGES.map(s=><span key={s} className={selected.status===s?'current':selected.history?.some(x=>x.status===s)?'visited':''}>{s.replaceAll('_',' ')}</span>)}</div>
        {selected.controller&&<div className="facts"><div>Watchdog armed <b>{String(selected.controller.watchdog_armed)}</b></div><div>Fault verified <b>{String(selected.controller.injection_verified)}</b></div><div>Rollback verified <b>{String(selected.controller.rollback_verified)}</b></div><div>Activation evidence <b>{selected.controller.activation_evidence?.evidence_type||'Pending'}</b></div></div>}
        {selected.events?.length>0&&<div className="facts"><div>Synthetic events accepted <b>{selected.events.filter(e=>e.accepted).length}/{selected.events.length}</b></div><div>Metered count <b>{selected.metered_count??'Pending'}</b></div></div>}
        {selected.error&&<div className="alert">{selected.error}</div>}
        {(!TERMINAL.includes(selected.status)||selected.status==='RECOVERY_FAILED')&&<button className="danger" disabled={busy} onClick={abort}>Emergency abort</button>}
      </>:<p className="muted">Choose a test to see its progress here.</p>}</section>
    </div>
    <div className="grid">
      <section className="card"><h2>Observations</h2>{selected?.probes?.length?<><p className="small">{selected.probes.length} probes collected {selected.simulated?'(simulated)':''}</p><div className="probes">{selected.probes.slice(-12).reverse().map((p,i)=><div key={i}><span>{p.phase}</span><span className={p.reachable?'ok':'bad'}>{p.reachable?'reachable':'failed'}</span><span>{p.latency_ms!=null?`${p.latency_ms} ms`:'—'}</span></div>)}</div></>:<p className="muted">Measurements appear after baseline monitoring starts.</p>}</section>
      <section className="card"><h2>Analysis & report</h2>{selected?.report?<><p>{selected.report.diagnosis}</p><h3>Evidence</h3><ul>{selected.report.evidence.map((x,i)=><li key={i}>{x}</li>)}</ul><h3>Recommendations</h3><ul>{selected.report.recommendations.map((x,i)=><li key={i}>{x}</li>)}</ul><p className="small">Source: {selected.report.analysis_source}. {selected.report.limitations.join(' ')}</p><button onClick={downloadReport}>Download report</button></>:<p className="muted">Reports appear after recovery evaluation.</p>}</section>
    </div>
    {selected?.report&&<section className="card metrics"><h2>Measured resilience</h2><div className="table-scroll"><table><thead><tr><th>Window</th><th>Samples</th><th>Availability</th><th>Error rate</th><th>P95 latency (ms)</th></tr></thead><tbody>{['baseline','fault','recovery'].map(phase=>{const m=selected.report.metrics[phase];return <tr key={phase}><td>{phase}</td><td>{m?.samples??0}</td><td>{display(m?.availability)}</td><td>{display(m?.error_rate)}</td><td>{display(m?.p95_latency_ms)}</td></tr>})}</tbody></table></div><p className="small">Simulated observations are excluded from measured resilience. Throughput, resource usage, processing lag and aggregate agent accuracy remain unavailable until supported evidence is collected.</p></section>}
    <section className="card"><h2>Recent experiments</h2><div className="history">{items.map(e=><button key={e.experiment_id} onClick={()=>setSelected(e)}><span>{e.goal}</span><b>{e.status}</b></button>)}{!items.length&&<p className="muted">No experiments yet.</p>}</div></section>
    <footer>ChaosAI runs separately from OpenMeter. Infrastructure changes are requested through the restricted remote controller.</footer>
  </main>;
}
createRoot(document.getElementById('root')).render(<App/>);
