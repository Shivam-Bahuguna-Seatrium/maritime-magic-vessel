import React, { useState, useCallback, useEffect } from 'react';
import Dashboard from './components/Dashboard';
import GraphViewer from './components/GraphViewer';
import ChatPanel from './components/ChatPanel';
import FilterPanel from './components/FilterPanel';
import CaseStudy from './components/CaseStudy';
import DEBUG from './debug';

const TABS = ['Dashboard', 'Knowledge Graph', 'Chat', 'Case Study'];

export default function App() {
  const [tab, setTab] = useState('Dashboard');
  const [graphFilters, setGraphFilters] = useState({});
  const [status, setStatus] = useState({
    dataset_loaded: false,
    record_count: 0,
    validated: false,
    graph_built: false,
  });

  // Log app initialization
  useEffect(() => {
    DEBUG.log('APP', '🚀 App initialized');
    DEBUG.log('APP', `Environment: ${window.location.hostname}:${window.location.port}`);
    DEBUG.log('APP', `API Base URL: ${window.location.origin}`);
  }, []);

  const refreshStatus = useCallback(async () => {
    try {
      DEBUG.api('GET', '/api/status');
      const res = await fetch('/api/status');
      DEBUG.apiResponse('GET', '/api/status', res.status, { ok: res.ok });
      if (res.ok) {
        const data = await res.json();
        setStatus(data);
        DEBUG.info('APP', 'Status refreshed', data);
      } else {
        DEBUG.warn('APP', `Status endpoint returned ${res.status}`);
      }
    } catch (err) {
      DEBUG.apiError('GET', '/api/status', err);
    }
  }, []);

  return (
    <div style={{ display: 'flex', flexDirection: 'column', minHeight: '100vh' }}>
      {/* ---- Header (Fixed at top) ---- */}
      <header>
        <h1>
          🚢 Maritime Vessel Identity System
        </h1>
        <nav>
          {TABS.map(t => (
            <button
              key={t}
              onClick={() => setTab(t)}
              title={`View ${t} tab`}
              style={{
                background: tab === t ? 'var(--primary)' : 'transparent',
                color: tab === t ? '#fff' : 'var(--text-muted)',
                fontWeight: 600,
                border: 'none',
                cursor: 'pointer',
                transition: 'all 0.2s',
              }}
              onMouseEnter={(e) => {
                if (tab !== t) e.currentTarget.style.background = 'rgba(88, 166, 255, 0.1)';
              }}
              onMouseLeave={(e) => {
                if (tab !== t) e.currentTarget.style.background = 'transparent';
              }}
            >
              {t}
            </button>
          ))}
        </nav>

        {/* Status indicators with tooltips */}
        <div style={{ marginLeft: 'auto', display: 'flex', gap: 12, fontSize: '0.8rem', flexWrap: 'wrap', justifyContent: 'flex-end' }} className="status-indicators">
          <span title="Number of rows in uploaded dataset">
            Dataset: {status.dataset_loaded
              ? <span className="badge valid">{status.record_count} rows</span>
              : <span className="badge invalid">None</span>}
          </span>
          <span title="Dataset validation status">
            Validated: {status.validated
              ? <span className="badge valid">Yes</span>
              : <span className="badge warning">No</span>}
          </span>
          <span title="Knowledge graph building status">
            Graph: {status.graph_built
              ? <span className="badge valid">Built</span>
              : <span className="badge warning">Pending</span>}
          </span>
        </div>
      </header>

      {/* ---- Body ---- */}
      <main style={{ flex: 1, overflow: 'auto', display: 'flex', flexDirection: 'column' }}>
        {tab === 'Dashboard' && (
          <Dashboard status={status} refreshStatus={refreshStatus} />
        )}

        {tab === 'Knowledge Graph' && (
          <div style={{ display: 'flex', gap: 16, flex: 1, minHeight: 0 }} className="kg-layout">
            <FilterPanel
              graphBuilt={status.graph_built}
              filters={graphFilters}
              onChange={setGraphFilters}
            />
            <div style={{ flex: 1, minHeight: 0, display: 'flex', flexDirection: 'column' }} className="graph-container">
              <GraphViewer filters={graphFilters} graphBuilt={status.graph_built} refreshStatus={refreshStatus} />
            </div>
          </div>
        )}

        {tab === 'Chat' && (
          <ChatPanel graphBuilt={status.graph_built} />
        )}

        {tab === 'Case Study' && (
          <CaseStudy />
        )}
      </main>
    </div>
  );
}
