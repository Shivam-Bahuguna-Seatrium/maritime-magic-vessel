import React, { useEffect, useState, useRef, useCallback } from 'react';

/**
 * GraphViewer – renders the Neo4j knowledge graph using @neo4j-nvl/react.
 *
 * Falls back to a simple canvas rendering if NVL is not available.
 */

let InteractiveNvlWrapper = null;
let NVL_LOAD_ATTEMPTED = false;

// Dynamically load NVL on demand
async function loadNVL() {
  if (NVL_LOAD_ATTEMPTED) return InteractiveNvlWrapper;
  NVL_LOAD_ATTEMPTED = true;
  try {
    const nvlModule = await import('@neo4j-nvl/react');
    InteractiveNvlWrapper = nvlModule.InteractiveNvlWrapper || nvlModule.default;
  } catch (err) {
    console.warn('NVL not available, using canvas fallback:', err.message);
    InteractiveNvlWrapper = null;
  }
  return InteractiveNvlWrapper;
}

export default function GraphViewer({ filters, graphBuilt }) {
  const [graphData, setGraphData] = useState({ nodes: [], relationships: [] });
  const [loading, setLoading] = useState(false);
  const [selected, setSelected] = useState(null);
  const [nvlReady, setNvlReady] = useState(false);
  const [vesselCount, setVesselCount] = useState(0); // Total unique vessel count
  const [dataLimit, setDataLimit] = useState(20); // Frontend data limit (default segment)

  // Load NVL on mount
  useEffect(() => {
    loadNVL().then(() => setNvlReady(true));
  }, []);

  // Fetch vessel count when graph is built
  useEffect(() => {
    if (!graphBuilt) return;
    fetch('/api/kg/filters')
      .then(r => r.json())
      .then(data => {
        console.log('Filters API response:', data);
        const count = data.vessel_count || data.vessel_names?.length || 0;
        console.log('Setting vessel count to:', count);
        setVesselCount(count);
      })
      .catch(err => console.error('Error fetching vessel count:', err));
  }, [graphBuilt]);

  // Fetch graph data whenever filters change
  const fetchGraph = useCallback(async () => {
    if (!graphBuilt) return;
    setLoading(true);
    try {
      const params = new URLSearchParams();
      
      // Apply filters - Category → Vessel Type → Vessel Name → Flag → Validation Status
      if (filters.category) params.set('category', filters.category);
      if (filters.vessel_type) params.set('vessel_type', filters.vessel_type);
      if (filters.vessel_name) params.set('vessel_name', filters.vessel_name);
      if (filters.flag) params.set('flag', filters.flag);
      if (filters.validation_status) params.set('validation_status', filters.validation_status);

      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), 15000); // 15s timeout
      
      // Use new dedicated knowledge graph router
      const res = await fetch(`/api/kg/data?${params}`, { signal: controller.signal });
      clearTimeout(timeoutId);
      
      if (res.ok) {
        let data = await res.json();
        
        // Frontend-side limiting if too many nodes
        if (data.nodes && data.nodes.length > dataLimit) {
          const vessels = data.nodes.filter(n => n.labels?.includes('Vessel'));
          const others = data.nodes.filter(n => !n.labels?.includes('Vessel'));
          const limitedVessels = vessels.slice(0, dataLimit);
          const limitedVesselIds = new Set(limitedVessels.map(v => v.id));
          const limitedRels = data.relationships.filter(r => limitedVesselIds.has(r.startNode));
          
          data = {
            nodes: [...limitedVessels, ...others.slice(0, 100)],
            relationships: limitedRels,
          };
          console.log(`Limited to ${limitedVessels.length} vessels`);
        }
        
        setGraphData(data || { nodes: [], relationships: [] });
      } else {
        console.error('Graph fetch error:', res.status, res.statusText);
        setGraphData({ nodes: [], relationships: [] });
      }
    } catch (err) {
      if (err.name === 'AbortError') {
        console.error('Graph fetch timeout - try adjusting filters');
      } else {
        console.error('Error fetching graph:', err);
      }
      setGraphData({ nodes: [], relationships: [] });
    }
    setLoading(false);
  }, [filters, graphBuilt, dataLimit]);

  useEffect(() => { 
    fetchGraph(); 
  }, [fetchGraph]);

  if (!graphBuilt) {
    return (
      <div className="card" style={{ height: '100%', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
        <p style={{ color: 'var(--text-muted)' }}>Build the knowledge graph to see the visualization.</p>
      </div>
    );
  }

  // Transform data for NVL
  const nvlNodes = graphData.nodes.map(n => ({
    id: n.id,
    size: n.labels?.includes('Vessel') ? 30 : n.labels?.includes('VesselCategory') ? 35 : 20,
    color: n.color || '#3498db',
    caption: n.properties?.name || n.properties?.value || n.properties?.code || n.labels?.[0] || '',
    // Store all data for the detail panel
    _raw: n,
  }));

  const nvlRels = graphData.relationships.map(r => ({
    id: r.id,
    from: r.startNode,
    to: r.endNode,
    caption: r.type,
    _raw: r,
  }));

  return (
    <div className="card" style={{ height: '100%', display: 'flex', flexDirection: 'column', padding: 0, overflow: 'hidden', flex: 1 }}>
      {/* Toolbar */}
      <div style={{
        padding: '12px 16px',
        borderBottom: '1px solid var(--border)',
        display: 'flex', flexDirection: 'column', gap: 8,
        fontSize: '0.82rem', color: 'var(--text-muted)',
        background: 'var(--background)',
        flexShrink: 0,
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: 12, flexWrap: 'wrap' }}>
          <span title="Total nodes in current view">Nodes: <strong style={{ color: 'var(--text)' }}>{nvlNodes.length}</strong></span>
          <span title="Total relationships/edges in current view">Relationships: <strong style={{ color: 'var(--text)' }}>{nvlRels.length}</strong></span>
          {loading && <span style={{ color: 'var(--warning)' }}>Loading…</span>}
          <button
            style={{ marginLeft: 'auto', background: 'var(--border)', color: 'var(--text)', padding: '4px 12px', fontSize: '0.78rem' }}
            onClick={fetchGraph}
            title="Reload graph with current filters"
          >
            ⟳ Refresh
          </button>
        </div>
        
        {/* Control Panel */}
        <div className="controls" style={{ display: 'flex', gap: 12, alignItems: 'center', fontSize: '0.75rem', flexWrap: 'wrap' }}>
          <div style={{ color: 'var(--text-muted)' }} title="Total unique vessels in dataset">
            Total Vessels: <strong style={{ color: 'var(--text)' }}>{vesselCount}</strong>
          </div>
          
          <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
            <label title="Limit how many vessels to display (for performance)">Max Vessels:</label>
            <select 
              value={dataLimit} 
              onChange={(e) => setDataLimit(Number(e.target.value))}
              style={{ padding: '4px 8px', fontSize: '0.75rem', background: 'var(--surface)', color: 'var(--text)', border: '1px solid var(--border)', borderRadius: '4px' }}
              title="Choose how many vessels to visualize at once"
            >
              <option value="20">20</option>
              <option value="40">40</option>
              <option value="100">100</option>
              <option value="500">500</option>
              <option value={vesselCount + 1000}>All ({vesselCount})</option>
            </select>
          </div>
        </div>
      </div>

      {/* Graph area */}
      <div style={{ flex: 1, position: 'relative', display: 'flex', alignItems: 'center', justifyContent: 'center', background: 'var(--background)', overflow: 'hidden' }}>
        {loading && graphData.nodes.length === 0 ? (
          <div style={{ textAlign: 'center', color: 'var(--text-muted)' }}>
            <div style={{ animation: 'spin 1s linear infinite', fontSize: '2rem', marginBottom: 8 }}>⟳</div>
            <p>Loading graph data...</p>
          </div>
        ) : graphData.nodes.length === 0 ? (
          <div style={{ textAlign: 'center', color: 'var(--text-muted)' }}>
            <p>No data to visualize</p>
            <p style={{ fontSize: '0.85rem', marginTop: 8 }}>Try adjusting filters or build the graph again</p>
          </div>
        ) : nvlReady && InteractiveNvlWrapper ? (
          <NvlGraph nodes={nvlNodes} rels={nvlRels} onSelect={setSelected} />
        ) : (
          <CanvasFallback nodes={nvlNodes} rels={nvlRels} onSelect={setSelected} />
        )}

        {/* Detail panel */}
        {selected && (
          <div className="detail-panel" style={{
            position: 'absolute', top: 10, right: 10, width: 300,
            background: 'var(--surface)', border: '1px solid var(--border)',
            borderRadius: 'var(--radius)', padding: 14,
            maxHeight: '70%', overflowY: 'auto', fontSize: '0.8rem',
            boxShadow: '0 4px 12px rgba(0, 0, 0, 0.3)',
          }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 12, alignItems: 'center' }}>
              <strong title={`${selected.labels?.join(', ')}`} style={{ fontSize: '0.9rem' }}>
                {selected.labels?.join(', ')}
              </strong>
              <button
                style={{ background: 'transparent', color: 'var(--text-muted)', padding: '0 4px', cursor: 'pointer', fontSize: '1.2rem' }}
                onClick={() => setSelected(null)}
                title="Close details panel"
              >✕</button>
            </div>
            <table style={{ width: '100%', borderCollapse: 'collapse' }}>
              <tbody>
                {Object.entries(selected.properties || {}).map(([k, v]) => (
                  <tr key={k} style={{ borderBottom: '1px solid var(--border)' }}>
                    <td 
                      style={{ 
                        color: 'var(--text-muted)', 
                        padding: '6px 4px', 
                        verticalAlign: 'top',
                        fontWeight: 600,
                        wordBreak: 'break-word'
                      }} 
                      title={`Property: ${k}`}
                    >
                      {k}
                    </td>
                    <td style={{ padding: '6px 4px', wordBreak: 'break-word' }}>
                      {k === 'validation_status' ? (
                        <span className={`badge ${v === 'valid' ? 'valid' : 'invalid'}`}>{v}</span>
                      ) : String(v).substring(0, 50)}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}


/* ---------- NVL wrapper ---------- */
function NvlGraph({ nodes, rels, onSelect }) {
  const handleClick = useCallback((node) => {
    if (node?._raw) onSelect(node._raw);
  }, [onSelect]);

  return (
    <InteractiveNvlWrapper
      nodes={nodes}
      rels={rels}
      nvlOptions={{
        layout: 'force-directed',
        useWebGL: true,
        relationshipThreshold: 0.55,
        initialZoom: 1,
        fitViewOnLoad: true,
        physics: {
          enabled: true,
          forceAtlas: {
            enabled: true,
            iterations: 10,
          },
          stabilizer: {
            iterations: 200,
          },
        },
        interaction: {
          navigationButtons: true,
          keyboard: true,
          zoomView: true,
          dragView: true,
          mouseWheelZoom: true,
        },
      }}
      mouseEventCallbacks={{
        onNodeClick: handleClick,
      }}
      style={{ width: '100%', height: '100%' }}
    />
  );
}


/* ---------- Canvas fallback (no NVL) ---------- */
function CanvasFallback({ nodes, rels, onSelect }) {
  const canvasRef = useRef(null);
  const posRef = useRef({});
  const dragRef = useRef(null);

  const redraw = useCallback(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    const W = canvas.width;
    const H = canvas.height;
    const pos = posRef.current;

    // Clear with dark background
    ctx.fillStyle = '#1a1a1a';
    ctx.fillRect(0, 0, W, H);

    // Draw edges
    ctx.strokeStyle = '#ffffff';
    ctx.lineWidth = 1;
    rels.forEach(r => {
      const a = pos[r.from], b = pos[r.to];
      if (!a || !b) return;
      ctx.beginPath();
      ctx.moveTo(a.x, a.y);
      ctx.lineTo(b.x, b.y);
      ctx.stroke();
    });

    // Draw nodes
    nodes.forEach(n => {
      const p = pos[n.id];
      if (!p) return;
      
      ctx.beginPath();
      ctx.arc(p.x, p.y, n.size / 2, 0, Math.PI * 2);
      ctx.fillStyle = n.color;
      ctx.fill();
      ctx.strokeStyle = '#1e293b';
      ctx.lineWidth = 2;
      ctx.stroke();

      // Label
      ctx.fillStyle = '#e2e8f0';
      ctx.font = 'bold 10px Inter, sans-serif';
      ctx.textAlign = 'center';
      const caption = String(n.caption || '').trim();
      const label = caption.substring(0, 18);
      ctx.fillText(label, p.x, p.y + n.size / 2 + 12);
    });
  }, [nodes, rels]);



  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const W = canvas.parentElement.clientWidth;
    const H = canvas.parentElement.clientHeight;
    canvas.width = W;
    canvas.height = H;

    const pos = posRef.current;
    // Initialize positions if needed
    nodes.forEach(n => {
      if (!pos[n.id]) {
        pos[n.id] = { x: 60 + Math.random() * (W - 120), y: 60 + Math.random() * (H - 120) };
      }
    });

    // Simple force-directed layout (one-time calculation)
    for (let iter = 0; iter < 80; iter++) {
      // Repulsion
      for (let i = 0; i < nodes.length; i++) {
        for (let j = i + 1; j < nodes.length; j++) {
          const a = pos[nodes[i].id], b = pos[nodes[j].id];
          if (!a || !b) continue;
          const dx = b.x - a.x, dy = b.y - a.y;
          const dist = Math.max(1, Math.sqrt(dx * dx + dy * dy));
          if (dist > 0) {
            const force = 800 / (dist * dist);
            a.x -= (dx / dist) * force;
            a.y -= (dy / dist) * force;
            b.x += (dx / dist) * force;
            b.y += (dy / dist) * force;
          }
        }
      }
      // Attraction along edges
      rels.forEach(r => {
        const a = pos[r.from], b = pos[r.to];
        if (!a || !b) return;
        const dx = b.x - a.x, dy = b.y - a.y;
        const dist = Math.max(1, Math.sqrt(dx * dx + dy * dy));
        if (dist > 0) {
          const force = (dist - 120) * 0.02;
          a.x += (dx / dist) * force;
          a.y += (dy / dist) * force;
          b.x -= (dx / dist) * force;
          b.y -= (dy / dist) * force;
        }
      });
      // Constrain
      nodes.forEach(n => {
        const p = pos[n.id];
        if (p) {
          p.x = Math.max(30, Math.min(W - 30, p.x));
          p.y = Math.max(30, Math.min(H - 30, p.y));
        }
      });
    }

    // Initial draw
    redraw();

    // Mouse handlers
    const handleMouseDown = (e) => {
      const rect = canvas.getBoundingClientRect();
      const mx = e.clientX - rect.left, my = e.clientY - rect.top;
      for (const n of nodes) {
        const p = pos[n.id];
        if (!p) continue;
        const dx = mx - p.x, dy = my - p.y;
        if (dx * dx + dy * dy < (n.size / 2 + 4) ** 2) {
          dragRef.current = { id: n.id };
          onSelect(n._raw);
          return;
        }
      }
    };

    const handleMouseMove = (e) => {
      if (!dragRef.current) return;
      const rect = canvas.getBoundingClientRect();
      const mx = e.clientX - rect.left, my = e.clientY - rect.top;
      const p = pos[dragRef.current.id];
      if (p) {
        p.x = mx;
        p.y = my;
        redraw();
      }
    };

    const handleMouseUp = () => {
      dragRef.current = null;
    };

    canvas.addEventListener('mousedown', handleMouseDown);
    canvas.addEventListener('mousemove', handleMouseMove);
    canvas.addEventListener('mouseup', handleMouseUp);
    canvas.addEventListener('mouseleave', handleMouseUp);

    return () => {
      canvas.removeEventListener('mousedown', handleMouseDown);
      canvas.removeEventListener('mousemove', handleMouseMove);
      canvas.removeEventListener('mouseup', handleMouseUp);
      canvas.removeEventListener('mouseleave', handleMouseUp);
    };
  }, [nodes, rels, onSelect, redraw]);

  return (
    <canvas 
      ref={canvasRef} 
      style={{ 
        width: '100%', 
        height: '100%', 
        display: 'block',
        cursor: dragRef.current ? 'grabbing' : 'grab',
      }} 
    />
  );
}
