import React, { useState, useRef, useEffect } from 'react';
import ResultsTable from './ResultsTable';

/**
 * ChatPanel – conversational interface that:
 *   1. Accepts a natural-language question
 *   2. Sends it to /api/chat
 *   3. Displays the generated Cypher query
 *   4. Shows the natural-language answer
 */

// Advanced predefined query templates based on real maritime data patterns
const PREDEFINED_QUERIES = [
  {
    title: '⚡ Energy Sector Giants',
    query: 'Top 50 large tanker vessels (>20,000 GT)',
    exampleCypher: `MATCH (v:Vessel)-[:IS_TYPE]->(vt:VesselType) 
WHERE vt.name CONTAINS 'Tanker' AND v.properties.gross_tonnage > 20000
RETURN v.properties.name, v.properties.gross_tonnage, 
       v.properties.flag, vt.name
ORDER BY v.properties.gross_tonnage DESC LIMIT 50`,
    exampleResult: {
      count: '50',
      description: 'Strategic assets moving global energy reserves',
    },
  },
  {
    title: '🌍 Global Maritime Jurisdiction',
    query: 'Top 50 flag countries & vessel types by distribution',
    exampleCypher: `MATCH (v:Vessel)-[:IS_TYPE]->(vt:VesselType)
RETURN v.properties.flag AS flag, vt.name AS type, COUNT(*) AS count
ORDER BY count DESC LIMIT 50`,
    exampleResult: {
      count: '50',
      description: 'Geopolitical maritime trade patterns and control points',
    },
  },
  {
    title: '⚠️ Aging Asset Lifecycle',
    query: 'Top 50 oldest vessels built before 2010',
    exampleCypher: `MATCH (v:Vessel)
WHERE v.properties.built_year > 0 AND v.properties.built_year < 2010
RETURN v.properties.name, v.properties.built_year, 
       v.properties.gross_tonnage
ORDER BY v.properties.built_year ASC LIMIT 50`,
    exampleResult: {
      count: '50',
      description: 'Maintenance costs, compliance risks & obsolescence considerations',
    },
  },
  {
    title: '💎 Ultra-Premium Fleet Leaders',
    query: 'Top 50 high-capacity vessels (>50,000 GT)',
    exampleCypher: `MATCH (v:Vessel)-[:IS_TYPE]->(vt:VesselType)
WHERE v.properties.gross_tonnage > 50000
RETURN v.properties.name, v.properties.gross_tonnage, 
       v.properties.flag, vt.name
ORDER BY v.properties.gross_tonnage DESC LIMIT 50`,
    exampleResult: {
      count: '50',
      description: 'Most valuable maritime assets dominating global trade',
    },
  },
  {
    title: '📊 Fleet Statistics & Analytics',
    query: 'Complete fleet metrics - count, avg tonnage & age',
    exampleCypher: `MATCH (v:Vessel)
RETURN COUNT(*) AS total_vessels, AVG(v.properties.gross_tonnage) AS avg_tonnage, 
       AVG(v.properties.built_year) AS avg_year LIMIT 1`,
    exampleResult: {
      count: '1',
      description: 'Strategic market intelligence and fleet composition data',
    },
  },
];

export default function ChatPanel({ graphBuilt }) {
  const [messages, setMessages] = useState([
    {
      role: 'assistant',
      text: 'Hello! Ask me anything about the maritime vessel knowledge graph. I can generate Cypher queries and answer in natural language. Try one of the predefined queries below to see examples! 🚢',
    },
  ]);
  const [input, setInput] = useState('');
  const [loading, setLoading] = useState(false);
  const [showExamples, setShowExamples] = useState(true);
  const bottomRef = useRef(null);

  useEffect(() => {
    bottomRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  const send = async () => {
    const msg = input.trim();
    if (!msg) return;
    setInput('');
    setMessages(prev => [...prev, { role: 'user', text: msg }]);
    setLoading(true);
    setShowExamples(false);

    try {
      // First try predefined endpoint (no OpenAI required)
      const res = await fetch('/api/chat/predefined', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: msg }),
      });
      const data = await res.json();

      // Show query and answer
      const parts = [];
      if (data.cypher) {
        parts.push(`**Executing Query:**\n\`\`\`cypher\n${data.cypher}\n\`\`\``);
      }
      parts.push(data.answer || 'No response.');

      setMessages(prev => [...prev, { role: 'assistant', text: parts.join('\n\n'), data: data.data }]);
    } catch (e) {
      setMessages(prev => [...prev, { role: 'assistant', text: `Error: ${e.message}` }]);
    }
    setLoading(false);
  };

  const handleKey = (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      send();
    }
  };

  const runPredefinedQuery = async (predefinedQuery) => {
    const msg = predefinedQuery.query;
    setInput('');
    setMessages(prev => [...prev, { role: 'user', text: msg }]);
    setLoading(true);
    setShowExamples(false);

    try {
      // Use predefined endpoint
      const res = await fetch('/api/chat/predefined', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ message: msg }),
      });
      const data = await res.json();

      // Show query first (streaming effect)
      if (data.cypher) {
        setMessages(prev => [...prev, { 
          role: 'assistant', 
          text: `**Executing Query:**\n\`\`\`cypher\n${data.cypher}\n\`\`\``,
          isQuery: true,
        }]);
        
        // Small delay for visual streaming effect
        await new Promise(resolve => setTimeout(resolve, 300));
      }

      // Then show answer
      setMessages(prev => [...prev, { 
        role: 'assistant', 
        text: data.answer || 'No response.',
        data: data.data,
      }]);
    } catch (e) {
      setMessages(prev => [...prev, { role: 'assistant', text: `Error: ${e.message}` }]);
    }
    setLoading(false);
  };

  return (
    <div style={{ width: '100%', maxWidth: 900, margin: '0 auto', display: 'flex', flexDirection: 'column', height: 'calc(100vh - 120px)', padding: 0 }} className="chat-container">
      <h2 style={{ marginBottom: 12, fontSize: 'clamp(1rem, 5vw, 1.2rem)' }}>💬 Chat & Query Builder</h2>

      {/* Predefined Queries Section */}
      {showExamples && graphBuilt && (
        <div style={{
          background: 'linear-gradient(135deg, #667eea15, #764ba215)',
          border: '1px solid var(--border)',
          borderRadius: 'var(--radius)',
          padding: 16,
          marginBottom: 16,
          maxHeight: '65vh',
          overflowY: 'auto',
          marginTop: 8,
        }} className="chat-predefined-queries">
          <div style={{ display: 'flex', alignItems: 'center', gap: 8, marginBottom: 12, position: 'sticky', top: 0, background: 'linear-gradient(135deg, #667eea15, #764ba215)', paddingBottom: 8, zIndex: 10 }}>
            <span style={{ fontSize: 'clamp(0.9rem, 3vw, 1rem)' }}>📋</span>
            <h3 style={{ fontSize: 'clamp(0.85rem, 3vw, 0.95rem)', margin: 0, fontWeight: 600 }}>5 Creative Maritime Queries</h3>
            <button
              onClick={() => setShowExamples(false)}
              style={{
                marginLeft: 'auto',
                padding: '2px 8px',
                fontSize: '0.7rem',
                background: 'transparent',
                border: '1px solid var(--border)',
                borderRadius: 4,
                cursor: 'pointer',
                color: 'var(--text-muted)',
              }}
            >
              Hide
            </button>
          </div>

          <div style={{ display: 'flex', flexDirection: 'column', gap: 8 }}>
            {PREDEFINED_QUERIES.map((pq, idx) => (
              <div
                key={idx}
                style={{
                  background: 'var(--surface)',
                  border: '1px solid var(--border)',
                  borderRadius: 6,
                  padding: 8,
                  cursor: 'pointer',
                  transition: 'all 0.2s',
                  hover: { background: 'var(--primary)' },
                }}
                onClick={() => runPredefinedQuery(pq)}
                onMouseEnter={(e) => e.currentTarget.style.background = '#334155'}
                onMouseLeave={(e) => e.currentTarget.style.background = 'var(--surface)'}
              >
                <div style={{ display: 'flex', alignItems: 'flex-start', gap: 8 }}>
                  <span style={{ fontSize: '1rem', flexShrink: 0 }}>🔍</span>
                  <div style={{ flex: 1, minWidth: 0 }}>
                    <div style={{ fontWeight: 600, fontSize: '0.85rem' }}>{pq.title}</div>
                    <div style={{ fontSize: '0.75rem', color: 'var(--text-muted)', marginTop: 2 }}>
                      {pq.query}
                    </div>
                  </div>
                </div>
              </div>
            ))}
          </div>

          <div style={{
            marginTop: 8,
            padding: 8,
            background: '#0f172a',
            borderRadius: 6,
            fontSize: '0.75rem',
            color: 'var(--text-muted)',
            lineHeight: 1.4,
          }}>
            📌 <strong>Note:</strong> Using predefined queries. Full natural language AI requires paid API key (OpenAI/Claude).
          </div>
        </div>
      )}

      {/* Messages */}
      <div style={{
        flex: 1, overflowY: 'auto',
        border: '1px solid var(--border)', borderRadius: 'var(--radius)',
        padding: 'clamp(8px, 3vw, 16px)', background: 'var(--surface)',
        display: 'flex', flexDirection: 'column', gap: 12,
        minHeight: 200,
      }} className="chat-messages">
        {messages.map((m, i) => (
          <div
            key={i}
            style={{
              alignSelf: m.role === 'user' ? 'flex-end' : 'flex-start',
              maxWidth: m.role === 'user' ? 'min(85%, 600px)' : 'min(95%, 600px)',
              background: m.role === 'user' ? 'var(--primary)' : '#334155',
              borderRadius: 12,
              padding: 'clamp(8px, 2vw, 14px)',
              fontSize: 'clamp(0.8rem, 2.5vw, 0.88rem)',
              lineHeight: 1.5,
              whiteSpace: 'pre-wrap',
              wordWrap: 'break-word',
              overflowWrap: 'break-word',
            }}
            className="chat-message"
          >
            <MessageContent text={m.text} />

            {/* Show results table if query returned rows */}
            {m.data && Array.isArray(m.data) && m.data.length > 0 && (
              <div style={{ marginTop: 12 }}>
                <ResultsTable data={m.data} />
              </div>
            )}
          </div>
        ))}
        {loading && (
          <div style={{ color: 'var(--text-muted)', fontSize: '0.85rem' }}>Thinking…</div>
        )}
        <div ref={bottomRef} />
      </div>

      {/* Input */}
      <div style={{ display: 'flex', gap: 8, marginTop: 10, flexWrap: 'wrap' }} className="chat-input-area">
        <textarea
          value={input}
          onChange={e => setInput(e.target.value)}
          onKeyDown={handleKey}
          placeholder={graphBuilt ? 'Ask about vessels…' : 'Build the graph first to use chat'}
          disabled={!graphBuilt}
          rows={2}
          style={{ flex: 1, minWidth: 200, resize: 'none', fontSize: 'clamp(0.85rem, 2vw, 1rem)' }}
        />
        <button
          className="primary"
          onClick={send}
          disabled={!graphBuilt || loading || !input.trim()}
          style={{ alignSelf: 'flex-end', whiteSpace: 'nowrap' }}
        >
          Send
        </button>
      </div>
    </div>
  );
}


/* Simple markdown-ish renderer for ``code blocks`` and **bold** */
function MessageContent({ text }) {
  if (!text) return null;

  // Split by code blocks
  const parts = text.split(/(```[\s\S]*?```)/g);
  return (
    <>
      {parts.map((part, i) => {
        if (part.startsWith('```')) {
          const inner = part.replace(/```\w*\n?/, '').replace(/```$/, '');
          return (
            <pre
              key={i}
              style={{
                background: '#0f172a',
                padding: '8px 12px',
                borderRadius: 6,
                fontSize: '0.78rem',
                overflowX: 'auto',
                margin: '6px 0',
              }}
            >
              {inner}
            </pre>
          );
        }
        // Bold
        const boldParts = part.split(/(\*\*.*?\*\*)/g);
        return (
          <span key={i}>
            {boldParts.map((bp, j) =>
              bp.startsWith('**') && bp.endsWith('**')
                ? <strong key={j}>{bp.slice(2, -2)}</strong>
                : bp
            )}
          </span>
        );
      })}
    </>
  );
}
