import React, { useEffect, useRef } from 'react';

const Mermaid = ({ chart }) => {
  const containerRef = useRef(null);

  useEffect(() => {
    if (window.mermaid && containerRef.current) {
      try {
        window.mermaid.contentLoaded();
      } catch (err) {
        // Mermaid already initialized
      }
    }
  }, [chart]);

  return (
    <div
      ref={containerRef}
      className="mermaid"
      style={{
        background: 'var(--surface)',
        padding: '16px',
        borderRadius: '6px',
        border: '1px solid var(--border)',
        marginBottom: '16px',
        overflow: 'auto',
        minHeight: '200px',
        display: 'flex',
        alignItems: 'center',
        justifyContent: 'center',
      }}
    >
      {chart}
    </div>
  );
};

export default Mermaid;
