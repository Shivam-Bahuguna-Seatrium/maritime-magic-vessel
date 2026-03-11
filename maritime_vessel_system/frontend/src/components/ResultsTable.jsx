import React from 'react';

/**
 * ResultsTable - Displays query results in a fixed scrollable table with analytics
 */
export default function ResultsTable({ data, title = 'Query Results' }) {
  if (!data || !Array.isArray(data) || data.length === 0) {
    return null;
  }

  // Get columns from first row
  const columns = Object.keys(data[0]);
  const rowCount = data.length;

  // Calculate analytics
  const analytics = {
    totalRows: rowCount,
    columns: columns.length,
    columnNames: columns,
  };

  return (
    <div style={{
      marginTop: 16,
      border: '1px solid var(--border)',
      borderRadius: 'var(--radius)',
      overflow: 'hidden',
      backgroundColor: 'var(--surface)',
    }} className="results-table">
      {/* Analytics Header */}
      <div style={{
        background: 'linear-gradient(135deg, #667eea15, #764ba215)',
        padding: 'clamp(8px, 2vw, 12px) clamp(12px, 3vw, 16px)',
        borderBottom: '1px solid var(--border)',
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        flexWrap: 'wrap',
        gap: 'clamp(8px, 3vw, 16px)',
      }}>
        <div style={{ display: 'flex', gap: 'clamp(12px, 4vw, 24px)' }}>
          <div>
            <div style={{ fontSize: '0.7rem', color: 'var(--text-muted)', textTransform: 'uppercase', fontWeight: 600 }}>
              📊 Total Rows
            </div>
            <div style={{ fontSize: 'clamp(1rem, 4vw, 1.3rem)', fontWeight: 700, color: 'var(--primary)' }}>
              {rowCount.toLocaleString()}
            </div>
          </div>
          <div>
            <div style={{ fontSize: '0.7rem', color: 'var(--text-muted)', textTransform: 'uppercase', fontWeight: 600 }}>
              📋 Columns
            </div>
            <div style={{ fontSize: 'clamp(1rem, 4vw, 1.3rem)', fontWeight: 700, color: 'var(--primary)' }}>
              {analytics.columns}
            </div>
          </div>
        </div>
        <div style={{ fontSize: 'clamp(0.7rem, 2vw, 0.8rem)', color: 'var(--text-muted)', minWidth: '100%', overflow: 'hidden', textOverflow: 'ellipsis' }}>
          <strong>Columns:</strong> {columns.map(c => `"${c}"`).join(', ')}
        </div>
      </div>

      {/* Scrollable Table */}
      <div style={{
        maxHeight: 'clamp(250px, 50vh, 500px)',
        overflowY: 'auto',
        overflowX: 'auto',
      }} className="results-table-scroll">
        <table style={{
          width: '100%',
          borderCollapse: 'collapse',
          fontSize: 'clamp(0.75rem, 2vw, 0.85rem)',
        }}>
          {/* Fixed Header */}
          <thead style={{
            position: 'sticky',
            top: 0,
            backgroundColor: '#334155',
            zIndex: 10,
            borderBottom: '2px solid var(--border)',
          }}>
            <tr>
              <th style={{
                padding: 'clamp(6px, 1.5vw, 10px) clamp(8px, 2vw, 12px)',
                textAlign: 'left',
                fontWeight: 600,
                color: 'var(--primary)',
                whiteSpace: 'nowrap',
                minWidth: '30px',
                backgroundColor: '#334155',
              }}>
                #
              </th>
              {columns.map(col => (
                <th
                  key={col}
                  style={{
                    padding: 'clamp(6px, 1.5vw, 10px) clamp(8px, 2vw, 12px)',
                    textAlign: 'left',
                    fontWeight: 600,
                    color: 'var(--primary)',
                    whiteSpace: 'nowrap',
                    minWidth: 'clamp(80px, 15vw, 120px)',
                    backgroundColor: '#334155',
                  }}
                  title={col}
                >
                  {col}
                </th>
              ))}
            </tr>
          </thead>

          {/* Table Body */}
          <tbody>
            {data.map((row, rowIdx) => (
              <tr
                key={rowIdx}
                style={{
                  borderBottom: '1px solid var(--border)',
                  backgroundColor: rowIdx % 2 === 0 ? '#1e293b' : '#0f172a',
                  transition: 'background-color 0.15s',
                }}
                onMouseEnter={(e) => e.currentTarget.style.backgroundColor = '#334155'}
                onMouseLeave={(e) => e.currentTarget.style.backgroundColor = rowIdx % 2 === 0 ? '#1e293b' : '#0f172a'}
              >
                {/* Row Number */}
                <td style={{
                  padding: 'clamp(6px, 1.5vw, 8px) clamp(8px, 2vw, 12px)',
                  fontWeight: 600,
                  color: 'var(--text-muted)',
                  whiteSpace: 'nowrap',
                  textAlign: 'center',
                }}>
                  {rowIdx + 1}
                </td>

                {/* Data Cells */}
                {columns.map(col => {
                  const value = row[col];
                  let displayValue = value;

                  // Format different data types
                  if (value === null || value === undefined) {
                    displayValue = <span style={{ color: 'var(--text-muted)', fontStyle: 'italic' }}>null</span>;
                  } else if (typeof value === 'number') {
                    displayValue = Number.isInteger(value)
                      ? value.toLocaleString()
                      : value.toFixed(2);
                  } else if (typeof value === 'boolean') {
                    displayValue = <span style={{ color: value ? '#10b981' : '#ef4444' }}>
                      {value ? '✓ true' : '✗ false'}
                    </span>;
                  } else if (typeof value === 'object') {
                    displayValue = JSON.stringify(value);
                  } else {
                    displayValue = String(value);
                  }

                  return (
                    <td
                      key={`${rowIdx}-${col}`}
                      style={{
                        padding: 'clamp(6px, 1.5vw, 8px) clamp(8px, 2vw, 12px)',
                        whiteSpace: 'nowrap',
                        overflow: 'hidden',
                        textOverflow: 'ellipsis',
                        maxWidth: 'clamp(80px, 15vw, 200px)',
                      }}
                      title={String(displayValue)}
                    >
                      {displayValue}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      {/* Footer Info */}
      <div style={{
        padding: 'clamp(8px, 2vw, 10px) clamp(12px, 3vw, 16px)',
        borderTop: '1px solid var(--border)',
        fontSize: 'clamp(0.65rem, 1.5vw, 0.75rem)',
        color: 'var(--text-muted)',
        background: '#0f172a',
        display: 'flex',
        justifyContent: 'space-between',
        flexWrap: 'wrap',
        gap: 8,
      }} className="results-table-footer">
        <span>Showing {rowCount.toLocaleString()} result{rowCount !== 1 ? 's' : ''}</span>
        <span>💾 Scroll to see more</span>
      </div>
    </div>
  );
}
