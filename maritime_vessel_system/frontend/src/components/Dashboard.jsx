import React, { useState, useEffect, useRef } from 'react';
import DEBUG from '../debug';

/**
 * Dashboard – main landing page.
 *
 * Lets the user:
 *  1. Upload or load a CSV dataset
 *  2. Run EDA analysis
 *  3. Run validation
 *  4. Build the Knowledge Graph (after validation)
 */

// Validation rule descriptions and icons
const VALIDATION_RULES = {
  'RULE_IMO_001': {
    name: 'IMO Format',
    icon: '🔢',
    description: 'Validates International Maritime Organization number format (7 digits)',
    tooltip: 'IMO is a unique 7-digit identifier for vessels'
  },
  'RULE_MMSI_001': {
    name: 'MMSI Format',
    icon: '📡',
    description: 'Validates Maritime Mobile Service Identity number format (9 digits)',
    tooltip: 'MMSI is a unique 9-digit code for radio communications'
  },
  'RULE_GEO_LAST_POSITION_LATITUDE': {
    name: 'Latitude Range',
    icon: '🗺️',
    description: 'Validates last known position latitude (-90 to 90)',
    tooltip: 'Latitude must be between -90° (South Pole) and 90° (North Pole)'
  },
  'RULE_GEO_LAST_POSITION_LONGITUDE': {
    name: 'Longitude Range',
    icon: '🗺️',
    description: 'Validates last known position longitude (-180 to 180)',
    tooltip: 'Longitude must be between -180° (West) and 180° (East)'
  },
  'RULE_TS_LAST_POSITION_UPDATETIMESTAMP': {
    name: 'Position Timestamp',
    icon: '⏰',
    description: 'Validates last position update timestamp is not in future',
    tooltip: 'Position timestamp should be from past or present'
  },
  'RULE_TS_STATICDATA_UPDATETIMESTAMP': {
    name: 'Static Data Timestamp',
    icon: '⏰',
    description: 'Validates static data update timestamp is not in future',
    tooltip: 'Static data timestamp should be from past or present'
  },
  'RULE_CONSIST_GROSSTONNAGE': {
    name: 'Gross Tonnage',
    icon: '⚖️',
    description: 'Validates gross tonnage is reasonable (> 0)',
    tooltip: 'Gross tonnage is the size of vessel in metric tons'
  },
  'RULE_CONSIST_LENGTH': {
    name: 'Vessel Length',
    icon: '📏',
    description: 'Validates vessel length is reasonable (> 0)',
    tooltip: 'Length is the overall dimension of the vessel'
  },
  'RULE_CONSIST_BUILTYEAR': {
    name: 'Build Year',
    icon: '📅',
    description: 'Validates build year is reasonable (between 1800 and current year)',
    tooltip: 'Year vessel was constructed or launched'
  },
};

export default function Dashboard({ status, refreshStatus }) {
  const [loading, setLoading] = useState('');
  const [analysisResult, setAnalysisResult] = useState(null);
  const [validationResult, setValidationResult] = useState(null);
  const [graphResult, setGraphResult] = useState(null);
  const [message, setMessage] = useState('');
  const [selectedFile, setSelectedFile] = useState(null);
  const [defaultFileName, setDefaultFileName] = useState(null);
  const fileRef = useRef();

  useEffect(() => { 
    refreshStatus();
    // Auto-load default CSV if not already loaded
    if (!status.dataset_loaded) {
      loadDefaultCSV();
    }
  }, [refreshStatus]);

  // ---- Auto-load default CSV ----
  const loadDefaultCSV = async () => {
    try {
      DEBUG.log('DASHBOARD', 'Loading default CSV...');
      // Try to load the default case study dataset
      DEBUG.api('GET', '/api/load-default');
      const response = await fetch('/api/load-default');
      DEBUG.apiResponse('GET', '/api/load-default', response.status);
      
      if (response.ok) {
        const data = await response.json();
        DEBUG.info('DASHBOARD', 'Default CSV loaded successfully', data);
        setDefaultFileName('case_study_dataset_202509152039.csv');
        setMessage(`📁 Auto-loaded: ${data.records} records with ${data.columns.length} columns.`);
        refreshStatus();
      } else {
        // If auto-load fails, show upload prompt
        DEBUG.warn('DASHBOARD', `Load-default returned ${response.status}`);
        const errorText = await response.text();
        DEBUG.error('DASHBOARD', 'Server response', errorText);
        setMessage('Upload a CSV file to get started.');
      }
    } catch (e) {
      DEBUG.apiError('GET', '/api/load-default', e);
      setMessage('Upload a CSV file to get started.');
    }
  };

  // ---- Handle File Selection ----
  const handleFileChange = (e) => {
    const file = e.target.files?.[0];
    if (file) {
      setSelectedFile(file);
      setMessage('');
    }
  };

  // ---- Clear Selected File ----
  const handleClearFile = () => {
    setSelectedFile(null);
    if (fileRef.current) {
      fileRef.current.value = '';
    }
  };

  // ---- Upload CSV ----
  const handleUpload = async () => {
    const file = selectedFile || fileRef.current?.files?.[0];
    if (!file) return;
    setLoading('upload');
    setMessage('');
    try {
      const form = new FormData();
      form.append('file', file);
      const res = await fetch('/api/upload', { method: 'POST', body: form });
      const data = await res.json();
      if (res.ok) {
        setMessage(`✅ Loaded ${data.records} records with ${data.columns.length} columns.`);
        setSelectedFile(null);
        setDefaultFileName(null);
        if (fileRef.current) fileRef.current.value = '';
      } else {
        setMessage(`Error: ${data.detail}`);
      }
    } catch (e) {
      setMessage(`Upload failed: ${e.message}`);
    }
    setLoading('');
    refreshStatus();
  };

  // ---- Run Analysis ----
  const handleAnalyze = async () => {
    setLoading('analyze');
    setMessage('');
    try {
      const res = await fetch('/api/analyze', { method: 'POST' });
      const data = await res.json();
      if (res.ok) {
        setAnalysisResult(data);
        setMessage('Analysis complete.');
      } else {
        setMessage(`Error: ${data.detail}`);
      }
    } catch (e) {
      setMessage(`Analysis failed: ${e.message}`);
    }
    setLoading('');
  };

  // ---- Run Validation ----
  const handleValidate = async () => {
    setLoading('validate');
    setMessage('');
    try {
      const res = await fetch('/api/validate', { method: 'POST' });
      const data = await res.json();
      if (res.ok) {
        setValidationResult(data);
        setMessage('Validation complete.');
      } else {
        setMessage(`Error: ${data.detail}`);
      }
    } catch (e) {
      setMessage(`Validation failed: ${e.message}`);
    }
    setLoading('');
    refreshStatus();
  };

  // ---- Build Knowledge Graph ----
  const handleBuildGraph = async () => {
    setLoading('graph');
    setMessage('');
    try {
      const res = await fetch('/api/build-graph', { method: 'POST' });
      const data = await res.json();
      if (res.ok) {
        setGraphResult(data);
        setMessage(`Graph built: ${data.nodes_created} nodes, ${data.relationships_created} relationships.`);
      } else {
        setMessage(`Error: ${data.detail}`);
      }
    } catch (e) {
      setMessage(`Graph build failed: ${e.message}`);
    }
    setLoading('');
    refreshStatus();
  };

  return (
    <div style={{ maxWidth: 960, margin: '0 auto', padding: '0 clamp(8px, 3vw, 16px)' }} className="dashboard-container">
      <h2 style={{ marginBottom: 20, fontSize: 'clamp(1.1rem, 4vw, 1.3rem)' }}>Dashboard</h2>

      {/* --- Step 1: Upload --- */}
      <div className="card" style={{ marginBottom: 16 }}>
        <h3 style={{ marginBottom: 10, fontSize: 'clamp(0.95rem, 3vw, 1rem)' }} title="Upload your vessel data in CSV format">
          📁 1. Upload Dataset
        </h3>
        <p style={{ fontSize: 'clamp(0.8rem, 2.5vw, 0.82rem)', color: 'var(--text-muted)', marginBottom: 10 }} title="Select a CSV file containing vessel information">
          Choose a CSV file with vessel data (e.g., IMO, MMSI, vessel names, types, flags, etc.)
        </p>
        <div style={{ display: 'flex', gap: 10, alignItems: 'center', flexWrap: 'wrap' }} className="upload-group">
          <input 
            type="file" 
            accept=".csv" 
            ref={fileRef} 
            onChange={handleFileChange}
            title="Select CSV file from your computer"
            style={{ flex: 1, minWidth: '150px', fontSize: 'clamp(0.8rem, 2vw, 0.9rem)' }}
          />
          
          {/* Show default or selected file with clear button */}
          {(selectedFile || defaultFileName) && (
            <div style={{
              display: 'flex',
              alignItems: 'center',
              gap: '8px',
              background: 'rgba(16, 185, 129, 0.1)',
              border: '1px solid rgb(16, 185, 129)',
              borderRadius: '4px',
              padding: '6px 10px',
              fontSize: 'clamp(0.8rem, 2.5vw, 0.9rem)',
              color: 'rgb(16, 185, 129)',
              fontWeight: 500,
              whiteSpace: 'nowrap',
            }} className="upload-file-badge">
              <span>✓ {selectedFile?.name || defaultFileName}</span>
              <button
                onClick={handleClearFile}
                title="Clear selected file"
                style={{
                  background: 'transparent',
                  border: 'none',
                  color: 'rgb(16, 185, 129)',
                  cursor: 'pointer',
                  fontSize: '1rem',
                  padding: '0',
                  display: 'flex',
                  alignItems: 'center',
                  justifyContent: 'center',
                  width: '20px',
                  height: '20px',
                  borderRadius: '50%',
                  transition: 'all 0.2s',
                }}
                onMouseEnter={(e) => {
                  e.currentTarget.style.background = 'rgba(16, 185, 129, 0.2)';
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.background = 'transparent';
                }}
              >
                ✕
              </button>
            </div>
          )}
          
          <button 
            className="primary" 
            onClick={handleUpload} 
            disabled={loading === 'upload' || (!selectedFile && !defaultFileName)}
            title={selectedFile || defaultFileName ? "Upload selected CSV file to the system" : "Select a file first"}
            style={{
              opacity: (selectedFile || defaultFileName) ? 1 : 0.6,
              cursor: (selectedFile || defaultFileName) ? 'pointer' : 'not-allowed',
            }}
          >
            {loading === 'upload' ? 'Uploading…' : '↑ Upload CSV'}
          </button>
        </div>
      </div>

      {/* --- Step 2: Analyze --- */}
      <div className="card" style={{ marginBottom: 16 }}>
        <h3 style={{ marginBottom: 10, fontSize: 'clamp(0.95rem, 3vw, 1rem)' }} title="Analyze dataset structure and content">
          📊 2. Run Analysis
        </h3>
        <p style={{ fontSize: 'clamp(0.8rem, 2.5vw, 0.82rem)', color: 'var(--text-muted)', marginBottom: 10 }} title="Generates statistical analysis of your data">
          Run Exploratory Data Analysis (EDA) to understand distribution, missing values, and patterns
        </p>
        <button
          className="primary"
          onClick={handleAnalyze}
          disabled={!status.dataset_loaded || loading === 'analyze'}
          title="Analyze dataset for patterns, distributions, and statistics"
        >
          {loading === 'analyze' ? 'Analyzing…' : '📈 Run EDA Analysis'}
        </button>

        {analysisResult && (
          <div style={{ marginTop: 12, fontSize: 'clamp(0.8rem, 2vw, 0.85rem)' }}>
            <table style={{ width: '100%', borderCollapse: 'collapse' }}>
              <tbody>
                {Object.entries(analysisResult).filter(([k]) => typeof analysisResult[k] !== 'object').map(([k, v]) => (
                  <tr key={k} style={{ borderBottom: '1px solid var(--border)' }}>
                    <td style={{ padding: '4px 8px', color: 'var(--text-muted)' }}>{k}</td>
                    <td style={{ padding: '4px 8px', fontWeight: 600 }}>{v}</td>
                  </tr>
                ))}
              </tbody>
            </table>

            {analysisResult.vessel_types && (
              <div style={{ marginTop: 10 }}>
                <strong>Vessel Type Distribution:</strong>
                <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6, marginTop: 6 }}>
                  {Object.entries(analysisResult.vessel_types).map(([t, c]) => (
                    <span key={t} className="badge valid" style={{ background: '#1e3a5f', color: '#93c5fd' }}>
                      {t}: {c}
                    </span>
                  ))}
                </div>
              </div>
            )}
          </div>
        )}
      </div>

      {/* --- Step 3: Validate --- */}
      <div className="card" style={{ marginBottom: 16 }}>
        <h3 style={{ marginBottom: 10, fontSize: 'clamp(0.95rem, 3vw, 1rem)' }} title="Check data quality and integrity">
          ✓ 3. Run Validation
        </h3>
        <p style={{ fontSize: 'clamp(0.8rem, 2.5vw, 0.82rem)', color: 'var(--text-muted)', marginBottom: 10 }} title="Validates IMO, MMSI, coordinates, and other vessel information">
          Validates data quality (IMO/MMSI format, coordinates, vessel types, duplicates, anomalies)
        </p>
        <button
          className="primary"
          onClick={handleValidate}
          disabled={!status.dataset_loaded || loading === 'validate'}
          title="Run data validation checks to identify quality issues"
        >
          {loading === 'validate' ? 'Validating…' : '🔍 Run Validation'}
        </button>

        {validationResult && (
          <div style={{ marginTop: 12, fontSize: 'clamp(0.8rem, 2vw, 0.85rem)' }}>
            <div style={{ display: 'flex', gap: 16, marginBottom: 14, flexWrap: 'wrap' }}>
              <div title="Total validation checks performed">
                <span style={{ color: 'var(--text-muted)' }}>📊 Total: </span>
                <strong style={{ fontSize: '1.1rem' }}>{validationResult.total_validations}</strong>
              </div>
              <div title="Records that passed all applicable validation checks">
                <span style={{ color: 'var(--success)' }}>✓ Passed: </span>
                <strong style={{ fontSize: '1.1rem', color: 'var(--success)' }}>{validationResult.passed}</strong>
              </div>
              <div title="Records that failed one or more validation checks">
                <span style={{ color: 'var(--danger)' }}>✗ Failed: </span>
                <strong style={{ fontSize: '1.1rem', color: 'var(--danger)' }}>{validationResult.failed}</strong>
              </div>
            </div>

            {validationResult.by_rule && (
              <div style={{ marginTop: 12 }}>
                <p style={{ fontSize: '0.78rem', color: 'var(--text-muted)', marginBottom: 8 }}>📋 Validation Rule Details (hover for descriptions):</p>
                <table style={{ width: '100%', borderCollapse: 'collapse' }}>
                  <thead>
                    <tr style={{ borderBottom: '2px solid var(--border)' }}>
                      <th style={{ textAlign: 'left', padding: '6px 4px', color: 'var(--text-muted)', fontSize: '0.75rem', fontWeight: 600 }}>Rule</th>
                      <th style={{ textAlign: 'right', padding: '6px 4px', color: 'var(--success)', fontSize: '0.75rem', fontWeight: 600 }}>✓ Passed</th>
                      <th style={{ textAlign: 'right', padding: '6px 4px', color: 'var(--danger)', fontSize: '0.75rem', fontWeight: 600 }}>✗ Failed</th>
                    </tr>
                  </thead>
                  <tbody>
                    {Object.entries(validationResult.by_rule).map(([rule, counts]) => {
                      const ruleInfo = VALIDATION_RULES[rule] || { 
                        name: rule.replace('RULE_', '').replace(/_/g, ' '), 
                        icon: '✓',
                        tooltip: rule
                      };
                      const passRate = counts.passed + counts.failed > 0 
                        ? Math.round((counts.passed / (counts.passed + counts.failed)) * 100) 
                        : 0;

                      return (
                        <tr key={rule} style={{ borderBottom: '1px solid var(--border)', background: counts.failed > 0 ? 'rgba(239, 68, 68, 0.05)' : 'transparent' }}>
                          <td 
                            style={{ 
                              padding: '8px 4px', 
                              fontFamily: 'monospace', 
                              fontSize: '0.75rem',
                              cursor: 'help',
                            }}
                            title={ruleInfo.tooltip}
                          >
                            <span style={{ marginRight: 6 }}>{ruleInfo.icon}</span>
                            <strong>{ruleInfo.name}</strong>
                            <div style={{ fontSize: '0.7rem', color: 'var(--text-muted)', marginTop: 2 }}>
                              {ruleInfo.description}
                            </div>
                          </td>
                          <td style={{ padding: '8px 4px', textAlign: 'right', color: 'var(--success)', fontWeight: 600 }}>
                            {counts.passed}
                          </td>
                          <td style={{ 
                            padding: '8px 4px', 
                            textAlign: 'right', 
                            color: counts.failed > 0 ? 'var(--danger)' : 'var(--success)',
                            fontWeight: 600
                          }}>
                            {counts.failed}
                            <div style={{ fontSize: '0.7rem', color: 'var(--text-muted)' }}>
                              ({passRate}% pass)
                            </div>
                          </td>
                        </tr>
                      );
                    })}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        )}
      </div>

      {/* --- Step 4: Build Knowledge Graph --- */}
      <div className="card" style={{ marginBottom: 16 }}>
        <h3 style={{ marginBottom: 10, fontSize: 'clamp(0.95rem, 3vw, 1rem)' }} title="Create graph database for vessel relationships">
          🔗 4. Build Knowledge Graph
        </h3>
        <p style={{ fontSize: 'clamp(0.8rem, 2.5vw, 0.82rem)', color: 'var(--text-muted)', marginBottom: 10 }} title="Creates relationships between vessels, ports, flags, and types">
          Creates a knowledge graph database showing vessel relationships, identifiers, and attributes. <span style={{ color: 'var(--success)' }}>Green</span> nodes = valid data, <span style={{ color: 'var(--danger)' }}>red</span> nodes = validation failures.
        </p>
        <button
          className="primary"
          onClick={handleBuildGraph}
          disabled={!status.validated || loading === 'graph'}
          title={status.validated ? 'Build knowledge graph with validated data' : 'Run validation first'}
        >
          {loading === 'graph' ? 'Building…' : '🏗️ Build Knowledge Graph'}
        </button>

        {graphResult && (
          <div style={{ marginTop: 12, fontSize: 'clamp(0.8rem, 2vw, 0.85rem)' }}>
            <span className="badge valid">
              {graphResult.nodes_created} nodes, {graphResult.relationships_created} relationships
            </span>
          </div>
        )}
      </div>

      {/* --- Message --- */}
      {message && (
        <div style={{
          padding: '10px 16px',
          background: message.includes('Error') ? '#450a0a' : '#064e3b',
          border: `1px solid ${message.includes('Error') ? '#dc2626' : '#16a34a'}`,
          borderRadius: 'var(--radius)',
          fontSize: 'clamp(0.8rem, 2vw, 0.85rem)',
        }}>
          {message}
        </div>
      )}
    </div>
  );
}
