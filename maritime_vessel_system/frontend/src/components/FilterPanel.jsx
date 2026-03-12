import React, { useEffect, useState } from 'react';
import DEBUG from '../debug';

/**
 * FilterPanel – hierarchical dropdown filters for the Knowledge Graph.
 *
 * Filter chain: Category → Vessel Type → Flag → Validation Status
 */
export default function FilterPanel({ graphBuilt, filters, onChange }) {
  const [allOptions, setAllOptions] = useState({
    categories: [],
    vessel_types: [],
    vessel_names: [],
    flags: [],
    validation_statuses: [],
    vessel_count: 0,
  });
  const [filteredOptions, setFilteredOptions] = useState({
    categories: [],
    vessel_types: [],
    vessel_names: [],
    flags: [],
    validation_statuses: [],
  });

  useEffect(() => {
    if (!graphBuilt) return;
    DEBUG.log('FILTERPANEL', 'Loading filters...');
    DEBUG.api('GET', '/api/kg/filters');
    fetch('/api/kg/filters')
      .then(r => {
        DEBUG.apiResponse('GET', '/api/kg/filters', r.status);
        return r.json();
      })
      .then(data => {
        DEBUG.info('FILTERPANEL', 'Filters API response received', data);
        const newOptions = {
          categories: data.categories || [],
          vessel_types: data.vessel_types || [],
          vessel_names: data.vessel_names || [],
          flags: data.flags || [],
          validation_statuses: data.validation_statuses || [],
          vessel_count: data.vessel_count || 0,
        };
        DEBUG.log('FILTERPANEL', `Processed ${newOptions.categories.length} categories, ${newOptions.vessel_types.length} types`, newOptions);
        setAllOptions(newOptions);
        setFilteredOptions({
          categories: newOptions.categories,
          vessel_types: newOptions.vessel_types,
          vessel_names: newOptions.vessel_names,
          flags: newOptions.flags,
          validation_statuses: newOptions.validation_statuses,
        });
      })
      .catch((err) => {
        DEBUG.apiError('GET', '/api/kg/filters', err);
      });
  }, [graphBuilt]);

  // Update filtered options when filters change - cascade filtering
  useEffect(() => {
    console.log('[FilterPanel] Cascade filter update, current filters:', filters);
    
    let filteredTypes = allOptions.vessel_types;
    let filteredNames = allOptions.vessel_names;
    let filteredFlags = allOptions.flags;
    let filteredStatuses = allOptions.validation_statuses;

    // Fetch filtered options from backend based on current selections
    const fetchFilteredOptions = async () => {
      try {
        const params = new URLSearchParams();
        if (filters.category) params.append('category', filters.category);
        if (filters.vessel_type) params.append('vessel_type', filters.vessel_type);
        if (filters.vessel_name) params.append('vessel_name', filters.vessel_name);
        if (filters.flag) params.append('flag', filters.flag);
        
        // If we have any filters, fetch available options for next level
        if (params.toString()) {
          const res = await fetch(`/api/kg/filter-options?${params}`);
          if (res.ok) {
            const data = await res.json();
            console.log('[FilterPanel] Filtered options from backend:', data);
            filteredTypes = data.vessel_types || allOptions.vessel_types;
            filteredNames = data.vessel_names || allOptions.vessel_names;
            filteredFlags = data.flags || allOptions.flags;
            filteredStatuses = data.validation_statuses || allOptions.validation_statuses;
          }
        }
      } catch (err) {
        console.error('[FilterPanel] Error fetching filtered options:', err);
        // Fallback to all options if error
        filteredTypes = allOptions.vessel_types;
        filteredNames = allOptions.vessel_names;
        filteredFlags = allOptions.flags;
        filteredStatuses = allOptions.validation_statuses;
      }

      setFilteredOptions({
        categories: allOptions.categories, // Categories always show all available
        vessel_types: filteredTypes,
        vessel_names: filteredNames,
        flags: filteredFlags,
        validation_statuses: filteredStatuses,
      });
    };

    fetchFilteredOptions();
  }, [filters, allOptions]);

  const update = (key, value) => {
    const next = { ...filters, [key]: value || undefined };
    // Reset child filters when parent changes
    if (key === 'category') {
      delete next.vessel_type;
      delete next.vessel_name;
      delete next.flag;
      delete next.validation_status;
    } else if (key === 'vessel_type') {
      delete next.vessel_name;
      delete next.flag;
      delete next.validation_status;
    } else if (key === 'vessel_name') {
      delete next.flag;
      delete next.validation_status;
    } else if (key === 'flag') {
      delete next.validation_status;
    }
    onChange(next);
  };

  if (!graphBuilt) {
    return (
      <aside className="card filter-panel" style={{ width: 280, flexShrink: 0 }}>
        <p style={{ color: 'var(--text-muted)', fontSize: '0.85rem' }}>
          Build the knowledge graph first to enable filters.
        </p>
      </aside>
    );
  }

  const Tooltip = ({ label, tooltip, children }) => (
    <div style={{ display: 'flex', alignItems: 'center', gap: 6 }}>
      <label style={labelStyle}>
        {label}
        {children}
      </label>
      {tooltip && (
        <div className="tooltip-container" style={{ marginTop: 6 }}>
          <span>ⓘ</span>
          <span className="tooltip-text">{tooltip}</span>
        </div>
      )}
    </div>
  );

  return (
    <aside className="filter-panel-container card" style={{ 
      width: 280, 
      flexShrink: 0, 
      display: 'flex', 
      flexDirection: 'column', 
      gap: 12,
    }}>
      <h3 style={{ fontSize: '0.95rem', marginBottom: 4, display: 'flex', alignItems: 'center', gap: 8 }}>
        🔍 Filters 
        <span style={{ fontSize: '0.75rem', color: 'var(--text-muted)', fontWeight: 'normal' }}>
          ({allOptions.vessel_count} vessels)
        </span>
      </h3>

      {/* Category */}
      <Tooltip 
        label="Category"
        tooltip="Vessel classification category (e.g., Tanker, Cargo, Container)"
      >
        <select 
          value={filters.category || ''} 
          onChange={e => update('category', e.target.value)}
          title="Select vessel category"
        >
          <option value="">All Categories</option>
          {filteredOptions.categories.map(c => <option key={c} value={c}>{c}</option>)}
        </select>
      </Tooltip>

      {/* Vessel Type */}
      <Tooltip 
        label="Vessel Type"
        tooltip="Specific type of vessel (e.g., Bulk Carrier, Oil Tanker)"
      >
        <select 
          value={filters.vessel_type || ''} 
          onChange={e => update('vessel_type', e.target.value)}
          title="Select vessel type"
        >
          <option value="">All Types</option>
          {filteredOptions.vessel_types.map(t => <option key={t} value={t}>{t}</option>)}
        </select>
      </Tooltip>

      {/* Vessel Name */}
      <Tooltip 
        label="Vessel Name"
        tooltip="Search by specific vessel name"
      >
        <select 
          value={filters.vessel_name || ''} 
          onChange={e => update('vessel_name', e.target.value)}
          title="Select vessel name"
        >
          <option value="">All Vessels</option>
          {filteredOptions.vessel_names.map(n => <option key={n} value={n}>{n}</option>)}
        </select>
      </Tooltip>

      {/* Flag */}
      <Tooltip 
        label="Flag"
        tooltip="Country of vessel registration (flag state)"
      >
        <select 
          value={filters.flag || ''} 
          onChange={e => update('flag', e.target.value)}
          title="Select flag country"
        >
          <option value="">All Flags</option>
          {filteredOptions.flags.map(f => <option key={f} value={f}>{f}</option>)}
        </select>
      </Tooltip>

      {/* Validation Status */}
      <Tooltip 
        label="Validation Status"
        tooltip="Data quality status: valid or invalid records"
      >
        <select 
          value={filters.validation_status || ''} 
          onChange={e => update('validation_status', e.target.value)}
          title="Select validation status"
        >
          <option value="">All</option>
          {filteredOptions.validation_statuses.map(s => <option key={s} value={s}>{s}</option>)}
        </select>
      </Tooltip>

      {/* Reset */}
      <button
        style={{ marginTop: 8, background: 'var(--border)', color: 'var(--text)' }}
        onClick={() => onChange({})}
        title="Clear all filters and show all vessels"
      >
        ↻ Reset Filters
      </button>
    </aside>
  );
}

const labelStyle = {
  display: 'flex',
  flexDirection: 'column',
  gap: 4,
  fontSize: '0.82rem',
  color: 'var(--text-muted)',
};
