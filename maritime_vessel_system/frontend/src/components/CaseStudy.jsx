import React, { useState, useEffect } from 'react';
import Mermaid from './Mermaid';

const CaseStudy = () => {
  const [expandedSection, setExpandedSection] = useState('introduction');

  const styles = {
    container: {
      maxWidth: '1200px',
      margin: '0 auto',
      padding: 'clamp(16px, 5vw, 24px)',
      fontFamily: 'var(--font-sans)',
      color: 'var(--text)',
      overflowY: 'auto',
      maxHeight: 'calc(100vh - 100px)',
      lineHeight: '1.6',
    },
    header: {
      textAlign: 'center',
      marginBottom: 'clamp(20px, 5vw, 32px)',
      borderBottom: '2px solid var(--primary)',
      paddingBottom: '16px',
    },
    mainTitle: {
      fontSize: 'clamp(1.5rem, 6vw, 2.2rem)',
      fontWeight: 700,
      marginBottom: '8px',
      color: 'var(--text)',
    },
    subtitle: {
      fontSize: 'clamp(0.85rem, 2.5vw, 1rem)',
      color: 'var(--text-muted)',
      fontStyle: 'italic',
    },
    toc: {
      background: 'var(--surface)',
      border: '1px solid var(--border)',
      borderRadius: '8px',
      padding: 'clamp(12px, 3vw, 24px)',
      marginBottom: 'clamp(20px, 5vw, 32px)',
    },
    tocTitle: {
      fontSize: 'clamp(0.95rem, 3vw, 1.1rem)',
      fontWeight: 600,
      marginBottom: '12px',
      color: 'var(--text)',
    },
    tocList: {
      listStyle: 'none',
      padding: 0,
      margin: 0,
      display: 'grid',
      gridTemplateColumns: 'repeat(auto-fit, minmax(200px, 1fr))',
      gap: '8px',
    },
    tocItem: {
      padding: '8px 12px',
      cursor: 'pointer',
      borderRadius: '4px',
      transition: 'background 0.2s',
      fontSize: 'clamp(0.85rem, 2.5vw, 0.95rem)',
    },
    tocItemActive: {
      background: 'var(--primary)',
      color: '#fff',
    },
    section: {
      marginBottom: 'clamp(25px, 5vw, 40px)',
    },
    sectionHeader: {
      display: 'flex',
      alignItems: 'center',
      gap: '12px',
      cursor: 'pointer',
      marginBottom: '16px',
      padding: 'clamp(8px, 2vw, 12px) clamp(12px, 3vw, 16px)',
      background: 'var(--surface)',
      borderRadius: '6px',
      border: '1px solid var(--border)',
      transition: 'all 0.2s',
      flexWrap: 'wrap',
    },
    sectionHeaderHover: {
      background: 'var(--surface)',
      borderColor: 'var(--primary)',
    },
    sectionTitle: {
      fontSize: 'clamp(1.2rem, 4vw, 1.5rem)',
      fontWeight: 700,
      color: 'var(--text)',
      flex: 1,
      minWidth: '200px',
    },
    toggleIcon: {
      fontSize: '1.2rem',
      transition: 'transform 0.2s',
    },
    sectionContent: {
      paddingLeft: 'clamp(8px, 2vw, 16px)',
      paddingRight: 'clamp(8px, 2vw, 16px)',
    },
    subsection: {
      marginBottom: '24px',
    },
    subsectionTitle: {
      fontSize: 'clamp(1rem, 3vw, 1.15rem)',
      fontWeight: 600,
      color: 'var(--primary)',
      marginBottom: '8px',
      marginTop: '16px',
    },
    paragraph: {
      lineHeight: '1.6',
      marginBottom: '12px',
      color: 'var(--text)',
      fontSize: 'clamp(0.85rem, 2.5vw, 0.95rem)',
    },
    list: {
      marginLeft: 'clamp(12px, 4vw, 24px)',
      marginBottom: '16px',
    },
    listItem: {
      marginBottom: '8px',
      lineHeight: '1.6',
      color: 'var(--text)',
      fontSize: 'clamp(0.85rem, 2.5vw, 0.95rem)',
    },
    codeBlock: {
      background: '#1e1e1e',
      color: '#d4d4d4',
      padding: 'clamp(12px, 3vw, 16px)',
      borderRadius: '6px',
      borderLeft: '4px solid var(--primary)',
      overflow: 'auto',
      fontSize: 'clamp(0.75rem, 2vw, 0.85rem)',
      fontFamily: 'Courier New, monospace',
      marginBottom: '16px',
      lineHeight: '1.4',
    },
    diagram: {
      background: 'var(--surface)',
      border: '1px solid var(--border)',
      padding: 'clamp(12px, 3vw, 16px)',
      borderRadius: '6px',
      fontFamily: 'Courier New, monospace',
      fontSize: 'clamp(0.75rem, 2vw, 0.85rem)',
      marginBottom: '16px',
      whiteSpace: 'pre-wrap',
      color: 'var(--text-muted)',
      overflow: 'auto',
    },
    highlightBox: {
      background: 'rgba(88, 166, 255, 0.1)',
      border: '1px solid var(--primary)',
      borderRadius: '6px',
      padding: '16px',
      marginBottom: '16px',
    },
    highlightTitle: {
      fontWeight: 600,
      color: 'var(--primary)',
      marginBottom: '8px',
    },
    flowChart: {
      background: 'var(--surface)',
      border: '2px solid var(--border)',
      borderRadius: '8px',
      padding: '24px',
      marginBottom: '24px',
      textAlign: 'center',
      fontFamily: 'Courier New, monospace',
      color: 'var(--text-muted)',
      fontSize: '0.9rem',
      whiteSpace: 'pre-wrap',
    },
  };

  const sections = [
    {
      id: 'introduction',
      label: 'Introduction',
      icon: '📖',
    },
    {
      id: 'overview',
      label: 'Overview',
      icon: '🎯',
    },
    {
      id: 'problem',
      label: 'Problem Statement',
      icon: '⚠️',
    },
    {
      id: 'questions',
      label: 'Key Questions',
      icon: '❓',
    },
    {
      id: 'exercise',
      label: 'Exercise',
      icon: '✏️',
    },
    {
      id: 'instructions',
      label: 'Instructions',
      icon: '📋',
    },
    {
      id: 'architecture',
      label: 'System Architecture',
      icon: '🏗️',
    },
    {
      id: 'implementation',
      label: 'Implementation Details',
      icon: '⚙️',
    },
    {
      id: 'endgoal',
      label: 'End Goal',
      icon: '🎁',
    },
  ];

  const toggleSection = (id) => {
    setExpandedSection(expandedSection === id ? null : id);
  };

  const SectionRenderer = ({ id, title, children }) => {
    const isExpanded = expandedSection === id;
    return (
      <div style={styles.section}>
        <div
          style={{
            ...styles.sectionHeader,
            ...(isExpanded ? styles.sectionHeaderHover : {}),
          }}
          onClick={() => toggleSection(id)}
          onMouseEnter={(e) => {
            e.currentTarget.style.borderColor = 'var(--primary)';
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.borderColor = 'var(--border)';
          }}
        >
          <span style={{ fontSize: '1.8rem' }}>
            {sections.find((s) => s.id === id)?.icon}
          </span>
          <h2 style={styles.sectionTitle}>{title}</h2>
          <span
            style={{
              ...styles.toggleIcon,
              transform: isExpanded ? 'rotate(180deg)' : 'rotate(0deg)',
            }}
          >
            ▼
          </span>
        </div>
        {isExpanded && <div style={styles.sectionContent}>{children}</div>}
      </div>
    );
  };

  return (
    <div style={styles.container}>
      <div style={styles.header}>
        <h1 style={styles.mainTitle}>🚢 Case Study: Vessel Identification & AI Agent</h1>
        <p style={styles.subtitle}>
          A comprehensive system design for maritime vessel identity resolution with AI-powered retrieval
        </p>
      </div>

      <div style={styles.toc}>
        <h3 style={styles.tocTitle}>Table of Contents</h3>
        <ul style={styles.tocList}>
          {sections.map((section) => (
            <li
              key={section.id}
              style={{
                ...styles.tocItem,
                ...(expandedSection === section.id ? styles.tocItemActive : {}),
              }}
              onMouseEnter={(e) => {
                e.currentTarget.style.background =
                  expandedSection === section.id
                    ? 'var(--primary)'
                    : 'var(--border)';
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.background = expandedSection === section.id ? 'var(--primary)' : 'transparent';
              }}
              onClick={() => toggleSection(section.id)}
            >
              {section.icon} {section.label}
            </li>
          ))}
        </ul>
      </div>

      {/* Introduction Section */}
      <SectionRenderer
        id="introduction"
        title="Introduction"
      >
        <div style={styles.sectionContent}>
          <p style={styles.paragraph}>
            Alright, so here's the thing. We're dealing with vessel data - it's messy, inconsistent, and spread across different systems. 
            This case study is about really thinking through how you'd build an AI system to figure out what vessel is what, 
            even when the data's all over the place.
          </p>

          <div style={styles.subsection}>
            <h4 style={styles.subsectionTitle}>What You'll Actually Need to Show:</h4>
            <ul style={styles.list}>
              <li style={styles.listItem}>
                <strong>Data Engineering:</strong> How do you actually clean up messy maritime data?
              </li>
              <li style={styles.listItem}>
                <strong>AI Problem Solving:</strong> When you have 3 records that might be the same ship, how do you figure it out?
              </li>
              <li style={styles.listItem}>
                <strong>System Thinking:</strong> What tools would actually work? Neo4j? PostgreSQL? Vector databases?
              </li>
              <li style={styles.listItem}>
                <strong>Communication:</strong> Can you explain your choices clearly? With code examples, diagrams, the whole thing.
              </li>
            </ul>
          </div>
        </div>
      </SectionRenderer>

      {/* Overview Section */}
      <SectionRenderer id="overview" title="Overview">
        <div style={styles.sectionContent}>
          <p style={styles.paragraph}>
            OK so basically we need to solve the "which ship is which" problem. We've got vessel data from two main sources, 
            and they don't always agree on who's who. That's what we're dealing with here.
          </p>

          <div style={styles.subsection}>
            <h4 style={styles.subsectionTitle}>Where the Data Comes From</h4>
            <ul style={styles.list}>
              <li style={styles.listItem}>
                <strong>Registry Data:</strong> Static stuff like the vessel type, what country it's registered in, who owns it, that kind of thing
              </li>
              <li style={styles.listItem}>
                <strong>AIS Data:</strong> Real-time updates on where the ship is, what route it's taking, current voyage info
              </li>
            </ul>
          </div>

          <div style={styles.subsection}>
            <h4 style={styles.subsectionTitle}>The Identifiers We Use</h4>
            <Mermaid chart={`graph LR
    A["Vessel Identity"] --> B["IMO Number<br/>(Should be unique)"]
    A --> C["MMSI Number<br/>(Can change over time)"]
    B --> D["7 digits<br/>Rarely changes"]
    C --> E["9 digits<br/>Changes when ownership shifts"]
    style A fill:#3b82f6,stroke:#1e40af,color:#fff
    style B fill:#10b981,stroke:#065f46,color:#fff
    style C fill:#f59e0b,stroke:#92400e,color:#fff`}
            />
          </div>

          <div style={styles.highlightBox}>
            <div style={styles.highlightTitle}>💡 The Big Challenge</div>
            <p style={styles.paragraph}>
              Two systems tracking the same ship but using different data. Sometimes they agree, sometimes they don't. 
              We need to figure out when they're talking about the same vessel.
            </p>
          </div>
        </div>
      </SectionRenderer>

      {/* Problem Statement Section */}
      <SectionRenderer id="problem" title="Problem Statement">
        <div style={styles.sectionContent}>
          <p style={styles.paragraph}>
            Real vessel data is a mess. Here's what we're actually dealing with:
          </p>

          <Mermaid chart={`graph TB
    A["Reality Check:<br/>Raw Vessel Data"] --> B["🔴 Bad IMO Numbers<br/>Duplicates, Invalid Formats"]
    A --> C["🔴 Changing MMSIs<br/>When ownership changes"]
    A --> D["🔴 Name Variations<br/>Same ship, different names"]
    A --> E["🔴 One-to-Many Issues<br/>1 IMO → multiple MMSIs"]
    A --> F["🔴 Many-to-One Issues<br/>Multiple IMOs → 1 MMSI"]
    style A fill:#ef4444,stroke:#991b1b,color:#fff
    style B fill:#dc2626,stroke:#7f1d1d,color:#fff
    style C fill:#dc2626,stroke:#7f1d1d,color:#fff
    style D fill:#dc2626,stroke:#7f1d1d,color:#fff
    style E fill:#dc2626,stroke:#7f1d1d,color:#fff
    style F fill:#dc2626,stroke:#7f1d1d,color:#fff`}
          />

          <div style={styles.subsection}>
            <h4 style={styles.subsectionTitle}>What We Need to Do</h4>
            <ul style={styles.list}>
              <li style={styles.listItem}>
                <strong>Figure out which records are the same ship.</strong> Even when the data disagrees.
              </li>
              <li style={styles.listItem}>
                <strong>Clean up the conflicts.</strong> Pick the most likely identity.
              </li>
              <li style={styles.listItem}>
                <strong>Build a searchable system.</strong> So people can actually find vessels reliably.
              </li>
            </ul>
          </div>
        </div>
      </SectionRenderer>

      {/* Key Questions Section */}
      <SectionRenderer id="questions" title="Key Questions">
        <div style={styles.sectionContent}>
          <p style={styles.paragraph}>
            These are the real questions you need to answer when building this system:
          </p>

          <ol style={styles.list}>
            <li style={styles.listItem}>
              <strong>How do you actually tell if two records are the same ship?</strong>
              <p style={{ marginTop: '4px', color: 'var(--text-muted)', fontSize: '0.9rem' }}>
                Look at name similarity, IMO/MMSI matches, vessel type, age... basically a fuzzy matching problem.
              </p>
            </li>
            <li style={styles.listItem}>
              <strong>What records are clearly broken?</strong>
              <p style={{ marginTop: '4px', color: 'var(--text-muted)', fontSize: '0.9rem' }}>
                IMO with wrong number of digits? MMSI that doesn't make sense? Flag that doesn't exist? Mark them.
              </p>
            </li>
            <li style={styles.listItem}>
              <strong>How do you track when a ship changes names or ownership?</strong>
              <p style={{ marginTop: '4px', color: 'var(--text-muted)', fontSize: '0.9rem' }}>
                Keep timestamps. Build a timeline. Show the history.
              </p>
            </li>
            <li style={styles.listItem}>
              <strong>Can you actually build a "correct" vessel database?</strong>
              <p style={{ marginTop: '4px', color: 'var(--text-muted)', fontSize: '0.9rem' }}>
                Yeah. Get humans to review conflicts, build consensus, trust the signals.
              </p>
            </li>
            <li style={styles.listItem}>
              <strong>What's the actual architecture?</strong>
              <p style={{ marginTop: '4px', color: 'var(--text-muted)', fontSize: '0.9rem' }}>
                Neo4j for relationships. Structured queries. Maybe vector search. Smart caching.
              </p>
            </li>
            <li style={styles.listItem}>
              <strong>How does a chatbot fit into all this?</strong>
              <p style={{ marginTop: '4px', color: 'var(--text-muted)', fontSize: '0.9rem' }}>
                LLM converts natural language to database queries. Talks back in plain English.
              </p>
            </li>
            <li style={styles.listItem}>
              <strong>How do you stop the AI from making stuff up?</strong>
              <p style={{ marginTop: '4px', color: 'var(--text-muted)', fontSize: '0.9rem' }}>
                Ground everything in actual data. Validation checks. Ask the database to verify.
              </p>
            </li>
            <li style={styles.listItem}>
              <strong>How do you measure if it actually works?</strong>
              <p style={{ marginTop: '4px', color: 'var(--text-muted)', fontSize: '0.9rem' }}>
                Accuracy metrics, user feedback, how fast responses come back.
              </p>
            </li>
          </ol>
        </div>
      </SectionRenderer>

      {/* Exercise Section */}
      <SectionRenderer id="exercise" title="Exercise">
        <div style={styles.sectionContent}>
          <p style={styles.paragraph}>
            We've given you vessel data. Here's what you need to do with it:
          </p>

          <ul style={styles.list}>
            <li style={styles.listItem}>
              ✅ <strong>Look at the data</strong> - Figure out what each column means
            </li>
            <li style={styles.listItem}>
              ✅ <strong>Write some code to match vessels</strong> - Fuzzy matching, scoring, all that
            </li>
            <li style={styles.listItem}>
              ✅ <strong>Draw the system</strong> - How would data flow from CSV to chatbot?
            </li>
            <li style={styles.listItem}>
              ✅ <strong>Pick your tools</strong> - Database? API? Why that choice?
            </li>
            <li style={styles.listItem}>
              ✅ <strong>Show how the AI actually works</strong> - User asks → LLM thinks → Queries DB → Returns answer
            </li>
            <li style={styles.listItem}>
              ✅ <strong>Implement caching</strong> - Same query asked twice? Use the cached result
            </li>
          </ul>

          <div style={styles.highlightBox}>
            <div style={styles.highlightTitle}>📊 You Get This Dataset:</div>
            <p style={styles.paragraph}>
              <code>case_study_dataset_202509152039.csv</code> - Lot of vessel records with IMO, MMSI, names, types, countries, sizes, etc
            </p>
          </div>
        </div>
      </SectionRenderer>

      {/* Instructions Section */}
      <SectionRenderer id="instructions" title="Instructions">
        <div style={styles.sectionContent}>
          <div style={styles.subsection}>
            <h4 style={styles.subsectionTitle}>Tech Stack</h4>
            <ul style={styles.list}>
              <li style={styles.listItem}>
                <strong>Backend:</strong> Python (obviously)
              </li>
              <li style={styles.listItem}>
                <strong>Frontend:</strong> React - what you're looking at right now
              </li>
              <li style={styles.listItem}>
                <strong>Database:</strong> Neo4j - for the graph 
              </li>
              <li style={styles.listItem}>
                <strong>AI:</strong> OpenAI or whatever LLM you want
              </li>
            </ul>
          </div>

          <div style={styles.subsection}>
            <h4 style={styles.subsectionTitle}>Just Focus On</h4>
            <ul style={styles.list}>
              <li style={styles.listItem}>Good design, not a perfect implementation</li>
              <li style={styles.listItem}>Showing your thinking process</li>
              <li style={styles.listItem}>Real code samples that actually work</li>
              <li style={styles.listItem}>Clear diagrams and explanations</li>
            </ul>
          </div>

          <div style={styles.subsection}>
            <h4 style={styles.subsectionTitle}>What You Need to Deliver</h4>
            <ul style={styles.list}>
              <li style={styles.listItem}>✓ A design doc explaining your choices</li>
              <li style={styles.listItem}>✓ Diagrams (Mermaid, draw.io, whatever) showing how it works</li>
              <li style={styles.listItem}>✓ Real code samples that show the concepts</li>
              <li style={styles.listItem}>✓ A README that explains everything</li>
              <li style={styles.listItem}>✓ A working prototype showing the core ideas</li>
            </ul>
          </div>
        </div>
      </SectionRenderer>

      {/* System Architecture Section */}
      <SectionRenderer id="architecture" title="System Architecture">
        <div style={styles.sectionContent}>
          <div style={styles.subsection}>
            <h4 style={styles.subsectionTitle}>How It All Fits Together</h4>
            <Mermaid chart={`graph TD
    A["User Types Question<br/>in Chat"] -->|Natural Language| B["LLM<br/>GPT-4, Claude, etc"]
    B -->|Understands Intent| C["Agent System<br/>Routes to right tool"]
    C -->|Queries| D["Neo4j<br/>Knowledge Graph"]
    C -->|Validates| E["RAG Guard<br/>Fact Checking"]
    D -->|Returns Data| E
    E -->|Fact-checked| F["LLM Replies<br/>in English"]
    F -->|Answer| A
    
    H["Data Pipeline"] -->|Ingest| I["CSV Data"]
    I -->|Validate| J["Validation Rules<br/>Format checks"]
    J -->|Deduplicate| K["Entity Resolver<br/>Fuzzy matching"]
    K -->|Build| D
    
    style A fill:#3b82f6,stroke:#1e40af,color:#fff
    style B fill:#8b5cf6,stroke:#5b21b6,color:#fff
    style C fill:#8b5cf6,stroke:#5b21b6,color:#fff
    style D fill:#10b981,stroke:#065f46,color:#fff
    style E fill:#f59e0b,stroke:#92400e,color:#fff
    style F fill:#3b82f6,stroke:#1e40af,color:#fff`}
            />
          </div>

          <div style={styles.subsection}>
            <h4 style={styles.subsectionTitle}>Each Piece Does This:</h4>
            <ul style={styles.list}>
              <li style={styles.listItem}>
                <strong>Validation Pipeline:</strong> "Is this IMO number actually valid? Does it have 7 digits?"
              </li>
              <li style={styles.listItem}>
                <strong>Entity Resolver:</strong> "Records X and Y look like the same ship, should we merge them?"
              </li>
              <li style={styles.listItem}>
                <strong>Knowledge Graph (Neo4j):</strong> Stores all the ships and how they're related
              </li>
              <li style={styles.listItem}>
                <strong>LLM + Agents:</strong> Figures out what the user is actually asking for
              </li>
              <li style={styles.listItem}>
                <strong>RAG Guard:</strong> Makes sure the answer is actually based on real data, not hallucinated
              </li>
            </ul>
          </div>

          <div style={styles.subsection}>
            <h4 style={styles.subsectionTitle}>Data Workflow</h4>
            <Mermaid chart={`graph LR
    A["CSV File"] -->|Parse| B["Raw Records"]
    B -->|Validate| C["Clean Records"]
    C -->|Entity Resolution| D["Groups/Merges"]
    D -->|Build Graph| E["Neo4j DB"]
    E -->|Query| F["Cache Layer"]
    F -->|LLM Processing| G["User Response"]
    
    style A fill:#06b6d4,stroke:#0e7490,color:#fff
    style B fill:#f59e0b,stroke:#92400e,color:#fff
    style C fill:#10b981,stroke:#065f46,color:#fff
    style D fill:#8b5cf6,stroke:#5b21b6,color:#fff
    style E fill:#ec4899,stroke:#831843,color:#fff
    style F fill:#3b82f6,stroke:#1e40af,color:#fff
    style G fill:#10b981,stroke:#065f46,color:#fff`}
            />
          </div>
        </div>
      </SectionRenderer>

      {/* Implementation Details Section */}
      <SectionRenderer id="implementation" title="Implementation Details">
        <div style={styles.sectionContent}>
          <div style={styles.subsection}>
            <h4 style={styles.subsectionTitle}>Data Validation Example</h4>
            <div style={styles.codeBlock}>
              {`# Vessel record validation
class VesselValidator:
    def validate_imo(self, imo):
        """IMO must be 7 digits"""
        return isinstance(imo, int) and 1000000 <= imo <= 9999999
    
    def validate_mmsi(self, mmsi):
        """MMSI must be 9 digits"""
        return isinstance(mmsi, int) and 100000000 <= mmsi <= 999999999
    
    def detect_duplicates(self, records):
        """Find potential duplicates by similarity"""
        duplicates = []
        for i, rec1 in enumerate(records):
            for rec2 in records[i+1:]:
                score = similarity_score(rec1, rec2)
                if score > 0.85:  # High similarity threshold
                    duplicates.append((rec1, rec2, score))
        return duplicates`}
            </div>
          </div>

          <div style={styles.subsection}>
            <h4 style={styles.subsectionTitle}>Entity Resolution Example</h4>
            <div style={styles.codeBlock}>
              {`# Entity resolution and matching
class EntityResolver:
    def match_vessels(self, vessel1, vessel2):
        """Score similarity between two vessel records"""
        score = 0.0
        
        # IMO exact match
        if vessel1.imo == vessel2.imo:
            score += 0.4
        
        # Name similarity (fuzzy)
        name_sim = fuzz.token_set_ratio(
            vessel1.name, vessel2.name
        ) / 100
        score += name_sim * 0.3
        
        # Flag + Type match
        if vessel1.flag == vessel2.flag:
            score += 0.15
        if vessel1.vessel_type == vessel2.vessel_type:
            score += 0.15
        
        return score
    
    def merge_records(self, records, threshold=0.85):
        """Merge duplicate records above threshold"""
        merged_groups = []
        processed = set()
        
        for i, rec in enumerate(records):
            if i in processed:
                continue
            group = [rec]
            for j, other in enumerate(records[i+1:], i+1):
                if j not in processed:
                    if self.match_vessels(rec, other) > threshold:
                        group.append(other)
                        processed.add(j)
            merged_groups.append(group)
        
        return merged_groups`}
            </div>
          </div>

          <div style={styles.subsection}>
            <h4 style={styles.subsectionTitle}>Knowledge Graph Schema</h4>
            <div style={styles.codeBlock}>
              {`# Neo4j Knowledge Graph Schema
NODES:
  ├── Vessel
  │   ├── imo (unique)
  │   ├── mmsi (can change)
  │   ├── name
  │   ├── flag
  │   ├── vessel_type
  │   ├── built_year
  │   └── gross_tonnage
  │
  ├── VesselType
  │   └── name (e.g., Tanker, Bulk Carrier)
  │
  ├── Country
  │   └── name (flag nations)
  │
  └── Owner/Operator
      └── name

RELATIONSHIPS:
  ├── Vessel -[:IS_TYPE]-> VesselType
  ├── Vessel -[:FLAGGED_BY]-> Country
  ├── Vessel -[:MANAGED_BY]-> Owner
  ├── Vessel -[:FORMER_MMSI]-> Vessel (temporal)
  ├── Vessel -[:POTENTIAL_DUPLICATE]-> Vessel
  └── Owner -[:OWNS]-> Vessel`}
            </div>
          </div>

          <div style={styles.subsection}>
            <h4 style={styles.subsectionTitle}>LLM-Powered Query Generation</h4>
            <div style={styles.codeBlock}>
              {`# Multi-agent LLM orchestration
class ConversationalAI:
    def process_query(self, user_query: str) -> QueryResponse:
        """
        1. Parse natural language query
        2. Determine intent (search, filter, aggregate)
        3. Generate Cypher query
        4. Execute with caching
        5. Ground response with data
        """
        
        # Step 1: Intent classification
        intent = self.classify_intent(user_query)
        # "search" | "filter" | "aggregate" | "relationship"
        
        # Step 2: Entity extraction
        entities = self.extract_entities(user_query)
        # {"vessel_name": "...", "flag": "...", ...}
        
        # Step 3: Generate Cypher
        cypher = self.llm_generate_cypher(
            user_query, intent, entities
        )
        
        # Step 4: Execute with caching
        cache_key = hash(cypher)
        if cache_key in self.cache:
            results = self.cache[cache_key]
        else:
            results = self.kg.execute_cypher(cypher)
            self.cache[cache_key] = results
        
        # Step 5: Generate grounded response
        response = self.ground_and_respond(
            user_query, results, cypher
        )
        
        return response`}
            </div>
          </div>

          <div style={styles.subsection}>
            <h4 style={styles.subsectionTitle}>Caching & Session Management</h4>
            <div style={styles.codeBlock}>
              {`# Query caching and session context
class SessionManager:
    def __init__(self):
        self.session_cache = {}  # User session ID -> context
        self.query_cache = {}    # Query hash -> results
        self.ttl = 3600  # 1 hour cache TTL
    
    def get_session_context(self, user_id: str) -> Dict:
        """Get conversation history and context"""
        if user_id in self.session_cache:
            return self.session_cache[user_id]
        return {"history": [], "filters": {}}
    
    def cache_query(self, query_hash: str, results: Any):
        """Cache query results with TTL"""
        self.query_cache[query_hash] = {
            "results": results,
            "timestamp": time.time()
        }
    
    def get_cached_result(self, query_hash: str) -> Optional[Any]:
        """Get cached result if not expired"""
        if query_hash in self.query_cache:
            cached = self.query_cache[query_hash]
            if time.time() - cached["timestamp"] < self.ttl:
                return cached["results"]
            else:
                del self.query_cache[query_hash]
        return None
    
    def update_session(self, user_id: str, query: str, response: str):
        """Update conversation history"""
        if user_id not in self.session_cache:
            self.session_cache[user_id] = {"history": [], "filters": {}}
        
        self.session_cache[user_id]["history"].append({
            "query": query,
            "response": response,
            "timestamp": time.time()
        })`}
            </div>
          </div>

          <div style={styles.subsection}>
            <h4 style={styles.subsectionTitle}>RAG Guard Implementation</h4>
            <div style={styles.codeBlock}>
              {`# RAG guard to prevent hallucinations
class RAGGuard:
    def validate_response(self, response: str, 
                         results: Dict) -> bool:
        """Verify response is grounded in actual data"""
        
        # 1. Check all vessel names mentioned exist in results
        mentioned_vessels = extract_vessel_names(response)
        result_vessels = {v["name"] for v in results}
        
        for vessel in mentioned_vessels:
            if vessel not in result_vessels:
                return False
        
        # 2. Check numeric claims against actual data
        claimed_numbers = extract_numbers(response)
        actual_numbers = extract_result_numbers(results)
        
        for num in claimed_numbers:
            if num not in actual_numbers:
                return False
        
        # 3. Check relationships are factual
        if not self.verify_relationships(response, results):
            return False
        
        return True
    
    def ground_response(self, llm_response: str, 
                       results: Dict) -> str:
        """Ensure response only uses factual data"""
        if not self.validate_response(llm_response, results):
            return self.synthesize_safe_response(results)
        return llm_response`}
            </div>
          </div>
        </div>
      </SectionRenderer>

      {/* End Goal Section */}
      <SectionRenderer id="endgoal" title="End Goal">
        <div style={styles.sectionContent}>
          <p style={styles.paragraph}>
            At the end, you should be able to show us:
          </p>

          <div style={styles.subsection}>
            <h4 style={styles.subsectionTitle}>The Main Things We're Looking For</h4>
            <ul style={styles.list}>
              <li style={styles.listItem}>
                ✓ <strong>You get the problem</strong> - You understand why vessel data is messy and what we're trying to solve
              </li>
              <li style={styles.listItem}>
                ✓ <strong>You have a solution</strong> - Show how you'd match vessels, score similarity, deduplicate
              </li>
              <li style={styles.listItem}>
                ✓ <strong>Your system design makes sense</strong> - Database choice, API design, caching strategy all reasonable
              </li>
              <li style={styles.listItem}>
                ✓ <strong>The AI layer is real</strong> - Not just talking about it, show actual LLM + tool use patterns
              </li>
              <li style={styles.listItem}>
                ✓ <strong>You have actual working code</strong> - Validation logic, entity resolution, query generation
              </li>
            </ul>
          </div>

          <div style={styles.highlightBox}>
            <div style={styles.highlightTitle}>🎯 Success Looks Like</div>
            <ul style={styles.list}>
              <li style={styles.listItem}>System actually handles real messy vessel data correctly</li>
              <li style={styles.listItem}>Your matching algorithm works (85%+ accuracy is good)</li>
              <li style={styles.listItem}>Users can ask questions in plain English and get answers</li>
              <li style={styles.listItem}>Responses are grounded in real data, not made up by the AI</li>
              <li style={styles.listItem}>Repeated queries are fast because of caching</li>
              <li style={styles.listItem}>System can handle lots of vessel records without breaking</li>
            </ul>
          </div>

          <div style={styles.subsection}>
            <h4 style={styles.subsectionTitle}>The Big Picture</h4>
            <p style={styles.paragraph}>
              This case study brings together data engineering, AI design, system architecture, and practical coding. 
              You're showing that you can think through a messy real-world problem, design a solution that actually works, 
              and implement it in a way that's maintainable and scalable. That's the whole thing.
            </p>
          </div>
        </div>
      </SectionRenderer>

      <div style={{ height: '20px' }} />
    </div>
  );
};

export default CaseStudy;
