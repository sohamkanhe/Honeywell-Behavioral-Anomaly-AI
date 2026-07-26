/**
 * React.js Security Operations Center (SOC) Analyst Dashboard
 * Built with React 18 (Functional Components, Hooks: useState, useEffect, useCallback, useMemo, useRef)
 * Connected to FastAPI Backend & Real-time Kafka WebSocket Feed
 */

const { useState, useEffect, useCallback, useMemo, useRef } = React;

const THREAT_LABELS = {
  impossible_travel: "Impossible Travel",
  lateral_movement: "Lateral Movement",
  brute_force: "Brute Force",
  credential_stuffing: "Credential Stuffing",
  low_and_slow_exfiltration: "Low & Slow Exfiltration",
  device_spoofing: "Device Spoofing",
  insider_drift: "Insider Drift"
};

const THREAT_COLORS = {
  impossible_travel: "#FF0055",
  lateral_movement: "#E600FF",
  brute_force: "#FF9900",
  credential_stuffing: "#FF6600",
  low_and_slow_exfiltration: "#9933FF",
  device_spoofing: "#00E5FF",
  insider_drift: "#0099FF"
};

// 1. Header Component
function Header({ toggleSidebar }) {
  return (
    <header className="app-header">
      <div style={{ display: 'flex', alignItems: 'center', gap: '16px' }}>
        <button className="sidebar-toggle-btn" onClick={toggleSidebar} title="Toggle Navigation Sidebar">
          ☰
        </button>
        <div className="brand-title">
          🛡️ UEBA SOC ANALYST RADAR
          <span className="brand-badge">REACT.JS + FASTAPI</span>
        </div>
      </div>
    </header>
  );
}

// 2. Sidebar Component (Collapsible Drawer)
function Sidebar({ isOpen, activeView, setActiveView, search, setSearch, minRisk, setMinRisk, statusFilter, setStatusFilter, theme, toggleTheme }) {
  if (!isOpen) return null;

  return (
    <aside className="sidebar">
      <div>
        <div className="sidebar-section-title">Navigation</div>
        <div className={`nav-item ${activeView === 'dashboard' ? 'active' : ''}`} onClick={() => setActiveView('dashboard')}>
          📊 Radar Dashboard
        </div>
        <div className={`nav-item ${activeView === 'charts' ? 'active' : ''}`} onClick={() => setActiveView('charts')}>
          📈 Analytics & Charts
        </div>
      </div>

      <div>
        <div className="sidebar-section-title">Search & Filters</div>
        <div style={{ display: 'flex', flexDirection: 'column', gap: '12px' }}>
          <input
            type="text"
            className="search-box"
            placeholder="Search Entity ID, IP, Resource..."
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />

          <div>
            <label style={{ fontSize: '11px', color: 'var(--text-muted)', fontWeight: 700, display: 'block', marginBottom: '4px' }}>
              MIN RISK SCORE THRESHOLD
            </label>
            <select className="select-control" value={minRisk} onChange={(e) => setMinRisk(parseFloat(e.target.value))}>
              <option value="0">Show All Risk Levels (1 - 10)</option>
              <option value="4.0">Medium Risk & Above (>= 4.0)</option>
              <option value="7.0">High Risk & Above (>= 7.0)</option>
              <option value="9.0">Critical Threats Only (>= 9.0)</option>
              <option value="10.0">Extreme Attacks Only (10.0)</option>
            </select>
          </div>

          <div>
            <label style={{ fontSize: '11px', color: 'var(--text-muted)', fontWeight: 700, display: 'block', marginBottom: '4px' }}>
              ALERT STATUS
            </label>
            <select className="select-control" value={statusFilter} onChange={(e) => setStatusFilter(e.target.value)}>
              <option value="">All Statuses</option>
              <option value="NEW">NEW (Unacknowledged)</option>
              <option value="ACKNOWLEDGED">ACKNOWLEDGED</option>
            </select>
          </div>
        </div>
      </div>

      <div style={{ marginTop: 'auto' }}>
        <button className="theme-toggle-btn" onClick={toggleTheme}>
          <span>{theme === 'dark' ? '☀️' : '🌙'}</span>
          <span>{theme === 'dark' ? 'Switch to Light Theme' : 'Switch to Dark Theme'}</span>
        </button>
      </div>
    </aside>
  );
}

// 3. Metrics Bar Component
function MetricsBar({ metrics }) {
  return (
    <div className="metrics-grid">
      <div className="glass-panel metric-card">
        <span className="metric-label">Processed Access Logs</span>
        <span className="metric-value">{metrics.total_logs_processed ? metrics.total_logs_processed.toLocaleString() : 0}</span>
      </div>
      <div className="glass-panel metric-card warning">
        <span className="metric-label">Flagged Anomalies</span>
        <span className="metric-value">{metrics.total_alerts_flagged ? metrics.total_alerts_flagged.toLocaleString() : 0}</span>
      </div>
      <div className="glass-panel metric-card critical">
        <span className="metric-label">High-Risk Incidents (>= 7.0)</span>
        <span className="metric-value">{metrics.high_risk_anomalies ? metrics.high_risk_anomalies.toLocaleString() : 0}</span>
      </div>
      <div className="glass-panel metric-card green">
        <span className="metric-label">Acknowledged Alerts</span>
        <span className="metric-value" style={{ color: 'var(--accent-green)' }}>{metrics.acknowledged_alerts ? metrics.acknowledged_alerts.toLocaleString() : 0}</span>
      </div>
    </div>
  );
}

// Helper Badges
function RiskBadge({ score }) {
  if (score >= 9.0) return <span className="risk-badge risk-critical">CRITICAL {score}</span>;
  if (score >= 7.0) return <span className="risk-badge risk-high">HIGH {score}</span>;
  if (score >= 4.0) return <span className="risk-badge risk-medium">MED {score}</span>;
  return <span className="risk-badge risk-low">LOW {score}</span>;
}

function StatusBadge({ status }) {
  if (status === 'ACKNOWLEDGED') {
    return <span className="status-badge status-ack">✓ ACKNOWLEDGED</span>;
  }
  return <span className="status-badge status-new">NEW</span>;
}

function ThreatTag({ type }) {
  const label = THREAT_LABELS[type] || type;
  return <span className={`threat-tag tag-${type}`}>● {label}</span>;
}

// 4. Alert Row Component
function AlertRow({ alert, isSelected, onSelect, onAcknowledge }) {
  const isAck = alert.status === 'ACKNOWLEDGED';

  return (
    <tr className={isSelected ? 'selected-row' : ''} onClick={() => onSelect(alert.alert_id)}>
      <td><RiskBadge score={alert.risk_score} /></td>
      <td><StatusBadge status={alert.status} /></td>
      <td style={{ fontFamily: 'var(--font-mono)', fontSize: '11px', whiteSpace: 'nowrap' }}>{alert.timestamp}</td>
      <td style={{ fontWeight: 700, color: 'var(--accent-cyan)' }}>{alert.entity_id}</td>
      <td style={{ whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', maxWidth: '100px' }}>{alert.role}</td>
      <td><ThreatTag type={alert.anomaly_type} /></td>
      <td style={{ fontFamily: 'var(--font-mono)', fontSize: '11px', whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis', maxWidth: '120px' }}>{alert.resource_accessed}</td>
      <td style={{ fontFamily: 'var(--font-mono)', fontSize: '11px', whiteSpace: 'nowrap' }}>{alert.source_ip}</td>
      <td onClick={(e) => e.stopPropagation()}>
        {isAck ? (
          <span className="btn-table-acked">✓ ACKED</span>
        ) : (
          <button className="btn-table-ack" onClick={() => onAcknowledge(alert.alert_id)}>✓ ACK</button>
        )}
      </td>
    </tr>
  );
}

// 5. XAI Side Panel Component
function XaiSidePanel({ alertDetail, timelineData, onAcknowledge }) {
  if (!alertDetail) {
    return (
      <div className="glass-panel xai-panel">
        <div style={{ textAlign: 'center', padding: '60px', color: 'var(--text-muted)' }}>
          Select an alert from the queue to inspect TreeSHAP XAI feature attributions & entity timeline.
        </div>
      </div>
    );
  }

  const topShap = alertDetail.feature_attributions?.top_attributions || [];
  const isAcked = alertDetail.status === 'ACKNOWLEDGED';

  return (
    <div className="glass-panel xai-panel">
      <div>
        <div className="xai-header" style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <div>
            <div style={{ fontSize: '11px', color: 'var(--text-muted)', fontFamily: 'var(--font-mono)' }}>ALERT DETAILS & SHAP XAI</div>
            <h3 style={{ fontSize: '16px', fontWeight: 800, marginTop: '4px' }}>{alertDetail.alert_id}</h3>
          </div>
          <div style={{ display: 'flex', flexDirection: 'column', alignItems: 'flex-end', gap: '4px' }}>
            <RiskBadge score={alertDetail.risk_score} />
            <StatusBadge status={alertDetail.status} />
          </div>
        </div>

        <div style={{ display: 'flex', flexDirection: 'column', gap: '8px', marginTop: '14px', fontSize: '12px' }}>
          <div style={{ display: 'flex', justifyContent: 'space-between' }}>
            <span style={{ color: 'var(--text-muted)' }}>Entity ID:</span>
            <span style={{ fontWeight: 700, color: 'var(--accent-cyan)' }}>{alertDetail.entity_id} ({alertDetail.role})</span>
          </div>
          <div style={{ display: 'flex', justifyContent: 'space-between' }}>
            <span style={{ color: 'var(--text-muted)' }}>Threat Type:</span>
            <ThreatTag type={alertDetail.anomaly_type} />
          </div>
          <div style={{ display: 'flex', justifyContent: 'space-between' }}>
            <span style={{ color: 'var(--text-muted)' }}>Location / IP:</span>
            <span style={{ fontFamily: 'var(--font-mono)' }}>{alertDetail.geo_location} ({alertDetail.source_ip})</span>
          </div>
          <div style={{ display: 'flex', justifyContent: 'space-between' }}>
            <span style={{ color: 'var(--text-muted)' }}>Point AE Error:</span>
            <span style={{ fontFamily: 'var(--font-mono)', color: '#FF3377' }}>{alertDetail.point_error}</span>
          </div>
          <div style={{ display: 'flex', justifyContent: 'space-between' }}>
            <span style={{ color: 'var(--text-muted)' }}>LSTM Sequence Error:</span>
            <span style={{ fontFamily: 'var(--font-mono)', color: '#FF9900' }}>{alertDetail.seq_error}</span>
          </div>
        </div>

        <div style={{ marginTop: '14px', borderTop: '1px solid var(--panel-border)', paddingTop: '14px' }}>
          {isAcked ? (
            <div style={{ color: '#00FF88', fontWeight: 700, fontSize: '12px', display: 'flex', alignItems: 'center', gap: '6px' }}>
              ✓ THIS ALERT IS ACKNOWLEDGED & RECORDED IN DATABASE
            </div>
          ) : (
            <button className="btn-ack" onClick={() => onAcknowledge(alertDetail.alert_id)}>
              ✓ ACKNOWLEDGE ALERT (SOC ANALYST ACTION)
            </button>
          )}
        </div>

        {/* TreeSHAP Horizontal Bar Chart */}
        <div style={{ marginTop: '20px' }}>
          <h4 style={{ fontSize: '13px', fontWeight: 700, marginBottom: '6px', color: 'var(--accent-cyan)' }}>
            📊 TreeSHAP Feature Attributions (Why Flagged)
          </h4>
          <p style={{ fontSize: '11px', color: 'var(--text-muted)', marginBottom: '10px' }}>
            Features pushing towards anomaly classification (Red/Orange) vs Normal (Blue/Green).
          </p>

          <div className="shap-bar-container">
            {topShap.map((attr, i) => {
              const isPos = attr.shap_value >= 0;
              const widthPct = Math.min(100, (Math.abs(attr.shap_value) / 0.5) * 100);
              const displayVal = isPos ? `+${attr.shap_value}` : attr.shap_value;
              return (
                <div className="shap-item" key={i}>
                  <div className="shap-label-row">
                    <span>{attr.feature} (val: {attr.feature_value})</span>
                    <span style={{ color: isPos ? '#FF0055' : '#00FF88' }}>{displayVal}</span>
                  </div>
                  <div className="shap-bar-track">
                    <div className={`shap-bar-fill ${isPos ? 'shap-positive' : 'shap-negative'}`} style={{ width: `${widthPct}%` }}></div>
                  </div>
                </div>
              );
            })}
          </div>
        </div>

        {/* Entity Behavioral Sequence Timeline */}
        <div style={{ marginTop: '20px' }}>
          <h4 style={{ fontSize: '13px', fontWeight: 700, marginBottom: '6px', color: 'var(--accent-magenta)' }}>
            ⏱️ Entity Behavioral Sequence Timeline
          </h4>
          <div className="timeline-container">
            {(timelineData.recent_events || []).map((ev, idx) => (
              <div className="timeline-item" key={idx}>
                <div style={{ display: 'flex', flexDirection: 'column', gap: '4px', width: '100%' }}>
                  <div style={{ display: 'flex', justifyContent: 'space-between', fontFamily: 'var(--font-mono)' }}>
                    <span style={{ color: 'var(--accent-cyan)' }}>{ev.timestamp}</span>
                    <span style={{ color: '#FF3377', fontWeight: 700 }}>Risk {ev.risk_score}</span>
                  </div>
                  <div>Accessed: <strong style={{ color: 'var(--text-main)' }}>{ev.resource_accessed}</strong> via {ev.source_ip}</div>
                  <div style={{ color: 'var(--text-muted)' }}>Location: {ev.geo_location}</div>
                </div>
              </div>
            ))}
          </div>
        </div>
      </div>
    </div>
  );
}

// 6. SVG Pie Chart Component for Analytics
function ThreatPieChart({ threatData }) {
  if (!threatData || threatData.length === 0) return null;

  const total = threatData.reduce((acc, curr) => acc + curr.count, 0);
  if (total === 0) return <div>No data for pie chart.</div>;

  let cumulativeAngle = 0;
  const radius = 80;
  const cx = 100;
  const cy = 100;

  const slices = threatData.map((d) => {
    const percentage = d.count / total;
    const angle = percentage * 360;
    const startAngle = cumulativeAngle;
    const endAngle = cumulativeAngle + angle;
    cumulativeAngle += angle;

    const x1 = cx + radius * Math.cos((Math.PI * (startAngle - 90)) / 180);
    const y1 = cy + radius * Math.sin((Math.PI * (startAngle - 90)) / 180);
    const x2 = cx + radius * Math.cos((Math.PI * (endAngle - 90)) / 180);
    const y2 = cy + radius * Math.sin((Math.PI * (endAngle - 90)) / 180);

    const largeArcFlag = angle > 180 ? 1 : 0;
    const pathData = `M ${cx} ${cy} L ${x1} ${y1} A ${radius} ${radius} 0 ${largeArcFlag} 1 ${x2} ${y2} Z`;

    const color = THREAT_COLORS[d.anomaly_type] || '#00E5FF';
    return {
      pathData,
      color,
      name: THREAT_LABELS[d.anomaly_type] || d.anomaly_type,
      count: d.count,
      percent: (percentage * 100).toFixed(1)
    };
  });

  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: '20px', flexWrap: 'wrap' }}>
      <svg width="200" height="200" viewBox="0 0 200 200">
        {slices.map((slice, i) => (
          <path key={i} d={slice.pathData} fill={slice.color} opacity="0.85" stroke="var(--bg-dark)" strokeWidth="2">
            <title>{`${slice.name}: ${slice.count} (${slice.percent}%)`}</title>
          </path>
        ))}
      </svg>
      <div style={{ display: 'flex', flexDirection: 'column', gap: '6px', fontSize: '12px' }}>
        {slices.map((slice, i) => (
          <div key={i} style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
            <span style={{ width: '12px', height: '12px', borderRadius: '3px', background: slice.color, display: 'inline-block' }}></span>
            <span style={{ color: 'var(--text-muted)', fontWeight: 600 }}>{slice.name}:</span>
            <span style={{ fontFamily: 'var(--font-mono)', fontWeight: 700 }}>{slice.count} ({slice.percent}%)</span>
          </div>
        ))}
      </div>
    </div>
  );
}

// 7. Analytics Charts Component
function AnalyticsCharts({ summary }) {
  if (!summary) return <div>Loading analytics...</div>;

  const threatDist = summary.threat_distribution || [];
  const maxTax = Math.max(...threatDist.map(d => d.count), 1);

  const riskHist = summary.risk_histogram || [];
  const maxRisk = Math.max(...riskHist.map(d => d.count), 1);

  const topEntities = summary.top_targeted_entities || [];
  const maxTarget = Math.max(...topEntities.map(d => d.alert_count), 1);

  return (
    <div className="charts-grid">
      <div className="glass-panel chart-card" style={{ gridColumn: 'span 2' }}>
        <h3 style={{ fontSize: '15px', fontWeight: 700, color: 'var(--accent-cyan)' }}>
          🍰 Threat Taxonomy Pie Chart & Share Analysis
        </h3>
        <ThreatPieChart threatData={threatDist} />
      </div>

      <div className="glass-panel chart-card">
        <h3 style={{ fontSize: '15px', fontWeight: 700, color: 'var(--accent-amber)' }}>Risk Score Spectrum</h3>
        <div>
          {riskHist.map((d, i) => (
            <div className="chart-bar-row" key={i}>
              <div className="chart-bar-label">{d.risk_range}</div>
              <div className="chart-bar-bg">
                <div className="chart-bar-val" style={{ width: `${(d.count / maxRisk) * 100}%`, background: 'linear-gradient(90deg, #FF9900, #FF0055)' }}></div>
              </div>
              <div style={{ fontFamily: 'var(--font-mono)', fontWeight: 700, width: '40px', textAlign: 'right' }}>{d.count}</div>
            </div>
          ))}
        </div>
      </div>

      <div className="glass-panel chart-card">
        <h3 style={{ fontSize: '15px', fontWeight: 700, color: 'var(--accent-magenta)' }}>Top Targeted Entities</h3>
        <div>
          {topEntities.map((d, i) => (
            <div className="chart-bar-row" key={i}>
              <div className="chart-bar-label">{d.entity_id} ({d.role})</div>
              <div className="chart-bar-bg">
                <div className="chart-bar-val" style={{ width: `${(d.alert_count / maxTarget) * 100}%`, background: 'linear-gradient(90deg, #00E5FF, #E600FF)' }}></div>
              </div>
              <div style={{ fontFamily: 'var(--font-mono)', fontWeight: 700, width: '40px', textAlign: 'right' }}>{d.alert_count}</div>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

// Main React App Component
function App() {
  const [isSidebarOpen, setIsSidebarOpen] = useState(true);
  const [activeView, setActiveView] = useState('dashboard');
  const [theme, setTheme] = useState('dark');
  const [metrics, setMetrics] = useState({});
  const [alerts, setAlerts] = useState([]);
  const [selectedAlertId, setSelectedAlertId] = useState(null);
  const [alertDetail, setAlertDetail] = useState(null);
  const [timelineData, setTimelineData] = useState({ recent_events: [] });
  const [chartsSummary, setChartsSummary] = useState(null);
  const [wsConnected, setWsConnected] = useState(false);

  // Filters State
  const [currentSort, setSort] = useState('risk_score');
  const [currentFilter, setFilter] = useState(null);
  const [minRisk, setMinRisk] = useState(0);
  const [search, setSearch] = useState('');
  const [statusFilter, setStatusFilter] = useState('');

  const toggleSidebar = () => setIsSidebarOpen(prev => !prev);

  // Toggle Dark/Light Theme
  const toggleTheme = () => {
    const nextTheme = theme === 'dark' ? 'light' : 'dark';
    setTheme(nextTheme);
    if (nextTheme === 'light') {
      document.body.classList.add('light-theme');
    } else {
      document.body.classList.remove('light-theme');
    }
  };

  // Fetch System Metrics
  const fetchMetrics = useCallback(async () => {
    try {
      const res = await fetch('/api/metrics');
      if (res.ok) {
        const data = await res.json();
        setMetrics(data);
      }
    } catch (e) {}
  }, []);

  // Fetch Alert Queue
  const fetchAlerts = useCallback(async () => {
    try {
      let url = `/api/alerts?limit=100&sort_by=${currentSort}&order=desc`;
      if (currentFilter) url += `&anomaly_type=${encodeURIComponent(currentFilter)}`;
      if (minRisk > 0) url += `&min_risk=${minRisk}`;
      if (search) url += `&search=${encodeURIComponent(search)}`;
      if (statusFilter) url += `&status=${encodeURIComponent(statusFilter)}`;

      const res = await fetch(url);
      if (res.ok) {
        const data = await res.json();
        setAlerts(data);
        if (data.length > 0 && !selectedAlertId) {
          setSelectedAlertId(data[0].alert_id);
        }
      }
    } catch (e) {}
  }, [currentSort, currentFilter, minRisk, search, statusFilter, selectedAlertId]);

  // Fetch Selected Alert Detail & Timeline
  const selectAlert = useCallback(async (alertId) => {
    setSelectedAlertId(alertId);
    try {
      const res = await fetch(`/api/alerts/${alertId}`);
      if (res.ok) {
        const detail = await res.json();
        setAlertDetail(detail);

        const resTimeline = await fetch(`/api/entities/${detail.entity_id}/timeline`);
        if (resTimeline.ok) {
          const timeline = await resTimeline.json();
          setTimelineData(timeline);
        }
      }
    } catch (e) {}
  }, []);

  // Fetch Analytics Charts
  const fetchCharts = useCallback(async () => {
    try {
      const res = await fetch('/api/charts/summary');
      if (res.ok) {
        const summary = await res.json();
        setChartsSummary(summary);
      }
    } catch (e) {}
  }, []);

  // Acknowledge Alert Handler
  const handleAcknowledge = async (alertId) => {
    try {
      const res = await fetch(`/api/alerts/${alertId}/acknowledge`, { method: 'POST' });
      if (res.ok) {
        fetchMetrics();
        fetchAlerts();
        if (selectedAlertId === alertId) {
          selectAlert(alertId);
        }
      }
    } catch (e) {}
  };

  // Initial Load & Timers
  useEffect(() => {
    fetchMetrics();
    fetchAlerts();
    const intervalM = setInterval(fetchMetrics, 3000);
    const intervalA = setInterval(fetchAlerts, 5000);
    return () => {
      clearInterval(intervalM);
      clearInterval(intervalA);
    };
  }, [fetchMetrics, fetchAlerts]);

  // Load Charts when view changes
  useEffect(() => {
    if (activeView === 'charts') {
      fetchCharts();
    }
  }, [activeView, fetchCharts]);

  // Load Alert detail when selected ID changes
  useEffect(() => {
    if (selectedAlertId) {
      selectAlert(selectedAlertId);
    }
  }, [selectedAlertId, selectAlert]);

  // WebSocket Live Stream Connection
  useEffect(() => {
    const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
    const wsUrl = `${protocol}//${window.location.host}/ws/alerts`;
    const ws = new WebSocket(wsUrl);

    ws.onopen = () => setWsConnected(true);
    ws.onclose = () => setWsConnected(false);

    ws.onmessage = (event) => {
      try {
        const payload = JSON.parse(event.data);

        if (payload.event_type === 'ALERT_ACKNOWLEDGED') {
          setAlerts(prev => prev.map(a => a.alert_id === payload.alert.alert_id ? { ...a, status: 'ACKNOWLEDGED' } : a));
          fetchMetrics();
          return;
        }

        const newAlert = payload.alert || payload;

        let passesFilter = true;
        if (currentFilter && newAlert.anomaly_type !== currentFilter) passesFilter = false;
        if (minRisk > 0 && newAlert.risk_score < minRisk) passesFilter = false;
        if (statusFilter && newAlert.status !== statusFilter) passesFilter = false;
        if (search) {
          const s = search.toLowerCase();
          const match = (newAlert.entity_id || '').toLowerCase().includes(s) ||
                        (newAlert.alert_id || '').toLowerCase().includes(s) ||
                        (newAlert.source_ip || '').toLowerCase().includes(s) ||
                        (newAlert.resource_accessed || '').toLowerCase().includes(s);
          if (!match) passesFilter = false;
        }

        if (passesFilter) {
          setAlerts(prev => [newAlert, ...prev.slice(0, 99)]);
        }
        fetchMetrics();
      } catch (e) {}
    };

    return () => ws.close();
  }, [currentFilter, minRisk, statusFilter, search, fetchMetrics]);

  return (
    <React.Fragment>
      <Header toggleSidebar={toggleSidebar} />
      <div className="layout-wrapper">
        <Sidebar
          isOpen={isSidebarOpen}
          activeView={activeView}
          setActiveView={setActiveView}
          search={search}
          setSearch={setSearch}
          minRisk={minRisk}
          setMinRisk={setMinRisk}
          statusFilter={statusFilter}
          setStatusFilter={setStatusFilter}
          theme={theme}
          toggleTheme={toggleTheme}
        />

        <main className="content-area">
          <MetricsBar metrics={metrics} />

          {activeView === 'dashboard' && (
            <div className="dashboard-grid">
              <div className="glass-panel" style={{ padding: '20px', overflowX: 'hidden' }}>
                <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '16px' }}>
                  <h2 style={{ fontSize: '15px', fontWeight: 700 }}>Ranked Alert Queue</h2>
                  <div style={{ display: 'flex', gap: '10px', fontSize: '12px' }}>
                    <button
                      onClick={() => setSort('risk_score')}
                      style={{
                        background: currentSort === 'risk_score' ? 'var(--table-selected)' : 'transparent',
                        color: 'var(--accent-cyan)',
                        border: '1px solid var(--accent-cyan)',
                        padding: '4px 10px',
                        borderRadius: '4px',
                        cursor: 'pointer',
                        fontWeight: 700
                      }}
                    >
                      Sort by Risk Score
                    </button>
                    <button
                      onClick={() => setSort('timestamp')}
                      style={{
                        background: currentSort === 'timestamp' ? 'var(--table-selected)' : 'transparent',
                        color: 'var(--accent-cyan)',
                        border: '1px solid var(--accent-cyan)',
                        padding: '4px 10px',
                        borderRadius: '4px',
                        cursor: 'pointer',
                        fontWeight: 700
                      }}
                    >
                      Sort by Timestamp
                    </button>
                  </div>
                </div>

                {/* Threat Taxonomy Pills */}
                <div className="taxonomy-bar">
                  <div className={`taxonomy-pill ${currentFilter === null ? 'active' : ''}`} onClick={() => setFilter(null)}>
                    <span className="taxonomy-dot"></span> All Threats
                  </div>
                  <div className={`taxonomy-pill ${currentFilter === 'brute_force' ? 'active' : ''}`} onClick={() => setFilter('brute_force')} style={{ color: 'var(--accent-amber)' }}>
                    <span className="taxonomy-dot"></span> Brute Force (35%)
                  </div>
                  <div className={`taxonomy-pill ${currentFilter === 'credential_stuffing' ? 'active' : ''}`} onClick={() => setFilter('credential_stuffing')} style={{ color: '#EA580C' }}>
                    <span className="taxonomy-dot"></span> Credential Stuffing (25%)
                  </div>
                  <div className={`taxonomy-pill ${currentFilter === 'lateral_movement' ? 'active' : ''}`} onClick={() => setFilter('lateral_movement')} style={{ color: 'var(--accent-magenta)' }}>
                    <span className="taxonomy-dot"></span> Lateral Movement (15%)
                  </div>
                  <div className={`taxonomy-pill ${currentFilter === 'low_and_slow_exfiltration' ? 'active' : ''}`} onClick={() => setFilter('low_and_slow_exfiltration')} style={{ color: 'var(--accent-purple)' }}>
                    <span className="taxonomy-dot"></span> Exfiltration (10%)
                  </div>
                  <div className={`taxonomy-pill ${currentFilter === 'device_spoofing' ? 'active' : ''}`} onClick={() => setFilter('device_spoofing')} style={{ color: 'var(--accent-cyan)' }}>
                    <span className="taxonomy-dot"></span> Device Spoofing (10%)
                  </div>
                  <div className={`taxonomy-pill ${currentFilter === 'impossible_travel' ? 'active' : ''}`} onClick={() => setFilter('impossible_travel')} style={{ color: 'var(--accent-red)' }}>
                    <span className="taxonomy-dot"></span> Impossible Travel (5%)
                  </div>
                </div>

                {/* Alert Table */}
                <div className="table-container">
                  <table>
                    <thead>
                      <tr>
                        <th>Risk</th>
                        <th>Status</th>
                        <th>Timestamp</th>
                        <th>Entity ID</th>
                        <th>Role</th>
                        <th>Threat Classification</th>
                        <th>Resource</th>
                        <th>Source IP</th>
                        <th>Action</th>
                      </tr>
                    </thead>
                    <tbody>
                      {alerts.length === 0 ? (
                        <tr>
                          <td colSpan="9" style={{ textAlign: 'center', padding: '40px', color: 'var(--text-muted)' }}>
                            No alerts match current search & filter criteria.
                          </td>
                        </tr>
                      ) : (
                        alerts.map(item => (
                          <AlertRow
                            key={item.alert_id}
                            alert={item}
                            isSelected={selectedAlertId === item.alert_id}
                            onSelect={selectAlert}
                            onAcknowledge={handleAcknowledge}
                          />
                        ))
                      )}
                    </tbody>
                  </table>
                </div>
              </div>

              <XaiSidePanel
                alertDetail={alertDetail}
                timelineData={timelineData}
                onAcknowledge={handleAcknowledge}
              />
            </div>
          )}

          {activeView === 'charts' && (
            <AnalyticsCharts summary={chartsSummary} />
          )}
        </main>
      </div>
    </React.Fragment>
  );
}

// Render React 18 Application Root
const root = ReactDOM.createRoot(document.getElementById('root'));
root.render(<App />);
