import React, { useState, useEffect } from 'react';
import {
    Chart as ChartJS,
    CategoryScale,
    LinearScale,
    PointElement,
    LineElement,
    BarElement,
    Title,
    Tooltip,
    Legend,
    ArcElement,
} from 'chart.js';
import { Line, Bar, Doughnut } from 'react-chartjs-2';

ChartJS.register(
    CategoryScale,
    LinearScale,
    PointElement,
    LineElement,
    BarElement,
    ArcElement,
    Title,
    Tooltip,
    Legend
);

const API_BASE = 'http://localhost:8000/api/kpi';

const KPICard = ({ label, value, loading }) => (
    <div className="kpi-card">
        <div className="kpi-value">{loading ? '...' : value}</div>
        <div className="kpi-label">{label}</div>
    </div>
);

const LineageNode = ({ name, count, layer }) => (
    <div className="lineage-step">
        <div className={`lineage-node node-${layer.toLowerCase()}`} title={`${name}: ${count.toLocaleString()} rows`}>
            <div className="node-count">{count > 1000000 ? (count / 1000000).toFixed(1) + 'M' : count.toLocaleString()}</div>
            <div className="node-label">{layer}</div>
        </div>
        <div className="lineage-label">{name.split(' (')[0]}</div>
    </div>
);

const MedallionLineage = ({ stats }) => {
    // Group stats by layer
    const landing = stats.filter(s => s.layer === 'Landing');
    const bronze = stats.filter(s => s.layer === 'Bronze');
    const silver = stats.filter(s => s.layer === 'Silver');
    const gold = stats.filter(s => s.layer === 'Gold');

    const totalLanding = landing.reduce((acc, s) => acc + s.count, 0);
    const totalBronze = bronze.reduce((acc, s) => acc + s.count, 0);
    const totalSilver = silver.reduce((acc, s) => acc + s.count, 0);
    const totalGoldFact = gold.filter(g => g.name.startsWith('fact')).reduce((acc, s) => acc + s.count, 0);

    return (
        <div className="chart-container full-chart" style={{ padding: '30px' }}>
            <div className="lineage-header">
                <div>
                    <span className="status-indicator"></span>
                    <strong>Data Transformation Lineage (Data Funnel)</strong>
                </div>
                <div style={{ fontSize: '12px', color: '#666' }}>
                    Visualizing 4 pipeline stages
                </div>
            </div>
            <div className="lineage-container">
                <LineageNode layer="Landing" name="Raw Sources" count={totalLanding} />
                <div className="lineage-arrow"></div>
                <LineageNode layer="Bronze" name="Ingested" count={totalBronze} />
                <div className="lineage-arrow"></div>
                <LineageNode layer="Silver" name="Normalized" count={totalSilver} />
                <div className="lineage-arrow"></div>
                <LineageNode layer="Gold" name="Business Ready" count={totalGoldFact} />
            </div>
            <div style={{ marginTop: '20px', display: 'grid', gridTemplateColumns: 'repeat(4, 1fr)', gap: '10px' }}>
                <div className="lineage-subtext">CSV/XLSX raw files from ERP, POS, WMS systems.</div>
                <div className="lineage-subtext">Partitioned Parquet. Preservation of original data.</div>
                <div className="lineage-subtext">Cleaned, Deduplicated, Typed, and Normalized.</div>
                <div className="lineage-subtext">Unified Star Schema (Facts & Dimensions).</div>
            </div>
        </div>
    );
};

function App() {
    const [summary, setSummary] = useState({});
    const [revenueTrend, setRevenueTrend] = useState([]);
    const [topProducts, setTopProducts] = useState([]);
    const [channelDist, setChannelDist] = useState([]);
    const [inventory, setInventory] = useState([]);
    const [lineageStats, setLineageStats] = useState([]);
    const [loading, setLoading] = useState(true);

    const fetchData = async () => {
        setLoading(true);
        try {
            const [sumRes, revRes, prodRes, chanRes, invRes, linRes] = await Promise.all([
                fetch(`${API_BASE}/summary`).then(r => r.json()),
                fetch(`${API_BASE}/revenue-trend`).then(r => r.json()),
                fetch(`${API_BASE}/top-products`).then(r => r.json()),
                fetch(`${API_BASE}/sales-channel`).then(r => r.json()),
                fetch(`${API_BASE}/inventory-status`).then(r => r.json()),
                fetch(`${API_BASE}/lineage-stats`).then(r => r.json()),
            ]);

            setSummary(sumRes);
            setRevenueTrend(revRes);
            setTopProducts(prodRes);
            setChannelDist(chanRes);
            setInventory(invRes);
            setLineageStats(linRes);
        } catch (err) {
            console.error('Error fetching data:', err);
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        fetchData();
    }, []);

    const revenueData = {
        labels: revenueTrend.map(d => `${d.month}/${d.year}`),
        datasets: [{
            label: 'Monthly Revenue',
            data: revenueTrend.map(d => d.revenue),
            borderColor: '#0078d4',
            backgroundColor: 'rgba(0, 120, 212, 0.1)',
            fill: true,
            tension: 0.4
        }]
    };

    const productData = {
        labels: topProducts.map(d => d.product_description),
        datasets: [{
            label: 'Revenue by Product',
            data: topProducts.map(d => d.total_revenue),
            backgroundColor: '#0078d4',
        }]
    };

    const channelData = {
        labels: channelDist.map(d => d.source_channel),
        datasets: [{
            data: channelDist.map(d => d.revenue),
            backgroundColor: ['#0078d4', '#2b88d8', '#5ca1e1', '#8dbbe9', '#bed5f2'],
        }]
    };

    const inventoryData = {
        labels: inventory.map(d => d.product_description),
        datasets: [{
            label: 'Current Stock',
            data: inventory.map(d => d.current_stock),
            backgroundColor: '#107c10',
        }]
    };

    const formatCurrency = (val) => {
        if (!val) return '$0';
        return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }).format(val);
    };

    const formatNumber = (val) => {
        if (!val) return '0';
        return new Intl.NumberFormat('en-US').format(val);
    };

    return (
        <div className="dashboard">
            <header className="header">
                Retail Analytics Dashboard (Mimicking Power BI)
            </header>

            <main className="main-content">
                <KPICard label="Total Revenue" value={formatCurrency(summary.total_revenue)} loading={loading} />
                <KPICard label="Total Orders" value={formatNumber(summary.total_orders)} loading={loading} />
                <KPICard label="Total Customers" value={formatNumber(summary.total_customers)} loading={loading} />
                <div className="kpi-card">
                    <button onClick={fetchData} style={{
                        padding: '8px 16px',
                        backgroundColor: '#0078d4',
                        color: 'white',
                        border: 'none',
                        borderRadius: '4px',
                        cursor: 'pointer',
                        fontWeight: '600'
                    }}>
                        Refresh Data
                    </button>
                    <div className="kpi-label">Last updated: {new Date().toLocaleTimeString()}</div>
                </div>

                {loading ? (
                    <div className="chart-container full-chart" style={{ textAlign: 'center', padding: '100px' }}>
                        Loading Data Lineage...
                    </div>
                ) : (
                    <MedallionLineage stats={lineageStats} />
                )}

                <div className="chart-container full-chart">
                    <div className="chart-header">Revenue Trend (Last 12 Months)</div>
                    <div style={{ height: '300px' }}>
                        {loading ? 'Loading...' : <Line data={revenueData} options={{ maintainAspectRatio: false }} />}
                    </div>
                </div>

                <div className="chart-container">
                    <div className="chart-header">Top 5 Products by Revenue</div>
                    <div style={{ height: '300px' }}>
                        {loading ? 'Loading...' : <Bar data={productData} options={{ indexAxis: 'y', maintainAspectRatio: false }} />}
                    </div>
                </div>

                <div className="chart-container">
                    <div className="chart-header">Revenue by Sales Channel</div>
                    <div style={{ height: '300px', display: 'flex', justifyContent: 'center' }}>
                        {loading ? 'Loading...' : <Doughnut data={channelData} options={{ maintainAspectRatio: false }} />}
                    </div>
                </div>

                <div className="chart-container full-chart">
                    <div className="chart-header">Inventory Levels (Top Products)</div>
                    <div style={{ height: '300px' }}>
                        {loading ? 'Loading...' : <Bar data={inventoryData} options={{ maintainAspectRatio: false }} />}
                    </div>
                </div>
            </main>
        </div>
    );
}

export default App;
