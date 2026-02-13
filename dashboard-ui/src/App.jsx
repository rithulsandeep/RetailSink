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

const API_BASE = 'http://localhost:8001/api/kpi';

const KPICard = ({ label, value, loading, subtext }) => (
    <div className="kpi-card">
        <div className="kpi-value">{loading ? '...' : value}</div>
        <div className="kpi-label">{label}</div>
        {subtext && <div className="kpi-subtext">{subtext}</div>}
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
                <div className="lineage-subtext">CSV/XLSX raw files from ERP, POS, WMS, Logistics.</div>
                <div className="lineage-subtext">Partitioned Parquet. Preservation of original data.</div>
                <div className="lineage-subtext">Cleaned, Deduplicated, Typed, and Normalized.</div>
                <div className="lineage-subtext">Unified Star Schema (Facts & Dimensions).</div>
            </div>
        </div>
    );
};

function App() {
    const [activeTab, setActiveTab] = useState('commercial');
    const [summary, setSummary] = useState({});
    const [revenueTrend, setRevenueTrend] = useState([]);
    const [topProducts, setTopProducts] = useState([]);
    const [citySales, setCitySales] = useState([]);
    const [opsMetrics, setOpsMetrics] = useState({});
    const [custInsights, setCustInsights] = useState({});
    const [channelDist, setChannelDist] = useState([]);
    const [inventory, setInventory] = useState([]);
    const [lineageStats, setLineageStats] = useState([]);
    const [loading, setLoading] = useState(true);

    const fetchData = async () => {
        setLoading(true);
        try {
            const [sumRes, revRes, prodRes, cityRes, opsRes, custRes, chanRes, invRes, linRes] = await Promise.all([
                fetch(`${API_BASE}/summary`).then(r => r.json()),
                fetch(`${API_BASE}/revenue-trend`).then(r => r.json()),
                fetch(`${API_BASE}/top-products`).then(r => r.json()),
                fetch(`${API_BASE}/city-sales`).then(r => r.json()),
                fetch(`${API_BASE}/operations-metrics`).then(r => r.json()),
                fetch(`${API_BASE}/customer-insights`).then(r => r.json()),
                fetch(`${API_BASE}/sales-channel`).then(r => r.json()),
                fetch(`${API_BASE}/inventory-status`).then(r => r.json()),
                fetch(`${API_BASE}/lineage-stats`).then(r => r.json()),
            ]);

            setSummary(sumRes);
            setRevenueTrend(revRes);
            setTopProducts(prodRes);
            setCitySales(cityRes);
            setOpsMetrics(opsRes);
            setCustInsights(custRes);
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

    const citySalesData = {
        labels: citySales.map(d => d.city),
        datasets: [{
            label: 'Revenue by City',
            data: citySales.map(d => d.revenue),
            backgroundColor: '#5ca1e1',
        }]
    };

    const seasonalData = {
        labels: opsMetrics.seasonal_demand?.map(d => `Month ${d.month}`) || [],
        datasets: [{
            label: 'Seasonal Demand',
            data: opsMetrics.seasonal_demand?.map(d => d.revenue) || [],
            borderColor: '#ffb900',
            backgroundColor: 'rgba(255, 185, 0, 0.1)',
            fill: true,
        }]
    };

    const segmentData = {
        labels: custInsights.segments?.map(s => s.segment) || [],
        datasets: [{
            data: custInsights.segments?.map(s => s.count) || [],
            backgroundColor: ['#0078d4', '#107c10'],
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
                <div>Retail Intelligence Platform</div>
                <div className="tabs">
                    <button className={activeTab === 'commercial' ? 'active' : ''} onClick={() => setActiveTab('commercial')}>Commercial</button>
                    <button className={activeTab === 'operations' ? 'active' : ''} onClick={() => setActiveTab('operations')}>Operations</button>
                    <button className={activeTab === 'customer' ? 'active' : ''} onClick={() => setActiveTab('customer')}>Customer</button>
                    <button className={activeTab === 'lineage' ? 'active' : ''} onClick={() => setActiveTab('lineage')}>Lineage</button>
                </div>
            </header>

            <main className="main-content">
                <div className="kpi-grid">
                    <KPICard label="Total Revenue" value={formatCurrency(summary.total_revenue)} loading={loading} />
                    <KPICard label="Total Orders" value={formatNumber(summary.total_orders)} loading={loading} />
                    <KPICard label="Avg Delivery" value={`${opsMetrics.avg_delivery_days} Days`} loading={loading} subtext="Across all shipments" />
                    <KPICard label="Customer CLV" value={formatCurrency(custInsights.clv)} loading={loading} subtext="Avg Lifetime Value" />
                </div>

                {activeTab === 'commercial' && (
                    <div className="tab-content">
                        <div className="chart-container full-chart">
                            <div className="chart-header">City-wise Sales Performance</div>
                            <div style={{ height: '300px' }}>
                                <Bar data={citySalesData} options={{ maintainAspectRatio: false }} />
                            </div>
                        </div>
                        <div className="chart-container">
                            <div className="chart-header">Revenue Trend (12 Months)</div>
                            <div style={{ height: '300px' }}>
                                <Line data={revenueData} options={{ maintainAspectRatio: false }} />
                            </div>
                        </div>
                        <div className="chart-container">
                            <div className="chart-header">Top 5 Products</div>
                            <div style={{ height: '300px' }}>
                                <Bar data={{
                                    labels: topProducts.map(p => p.product_description),
                                    datasets: [{ label: 'Revenue', data: topProducts.map(p => p.total_revenue), backgroundColor: '#0078d4' }]
                                }} options={{ indexAxis: 'y', maintainAspectRatio: false }} />
                            </div>
                        </div>
                    </div>
                )}

                {activeTab === 'operations' && (
                    <div className="tab-content">
                        <div className="chart-container full-chart">
                            <div className="chart-header">Seasonal Demand Trends</div>
                            <div style={{ height: '300px' }}>
                                <Line data={seasonalData} options={{ maintainAspectRatio: false }} />
                            </div>
                        </div>
                        <div className="chart-container">
                            <div className="chart-header">Inventory Levels (Top Products)</div>
                            <div style={{ height: '300px' }}>
                                <Bar data={{
                                    labels: inventory.map(d => d.product_description),
                                    datasets: [{ label: 'Stock', data: inventory.map(d => d.current_stock), backgroundColor: '#107c10' }]
                                }} options={{ maintainAspectRatio: false }} />
                            </div>
                        </div>
                        <div className="chart-container">
                            <div className="chart-header">Inventory Turnover Ratio</div>
                            <div className="kpi-value-large" style={{ marginTop: '50px' }}>
                                {opsMetrics.turnover_ratio}x
                            </div>
                            <div className="lineage-subtext" style={{ textAlign: 'center' }}>
                                High turnover indicates efficient inventory management.
                            </div>
                        </div>
                    </div>
                )}

                {activeTab === 'customer' && (
                    <div className="tab-content">
                        <div className="chart-container">
                            <div className="chart-header">New vs Returning Shoppers</div>
                            <div style={{ height: '300px', display: 'flex', justifyContent: 'center' }}>
                                <Doughnut data={segmentData} options={{ maintainAspectRatio: false }} />
                            </div>
                        </div>
                        <div className="chart-container">
                            <div className="chart-header">"Market Basket" Insights</div>
                            <div className="market-basket-list">
                                {custInsights.market_basket?.map((item, i) => (
                                    <div key={i} className="basket-item">
                                        <strong>{item.item_a}</strong> + <strong>{item.item_b}</strong>
                                        <div className="basket-freq">{item.frequency} times paired</div>
                                    </div>
                                ))}
                            </div>
                        </div>
                        <div className="chart-container full-chart">
                            <div className="chart-header">Revenue by Sales Channel</div>
                            <div style={{ height: '200px' }}>
                                <Bar data={{
                                    labels: channelDist.map(d => d.source_channel),
                                    datasets: [{ label: 'Revenue', data: channelDist.map(d => d.revenue), backgroundColor: '#bed5f2' }]
                                }} options={{ indexAxis: 'y', maintainAspectRatio: false }} />
                            </div>
                        </div>
                    </div>
                )}

                {activeTab === 'lineage' && (
                    <div className="tab-content">
                        <MedallionLineage stats={lineageStats} />
                    </div>
                )}
            </main>
        </div>
    );
}

export default App;
