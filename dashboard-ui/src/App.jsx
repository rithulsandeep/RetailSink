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
import BackgroundAnimation from './BackgroundAnimation';

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

const KPICard = ({ label, value, loading, subtext, className }) => (
    <div className={`kpi-card ${className || ''}`}>
        <div className="kpi-label">{label}</div>
        <div className="kpi-value">{loading ? '...' : value}</div>
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
    const [theme, setTheme] = useState(() => {
        const savedTheme = localStorage.getItem('theme');
        return savedTheme || 'light';
    });

    useEffect(() => {
        document.documentElement.setAttribute('data-theme', theme);
        localStorage.setItem('theme', theme);
    }, [theme]);

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

    const getChartColors = () => {
        const isDark = theme === 'dark';
        return {
            primary: '#8FA290', // RetailSink Primary
            secondary: '#7A8D7B', // RetailSink Secondary
            accent: '#C5A059', // RetailSink Accent
            success: '#8FA290',
            text: isDark ? '#F8F5F2' : '#1A1C1E',
            grid: isDark ? 'rgba(255,255,255,0.05)' : 'rgba(0,0,0,0.05)',
            muted: isDark ? '#2D2F31' : '#EDEFEE',
        };
    };

    const colors = getChartColors();

    const commonChartOptions = {
        responsive: true,
        maintainAspectRatio: false,
        animation: {
            duration: 1200,
            easing: 'easeInOutExpo',
        },
        plugins: {
            legend: {
                position: 'bottom',
                labels: {
                    usePointStyle: true,
                    pointStyle: 'circle',
                    color: colors.text,
                    font: {
                        family: 'Outfit',
                        size: 12,
                        weight: '500',
                    },
                    padding: 20,
                },
            },
            tooltip: {
                backgroundColor: theme === 'dark' ? '#1A1C1E' : '#FFFFFF',
                titleColor: colors.text,
                bodyColor: colors.text,
                borderColor: colors.grid,
                borderWidth: 1,
                padding: 16,
                boxPadding: 8,
                cornerRadius: 12,
                titleFont: { family: 'Outfit', size: 14, weight: '600' },
                bodyFont: { family: 'Outfit', size: 13 },
            },
        },
        scales: {
            x: {
                grid: { display: false },
                ticks: { color: colors.text, font: { family: 'Outfit', size: 12 } },
            },
            y: {
                grid: { color: colors.grid, drawBorder: false },
                ticks: { color: colors.text, font: { family: 'Outfit', size: 12 } },
            },
        },
    };

    const revenueData = {
        labels: revenueTrend.map(d => `${d.month}/${d.year}`),
        datasets: [{
            label: 'Monthly Revenue',
            data: revenueTrend.map(d => d.revenue),
            borderColor: colors.primary,
            backgroundColor: (context) => {
                const ctx = context.chart.ctx;
                const gradient = ctx.createLinearGradient(0, 0, 0, 400);
                gradient.addColorStop(0, 'rgba(143, 162, 144, 0.3)');
                gradient.addColorStop(1, 'rgba(143, 162, 144, 0)');
                return gradient;
            },
            fill: true,
            tension: 0.4,
            borderWidth: 2,
            pointRadius: 0,
            pointHoverRadius: 6,
            pointHoverBackgroundColor: colors.primary,
            pointHoverBorderColor: '#fff',
            pointHoverBorderWidth: 2,
        }]
    };

    const citySalesData = {
        labels: citySales.map(d => d.city),
        datasets: [{
            label: 'Revenue by City',
            data: citySales.map(d => d.revenue),
            backgroundColor: colors.primary,
            borderRadius: 20,
            borderWidth: 0,
            barThickness: 32,
        }]
    };

    const seasonalData = {
        labels: opsMetrics.seasonal_demand?.map(d => `Month ${d.month}`) || [],
        datasets: [{
            label: 'Seasonal Demand',
            data: opsMetrics.seasonal_demand?.map(d => d.revenue) || [],
            borderColor: colors.accent,
            backgroundColor: (context) => {
                const ctx = context.chart.ctx;
                const gradient = ctx.createLinearGradient(0, 0, 0, 400);
                gradient.addColorStop(0, 'rgba(197, 160, 89, 0.2)');
                gradient.addColorStop(1, 'rgba(197, 160, 89, 0)');
                return gradient;
            },
            fill: true,
            tension: 0.4,
            borderWidth: 2,
            pointRadius: 0,
            pointHoverRadius: 6,
        }]
    };

    const segmentData = {
        labels: custInsights.segments?.map(s => s.segment) || [],
        datasets: [{
            data: custInsights.segments?.map(s => s.count) || [],
            backgroundColor: [
                'rgba(102, 126, 234, 0.9)',
                'rgba(79, 172, 254, 0.9)',
            ],
            borderWidth: 0,
            hoverOffset: 15,
        }]
    };

    const formatCurrency = (val) => {
        if (!val) return '$0';
        return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', maximumFractionDigits: 0 }).format(val);
    };

    const [view, setView] = useState('landing');

    const toggleTheme = () => {
        const newTheme = theme === 'light' ? 'dark' : 'light';
        setTheme(newTheme);
        localStorage.setItem('theme', newTheme);
        document.documentElement.setAttribute('data-theme', newTheme);
    };

    const formatNumber = (val) => {
        if (!val) return '0';
        return new Intl.NumberFormat('en-US').format(val);
    };

    const LandingView = () => (
        <section className="hero-section landing-fade-in">
            <BackgroundAnimation />
            <div className="hero-visual"></div>
            <div className="hero-3d-shape"></div>
            <div className="hero-tag">Intelligent Commerce</div>
            <h1 className="hero-title">Experience the<br />Future of Retail.</h1>
            <p className="hero-subtitle">
                Transforming complex data into sleek, actionable insights.
                Powering the next generation of global retail intelligence.
            </p>
            <button className="explore-button" onClick={() => setView('dashboard')}>
                Explore Analytics <span>→</span>
            </button>
        </section>
    );

    const DashboardView = () => (
        <main className="main-content dashboard-fade-in">
            <div className="kpi-grid">
                <KPICard label="Total Revenue" value={formatCurrency(summary.total_revenue)} loading={loading} className="wide" />
                <KPICard label="Total Orders" value={formatNumber(summary.total_orders)} loading={loading} />
                <KPICard label="Avg Delivery" value={`${opsMetrics.avg_delivery_days} Days`} loading={loading} subtext="System-wide average" />
                <KPICard label="Customer CLV" value={formatCurrency(custInsights.clv)} loading={loading} className="wide" subtext="Projected Lifetime Value" />
            </div>

            <div className="tab-navigation">
                <div className="tabs">
                    <button className={activeTab === 'commercial' ? 'active' : ''} onClick={() => setActiveTab('commercial')}>Commercial</button>
                    <button className={activeTab === 'operations' ? 'active' : ''} onClick={() => setActiveTab('operations')}>Operations</button>
                    <button className={activeTab === 'customer' ? 'active' : ''} onClick={() => setActiveTab('customer')}>Customer</button>
                    <button className={activeTab === 'lineage' ? 'active' : ''} onClick={() => setActiveTab('lineage')}>Lineage</button>
                </div>
            </div>

            {activeTab === 'commercial' && (
                <div className="tab-content">
                    <div className="chart-container full-chart">
                        <div className="chart-header">City-wise Performance</div>
                        <div style={{ height: '350px' }}>
                            <Bar data={citySalesData} options={commonChartOptions} />
                        </div>
                    </div>
                    <div className="chart-container side-chart">
                        <div className="chart-header">Revenue Trend</div>
                        <div style={{ height: '350px' }}>
                            <Line data={revenueData} options={commonChartOptions} />
                        </div>
                    </div>
                    <div className="chart-container half-chart" style={{ gridColumn: 'span 6' }}>
                        <div className="chart-header">Top 5 Products</div>
                        <div style={{ height: '300px' }}>
                            <Bar data={{
                                labels: topProducts.map(p => p.product_description),
                                datasets: [{
                                    label: 'Revenue',
                                    data: topProducts.map(p => p.total_revenue),
                                    backgroundColor: colors.secondary,
                                    borderRadius: 12,
                                }]
                            }} options={{ ...commonChartOptions, indexAxis: 'y' }} />
                        </div>
                    </div>
                    <div className="chart-container half-chart" style={{ gridColumn: 'span 6' }}>
                        <div className="chart-header">Revenue by Sales Channel</div>
                        <div style={{ height: '300px' }}>
                            <Bar data={{
                                labels: channelDist.map(d => d.source_channel),
                                datasets: [{
                                    label: 'Revenue',
                                    data: channelDist.map(d => d.revenue),
                                    backgroundColor: colors.primary,
                                    borderRadius: 12,
                                }]
                            }} options={{ ...commonChartOptions, indexAxis: 'y' }} />
                        </div>
                    </div>
                </div>
            )}

            {activeTab === 'operations' && (
                <div className="tab-content">
                    <div className="chart-container full-chart">
                        <div className="chart-header">Seasonal Demand Patterns</div>
                        <div style={{ height: '350px' }}>
                            <Line data={seasonalData} options={commonChartOptions} />
                        </div>
                    </div>
                    <div className="chart-container side-chart">
                        <div className="chart-header">Inventory Efficiency</div>
                        <div className="kpi-value-large" style={{ marginTop: '50px' }}>
                            {opsMetrics.turnover_ratio}x
                        </div>
                        <p style={{ textAlign: 'center', color: 'var(--text-secondary)', fontSize: '14px' }}>
                            Inventory Turnover Ratio
                        </p>
                    </div>
                    <div className="chart-container full-chart">
                        <div className="chart-header">Current Inventory Status (Top Items)</div>
                        <div style={{ height: '300px' }}>
                            <Bar data={{
                                labels: inventory.map(d => d.product_description),
                                datasets: [{
                                    label: 'Stock',
                                    data: inventory.map(d => d.current_stock),
                                    backgroundColor: colors.primary,
                                    borderRadius: 12,
                                }]
                            }} options={commonChartOptions} />
                        </div>
                    </div>
                </div>
            )}

            {activeTab === 'customer' && (
                <div className="tab-content">
                    <div className="chart-container side-chart">
                        <div className="chart-header">Segment Distribution</div>
                        <div style={{ height: '300px', padding: '20px' }}>
                            <Doughnut data={segmentData} options={{ ...commonChartOptions, cutout: '70%' }} />
                        </div>
                    </div>
                    <div className="chart-container full-chart">
                        <div className="chart-header">Market Basket Association</div>
                        <div className="market-basket-list">
                            {custInsights.market_basket?.map((item, i) => (
                                <div key={i} className="basket-item">
                                    <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
                                        <div>
                                            <strong>{item.item_a}</strong> + <strong>{item.item_b}</strong>
                                        </div>
                                        <div className="basket-freq">{item.frequency} pairings</div>
                                    </div>
                                </div>
                            ))}
                        </div>
                    </div>
                </div>
            )}

            {activeTab === 'lineage' && (
                <div className="tab-content">
                    <div className="chart-container full-chart" style={{ gridColumn: 'span 12' }}>
                        <MedallionLineage stats={lineageStats} />
                    </div>
                </div>
            )}
        </main>
    );

    return (
        <div className="dashboard">
            <header className="header">
                <div className="header-title" onClick={() => setView('landing')} style={{ cursor: 'pointer' }}>
                    RETAILSINK
                </div>
                <div className="header-controls">
                    <button className="theme-toggle" onClick={toggleTheme}>
                        {theme === 'light' ? '🌙' : '☀️'}
                    </button>
                    {view === 'dashboard' && (
                        <button className="back-to-landing" onClick={() => setView('landing')}>
                            Back to Home
                        </button>
                    )}
                </div>
            </header>

            {view === 'landing' ? <LandingView /> : <DashboardView />}
        </div>
    );
}

export default App;
