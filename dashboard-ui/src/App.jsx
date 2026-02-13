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

function App() {
    const [summary, setSummary] = useState({});
    const [revenueTrend, setRevenueTrend] = useState([]);
    const [topProducts, setTopProducts] = useState([]);
    const [channelDist, setChannelDist] = useState([]);
    const [inventory, setInventory] = useState([]);
    const [loading, setLoading] = useState(true);

    const fetchData = async () => {
        setLoading(true);
        try {
            const [sumRes, revRes, prodRes, chanRes, invRes] = await Promise.all([
                fetch(`${API_BASE}/summary`).then(r => r.json()),
                fetch(`${API_BASE}/revenue-trend`).then(r => r.json()),
                fetch(`${API_BASE}/top-products`).then(r => r.json()),
                fetch(`${API_BASE}/sales-channel`).then(r => r.json()),
                fetch(`${API_BASE}/inventory-status`).then(r => r.json()),
            ]);

            setSummary(sumRes);
            setRevenueTrend(revRes);
            setTopProducts(prodRes);
            setChannelDist(chanRes);
            setInventory(invRes);
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
