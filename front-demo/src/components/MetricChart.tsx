import { AreaChart, Area, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';

interface MetricData {
    chain_id: number;
    metric_name: string;
    granularity: string;
    period: string;
    value: number;
    computed_at: string;
}

interface MetricChartProps {
    metricName: string;
    data: MetricData[];
    granularity: string;
}

interface ChartDataPoint {
    period: number;
    value: number;
    periodLabel: string;
}

interface CustomTooltipProps {
    active?: boolean;
    payload?: Array<{
        payload: ChartDataPoint;
        value: number;
        dataKey: string;
    }>;
    label?: number;
}

function CustomTooltip({ active, payload }: CustomTooltipProps) {
    if (active && payload && payload.length) {
        const data = payload[0].payload;
        return (
            <div className="bg-white border border-gray-200 rounded-lg p-3 shadow-lg">
                <p className="text-sm font-semibold text-gray-900 mb-1">{data.periodLabel}</p>
                <p className="text-sm text-gray-600">
                    Value: <span className="font-semibold text-blue-600">{data.value.toLocaleString()}</span>
                </p>
            </div>
        );
    }
    return null;
}

function MetricChart({ metricName, data, granularity }: MetricChartProps) {
    const chartData = data.map((item) => ({
        period: new Date(item.period).getTime(),
        value: item.value,
        periodLabel: formatDate(item.period, granularity),
    }));

    function formatDate(dateString: string, granularity: string): string {
        const date = new Date(dateString);

        switch (granularity) {
            case 'hour':
                return date.toLocaleString('en-US', { month: 'short', day: 'numeric', hour: '2-digit' });
            case 'day':
                return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
            case 'week':
                return date.toLocaleDateString('en-US', { month: 'short', day: 'numeric' });
            case 'month':
                return date.toLocaleDateString('en-US', { month: 'short', year: 'numeric' });
            default:
                return date.toLocaleDateString('en-US');
        }
    }

    function formatMetricName(name: string): string {
        return name
            .split('_')
            .map(word => word.charAt(0).toUpperCase() + word.slice(1))
            .join(' ');
    }

    function formatValue(value: number): string {
        if (value >= 1_000_000_000) {
            return (value / 1_000_000_000).toFixed(2) + 'B';
        }
        if (value >= 1_000_000) {
            return (value / 1_000_000).toFixed(2) + 'M';
        }
        if (value >= 1_000) {
            return (value / 1_000).toFixed(2) + 'K';
        }
        return value.toFixed(0);
    }

    if (!data || data.length === 0) {
        return (
            <div className="bg-white rounded-lg shadow p-6">
                <h3 className="text-lg font-semibold text-gray-900 mb-4">{formatMetricName(metricName)}</h3>
                <p className="text-gray-500">No data available</p>
            </div>
        );
    }

    return (
        <div className="bg-white rounded-lg shadow p-6">
            <h3 className="text-lg font-semibold text-gray-900 mb-4">{formatMetricName(metricName)}</h3>
            <ResponsiveContainer width="100%" height={300}>
                <AreaChart data={chartData}>
                    <defs>
                        <linearGradient id={`color-${metricName}`} x1="0" y1="0" x2="0" y2="1">
                            <stop offset="5%" stopColor="#3b82f6" stopOpacity={0.3} />
                            <stop offset="95%" stopColor="#3b82f6" stopOpacity={0} />
                        </linearGradient>
                    </defs>
                    <CartesianGrid strokeDasharray="3 3" stroke="#e5e7eb" />
                    <XAxis
                        dataKey="period"
                        tickFormatter={(value) => {
                            const item = chartData.find(d => d.period === value);
                            return item?.periodLabel || '';
                        }}
                        stroke="#6b7280"
                        style={{ fontSize: '12px' }}
                    />
                    <YAxis
                        tickFormatter={formatValue}
                        stroke="#6b7280"
                        style={{ fontSize: '12px' }}
                    />
                    <Tooltip content={<CustomTooltip />} />
                    <Area
                        type="monotone"
                        dataKey="value"
                        stroke="#3b82f6"
                        strokeWidth={2}
                        fillOpacity={1}
                        fill={`url(#color-${metricName})`}
                    />
                </AreaChart>
            </ResponsiveContainer>
        </div>
    );
}

export default MetricChart;

