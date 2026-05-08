import React, { useEffect, useMemo, useState } from "react";
import {
    getOrderExpenseSettings,
    getOrderReportByEnterprise,
    getOrderReportBySupplier,
    getOrderReportDetails,
    getOrderReportFunnel,
    getOrderReportSummary,
    syncOrderReports,
    upsertOrderExpenseSetting,
} from "../api/orderReportsApi";

const pageStyle = { padding: 24, display: "grid", gap: 16, maxWidth: 1500, margin: "0 auto", boxSizing: "border-box" };
const panelStyle = { background: "#fff", border: "1px solid #d9dee8", borderRadius: 8, padding: 16 };
const inputStyle = { border: "1px solid #cbd5e1", borderRadius: 8, padding: "9px 10px", fontSize: 14, background: "#fff" };
const buttonStyle = { ...inputStyle, fontWeight: 800, cursor: "pointer" };
const primaryButtonStyle = { ...buttonStyle, background: "#2563eb", color: "#fff", borderColor: "#2563eb" };
const tableStyle = { width: "100%", borderCollapse: "collapse", fontSize: 13 };
const thStyle = { textAlign: "left", padding: "9px 10px", borderBottom: "1px solid #e2e8f0", color: "#475569", fontSize: 11, textTransform: "uppercase" };
const tdStyle = { padding: "9px 10px", borderBottom: "1px solid #eef2f7", verticalAlign: "top" };
const amountStyle = { ...tdStyle, textAlign: "right", fontVariantNumeric: "tabular-nums" };

const today = new Date();
const defaultPeriodFrom = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, "0")}-01`;
const defaultPeriodTo = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, "0")}-${String(today.getDate()).padStart(2, "0")}`;

const formatAmount = (value) => {
    const number = Number(value || 0);
    return Number.isFinite(number) ? number.toLocaleString("ru-RU", { minimumFractionDigits: 2, maximumFractionDigits: 2 }) : String(value || "0");
};

const MetricCard = ({ label, value, tone = "default" }) => {
    const tones = {
        default: ["#f8fafc", "#0f172a"],
        good: ["#ecfdf5", "#047857"],
        warn: ["#fffbeb", "#b45309"],
        bad: ["#fef2f2", "#b91c1c"],
        blue: ["#eff6ff", "#1d4ed8"],
    };
    const [background, color] = tones[tone] || tones.default;
    return (
        <div style={{ background, color, border: "1px solid #e2e8f0", borderRadius: 8, padding: 14, minHeight: 82 }}>
            <div style={{ color: "#64748b", fontSize: 12, fontWeight: 900, textTransform: "uppercase" }}>{label}</div>
            <div style={{ marginTop: 8, fontSize: 22, fontWeight: 900, fontVariantNumeric: "tabular-nums" }}>{value}</div>
        </div>
    );
};

const TabButton = ({ active, children, onClick }) => (
    <button
        style={{ ...buttonStyle, background: active ? "#111827" : "#fff", color: active ? "#fff" : "#111827" }}
        onClick={onClick}
    >
        {children}
    </button>
);

const OrderReportsPage = () => {
    const [periodFrom, setPeriodFrom] = useState(defaultPeriodFrom);
    const [periodTo, setPeriodTo] = useState(defaultPeriodTo);
    const [enterpriseCode, setEnterpriseCode] = useState("");
    const [summary, setSummary] = useState(null);
    const [funnel, setFunnel] = useState([]);
    const [byEnterprise, setByEnterprise] = useState([]);
    const [bySupplier, setBySupplier] = useState([]);
    const [details, setDetails] = useState({ rows: [] });
    const [settings, setSettings] = useState([]);
    const [activeTab, setActiveTab] = useState("overview");
    const [loading, setLoading] = useState(false);
    const [error, setError] = useState("");
    const [message, setMessage] = useState("");
    const [settingsDrafts, setSettingsDrafts] = useState({});

    const enterpriseOptions = summary?.business_enterprises || [];

    const loadReports = async () => {
        setLoading(true);
        setError("");
        try {
            const params = { periodFrom, periodTo, enterpriseCode: enterpriseCode || null };
            const [summaryData, funnelData, enterpriseData, supplierData, detailsData, settingsData] = await Promise.all([
                getOrderReportSummary(params),
                getOrderReportFunnel(params),
                getOrderReportByEnterprise(params),
                getOrderReportBySupplier(params),
                getOrderReportDetails(params),
                getOrderExpenseSettings(),
            ]);
            setSummary(summaryData);
            setFunnel(funnelData || []);
            setByEnterprise(enterpriseData || []);
            setBySupplier(supplierData || []);
            setDetails(detailsData || { rows: [] });
            setSettings(settingsData || []);
        } catch (err) {
            setError(err?.response?.data?.detail || err.message || "Не удалось загрузить отчеты по заказам.");
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        loadReports();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    const runSync = async () => {
        setLoading(true);
        setMessage("");
        setError("");
        try {
            const result = await syncOrderReports({ periodFrom, periodTo, enterpriseCode: enterpriseCode || null });
            const reasons = result.failed_reasons ? `, reasons=${JSON.stringify(result.failed_reasons)}` : "";
            const errorMessage = result.error_message ? `, error=${result.error_message}` : "";
            setMessage(`Sync: ${result.status}, created=${result.created_count}, updated=${result.updated_count}, failed=${result.failed_count}${reasons}${errorMessage}`);
            await loadReports();
        } catch (err) {
            setError(err?.response?.data?.detail || err.message || "Sync не выполнен.");
            setLoading(false);
        }
    };

    const saveSetting = async (row) => {
        const draft = settingsDrafts[row.enterprise_code] || {};
        await upsertOrderExpenseSetting({
            enterprise_code: row.enterprise_code,
            expense_percent: draft.expense_percent ?? row.expense_percent ?? "0",
            active_from: draft.active_from ?? row.active_from ?? defaultPeriodFrom,
            active_to: draft.active_to ?? row.active_to ?? null,
        });
        await loadReports();
    };

    const maxFunnelCount = useMemo(() => Math.max(1, ...funnel.map((item) => Number(item.count || 0))), [funnel]);

    return (
        <div style={pageStyle}>
            <div style={{ display: "flex", justifyContent: "space-between", gap: 12, alignItems: "center" }}>
                <div>
                    <h1 style={{ margin: 0, fontSize: 24, color: "#111827" }}>Отчеты по заказам</h1>
                    <p style={{ margin: "6px 0 0", color: "#64748b", fontSize: 13 }}>Business enterprises, статусы, продажи, отказы, возвраты и прибыль.</p>
                </div>
                <div style={{ display: "flex", gap: 8, alignItems: "center", flexWrap: "wrap", justifyContent: "flex-end" }}>
                    <input style={inputStyle} type="date" value={periodFrom} onChange={(e) => setPeriodFrom(e.target.value)} />
                    <input style={inputStyle} type="date" value={periodTo} onChange={(e) => setPeriodTo(e.target.value)} />
                    <select style={inputStyle} value={enterpriseCode} onChange={(e) => setEnterpriseCode(e.target.value)}>
                        <option value="">Все Business</option>
                        {enterpriseOptions.map((item) => (
                            <option key={item.enterprise_code} value={item.enterprise_code}>
                                {item.enterprise_name} ({item.enterprise_code})
                            </option>
                        ))}
                    </select>
                    <button style={primaryButtonStyle} onClick={loadReports} disabled={loading}>Обновить</button>
                    <button style={buttonStyle} onClick={runSync} disabled={loading}>Sync SalesDrive</button>
                </div>
            </div>

            {error && <div style={{ ...panelStyle, borderColor: "#fecaca", color: "#b91c1c" }}>{error}</div>}
            {message && <div style={{ ...panelStyle, borderColor: "#bbf7d0", color: "#047857" }}>{message}</div>}

            <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(170px, 1fr))", gap: 12 }}>
                <MetricCard label="Всего заказов" value={summary?.total_orders || 0} tone="blue" />
                <MetricCard label="Продаж" value={summary?.sales_count || 0} tone="good" />
                <MetricCard label="Отказов" value={summary?.cancelled_count || 0} tone="bad" />
                <MetricCard label="% отказов" value={`${formatAmount(summary?.refusal_rate)}%`} tone="bad" />
                <MetricCard label="Возвратов" value={summary?.return_count || 0} tone="warn" />
                <MetricCard label="% возвратов" value={`${formatAmount(summary?.return_rate)}%`} tone="warn" />
                <MetricCard label="Выручка продаж" value={formatAmount(summary?.sale_amount)} tone="good" />
                <MetricCard label="Чистая прибыль" value={formatAmount(summary?.net_profit_amount)} tone="default" />
            </div>

            <div style={{ ...panelStyle, display: "flex", gap: 8, flexWrap: "wrap" }}>
                {[
                    ["overview", "Обзор"],
                    ["funnel", "Воронка"],
                    ["enterprise", "По предприятиям"],
                    ["supplier", "По поставщикам"],
                    ["details", "Заказы"],
                    ["settings", "Настройки расходов"],
                ].map(([key, label]) => (
                    <TabButton key={key} active={activeTab === key} onClick={() => setActiveTab(key)}>{label}</TabButton>
                ))}
            </div>

            {activeTab === "overview" && (
                <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))", gap: 16 }}>
                    <section style={panelStyle}>
                        <h2 style={{ marginTop: 0, fontSize: 18 }}>Предварительно</h2>
                        <table style={tableStyle}><tbody>
                            <tr><td style={tdStyle}>Сумма всех заказов</td><td style={amountStyle}>{formatAmount(summary?.order_amount)}</td></tr>
                            <tr><td style={tdStyle}>Штук в заказах</td><td style={amountStyle}>{formatAmount(summary?.items_quantity)}</td></tr>
                            <tr><td style={tdStyle}>Активные / в работе</td><td style={amountStyle}>{summary?.active_orders || 0}</td></tr>
                            <tr><td style={tdStyle}>Удаленные</td><td style={amountStyle}>{summary?.deleted_count || 0}</td></tr>
                        </tbody></table>
                    </section>
                    <section style={panelStyle}>
                        <h2 style={{ marginTop: 0, fontSize: 18 }}>Окончательно по продажам</h2>
                        <table style={tableStyle}><tbody>
                            <tr><td style={tdStyle}>Штук продано</td><td style={amountStyle}>{formatAmount(summary?.sale_quantity)}</td></tr>
                            <tr><td style={tdStyle}>Себестоимость</td><td style={amountStyle}>{formatAmount(summary?.supplier_cost_total)}</td></tr>
                            <tr><td style={tdStyle}>Валовая прибыль</td><td style={amountStyle}>{formatAmount(summary?.gross_profit_amount)}</td></tr>
                            <tr><td style={tdStyle}>Расходы</td><td style={amountStyle}>{formatAmount(summary?.expense_amount)}</td></tr>
                        </tbody></table>
                    </section>
                </div>
            )}

            {activeTab === "funnel" && (
                <section style={panelStyle}>
                    <table style={tableStyle}>
                        <thead><tr><th style={thStyle}>Статус</th><th style={thStyle}>Группа</th><th style={amountStyle}>Кол-во</th><th style={amountStyle}>Сумма заказов</th></tr></thead>
                        <tbody>
                            {funnel.map((row) => (
                                <tr key={row.status_id}>
                                    <td style={tdStyle}>
                                        <div style={{ fontWeight: 800 }}>{row.status_name}</div>
                                        <div style={{ height: 7, background: "#e2e8f0", borderRadius: 999, marginTop: 6 }}>
                                            <div style={{ width: `${(Number(row.count || 0) / maxFunnelCount) * 100}%`, height: 7, background: "#2563eb", borderRadius: 999 }} />
                                        </div>
                                    </td>
                                    <td style={tdStyle}>{row.status_group || "—"}</td>
                                    <td style={amountStyle}>{row.count}</td>
                                    <td style={amountStyle}>{formatAmount(row.order_amount)}</td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </section>
            )}

            {activeTab === "enterprise" && <ReportTable rows={byEnterprise} mode="enterprise" />}
            {activeTab === "supplier" && <SupplierTable rows={bySupplier} />}
            {activeTab === "details" && <DetailsTable rows={details.rows || []} />}
            {activeTab === "settings" && (
                <section style={panelStyle}>
                    <table style={tableStyle}>
                        <thead><tr><th style={thStyle}>Предприятие</th><th style={amountStyle}>Expense %</th><th style={thStyle}>Active from</th><th style={thStyle}>Active to</th><th style={thStyle}>Действия</th></tr></thead>
                        <tbody>
                            {settings.map((row) => {
                                const draft = settingsDrafts[row.enterprise_code] || {};
                                return (
                                    <tr key={`${row.enterprise_code}-${row.setting_id || "new"}`}>
                                        <td style={tdStyle}>{row.enterprise_name}<br /><span style={{ color: "#64748b" }}>{row.enterprise_code}</span></td>
                                        <td style={amountStyle}><input style={{ ...inputStyle, width: 90 }} value={draft.expense_percent ?? row.expense_percent ?? "0"} onChange={(e) => setSettingsDrafts((prev) => ({ ...prev, [row.enterprise_code]: { ...(prev[row.enterprise_code] || {}), expense_percent: e.target.value } }))} /></td>
                                        <td style={tdStyle}><input style={inputStyle} type="date" value={draft.active_from ?? row.active_from ?? defaultPeriodFrom} onChange={(e) => setSettingsDrafts((prev) => ({ ...prev, [row.enterprise_code]: { ...(prev[row.enterprise_code] || {}), active_from: e.target.value } }))} /></td>
                                        <td style={tdStyle}><input style={inputStyle} type="date" value={draft.active_to ?? row.active_to ?? ""} onChange={(e) => setSettingsDrafts((prev) => ({ ...prev, [row.enterprise_code]: { ...(prev[row.enterprise_code] || {}), active_to: e.target.value } }))} /></td>
                                        <td style={tdStyle}><button style={buttonStyle} onClick={() => saveSetting(row)}>Сохранить</button></td>
                                    </tr>
                                );
                            })}
                        </tbody>
                    </table>
                </section>
            )}
        </div>
    );
};

const ReportTable = ({ rows }) => (
    <section style={panelStyle}>
        <table style={tableStyle}>
            <thead><tr><th style={thStyle}>Предприятие</th><th style={amountStyle}>Заказы</th><th style={amountStyle}>Продажи</th><th style={amountStyle}>Отказы</th><th style={amountStyle}>% отказов</th><th style={amountStyle}>Возвраты</th><th style={amountStyle}>% возвратов</th><th style={amountStyle}>Выручка</th><th style={amountStyle}>Чистая прибыль</th></tr></thead>
            <tbody>{rows.map((row) => <tr key={row.enterprise_code}><td style={tdStyle}>{row.enterprise_name}<br /><span style={{ color: "#64748b" }}>{row.enterprise_code}</span></td><td style={amountStyle}>{row.total_orders}</td><td style={amountStyle}>{row.sales_count}</td><td style={amountStyle}>{row.cancelled_count}</td><td style={amountStyle}>{formatAmount(row.refusal_rate)}%</td><td style={amountStyle}>{row.return_count}</td><td style={amountStyle}>{formatAmount(row.return_rate)}%</td><td style={amountStyle}>{formatAmount(row.sale_amount)}</td><td style={amountStyle}>{formatAmount(row.net_profit_amount)}</td></tr>)}</tbody>
        </table>
    </section>
);

const SupplierTable = ({ rows }) => (
    <section style={panelStyle}>
        <table style={tableStyle}>
            <thead><tr><th style={thStyle}>Поставщик</th><th style={amountStyle}>Заказы</th><th style={amountStyle}>Штук</th><th style={amountStyle}>Продажи</th><th style={amountStyle}>Себестоимость</th><th style={amountStyle}>Валовая</th><th style={amountStyle}>Доля</th><th style={amountStyle}>Расходы</th><th style={amountStyle}>Чистая</th></tr></thead>
            <tbody>{rows.map((row) => <tr key={row.supplier_code}><td style={tdStyle}>{row.supplier_name}<br /><span style={{ color: "#64748b" }}>{row.supplier_code}</span></td><td style={amountStyle}>{row.orders_count}</td><td style={amountStyle}>{formatAmount(row.quantity)}</td><td style={amountStyle}>{formatAmount(row.sale_amount)}</td><td style={amountStyle}>{formatAmount(row.cost_amount)}</td><td style={amountStyle}>{formatAmount(row.gross_profit_amount)}</td><td style={amountStyle}>{formatAmount(row.sales_share_percent)}%</td><td style={amountStyle}>{formatAmount(row.allocated_expense_amount)}</td><td style={amountStyle}>{formatAmount(row.net_profit_amount)}</td></tr>)}</tbody>
        </table>
    </section>
);

const DetailsTable = ({ rows }) => (
    <section style={panelStyle}>
        <table style={tableStyle}>
            <thead><tr><th style={thStyle}>Дата</th><th style={thStyle}>Предприятие</th><th style={thStyle}>Заказ</th><th style={thStyle}>Статус</th><th style={amountStyle}>Сумма</th><th style={amountStyle}>Себест.</th><th style={amountStyle}>Валовая</th><th style={amountStyle}>Расходы</th><th style={amountStyle}>Чистая</th><th style={thStyle}>Позиции</th></tr></thead>
            <tbody>{rows.map((row) => <tr key={row.id}><td style={tdStyle}>{row.order_created_at ? row.order_created_at.slice(0, 10) : "—"}</td><td style={tdStyle}>{row.enterprise_code}</td><td style={tdStyle}>{row.order_number || row.external_order_id}<br /><span style={{ color: "#64748b" }}>{row.source}</span></td><td style={tdStyle}>{row.status_name}<br /><span style={{ color: "#64748b" }}>{row.status_group}</span></td><td style={amountStyle}>{formatAmount(row.order_amount)}</td><td style={amountStyle}>{formatAmount(row.supplier_cost_total)}</td><td style={amountStyle}>{formatAmount(row.gross_profit_amount)}</td><td style={amountStyle}>{formatAmount(row.expense_amount)}</td><td style={amountStyle}>{formatAmount(row.net_profit_amount)}</td><td style={tdStyle}>{(row.items || []).slice(0, 3).map((item) => <div key={item.line_index}>{item.product_name || item.sku} · {item.supplier_code || "unmapped"}</div>)}</td></tr>)}</tbody>
        </table>
    </section>
);

export default OrderReportsPage;
