import React, { useEffect, useMemo, useState } from "react";
import {
    createPaymentCounterpartyMapping,
    getPaymentCounterpartyMappings,
    getPaymentImportRuns,
    getPaymentManagementSummary,
    getPaymentSummary,
    getPaymentUnmappedCounterparties,
    importSalesDrivePayments,
    recalculatePaymentReport,
    upsertAccountBalanceAdjustment,
} from "../api/paymentReportsApi";
import { getDropshipEnterprises } from "../api/dropshipEnterpriseApi";

const pageStyle = {
    padding: "24px",
    display: "grid",
    gap: "18px",
    maxWidth: "1440px",
    margin: "0 auto",
    boxSizing: "border-box",
};

const panelStyle = {
    backgroundColor: "#ffffff",
    border: "1px solid #d9dee8",
    borderRadius: "8px",
    boxShadow: "0 6px 18px rgba(15, 23, 42, 0.05)",
};

const panelBodyStyle = {
    padding: "16px",
};

const titleStyle = {
    margin: 0,
    fontSize: "22px",
    fontWeight: 800,
    color: "#111827",
};

const sectionTitleStyle = {
    margin: 0,
    fontSize: "17px",
    fontWeight: 800,
    color: "#111827",
};

const mutedTextStyle = {
    margin: 0,
    color: "#64748b",
    fontSize: "13px",
};

const inputStyle = {
    border: "1px solid #cbd5e1",
    borderRadius: "8px",
    padding: "9px 10px",
    fontSize: "14px",
    color: "#0f172a",
    backgroundColor: "#ffffff",
};

const buttonStyle = {
    border: "1px solid #cbd5e1",
    borderRadius: "8px",
    padding: "9px 12px",
    fontSize: "14px",
    fontWeight: 700,
    cursor: "pointer",
    backgroundColor: "#ffffff",
    color: "#111827",
};

const primaryButtonStyle = {
    ...buttonStyle,
    borderColor: "#2563eb",
    backgroundColor: "#2563eb",
    color: "#ffffff",
};

const tableStyle = {
    width: "100%",
    borderCollapse: "collapse",
    fontSize: "13px",
};

const thStyle = {
    textAlign: "left",
    padding: "9px 10px",
    borderBottom: "1px solid #e2e8f0",
    color: "#475569",
    fontSize: "11px",
    textTransform: "uppercase",
    fontWeight: 800,
};

const tdStyle = {
    padding: "9px 10px",
    borderBottom: "1px solid #eef2f7",
    color: "#111827",
    verticalAlign: "top",
};

const amountStyle = {
    ...tdStyle,
    textAlign: "right",
    fontVariantNumeric: "tabular-nums",
};

const categoryLabels = {
    customer_receipt: "Клиентские поступления",
    excluded_receipt: "Исключенные поступления",
    other_receipt: "Прочие поступления / возвраты",
    internal_transfer: "Внутренние переводы",
    unknown_incoming: "Неизвестные входящие",
    supplier_payment: "Оплаты поставщикам",
    tax_payment: "Налоги",
    owner_withdrawal: "Личные перебросы",
    logistics_expense: "Логистика / Нова Пошта",
    platform_fee: "Платформы / Tabletki",
    unknown_outgoing: "Неизвестные исходящие",
};

const balanceSourceLabels = {
    manual_current_period: "Введено за период",
    carried_forward: "Перенесено",
    balance_checkpoint: "От факта на дату",
};

const qualityLabels = {
    unmapped_outgoing: "Unmapped outgoing",
    unknown_incoming: "Unknown incoming",
    payments_without_entity: "Без предприятия",
    payments_without_account: "Без счета",
    payments_without_counterparty: "Без контрагента",
    direct_internal_without_pair: "Внутренние без пары",
    unverified_entities: "Непроверенные предприятия",
};

const today = new Date();
const defaultPeriodFrom = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, "0")}-01`;
const defaultPeriodTo = `${today.getFullYear()}-${String(today.getMonth() + 1).padStart(2, "0")}-${String(today.getDate()).padStart(2, "0")}`;

const formatAmount = (value) => {
    if (value === null || value === undefined || value === "") {
        return "—";
    }
    const number = Number(value);
    if (!Number.isFinite(number)) {
        return String(value);
    }
    return number.toLocaleString("ru-RU", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
};

const sumAmount = (rows, key = "amount") =>
    rows.reduce((acc, row) => acc + (Number(row?.[key]) || 0), 0).toFixed(2);

const Panel = ({ title, children, aside }) => (
    <section style={panelStyle}>
        <div style={{ ...panelBodyStyle, display: "grid", gap: "12px" }}>
            <div style={{ display: "flex", justifyContent: "space-between", gap: "12px", alignItems: "center" }}>
                <h2 style={sectionTitleStyle}>{title}</h2>
                {aside}
            </div>
            {children}
        </div>
    </section>
);

const MetricCard = ({ label, amount, count, tone = "default" }) => {
    const colors = {
        default: ["#f8fafc", "#0f172a"],
        good: ["#ecfdf5", "#047857"],
        warn: ["#fffbeb", "#b45309"],
        bad: ["#fef2f2", "#b91c1c"],
        blue: ["#eff6ff", "#1d4ed8"],
    };
    const [backgroundColor, color] = colors[tone] || colors.default;
    return (
        <div
            style={{
                backgroundColor,
                border: "1px solid #e2e8f0",
                borderRadius: "8px",
                padding: "14px",
                minHeight: "88px",
                display: "grid",
                gap: "6px",
            }}
        >
            <div style={{ color: "#64748b", fontSize: "12px", fontWeight: 800, textTransform: "uppercase" }}>{label}</div>
            <div style={{ color, fontSize: "22px", fontWeight: 900, fontVariantNumeric: "tabular-nums" }}>{formatAmount(amount)}</div>
            {count !== undefined && <div style={{ color: "#64748b", fontSize: "13px" }}>{count} платежей</div>}
        </div>
    );
};

const PaymentReportsPage = () => {
    const [periodFrom, setPeriodFrom] = useState(defaultPeriodFrom);
    const [periodTo, setPeriodTo] = useState(defaultPeriodTo);
    const [management, setManagement] = useState(null);
    const [summary, setSummary] = useState(null);
    const [imports, setImports] = useState([]);
    const [unmapped, setUnmapped] = useState([]);
    const [mappings, setMappings] = useState([]);
    const [suppliers, setSuppliers] = useState([]);
    const [activeTab, setActiveTab] = useState("summary");
    const [loading, setLoading] = useState(false);
    const [actionMessage, setActionMessage] = useState("");
    const [error, setError] = useState("");
    const [balanceDrafts, setBalanceDrafts] = useState({});
    const [mappingDrafts, setMappingDrafts] = useState({});

    const loadReports = async () => {
        setLoading(true);
        setError("");
        try {
            const [managementData, summaryData, importsData, unmappedData, mappingsData, suppliersData] = await Promise.all([
                getPaymentManagementSummary({ periodFrom, periodTo }),
                getPaymentSummary({ periodFrom, periodTo }),
                getPaymentImportRuns({ limit: 8 }),
                getPaymentUnmappedCounterparties({ periodFrom, periodTo, limit: 100, examples: 2 }),
                getPaymentCounterpartyMappings({ limit: 500 }),
                getDropshipEnterprises(),
            ]);
            setManagement(managementData);
            setSummary(summaryData);
            setImports(importsData);
            setUnmapped(unmappedData?.groups || []);
            setMappings(mappingsData || []);
            setSuppliers(suppliersData || []);
        } catch (err) {
            setError(err?.response?.data?.detail || err.message || "Не удалось загрузить отчеты.");
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        loadReports();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    const runImport = async () => {
        setLoading(true);
        setError("");
        setActionMessage("");
        try {
            const result = await importSalesDrivePayments({ periodFrom, periodTo, paymentType: "all" });
            setActionMessage(`Импорт: incoming=${result.incoming_count}, outcoming=${result.outcoming_count}, created=${result.created_count}, updated=${result.updated_count}`);
            await recalculatePaymentReport({ periodFrom, periodTo });
            await loadReports();
        } catch (err) {
            setError(err?.response?.data?.detail || err.message || "Импорт не выполнен.");
            setLoading(false);
        }
    };

    const runRecalculate = async () => {
        setLoading(true);
        setError("");
        setActionMessage("");
        try {
            const result = await recalculatePaymentReport({ periodFrom, periodTo });
            setActionMessage(`Пересчет: всего=${result.total_payments}, mapped=${result.supplier_mapped}, unmapped=${result.supplier_unmapped}`);
            await loadReports();
        } catch (err) {
            setError(err?.response?.data?.detail || err.message || "Пересчет не выполнен.");
            setLoading(false);
        }
    };

    const saveAccountBalance = async (accountId) => {
        const draft = balanceDrafts[accountId] || {};
        setLoading(true);
        setError("");
        try {
            await upsertAccountBalanceAdjustment({
                account_id: accountId,
                balance_date: draft.balance_date || periodTo,
                actual_balance: draft.actual_balance || null,
                opening_balance_adjustment: "0",
                closing_balance_adjustment: "0",
                comment: draft.comment || null,
                created_by: localStorage.getItem("user_login") || "admin-panel",
            });
            setActionMessage("Фактический остаток счета сохранен.");
            await loadReports();
        } catch (err) {
            setError(err?.response?.data?.detail || err.message || "Не удалось сохранить остатки.");
            setLoading(false);
        }
    };

    const createMapping = async (mappingKey, source = {}) => {
        const draft = mappingDrafts[mappingKey] || {};
        const supplierCode = draft.supplier_code || "";
        const matchType = draft.match_type || (source.counterparty_tax_id ? "tax_id" : "exact");
        const fieldScope = matchType === "tax_id" ? "tax_id" : draft.field_scope || "counterparty_name";
        const pattern = draft.counterparty_pattern ?? source.counterparty_name ?? "";
        const taxId = draft.counterparty_tax_id ?? source.counterparty_tax_id ?? (matchType === "tax_id" ? pattern : "");

        setLoading(true);
        setError("");
        setActionMessage("");
        try {
            await createPaymentCounterpartyMapping({
                supplier_code: supplierCode,
                match_type: matchType,
                field_scope: fieldScope,
                counterparty_pattern: matchType === "tax_id" ? null : pattern,
                counterparty_tax_id: matchType === "tax_id" ? taxId : taxId || null,
                priority: Number(draft.priority || 100),
                is_active: true,
                notes: draft.notes || null,
                created_by: localStorage.getItem("user_login") || "admin-panel",
            });
            await recalculatePaymentReport({ periodFrom, periodTo });
            setActionMessage("Правило мапинга создано, период пересчитан.");
            await loadReports();
        } catch (err) {
            setError(err?.response?.data?.detail || err.message || "Не удалось создать правило мапинга.");
            setLoading(false);
        }
    };

    const incomingByCategory = useMemo(() => summary?.incoming_by_category || [], [summary]);
    const outgoingByCategory = useMemo(() => summary?.outgoing_by_category || [], [summary]);
    const supplierPayments = useMemo(() => management?.supplier_payments_by_entity || [], [management]);
    const accountMovements = useMemo(() => management?.account_movements || [], [management]);
    const dataQuality = management?.data_quality || {};

    const kpis = useMemo(() => {
        const byIncoming = Object.fromEntries(incomingByCategory.map((item) => [item.category, item]));
        const byOutgoing = Object.fromEntries(outgoingByCategory.map((item) => [item.category, item]));
        const externalIncoming = Number(summary?.incoming_total?.amount || 0) - Number(byIncoming.internal_transfer?.amount || 0);
        const externalOutgoing = Number(summary?.outcoming_total?.amount || 0) - Number(byOutgoing.internal_transfer?.amount || 0);
        return {
            customer: byIncoming.customer_receipt || { count: 0, amount: "0" },
            suppliers: byOutgoing.supplier_payment || { count: 0, amount: "0" },
            taxes: byOutgoing.tax_payment || { count: 0, amount: "0" },
            owner: byOutgoing.owner_withdrawal || { count: 0, amount: "0" },
            netExternal: (externalIncoming - externalOutgoing).toFixed(2),
        };
    }, [incomingByCategory, outgoingByCategory, summary]);

    const tabs = [
        ["summary", "Сводка"],
        ["accounts", "Счета"],
        ["incoming", "Входящие"],
        ["outgoing", "Исходящие"],
        ["suppliers", "Поставщики"],
        ["mapping", "Мапинг"],
        ["quality", "Контроль"],
        ["imports", "Импорты"],
    ];

    return (
        <div style={pageStyle}>
            <section style={panelStyle}>
                <div style={{ ...panelBodyStyle, display: "grid", gap: "14px" }}>
                    <div style={{ display: "flex", justifyContent: "space-between", gap: "16px", flexWrap: "wrap" }}>
                        <div>
                            <h1 style={titleStyle}>Платежная отчетность</h1>
                            <p style={mutedTextStyle}>Управленческая сводка по платежам, счетам, поставщикам и качеству данных.</p>
                        </div>
                        <div style={{ display: "flex", gap: "8px", alignItems: "end", flexWrap: "wrap" }}>
                            <label style={{ display: "grid", gap: "5px", fontSize: "12px", fontWeight: 700, color: "#475569" }}>
                                С
                                <input style={inputStyle} type="date" value={periodFrom} onChange={(event) => setPeriodFrom(event.target.value)} />
                            </label>
                            <label style={{ display: "grid", gap: "5px", fontSize: "12px", fontWeight: 700, color: "#475569" }}>
                                По
                                <input style={inputStyle} type="date" value={periodTo} onChange={(event) => setPeriodTo(event.target.value)} />
                            </label>
                            <button type="button" style={buttonStyle} onClick={loadReports} disabled={loading}>
                                Загрузить отчет
                            </button>
                            <button type="button" style={buttonStyle} onClick={runRecalculate} disabled={loading}>
                                Пересчитать
                            </button>
                            <button type="button" style={primaryButtonStyle} onClick={runImport} disabled={loading}>
                                Импорт из SalesDrive
                            </button>
                        </div>
                    </div>
                    {(loading || error || actionMessage) && (
                        <div style={{ color: error ? "#b91c1c" : "#475569", fontSize: "14px", fontWeight: 700 }}>
                            {loading ? "Загрузка..." : error || actionMessage}
                        </div>
                    )}
                </div>
            </section>

            <div style={{ display: "flex", gap: "8px", flexWrap: "wrap" }}>
                {tabs.map(([key, label]) => (
                    <button
                        key={key}
                        type="button"
                        style={{
                            ...buttonStyle,
                            backgroundColor: activeTab === key ? "#111827" : "#ffffff",
                            color: activeTab === key ? "#ffffff" : "#111827",
                        }}
                        onClick={() => setActiveTab(key)}
                    >
                        {label}
                    </button>
                ))}
            </div>

            {activeTab === "summary" && (
                <>
                    <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(210px, 1fr))", gap: "12px" }}>
                        <MetricCard label="Клиентские поступления" amount={kpis.customer.amount} count={kpis.customer.count} tone="good" />
                        <MetricCard label="Оплаты поставщикам" amount={kpis.suppliers.amount} count={kpis.suppliers.count} tone="blue" />
                        <MetricCard label="Налоги" amount={kpis.taxes.amount} count={kpis.taxes.count} tone="warn" />
                        <MetricCard label="Личные перебросы" amount={kpis.owner.amount} count={kpis.owner.count} />
                        <MetricCard label="Внешний cash flow" amount={kpis.netExternal} tone={Number(kpis.netExternal) >= 0 ? "good" : "bad"} />
                    </div>
                    <Panel title="Сводка по категориям">
                        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(360px, 1fr))", gap: "16px" }}>
                            <CategoryTable title="Входящие" rows={incomingByCategory} />
                            <CategoryTable title="Исходящие" rows={outgoingByCategory} />
                        </div>
                    </Panel>
                    <AccountBalancesSummary rows={accountMovements} />
                </>
            )}

            {activeTab === "accounts" && (
                <AccountsTable
                    rows={accountMovements}
                    drafts={balanceDrafts}
                    setDrafts={setBalanceDrafts}
                    onSave={saveAccountBalance}
                    loading={loading}
                    defaultBalanceDate={periodTo}
                />
            )}
            {activeTab === "incoming" && <BreakdownTable title="Входящие по предприятиям и счетам" rows={management?.incoming_by_category_entity_account || []} />}
            {activeTab === "outgoing" && <BreakdownTable title="Исходящие по предприятиям и счетам" rows={management?.outgoing_by_category_entity_account || []} />}
            {activeTab === "suppliers" && <SupplierTable rows={supplierPayments} />}
            {activeTab === "mapping" && (
                <MappingPanel
                    unmapped={unmapped}
                    mappings={mappings}
                    suppliers={suppliers}
                    drafts={mappingDrafts}
                    setDrafts={setMappingDrafts}
                    onCreate={createMapping}
                    loading={loading}
                />
            )}
            {activeTab === "quality" && <QualityPanel quality={dataQuality} />}
            {activeTab === "imports" && <ImportsTable rows={imports} />}
        </div>
    );
};

const CategoryTable = ({ title, rows }) => (
    <div>
        <h3 style={{ margin: "0 0 8px", fontSize: "15px", color: "#111827" }}>{title}</h3>
        <table style={tableStyle}>
            <thead>
                <tr>
                    <th style={thStyle}>Категория</th>
                    <th style={thStyle}>Кол-во</th>
                    <th style={{ ...thStyle, textAlign: "right" }}>Сумма</th>
                </tr>
            </thead>
            <tbody>
                {rows.map((row) => (
                    <tr key={`${row.category}-${row.mapping_status || ""}`}>
                        <td style={tdStyle}>{categoryLabels[row.category] || row.category || "—"}</td>
                        <td style={tdStyle}>{row.count}</td>
                        <td style={amountStyle}>{formatAmount(row.amount)}</td>
                    </tr>
                ))}
            </tbody>
        </table>
    </div>
);

const AccountBalancesSummary = ({ rows }) => (
    <Panel title="Расчетные остатки на счетах" aside={<span style={mutedTextStyle}>Итого {formatAmount(sumAmount(rows, "calculated_closing_balance"))}</span>}>
        <div style={{ overflowX: "auto" }}>
            <table style={tableStyle}>
                <thead>
                    <tr>
                        <th style={thStyle}>Предприятие</th>
                        <th style={thStyle}>Счет</th>
                        <th style={{ ...thStyle, textAlign: "right" }}>Начало</th>
                        <th style={{ ...thStyle, textAlign: "right" }}>Входящие</th>
                        <th style={{ ...thStyle, textAlign: "right" }}>Исходящие</th>
                        <th style={{ ...thStyle, textAlign: "right" }}>Конец расчет</th>
                        <th style={thStyle}>Источник</th>
                    </tr>
                </thead>
                <tbody>
                    {rows.map((row) => (
                        <tr key={row.account_id}>
                            <td style={tdStyle}>{row.business_entity_name}</td>
                            <td style={tdStyle}>
                                <strong>{row.account_label || "—"}</strong>
                                <div style={{ color: "#64748b", fontSize: "12px" }}>{row.account_number}</div>
                            </td>
                            <td style={amountStyle}>{formatAmount(row.opening_balance)}</td>
                            <td style={amountStyle}>{formatAmount(row.incoming?.amount)}</td>
                            <td style={amountStyle}>{formatAmount(row.outcoming?.amount)}</td>
                            <td style={amountStyle}>{formatAmount(row.calculated_closing_balance)}</td>
                            <td style={tdStyle}>
                                {balanceSourceLabels[row.opening_balance_source] || "—"}
                                {row.opening_balance_source_period && (
                                    <div style={{ color: "#64748b", fontSize: "12px" }}>{row.opening_balance_source_period.slice(0, 7)}</div>
                                )}
                            </td>
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    </Panel>
);

const AccountsTable = ({ rows, drafts, setDrafts, onSave, loading, defaultBalanceDate }) => {
    const updateDraft = (accountId, field, value) => {
        setDrafts((prev) => ({
            ...prev,
            [accountId]: {
                balance_date: rows.find((row) => row.account_id === accountId)?.balance_checkpoint_date || defaultBalanceDate,
                actual_balance: rows.find((row) => row.account_id === accountId)?.balance_checkpoint_amount || "",
                comment: "",
                ...(prev[accountId] || {}),
                [field]: value,
            },
        }));
    };
    const draftValue = (row, field, fallback) => drafts[row.account_id]?.[field] ?? fallback ?? "";
    return (
    <Panel title="Движение и контроль остатков по счетам">
        <div style={{ overflowX: "auto" }}>
            <table style={tableStyle}>
                <thead>
                    <tr>
                        <th style={thStyle}>Предприятие</th>
                        <th style={thStyle}>Счет</th>
                        <th style={thStyle}>Факт зафиксирован</th>
                        <th style={{ ...thStyle, textAlign: "right" }}>Факт</th>
                        <th style={{ ...thStyle, textAlign: "right" }}>Начало периода</th>
                        <th style={{ ...thStyle, textAlign: "right" }}>Входящие</th>
                        <th style={{ ...thStyle, textAlign: "right" }}>Исходящие</th>
                        <th style={{ ...thStyle, textAlign: "right" }}>Конец периода расчет</th>
                        <th style={thStyle}>Контроль</th>
                        <th style={thStyle}>Ввести факт</th>
                    </tr>
                </thead>
                <tbody>
                    {rows.map((row) => {
                        const hasWarning = row.balance_checkpoint_status === "warning";
                        return (
                        <tr key={row.account_id}>
                            <td style={tdStyle}>{row.business_entity_name}</td>
                            <td style={tdStyle}>
                                <strong>{row.account_label || "—"}</strong>
                                <div style={{ color: "#64748b", fontSize: "12px" }}>{row.account_number}</div>
                            </td>
                            <td style={tdStyle}>{row.balance_checkpoint_date || "—"}</td>
                            <td style={amountStyle}>{formatAmount(row.balance_checkpoint_amount)}</td>
                            <td style={amountStyle}>{formatAmount(row.opening_balance)}</td>
                            <td style={amountStyle}>{formatAmount(row.incoming?.amount)}</td>
                            <td style={amountStyle}>{formatAmount(row.outcoming?.amount)}</td>
                            <td style={amountStyle}>{formatAmount(row.calculated_closing_balance)}</td>
                            <td style={{ ...tdStyle, minWidth: "190px" }}>
                                {hasWarning ? (
                                    <div style={{ color: "#b91c1c", fontWeight: 800 }}>
                                        Расхождение {formatAmount(row.balance_checkpoint_difference)}
                                    </div>
                                ) : (
                                    <div style={{ color: "#047857", fontWeight: 800 }}>
                                        {row.balance_checkpoint_status === "ok" ? "Совпадает" : "Нет предыдущего факта"}
                                    </div>
                                )}
                                {row.balance_checkpoint_calculated_amount && (
                                    <div style={{ color: "#64748b", fontSize: "12px" }}>
                                        расчет на дату: {formatAmount(row.balance_checkpoint_calculated_amount)}
                                    </div>
                                )}
                                <div style={{ color: "#64748b", fontSize: "12px" }}>
                                    начало: {balanceSourceLabels[row.opening_balance_source] || "нет"}{row.opening_balance_source_period ? ` (${row.opening_balance_source_period})` : ""}
                                </div>
                            </td>
                            <td style={{ ...tdStyle, minWidth: "320px" }}>
                                <div style={{ display: "grid", gridTemplateColumns: "150px minmax(120px, 1fr)", gap: "8px" }}>
                                    <input
                                        style={inputStyle}
                                        type="date"
                                        value={draftValue(row, "balance_date", row.balance_checkpoint_date || defaultBalanceDate)}
                                        onChange={(event) => updateDraft(row.account_id, "balance_date", event.target.value)}
                                    />
                                    <input
                                         style={inputStyle}
                                         type="number"
                                         step="0.01"
                                         placeholder="Факт остаток"
                                         value={draftValue(row, "actual_balance", row.balance_checkpoint_amount || "")}
                                         onChange={(event) => updateDraft(row.account_id, "actual_balance", event.target.value)}
                                     />
                                </div>
                                <div style={{ display: "flex", gap: "8px", marginTop: "8px" }}>
                                    <input
                                        style={{ ...inputStyle, flex: 1 }}
                                        placeholder="Комментарий"
                                        value={draftValue(row, "comment", "")}
                                        onChange={(event) => updateDraft(row.account_id, "comment", event.target.value)}
                                    />
                                    <button type="button" style={primaryButtonStyle} disabled={loading} onClick={() => onSave(row.account_id)}>
                                        Зафиксировать
                                    </button>
                                </div>
                            </td>
                        </tr>
                    );})}
                </tbody>
            </table>
        </div>
    </Panel>
    );
};

const MappingPanel = ({ unmapped, mappings, suppliers, drafts, setDrafts, onCreate, loading }) => {
    const supplierOptions = [...suppliers].sort((left, right) => String(left.code).localeCompare(String(right.code)));
    const updateDraft = (key, field, value) => {
        setDrafts((prev) => ({
            ...prev,
            [key]: {
                match_type: "exact",
                field_scope: "counterparty_name",
                priority: "100",
                ...(prev[key] || {}),
                [field]: value,
            },
        }));
    };
    const draftValue = (key, field, fallback = "") => drafts[key]?.[field] ?? fallback;
    const renderSupplierSelect = (key) => (
        <select
            style={inputStyle}
            value={draftValue(key, "supplier_code")}
            onChange={(event) => updateDraft(key, "supplier_code", event.target.value)}
        >
            <option value="">Поставщик</option>
            {supplierOptions.map((supplier) => (
                <option key={supplier.code} value={supplier.code}>
                    {supplier.code} · {supplier.name}
                </option>
            ))}
        </select>
    );

    return (
        <div style={{ display: "grid", gap: "16px" }}>
            <Panel title="Создать правило">
                <div style={{ display: "grid", gridTemplateColumns: "minmax(180px, 1fr) minmax(160px, 0.7fr) minmax(160px, 0.7fr) minmax(220px, 1.2fr) 90px auto", gap: "8px", alignItems: "center" }}>
                    {renderSupplierSelect("manual")}
                    <select style={inputStyle} value={draftValue("manual", "match_type", "exact")} onChange={(event) => updateDraft("manual", "match_type", event.target.value)}>
                        <option value="exact">exact</option>
                        <option value="contains">contains</option>
                        <option value="search_text_contains">search text</option>
                        <option value="tax_id">tax id</option>
                    </select>
                    <select style={inputStyle} value={draftValue("manual", "field_scope", "counterparty_name")} onChange={(event) => updateDraft("manual", "field_scope", event.target.value)}>
                        <option value="counterparty_name">Плательщик/получатель</option>
                        <option value="purpose">Назначение</option>
                        <option value="comment">Комментарий</option>
                        <option value="search_text">Весь текст</option>
                        <option value="tax_id">ЕДРПОУ</option>
                    </select>
                    <input style={inputStyle} placeholder="Паттерн или название" value={draftValue("manual", "counterparty_pattern")} onChange={(event) => updateDraft("manual", "counterparty_pattern", event.target.value)} />
                    <input style={inputStyle} type="number" placeholder="Приоритет" value={draftValue("manual", "priority", "100")} onChange={(event) => updateDraft("manual", "priority", event.target.value)} />
                    <button type="button" style={primaryButtonStyle} disabled={loading} onClick={() => onCreate("manual", {})}>
                        Создать
                    </button>
                </div>
            </Panel>

            <Panel title="Не сопоставлено за период" aside={<span style={mutedTextStyle}>{unmapped.length} групп</span>}>
                <div style={{ overflowX: "auto" }}>
                    <table style={tableStyle}>
                        <thead>
                            <tr>
                                <th style={thStyle}>Контрагент</th>
                                <th style={thStyle}>ЕДРПОУ</th>
                                <th style={thStyle}>Кол-во</th>
                                <th style={{ ...thStyle, textAlign: "right" }}>Сумма</th>
                                <th style={thStyle}>Пример</th>
                                <th style={thStyle}>Мапинг</th>
                            </tr>
                        </thead>
                        <tbody>
                            {unmapped.map((row, index) => {
                                const key = `${row.counterparty_name || "empty"}-${row.counterparty_tax_id || "empty"}-${index}`;
                                return (
                                    <tr key={key}>
                                        <td style={tdStyle}>{row.counterparty_name || "—"}</td>
                                        <td style={tdStyle}>{row.counterparty_tax_id || "—"}</td>
                                        <td style={tdStyle}>{row.count}</td>
                                        <td style={amountStyle}>{formatAmount(row.amount)}</td>
                                        <td style={{ ...tdStyle, maxWidth: "360px" }}>{row.examples?.[0]?.purpose || "—"}</td>
                                        <td style={{ ...tdStyle, minWidth: "520px" }}>
                                            <div style={{ display: "grid", gridTemplateColumns: "minmax(180px, 1fr) minmax(130px, 0.6fr) minmax(120px, 0.5fr) auto", gap: "8px" }}>
                                                {renderSupplierSelect(key)}
                                                <select style={inputStyle} value={draftValue(key, "match_type", row.counterparty_tax_id ? "tax_id" : "exact")} onChange={(event) => updateDraft(key, "match_type", event.target.value)}>
                                                    <option value="tax_id">tax id</option>
                                                    <option value="exact">exact</option>
                                                    <option value="contains">contains</option>
                                                    <option value="search_text_contains">search text</option>
                                                </select>
                                                <input style={inputStyle} type="number" placeholder="Приоритет" value={draftValue(key, "priority", "100")} onChange={(event) => updateDraft(key, "priority", event.target.value)} />
                                                <button type="button" style={primaryButtonStyle} disabled={loading} onClick={() => onCreate(key, row)}>
                                                    Связать
                                                </button>
                                            </div>
                                        </td>
                                    </tr>
                                );
                            })}
                        </tbody>
                    </table>
                </div>
            </Panel>

            <Panel title="Действующие правила" aside={<span style={mutedTextStyle}>{mappings.length} правил</span>}>
                <div style={{ overflowX: "auto" }}>
                    <table style={tableStyle}>
                        <thead>
                            <tr>
                                <th style={thStyle}>Приоритет</th>
                                <th style={thStyle}>Поставщик</th>
                                <th style={thStyle}>Тип</th>
                                <th style={thStyle}>Поле</th>
                                <th style={thStyle}>Паттерн</th>
                                <th style={thStyle}>ЕДРПОУ</th>
                                <th style={thStyle}>Статус</th>
                            </tr>
                        </thead>
                        <tbody>
                            {mappings.map((row) => (
                                <tr key={row.id}>
                                    <td style={tdStyle}>{row.priority}</td>
                                    <td style={tdStyle}>
                                        <strong>{row.supplier_code}</strong>
                                        <div style={{ color: "#64748b", fontSize: "12px" }}>{row.supplier_name || "—"}</div>
                                    </td>
                                    <td style={tdStyle}>{row.match_type}</td>
                                    <td style={tdStyle}>{row.field_scope}</td>
                                    <td style={tdStyle}>{row.counterparty_pattern || "—"}</td>
                                    <td style={tdStyle}>{row.counterparty_tax_id || "—"}</td>
                                    <td style={tdStyle}>{row.is_active ? "active" : "inactive"}</td>
                                </tr>
                            ))}
                        </tbody>
                    </table>
                </div>
            </Panel>
        </div>
    );
};

const BreakdownTable = ({ title, rows }) => (
    <Panel title={title} aside={<span style={mutedTextStyle}>Итого {formatAmount(sumAmount(rows))}</span>}>
        <div style={{ overflowX: "auto" }}>
            <table style={tableStyle}>
                <thead>
                    <tr>
                        <th style={thStyle}>Предприятие</th>
                        <th style={thStyle}>Счет</th>
                        <th style={thStyle}>Категория</th>
                        <th style={thStyle}>Кол-во</th>
                        <th style={{ ...thStyle, textAlign: "right" }}>Сумма</th>
                    </tr>
                </thead>
                <tbody>
                    {rows.map((row, index) => (
                        <tr key={`${row.category}-${row.business_entity_name}-${row.account_label}-${index}`}>
                            <td style={tdStyle}>{row.business_entity_name || "—"}</td>
                            <td style={tdStyle}>{row.account_label || "—"}</td>
                            <td style={tdStyle}>{categoryLabels[row.category] || row.category || "—"}</td>
                            <td style={tdStyle}>{row.count}</td>
                            <td style={amountStyle}>{formatAmount(row.amount)}</td>
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    </Panel>
);

const SupplierTable = ({ rows }) => (
    <Panel title="Оплаты поставщикам" aside={<span style={mutedTextStyle}>Итого {formatAmount(sumAmount(rows))}</span>}>
        <div style={{ overflowX: "auto" }}>
            <table style={tableStyle}>
                <thead>
                    <tr>
                        <th style={thStyle}>Предприятие</th>
                        <th style={thStyle}>Код</th>
                        <th style={thStyle}>Поставщик</th>
                        <th style={thStyle}>Кол-во</th>
                        <th style={{ ...thStyle, textAlign: "right" }}>Сумма</th>
                    </tr>
                </thead>
                <tbody>
                    {rows.map((row) => (
                        <tr key={`${row.business_entity_name}-${row.supplier_code}`}>
                            <td style={tdStyle}>{row.business_entity_name || "—"}</td>
                            <td style={tdStyle}>{row.supplier_code}</td>
                            <td style={tdStyle}>{row.supplier_name}</td>
                            <td style={tdStyle}>{row.count}</td>
                            <td style={amountStyle}>{formatAmount(row.amount)}</td>
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    </Panel>
);

const QualityPanel = ({ quality }) => (
    <Panel title="Контроль качества данных">
        <div style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))", gap: "12px" }}>
            {Object.entries(quality || {}).map(([key, item]) => {
                const hasIssue = Number(item.count || 0) > 0;
                return (
                    <MetricCard
                        key={key}
                        label={qualityLabels[key] || key}
                        amount={item.amount}
                        count={item.count}
                        tone={hasIssue ? "warn" : "good"}
                    />
                );
            })}
        </div>
    </Panel>
);

const ImportsTable = ({ rows }) => (
    <Panel title="История импортов">
        <div style={{ overflowX: "auto" }}>
            <table style={tableStyle}>
                <thead>
                    <tr>
                        <th style={thStyle}>ID</th>
                        <th style={thStyle}>Статус</th>
                        <th style={thStyle}>Период</th>
                        <th style={thStyle}>Incoming</th>
                        <th style={thStyle}>Outcoming</th>
                        <th style={thStyle}>Created</th>
                        <th style={thStyle}>Updated</th>
                    </tr>
                </thead>
                <tbody>
                    {rows.map((row) => (
                        <tr key={row.id}>
                            <td style={tdStyle}>{row.id}</td>
                            <td style={tdStyle}>{row.status}</td>
                            <td style={tdStyle}>{row.period_from?.slice(0, 10)} - {row.period_to?.slice(0, 10)}</td>
                            <td style={tdStyle}>{row.incoming_count}</td>
                            <td style={tdStyle}>{row.outcoming_count}</td>
                            <td style={tdStyle}>{row.created_count}</td>
                            <td style={tdStyle}>{row.updated_count}</td>
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    </Panel>
);

export default PaymentReportsPage;
