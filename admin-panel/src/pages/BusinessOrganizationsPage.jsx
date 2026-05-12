import React, { useCallback, useEffect, useMemo, useState } from "react";
import {
    createBusinessAccount,
    createBusinessOrganization,
    createCheckboxExclusion,
    createCheckboxRegister,
    getBusinessOrganizations,
    testCheckboxRegister,
    updateBusinessAccount,
    updateBusinessOrganization,
    updateCheckboxExclusion,
    updateCheckboxRegister,
} from "../api/businessOrganizationsApi";
import { getBusinessStores } from "../api/suppliersApi";
import { handleAuthError } from "../api/developerApi";

const pageStyle = { padding: "24px", maxWidth: "1440px", margin: "0 auto", display: "grid", gap: "20px" };
const cardStyle = { background: "#fff", border: "1px solid #d9dee8", borderRadius: "12px", padding: "20px 24px", boxShadow: "0 8px 24px rgba(15,23,42,.06)" };
const gridStyle = { display: "grid", gridTemplateColumns: "320px 1fr", gap: "20px", alignItems: "start" };
const formGridStyle = { display: "grid", gridTemplateColumns: "repeat(2, minmax(220px, 1fr))", gap: "14px 18px" };
const inputStyle = { width: "100%", border: "1px solid #cbd5e1", borderRadius: "10px", padding: "10px 12px", fontSize: "14px", boxSizing: "border-box" };
const labelStyle = { display: "grid", gap: "6px", fontSize: "14px", fontWeight: 600, color: "#111827" };
const buttonStyle = { border: "1px solid transparent", borderRadius: "10px", padding: "10px 14px", fontWeight: 700, cursor: "pointer", background: "#2563eb", color: "#fff" };
const secondaryButtonStyle = { ...buttonStyle, background: "#eff6ff", color: "#1d4ed8", borderColor: "#bfdbfe" };
const tableCellStyle = { padding: "10px 12px", borderBottom: "1px solid #e5e7eb", fontSize: "13px", textAlign: "left", verticalAlign: "top" };
const mutedTextStyle = { margin: 0, color: "#64748b", fontSize: "14px", lineHeight: 1.5 };
const errorStyle = { border: "1px solid #fca5a5", background: "#fef2f2", color: "#b91c1c", borderRadius: "10px", padding: "12px 14px" };
const successStyle = { border: "1px solid #bbf7d0", background: "#f0fdf4", color: "#166534", borderRadius: "10px", padding: "12px 14px" };

const emptyOrganization = {
    salesdrive_organization_id: "",
    short_name: "",
    full_name: "",
    tax_id: "",
    entity_type: "other",
    verification_status: "needs_review",
    vat_enabled: false,
    vat_payer: false,
    without_stamp: false,
    signer_name: "",
    signer_position: "",
    chief_accountant_name: "",
    cashier_name: "",
    address: "",
    postal_code: "",
    city: "",
    region: "",
    country: "Україна",
    phone: "",
    is_active: true,
    notes: "",
};

const emptyAccount = {
    salesdrive_account_id: "",
    account_number: "",
    account_title: "",
    label: "",
    card_mask: "",
    currency: "UAH",
    bank_name: "",
    mfo: "",
    is_active: true,
};

const emptyRegister = {
    business_store_id: "",
    enterprise_code: "",
    register_name: "",
    cash_register_code: "",
    checkbox_license_key: "",
    cashier_login: "",
    cashier_password: "",
    cashier_pin: "",
    api_base_url: "https://api.checkbox.ua",
    is_test_mode: true,
    is_active: true,
    is_default: false,
    shift_open_mode: "on_fiscalization",
    shift_open_time: "",
    shift_close_time: "23:50",
    timezone: "Europe/Kiev",
    receipt_notifications_enabled: false,
    shift_notifications_enabled: true,
    notes: "",
};

const emptyExclusion = {
    cash_register_id: "",
    supplier_code: "",
    supplier_name: "",
    is_active: true,
    comment: "",
};

const normalizeText = (value) => {
    const normalized = String(value ?? "").trim();
    return normalized || null;
};

const normalizeId = (value) => {
    const normalized = String(value ?? "").trim();
    return normalized ? Number(normalized) : null;
};

const formatError = (error, fallback) => {
    const detail = error?.response?.data?.detail;
    if (Array.isArray(detail)) {
        return detail.map((item) => item.msg || JSON.stringify(item)).join("; ");
    }
    return detail || error?.message || fallback;
};

const organizationToDraft = (item) => ({
    ...emptyOrganization,
    ...item,
    salesdrive_organization_id: item?.salesdrive_organization_id || "",
    short_name: item?.short_name || "",
    full_name: item?.full_name || "",
    tax_id: item?.tax_id || "",
    signer_name: item?.signer_name || "",
    signer_position: item?.signer_position || "",
    chief_accountant_name: item?.chief_accountant_name || "",
    cashier_name: item?.cashier_name || "",
    address: item?.address || "",
    postal_code: item?.postal_code || "",
    city: item?.city || "",
    region: item?.region || "",
    country: item?.country || "",
    phone: item?.phone || "",
    notes: item?.notes || "",
});

const accountToDraft = (item) => ({
    ...emptyAccount,
    ...item,
    salesdrive_account_id: item?.salesdrive_account_id || "",
    account_number: item?.account_number || "",
    account_title: item?.account_title || "",
    label: item?.label || "",
    card_mask: item?.card_mask || "",
    currency: item?.currency || "UAH",
    bank_name: item?.bank_name || "",
    mfo: item?.mfo || "",
});

const registerToDraft = (item) => ({
    ...emptyRegister,
    ...item,
    business_store_id: item?.business_store_id ?? "",
    enterprise_code: item?.enterprise_code || "",
    checkbox_license_key: "",
    cashier_password: "",
    cashier_pin: "",
    cashier_login: item?.cashier_login || "",
    api_base_url: item?.api_base_url || "https://api.checkbox.ua",
    shift_open_time: item?.shift_open_time || "",
    shift_close_time: item?.shift_close_time || "",
    notes: item?.notes || "",
});

const exclusionToDraft = (item) => ({
    ...emptyExclusion,
    ...item,
    cash_register_id: item?.cash_register_id ?? "",
    supplier_code: item?.supplier_code || "",
    supplier_name: item?.supplier_name || "",
    comment: item?.comment || "",
});

const Field = ({ label, children }) => (
    <label style={labelStyle}>
        <span>{label}</span>
        {children}
    </label>
);

const BusinessOrganizationsPage = () => {
    const [organizations, setOrganizations] = useState([]);
    const [stores, setStores] = useState([]);
    const [selectedId, setSelectedId] = useState(null);
    const [activeTab, setActiveTab] = useState("organization");
    const [organizationDraft, setOrganizationDraft] = useState(emptyOrganization);
    const [accountDraft, setAccountDraft] = useState(emptyAccount);
    const [editingAccountId, setEditingAccountId] = useState(null);
    const [registerDraft, setRegisterDraft] = useState(emptyRegister);
    const [editingRegisterId, setEditingRegisterId] = useState(null);
    const [exclusionDraft, setExclusionDraft] = useState(emptyExclusion);
    const [editingExclusionId, setEditingExclusionId] = useState(null);
    const [testRegisterId, setTestRegisterId] = useState("");
    const [testResult, setTestResult] = useState(null);
    const [loading, setLoading] = useState(true);
    const [saving, setSaving] = useState(false);
    const [error, setError] = useState("");
    const [success, setSuccess] = useState("");

    const selectedOrganization = useMemo(
        () => organizations.find((item) => item.id === selectedId) || null,
        [organizations, selectedId],
    );

    const loadData = useCallback(async () => {
        setLoading(true);
        setError("");
        try {
            const [organizationRows, storeRows] = await Promise.all([
                getBusinessOrganizations(),
                getBusinessStores(),
            ]);
            setOrganizations(organizationRows);
            setStores(storeRows);
            const nextSelected = selectedId || organizationRows[0]?.id || null;
            setSelectedId(nextSelected);
            const nextOrganization = organizationRows.find((item) => item.id === nextSelected) || organizationRows[0] || null;
            setOrganizationDraft(nextOrganization ? organizationToDraft(nextOrganization) : emptyOrganization);
        } catch (loadError) {
            handleAuthError(loadError);
            setError(formatError(loadError, "Не удалось загрузить организации."));
        } finally {
            setLoading(false);
        }
    }, [selectedId]);

    useEffect(() => {
        loadData();
    }, [loadData]);

    const selectOrganization = (item) => {
        setSelectedId(item?.id || null);
        setOrganizationDraft(item ? organizationToDraft(item) : emptyOrganization);
        setAccountDraft(emptyAccount);
        setEditingAccountId(null);
        setRegisterDraft(emptyRegister);
        setEditingRegisterId(null);
        setExclusionDraft(emptyExclusion);
        setEditingExclusionId(null);
        setSuccess("");
        setError("");
    };

    const saveOrganization = async () => {
        setSaving(true);
        setError("");
        setSuccess("");
        try {
            const payload = {
                ...organizationDraft,
                salesdrive_organization_id: normalizeText(organizationDraft.salesdrive_organization_id),
                short_name: normalizeText(organizationDraft.short_name),
                full_name: normalizeText(organizationDraft.full_name),
                tax_id: normalizeText(organizationDraft.tax_id),
                signer_name: normalizeText(organizationDraft.signer_name),
                signer_position: normalizeText(organizationDraft.signer_position),
                chief_accountant_name: normalizeText(organizationDraft.chief_accountant_name),
                cashier_name: normalizeText(organizationDraft.cashier_name),
                address: normalizeText(organizationDraft.address),
                postal_code: normalizeText(organizationDraft.postal_code),
                city: normalizeText(organizationDraft.city),
                region: normalizeText(organizationDraft.region),
                country: normalizeText(organizationDraft.country),
                phone: normalizeText(organizationDraft.phone),
                notes: normalizeText(organizationDraft.notes),
            };
            const saved = selectedOrganization
                ? await updateBusinessOrganization(selectedOrganization.id, payload)
                : await createBusinessOrganization(payload);
            await loadData();
            setSelectedId(saved.id);
            setOrganizationDraft(organizationToDraft(saved));
            setSuccess("Организация сохранена.");
        } catch (saveError) {
            handleAuthError(saveError);
            setError(formatError(saveError, "Не удалось сохранить организацию."));
        } finally {
            setSaving(false);
        }
    };

    const saveAccount = async () => {
        if (!selectedOrganization) return;
        setSaving(true);
        setError("");
        try {
            const payload = {
                ...accountDraft,
                salesdrive_account_id: normalizeText(accountDraft.salesdrive_account_id),
                account_number: normalizeText(accountDraft.account_number),
                account_title: normalizeText(accountDraft.account_title),
                label: normalizeText(accountDraft.label),
                card_mask: normalizeText(accountDraft.card_mask),
                currency: normalizeText(accountDraft.currency) || "UAH",
                bank_name: normalizeText(accountDraft.bank_name),
                mfo: normalizeText(accountDraft.mfo),
            };
            if (editingAccountId) {
                await updateBusinessAccount(selectedOrganization.id, editingAccountId, payload);
            } else {
                await createBusinessAccount(selectedOrganization.id, payload);
            }
            await loadData();
            setAccountDraft(emptyAccount);
            setEditingAccountId(null);
            setSuccess("Счет сохранен.");
        } catch (saveError) {
            handleAuthError(saveError);
            setError(formatError(saveError, "Не удалось сохранить счет."));
        } finally {
            setSaving(false);
        }
    };

    const saveRegister = async () => {
        if (!selectedOrganization) return;
        setSaving(true);
        setError("");
        try {
            const payload = {
                ...registerDraft,
                business_store_id: normalizeId(registerDraft.business_store_id),
                enterprise_code: normalizeText(registerDraft.enterprise_code),
                register_name: normalizeText(registerDraft.register_name),
                cash_register_code: normalizeText(registerDraft.cash_register_code),
                checkbox_license_key: normalizeText(registerDraft.checkbox_license_key),
                cashier_login: normalizeText(registerDraft.cashier_login),
                cashier_password: normalizeText(registerDraft.cashier_password),
                cashier_pin: normalizeText(registerDraft.cashier_pin),
                api_base_url: normalizeText(registerDraft.api_base_url),
                shift_open_time: normalizeText(registerDraft.shift_open_time),
                shift_close_time: normalizeText(registerDraft.shift_close_time),
                timezone: normalizeText(registerDraft.timezone) || "Europe/Kiev",
                notes: normalizeText(registerDraft.notes),
            };
            if (editingRegisterId) {
                await updateCheckboxRegister(selectedOrganization.id, editingRegisterId, payload);
            } else {
                await createCheckboxRegister(selectedOrganization.id, payload);
            }
            await loadData();
            setRegisterDraft(emptyRegister);
            setEditingRegisterId(null);
            setSuccess("Касса сохранена.");
        } catch (saveError) {
            handleAuthError(saveError);
            setError(formatError(saveError, "Не удалось сохранить кассу."));
        } finally {
            setSaving(false);
        }
    };

    const saveExclusion = async () => {
        if (!selectedOrganization) return;
        setSaving(true);
        setError("");
        try {
            const payload = {
                ...exclusionDraft,
                cash_register_id: normalizeId(exclusionDraft.cash_register_id),
                supplier_code: normalizeText(exclusionDraft.supplier_code),
                supplier_name: normalizeText(exclusionDraft.supplier_name),
                comment: normalizeText(exclusionDraft.comment),
            };
            if (editingExclusionId) {
                await updateCheckboxExclusion(selectedOrganization.id, editingExclusionId, payload);
            } else {
                await createCheckboxExclusion(selectedOrganization.id, payload);
            }
            await loadData();
            setExclusionDraft(emptyExclusion);
            setEditingExclusionId(null);
            setSuccess("Исключение сохранено.");
        } catch (saveError) {
            handleAuthError(saveError);
            setError(formatError(saveError, "Не удалось сохранить исключение."));
        } finally {
            setSaving(false);
        }
    };

    const runRegisterTest = async (action) => {
        if (!selectedOrganization || !testRegisterId) return;
        setSaving(true);
        setError("");
        setTestResult(null);
        try {
            const result = await testCheckboxRegister(selectedOrganization.id, testRegisterId, action);
            setTestResult(result);
            setSuccess(`Checkbox test ${action}: ok`);
        } catch (testError) {
            handleAuthError(testError);
            setError(formatError(testError, "Не удалось выполнить тест кассы."));
        } finally {
            setSaving(false);
        }
    };

    const tabButton = (key, label) => (
        <button
            type="button"
            style={activeTab === key ? buttonStyle : secondaryButtonStyle}
            onClick={() => setActiveTab(key)}
        >
            {label}
        </button>
    );

    return (
        <div style={pageStyle}>
            <div style={cardStyle}>
                <h1 style={{ margin: 0, fontSize: "28px", color: "#111827" }}>Организации и кассы</h1>
                <p style={mutedTextStyle}>Юрлица, банковские счета, Checkbox-кассы и исключения для фискализации.</p>
            </div>
            {error ? <div style={errorStyle}>{error}</div> : null}
            {success ? <div style={successStyle}>{success}</div> : null}
            {loading ? <div style={cardStyle}>Загрузка...</div> : (
                <div style={gridStyle}>
                    <div style={{ ...cardStyle, display: "grid", gap: "12px" }}>
                        <button type="button" style={buttonStyle} onClick={() => selectOrganization(null)}>
                            + Добавить организацию
                        </button>
                        {organizations.map((item) => (
                            <button
                                key={item.id}
                                type="button"
                                style={{
                                    ...secondaryButtonStyle,
                                    textAlign: "left",
                                    background: item.id === selectedId ? "#dbeafe" : "#eff6ff",
                                }}
                                onClick={() => selectOrganization(item)}
                            >
                                <div>{item.short_name}</div>
                                <div style={{ fontSize: 12, fontWeight: 500 }}>
                                    SalesDrive {item.salesdrive_organization_id || "—"} / {item.tax_id || "без ЕДРПОУ"}
                                </div>
                            </button>
                        ))}
                    </div>

                    <div style={{ ...cardStyle, display: "grid", gap: "18px" }}>
                        <div style={{ display: "flex", gap: "10px", flexWrap: "wrap" }}>
                            {tabButton("organization", "Основные данные")}
                            {tabButton("accounts", "Банковские счета")}
                            {tabButton("registers", "Кассы Checkbox")}
                            {tabButton("exclusions", "Исключения чеков")}
                            {tabButton("tests", "Тесты")}
                        </div>

                        {activeTab === "organization" ? (
                            <div style={{ display: "grid", gap: "16px" }}>
                                <div style={formGridStyle}>
                                    <Field label="Короткое наименование">
                                        <input style={inputStyle} value={organizationDraft.short_name} onChange={(e) => setOrganizationDraft((p) => ({ ...p, short_name: e.target.value }))} />
                                    </Field>
                                    <Field label="SalesDrive ID организации">
                                        <input style={inputStyle} value={organizationDraft.salesdrive_organization_id} onChange={(e) => setOrganizationDraft((p) => ({ ...p, salesdrive_organization_id: e.target.value }))} />
                                    </Field>
                                    <Field label="Полное наименование">
                                        <input style={inputStyle} value={organizationDraft.full_name} onChange={(e) => setOrganizationDraft((p) => ({ ...p, full_name: e.target.value }))} />
                                    </Field>
                                    <Field label="ЕДРПОУ / РНОКПП">
                                        <input style={inputStyle} value={organizationDraft.tax_id} onChange={(e) => setOrganizationDraft((p) => ({ ...p, tax_id: e.target.value }))} />
                                    </Field>
                                    <Field label="Тип">
                                        <select style={inputStyle} value={organizationDraft.entity_type} onChange={(e) => setOrganizationDraft((p) => ({ ...p, entity_type: e.target.value }))}>
                                            <option value="fop">ФОП</option>
                                            <option value="company">Компания</option>
                                            <option value="individual">Физлицо</option>
                                            <option value="other">Другое</option>
                                        </select>
                                    </Field>
                                    <Field label="Телефон">
                                        <input style={inputStyle} value={organizationDraft.phone} onChange={(e) => setOrganizationDraft((p) => ({ ...p, phone: e.target.value }))} />
                                    </Field>
                                    <Field label="Адрес">
                                        <input style={inputStyle} value={organizationDraft.address} onChange={(e) => setOrganizationDraft((p) => ({ ...p, address: e.target.value }))} />
                                    </Field>
                                    <Field label="Город">
                                        <input style={inputStyle} value={organizationDraft.city} onChange={(e) => setOrganizationDraft((p) => ({ ...p, city: e.target.value }))} />
                                    </Field>
                                    <Field label="Подписант">
                                        <input style={inputStyle} value={organizationDraft.signer_name} onChange={(e) => setOrganizationDraft((p) => ({ ...p, signer_name: e.target.value }))} />
                                    </Field>
                                    <Field label="Должность подписанта">
                                        <input style={inputStyle} value={organizationDraft.signer_position} onChange={(e) => setOrganizationDraft((p) => ({ ...p, signer_position: e.target.value }))} />
                                    </Field>
                                    <Field label="Главный бухгалтер">
                                        <input style={inputStyle} value={organizationDraft.chief_accountant_name} onChange={(e) => setOrganizationDraft((p) => ({ ...p, chief_accountant_name: e.target.value }))} />
                                    </Field>
                                    <Field label="Кассир / ответственное лицо">
                                        <input style={inputStyle} value={organizationDraft.cashier_name} onChange={(e) => setOrganizationDraft((p) => ({ ...p, cashier_name: e.target.value }))} />
                                    </Field>
                                    <label style={labelStyle}><span><input type="checkbox" checked={organizationDraft.vat_payer} onChange={(e) => setOrganizationDraft((p) => ({ ...p, vat_payer: e.target.checked, vat_enabled: e.target.checked }))} /> Плательщик НДС</span></label>
                                    <label style={labelStyle}><span><input type="checkbox" checked={organizationDraft.is_active} onChange={(e) => setOrganizationDraft((p) => ({ ...p, is_active: e.target.checked }))} /> Организация активна</span></label>
                                </div>
                                <Field label="Комментарий">
                                    <textarea style={{ ...inputStyle, minHeight: 80 }} value={organizationDraft.notes} onChange={(e) => setOrganizationDraft((p) => ({ ...p, notes: e.target.value }))} />
                                </Field>
                                <button type="button" style={buttonStyle} onClick={saveOrganization} disabled={saving}>Сохранить организацию</button>
                            </div>
                        ) : null}

                        {activeTab === "accounts" && selectedOrganization ? (
                            <div style={{ display: "grid", gap: "16px" }}>
                                <table style={{ width: "100%", borderCollapse: "collapse" }}>
                                    <thead><tr><th style={tableCellStyle}>Счет</th><th style={tableCellStyle}>Банк</th><th style={tableCellStyle}>МФО</th><th style={tableCellStyle}>Статус</th></tr></thead>
                                    <tbody>{(selectedOrganization.accounts || []).map((item) => (
                                        <tr key={item.id} onClick={() => { setEditingAccountId(item.id); setAccountDraft(accountToDraft(item)); }} style={{ cursor: "pointer" }}>
                                            <td style={tableCellStyle}>{item.account_number}<br />{item.label || item.account_title || ""}</td>
                                            <td style={tableCellStyle}>{item.bank_name || "—"}</td>
                                            <td style={tableCellStyle}>{item.mfo || "—"}</td>
                                            <td style={tableCellStyle}>{item.is_active ? "active" : "off"}</td>
                                        </tr>
                                    ))}</tbody>
                                </table>
                                <div style={formGridStyle}>
                                    <Field label="IBAN / счет"><input style={inputStyle} value={accountDraft.account_number} onChange={(e) => setAccountDraft((p) => ({ ...p, account_number: e.target.value }))} /></Field>
                                    <Field label="Метка"><input style={inputStyle} value={accountDraft.label} onChange={(e) => setAccountDraft((p) => ({ ...p, label: e.target.value }))} /></Field>
                                    <Field label="Банк"><input style={inputStyle} value={accountDraft.bank_name} onChange={(e) => setAccountDraft((p) => ({ ...p, bank_name: e.target.value }))} /></Field>
                                    <Field label="МФО"><input style={inputStyle} value={accountDraft.mfo} onChange={(e) => setAccountDraft((p) => ({ ...p, mfo: e.target.value }))} /></Field>
                                </div>
                                <button type="button" style={buttonStyle} onClick={saveAccount} disabled={saving}>{editingAccountId ? "Сохранить счет" : "Добавить счет"}</button>
                            </div>
                        ) : null}

                        {activeTab === "registers" && selectedOrganization ? (
                            <div style={{ display: "grid", gap: "16px" }}>
                                <table style={{ width: "100%", borderCollapse: "collapse" }}>
                                    <thead><tr><th style={tableCellStyle}>Касса</th><th style={tableCellStyle}>Привязка</th><th style={tableCellStyle}>Смена</th><th style={tableCellStyle}>Секреты</th></tr></thead>
                                    <tbody>{(selectedOrganization.cash_registers || []).map((item) => (
                                        <tr key={item.id} onClick={() => { setEditingRegisterId(item.id); setRegisterDraft(registerToDraft(item)); }} style={{ cursor: "pointer" }}>
                                            <td style={tableCellStyle}>{item.register_name}<br />{item.cash_register_code}</td>
                                            <td style={tableCellStyle}>{item.business_store_id ? `store ${item.business_store_id}` : "вся организация"}<br />{item.enterprise_code || ""}</td>
                                            <td style={tableCellStyle}>{item.shift_open_mode}<br />закрытие {item.shift_close_time || "—"}</td>
                                            <td style={tableCellStyle}>license {item.checkbox_license_key_set ? "есть" : "нет"}<br />pin {item.cashier_pin_set ? "есть" : "нет"}</td>
                                        </tr>
                                    ))}</tbody>
                                </table>
                                <div style={formGridStyle}>
                                    <Field label="Название кассы"><input style={inputStyle} value={registerDraft.register_name} onChange={(e) => setRegisterDraft((p) => ({ ...p, register_name: e.target.value }))} /></Field>
                                    <Field label="Код кассы / alias"><input style={inputStyle} value={registerDraft.cash_register_code} onChange={(e) => setRegisterDraft((p) => ({ ...p, cash_register_code: e.target.value }))} /></Field>
                                    <Field label="Магазин"><select style={inputStyle} value={registerDraft.business_store_id} onChange={(e) => setRegisterDraft((p) => ({ ...p, business_store_id: e.target.value }))}><option value="">Вся организация</option>{stores.map((store) => <option key={store.id} value={store.id}>{store.store_name} / {store.tabletki_branch}</option>)}</select></Field>
                                    <Field label="Enterprise code"><input style={inputStyle} value={registerDraft.enterprise_code} onChange={(e) => setRegisterDraft((p) => ({ ...p, enterprise_code: e.target.value }))} /></Field>
                                    <Field label="License key"><input style={inputStyle} type="password" placeholder={editingRegisterId ? "оставить пустым, чтобы не менять" : ""} value={registerDraft.checkbox_license_key} onChange={(e) => setRegisterDraft((p) => ({ ...p, checkbox_license_key: e.target.value }))} /></Field>
                                    <Field label="Cashier login"><input style={inputStyle} value={registerDraft.cashier_login} onChange={(e) => setRegisterDraft((p) => ({ ...p, cashier_login: e.target.value }))} /></Field>
                                    <Field label="Cashier password"><input style={inputStyle} type="password" placeholder={editingRegisterId ? "оставить пустым, чтобы не менять" : ""} value={registerDraft.cashier_password} onChange={(e) => setRegisterDraft((p) => ({ ...p, cashier_password: e.target.value }))} /></Field>
                                    <Field label="Cashier PIN"><input style={inputStyle} type="password" placeholder={editingRegisterId ? "оставить пустым, чтобы не менять" : ""} value={registerDraft.cashier_pin} onChange={(e) => setRegisterDraft((p) => ({ ...p, cashier_pin: e.target.value }))} /></Field>
                                    <Field label="Открытие смены"><select style={inputStyle} value={registerDraft.shift_open_mode} onChange={(e) => setRegisterDraft((p) => ({ ...p, shift_open_mode: e.target.value }))}><option value="manual">Вручную</option><option value="scheduled">По времени</option><option value="first_status_4">По первому статусу 4</option><option value="on_fiscalization">При фискализации</option></select></Field>
                                    <Field label="Закрытие смены"><input style={inputStyle} value={registerDraft.shift_close_time} onChange={(e) => setRegisterDraft((p) => ({ ...p, shift_close_time: e.target.value }))} /></Field>
                                    <label style={labelStyle}><span><input type="checkbox" checked={registerDraft.is_default} onChange={(e) => setRegisterDraft((p) => ({ ...p, is_default: e.target.checked }))} /> Default</span></label>
                                    <label style={labelStyle}><span><input type="checkbox" checked={registerDraft.is_test_mode} onChange={(e) => setRegisterDraft((p) => ({ ...p, is_test_mode: e.target.checked }))} /> Test mode</span></label>
                                    <label style={labelStyle}><span><input type="checkbox" checked={registerDraft.shift_notifications_enabled} onChange={(e) => setRegisterDraft((p) => ({ ...p, shift_notifications_enabled: e.target.checked }))} /> Telegram смены</span></label>
                                    <label style={labelStyle}><span><input type="checkbox" checked={registerDraft.receipt_notifications_enabled} onChange={(e) => setRegisterDraft((p) => ({ ...p, receipt_notifications_enabled: e.target.checked }))} /> Telegram чеки</span></label>
                                </div>
                                <button type="button" style={buttonStyle} onClick={saveRegister} disabled={saving}>{editingRegisterId ? "Сохранить кассу" : "Добавить кассу"}</button>
                            </div>
                        ) : null}

                        {activeTab === "exclusions" && selectedOrganization ? (
                            <div style={{ display: "grid", gap: "16px" }}>
                                <table style={{ width: "100%", borderCollapse: "collapse" }}>
                                    <thead><tr><th style={tableCellStyle}>Поставщик</th><th style={tableCellStyle}>Касса</th><th style={tableCellStyle}>Статус</th></tr></thead>
                                    <tbody>{(selectedOrganization.receipt_exclusions || []).map((item) => (
                                        <tr key={item.id} onClick={() => { setEditingExclusionId(item.id); setExclusionDraft(exclusionToDraft(item)); }} style={{ cursor: "pointer" }}>
                                            <td style={tableCellStyle}>{item.supplier_code}<br />{item.supplier_name || ""}</td>
                                            <td style={tableCellStyle}>{item.cash_register_id ? `касса ${item.cash_register_id}` : "все кассы"}</td>
                                            <td style={tableCellStyle}>{item.is_active ? "active" : "off"}</td>
                                        </tr>
                                    ))}</tbody>
                                </table>
                                <div style={formGridStyle}>
                                    <Field label="Код поставщика"><input style={inputStyle} value={exclusionDraft.supplier_code} onChange={(e) => setExclusionDraft((p) => ({ ...p, supplier_code: e.target.value }))} /></Field>
                                    <Field label="Название"><input style={inputStyle} value={exclusionDraft.supplier_name} onChange={(e) => setExclusionDraft((p) => ({ ...p, supplier_name: e.target.value }))} /></Field>
                                    <Field label="Касса"><select style={inputStyle} value={exclusionDraft.cash_register_id} onChange={(e) => setExclusionDraft((p) => ({ ...p, cash_register_id: e.target.value }))}><option value="">Все кассы</option>{(selectedOrganization.cash_registers || []).map((item) => <option key={item.id} value={item.id}>{item.register_name}</option>)}</select></Field>
                                    <label style={labelStyle}><span><input type="checkbox" checked={exclusionDraft.is_active} onChange={(e) => setExclusionDraft((p) => ({ ...p, is_active: e.target.checked }))} /> Активно</span></label>
                                </div>
                                <Field label="Комментарий"><input style={inputStyle} value={exclusionDraft.comment} onChange={(e) => setExclusionDraft((p) => ({ ...p, comment: e.target.value }))} /></Field>
                                <button type="button" style={buttonStyle} onClick={saveExclusion} disabled={saving}>{editingExclusionId ? "Сохранить исключение" : "Добавить исключение"}</button>
                            </div>
                        ) : null}

                        {activeTab === "tests" && selectedOrganization ? (
                            <div style={{ display: "grid", gap: "16px" }}>
                                <Field label="Касса">
                                    <select
                                        style={inputStyle}
                                        value={testRegisterId}
                                        onChange={(e) => setTestRegisterId(e.target.value)}
                                    >
                                        <option value="">Выберите кассу</option>
                                        {(selectedOrganization.cash_registers || []).map((item) => (
                                            <option key={item.id} value={item.id}>
                                                {item.register_name} / {item.cash_register_code}
                                            </option>
                                        ))}
                                    </select>
                                </Field>
                                <div style={{ display: "flex", gap: "10px", flexWrap: "wrap" }}>
                                    <button type="button" style={buttonStyle} onClick={() => runRegisterTest("auth")} disabled={saving || !testRegisterId}>Проверить авторизацию</button>
                                    <button type="button" style={secondaryButtonStyle} onClick={() => runRegisterTest("open_shift")} disabled={saving || !testRegisterId}>Открыть смену</button>
                                    <button type="button" style={secondaryButtonStyle} onClick={() => runRegisterTest("close_shift")} disabled={saving || !testRegisterId}>Закрыть смену</button>
                                </div>
                                {testResult ? (
                                    <pre style={{ ...inputStyle, whiteSpace: "pre-wrap", minHeight: 120 }}>
                                        {JSON.stringify(testResult, null, 2)}
                                    </pre>
                                ) : null}
                            </div>
                        ) : null}
                    </div>
                </div>
            )}
        </div>
    );
};

export default BusinessOrganizationsPage;
