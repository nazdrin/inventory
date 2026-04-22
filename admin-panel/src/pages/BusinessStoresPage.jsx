import React, { useCallback, useEffect, useMemo, useState } from "react";
import axios from "axios";
import { getEnterpriseByCode, updateEnterprise } from "../api/enterpriseApi";
import { getAuthHeaders, handleAuthError } from "../api/developerApi";
import { API_BASE_URL } from "../config";

const pageStyle = {
    padding: "24px",
    display: "grid",
    gap: "20px",
    width: "100%",
    maxWidth: "1440px",
    margin: "0 auto",
    boxSizing: "border-box",
};

const cardStyle = {
    backgroundColor: "#ffffff",
    border: "1px solid #d9dee8",
    borderRadius: "12px",
    boxShadow: "0 8px 24px rgba(15, 23, 42, 0.06)",
};

const sectionTitleStyle = {
    margin: 0,
    fontSize: "20px",
    fontWeight: 700,
    color: "#111827",
};

const subSectionTitleStyle = {
    margin: 0,
    fontSize: "17px",
    fontWeight: 700,
    color: "#111827",
};

const mutedTextStyle = {
    margin: 0,
    color: "#64748b",
    fontSize: "14px",
    lineHeight: 1.5,
};

const inputStyle = {
    width: "100%",
    border: "1px solid #cbd5e1",
    borderRadius: "10px",
    padding: "10px 12px",
    fontSize: "14px",
    color: "#0f172a",
    backgroundColor: "#ffffff",
    boxSizing: "border-box",
};

const readonlyInputStyle = {
    ...inputStyle,
    backgroundColor: "#f8fafc",
    color: "#475569",
};

const checkboxStyle = {
    width: "18px",
    height: "18px",
    accentColor: "#2563eb",
    margin: 0,
};

const buttonBaseStyle = {
    borderRadius: "10px",
    padding: "10px 14px",
    fontSize: "14px",
    fontWeight: 700,
    cursor: "pointer",
    border: "1px solid transparent",
};

const primaryButtonStyle = {
    ...buttonBaseStyle,
    backgroundColor: "#2563eb",
    color: "#ffffff",
};

const secondaryButtonStyle = {
    ...buttonBaseStyle,
    backgroundColor: "#eff6ff",
    color: "#1d4ed8",
    borderColor: "#bfdbfe",
};

const dangerButtonStyle = {
    ...buttonBaseStyle,
    backgroundColor: "#fff7ed",
    color: "#c2410c",
    borderColor: "#fdba74",
};

const warningCardStyle = {
    border: "1px solid #fed7aa",
    backgroundColor: "#fff7ed",
    color: "#9a3412",
    borderRadius: "10px",
    padding: "12px 14px",
    fontSize: "14px",
    lineHeight: 1.5,
};

const redWarningCardStyle = {
    ...warningCardStyle,
    borderColor: "#fca5a5",
    backgroundColor: "#fef2f2",
    color: "#b91c1c",
};

const successCardStyle = {
    ...warningCardStyle,
    borderColor: "#bbf7d0",
    backgroundColor: "#f0fdf4",
    color: "#166534",
};

const formGridStyle = {
    display: "grid",
    gap: "16px 20px",
    gridTemplateColumns: "repeat(2, minmax(240px, 1fr))",
};

const labelStyle = {
    display: "grid",
    gap: "6px",
    fontSize: "14px",
    fontWeight: 600,
    color: "#111827",
};

const tableCellStyle = {
    padding: "10px 12px",
    borderBottom: "1px solid #e5e7eb",
    fontSize: "13px",
    color: "#111827",
    verticalAlign: "top",
    textAlign: "left",
};

const tableHeaderStyle = {
    ...tableCellStyle,
    fontWeight: 700,
    color: "#334155",
    backgroundColor: "#f8fafc",
    position: "sticky",
    top: 0,
};

const infoCardStyle = {
    display: "grid",
    gap: "4px",
    border: "1px solid #dbe4ee",
    borderRadius: "10px",
    padding: "12px 14px",
    backgroundColor: "#f8fafc",
};

const badgeStyle = {
    display: "inline-block",
    padding: "4px 8px",
    borderRadius: "999px",
    fontSize: "12px",
    fontWeight: 700,
    backgroundColor: "#eef2ff",
    color: "#4338ca",
};

const emptyValue = "—";

const formatApiError = (error, fallbackMessage) => {
    const detail = error?.response?.data?.detail;
    if (typeof detail === "string" && detail.trim()) {
        return detail;
    }
    if (Array.isArray(detail) && detail.length) {
        return detail
            .map((item) => {
                if (typeof item === "string") {
                    return item;
                }
                if (item && typeof item === "object") {
                    const location = Array.isArray(item.loc) ? item.loc.join(".") : "";
                    const message = typeof item.msg === "string" ? item.msg : JSON.stringify(item);
                    return location ? `${location}: ${message}` : message;
                }
                return String(item);
            })
            .join(" | ");
    }
    if (detail && typeof detail === "object") {
        return JSON.stringify(detail);
    }
    if (typeof error?.message === "string" && error.message.trim()) {
        return error.message;
    }
    return fallbackMessage;
};

const initialEnterpriseDraft = {
    enterprise_code: "",
    enterprise_name: "",
    branch_id: "",
    data_format: "",
    stock_upload_frequency: "",
    catalog_upload_frequency: "",
    tabletki_login: "",
    tabletki_password: "",
    token: "",
    catalog_enabled: false,
    stock_enabled: false,
    order_fetcher: false,
    auto_confirm: false,
    stock_correction: false,
};

const initialStoreDraft = {
    store_code: "",
    store_name: "",
    legal_entity_name: "",
    tax_identifier: "",
    is_active: true,
    is_legacy_default: false,
    enterprise_code: "",
    legacy_scope_key: "",
    tabletki_enterprise_code: "",
    tabletki_branch: "",
    salesdrive_enterprise_code: "",
    salesdrive_enterprise_id: "",
    salesdrive_store_name: "",
    catalog_enabled: false,
    stock_enabled: false,
    orders_enabled: false,
    catalog_only_in_stock: true,
    code_strategy: "opaque_mapping",
    code_prefix: "",
    name_strategy: "supplier_random",
    extra_markup_enabled: false,
    extra_markup_mode: "percent",
    extra_markup_min: "",
    extra_markup_max: "",
    extra_markup_strategy: "stable_per_product",
    takes_over_legacy_scope: false,
    migration_status: "draft",
};

const codeStrategyOptions = [
    {
        value: "legacy_same",
        label: "legacy_same",
        help: "Внешний код = внутренний product_code. Для базового магазина.",
    },
    {
        value: "opaque_mapping",
        label: "opaque_mapping",
        help: "Создаются отдельные внешние коды. Для новых магазинов.",
    },
    {
        value: "prefix_mapping",
        label: "prefix_mapping",
        help: "Коды с префиксом. Использовать только осознанно.",
    },
];

const nameStrategyOptions = [
    {
        value: "base",
        label: "base",
        help: "Использовать название из master_catalog.",
    },
    {
        value: "supplier_random",
        label: "supplier_random",
        help: "Один раз выбрать supplier name и сохранить для store + product.",
    },
];

const migrationStatusOptions = [
    { value: "draft", label: "draft", help: "Черновик. Не участвует в live-выгрузках." },
    { value: "dry_run", label: "dry_run", help: "Тест. Можно смотреть preview и dry-run." },
    { value: "stock_live", label: "stock_live", help: "Промежуточный этап запуска остатков." },
    { value: "catalog_stock_live", label: "catalog_stock_live", help: "Каталог и остатки live." },
    { value: "orders_live", label: "orders_live", help: "Каталог, остатки и заказы live." },
    { value: "disabled", label: "disabled", help: "Магазин выключен из нового контура." },
];

const boolOnOff = (value) => (value ? "Включено" : "Выключено");
const boolShort = (value) => (value ? "Вкл" : "Выкл");

const formatMigrationStatusLabel = (value) => {
    const option = migrationStatusOptions.find((item) => item.value === value);
    if (!option) {
        return value || emptyValue;
    }
    return `${option.value} · ${option.help}`;
};

const normalizeOptionalText = (value) => {
    const normalized = String(value ?? "").trim();
    return normalized || null;
};

const normalizeRequiredText = (value, label) => {
    const normalized = String(value ?? "").trim();
    if (!normalized) {
        throw new Error(`${label} обязательно.`);
    }
    return normalized;
};

const normalizeOptionalInteger = (value, label) => {
    const normalized = String(value ?? "").trim();
    if (!normalized) {
        return null;
    }
    const parsed = Number(normalized);
    if (!Number.isInteger(parsed) || parsed < 0) {
        throw new Error(`${label} должно быть неотрицательным целым числом.`);
    }
    return parsed;
};

const normalizeOptionalDecimal = (value, label) => {
    const normalized = String(value ?? "").trim();
    if (!normalized) {
        return null;
    }
    const parsed = Number(normalized);
    if (Number.isNaN(parsed) || parsed < 0) {
        throw new Error(`${label} должно быть неотрицательным числом.`);
    }
    return String(parsed);
};

const buildEnterpriseDraft = (enterprise) => ({
    enterprise_code: String(enterprise?.enterprise_code || ""),
    enterprise_name: String(enterprise?.enterprise_name || ""),
    branch_id: String(enterprise?.branch_id || ""),
    data_format: String(enterprise?.data_format || ""),
    stock_upload_frequency: enterprise?.stock_upload_frequency ?? "",
    catalog_upload_frequency: enterprise?.catalog_upload_frequency ?? "",
    tabletki_login: String(enterprise?.tabletki_login || ""),
    tabletki_password: String(enterprise?.tabletki_password || ""),
    token: String(enterprise?.token || ""),
    catalog_enabled: Boolean(enterprise?.catalog_enabled),
    stock_enabled: Boolean(enterprise?.stock_enabled),
    order_fetcher: Boolean(enterprise?.order_fetcher),
    auto_confirm: Boolean(enterprise?.auto_confirm),
    stock_correction: Boolean(enterprise?.stock_correction),
});

const buildSuggestedStoreCode = (enterprise, existingStores = []) => {
    const enterpriseCode = String(enterprise?.enterprise_code || "").trim();
    const baseCode = enterpriseCode ? `business_${enterpriseCode}` : "business_store";
    const usedCodes = new Set(
        existingStores.map((item) => String(item.store_code || "").trim()).filter(Boolean),
    );
    if (!usedCodes.has(baseCode)) {
        return baseCode;
    }
    let counter = 2;
    while (usedCodes.has(`${baseCode}_${counter}`)) {
        counter += 1;
    }
    return `${baseCode}_${counter}`;
};

const buildStoreDraftFromEnterprise = (enterprise, existingStores = []) => ({
    ...initialStoreDraft,
    store_code: buildSuggestedStoreCode(enterprise, existingStores),
    store_name: String(enterprise?.enterprise_name || "").trim(),
    enterprise_code: String(enterprise?.enterprise_code || "").trim(),
    tabletki_enterprise_code: String(enterprise?.enterprise_code || "").trim(),
    tabletki_branch: String(enterprise?.branch_id || "").trim(),
});

const buildStoreDraftFromStore = (store) => ({
    store_code: String(store?.store_code || ""),
    store_name: String(store?.store_name || ""),
    legal_entity_name: String(store?.legal_entity_name || ""),
    tax_identifier: String(store?.tax_identifier || ""),
    is_active: Boolean(store?.is_active),
    is_legacy_default: Boolean(store?.is_legacy_default),
    enterprise_code: String(store?.enterprise_code || ""),
    legacy_scope_key: String(store?.legacy_scope_key || ""),
    tabletki_enterprise_code: String(store?.tabletki_enterprise_code || ""),
    tabletki_branch: String(store?.tabletki_branch || ""),
    salesdrive_enterprise_code: String(store?.salesdrive_enterprise_code || ""),
    salesdrive_enterprise_id: store?.salesdrive_enterprise_id ?? "",
    salesdrive_store_name: String(store?.salesdrive_store_name || ""),
    catalog_enabled: Boolean(store?.catalog_enabled),
    stock_enabled: Boolean(store?.stock_enabled),
    orders_enabled: Boolean(store?.orders_enabled),
    catalog_only_in_stock: Boolean(store?.catalog_only_in_stock),
    code_strategy: String(store?.code_strategy || "opaque_mapping"),
    code_prefix: String(store?.code_prefix || ""),
    name_strategy: String(store?.name_strategy || "base"),
    extra_markup_enabled: Boolean(store?.extra_markup_enabled),
    extra_markup_mode: String(store?.extra_markup_mode || "percent"),
    extra_markup_min: store?.extra_markup_min ?? "",
    extra_markup_max: store?.extra_markup_max ?? "",
    extra_markup_strategy: String(store?.extra_markup_strategy || "stable_per_product"),
    takes_over_legacy_scope: Boolean(store?.takes_over_legacy_scope),
    migration_status: String(store?.migration_status || "draft"),
});

const buildEnterprisePayload = (draft) => ({
    enterprise_code: normalizeRequiredText(draft.enterprise_code, "enterprise_code"),
    enterprise_name: normalizeRequiredText(draft.enterprise_name, "Название предприятия"),
    branch_id: normalizeRequiredText(draft.branch_id, "Branch ID"),
    stock_upload_frequency: normalizeOptionalInteger(draft.stock_upload_frequency, "Частота stock"),
    catalog_upload_frequency: normalizeOptionalInteger(draft.catalog_upload_frequency, "Частота catalog"),
    tabletki_login: normalizeOptionalText(draft.tabletki_login),
    tabletki_password: normalizeOptionalText(draft.tabletki_password),
    token: normalizeOptionalText(draft.token),
    catalog_enabled: Boolean(draft.catalog_enabled),
    stock_enabled: Boolean(draft.stock_enabled),
    order_fetcher: Boolean(draft.order_fetcher),
    auto_confirm: Boolean(draft.auto_confirm),
    stock_correction: Boolean(draft.stock_correction),
});

const buildStorePayload = (draft, selectedEnterpriseCode) => ({
    store_code: normalizeRequiredText(draft.store_code, "store_code"),
    store_name: normalizeRequiredText(draft.store_name, "store_name"),
    legal_entity_name: normalizeOptionalText(draft.legal_entity_name),
    tax_identifier: normalizeOptionalText(draft.tax_identifier),
    is_active: Boolean(draft.is_active),
    is_legacy_default: Boolean(draft.is_legacy_default),
    enterprise_code: normalizeOptionalText(selectedEnterpriseCode),
    legacy_scope_key: normalizeOptionalText(draft.legacy_scope_key),
    tabletki_enterprise_code: normalizeOptionalText(draft.tabletki_enterprise_code),
    tabletki_branch: normalizeOptionalText(draft.tabletki_branch),
    salesdrive_enterprise_code: normalizeOptionalText(draft.salesdrive_enterprise_code),
    salesdrive_enterprise_id: String(draft.salesdrive_enterprise_id).trim() === ""
        ? null
        : Number(draft.salesdrive_enterprise_id),
    salesdrive_store_name: normalizeOptionalText(draft.salesdrive_store_name),
    catalog_enabled: Boolean(draft.catalog_enabled),
    stock_enabled: Boolean(draft.stock_enabled),
    orders_enabled: Boolean(draft.orders_enabled),
    catalog_only_in_stock: Boolean(draft.catalog_only_in_stock),
    code_strategy: String(draft.code_strategy || "opaque_mapping"),
    code_prefix: normalizeOptionalText(draft.code_prefix),
    name_strategy: String(draft.name_strategy || "base"),
    extra_markup_enabled: Boolean(draft.extra_markup_enabled),
    extra_markup_mode: "percent",
    extra_markup_min: normalizeOptionalDecimal(draft.extra_markup_min, "Минимальная наценка"),
    extra_markup_max: normalizeOptionalDecimal(draft.extra_markup_max, "Максимальная наценка"),
    extra_markup_strategy: "stable_per_product",
    takes_over_legacy_scope: Boolean(draft.takes_over_legacy_scope),
    migration_status: String(draft.migration_status || "draft"),
});

const Section = ({ title, description, children, actions = null }) => (
    <div style={{ ...cardStyle, padding: "20px 24px", display: "grid", gap: "18px" }}>
        <div style={{ display: "flex", justifyContent: "space-between", gap: "16px", alignItems: "flex-start", flexWrap: "wrap" }}>
            <div style={{ display: "grid", gap: "8px" }}>
                <h2 style={sectionTitleStyle}>{title}</h2>
                {description ? <p style={mutedTextStyle}>{description}</p> : null}
            </div>
            {actions}
        </div>
        {children}
    </div>
);

const Field = ({ label, children, helpText }) => (
    <label style={labelStyle}>
        <span>{label}</span>
        {children}
        {helpText ? <span style={mutedTextStyle}>{helpText}</span> : null}
    </label>
);

const InfoItem = ({ label, value }) => (
    <div style={infoCardStyle}>
        <div style={{ fontSize: "13px", color: "#64748b", fontWeight: 600 }}>{label}</div>
        <div style={{ fontSize: "15px", color: "#111827", fontWeight: 700 }}>{value || emptyValue}</div>
    </div>
);

const BusinessStoresPage = () => {
    const [businessEnterprises, setBusinessEnterprises] = useState([]);
    const [stores, setStores] = useState([]);
    const [legacyScopes, setLegacyScopes] = useState([]);
    const [selectedEnterpriseCode, setSelectedEnterpriseCode] = useState("");
    const [selectedStoreId, setSelectedStoreId] = useState(null);
    const [enterpriseDraft, setEnterpriseDraft] = useState(initialEnterpriseDraft);
    const [storeDraft, setStoreDraft] = useState(initialStoreDraft);
    const [enterpriseSaving, setEnterpriseSaving] = useState(false);
    const [storeSaving, setStoreSaving] = useState(false);
    const [actionLoading, setActionLoading] = useState(false);
    const [pageLoading, setPageLoading] = useState(true);
    const [pageError, setPageError] = useState("");
    const [enterpriseError, setEnterpriseError] = useState("");
    const [enterpriseSuccess, setEnterpriseSuccess] = useState("");
    const [storeError, setStoreError] = useState("");
    const [storeSuccess, setStoreSuccess] = useState("");
    const [dryRunResult, setDryRunResult] = useState(null);
    const [catalogPreviewResult, setCatalogPreviewResult] = useState(null);
    const [stockPreviewResult, setStockPreviewResult] = useState(null);

    const loadBusinessEnterprises = useCallback(async () => {
        const response = await axios.get(
            `${API_BASE_URL}/business-stores/meta/business-enterprises`,
            getAuthHeaders(),
        );
        const rows = response.data || [];
        setBusinessEnterprises(rows);
        return rows;
    }, []);

    const loadLegacyScopes = useCallback(async () => {
        const response = await axios.get(
            `${API_BASE_URL}/business-stores/meta/legacy-scopes`,
            getAuthHeaders(),
        );
        const rows = response.data || [];
        setLegacyScopes(rows);
        return rows;
    }, []);

    const loadStores = useCallback(async () => {
        const response = await axios.get(`${API_BASE_URL}/business-stores`, getAuthHeaders());
        const rows = response.data || [];
        setStores(rows);
        return rows;
    }, []);

    const reloadMeta = useCallback(async () => {
        const [enterpriseRows] = await Promise.all([
            loadBusinessEnterprises(),
            loadLegacyScopes(),
            loadStores(),
        ]);
        return enterpriseRows;
    }, [loadBusinessEnterprises, loadLegacyScopes, loadStores]);

    useEffect(() => {
        async function bootstrap() {
            setPageLoading(true);
            setPageError("");
            try {
                const enterpriseRows = await reloadMeta();
                if (!selectedEnterpriseCode && enterpriseRows.length > 0) {
                    setSelectedEnterpriseCode(String(enterpriseRows[0].enterprise_code || ""));
                }
            } catch (error) {
                handleAuthError(error);
                console.error("Error loading Business Stores page:", error);
                setPageError(formatApiError(error, "Не удалось загрузить Business-магазины."));
            } finally {
                setPageLoading(false);
            }
        }

        bootstrap();
    }, [reloadMeta, selectedEnterpriseCode]);

    const selectedEnterpriseMeta = useMemo(
        () => businessEnterprises.find((item) => item.enterprise_code === selectedEnterpriseCode) || null,
        [businessEnterprises, selectedEnterpriseCode],
    );

    const storesForSelectedEnterprise = useMemo(
        () => stores.filter((item) => item.enterprise_code === selectedEnterpriseCode),
        [stores, selectedEnterpriseCode],
    );

    const selectedLegacyScopeOption = useMemo(
        () => legacyScopes.find((item) => item.legacy_scope_key === storeDraft.legacy_scope_key) || null,
        [legacyScopes, storeDraft.legacy_scope_key],
    );

    const selectedCodeStrategy = useMemo(
        () => codeStrategyOptions.find((item) => item.value === storeDraft.code_strategy) || codeStrategyOptions[1],
        [storeDraft.code_strategy],
    );

    const selectedNameStrategy = useMemo(
        () => nameStrategyOptions.find((item) => item.value === storeDraft.name_strategy) || nameStrategyOptions[0],
        [storeDraft.name_strategy],
    );

    const selectedMigrationStatus = useMemo(
        () => migrationStatusOptions.find((item) => item.value === storeDraft.migration_status) || migrationStatusOptions[0],
        [storeDraft.migration_status],
    );

    const isNewStoreDraft = !selectedStoreId;

    useEffect(() => {
        async function loadEnterpriseContext() {
            if (!selectedEnterpriseCode) {
                setEnterpriseDraft(initialEnterpriseDraft);
                setStoreDraft(initialStoreDraft);
                setSelectedStoreId(null);
                setDryRunResult(null);
                setCatalogPreviewResult(null);
                setStockPreviewResult(null);
                return;
            }

            try {
                const enterprise = await getEnterpriseByCode(selectedEnterpriseCode);
                setEnterpriseDraft(buildEnterpriseDraft(enterprise));

                const storeForSelection = storesForSelectedEnterprise.find(
                    (item) => item.id === selectedStoreId,
                ) || null;

                if (storeForSelection) {
                    setStoreDraft(buildStoreDraftFromStore(storeForSelection));
                } else if (storesForSelectedEnterprise.length > 0) {
                    const firstStore = storesForSelectedEnterprise[0];
                    setSelectedStoreId(firstStore.id);
                    setStoreDraft(buildStoreDraftFromStore(firstStore));
                } else {
                    setSelectedStoreId(null);
                    setStoreDraft(buildStoreDraftFromEnterprise(enterprise, storesForSelectedEnterprise));
                }
            } catch (error) {
                handleAuthError(error);
                console.error("Error loading selected Business enterprise:", error);
                setPageError(formatApiError(error, "Не удалось загрузить данные выбранного Business-предприятия."));
            }
        }

        loadEnterpriseContext();
    }, [selectedEnterpriseCode, selectedStoreId, storesForSelectedEnterprise]);

    const onEnterpriseChange = (key, value) => {
        setEnterpriseDraft((prev) => ({ ...prev, [key]: value }));
    };

    const onStoreChange = (key, value) => {
        setStoreDraft((prev) => ({ ...prev, [key]: value }));
    };

    const selectOverlay = (store) => {
        setStoreError("");
        setStoreSuccess("");
        setDryRunResult(null);
        setCatalogPreviewResult(null);
        setStockPreviewResult(null);
        setSelectedStoreId(store.id);
        setStoreDraft(buildStoreDraftFromStore(store));
    };

    const handleSaveEnterprise = async () => {
        setEnterpriseSaving(true);
        setEnterpriseError("");
        setEnterpriseSuccess("");
        try {
            const payload = buildEnterprisePayload(enterpriseDraft);
            await updateEnterprise(selectedEnterpriseCode, payload);
            const [updatedEnterprise] = await Promise.all([
                getEnterpriseByCode(selectedEnterpriseCode),
                loadBusinessEnterprises(),
            ]);
            setEnterpriseDraft(buildEnterpriseDraft(updatedEnterprise));
            setStoreDraft((prev) => ({
                ...prev,
                enterprise_code: String(updatedEnterprise.enterprise_code || ""),
                tabletki_branch: prev.tabletki_branch || String(updatedEnterprise.branch_id || ""),
            }));
            setEnterpriseSuccess("Настройки предприятия сохранены.");
        } catch (error) {
            handleAuthError(error);
            console.error("Error saving enterprise settings:", error);
            setEnterpriseError(formatApiError(error, "Не удалось сохранить предприятие."));
        } finally {
            setEnterpriseSaving(false);
        }
    };

    const handleSaveStore = async () => {
        setStoreSaving(true);
        setStoreError("");
        setStoreSuccess("");
        try {
            const payload = buildStorePayload(storeDraft, selectedEnterpriseCode);
            let response;
            if (!selectedStoreId) {
                response = await axios.post(`${API_BASE_URL}/business-stores`, payload, getAuthHeaders());
            } else {
                const updatePayload = { ...payload };
                delete updatePayload.store_code;
                response = await axios.put(
                    `${API_BASE_URL}/business-stores/${selectedStoreId}`,
                    updatePayload,
                    getAuthHeaders(),
                );
            }

            const savedStore = response.data;
            await loadStores();
            setSelectedStoreId(savedStore.id);
            setStoreDraft(buildStoreDraftFromStore(savedStore));
            setCatalogPreviewResult(null);
            setStockPreviewResult(null);
            setStoreSuccess(!selectedStoreId
                ? "Магазин создан."
                : "Изменения магазина сохранены.");
        } catch (error) {
            handleAuthError(error);
            console.error("Error saving Business Store:", error);
            setStoreError(formatApiError(error, "Не удалось сохранить настройки магазина."));
        } finally {
            setStoreSaving(false);
        }
    };

    const runDryRun = async (storeId = selectedStoreId) => {
        if (!storeId) {
            return;
        }
        setActionLoading(true);
        setStoreError("");
        setStoreSuccess("");
        try {
            const response = await axios.post(
                `${API_BASE_URL}/business-stores/${storeId}/dry-run`,
                {},
                getAuthHeaders(),
            );
            setDryRunResult(response.data);
            setStoreSuccess("Dry-run выполнен.");
        } catch (error) {
            handleAuthError(error);
            console.error("Error running Business Store dry-run:", error);
            setDryRunResult(null);
            setStoreError(formatApiError(error, "Не удалось выполнить dry-run."));
        } finally {
            setActionLoading(false);
        }
    };

    const generateMissingCodes = async () => {
        if (!selectedStoreId) {
            return;
        }
        if (!window.confirm("Сгенерировать недостающие коды товаров для выбранного магазина?")) {
            return;
        }
        setActionLoading(true);
        setStoreError("");
        setStoreSuccess("");
        try {
            const response = await axios.post(
                `${API_BASE_URL}/business-stores/${selectedStoreId}/generate-missing-codes`,
                {},
                getAuthHeaders(),
            );
            setDryRunResult(response.data);
            setStoreSuccess("Недостающие коды сгенерированы.");
        } catch (error) {
            handleAuthError(error);
            console.error("Error generating missing codes:", error);
            setDryRunResult(null);
            setStoreError(formatApiError(error, "Не удалось сгенерировать недостающие коды."));
        } finally {
            setActionLoading(false);
        }
    };

    const loadCatalogPreview = async (storeId = selectedStoreId) => {
        if (!storeId) {
            return;
        }
        setActionLoading(true);
        setStoreError("");
        setStoreSuccess("");
        try {
            const response = await axios.post(
                `${API_BASE_URL}/business-stores/${storeId}/catalog-preview`,
                { limit: 100, include_not_exportable: true },
                getAuthHeaders(),
            );
            setCatalogPreviewResult(response.data);
            setStoreSuccess("Preview каталога построен.");
        } catch (error) {
            handleAuthError(error);
            console.error("Error building catalog preview:", error);
            setCatalogPreviewResult(null);
            setStoreError(formatApiError(error, "Не удалось построить preview каталога."));
        } finally {
            setActionLoading(false);
        }
    };

    const loadStockPreview = async (storeId = selectedStoreId) => {
        if (!storeId) {
            return;
        }
        setActionLoading(true);
        setStoreError("");
        setStoreSuccess("");
        try {
            const response = await axios.post(
                `${API_BASE_URL}/business-stores/${storeId}/stock-preview`,
                { limit: 100, include_not_exportable: true },
                getAuthHeaders(),
            );
            setStockPreviewResult(response.data);
            setStoreSuccess("Preview остатков построен.");
        } catch (error) {
            handleAuthError(error);
            console.error("Error building stock preview:", error);
            setStockPreviewResult(null);
            setStoreError(formatApiError(error, "Не удалось построить preview остатков."));
        } finally {
            setActionLoading(false);
        }
    };

    const generateMissingNames = async () => {
        if (!selectedStoreId) {
            return;
        }
        if (!window.confirm("Сгенерировать missing product names для выбранного магазина?")) {
            return;
        }
        setActionLoading(true);
        setStoreError("");
        setStoreSuccess("");
        try {
            const response = await axios.post(
                `${API_BASE_URL}/business-stores/${selectedStoreId}/generate-missing-names`,
                {},
                getAuthHeaders(),
            );
            setDryRunResult(response.data);
            setStoreSuccess("Missing names сгенерированы.");
        } catch (error) {
            handleAuthError(error);
            console.error("Error generating missing names:", error);
            setDryRunResult(null);
            setStoreError(formatApiError(error, "Не удалось сгенерировать missing names."));
        } finally {
            setActionLoading(false);
        }
    };

    const cleanupProductNames = async () => {
        if (!selectedStoreId) {
            return;
        }
        if (!window.confirm("Emergency очистка names mapping деактивирует текущие generated names. Продолжить?")) {
            return;
        }
        setActionLoading(true);
        setStoreError("");
        setStoreSuccess("");
        try {
            const response = await axios.post(
                `${API_BASE_URL}/business-stores/${selectedStoreId}/cleanup-product-names`,
                { confirm: true, mode: "deactivate" },
                getAuthHeaders(),
            );
            setStoreSuccess(`Names mapping очищен: ${response.data.affected_count || 0}.`);
            await runDryRun(selectedStoreId);
        } catch (error) {
            handleAuthError(error);
            console.error("Error cleaning product names:", error);
            setDryRunResult(null);
            setStoreError(formatApiError(error, "Не удалось очистить names mapping."));
        } finally {
            setActionLoading(false);
        }
    };

    const generateMissingPriceAdjustments = async () => {
        if (!selectedStoreId) {
            return;
        }
        if (!window.confirm("Сгенерировать missing price adjustments для выбранного магазина?")) {
            return;
        }
        setActionLoading(true);
        setStoreError("");
        setStoreSuccess("");
        try {
            const response = await axios.post(
                `${API_BASE_URL}/business-stores/${selectedStoreId}/generate-missing-price-adjustments`,
                {},
                getAuthHeaders(),
            );
            setDryRunResult(response.data);
            setStoreSuccess("Missing price adjustments сгенерированы.");
        } catch (error) {
            handleAuthError(error);
            console.error("Error generating missing price adjustments:", error);
            setDryRunResult(null);
            setStoreError(formatApiError(error, "Не удалось сгенерировать missing price adjustments."));
        } finally {
            setActionLoading(false);
        }
    };

    const renderSummaryBlock = (title, summary) => {
        if (!summary) {
            return null;
        }

        const largeKeys = [
            "sample_items",
            "missing_mapping_samples",
            "missing_name_samples",
            "missing_price_adjustment_samples",
        ];
        const detailKeys = largeKeys.filter((key) => key in summary);

        return (
            <div style={{ display: "grid", gap: "10px" }}>
                <h3 style={subSectionTitleStyle}>{title}</h3>
                <div style={{ display: "grid", gridTemplateColumns: "repeat(4, minmax(0, 1fr))", gap: "12px" }}>
                    {Object.entries(summary)
                        .filter(([key]) => !largeKeys.includes(key))
                        .map(([key, value]) => (
                            <div key={key} style={{ border: "1px solid #e2e8f0", borderRadius: "10px", padding: "10px 12px" }}>
                                <div style={{ fontSize: "12px", color: "#64748b", marginBottom: "4px" }}>{key}</div>
                                <div style={{ fontWeight: 700, color: "#111827" }}>{String(value ?? emptyValue)}</div>
                            </div>
                        ))}
                </div>
                {detailKeys.length ? (
                    <div style={{ display: "grid", gap: "10px", gridTemplateColumns: "repeat(2, minmax(0, 1fr))" }}>
                        {detailKeys.map((key) => (
                            <div key={key}>
                                <div style={{ fontWeight: 700, marginBottom: "6px" }}>{key}</div>
                                <pre style={{ margin: 0, backgroundColor: "#f8fafc", padding: "12px", borderRadius: "10px", overflow: "auto", fontSize: "12px" }}>
                                    {JSON.stringify(summary[key] || [], null, 2)}
                                </pre>
                            </div>
                        ))}
                    </div>
                ) : null}
            </div>
        );
    };

    return (
        <div style={pageStyle}>
            <div style={{ ...cardStyle, padding: "20px 24px", display: "grid", gap: "10px" }}>
                <h1 style={{ margin: 0, fontSize: "28px", color: "#111827" }}>Business-магазины</h1>
                <p style={mutedTextStyle}>
                    Настройка магазинов Business-контура: каталог, остатки, заказы, внешние коды и наценки.
                </p>
            </div>

            {pageError ? <div style={redWarningCardStyle}>{pageError}</div> : null}

            <Section
                title="1. Предприятие"
                description="Выберите Business-предприятие. Ниже редактируются его базовые настройки и список магазинов."
            >
                <div style={{ ...formGridStyle, gridTemplateColumns: "minmax(320px, 480px) 1fr" }}>
                    <Field label="Business-предприятие">
                        <select
                            style={inputStyle}
                            value={selectedEnterpriseCode}
                            onChange={(event) => {
                                setSelectedEnterpriseCode(event.target.value);
                                setSelectedStoreId(null);
                                setDryRunResult(null);
                                setCatalogPreviewResult(null);
                                setStockPreviewResult(null);
                                setEnterpriseError("");
                                setEnterpriseSuccess("");
                                setStoreError("");
                                setStoreSuccess("");
                            }}
                            disabled={pageLoading}
                        >
                            <option value="">Выберите Business-предприятие</option>
                            {businessEnterprises.map((item) => (
                                <option key={item.enterprise_code} value={item.enterprise_code}>
                                    {item.enterprise_name} ({item.enterprise_code})
                                </option>
                            ))}
                        </select>
                    </Field>
                    <InfoItem
                        label="Порядок работы"
                        value="Сначала настраивается предприятие, затем — конкретные магазины."
                    />
                </div>

                {selectedEnterpriseMeta ? (
                    <div style={{ display: "grid", gridTemplateColumns: "repeat(4, minmax(0, 1fr))", gap: "12px" }}>
                        <InfoItem label="Код предприятия" value={selectedEnterpriseMeta.enterprise_code} />
                        <InfoItem label="Название" value={selectedEnterpriseMeta.enterprise_name} />
                        <InfoItem label="Каталог предприятия" value={boolOnOff(selectedEnterpriseMeta.catalog_enabled)} />
                        <InfoItem
                            label="Остатки / заказы предприятия"
                            value={`остатки: ${boolOnOff(selectedEnterpriseMeta.stock_enabled)} / заказы: ${boolOnOff(selectedEnterpriseMeta.order_fetcher)}`}
                        />
                    </div>
                ) : null}

                {!selectedEnterpriseCode ? (
                    <div style={warningCardStyle}>Сначала выберите Business-предприятие.</div>
                ) : null}
            </Section>

            {selectedEnterpriseCode ? (
                <>
                    {enterpriseError ? <div style={redWarningCardStyle}>{enterpriseError}</div> : null}
                    {enterpriseSuccess ? <div style={successCardStyle}>{enterpriseSuccess}</div> : null}

                    <Section
                        title="2. Основные настройки предприятия"
                        description="Источник: enterprise_settings. Эти параметры относятся ко всему предприятию."
                        actions={(
                            <button
                                type="button"
                                style={primaryButtonStyle}
                                onClick={handleSaveEnterprise}
                                disabled={enterpriseSaving}
                            >
                                {enterpriseSaving ? "Сохранение..." : "Сохранить предприятие"}
                            </button>
                        )}
                    >
                        <div style={formGridStyle}>
                            <Field label="Код предприятия">
                                <input style={readonlyInputStyle} value={enterpriseDraft.enterprise_code} readOnly />
                            </Field>
                            <Field label="Название предприятия">
                                <input
                                    style={inputStyle}
                                    value={enterpriseDraft.enterprise_name}
                                    onChange={(event) => onEnterpriseChange("enterprise_name", event.target.value)}
                                />
                            </Field>
                            <Field label="Основной branch">
                                <input
                                    style={inputStyle}
                                    value={enterpriseDraft.branch_id}
                                    onChange={(event) => onEnterpriseChange("branch_id", event.target.value)}
                                />
                            </Field>
                            <Field label="Формат данных">
                                <input style={readonlyInputStyle} value={enterpriseDraft.data_format || emptyValue} disabled />
                            </Field>
                            <Field label="Частота остатков">
                                <input
                                    type="number"
                                    style={inputStyle}
                                    value={enterpriseDraft.stock_upload_frequency}
                                    onChange={(event) => onEnterpriseChange("stock_upload_frequency", event.target.value)}
                                />
                            </Field>
                            <Field label="Частота каталога">
                                <input
                                    type="number"
                                    style={inputStyle}
                                    value={enterpriseDraft.catalog_upload_frequency}
                                    onChange={(event) => onEnterpriseChange("catalog_upload_frequency", event.target.value)}
                                />
                            </Field>
                        </div>
                        <div style={{ display: "grid", gap: "16px" }}>
                            <h3 style={subSectionTitleStyle}>Доступы и интеграция</h3>
                            <div style={formGridStyle}>
                                <Field label="Логин Tabletki">
                                <input
                                    style={inputStyle}
                                    value={enterpriseDraft.tabletki_login}
                                    onChange={(event) => onEnterpriseChange("tabletki_login", event.target.value)}
                                />
                                </Field>
                                <Field label="Пароль Tabletki">
                                <input
                                    type="password"
                                    style={inputStyle}
                                    value={enterpriseDraft.tabletki_password}
                                    onChange={(event) => onEnterpriseChange("tabletki_password", event.target.value)}
                                />
                                </Field>
                                <Field label="Token">
                                <input
                                    type="password"
                                    style={inputStyle}
                                    value={enterpriseDraft.token}
                                    onChange={(event) => onEnterpriseChange("token", event.target.value)}
                                />
                                </Field>
                            </div>
                        </div>
                    </Section>

                    <Section
                        title="3. Разрешения предприятия"
                        description="Эти флаги включают или выключают процессы на уровне предприятия. Для нового Business-магазина также должны быть включены флаги самого магазина ниже."
                    >
                        <div style={warningCardStyle}>
                            Не путать с флагами магазина. Эти настройки разрешают работу предприятия в целом.
                        </div>
                        <div style={formGridStyle}>
                            {[
                                ["catalog_enabled", "Разрешить каталог предприятия"],
                                ["stock_enabled", "Разрешить остатки предприятия"],
                                ["order_fetcher", "Получать заказы предприятия"],
                                ["auto_confirm", "Автоподтверждение по старому контуру"],
                                ["stock_correction", "Коррекция остатков"],
                            ].map(([key, label]) => (
                                <label key={key} style={{ ...labelStyle, display: "flex", alignItems: "center", gap: "10px" }}>
                                    <input
                                        type="checkbox"
                                        style={checkboxStyle}
                                        checked={Boolean(enterpriseDraft[key])}
                                        onChange={(event) => onEnterpriseChange(key, event.target.checked)}
                                    />
                                    {label}
                                </label>
                            ))}
                        </div>
                    </Section>

                    {storeError ? <div style={redWarningCardStyle}>{storeError}</div> : null}
                    {storeSuccess ? <div style={successCardStyle}>{storeSuccess}</div> : null}

                    <Section
                        title={isNewStoreDraft ? "4. Новый магазин Business-контура" : "4. Магазин Business-контура"}
                        description={isNewStoreDraft
                            ? "Магазин ещё не сохранён. Он будет создан после нажатия «Сохранить магазин»."
                            : `Редактируется магазин: ${storeDraft.store_code || emptyValue}`}
                        actions={(
                            <div style={{ display: "flex", gap: "10px", flexWrap: "wrap" }}>
                                <button
                                    type="button"
                                    style={primaryButtonStyle}
                                    onClick={handleSaveStore}
                                    disabled={storeSaving}
                                >
                                    {storeSaving ? "Сохранение..." : (isNewStoreDraft ? "Сохранить магазин" : "Сохранить изменения")}
                                </button>
                            </div>
                        )}
                    >
                        <div style={{ display: "grid", gap: "16px" }}>
                            <h3 style={subSectionTitleStyle}>Идентификация магазина</h3>
                            <p style={mutedTextStyle}>
                                Здесь редактируются параметры конкретного магазина внутри Business-контура.
                            </p>
                            <div style={formGridStyle}>
                                <Field label="Код магазина">
                                    <input
                                        style={isNewStoreDraft ? inputStyle : readonlyInputStyle}
                                        value={storeDraft.store_code}
                                        onChange={(event) => onStoreChange("store_code", event.target.value)}
                                        readOnly={!isNewStoreDraft}
                                    />
                                </Field>
                                <Field label="Название магазина">
                                    <input
                                        style={inputStyle}
                                        value={storeDraft.store_name}
                                        onChange={(event) => onStoreChange("store_name", event.target.value)}
                                    />
                                </Field>
                                <Field label="Юрлицо / ФОП">
                                    <input
                                        style={inputStyle}
                                        value={storeDraft.legal_entity_name}
                                        onChange={(event) => onStoreChange("legal_entity_name", event.target.value)}
                                    />
                                </Field>
                                <Field label="ЕДРПОУ / РНОКПП">
                                    <input
                                        style={inputStyle}
                                        value={storeDraft.tax_identifier}
                                        onChange={(event) => onStoreChange("tax_identifier", event.target.value)}
                                    />
                                </Field>
                                <Field label="Этап запуска" helpText={selectedMigrationStatus.help}>
                                    <select
                                        style={inputStyle}
                                        value={storeDraft.migration_status}
                                        onChange={(event) => onStoreChange("migration_status", event.target.value)}
                                    >
                                        {migrationStatusOptions.map((item) => (
                                            <option key={item.value} value={item.value}>
                                                {item.value} — {item.help}
                                            </option>
                                        ))}
                                    </select>
                                </Field>
                                <label style={{ ...labelStyle, display: "flex", alignItems: "center", gap: "10px" }}>
                                    <input
                                        type="checkbox"
                                        style={checkboxStyle}
                                        checked={Boolean(storeDraft.is_active)}
                                        onChange={(event) => onStoreChange("is_active", event.target.checked)}
                                    />
                                    Магазин активен
                                </label>
                            </div>

                            <div style={{ display: "grid", gridTemplateColumns: "repeat(3, minmax(0, 1fr))", gap: "12px" }}>
                                <InfoItem label="Юрлицо / ФОП" value={storeDraft.legal_entity_name || emptyValue} />
                                <InfoItem label="ЕДРПОУ / РНОКПП" value={storeDraft.tax_identifier || emptyValue} />
                                <InfoItem label="Этап запуска" value={formatMigrationStatusLabel(storeDraft.migration_status)} />
                            </div>
                        </div>

                        <div style={{ display: "grid", gap: "16px" }}>
                            <h3 style={subSectionTitleStyle}>Привязка к Tabletki</h3>
                            <p style={mutedTextStyle}>
                                Branch Tabletki должен совпадать с branch в mapping_branch для этого магазина.
                            </p>
                            <div style={formGridStyle}>
                                <Field label="Город / scope остатков">
                                    <select
                                        style={inputStyle}
                                        value={storeDraft.legacy_scope_key}
                                        onChange={(event) => onStoreChange("legacy_scope_key", event.target.value)}
                                    >
                                        <option value="">Выберите legacy scope</option>
                                        {legacyScopes.map((item) => (
                                            <option key={item.legacy_scope_key} value={item.legacy_scope_key}>
                                                {item.legacy_scope_key} — {item.rows_count} rows / {item.products_count} products
                                            </option>
                                        ))}
                                    </select>
                                </Field>
                                <Field label="Код предприятия Tabletki">
                                    <input
                                        style={inputStyle}
                                        value={storeDraft.tabletki_enterprise_code}
                                        onChange={(event) => onStoreChange("tabletki_enterprise_code", event.target.value)}
                                    />
                                </Field>
                                <Field label="Branch Tabletki">
                                    <input
                                        style={inputStyle}
                                        value={storeDraft.tabletki_branch}
                                        onChange={(event) => onStoreChange("tabletki_branch", event.target.value)}
                                    />
                                </Field>
                                <Field label="SalesDrive ID">
                                    <input
                                        type="number"
                                        style={inputStyle}
                                        value={storeDraft.salesdrive_enterprise_id}
                                        onChange={(event) => onStoreChange("salesdrive_enterprise_id", event.target.value)}
                                    />
                                </Field>
                            </div>
                            <div style={{ display: "grid", gridTemplateColumns: "repeat(3, minmax(0, 1fr))", gap: "12px" }}>
                                <InfoItem label="Scope остатков" value={selectedLegacyScopeOption ? `${selectedLegacyScopeOption.legacy_scope_key} — ${selectedLegacyScopeOption.rows_count} rows / ${selectedLegacyScopeOption.products_count} products` : emptyValue} />
                                <InfoItem label="Код предприятия Tabletki" value={storeDraft.tabletki_enterprise_code || emptyValue} />
                                <InfoItem label="Branch Tabletki" value={storeDraft.tabletki_branch || emptyValue} />
                            </div>
                        </div>

                        <div style={{ display: "grid", gap: "16px" }}>
                            <h3 style={subSectionTitleStyle}>Коды товаров</h3>
                            <div style={formGridStyle}>
                                <Field label="Стратегия кодов">
                                    <select
                                        style={inputStyle}
                                        value={storeDraft.code_strategy}
                                        onChange={(event) => onStoreChange("code_strategy", event.target.value)}
                                    >
                                        {codeStrategyOptions.map((item) => (
                                            <option key={item.value} value={item.value}>{item.value}</option>
                                        ))}
                                    </select>
                                </Field>
                                {storeDraft.code_strategy === "prefix_mapping" ? (
                                    <Field label="Префикс кода">
                                        <input
                                            style={inputStyle}
                                            value={storeDraft.code_prefix}
                                            onChange={(event) => onStoreChange("code_prefix", event.target.value)}
                                        />
                                    </Field>
                                ) : <div />}
                                <label style={{ ...labelStyle, display: "flex", alignItems: "center", gap: "10px" }}>
                                    <input
                                        type="checkbox"
                                        style={checkboxStyle}
                                        checked={Boolean(storeDraft.is_legacy_default)}
                                        onChange={(event) => onStoreChange("is_legacy_default", event.target.checked)}
                                    />
                                    Базовый legacy-магазин
                                </label>
                            </div>
                            <div style={infoCardStyle}>
                                <div style={{ fontSize: "13px", color: "#64748b", fontWeight: 600 }}>Текущая стратегия</div>
                                <div style={{ fontSize: "15px", color: "#111827", fontWeight: 700 }}>{selectedCodeStrategy.label}</div>
                                <div style={{ fontSize: "14px", color: "#475569" }}>{selectedCodeStrategy.help}</div>
                            </div>
                            <div style={warningCardStyle}>
                                После генерации внешних кодов не меняйте стратегию без миграционного плана.
                            </div>
                        </div>

                        <div style={{ display: "grid", gap: "16px" }}>
                            <h3 style={subSectionTitleStyle}>Каталог</h3>
                            <div style={formGridStyle}>
                                <label style={{ ...labelStyle, display: "flex", alignItems: "center", gap: "10px" }}>
                                    <input
                                        type="checkbox"
                                        style={checkboxStyle}
                                        checked={Boolean(storeDraft.catalog_only_in_stock)}
                                        onChange={(event) => onStoreChange("catalog_only_in_stock", event.target.checked)}
                                    />
                                    В каталог только товары с остатком
                                </label>
                            </div>
                            <p style={mutedTextStyle}>
                                Если включено, каталог preview/publish берёт только товары с положительным остатком в выбранном scope.
                            </p>
                        </div>

                        <div style={{ display: "grid", gap: "16px" }}>
                            <h3 style={subSectionTitleStyle}>Названия товаров</h3>
                            <div style={formGridStyle}>
                                <Field label="Стратегия названий">
                                    <select
                                        style={inputStyle}
                                        value={storeDraft.name_strategy}
                                        onChange={(event) => onStoreChange("name_strategy", event.target.value)}
                                    >
                                        {nameStrategyOptions.map((item) => (
                                            <option key={item.value} value={item.value}>{item.value}</option>
                                        ))}
                                    </select>
                                </Field>
                            </div>
                            <div style={infoCardStyle}>
                                <div style={{ fontSize: "13px", color: "#64748b", fontWeight: 600 }}>Текущая стратегия</div>
                                <div style={{ fontSize: "15px", color: "#111827", fontWeight: 700 }}>{selectedNameStrategy.label}</div>
                                <div style={{ fontSize: "14px", color: "#475569" }}>{selectedNameStrategy.help}</div>
                            </div>
                        </div>

                        <div style={{ display: "grid", gap: "16px" }}>
                            <h3 style={subSectionTitleStyle}>Цены</h3>
                            <div style={formGridStyle}>
                                <label style={{ ...labelStyle, display: "flex", alignItems: "center", gap: "10px" }}>
                                    <input
                                        type="checkbox"
                                        style={checkboxStyle}
                                        checked={Boolean(storeDraft.extra_markup_enabled)}
                                        onChange={(event) => onStoreChange("extra_markup_enabled", event.target.checked)}
                                    />
                                    Дополнительная наценка включена
                                </label>
                                <Field label="Режим наценки">
                                    <input style={readonlyInputStyle} value={storeDraft.extra_markup_mode} readOnly />
                                </Field>
                                <Field label="Минимальная наценка (%)">
                                    <input
                                        type="number"
                                        min="0"
                                        step="0.01"
                                        style={inputStyle}
                                        value={storeDraft.extra_markup_min}
                                        onChange={(event) => onStoreChange("extra_markup_min", event.target.value)}
                                    />
                                </Field>
                                <Field label="Максимальная наценка (%)">
                                    <input
                                        type="number"
                                        min="0"
                                        step="0.01"
                                        style={inputStyle}
                                        value={storeDraft.extra_markup_max}
                                        onChange={(event) => onStoreChange("extra_markup_max", event.target.value)}
                                    />
                                </Field>
                                <Field label="Стратегия наценки">
                                    <input style={readonlyInputStyle} value={storeDraft.extra_markup_strategy} readOnly />
                                </Field>
                            </div>
                            <p style={mutedTextStyle}>
                                stable_per_product: один раз генерирует стабильную наценку для каждого товара магазина.
                            </p>
                        </div>

                        <div style={{ display: "grid", gap: "16px" }}>
                            <h3 style={subSectionTitleStyle}>Участие магазина в процессах</h3>
                            <p style={mutedTextStyle}>
                                Эти флаги определяют, участвует ли выбранный магазин в store-aware каталоге, остатках и заказах. Для работы также должны быть включены разрешения предприятия выше.
                            </p>
                            <div style={formGridStyle}>
                                {[
                                    ["catalog_enabled", "Магазин участвует в каталоге"],
                                    ["stock_enabled", "Магазин участвует в остатках"],
                                    ["orders_enabled", "Магазин участвует в заказах"],
                                ].map(([key, label]) => (
                                    <label key={key} style={{ ...labelStyle, display: "flex", alignItems: "center", gap: "10px" }}>
                                        <input
                                            type="checkbox"
                                            style={checkboxStyle}
                                            checked={Boolean(storeDraft[key])}
                                            onChange={(event) => onStoreChange(key, event.target.checked)}
                                        />
                                        {label}
                                    </label>
                                ))}
                                <label style={{ ...labelStyle, display: "flex", alignItems: "center", gap: "10px" }}>
                                    <input
                                        type="checkbox"
                                        style={checkboxStyle}
                                        checked={Boolean(storeDraft.takes_over_legacy_scope)}
                                        onChange={(event) => onStoreChange("takes_over_legacy_scope", event.target.checked)}
                                    />
                                    Забирает legacy scope
                                </label>
                            </div>
                            <div style={redWarningCardStyle}>
                                Не включать без отдельного миграционного плана.
                            </div>
                        </div>

                        <div style={{ display: "grid", gap: "12px" }}>
                            <h3 style={subSectionTitleStyle}>Действия по магазину</h3>
                            <p style={mutedTextStyle}>
                                Кнопки генерации создают недостающие mapping-записи. Preview не отправляет данные наружу.
                            </p>
                            <div style={{ display: "flex", gap: "12px", flexWrap: "wrap" }}>
                                <button
                                    type="button"
                                    style={secondaryButtonStyle}
                                    onClick={runDryRun}
                                    disabled={!selectedStoreId || actionLoading}
                                >
                                    Dry-run магазина
                                </button>
                                <button
                                    type="button"
                                    style={dangerButtonStyle}
                                    onClick={generateMissingCodes}
                                    disabled={!selectedStoreId || actionLoading}
                                >
                                    Сгенерировать коды
                                </button>
                                <button
                                    type="button"
                                    style={secondaryButtonStyle}
                                    onClick={generateMissingNames}
                                    disabled={!selectedStoreId || actionLoading}
                                >
                                    Сгенерировать названия
                                </button>
                                <button
                                    type="button"
                                    style={secondaryButtonStyle}
                                    onClick={generateMissingPriceAdjustments}
                                    disabled={!selectedStoreId || actionLoading}
                                >
                                    Сгенерировать наценки
                                </button>
                                <button
                                    type="button"
                                    style={secondaryButtonStyle}
                                    onClick={() => loadCatalogPreview(selectedStoreId)}
                                    disabled={!selectedStoreId || actionLoading}
                                >
                                    Preview каталога
                                </button>
                                <button
                                    type="button"
                                    style={secondaryButtonStyle}
                                    onClick={() => loadStockPreview(selectedStoreId)}
                                    disabled={!selectedStoreId || actionLoading}
                                >
                                    Preview остатков
                                </button>
                                <button
                                    type="button"
                                    style={dangerButtonStyle}
                                    onClick={cleanupProductNames}
                                    disabled={!selectedStoreId || actionLoading}
                                >
                                    Emergency: очистить названия
                                </button>
                            </div>
                            {!selectedStoreId ? (
                                <div style={warningCardStyle}>
                                    Сначала сохраните магазин.
                                </div>
                            ) : null}
                        </div>
                    </Section>

                    <Section
                        title="Список магазинов выбранного предприятия"
                        description="Магазины выбранного Business-предприятия."
                    >
                        {storesForSelectedEnterprise.length > 0 ? (
                        <div style={{ overflow: "auto", maxHeight: "420px", border: "1px solid #e2e8f0", borderRadius: "12px" }}>
                            <table style={{ width: "100%", borderCollapse: "collapse", minWidth: "1480px" }}>
                                <thead>
                                    <tr>
                                        {[
                                            "Код магазина",
                                            "Название",
                                            "Юрлицо",
                                            "Scope",
                                            "Код Tabletki",
                                            "Branch Tabletki",
                                            "Стратегия кодов",
                                            "Этап запуска",
                                            "Активен",
                                            "Каталог",
                                            "Остатки",
                                            "Заказы",
                                            "Действия",
                                        ].map((header) => (
                                            <th key={header} style={tableHeaderStyle}>{header}</th>
                                        ))}
                                    </tr>
                                </thead>
                                <tbody>
                                    {storesForSelectedEnterprise.map((item) => (
                                        <tr
                                            key={item.id}
                                            style={{ backgroundColor: item.id === selectedStoreId ? "#eff6ff" : "#ffffff", cursor: "pointer" }}
                                            onClick={() => selectOverlay(item)}
                                        >
                                            <td style={tableCellStyle}>{item.store_code}</td>
                                            <td style={tableCellStyle}>{item.store_name}</td>
                                            <td style={tableCellStyle}>{item.legal_entity_name || emptyValue}</td>
                                            <td style={tableCellStyle}>{item.legacy_scope_key || emptyValue}</td>
                                            <td style={tableCellStyle}>{item.tabletki_enterprise_code || emptyValue}</td>
                                            <td style={tableCellStyle}>{item.tabletki_branch || emptyValue}</td>
                                            <td style={tableCellStyle}>{item.code_strategy}</td>
                                            <td style={tableCellStyle}><span style={badgeStyle}>{formatMigrationStatusLabel(item.migration_status)}</span></td>
                                            <td style={tableCellStyle}>{item.is_active ? "Да" : "Нет"}</td>
                                            <td style={tableCellStyle}>{boolShort(item.catalog_enabled)}</td>
                                            <td style={tableCellStyle}>{boolShort(item.stock_enabled)}</td>
                                            <td style={tableCellStyle}>{boolShort(item.orders_enabled)}</td>
                                            <td style={tableCellStyle}>
                                                <div style={{ display: "grid", gap: "8px" }}>
                                                    <button type="button" style={secondaryButtonStyle} onClick={() => selectOverlay(item)}>
                                                        Открыть
                                                    </button>
                                                    <button type="button" style={secondaryButtonStyle} onClick={() => {
                                                        selectOverlay(item);
                                                        runDryRun(item.id);
                                                    }}>
                                                        Dry-run
                                                    </button>
                                                    <button type="button" style={secondaryButtonStyle} onClick={() => {
                                                        selectOverlay(item);
                                                        loadCatalogPreview(item.id);
                                                    }}>
                                                        Preview каталога
                                                    </button>
                                                    <button type="button" style={secondaryButtonStyle} onClick={() => {
                                                        selectOverlay(item);
                                                        loadStockPreview(item.id);
                                                    }}>
                                                        Preview остатков
                                                    </button>
                                                </div>
                                            </td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                        ) : (
                            <div style={warningCardStyle}>
                                Для выбранного предприятия пока нет сохранённых магазинов. Заполните форму выше и нажмите «Сохранить магазин».
                            </div>
                        )}
                    </Section>

                    {dryRunResult ? (
                        <Section title="Результат dry-run" description="Dry-run не отправляет данные наружу и не меняет live-процессы.">
                            <div style={{ display: "flex", gap: "10px", flexWrap: "wrap" }}>
                                {"generated_codes" in dryRunResult ? (
                                    <span style={badgeStyle}>generated_codes: {dryRunResult.generated_codes}</span>
                                ) : null}
                                {"generated_names" in dryRunResult ? (
                                    <span style={badgeStyle}>generated_names: {dryRunResult.generated_names}</span>
                                ) : null}
                                {"generated_price_adjustments" in dryRunResult ? (
                                    <span style={badgeStyle}>generated_price_adjustments: {dryRunResult.generated_price_adjustments}</span>
                                ) : null}
                            </div>
                            {dryRunResult.warnings?.length ? (
                                <pre style={{ margin: 0, backgroundColor: "#fff7ed", padding: "12px", borderRadius: "10px", color: "#9a3412", fontSize: "12px" }}>
                                    {JSON.stringify(dryRunResult.warnings, null, 2)}
                                </pre>
                            ) : null}
                            {"summary" in dryRunResult ? (
                                <pre style={{ margin: 0, backgroundColor: "#f8fafc", padding: "12px", borderRadius: "10px", color: "#0f172a", fontSize: "12px" }}>
                                    {JSON.stringify(dryRunResult.summary, null, 2)}
                                </pre>
                            ) : null}
                            {renderSummaryBlock("Stock", dryRunResult.stock)}
                            {renderSummaryBlock("Catalog", dryRunResult.catalog)}
                        </Section>
                    ) : null}

                    {catalogPreviewResult ? (
                        <Section title="Preview каталога" description="Payload строится поверх master_catalog и store mappings, но не отправляется в Tabletki.">
                            {catalogPreviewResult.warnings?.length ? (
                                <pre style={{ margin: 0, backgroundColor: "#fff7ed", padding: "12px", borderRadius: "10px", color: "#9a3412", fontSize: "12px" }}>
                                    {JSON.stringify(catalogPreviewResult.warnings, null, 2)}
                                </pre>
                            ) : null}
                            <div style={{ display: "grid", gridTemplateColumns: "repeat(4, minmax(0, 1fr))", gap: "12px" }}>
                                <InfoItem label="Код предприятия Tabletki" value={catalogPreviewResult.store?.tabletki_enterprise_code || emptyValue} />
                                <InfoItem label="Branch Tabletki" value={catalogPreviewResult.store?.tabletki_branch || emptyValue} />
                                <InfoItem label="Scope остатков" value={catalogPreviewResult.store?.legacy_scope_key || emptyValue} />
                                <InfoItem label="Источник каталога" value={catalogPreviewResult.summary?.catalog_source || emptyValue} />
                                <InfoItem label="Товаров-кандидатов" value={catalogPreviewResult.summary?.candidate_products ?? emptyValue} />
                                <InfoItem label="Товаров к выгрузке" value={catalogPreviewResult.summary?.exportable_products ?? emptyValue} />
                                <InfoItem label="Нет code mapping" value={catalogPreviewResult.summary?.missing_code_mapping ?? emptyValue} />
                                <InfoItem label="Нет name mapping" value={catalogPreviewResult.summary?.missing_name_mapping ?? emptyValue} />
                            </div>
                            {catalogPreviewResult.not_exportable_samples?.length ? (
                                <pre style={{ margin: 0, backgroundColor: "#f8fafc", padding: "12px", borderRadius: "10px", color: "#0f172a", fontSize: "12px" }}>
                                    {JSON.stringify(catalogPreviewResult.not_exportable_samples, null, 2)}
                                </pre>
                            ) : null}
                            <div style={{ overflow: "auto", maxHeight: "520px", border: "1px solid #e2e8f0", borderRadius: "12px" }}>
                                <table style={{ width: "100%", borderCollapse: "collapse", minWidth: "1320px" }}>
                                    <thead>
                                        <tr>
                                            {[
                                                "Внутренний код",
                                                "Внешний код",
                                                "Базовое название",
                                                "Внешнее название",
                                                "Штрихкод",
                                                "Производитель",
                                                "Бренд",
                                                "К выгрузке",
                                                "Причины",
                                            ].map((header) => (
                                                <th key={header} style={tableHeaderStyle}>{header}</th>
                                            ))}
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {(catalogPreviewResult.payload_preview || []).map((item) => (
                                            <tr key={`${item.internal_product_code}:${item.external_product_code || "missing"}`}>
                                                <td style={tableCellStyle}>{item.internal_product_code || emptyValue}</td>
                                                <td style={tableCellStyle}>{item.external_product_code || emptyValue}</td>
                                                <td style={tableCellStyle}>{item.base_name || emptyValue}</td>
                                                <td style={tableCellStyle}>{item.external_product_name || emptyValue}</td>
                                                <td style={tableCellStyle}>{item.barcode || emptyValue}</td>
                                                <td style={tableCellStyle}>{item.manufacturer || emptyValue}</td>
                                                <td style={tableCellStyle}>{item.brand || emptyValue}</td>
                                                <td style={tableCellStyle}>{item.exportable ? "Да" : "Нет"}</td>
                                                <td style={tableCellStyle}>{(item.reasons || []).join(", ") || emptyValue}</td>
                                            </tr>
                                        ))}
                                        {!catalogPreviewResult.payload_preview?.length ? (
                                            <tr>
                                                <td colSpan={9} style={{ ...tableCellStyle, textAlign: "center" }}>
                                                    Preview rows отсутствуют.
                                                </td>
                                            </tr>
                                        ) : null}
                                    </tbody>
                                </table>
                            </div>
                        </Section>
                    ) : null}

                    {stockPreviewResult ? (
                        <Section title="Preview остатков" description="Payload остатков строится поверх offers, code mappings и price adjustments, но не отправляется в Tabletki.">
                            {stockPreviewResult.warnings?.length ? (
                                <pre style={{ margin: 0, backgroundColor: "#fff7ed", padding: "12px", borderRadius: "10px", color: "#9a3412", fontSize: "12px" }}>
                                    {JSON.stringify(stockPreviewResult.warnings, null, 2)}
                                </pre>
                            ) : null}
                            <div style={{ display: "grid", gridTemplateColumns: "repeat(4, minmax(0, 1fr))", gap: "12px" }}>
                                <InfoItem label="Код предприятия Tabletki" value={stockPreviewResult.store?.tabletki_enterprise_code || emptyValue} />
                                <InfoItem label="Branch Tabletki" value={stockPreviewResult.store?.tabletki_branch || emptyValue} />
                                <InfoItem label="Scope остатков" value={stockPreviewResult.store?.legacy_scope_key || emptyValue} />
                                <InfoItem label="Источник остатков" value={stockPreviewResult.summary?.stock_source || emptyValue} />
                                <InfoItem label="Всего offer-строк" value={stockPreviewResult.summary?.offer_rows_total ?? emptyValue} />
                                <InfoItem label="Товаров-кандидатов" value={stockPreviewResult.summary?.candidate_products ?? emptyValue} />
                                <InfoItem label="Товаров к выгрузке" value={stockPreviewResult.summary?.exportable_products ?? emptyValue} />
                                <InfoItem label="Нет code mapping" value={stockPreviewResult.summary?.missing_code_mapping ?? emptyValue} />
                                <InfoItem label="Нет наценки" value={stockPreviewResult.summary?.missing_price_adjustment ?? emptyValue} />
                                <InfoItem label="С наценкой" value={stockPreviewResult.summary?.markup_applied_products ?? emptyValue} />
                            </div>
                            {stockPreviewResult.not_exportable_samples?.length ? (
                                <pre style={{ margin: 0, backgroundColor: "#f8fafc", padding: "12px", borderRadius: "10px", color: "#0f172a", fontSize: "12px" }}>
                                    {JSON.stringify(stockPreviewResult.not_exportable_samples, null, 2)}
                                </pre>
                            ) : null}
                            <div style={{ overflow: "auto", maxHeight: "520px", border: "1px solid #e2e8f0", borderRadius: "12px" }}>
                                <table style={{ width: "100%", borderCollapse: "collapse", minWidth: "1400px" }}>
                                    <thead>
                                        <tr>
                                            {[
                                                "Внутренний код",
                                                "Внешний код",
                                                "Поставщик",
                                                "Количество",
                                                "Базовая цена",
                                                "Наценка %",
                                                "Итоговая цена магазина",
                                                "К выгрузке",
                                                "Причины",
                                            ].map((header) => (
                                                <th key={header} style={tableHeaderStyle}>{header}</th>
                                            ))}
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {(stockPreviewResult.payload_preview || []).map((item) => (
                                            <tr key={`${item.internal_product_code}:${item.external_product_code || "missing"}:${item.supplier_code || "nosupplier"}`}>
                                                <td style={tableCellStyle}>{item.internal_product_code || emptyValue}</td>
                                                <td style={tableCellStyle}>{item.external_product_code || emptyValue}</td>
                                                <td style={tableCellStyle}>{item.supplier_code || emptyValue}</td>
                                                <td style={tableCellStyle}>{item.qty ?? emptyValue}</td>
                                                <td style={tableCellStyle}>{item.base_price || emptyValue}</td>
                                                <td style={tableCellStyle}>{item.markup_percent || emptyValue}</td>
                                                <td style={tableCellStyle}>{item.final_store_price_preview || emptyValue}</td>
                                                <td style={tableCellStyle}>{item.exportable ? "Да" : "Нет"}</td>
                                                <td style={tableCellStyle}>{(item.reasons || []).join(", ") || emptyValue}</td>
                                            </tr>
                                        ))}
                                        {!stockPreviewResult.payload_preview?.length ? (
                                            <tr>
                                                <td colSpan={9} style={{ ...tableCellStyle, textAlign: "center" }}>
                                                    Preview rows отсутствуют.
                                                </td>
                                            </tr>
                                        ) : null}
                                    </tbody>
                                </table>
                            </div>
                        </Section>
                    ) : null}
                </>
            ) : null}
        </div>
    );
};

export default BusinessStoresPage;
