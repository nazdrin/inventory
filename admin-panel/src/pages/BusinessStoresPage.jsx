import React, { useCallback, useEffect, useMemo, useRef, useState } from "react";
import axios from "axios";
import { getEnterpriseByCode, updateEnterprise } from "../api/enterpriseApi";
import { getBusinessStoreMappingBranches } from "../api/mappingBranchAPI";
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

const neutralInfoCardStyle = {
    border: "1px solid #dbeafe",
    backgroundColor: "#f8fbff",
    color: "#1e40af",
    borderRadius: "10px",
    padding: "12px 14px",
    fontSize: "14px",
    lineHeight: 1.5,
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
    business_runtime_mode: "baseline",
    has_enterprise_catalog_mappings: false,
    runtime_mode_switch_locked: false,
    runtime_mode_switch_lock_reason: "",
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
    code_strategy: "legacy_same",
    code_prefix: "",
    name_strategy: "base",
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
        label: "Базовая",
        help: "Коды отправляются как есть: внешний код = внутренний product_code.",
    },
    {
        value: "opaque_mapping",
        label: "Индивидуальные коды",
        help: "Для Tabletki используются отдельные коды из маппинга предприятия.",
    },
    {
        value: "prefix_mapping",
        label: "Коды с префиксом",
        help: "Отдельная стратегия с префиксом кода. Использовать только для специальных сценариев.",
    },
];

const nameStrategyOptions = [
    {
        value: "base",
        label: "Базовые",
        help: "Использовать названия из master_catalog.",
    },
    {
        value: "supplier_random",
        label: "Названия поставщиков",
        help: "Использовать сохранённые индивидуальные названия товаров.",
    },
];

const boolOnOff = (value) => (value ? "Включено" : "Выключено");
const boolShort = (value) => (value ? "Вкл" : "Выкл");

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
    business_runtime_mode: String(enterprise?.business_runtime_mode || "baseline"),
    has_enterprise_catalog_mappings: Boolean(enterprise?.has_enterprise_catalog_mappings),
    runtime_mode_switch_locked: Boolean(enterprise?.runtime_mode_switch_locked),
    runtime_mode_switch_lock_reason: String(enterprise?.runtime_mode_switch_lock_reason || ""),
    order_fetcher: Boolean(enterprise?.order_fetcher),
    auto_confirm: Boolean(enterprise?.auto_confirm),
    stock_correction: Boolean(enterprise?.stock_correction),
});

const buildSuggestedStoreCode = (enterprise, existingStores = [], branch = "") => {
    const enterpriseCode = String(enterprise?.enterprise_code || "").trim();
    const normalizedBranch = String(branch || "").trim();
    const baseCode = enterpriseCode
        ? (normalizedBranch ? `business_${enterpriseCode}_${normalizedBranch}` : `business_${enterpriseCode}`)
        : "business_store";
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

const buildStoreNameForBranch = (enterprise, branch = "") => {
    const enterpriseName = String(enterprise?.enterprise_name || "").trim() || "Business";
    const normalizedBranch = String(branch || "").trim();
    return normalizedBranch ? `${enterpriseName} / ${normalizedBranch}` : enterpriseName;
};

const buildStoreDraftFromEnterprise = (enterprise, existingStores = []) => ({
    ...initialStoreDraft,
    store_code: buildSuggestedStoreCode(enterprise, existingStores, String(enterprise?.branch_id || "").trim()),
    store_name: buildStoreNameForBranch(enterprise, String(enterprise?.branch_id || "").trim()),
    enterprise_code: String(enterprise?.enterprise_code || "").trim(),
    tabletki_enterprise_code: String(enterprise?.enterprise_code || "").trim(),
    tabletki_branch: String(enterprise?.branch_id || "").trim(),
    catalog_enabled: true,
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
    business_runtime_mode: String(draft.business_runtime_mode || "baseline") === "custom"
        ? "custom"
        : "baseline",
    order_fetcher: Boolean(draft.order_fetcher),
    auto_confirm: Boolean(draft.auto_confirm),
    stock_correction: Boolean(draft.stock_correction),
});

const buildStorePayload = (draft, selectedEnterpriseCode, { isBaselineEnterprise = false } = {}) => ({
    store_code: normalizeRequiredText(draft.store_code, "store_code"),
    store_name: normalizeRequiredText(draft.store_name, "store_name"),
    legal_entity_name: normalizeOptionalText(draft.legal_entity_name),
    tax_identifier: normalizeOptionalText(draft.tax_identifier),
    is_active: Boolean(draft.is_active),
    is_legacy_default: isBaselineEnterprise ? true : Boolean(draft.is_legacy_default),
    enterprise_code: normalizeOptionalText(selectedEnterpriseCode),
    legacy_scope_key: normalizeOptionalText(draft.legacy_scope_key),
    tabletki_enterprise_code: normalizeOptionalText(draft.tabletki_enterprise_code) || normalizeOptionalText(selectedEnterpriseCode),
    tabletki_branch: normalizeOptionalText(draft.tabletki_branch),
    salesdrive_enterprise_code: normalizeOptionalText(draft.salesdrive_enterprise_code),
    salesdrive_enterprise_id: String(draft.salesdrive_enterprise_id).trim() === ""
        ? null
        : Number(draft.salesdrive_enterprise_id),
    salesdrive_store_name: normalizeOptionalText(draft.salesdrive_store_name),
    // Deprecated compatibility field: catalog is now operator-managed on the
    // enterprise level, but backend/storage still carries BusinessStore.catalog_enabled.
    catalog_enabled: true,
    stock_enabled: Boolean(draft.stock_enabled),
    orders_enabled: Boolean(draft.orders_enabled),
    catalog_only_in_stock: Boolean(draft.catalog_only_in_stock),
    code_strategy: isBaselineEnterprise ? "legacy_same" : String(draft.code_strategy || "opaque_mapping"),
    code_prefix: isBaselineEnterprise ? null : normalizeOptionalText(draft.code_prefix),
    name_strategy: isBaselineEnterprise ? "base" : String(draft.name_strategy || "base"),
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
    const [mappingBranches, setMappingBranches] = useState([]);
    const [selectedEnterpriseCode, setSelectedEnterpriseCode] = useState("");
    const [selectedStoreId, setSelectedStoreId] = useState(null);
    const [storeDraftStoreId, setStoreDraftStoreId] = useState(null);
    const [enterpriseDraft, setEnterpriseDraft] = useState(initialEnterpriseDraft);
    const [storeDraft, setStoreDraft] = useState(initialStoreDraft);
    const [enterpriseSaving, setEnterpriseSaving] = useState(false);
    const [storeSaving, setStoreSaving] = useState(false);
    const [pageLoading, setPageLoading] = useState(true);
    const [pageError, setPageError] = useState("");
    const [enterpriseError, setEnterpriseError] = useState("");
    const [enterpriseSuccess, setEnterpriseSuccess] = useState("");
    const [storeError, setStoreError] = useState("");
    const [storeSuccess, setStoreSuccess] = useState("");
    const [enterpriseCatalogOnlyInStockDraft, setEnterpriseCatalogOnlyInStockDraft] = useState(true);
    const [mappingBranchesLoading, setMappingBranchesLoading] = useState(false);
    const [mappingBranchesError, setMappingBranchesError] = useState("");
    const enterpriseContextRequestIdRef = useRef(0);
    const storeDraftDirtyRef = useRef(false);

    const loadBusinessEnterprises = useCallback(async () => {
        const response = await axios.get(
            `${API_BASE_URL}/business-stores/meta/business-enterprises`,
            getAuthHeaders(),
        );
        const rows = response.data || [];
        setBusinessEnterprises(rows);
        return rows;
    }, []);

    const loadStores = useCallback(async () => {
        const response = await axios.get(`${API_BASE_URL}/business-stores`, getAuthHeaders());
        const rows = response.data || [];
        setStores(rows);
        return rows;
    }, []);

    const loadMappingBranches = useCallback(async (enterpriseCode) => {
        const normalizedEnterpriseCode = String(enterpriseCode || "").trim();
        if (!normalizedEnterpriseCode) {
            setMappingBranches([]);
            return [];
        }
        const rows = await getBusinessStoreMappingBranches(normalizedEnterpriseCode);
        setMappingBranches(rows || []);
        return rows || [];
    }, []);

    const replaceStoreDraft = useCallback((draft, ownerStoreId = null, { dirty = false } = {}) => {
        setStoreDraft(draft);
        setStoreDraftStoreId(ownerStoreId);
        storeDraftDirtyRef.current = Boolean(dirty);
    }, []);

    const reloadMeta = useCallback(async () => {
        const [enterpriseRows] = await Promise.all([
            loadBusinessEnterprises(),
            loadStores(),
        ]);
        return enterpriseRows;
    }, [loadBusinessEnterprises, loadStores]);

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

    useEffect(() => {
        async function loadBranchOptions() {
            if (!selectedEnterpriseCode) {
                setMappingBranches([]);
                setMappingBranchesError("");
                return;
            }

            setMappingBranchesLoading(true);
            setMappingBranchesError("");
            try {
                await loadMappingBranches(selectedEnterpriseCode);
            } catch (error) {
                handleAuthError(error);
                console.error("Error loading mapping branches:", error);
                setMappingBranches([]);
                setMappingBranchesError(formatApiError(error, "Не удалось загрузить branch-список из mapping_branch."));
            } finally {
                setMappingBranchesLoading(false);
            }
        }

        loadBranchOptions();
    }, [loadMappingBranches, selectedEnterpriseCode]);

    const selectedEnterpriseMeta = useMemo(
        () => businessEnterprises.find((item) => item.enterprise_code === selectedEnterpriseCode) || null,
        [businessEnterprises, selectedEnterpriseCode],
    );

    const storesForSelectedEnterprise = useMemo(
        () => stores.filter((item) => item.enterprise_code === selectedEnterpriseCode),
        [stores, selectedEnterpriseCode],
    );

    const mappingBranchOptions = useMemo(
        () => (mappingBranches || []).filter((item) => String(item.branch || "").trim()),
        [mappingBranches],
    );

    const mappingBranchValues = useMemo(
        () => new Set(mappingBranchOptions.map((item) => String(item.branch || "").trim()).filter(Boolean)),
        [mappingBranchOptions],
    );

    const currentStoreBranchMissingFromOptions = useMemo(() => {
        const currentBranch = String(storeDraft.tabletki_branch || "").trim();
        if (!currentBranch) {
            return false;
        }
        return !mappingBranchValues.has(currentBranch);
    }, [mappingBranchValues, storeDraft.tabletki_branch]);

    const catalogScopeStoreCandidates = useMemo(() => {
        const targetBranch = String(enterpriseDraft.branch_id || "").trim();
        if (!selectedEnterpriseCode || !targetBranch) {
            return [];
        }
        return storesForSelectedEnterprise.filter(
            (item) => Boolean(item.is_active) && String(item.tabletki_branch || "").trim() === targetBranch,
        );
    }, [enterpriseDraft.branch_id, selectedEnterpriseCode, storesForSelectedEnterprise]);

    const catalogScopeStore = useMemo(
        () => (catalogScopeStoreCandidates.length === 1 ? catalogScopeStoreCandidates[0] : null),
        [catalogScopeStoreCandidates],
    );


    const isNewStoreDraft = !selectedStoreId;
    const isBaselineEnterprise = enterpriseDraft.business_runtime_mode !== "custom";
    const isCustomEnterprise = !isBaselineEnterprise;
    const isRuntimeModeLocked = isCustomEnterprise && Boolean(enterpriseDraft.runtime_mode_switch_locked);
    const identityControlsDisabled = isBaselineEnterprise;
    const storeRoutingReadOnly = isBaselineEnterprise;
    const enterpriseStrategyStoreId = catalogScopeStore?.id || selectedStoreId || null;
    const effectiveCodeStrategy = isBaselineEnterprise ? "legacy_same" : String(storeDraft.code_strategy || "opaque_mapping");
    const effectiveNameStrategy = isBaselineEnterprise ? "base" : String(storeDraft.name_strategy || "base");
    const runtimeModeHelpText = isRuntimeModeLocked
        ? (enterpriseDraft.runtime_mode_switch_lock_reason || "Для підприємства вже створені індивідуальні коди або назви каталогу. Зміну режиму заблоковано.")
        : (isCustomEnterprise
            ? "Каталог, остатки, заказы и статусы используют настраиваемые коды предприятия."
            : "Каталог, остатки, заказы и статусы используют базовые коды без маппинга.");

    useEffect(() => {
        if (catalogScopeStore) {
            setEnterpriseCatalogOnlyInStockDraft(Boolean(catalogScopeStore.catalog_only_in_stock));
        } else {
            setEnterpriseCatalogOnlyInStockDraft(true);
        }
    }, [catalogScopeStore]);

    useEffect(() => {
        if (!isBaselineEnterprise) {
            return;
        }
        setStoreDraft((prev) => {
            if (
                prev.is_legacy_default === true
                && prev.code_strategy === "legacy_same"
                && prev.name_strategy === "base"
                && !prev.code_prefix
            ) {
                return prev;
            }
            return {
                ...prev,
                is_legacy_default: true,
                code_strategy: "legacy_same",
                code_prefix: "",
                name_strategy: "base",
            };
        });
    }, [isBaselineEnterprise]);

    useEffect(() => {
        if (!isNewStoreDraft || !selectedEnterpriseCode) {
            return;
        }

        const currentBranch = String(storeDraft.tabletki_branch || "").trim();
        if (currentBranch && mappingBranchValues.has(currentBranch)) {
            return;
        }

        const preferredBranch = (
            mappingBranchOptions.find((item) => Boolean(item.is_primary_enterprise_branch))?.branch
            || mappingBranchOptions[0]?.branch
            || String(enterpriseDraft.branch_id || "").trim()
        );
        if (!preferredBranch) {
            return;
        }

        replaceStoreDraft({
            ...storeDraft,
            store_code: buildSuggestedStoreCode(enterpriseDraft, storesForSelectedEnterprise, preferredBranch),
            store_name: buildStoreNameForBranch(enterpriseDraft, preferredBranch),
            enterprise_code: String(enterpriseDraft.enterprise_code || "").trim(),
            tabletki_enterprise_code: String(enterpriseDraft.enterprise_code || "").trim(),
            tabletki_branch: preferredBranch,
            is_legacy_default: isBaselineEnterprise,
            code_strategy: isBaselineEnterprise ? "legacy_same" : "opaque_mapping",
            name_strategy: isBaselineEnterprise ? "base" : "supplier_random",
        }, null, { dirty: false });
    }, [
        enterpriseDraft,
        isNewStoreDraft,
        isBaselineEnterprise,
        mappingBranchOptions,
        mappingBranchValues,
        replaceStoreDraft,
        selectedEnterpriseCode,
        storeDraft,
        storeDraft.tabletki_branch,
        storesForSelectedEnterprise,
    ]);

    useEffect(() => {
        const requestId = enterpriseContextRequestIdRef.current + 1;
        enterpriseContextRequestIdRef.current = requestId;
        let cancelled = false;

        const isLatestRequest = () => !cancelled && enterpriseContextRequestIdRef.current === requestId;

        async function loadEnterpriseContext() {
            if (!selectedEnterpriseCode) {
                setEnterpriseDraft(initialEnterpriseDraft);
                replaceStoreDraft(initialStoreDraft, null, { dirty: false });
                setSelectedStoreId(null);
                return;
            }

            try {
                const enterprise = await getEnterpriseByCode(selectedEnterpriseCode);
                if (!isLatestRequest()) {
                    return;
                }
                setEnterpriseDraft(buildEnterpriseDraft(enterprise));

                const storeForSelection = storesForSelectedEnterprise.find(
                    (item) => item.id === selectedStoreId,
                ) || null;

                if (storeForSelection) {
                    if (!storeDraftDirtyRef.current) {
                        replaceStoreDraft(buildStoreDraftFromStore(storeForSelection), storeForSelection.id, { dirty: false });
                    }
                } else if (storesForSelectedEnterprise.length > 0) {
                    if (storeDraftDirtyRef.current) {
                        return;
                    }
                    const firstStore = storesForSelectedEnterprise[0];
                    setSelectedStoreId(firstStore.id);
                    replaceStoreDraft(buildStoreDraftFromStore(firstStore), firstStore.id, { dirty: false });
                } else {
                    if (storeDraftDirtyRef.current) {
                        return;
                    }
                    setSelectedStoreId(null);
                    replaceStoreDraft(buildStoreDraftFromEnterprise(enterprise, storesForSelectedEnterprise), null, { dirty: false });
                }
            } catch (error) {
                if (!isLatestRequest()) {
                    return;
                }
                handleAuthError(error);
                console.error("Error loading selected Business enterprise:", error);
                setPageError(formatApiError(error, "Не удалось загрузить данные выбранного Business-предприятия."));
            }
        }

        loadEnterpriseContext();
        return () => {
            cancelled = true;
        };
    }, [replaceStoreDraft, selectedEnterpriseCode, selectedStoreId, storesForSelectedEnterprise]);

    const onEnterpriseChange = (key, value) => {
        setEnterpriseDraft((prev) => ({ ...prev, [key]: value }));
    };

    const onStoreChange = (key, value) => {
        storeDraftDirtyRef.current = true;
        setStoreDraft((prev) => ({ ...prev, [key]: value }));
    };

    const selectOverlay = (store) => {
        setStoreError("");
        setStoreSuccess("");
        setSelectedStoreId(store.id);
        replaceStoreDraft(buildStoreDraftFromStore(store), store.id, { dirty: false });
    };

    const handleSaveEnterprise = async () => {
        setEnterpriseSaving(true);
        setEnterpriseError("");
        setEnterpriseSuccess("");
        try {
            const payload = buildEnterprisePayload(enterpriseDraft);
            await updateEnterprise(selectedEnterpriseCode, payload);
            if (catalogScopeStore) {
                await axios.put(
                    `${API_BASE_URL}/business-stores/${catalogScopeStore.id}`,
                    { catalog_only_in_stock: Boolean(enterpriseCatalogOnlyInStockDraft) },
                    getAuthHeaders(),
                );
            }
            if (enterpriseStrategyStoreId) {
                await axios.put(
                    `${API_BASE_URL}/business-stores/${enterpriseStrategyStoreId}`,
                    {
                        is_legacy_default: isBaselineEnterprise ? true : Boolean(storeDraft.is_legacy_default),
                        code_strategy: isBaselineEnterprise ? "legacy_same" : String(storeDraft.code_strategy || "opaque_mapping"),
                        name_strategy: isBaselineEnterprise ? "base" : String(storeDraft.name_strategy || "base"),
                        code_prefix: isBaselineEnterprise ? null : normalizeOptionalText(storeDraft.code_prefix),
                    },
                    getAuthHeaders(),
                );
            }
            const [updatedEnterprise] = await Promise.all([
                getEnterpriseByCode(selectedEnterpriseCode),
                loadBusinessEnterprises(),
                loadStores(),
                loadMappingBranches(selectedEnterpriseCode),
            ]);
            setEnterpriseDraft(buildEnterpriseDraft(updatedEnterprise));
            setStoreDraft((prev) => ({
                ...prev,
                enterprise_code: String(updatedEnterprise.enterprise_code || ""),
                tabletki_branch: prev.tabletki_branch || String(updatedEnterprise.branch_id || ""),
            }));
            setEnterpriseSuccess("Настройки предприятия и ассортимента каталога сохранены.");
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
            if (selectedStoreId && selectedStoreId !== storeDraftStoreId) {
                const message = (
                    "Сохранение заблокировано: данные формы относятся к другому магазину. "
                    + "Выберите магазин ещё раз и повторите изменение."
                );
                console.warn("BusinessStore draft owner mismatch", {
                    selectedStoreId,
                    storeDraftStoreId,
                    store_code: storeDraft.store_code,
                });
                setStoreError(message);
                return;
            }

            const payload = buildStorePayload(storeDraft, selectedEnterpriseCode, { isBaselineEnterprise });
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
            replaceStoreDraft(buildStoreDraftFromStore(savedStore), savedStore.id, { dirty: false });
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

    return (
        <div style={pageStyle}>
            <div style={{ ...cardStyle, padding: "20px 24px", display: "grid", gap: "10px" }}>
                <h1 style={{ margin: 0, fontSize: "28px", color: "#111827" }}>Business-магазины</h1>
                <p style={mutedTextStyle}>
                    Основная operational page для enterprise runtime и store overlays Business-контура.
                </p>
                <p style={{ ...mutedTextStyle, fontSize: "13px" }}>
                    Сначала настраивается предприятие, затем — конкретные магазины.
                </p>
            </div>

            {pageError ? <div style={redWarningCardStyle}>{pageError}</div> : null}

            <Section
                title="1. Выбор предприятия"
                description="Выберите Business-предприятие, для которого редактируются runtime-настройки и store overlays."
            >
                <div style={{ ...formGridStyle, gridTemplateColumns: "minmax(320px, 480px) 1fr" }}>
                    <Field label="Business-предприятие">
                        <select
                            style={inputStyle}
                            value={selectedEnterpriseCode}
                            onChange={(event) => {
                                setSelectedEnterpriseCode(event.target.value);
                                setSelectedStoreId(null);
                                replaceStoreDraft(initialStoreDraft, null, { dirty: false });
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
                    <div style={{ ...neutralInfoCardStyle, display: "grid", alignContent: "center" }}>
                        Сначала настраивается предприятие, затем — конкретные магазины.
                    </div>
                </div>

                {selectedEnterpriseMeta ? (
                    <div style={{ display: "grid", gridTemplateColumns: "repeat(5, minmax(0, 1fr))", gap: "12px" }}>
                        <InfoItem label="Код предприятия" value={selectedEnterpriseMeta.enterprise_code} />
                        <InfoItem label="Название" value={selectedEnterpriseMeta.enterprise_name} />
                        <InfoItem label="Режим" value={isCustomEnterprise ? "Настраиваемый" : "Базовый"} />
                        <InfoItem label="Каталог предприятия" value={boolOnOff(selectedEnterpriseMeta.catalog_enabled)} />
                        <InfoItem
                            label="Остатки / заказы предприятия"
                            value={`остатки: ${boolOnOff(selectedEnterpriseMeta.stock_enabled)} / заказы: ${boolOnOff(selectedEnterpriseMeta.order_fetcher)}`}
                        />
                        <InfoItem label="Branch каталога" value={enterpriseDraft.branch_id || emptyValue} />
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
                        description="Источник: enterprise_settings и основной BusinessStore. Здесь управляются режим, коды для каталога, остатков, заказов и статусов."
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
                            <label style={{ ...labelStyle, display: "flex", alignItems: "center", gap: "10px" }}>
                                <input
                                    type="checkbox"
                                    style={checkboxStyle}
                                    checked={Boolean(enterpriseDraft.catalog_enabled)}
                                    onChange={(event) => onEnterpriseChange("catalog_enabled", event.target.checked)}
                                />
                                Каталог предприятия включён
                            </label>
                            <label style={{ ...labelStyle, display: "flex", alignItems: "center", gap: "10px" }}>
                                <input
                                    type="checkbox"
                                    style={checkboxStyle}
                                    checked={Boolean(enterpriseDraft.stock_enabled)}
                                    onChange={(event) => onEnterpriseChange("stock_enabled", event.target.checked)}
                                />
                                Остатки предприятия включены
                            </label>
                            <label style={{ ...labelStyle, display: "flex", alignItems: "center", gap: "10px" }}>
                                <input
                                    type="checkbox"
                                    style={checkboxStyle}
                                    checked={Boolean(enterpriseDraft.order_fetcher)}
                                    onChange={(event) => onEnterpriseChange("order_fetcher", event.target.checked)}
                                />
                                Получение заказов предприятия
                            </label>
                            <label style={{ ...labelStyle, display: "flex", alignItems: "center", gap: "10px" }}>
                                <input
                                    type="checkbox"
                                    style={checkboxStyle}
                                    checked={Boolean(enterpriseDraft.auto_confirm)}
                                    onChange={(event) => onEnterpriseChange("auto_confirm", event.target.checked)}
                                />
                                Автобронирование
                            </label>
                            <label style={{ ...labelStyle, display: "flex", alignItems: "center", gap: "10px" }}>
                                <input
                                    type="checkbox"
                                    style={checkboxStyle}
                                    checked={Boolean(enterpriseDraft.stock_correction)}
                                    onChange={(event) => onEnterpriseChange("stock_correction", event.target.checked)}
                                />
                                Коррекция остатков
                            </label>
                        </div>
                        <div style={{ display: "grid", gap: "16px" }}>
                            <h3 style={subSectionTitleStyle}>Стратегии кодов и названий</h3>
                            <div style={{ display: "grid", gap: "16px", maxWidth: "980px" }}>
                                <Field
                                    label="Режим"
                                    helpText={runtimeModeHelpText}
                                >
                                    <select
                                        style={inputStyle}
                                        value={enterpriseDraft.business_runtime_mode || "baseline"}
                                        onChange={(event) => onEnterpriseChange("business_runtime_mode", event.target.value)}
                                        disabled={isRuntimeModeLocked}
                                    >
                                        <option value="baseline">Базовый</option>
                                        <option value="custom">Настраиваемый</option>
                                    </select>
                                </Field>
                                <Field
                                    label="Стратегия кодов"
                                    helpText={codeStrategyOptions.find((item) => item.value === effectiveCodeStrategy)?.help}
                                >
                                    <select
                                        style={inputStyle}
                                        value={effectiveCodeStrategy}
                                        onChange={(event) => onStoreChange("code_strategy", event.target.value)}
                                        disabled={identityControlsDisabled}
                                    >
                                        {codeStrategyOptions.map((item) => (
                                            <option key={item.value} value={item.value}>{item.label}</option>
                                        ))}
                                    </select>
                                </Field>
                                <Field
                                    label="Стратегия названий"
                                    helpText={nameStrategyOptions.find((item) => item.value === effectiveNameStrategy)?.help}
                                >
                                    <select
                                        style={inputStyle}
                                        value={effectiveNameStrategy}
                                        onChange={(event) => onStoreChange("name_strategy", event.target.value)}
                                        disabled={identityControlsDisabled}
                                    >
                                        {nameStrategyOptions.map((item) => (
                                            <option key={item.value} value={item.value}>{item.label}</option>
                                        ))}
                                    </select>
                                </Field>
                                <label style={{ ...labelStyle, display: "flex", alignItems: "center", gap: "10px", minHeight: "42px" }}>
                                    <input
                                        type="checkbox"
                                        style={checkboxStyle}
                                        checked={Boolean(enterpriseCatalogOnlyInStockDraft)}
                                        onChange={(event) => setEnterpriseCatalogOnlyInStockDraft(event.target.checked)}
                                        disabled={!catalogScopeStore}
                                    />
                                    В каталог только товары с остатком главного магазина
                                </label>
                            </div>
                            {effectiveCodeStrategy === "prefix_mapping" ? (
                                <div style={{ display: "grid", gap: "16px", maxWidth: "980px" }}>
                                    <Field label="Префикс кода">
                                        <input
                                            style={inputStyle}
                                            value={storeDraft.code_prefix}
                                            onChange={(event) => onStoreChange("code_prefix", event.target.value)}
                                            disabled={identityControlsDisabled}
                                        />
                                    </Field>
                                </div>
                            ) : null}
                        </div>
                        <div style={{ display: "grid", gap: "16px" }}>
                            <h3 style={subSectionTitleStyle}>Доступ предприятия</h3>
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
                                <Field label="SalesDrive API key" helpText="Оставьте поле пустым, чтобы не менять текущий токен.">
                                    <input
                                        type="password"
                                        style={inputStyle}
                                        value={enterpriseDraft.token}
                                        onChange={(event) => onEnterpriseChange("token", event.target.value)}
                                        placeholder="Введите новый токен"
                                    />
                                </Field>
                            </div>
                        </div>
                    </Section>

                    {storeError ? <div style={redWarningCardStyle}>{storeError}</div> : null}
                    {storeSuccess ? <div style={successCardStyle}>{storeSuccess}</div> : null}

                    <Section
                        title={isNewStoreDraft ? "3. Новый магазин Business-контура" : "3. Магазин Business-контура"}
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
                        {enterpriseDraft.business_runtime_mode === "baseline" ? (
                            <div style={neutralInfoCardStyle}>
                                Для предприятия в базовом режиме коды и названия фиксированы как базовые: без маппинга для каталога, остатков, заказов и статусов.
                            </div>
                        ) : null}
                        <div style={{ display: "grid", gap: "16px" }}>
                            <h3 style={subSectionTitleStyle}>Данные магазина</h3>
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
                                <label style={{ ...labelStyle, display: "flex", alignItems: "center", gap: "10px" }}>
                                    <input
                                        type="checkbox"
                                        style={checkboxStyle}
                                        checked={Boolean(storeDraft.is_active)}
                                        onChange={(event) => onStoreChange("is_active", event.target.checked)}
                                    />
                                    Магазин активен
                                </label>
                                {[
                                    ["stock_enabled", "Остатки магазина включены"],
                                    ["orders_enabled", "Заказы магазина включены"],
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
                            </div>
                        </div>

                        <div style={{ display: "grid", gap: "16px" }}>
                            <h3 style={subSectionTitleStyle}>Branch и интеграции</h3>
                            <p style={mutedTextStyle}>
                                Branch магазина используется как branch в stock payload и routing заказов.
                            </p>
                            <div style={formGridStyle}>
                                <Field label="Branch магазина">
                                    <select
                                        style={inputStyle}
                                        value={storeDraft.tabletki_branch}
                                        onChange={(event) => onStoreChange("tabletki_branch", event.target.value)}
                                        disabled={storeRoutingReadOnly || mappingBranchesLoading || mappingBranchOptions.length === 0}
                                    >
                                        {currentStoreBranchMissingFromOptions && storeDraft.tabletki_branch ? (
                                            <option value={storeDraft.tabletki_branch}>
                                                {`${storeDraft.tabletki_branch} — текущий branch вне mapping_branch`}
                                            </option>
                                        ) : null}
                                        <option value="">
                                            {mappingBranchesLoading
                                                ? "Загрузка branch..."
                                                : (mappingBranchOptions.length > 0
                                                    ? "Выберите branch из mapping_branch"
                                                    : "Нет branch в mapping_branch")}
                                        </option>
                                        {mappingBranchOptions.map((item) => (
                                            <option key={item.branch} value={item.branch}>
                                                {item.is_primary_enterprise_branch
                                                    ? `${item.branch} — основной branch предприятия`
                                                    : item.branch}
                                            </option>
                                        ))}
                                    </select>
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
                            {mappingBranchesError ? (
                                <div style={redWarningCardStyle}>{mappingBranchesError}</div>
                            ) : null}
                            {!mappingBranchesLoading && mappingBranchOptions.length === 0 ? (
                                <div style={warningCardStyle}>
                                    Для выбранного предприятия нет branch в `mapping_branch`.
                                </div>
                            ) : null}
                            {currentStoreBranchMissingFromOptions ? (
                                <div style={redWarningCardStyle}>
                                    Текущий branch магазина отсутствует в `mapping_branch` выбранного предприятия.
                                </div>
                            ) : null}
                        </div>

                        <div style={{ display: "grid", gap: "16px" }}>
                            <h3 style={subSectionTitleStyle}>Store-specific pricing</h3>
                            <div style={neutralInfoCardStyle}>
                                Дополнительная наценка для нового stock-контура настраивается на странице поставщика
                                в блоке `Настройки поставщика по магазину`. Store-level поля наценки на этой странице
                                больше не являются основным рабочим интерфейсом.
                            </div>
                        </div>

                    </Section>

                    <Section
                        title="4. Магазины предприятия"
                        description="Операционный список store overlays выбранного Business-предприятия."
                    >
                        {storesForSelectedEnterprise.length > 0 ? (
                        <div style={{ overflow: "auto", maxHeight: "420px", border: "1px solid #e2e8f0", borderRadius: "12px" }}>
                            <table style={{ width: "100%", borderCollapse: "collapse", minWidth: "1000px" }}>
                                <thead>
                                    <tr>
                                        {[
                                            "Код магазина",
                                            "Branch",
                                            "Активен",
                                            "Остатки",
                                            "Заказы",
                                            "SalesDrive ID",
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
                                            <td style={tableCellStyle}>{item.tabletki_branch || emptyValue}</td>
                                            <td style={tableCellStyle}>{item.is_active ? "Да" : "Нет"}</td>
                                            <td style={tableCellStyle}>{boolShort(item.stock_enabled)}</td>
                                            <td style={tableCellStyle}>{boolShort(item.orders_enabled)}</td>
                                            <td style={tableCellStyle}>{item.salesdrive_enterprise_id ?? emptyValue}</td>
                                            <td style={tableCellStyle}>
                                                <button type="button" style={secondaryButtonStyle} onClick={() => selectOverlay(item)}>
                                                    Открыть
                                                </button>
                                            </td>
                                        </tr>
                                    ))}
                                </tbody>
                            </table>
                        </div>
                        ) : (
                            <div style={neutralInfoCardStyle}>
                                Для выбранного предприятия пока нет сохранённых магазинов. Заполните форму выше и сохраните первый store overlay.
                            </div>
                        )}
                    </Section>
                </>
            ) : null}
        </div>
    );
};

export default BusinessStoresPage;
