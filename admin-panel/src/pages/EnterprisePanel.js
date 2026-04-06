import React, { useEffect, useMemo, useState } from "react";
import {
    createEnterprise,
    getEnterpriseByCode,
    getEnterprises,
    getEnterpriseViewDetail,
    getEnterpriseViewList,
    updateEnterprise,
} from "../api/enterpriseApi";
import developerApi from "../api/developerApi";

const { getDataFormats } = developerApi;

const pageStyle = {
    padding: "24px",
    display: "grid",
    gap: "20px",
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

const mutedTextStyle = {
    margin: 0,
    color: "#64748b",
    fontSize: "14px",
    lineHeight: 1.5,
};

const labelStyle = {
    display: "block",
    textAlign: "left",
    marginBottom: "6px",
    fontWeight: 600,
    fontSize: "14px",
    color: "#111827",
};

const inputStyle = {
    width: "100%",
    padding: "10px 12px",
    borderRadius: "8px",
    border: "1px solid #cbd5e1",
    fontSize: "14px",
    boxSizing: "border-box",
    backgroundColor: "#ffffff",
};

const primaryButtonStyle = {
    padding: "10px 16px",
    backgroundColor: "#2563eb",
    color: "#ffffff",
    border: "none",
    borderRadius: "8px",
    cursor: "pointer",
    fontWeight: 700,
};

const secondaryButtonStyle = {
    padding: "10px 16px",
    backgroundColor: "#ffffff",
    color: "#111827",
    border: "1px solid #cbd5e1",
    borderRadius: "8px",
    cursor: "pointer",
    fontWeight: 700,
};

const badgeStyle = {
    display: "inline-block",
    padding: "5px 9px",
    borderRadius: "999px",
    fontSize: "12px",
    fontWeight: 600,
    backgroundColor: "#eff6ff",
    color: "#1d4ed8",
};

const warningBadgeStyle = {
    ...badgeStyle,
    backgroundColor: "#fff4e5",
    color: "#9a3412",
};

const emptyValue = "—";

const HIDDEN_HELP_TEXT_FIELDS = new Set(["data_format", "branch_id", "order_fetcher"]);

const DEFAULT_ENTERPRISE_VALUES = {
    enterprise_code: "",
    enterprise_name: "",
    data_format: "",
    branch_id: "",
    catalog_upload_frequency: "",
    stock_upload_frequency: "",
    catalog_enabled: true,
    stock_enabled: true,
    order_fetcher: false,
    tabletki_login: "",
    tabletki_password: "",
    auto_confirm: false,
    discount_rate: "",
    stock_correction: false,
    token: "",
    single_store: false,
    store_serial: "",
    google_drive_folder_id_ref: "",
    google_drive_folder_id_rest: "",
    last_stock_upload: null,
    last_catalog_upload: null,
};

const FALLBACK_FIELD_META = [
    { key: "enterprise_code", label: "Код предприятия", field_type: "text", readonly: false },
    { key: "enterprise_name", label: "Название предприятия", field_type: "text", readonly: false },
    {
        key: "data_format",
        label: "Формат данных",
        field_type: "select",
        readonly: false,
        help_text: null,
    },
    { key: "branch_id", label: "Branch ID", field_type: "text", readonly: false, help_text: null },
    { key: "catalog_upload_frequency", label: "Частота загрузки каталога", field_type: "number", readonly: false },
    { key: "stock_upload_frequency", label: "Частота загрузки остатков", field_type: "number", readonly: false },
    { key: "catalog_stock_enabled", label: "Выгрузка каталога и остатков", field_type: "checkbox", readonly: false, help_text: null },
    {
        key: "order_fetcher",
        label: "Получение заказов",
        field_type: "checkbox",
        readonly: false,
        help_text: null,
    },
    { key: "tabletki_login", label: "Логин Tabletki", field_type: "text", readonly: false },
    { key: "tabletki_password", label: "Пароль Tabletki", field_type: "password", readonly: false },
    { key: "auto_confirm", label: "Автоматическое бронирование", field_type: "checkbox", readonly: false },
    { key: "discount_rate", label: "Размер скидки", field_type: "number", readonly: false },
    { key: "stock_correction", label: "Коррекция остатков", field_type: "checkbox", readonly: false },
    {
        key: "token",
        label: "Токен / URL / ключ подключения",
        field_type: "text",
        readonly: false,
        help_text: "Используется как источник подключения для выбранного формата.",
    },
    {
        key: "single_store",
        label: "Single Store",
        field_type: "checkbox",
        readonly: false,
        help_text: null,
    },
    { key: "store_serial", label: "Store Serial", field_type: "text", readonly: false },
    { key: "google_drive_folder_id_ref", label: "Google Drive Folder ID для каталога", field_type: "text", readonly: false },
    { key: "google_drive_folder_id_rest", label: "Google Drive Folder ID для остатков", field_type: "text", readonly: false },
    { key: "last_stock_upload", label: "Последняя загрузка остатков", field_type: "datetime", readonly: true },
    { key: "last_catalog_upload", label: "Последняя загрузка каталога", field_type: "datetime", readonly: true },
];

const FALLBACK_SECTIONS = [
    { key: "main", title: "Основное", collapsible: false, default_open: true, field_keys: ["enterprise_code", "enterprise_name", "data_format"] },
    { key: "orders_export", title: "Экспорт и заказы", collapsible: false, default_open: true, field_keys: ["tabletki_login", "tabletki_password", "branch_id", "auto_confirm", "discount_rate", "stock_correction", "order_fetcher"] },
    { key: "scheduler", title: "Расписание", collapsible: false, default_open: true, field_keys: ["catalog_upload_frequency", "stock_upload_frequency", "catalog_stock_enabled"] },
    { key: "source", title: "Источник / подключение", collapsible: false, default_open: true, field_keys: ["token"] },
    {
        key: "format_fields",
        title: "Дополнительные поля формата",
        description: null,
        collapsible: true,
        default_open: false,
        field_keys: ["single_store", "store_serial", "google_drive_folder_id_ref", "google_drive_folder_id_rest"],
    },
    {
        key: "runtime",
        title: "Служебная информация",
        description: null,
        collapsible: true,
        default_open: false,
        field_keys: ["last_stock_upload", "last_catalog_upload"],
    },
];

const formatDateTime = (value) => {
    if (!value) {
        return emptyValue;
    }

    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
        return value;
    }

    return date.toLocaleString("uk-UA");
};

const isFileRelatedFormat = (dataFormat) => {
    const normalized = String(dataFormat || "").trim();
    return [
        "GoogleDrive",
        "JetVet",
        "TorgsoftGoogle",
        "TorgsoftGoogleMulti",
        "Ftp",
        "FtpMulti",
        "FtpZoomagazin",
        "FtpTabletki",
    ].includes(normalized);
};

const hasFormatSpecificFields = (values = {}) => (
    Boolean(values.single_store)
    || Boolean(String(values.store_serial || "").trim())
    || Boolean(String(values.google_drive_folder_id_ref || "").trim())
    || Boolean(String(values.google_drive_folder_id_rest || "").trim())
);

const buildFallbackListFromRawEnterprises = (items = []) => items.map((enterprise) => ({
    enterprise_code: enterprise.enterprise_code,
    enterprise_name: enterprise.enterprise_name,
    data_format: enterprise.data_format || null,
    branch_id: enterprise.branch_id || null,
    catalog_upload_frequency: enterprise.catalog_upload_frequency ?? null,
    stock_upload_frequency: enterprise.stock_upload_frequency ?? null,
    catalog_enabled: enterprise.catalog_enabled !== false,
    stock_enabled: enterprise.stock_enabled !== false,
    order_fetcher: Boolean(enterprise.order_fetcher),
    last_stock_upload: enterprise.last_stock_upload || null,
    last_catalog_upload: enterprise.last_catalog_upload || null,
    is_blank_format: String(enterprise.data_format || "").trim() === "Blank",
    has_format_specific_fields: (
        isFileRelatedFormat(enterprise.data_format)
        || hasFormatSpecificFields(enterprise)
    ),
}));

const buildFallbackDetailFromRawEnterprise = (enterprise) => ({
    enterprise_code: enterprise.enterprise_code,
    enterprise_name: enterprise.enterprise_name,
    data_format: enterprise.data_format || null,
    catalog_enabled: enterprise.catalog_enabled !== false,
    stock_enabled: enterprise.stock_enabled !== false,
    values: {
        ...DEFAULT_ENTERPRISE_VALUES,
        ...enterprise,
        catalog_enabled: enterprise.catalog_enabled !== false,
        stock_enabled: enterprise.stock_enabled !== false,
    },
    field_meta: FALLBACK_FIELD_META,
    sections: FALLBACK_SECTIONS,
    show_format_fields_block: (
        isFileRelatedFormat(enterprise.data_format)
        || hasFormatSpecificFields(enterprise)
    ),
    show_runtime_block: true,
});

const normalizeDraftValues = (values = {}) => ({
    ...DEFAULT_ENTERPRISE_VALUES,
    ...values,
    catalog_enabled: values.catalog_enabled !== false,
    stock_enabled: values.stock_enabled !== false,
    order_fetcher: Boolean(values.order_fetcher),
    auto_confirm: Boolean(values.auto_confirm),
    stock_correction: Boolean(values.stock_correction),
    single_store: Boolean(values.single_store),
});

const EnterpriseInfoItem = ({ label, value }) => (
    <div
        style={{
            backgroundColor: "#f8fafc",
            border: "1px solid #dbe4ee",
            borderRadius: "10px",
            padding: "12px 14px",
            display: "grid",
            gap: "4px",
        }}
    >
        <div style={{ fontSize: "13px", color: "#64748b", fontWeight: 600 }}>{label}</div>
        <div style={{ fontSize: "15px", color: "#111827", fontWeight: 600 }}>{value || emptyValue}</div>
    </div>
);

const EnterprisePanel = () => {
    const [enterpriseList, setEnterpriseList] = useState([]);
    const [dataFormats, setDataFormats] = useState([]);
    const [selectedEnterpriseCode, setSelectedEnterpriseCode] = useState("");
    const [detailView, setDetailView] = useState(null);
    const [draftValues, setDraftValues] = useState(DEFAULT_ENTERPRISE_VALUES);
    const [isCreating, setIsCreating] = useState(false);
    const [listLoading, setListLoading] = useState(true);
    const [detailLoading, setDetailLoading] = useState(false);
    const [listError, setListError] = useState("");
    const [detailError, setDetailError] = useState("");
    const [saveError, setSaveError] = useState("");
    const [saveSuccess, setSaveSuccess] = useState("");
    const [showOnlyActive, setShowOnlyActive] = useState(false);
    const [openSections, setOpenSections] = useState({
        format_fields: false,
        runtime: false,
    });

    const loadEnterpriseList = async () => {
        setListLoading(true);
        setListError("");
        try {
            let data;
            try {
                data = await getEnterpriseViewList();
            } catch (viewError) {
                console.error("Error loading enterprise view list, falling back to raw enterprise list:", viewError);
                const rawEnterprises = await getEnterprises();
                data = buildFallbackListFromRawEnterprises(rawEnterprises);
            }
            setEnterpriseList(data);
            if (!selectedEnterpriseCode && data.length > 0 && !isCreating) {
                setSelectedEnterpriseCode(data[0].enterprise_code);
            }
        } catch (error) {
            console.error("Error loading enterprise view list:", error);
            setListError("Не удалось загрузить список предприятий.");
        } finally {
            setListLoading(false);
        }
    };

    useEffect(() => {
        const fetchInitialData = async () => {
            try {
                const formats = await getDataFormats();
                setDataFormats(formats);
            } catch (error) {
                console.error("Error loading data formats:", error);
            }
            await loadEnterpriseList();
        };

        fetchInitialData();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    useEffect(() => {
        if (!selectedEnterpriseCode || isCreating) {
            return;
        }

        const loadDetail = async () => {
            setDetailLoading(true);
            setDetailError("");
            try {
                let data;
                try {
                    data = await getEnterpriseViewDetail(selectedEnterpriseCode);
                } catch (viewError) {
                    console.error("Error loading enterprise view detail, falling back to raw enterprise detail:", viewError);
                    const rawEnterprise = await getEnterpriseByCode(selectedEnterpriseCode);
                    data = buildFallbackDetailFromRawEnterprise(rawEnterprise);
                }
                setDetailView(data);
                setDraftValues(normalizeDraftValues(data.values));
                setOpenSections((prev) => ({
                    ...prev,
                    format_fields: prev.format_fields,
                }));
            } catch (error) {
                console.error("Error loading enterprise detail:", error);
                setDetailError("Не удалось загрузить детальную информацию по предприятию.");
                setDetailView(null);
            } finally {
                setDetailLoading(false);
            }
        };

        loadDetail();
    }, [selectedEnterpriseCode, isCreating]);

    const fieldMetaMap = useMemo(() => {
        const backendMeta = detailView?.field_meta || [];
        const meta = [...backendMeta];
        const existingKeys = new Set(backendMeta.map((field) => field.key));

        FALLBACK_FIELD_META.forEach((field) => {
            if (!existingKeys.has(field.key)) {
                meta.push(field);
            }
        });

        return meta.reduce((acc, field) => {
            acc[field.key] = field;
            return acc;
        }, {});
    }, [detailView]);

    const sectionList = useMemo(() => {
        const sourceSections = detailView?.sections || FALLBACK_SECTIONS;
        const normalizedSections = sourceSections.map((section) => {
            if (section.key === "main") {
                return {
                    ...section,
                    field_keys: ["enterprise_code", "enterprise_name", "data_format"],
                };
            }

            if (section.key === "orders_export") {
                return {
                    ...section,
                    field_keys: ["tabletki_login", "tabletki_password", "branch_id", "auto_confirm", "discount_rate", "stock_correction", "order_fetcher"],
                };
            }

            if (section.key === "scheduler") {
                return {
                    ...section,
                    field_keys: ["catalog_upload_frequency", "stock_upload_frequency", "catalog_stock_enabled"],
                };
            }

            if (section.key === "format_fields" || section.key === "runtime") {
                return {
                    ...section,
                    description: null,
                };
            }

            return section;
        });

        const sectionOrder = ["scheduler", "main", "source", "orders_export", "format_fields", "runtime"];
        return normalizedSections.sort((a, b) => sectionOrder.indexOf(a.key) - sectionOrder.indexOf(b.key));
    }, [detailView]);

    const filteredEnterpriseList = useMemo(() => (
        showOnlyActive
            ? enterpriseList.filter((enterprise) => enterprise.catalog_enabled && enterprise.stock_enabled)
            : enterpriseList
    ), [enterpriseList, showOnlyActive]);

    const resetToCreate = () => {
        setIsCreating(true);
        setSelectedEnterpriseCode("");
        setDetailView(null);
        setDetailError("");
        setSaveError("");
        setSaveSuccess("");
        setDraftValues(DEFAULT_ENTERPRISE_VALUES);
        setOpenSections({
            format_fields: false,
            runtime: false,
        });
    };

    const selectEnterprise = (enterpriseCode) => {
        setIsCreating(false);
        setSaveError("");
        setSaveSuccess("");
        setSelectedEnterpriseCode(enterpriseCode);
    };

    const handleFieldChange = (key, value) => {
        if (key === "catalog_stock_enabled") {
            setDraftValues((prev) => ({
                ...prev,
                catalog_enabled: Boolean(value),
                stock_enabled: Boolean(value),
            }));
            return;
        }

        setDraftValues((prev) => ({
            ...prev,
            [key]: value,
        }));
    };

    const toggleSection = (key) => {
        setOpenSections((prev) => ({
            ...prev,
            [key]: !prev[key],
        }));
    };

    const buildSavePayload = () => ({
        enterprise_code: String(draftValues.enterprise_code || "").trim(),
        enterprise_name: String(draftValues.enterprise_name || "").trim(),
        data_format: String(draftValues.data_format || "").trim() || null,
        branch_id: String(draftValues.branch_id || "").trim(),
        catalog_upload_frequency: draftValues.catalog_upload_frequency === "" ? null : Number(draftValues.catalog_upload_frequency),
        stock_upload_frequency: draftValues.stock_upload_frequency === "" ? null : Number(draftValues.stock_upload_frequency),
        catalog_enabled: Boolean(draftValues.catalog_enabled),
        stock_enabled: Boolean(draftValues.stock_enabled),
        order_fetcher: Boolean(draftValues.order_fetcher),
        tabletki_login: String(draftValues.tabletki_login || "").trim() || null,
        tabletki_password: String(draftValues.tabletki_password || "").trim() || null,
        auto_confirm: Boolean(draftValues.auto_confirm),
        discount_rate: draftValues.discount_rate === "" ? null : Number(draftValues.discount_rate),
        stock_correction: Boolean(draftValues.stock_correction),
        token: String(draftValues.token || "").trim() || null,
        single_store: Boolean(draftValues.single_store),
        store_serial: String(draftValues.store_serial || "").trim() || null,
        google_drive_folder_id_ref: String(draftValues.google_drive_folder_id_ref || "").trim() || null,
        google_drive_folder_id_rest: String(draftValues.google_drive_folder_id_rest || "").trim() || null,
        last_stock_upload: draftValues.last_stock_upload || null,
        last_catalog_upload: draftValues.last_catalog_upload || null,
    });

    const handleSave = async () => {
        setSaveError("");
        setSaveSuccess("");

        const payload = buildSavePayload();
        if (!payload.enterprise_code || !payload.enterprise_name || !payload.branch_id) {
            setSaveError("Поля код предприятия, название и Branch ID должны быть заполнены.");
            return;
        }

        try {
            if (isCreating) {
                await createEnterprise(payload);
                setSaveSuccess("Предприятие успешно создано.");
                setIsCreating(false);
                setSelectedEnterpriseCode(payload.enterprise_code);
            } else {
                await updateEnterprise(selectedEnterpriseCode, payload);
                setSaveSuccess("Изменения сохранены.");
            }

            await loadEnterpriseList();
            if (payload.enterprise_code) {
                let viewDetail;
                try {
                    viewDetail = await getEnterpriseViewDetail(payload.enterprise_code);
                } catch (viewError) {
                    console.error("Error reloading enterprise view detail after save, falling back to raw enterprise detail:", viewError);
                    const rawEnterprise = await getEnterpriseByCode(payload.enterprise_code);
                    viewDetail = buildFallbackDetailFromRawEnterprise(rawEnterprise);
                }
                setDetailView(viewDetail);
                setDraftValues(normalizeDraftValues(viewDetail.values));
                setSelectedEnterpriseCode(payload.enterprise_code);
                setOpenSections((prev) => ({
                    ...prev,
                    format_fields: prev.format_fields,
                }));
            }
        } catch (error) {
            console.error("Error saving enterprise:", error);
            setSaveError("Не удалось сохранить настройки предприятия.");
        }
    };

    const handleReset = async () => {
        setSaveError("");
        setSaveSuccess("");

        if (isCreating) {
            setDraftValues(DEFAULT_ENTERPRISE_VALUES);
            return;
        }

        if (!selectedEnterpriseCode) {
            return;
        }

        try {
            let data;
            try {
                data = await getEnterpriseViewDetail(selectedEnterpriseCode);
            } catch (viewError) {
                console.error("Error resetting enterprise draft from view detail, falling back to raw enterprise detail:", viewError);
                const rawEnterprise = await getEnterpriseByCode(selectedEnterpriseCode);
                data = buildFallbackDetailFromRawEnterprise(rawEnterprise);
            }
            setDetailView(data);
            setDraftValues(normalizeDraftValues(data.values));
        } catch (error) {
            console.error("Error resetting enterprise draft:", error);
        }
    };

    const renderField = (fieldKey) => {
        const meta = fieldMetaMap[fieldKey];
        if (!meta) {
            return null;
        }

        const value = fieldKey === "catalog_stock_enabled"
            ? Boolean(draftValues.catalog_enabled) && Boolean(draftValues.stock_enabled)
            : draftValues[fieldKey];
        const readonly = Boolean(meta.readonly);
        const isCheckbox = meta.field_type === "checkbox";
        const isReadonlyDisplay = readonly || meta.field_type === "datetime";

        return (
            <div
                key={fieldKey}
                style={{
                    display: "grid",
                    gap: "6px",
                    alignContent: "start",
                    gridColumn: fieldKey === "order_fetcher" ? "1 / -1" : "auto",
                }}
            >
                <label style={labelStyle}>{meta.label}</label>

                {isReadonlyDisplay ? (
                    <div
                        style={{
                            minHeight: "42px",
                            padding: "10px 12px",
                            borderRadius: "8px",
                            border: "1px solid #dbe4ee",
                            backgroundColor: "#f8fafc",
                            color: "#334155",
                            display: "flex",
                            alignItems: "center",
                            fontSize: "14px",
                        }}
                    >
                        {meta.field_type === "datetime" ? formatDateTime(value) : (value || emptyValue)}
                    </div>
                ) : isCheckbox ? (
                    <label
                        style={{
                            display: "flex",
                            alignItems: "center",
                            gap: "10px",
                            minHeight: "42px",
                            padding: "6px 0",
                            color: "#111827",
                            fontWeight: 500,
                        }}
                    >
                        <input
                            type="checkbox"
                            checked={Boolean(value)}
                            onChange={(e) => handleFieldChange(fieldKey, e.target.checked)}
                            style={{ width: "18px", height: "18px" }}
                        />
                        <span>{Boolean(value) ? "Включено" : "Выключено"}</span>
                    </label>
                ) : meta.field_type === "select" ? (
                    <select
                        value={value || ""}
                        onChange={(e) => handleFieldChange(fieldKey, e.target.value)}
                        style={inputStyle}
                    >
                        <option value="">-- Выберите формат --</option>
                        {dataFormats.map((format) => (
                            <option key={format.id || format.format_name} value={format.format_name}>
                                {format.format_name}
                            </option>
                        ))}
                    </select>
                ) : (
                    <input
                        type={meta.field_type === "number" ? "number" : meta.field_type === "password" ? "password" : "text"}
                        value={value ?? ""}
                        onChange={(e) => handleFieldChange(fieldKey, e.target.value)}
                        style={inputStyle}
                    />
                )}

                {meta.help_text && !HIDDEN_HELP_TEXT_FIELDS.has(fieldKey) ? (
                    <div style={{ fontSize: "12px", lineHeight: 1.5, color: "#64748b" }}>{meta.help_text}</div>
                ) : null}
            </div>
        );
    };

    return (
        <div style={pageStyle}>
            <div style={{ ...cardStyle, padding: "20px 24px" }}>
                <h1 style={{ margin: 0, fontSize: "28px", color: "#111827" }}>Настройки предприятий</h1>
            </div>

            <div
                style={{
                    display: "grid",
                    gridTemplateColumns: "minmax(320px, 380px) minmax(0, 1fr)",
                    gap: "20px",
                    alignItems: "start",
                }}
            >
                <div style={{ ...cardStyle, padding: "18px 20px", display: "grid", gap: "16px" }}>
                    <div style={{ display: "grid", gap: "8px" }}>
                        <h2 style={sectionTitleStyle}>Выбор предприятия</h2>
                    </div>

                    <select
                        value={selectedEnterpriseCode}
                        onChange={(e) => {
                            if (!e.target.value) {
                                setSelectedEnterpriseCode("");
                                setDetailView(null);
                                return;
                            }
                            selectEnterprise(e.target.value);
                        }}
                        style={inputStyle}
                    >
                        <option value="">-- Выберите предприятие --</option>
                        {enterpriseList.map((enterprise) => (
                            <option key={enterprise.enterprise_code} value={enterprise.enterprise_code}>
                                {enterprise.enterprise_name} ({enterprise.enterprise_code})
                            </option>
                        ))}
                    </select>

                    <div style={{ display: "flex", gap: "12px", flexWrap: "wrap" }}>
                        <button type="button" style={primaryButtonStyle} onClick={resetToCreate}>
                            Новое предприятие
                        </button>
                        <button type="button" style={secondaryButtonStyle} onClick={loadEnterpriseList}>
                            Обновить список
                        </button>
                    </div>

                    <label
                        style={{
                            display: "flex",
                            alignItems: "center",
                            gap: "10px",
                            color: "#111827",
                            fontWeight: 500,
                        }}
                    >
                        <input
                            type="checkbox"
                            checked={showOnlyActive}
                            onChange={(e) => setShowOnlyActive(e.target.checked)}
                            style={{ width: "18px", height: "18px" }}
                        />
                        <span>Только активные</span>
                    </label>

                    {listLoading ? <div style={mutedTextStyle}>Загрузка предприятий…</div> : null}
                    {listError ? <div style={{ color: "#b91c1c", fontWeight: 600 }}>{listError}</div> : null}
                    {!listLoading && !listError && showOnlyActive && filteredEnterpriseList.length === 0 ? (
                        <div style={mutedTextStyle}>Нет активных предприятий</div>
                    ) : null}

                    <div style={{ display: "grid", gap: "10px" }}>
                        {filteredEnterpriseList.slice(0, 12).map((enterprise) => (
                            <button
                                key={enterprise.enterprise_code}
                                type="button"
                                onClick={() => selectEnterprise(enterprise.enterprise_code)}
                                style={{
                                    textAlign: "left",
                                    padding: "14px",
                                    borderRadius: "10px",
                                    border: enterprise.enterprise_code === selectedEnterpriseCode && !isCreating
                                        ? "2px solid #2563eb"
                                        : "1px solid #dbe4ee",
                                    backgroundColor: "#ffffff",
                                    cursor: "pointer",
                                    display: "grid",
                                    gap: "6px",
                                }}
                            >
                                <div style={{ fontSize: "15px", fontWeight: 700, color: "#111827" }}>
                                    {enterprise.enterprise_name}
                                </div>
                                <div style={{ fontSize: "13px", color: "#64748b" }}>
                                    Код: {enterprise.enterprise_code}
                                </div>
                                <div style={{ display: "flex", gap: "8px", flexWrap: "wrap" }}>
                                    <span style={enterprise.is_blank_format ? warningBadgeStyle : badgeStyle}>
                                        {enterprise.data_format || "—"}
                                    </span>
                                    {enterprise.order_fetcher ? <span style={badgeStyle}>orders</span> : null}
                                    {enterprise.has_format_specific_fields ? <span style={badgeStyle}>file/google</span> : null}
                                </div>
                            </button>
                        ))}
                    </div>
                </div>

                <div style={{ display: "grid", gap: "20px" }}>
                    <div style={{ ...cardStyle, padding: "18px 20px", display: "grid", gap: "16px" }}>
                        <div style={{ display: "flex", justifyContent: "space-between", gap: "16px", flexWrap: "wrap" }}>
                            <div style={{ display: "grid", gap: "8px" }}>
                                <h2 style={sectionTitleStyle}>
                                    {isCreating ? "Новое предприятие" : "Карточка предприятия"}
                                </h2>
                            </div>
                            <div style={{ display: "flex", gap: "10px", flexWrap: "wrap" }}>
                                <button type="button" style={primaryButtonStyle} onClick={handleSave}>
                                    Сохранить
                                </button>
                                <button type="button" style={secondaryButtonStyle} onClick={handleReset}>
                                    Сбросить
                                </button>
                            </div>
                        </div>

                        {saveError ? <div style={{ color: "#b91c1c", fontWeight: 600 }}>{saveError}</div> : null}
                        {saveSuccess ? <div style={{ color: "#166534", fontWeight: 600 }}>{saveSuccess}</div> : null}
                        {detailError ? <div style={{ color: "#b91c1c", fontWeight: 600 }}>{detailError}</div> : null}
                        {detailLoading ? <div style={mutedTextStyle}>Загрузка детальной информации…</div> : null}

                        <div
                            style={{
                                display: "grid",
                                gridTemplateColumns: "repeat(2, minmax(180px, 1fr))",
                                gap: "12px",
                            }}
                        >
                            <EnterpriseInfoItem label="Предприятие" value={draftValues.enterprise_name || emptyValue} />
                            <EnterpriseInfoItem label="Код" value={draftValues.enterprise_code || emptyValue} />
                            <EnterpriseInfoItem label="Формат" value={draftValues.data_format || emptyValue} />
                            <EnterpriseInfoItem label="Branch ID" value={draftValues.branch_id || emptyValue} />
                        </div>
                    </div>

                    {sectionList.map((section) => {
                        const isOpen = section.collapsible ? Boolean(openSections[section.key]) : true;
                        const sectionGridStyle = section.key === "main"
                            ? {
                                display: "grid",
                                gridTemplateColumns: "repeat(3, minmax(220px, 1fr))",
                                gap: "16px 20px",
                                alignItems: "start",
                            }
                            : section.key === "orders_export"
                                ? {
                                    display: "grid",
                                    gridTemplateColumns: "repeat(3, minmax(220px, 1fr))",
                                    gap: "16px 20px",
                                    alignItems: "start",
                                }
                            : {
                                display: "grid",
                                gridTemplateColumns: "repeat(auto-fit, minmax(260px, 1fr))",
                                gap: "16px 20px",
                            };
                        return (
                            <div key={section.key} style={{ ...cardStyle, padding: "18px 20px", display: "grid", gap: "16px" }}>
                                <div
                                    style={{
                                        display: "flex",
                                        justifyContent: "space-between",
                                        alignItems: "flex-start",
                                        gap: "12px",
                                        flexWrap: "wrap",
                                    }}
                                >
                                    <div style={{ display: "grid", gap: "6px" }}>
                                        <h2 style={sectionTitleStyle}>{section.title}</h2>
                                        {section.description ? <p style={mutedTextStyle}>{section.description}</p> : null}
                                    </div>
                                    {section.collapsible ? (
                                        <button type="button" style={secondaryButtonStyle} onClick={() => toggleSection(section.key)}>
                                            {isOpen ? "Свернуть" : "Развернуть"}
                                        </button>
                                    ) : null}
                                </div>

                                {isOpen ? (
                                    <div style={sectionGridStyle}>
                                        {section.field_keys.map(renderField)}
                                    </div>
                                ) : null}
                            </div>
                        );
                    })}
                </div>
            </div>
        </div>
    );
};

export default EnterprisePanel;
