import React, { useEffect, useState } from "react";
import { getBusinessSettingsView, updateBusinessSettingsControlPlaneScope } from "../api/businessSettingsApi";

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

const badgeBaseStyle = {
    display: "inline-block",
    padding: "5px 9px",
    borderRadius: "999px",
    fontSize: "12px",
    fontWeight: 700,
};

const sourceBadgeStyle = {
    ...badgeBaseStyle,
    backgroundColor: "#eff6ff",
    color: "#1d4ed8",
};

const readonlyBadgeStyle = {
    ...badgeBaseStyle,
    backgroundColor: "#f8fafc",
    color: "#475569",
};

const statusStyleByResolution = {
    resolved: {
        backgroundColor: "#ecfdf5",
        borderColor: "#bbf7d0",
        color: "#166534",
    },
    ambiguous: {
        backgroundColor: "#fff7ed",
        borderColor: "#fed7aa",
        color: "#9a3412",
    },
    none: {
        backgroundColor: "#fef2f2",
        borderColor: "#fecaca",
        color: "#991b1b",
    },
    "db-primary": {
        backgroundColor: "#ecfeff",
        borderColor: "#a5f3fc",
        color: "#155e75",
    },
    "db-primary-enterprise-missing": {
        backgroundColor: "#fef2f2",
        borderColor: "#fecaca",
        color: "#991b1b",
    },
};

const emptyValue = "—";
const editableSectionKeys = new Set(["target_enterprise", "master_catalog", "orders_biotus"]);
const targetEditableItemKeys = new Set([
    "business_enterprise_code",
    "master_daily_publish_enterprise_explicit",
    "master_weekly_salesdrive_enterprise_explicit",
]);
const masterEditableItemKeys = new Set([
    "master_weekly_enabled",
    "master_weekly_day",
    "master_weekly_hour",
    "master_weekly_minute",
    "master_daily_publish_enabled",
    "master_daily_publish_hour",
    "master_daily_publish_minute",
    "master_daily_publish_limit",
    "master_archive_enabled",
    "master_archive_every_minutes",
]);
const biotusEditableItemKeys = new Set([
    "biotus_enable_unhandled_fallback",
    "biotus_unhandled_order_timeout_minutes",
    "biotus_fallback_additional_status_ids",
    "biotus_duplicate_status_id",
]);
const inputStyle = {
    width: "100%",
    border: "1px solid #cbd5e1",
    borderRadius: "10px",
    padding: "10px 12px",
    fontSize: "14px",
    color: "#0f172a",
    backgroundColor: "#ffffff",
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
    backgroundColor: "#0f766e",
    color: "#ffffff",
};
const secondaryButtonStyle = {
    ...buttonBaseStyle,
    backgroundColor: "#ffffff",
    color: "#334155",
    borderColor: "#cbd5e1",
};

const sourceBadgeBySource = {
    env: {
        backgroundColor: "#eff6ff",
        color: "#1d4ed8",
    },
    db: {
        backgroundColor: "#ecfeff",
        color: "#155e75",
    },
    "db-derived": {
        backgroundColor: "#cffafe",
        color: "#0f766e",
    },
    "env-fallback": {
        backgroundColor: "#fff7ed",
        color: "#9a3412",
    },
    EnterpriseSettings: {
        backgroundColor: "#eef2ff",
        color: "#4338ca",
    },
    "EnterpriseSettings-derived": {
        backgroundColor: "#eef2ff",
        color: "#4338ca",
    },
    computed: {
        backgroundColor: "#f8fafc",
        color: "#475569",
    },
    transitional: {
        backgroundColor: "#fff7ed",
        color: "#9a3412",
    },
    "secret-hidden": {
        backgroundColor: "#fef2f2",
        color: "#b91c1c",
    },
};

const formatValue = (value) => {
    if (value === null || value === undefined || value === "") {
        return emptyValue;
    }

    if (typeof value === "boolean") {
        return value ? "Да" : "Нет";
    }

    return String(value);
};

const normalizeOptionalValue = (value) => {
    const normalized = String(value || "").trim();
    return normalized || null;
};

const normalizeCommaSeparatedStatusIds = (value) =>
    String(value || "")
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean);

const parseStatusIdsForPayload = (value) => {
    const parts = normalizeCommaSeparatedStatusIds(value);
    if (parts.length === 0) {
        throw new Error("BIOTUS_FALLBACK_ADDITIONAL_STATUS_IDS должен содержать хотя бы один status id.");
    }

    const parsed = parts.map((item) => Number(item));
    if (parsed.some((item) => !Number.isInteger(item) || item < 1)) {
        throw new Error("BIOTUS_FALLBACK_ADDITIONAL_STATUS_IDS должен содержать только положительные целые status id.");
    }
    return parsed;
};

const parseNonNegativeInteger = (value, label) => {
    const parsed = Number(value);
    if (!Number.isInteger(parsed) || parsed < 0) {
        throw new Error(`${label} должен быть неотрицательным целым числом.`);
    }
    return parsed;
};

const parsePositiveInteger = (value, label) => {
    const parsed = Number(value);
    if (!Number.isInteger(parsed) || parsed < 1) {
        throw new Error(`${label} должен быть положительным целым числом.`);
    }
    return parsed;
};

const flattenItems = (viewModel) => {
    const itemMap = new Map();
    (viewModel?.sections || []).forEach((section) => {
        (section.items || []).forEach((item) => {
            itemMap.set(item.key, item);
        });
    });
    return itemMap;
};

const buildDraftFromViewModel = (viewModel) => {
    const items = flattenItems(viewModel);
    return {
        business_enterprise_code: normalizeOptionalValue(items.get("business_enterprise_code")?.value) || "",
        daily_publish_enterprise_code_override: normalizeOptionalValue(items.get("master_daily_publish_enterprise_explicit")?.value) || "",
        weekly_salesdrive_enterprise_code_override: normalizeOptionalValue(items.get("master_weekly_salesdrive_enterprise_explicit")?.value) || "",
        master_weekly_enabled: Boolean(items.get("master_weekly_enabled")?.value),
        master_weekly_day: normalizeOptionalValue(items.get("master_weekly_day")?.value) || "SUN",
        master_weekly_hour: String(items.get("master_weekly_hour")?.value ?? "3"),
        master_weekly_minute: String(items.get("master_weekly_minute")?.value ?? "0"),
        master_daily_publish_enabled: Boolean(items.get("master_daily_publish_enabled")?.value),
        master_daily_publish_hour: String(items.get("master_daily_publish_hour")?.value ?? "9"),
        master_daily_publish_minute: String(items.get("master_daily_publish_minute")?.value ?? "0"),
        master_daily_publish_limit: String(items.get("master_daily_publish_limit")?.value ?? "0"),
        master_archive_enabled: Boolean(items.get("master_archive_enabled")?.value),
        master_archive_every_minutes: String(items.get("master_archive_every_minutes")?.value ?? "60"),
        biotus_enable_unhandled_fallback: Boolean(items.get("biotus_enable_unhandled_fallback")?.value),
        biotus_unhandled_order_timeout_minutes: String(items.get("biotus_unhandled_order_timeout_minutes")?.value ?? "60"),
        biotus_fallback_additional_status_ids: normalizeCommaSeparatedStatusIds(
            items.get("biotus_fallback_additional_status_ids")?.value ?? "9,19,18,20",
        ).join(", "),
        biotus_duplicate_status_id: String(items.get("biotus_duplicate_status_id")?.value ?? "20"),
    };
};

const buildUpdatePayload = (draft) => ({
    business_enterprise_code: normalizeOptionalValue(draft.business_enterprise_code),
    daily_publish_enterprise_code_override: normalizeOptionalValue(draft.daily_publish_enterprise_code_override),
    weekly_salesdrive_enterprise_code_override: normalizeOptionalValue(draft.weekly_salesdrive_enterprise_code_override),
    biotus_enable_unhandled_fallback: Boolean(draft.biotus_enable_unhandled_fallback),
    biotus_unhandled_order_timeout_minutes: parseNonNegativeInteger(
        draft.biotus_unhandled_order_timeout_minutes,
        "BIOTUS_UNHANDLED_ORDER_TIMEOUT_MINUTES",
    ),
    biotus_fallback_additional_status_ids: parseStatusIdsForPayload(draft.biotus_fallback_additional_status_ids),
    biotus_duplicate_status_id: parsePositiveInteger(
        draft.biotus_duplicate_status_id,
        "BIOTUS_DUPLICATE_STATUS_ID",
    ),
    master_weekly_enabled: Boolean(draft.master_weekly_enabled),
    master_weekly_day: String(draft.master_weekly_day || "SUN").toUpperCase(),
    master_weekly_hour: Number(draft.master_weekly_hour),
    master_weekly_minute: Number(draft.master_weekly_minute),
    master_daily_publish_enabled: Boolean(draft.master_daily_publish_enabled),
    master_daily_publish_hour: Number(draft.master_daily_publish_hour),
    master_daily_publish_minute: Number(draft.master_daily_publish_minute),
    master_daily_publish_limit: Number(draft.master_daily_publish_limit),
    master_archive_enabled: Boolean(draft.master_archive_enabled),
    master_archive_every_minutes: Number(draft.master_archive_every_minutes),
});

const buildItemGroups = (items = []) => {
    const groups = [];
    const map = new Map();

    items.forEach((item) => {
        const groupKey = item.group || "__default__";
        if (!map.has(groupKey)) {
            const entry = {
                key: groupKey,
                title: item.group || "",
                items: [],
            };
            map.set(groupKey, entry);
            groups.push(entry);
        }
        map.get(groupKey).items.push(item);
    });

    return groups;
};

const SectionItem = ({ item }) => (
    <div
        style={{
            border: "1px solid #e2e8f0",
            borderRadius: "10px",
            padding: "14px 16px",
            display: "grid",
            gap: "8px",
            backgroundColor: "#f8fafc",
        }}
    >
        <div style={{ display: "flex", justifyContent: "space-between", gap: "12px", alignItems: "center", flexWrap: "wrap" }}>
            <div style={{ fontWeight: 700, color: "#111827", fontSize: "14px" }}>{item.label}</div>
            <div style={{ display: "flex", gap: "8px", flexWrap: "wrap" }}>
                <span style={{ ...sourceBadgeStyle, ...(sourceBadgeBySource[item.source] || {}) }}>{item.source}</span>
                {item.readonly && <span style={readonlyBadgeStyle}>read-only</span>}
            </div>
        </div>
        <div style={{ fontSize: "15px", color: "#0f172a", lineHeight: 1.5 }}>{formatValue(item.value)}</div>
        {item.help_text && <div style={mutedTextStyle}>{item.help_text}</div>}
    </div>
);

const FormField = ({ label, helpText, children }) => (
    <div style={{ display: "grid", gap: "8px" }}>
        <div style={{ fontWeight: 700, color: "#111827", fontSize: "14px" }}>{label}</div>
        {children}
        {helpText && <div style={mutedTextStyle}>{helpText}</div>}
    </div>
);

const EditToolbar = ({ editable, editing, saving, onEdit, onCancel, onSave }) => {
    if (!editable) {
        return null;
    }

    return (
        <div style={{ display: "flex", gap: "10px", flexWrap: "wrap", marginBottom: "16px" }}>
            {!editing && (
                <button type="button" style={secondaryButtonStyle} onClick={onEdit}>
                    Редактировать
                </button>
            )}
            {editing && (
                <>
                    <button type="button" style={primaryButtonStyle} onClick={onSave} disabled={saving}>
                        {saving ? "Сохранение..." : "Сохранить"}
                    </button>
                    <button type="button" style={secondaryButtonStyle} onClick={onCancel} disabled={saving}>
                        Отмена
                    </button>
                </>
            )}
        </div>
    );
};

const TargetEnterpriseEditor = ({ draft, onChange, businessOptions, enterpriseOptions }) => (
    <div style={{ display: "grid", gap: "12px", marginBottom: "18px" }}>
        <FormField label="Основное предприятие (primary Business enterprise)">
            <select
                style={inputStyle}
                value={draft.business_enterprise_code}
                onChange={(event) => onChange("business_enterprise_code", event.target.value)}
            >
                <option value="">Выберите Business enterprise</option>
                {businessOptions.map((option) => (
                    <option key={option.enterprise_code} value={option.enterprise_code}>
                        {option.enterprise_name} ({option.enterprise_code})
                    </option>
                ))}
            </select>
        </FormField>

        <FormField
            label="Daily publish override"
            helpText="Пусто = используется основное предприятие."
        >
            <select
                style={inputStyle}
                value={draft.daily_publish_enterprise_code_override}
                onChange={(event) => onChange("daily_publish_enterprise_code_override", event.target.value)}
            >
                <option value="">Использовать основное предприятие</option>
                {enterpriseOptions.map((option) => (
                    <option key={option.enterprise_code} value={option.enterprise_code}>
                        {option.enterprise_name} ({option.enterprise_code})
                    </option>
                ))}
            </select>
        </FormField>

        <FormField
            label="Weekly SalesDrive override"
            helpText="Пусто = используется основное предприятие."
        >
            <select
                style={inputStyle}
                value={draft.weekly_salesdrive_enterprise_code_override}
                onChange={(event) => onChange("weekly_salesdrive_enterprise_code_override", event.target.value)}
            >
                <option value="">Использовать основное предприятие</option>
                {enterpriseOptions.map((option) => (
                    <option key={option.enterprise_code} value={option.enterprise_code}>
                        {option.enterprise_name} ({option.enterprise_code})
                    </option>
                ))}
            </select>
        </FormField>
    </div>
);

const MasterCatalogEditor = ({ draft, onChange }) => (
    <div style={{ display: "grid", gap: "16px", marginBottom: "18px" }}>
        <FormField label="Weekly enrichment">
            <label style={{ display: "flex", gap: "10px", alignItems: "center", color: "#0f172a" }}>
                <input
                    type="checkbox"
                    checked={draft.master_weekly_enabled}
                    onChange={(event) => onChange("master_weekly_enabled", event.target.checked)}
                />
                Включено
            </label>
        </FormField>
        <div style={{ display: "grid", gap: "12px", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))" }}>
            <FormField label="День">
                <select
                    style={inputStyle}
                    value={draft.master_weekly_day}
                    onChange={(event) => onChange("master_weekly_day", event.target.value)}
                >
                    {["MON", "TUE", "WED", "THU", "FRI", "SAT", "SUN"].map((day) => (
                        <option key={day} value={day}>
                            {day}
                        </option>
                    ))}
                </select>
            </FormField>
            <FormField label="Час">
                <input
                    type="number"
                    min="0"
                    max="23"
                    style={inputStyle}
                    value={draft.master_weekly_hour}
                    onChange={(event) => onChange("master_weekly_hour", event.target.value)}
                />
            </FormField>
            <FormField label="Минута">
                <input
                    type="number"
                    min="0"
                    max="59"
                    style={inputStyle}
                    value={draft.master_weekly_minute}
                    onChange={(event) => onChange("master_weekly_minute", event.target.value)}
                />
            </FormField>
        </div>

        <FormField label="Daily publish">
            <label style={{ display: "flex", gap: "10px", alignItems: "center", color: "#0f172a" }}>
                <input
                    type="checkbox"
                    checked={draft.master_daily_publish_enabled}
                    onChange={(event) => onChange("master_daily_publish_enabled", event.target.checked)}
                />
                Включено
            </label>
        </FormField>
        <div style={{ display: "grid", gap: "12px", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))" }}>
            <FormField label="Час">
                <input
                    type="number"
                    min="0"
                    max="23"
                    style={inputStyle}
                    value={draft.master_daily_publish_hour}
                    onChange={(event) => onChange("master_daily_publish_hour", event.target.value)}
                />
            </FormField>
            <FormField label="Минута">
                <input
                    type="number"
                    min="0"
                    max="59"
                    style={inputStyle}
                    value={draft.master_daily_publish_minute}
                    onChange={(event) => onChange("master_daily_publish_minute", event.target.value)}
                />
            </FormField>
            <FormField label="Лимит публикации">
                <input
                    type="number"
                    min="0"
                    style={inputStyle}
                    value={draft.master_daily_publish_limit}
                    onChange={(event) => onChange("master_daily_publish_limit", event.target.value)}
                />
            </FormField>
        </div>

        <FormField label="Archive import">
            <label style={{ display: "flex", gap: "10px", alignItems: "center", color: "#0f172a" }}>
                <input
                    type="checkbox"
                    checked={draft.master_archive_enabled}
                    onChange={(event) => onChange("master_archive_enabled", event.target.checked)}
                />
                Включено
            </label>
        </FormField>
        <FormField label="Интервал (минуты)">
            <input
                type="number"
                min="1"
                style={inputStyle}
                value={draft.master_archive_every_minutes}
                onChange={(event) => onChange("master_archive_every_minutes", event.target.value)}
            />
        </FormField>
    </div>
);

const BiotusPolicyEditor = ({ draft, onChange }) => (
    <div style={{ display: "grid", gap: "16px", marginBottom: "18px" }}>
        <FormField label="Unhandled fallback">
            <label style={{ display: "flex", gap: "10px", alignItems: "center", color: "#0f172a" }}>
                <input
                    type="checkbox"
                    checked={draft.biotus_enable_unhandled_fallback}
                    onChange={(event) => onChange("biotus_enable_unhandled_fallback", event.target.checked)}
                />
                Включено
            </label>
        </FormField>
        <div style={{ display: "grid", gap: "12px", gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))" }}>
            <FormField label="Timeout (минуты)">
                <input
                    type="number"
                    min="0"
                    style={inputStyle}
                    value={draft.biotus_unhandled_order_timeout_minutes}
                    onChange={(event) => onChange("biotus_unhandled_order_timeout_minutes", event.target.value)}
                />
            </FormField>
            <FormField label="Duplicate status id">
                <input
                    type="number"
                    min="1"
                    style={inputStyle}
                    value={draft.biotus_duplicate_status_id}
                    onChange={(event) => onChange("biotus_duplicate_status_id", event.target.value)}
                />
            </FormField>
        </div>
        <FormField
            label="Fallback additional status ids"
            helpText="Введите comma-separated список положительных SalesDrive status id. В DB хранится как integer array."
        >
            <input
                type="text"
                style={inputStyle}
                value={draft.biotus_fallback_additional_status_ids}
                onChange={(event) => onChange("biotus_fallback_additional_status_ids", event.target.value)}
                placeholder="9, 19, 18, 20"
            />
        </FormField>
    </div>
);

const SectionCard = ({
    section,
    editing,
    saving,
    onEdit,
    onCancel,
    onSave,
    draft,
    onDraftChange,
    businessOptions,
    enterpriseOptions,
}) => {
    const groups = buildItemGroups(section.items);
    const editable = editableSectionKeys.has(section.key);
    const editableKeys = section.key === "target_enterprise"
        ? targetEditableItemKeys
        : section.key === "master_catalog"
            ? masterEditableItemKeys
            : section.key === "orders_biotus"
                ? biotusEditableItemKeys
            : new Set();
    const visibleGroups = editing && editable
        ? groups
            .map((group) => ({
                ...group,
                items: group.items.filter((item) => !editableKeys.has(item.key)),
            }))
            .filter((group) => group.items.length > 0)
        : groups;

    return (
        <div style={{ ...cardStyle, padding: "20px 24px" }}>
            <div style={{ display: "grid", gap: "8px", marginBottom: "16px" }}>
                <h2 style={sectionTitleStyle}>{section.title}</h2>
                {section.description && <p style={mutedTextStyle}>{section.description}</p>}
            </div>
            <EditToolbar
                editable={editable}
                editing={editing}
                saving={saving}
                onEdit={onEdit}
                onCancel={onCancel}
                onSave={onSave}
            />
            {editing && section.key === "target_enterprise" && (
                <TargetEnterpriseEditor
                    draft={draft}
                    onChange={onDraftChange}
                    businessOptions={businessOptions}
                    enterpriseOptions={enterpriseOptions}
                />
            )}
            {editing && section.key === "master_catalog" && (
                <MasterCatalogEditor
                    draft={draft}
                    onChange={onDraftChange}
                />
            )}
            {editing && section.key === "orders_biotus" && (
                <BiotusPolicyEditor
                    draft={draft}
                    onChange={onDraftChange}
                />
            )}
            <div style={{ display: "grid", gap: "18px" }}>
                {visibleGroups.map((group) => (
                    <div key={group.key} style={{ display: "grid", gap: "12px" }}>
                        {group.title && (
                            <div
                                style={{
                                    fontSize: "13px",
                                    fontWeight: 700,
                                    color: "#475569",
                                    textTransform: "uppercase",
                                    letterSpacing: "0.04em",
                                }}
                            >
                                {group.title}
                            </div>
                        )}
                        <div style={{ display: "grid", gap: "12px" }}>
                            {group.items.map((item) => (
                                <SectionItem key={item.key} item={item} />
                            ))}
                        </div>
                    </div>
                ))}
            </div>
        </div>
    );
};

const BusinessSettingsPage = () => {
    const [viewModel, setViewModel] = useState(null);
    const [draft, setDraft] = useState(null);
    const [loading, setLoading] = useState(true);
    const [saving, setSaving] = useState(false);
    const [editing, setEditing] = useState(false);
    const [error, setError] = useState("");
    const [saveError, setSaveError] = useState("");
    const [saveSuccess, setSaveSuccess] = useState("");

    useEffect(() => {
        const loadView = async () => {
            setLoading(true);
            setError("");
            try {
                const data = await getBusinessSettingsView();
                setViewModel(data);
            } catch (loadError) {
                console.error("Error loading business settings view:", loadError);
                setError("Не удалось загрузить Business Settings.");
            } finally {
                setLoading(false);
            }
        };

        loadView();
    }, []);

    useEffect(() => {
        if (viewModel) {
            setDraft(buildDraftFromViewModel(viewModel));
        }
    }, [viewModel]);

    const handleDraftChange = (key, value) => {
        setDraft((current) => ({
            ...(current || {}),
            [key]: value,
        }));
    };

    const handleEdit = () => {
        setSaveError("");
        setSaveSuccess("");
        setDraft(buildDraftFromViewModel(viewModel));
        setEditing(true);
    };

    const handleCancel = () => {
        setSaveError("");
        setSaveSuccess("");
        setDraft(buildDraftFromViewModel(viewModel));
        setEditing(false);
    };

    const handleSave = async () => {
        setSaving(true);
        setSaveError("");
        setSaveSuccess("");
        try {
            const updated = await updateBusinessSettingsControlPlaneScope(buildUpdatePayload(draft));
            setViewModel(updated);
            setEditing(false);
            setSaveSuccess("Настройки Business control-plane сохранены.");
        } catch (saveRequestError) {
            console.error("Error saving business settings:", saveRequestError);
            setSaveError(saveRequestError?.response?.data?.detail || saveRequestError?.message || "Не удалось сохранить Business Settings.");
        } finally {
            setSaving(false);
        }
    };

    const resolutionStyle = statusStyleByResolution[viewModel?.resolution_status] || statusStyleByResolution.none;
    const businessOptions = (viewModel?.enterprise_options || []).filter(
        (option) => String(option.data_format || "").trim().toLowerCase() === "business",
    );
    const enterpriseOptions = viewModel?.enterprise_options || [];

    return (
        <div style={pageStyle}>
            <div style={{ ...cardStyle, padding: "20px 24px", display: "grid", gap: "10px" }}>
                <h1 style={{ margin: 0, fontSize: "28px", color: "#111827" }}>Business Settings</h1>
                <p style={mutedTextStyle}>
                    Отдельный control-plane snapshot для Business contour. DB-backed fields read from business_settings with fallback only when the DB row is missing.
                </p>
            </div>

            {loading && (
                <div style={{ ...cardStyle, padding: "20px 24px" }}>
                    <p style={mutedTextStyle}>Загрузка Business Settings...</p>
                </div>
            )}

            {!loading && error && (
                <div style={{ ...cardStyle, padding: "20px 24px", borderColor: "#fecaca", backgroundColor: "#fef2f2" }}>
                    <p style={{ margin: 0, color: "#991b1b", fontWeight: 600 }}>{error}</p>
                </div>
            )}

            {!loading && !error && viewModel && (
                <>
                    <div
                        style={{
                            ...cardStyle,
                            padding: "20px 24px",
                            backgroundColor: resolutionStyle.backgroundColor,
                            borderColor: resolutionStyle.borderColor,
                            display: "grid",
                            gap: "10px",
                        }}
                    >
                        <div style={{ display: "flex", gap: "8px", alignItems: "center", flexWrap: "wrap" }}>
                            <span style={{ ...badgeBaseStyle, backgroundColor: "#ffffff", color: resolutionStyle.color }}>
                                {viewModel.resolution_status}
                            </span>
                            {viewModel.writable_supported ? (
                                <span style={{ ...readonlyBadgeStyle, backgroundColor: "#ffffff" }}>
                                    writable control-plane scope enabled
                                </span>
                            ) : (
                                <span style={{ ...readonlyBadgeStyle, backgroundColor: "#ffffff" }}>
                                    orchestration write deferred
                                </span>
                            )}
                        </div>
                        <p style={{ ...mutedTextStyle, color: resolutionStyle.color }}>{viewModel.resolution_message}</p>
                        {viewModel.deferred_write_reason && (
                            <p style={mutedTextStyle}>{viewModel.deferred_write_reason}</p>
                        )}
                        {Array.isArray(viewModel.business_candidates) && viewModel.business_candidates.length > 1 && (
                            <div style={{ display: "grid", gap: "8px" }}>
                                <div style={{ fontWeight: 700, color: "#111827" }}>Найденные Business enterprise</div>
                                <div style={{ display: "flex", gap: "8px", flexWrap: "wrap" }}>
                                    {viewModel.business_candidates.map((candidate) => (
                                        <span key={candidate.enterprise_code} style={sourceBadgeStyle}>
                                            {candidate.enterprise_name} ({candidate.enterprise_code})
                                        </span>
                                    ))}
                                </div>
                            </div>
                        )}
                        {Array.isArray(viewModel.planned_writable_keys) && viewModel.planned_writable_keys.length > 0 && (
                            <div style={{ display: "grid", gap: "8px" }}>
                                <div style={{ fontWeight: 700, color: "#111827" }}>Planned writable scope</div>
                                <div style={{ display: "flex", gap: "8px", flexWrap: "wrap" }}>
                                    {viewModel.planned_writable_keys.map((key) => (
                                        <span key={key} style={readonlyBadgeStyle}>
                                            {key}
                                        </span>
                                    ))}
                                </div>
                            </div>
                        )}
                    </div>

                    {saveError && (
                        <div style={{ ...cardStyle, padding: "16px 20px", borderColor: "#fecaca", backgroundColor: "#fef2f2" }}>
                            <p style={{ margin: 0, color: "#991b1b", fontWeight: 600 }}>{saveError}</p>
                        </div>
                    )}

                    {saveSuccess && (
                        <div style={{ ...cardStyle, padding: "16px 20px", borderColor: "#bbf7d0", backgroundColor: "#ecfdf5" }}>
                            <p style={{ margin: 0, color: "#166534", fontWeight: 600 }}>{saveSuccess}</p>
                        </div>
                    )}

                    {viewModel.sections.map((section) => (
                        <SectionCard
                            key={section.key}
                            section={section}
                            editing={editing}
                            saving={saving}
                            onEdit={handleEdit}
                            onCancel={handleCancel}
                            onSave={handleSave}
                            draft={draft}
                            onDraftChange={handleDraftChange}
                            businessOptions={businessOptions}
                            enterpriseOptions={enterpriseOptions}
                        />
                    ))}
                </>
            )}
        </div>
    );
};

export default BusinessSettingsPage;
