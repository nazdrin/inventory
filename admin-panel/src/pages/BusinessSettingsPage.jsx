import React, { useEffect, useState } from "react";
import {
    getBusinessSettingsView,
    updateBusinessSettingsControlPlaneScope,
    updateBusinessSettingsEnterpriseOperationalScope,
    updateBusinessSettingsPricingScope,
} from "../api/businessSettingsApi";

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
    padding: "4px 8px",
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

const resolutionLabelByStatus = {
    resolved: "Предприятие найдено",
    ambiguous: "Найдено несколько предприятий",
    none: "Предприятие не найдено",
    "db-primary": "Используется БД",
    "db-primary-enterprise-missing": "Предприятие из БД не найдено",
};

const emptyValue = "—";
const editableSectionKeysExtended = new Set([
    "target_enterprise",
    "master_catalog",
    "integration_access",
    "orders_biotus",
    "pricing",
    "stock_mapping_mode",
]);
const targetEditableItemKeys = new Set(["branch_id"]);
const integrationEditableItemKeys = new Set([
    "tabletki_login",
    "tabletki_password",
    "token_masked",
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
    "order_fetcher",
    "auto_confirm",
    "biotus_enable_unhandled_fallback",
    "biotus_unhandled_order_timeout_minutes",
    "biotus_fallback_additional_status_ids",
    "biotus_duplicate_status_id",
]);
const stockEditableItemKeys = new Set([
    "business_stock_enabled",
    "business_stock_interval_seconds",
    "stock_correction",
]);
const pricingEditableItemKeys = new Set([
    "pricing_base_thr",
    "pricing_price_band_low_max",
    "pricing_price_band_mid_max",
    "pricing_thr_add_low_uah",
    "pricing_thr_add_mid_uah",
    "pricing_thr_add_high_uah",
    "pricing_no_comp_add_low_uah",
    "pricing_no_comp_add_mid_uah",
    "pricing_no_comp_add_high_uah",
    "pricing_comp_discount_share",
    "pricing_comp_delta_min_uah",
    "pricing_comp_delta_max_uah",
    "pricing_jitter_enabled",
    "pricing_jitter_step_uah",
    "pricing_jitter_min_uah",
    "pricing_jitter_max_uah",
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

const sectionOrder = [
    "target_enterprise",
    "master_catalog",
    "stock_mapping_mode",
    "orders_biotus",
    "pricing",
    "integration_access",
];

const getSourceBadge = (source) => {
    if (source === "db") {
        return {
            label: "DB",
            style: {
                backgroundColor: "#ecfeff",
                color: "#155e75",
            },
        };
    }

    if (source === "env" || source === "env-fallback") {
        return {
            label: "ENV",
            style: {
                backgroundColor: "#fff7ed",
                color: "#9a3412",
            },
        };
    }

    if (source === "EnterpriseSettings") {
        return {
            label: "Enterprise",
            style: {
                backgroundColor: "#eef2ff",
                color: "#4338ca",
            },
        };
    }

    if (source === "derived" || source === "computed") {
        return {
            label: "derived",
            style: {
                backgroundColor: "#f8fafc",
                color: "#475569",
            },
        };
    }

    return null;
};

const formatValue = (item) => {
    const value = item?.value;
    if (value === null || value === undefined || value === "") {
        return emptyValue;
    }

    if (item?.key === "tabletki_password") {
        return "••••••••";
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

const parseDecimalString = (value, label, { min = null, exclusiveMin = null, max = null, exclusiveMax = null } = {}) => {
    const normalized = String(value ?? "").trim();
    if (!normalized) {
        throw new Error(`${label} обязателен.`);
    }

    const parsed = Number(normalized);
    if (!Number.isFinite(parsed)) {
        throw new Error(`${label} должен быть числом.`);
    }
    if (min !== null && parsed < min) {
        throw new Error(`${label} должен быть не меньше ${min}.`);
    }
    if (exclusiveMin !== null && parsed <= exclusiveMin) {
        throw new Error(`${label} должен быть больше ${exclusiveMin}.`);
    }
    if (max !== null && parsed > max) {
        throw new Error(`${label} должен быть не больше ${max}.`);
    }
    if (exclusiveMax !== null && parsed >= exclusiveMax) {
        throw new Error(`${label} должен быть меньше ${exclusiveMax}.`);
    }
    return normalized;
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
        business_enterprise_code: normalizeOptionalValue(items.get("business_enterprise_code")?.value)
            || normalizeOptionalValue(viewModel?.resolved_enterprise_code)
            || "",
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
        business_stock_enabled: Boolean(items.get("business_stock_enabled")?.value ?? true),
        business_stock_interval_seconds: String(items.get("business_stock_interval_seconds")?.value ?? "60"),
        branch_id: String(items.get("branch_id")?.value ?? ""),
        tabletki_login: String(items.get("tabletki_login")?.value ?? ""),
        tabletki_password: String(items.get("tabletki_password")?.value ?? ""),
        token: "",
        order_fetcher: Boolean(items.get("order_fetcher")?.value),
        auto_confirm: Boolean(items.get("auto_confirm")?.value),
        stock_correction: Boolean(items.get("stock_correction")?.value),
        pricing_base_thr: String(items.get("pricing_base_thr")?.value ?? "0.08"),
        pricing_price_band_low_max: String(items.get("pricing_price_band_low_max")?.value ?? "100"),
        pricing_price_band_mid_max: String(items.get("pricing_price_band_mid_max")?.value ?? "400"),
        pricing_thr_add_low_uah: String(items.get("pricing_thr_add_low_uah")?.value ?? "1.0"),
        pricing_thr_add_mid_uah: String(items.get("pricing_thr_add_mid_uah")?.value ?? "1.0"),
        pricing_thr_add_high_uah: String(items.get("pricing_thr_add_high_uah")?.value ?? "1.0"),
        pricing_no_comp_add_low_uah: String(items.get("pricing_no_comp_add_low_uah")?.value ?? "1.0"),
        pricing_no_comp_add_mid_uah: String(items.get("pricing_no_comp_add_mid_uah")?.value ?? "1.0"),
        pricing_no_comp_add_high_uah: String(items.get("pricing_no_comp_add_high_uah")?.value ?? "1.0"),
        pricing_comp_discount_share: String(items.get("pricing_comp_discount_share")?.value ?? "0.01"),
        pricing_comp_delta_min_uah: String(items.get("pricing_comp_delta_min_uah")?.value ?? "2"),
        pricing_comp_delta_max_uah: String(items.get("pricing_comp_delta_max_uah")?.value ?? "15"),
        pricing_jitter_enabled: Boolean(items.get("pricing_jitter_enabled")?.value),
        pricing_jitter_step_uah: String(items.get("pricing_jitter_step_uah")?.value ?? "0.5"),
        pricing_jitter_min_uah: String(items.get("pricing_jitter_min_uah")?.value ?? "-1.0"),
        pricing_jitter_max_uah: String(items.get("pricing_jitter_max_uah")?.value ?? "1.0"),
    };
};

const buildUpdatePayload = (draft) => ({
    business_enterprise_code: normalizeOptionalValue(draft.business_enterprise_code),
    daily_publish_enterprise_code_override: normalizeOptionalValue(draft.daily_publish_enterprise_code_override),
    weekly_salesdrive_enterprise_code_override: normalizeOptionalValue(draft.weekly_salesdrive_enterprise_code_override),
    business_stock_enabled: Boolean(draft.business_stock_enabled),
    business_stock_interval_seconds: parsePositiveInteger(
        draft.business_stock_interval_seconds,
        "BUSINESS_STOCK_INTERVAL_SECONDS",
    ),
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

const buildEnterpriseOperationalUpdatePayload = (draft) => {
    const branchId = String(draft.branch_id || "").trim();
    if (!branchId) {
        throw new Error("BRANCH_ID обязателен.");
    }

    return {
        branch_id: branchId,
        tabletki_login: normalizeOptionalValue(draft.tabletki_login),
        tabletki_password: normalizeOptionalValue(draft.tabletki_password),
        order_fetcher: Boolean(draft.order_fetcher),
        auto_confirm: Boolean(draft.auto_confirm),
        stock_correction: Boolean(draft.stock_correction),
        ...(normalizeOptionalValue(draft.token) ? { token: normalizeOptionalValue(draft.token) } : {}),
    };
};

const buildPricingUpdatePayload = (draft) => {
    const payload = {
        pricing_base_thr: parseDecimalString(draft.pricing_base_thr, "Базовый порог", { min: 0 }),
        pricing_price_band_low_max: parseDecimalString(draft.pricing_price_band_low_max, "Верхняя граница LOW", { min: 0 }),
        pricing_price_band_mid_max: parseDecimalString(draft.pricing_price_band_mid_max, "Верхняя граница MID", { min: 0 }),
        pricing_thr_add_low_uah: parseDecimalString(draft.pricing_thr_add_low_uah, "Надбавка LOW", { min: 0 }),
        pricing_thr_add_mid_uah: parseDecimalString(draft.pricing_thr_add_mid_uah, "Надбавка MID", { min: 0 }),
        pricing_thr_add_high_uah: parseDecimalString(draft.pricing_thr_add_high_uah, "Надбавка HIGH", { min: 0 }),
        pricing_no_comp_add_low_uah: parseDecimalString(draft.pricing_no_comp_add_low_uah, "Надбавка LOW без конкурента", { min: 0 }),
        pricing_no_comp_add_mid_uah: parseDecimalString(draft.pricing_no_comp_add_mid_uah, "Надбавка MID без конкурента", { min: 0 }),
        pricing_no_comp_add_high_uah: parseDecimalString(draft.pricing_no_comp_add_high_uah, "Надбавка HIGH без конкурента", { min: 0 }),
        pricing_comp_discount_share: parseDecimalString(draft.pricing_comp_discount_share, "Доля скидки относительно конкурента", { min: 0, exclusiveMax: 1 }),
        pricing_comp_delta_min_uah: parseDecimalString(draft.pricing_comp_delta_min_uah, "Минимальный delta", { min: 0 }),
        pricing_comp_delta_max_uah: parseDecimalString(draft.pricing_comp_delta_max_uah, "Максимальный delta", { min: 0 }),
        pricing_jitter_enabled: Boolean(draft.pricing_jitter_enabled),
        pricing_jitter_step_uah: parseDecimalString(draft.pricing_jitter_step_uah, "Шаг jitter", { exclusiveMin: 0 }),
        pricing_jitter_min_uah: parseDecimalString(draft.pricing_jitter_min_uah, "Минимальный jitter"),
        pricing_jitter_max_uah: parseDecimalString(draft.pricing_jitter_max_uah, "Максимальный jitter"),
    };

    if (Number(payload.pricing_price_band_mid_max) < Number(payload.pricing_price_band_low_max)) {
        throw new Error("Верхняя граница MID должна быть не меньше верхней границы LOW.");
    }
    if (Number(payload.pricing_comp_delta_max_uah) < Number(payload.pricing_comp_delta_min_uah)) {
        throw new Error("Максимальный delta должен быть не меньше минимального delta.");
    }
    if (Number(payload.pricing_jitter_max_uah) < Number(payload.pricing_jitter_min_uah)) {
        throw new Error("Максимальный jitter должен быть не меньше минимального jitter.");
    }

    return payload;
};
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

const sortSections = (sections = []) => {
    const rank = new Map(sectionOrder.map((key, index) => [key, index]));
    return [...sections].sort((left, right) => {
        const leftRank = rank.get(left.key) ?? 999;
        const rightRank = rank.get(right.key) ?? 999;
        return leftRank - rightRank;
    });
};

const SectionItem = ({ item }) => {
    const badge = getSourceBadge(item.source);

    return (
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
                    {badge && <span style={{ ...sourceBadgeStyle, ...badge.style }}>{badge.label}</span>}
                </div>
            </div>
            <div style={{ fontSize: "15px", color: "#0f172a", lineHeight: 1.5 }}>{formatValue(item)}</div>
            {item.help_text && <div style={mutedTextStyle}>{item.help_text}</div>}
        </div>
    );
};

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

const TargetEnterpriseEditor = ({ draft, onChange }) => (
    <div style={{ display: "grid", gap: "12px", marginBottom: "18px" }}>
        <FormField label="Branch ID">
            <input
                type="text"
                style={inputStyle}
                value={draft.branch_id}
                onChange={(event) => onChange("branch_id", event.target.value)}
            />
        </FormField>
    </div>
);

const MasterCatalogEditor = ({ draft, onChange }) => (
    <div style={{ display: "grid", gap: "16px", marginBottom: "18px" }}>
        <FormField label="Еженедельное обновление">
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

        <FormField label="Ежедневная выгрузка">
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

        <FormField label="Загрузка архива">
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
        <FormField label="Дополнительная обработка заказов">
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
            <FormField label="Ожидание, минут">
                <input
                    type="number"
                    min="0"
                    style={inputStyle}
                    value={draft.biotus_unhandled_order_timeout_minutes}
                    onChange={(event) => onChange("biotus_unhandled_order_timeout_minutes", event.target.value)}
                />
            </FormField>
            <FormField label="Статус для дублей">
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
            label="Дополнительные статусы SalesDrive"
            helpText="Введите status id через запятую. Используются только положительные целые значения."
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

const IntegrationAccessEditor = ({ draft, onChange }) => (
    <div style={{ display: "grid", gap: "12px", marginBottom: "18px" }}>
        <FormField label="Логин Tabletki">
            <input
                type="text"
                style={inputStyle}
                value={draft.tabletki_login}
                onChange={(event) => onChange("tabletki_login", event.target.value)}
            />
        </FormField>
        <FormField label="Пароль Tabletki">
            <input
                type="password"
                style={inputStyle}
                value={draft.tabletki_password}
                onChange={(event) => onChange("tabletki_password", event.target.value)}
            />
        </FormField>
        <FormField
            label="SalesDrive API key"
            helpText="Оставьте поле пустым, чтобы не менять текущий токен"
        >
            <input
                type="password"
                style={inputStyle}
                value={draft.token}
                onChange={(event) => onChange("token", event.target.value)}
                placeholder="Введите новый токен"
            />
        </FormField>
    </div>
);

const StockOperationalEditor = ({ draft, onChange }) => (
    <div style={{ display: "grid", gap: "16px", marginBottom: "18px" }}>
        <FormField
            label="Включить обработку стока"
            helpText="Включает или выключает отдельный Business stock scheduler."
        >
            <label style={{ display: "flex", gap: "10px", alignItems: "center", color: "#0f172a" }}>
                <input
                    type="checkbox"
                    checked={draft.business_stock_enabled}
                    onChange={(event) => onChange("business_stock_enabled", event.target.checked)}
                />
                Включено
            </label>
        </FormField>
        <FormField
            label="Интервал запуска, сек"
            helpText="Через сколько секунд отдельный Business stock scheduler запускает следующий цикл."
        >
            <input
                type="number"
                min="1"
                style={inputStyle}
                value={draft.business_stock_interval_seconds}
                onChange={(event) => onChange("business_stock_interval_seconds", event.target.value)}
            />
        </FormField>
        <FormField label="Коррекция остатков">
            <label style={{ display: "flex", gap: "10px", alignItems: "center", color: "#0f172a" }}>
                <input
                    type="checkbox"
                    checked={draft.stock_correction}
                    onChange={(event) => onChange("stock_correction", event.target.checked)}
                />
                Включено
            </label>
        </FormField>
    </div>
);

const OrdersBusinessEditor = ({ draft, onChange }) => (
    <div style={{ display: "grid", gap: "16px", marginBottom: "18px" }}>
        <FormField label="Получение заказов">
            <label style={{ display: "flex", gap: "10px", alignItems: "center", color: "#0f172a" }}>
                <input
                    type="checkbox"
                    checked={draft.order_fetcher}
                    onChange={(event) => onChange("order_fetcher", event.target.checked)}
                />
                Включено
            </label>
        </FormField>
        <FormField label="Автоматическое бронирование">
            <label style={{ display: "flex", gap: "10px", alignItems: "center", color: "#0f172a" }}>
                <input
                    type="checkbox"
                    checked={draft.auto_confirm}
                    onChange={(event) => onChange("auto_confirm", event.target.checked)}
                />
                Включено
            </label>
        </FormField>
        <BiotusPolicyEditor draft={draft} onChange={onChange} />
    </div>
);

const pricingLayoutStyle = {
    display: "grid",
    gap: "16px",
    marginBottom: "18px",
};

const PricingEditor = ({ draft, onChange }) => {
    const renderDecimalInput = (key, label, helpText, extra = {}) => (
        <FormField label={label} helpText={helpText}>
            <input
                type="number"
                step={extra.step || "0.01"}
                min={extra.min}
                style={inputStyle}
                value={draft[key]}
                onChange={(event) => onChange(key, event.target.value)}
            />
        </FormField>
    );

    return (
        <div style={pricingLayoutStyle}>
            <div style={{ display: "grid", gap: "12px" }}>
                <div style={{ fontSize: "13px", fontWeight: 700, color: "#475569", letterSpacing: "0.02em" }}>Базовый порог</div>
                {renderDecimalInput("pricing_base_thr", "Базовый порог", "Доля, которая участвует в threshold calculation. 0.08 = 8%.", { step: "0.000001", min: "0" })}
            </div>

            <div style={{ display: "grid", gap: "12px" }}>
                <div style={{ fontSize: "13px", fontWeight: 700, color: "#475569", letterSpacing: "0.02em" }}>Диапазоны цен</div>
                <div style={{ display: "grid", gap: "12px", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))" }}>
                    {renderDecimalInput("pricing_price_band_low_max", "Верхняя граница LOW", "Если price_opt меньше или равен этому значению, товар попадает в LOW.", { min: "0" })}
                    {renderDecimalInput("pricing_price_band_mid_max", "Верхняя граница MID", "Если price_opt выше LOW, но не выше этого значения, товар попадает в MID. Всё, что выше, идёт в HIGH.", { min: "0" })}
                </div>
            </div>

            <div style={{ display: "grid", gap: "12px" }}>
                <div style={{ fontSize: "13px", fontWeight: 700, color: "#475569", letterSpacing: "0.02em" }}>Реакция на конкурентов</div>
                <div style={{ display: "grid", gap: "12px", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))" }}>
                    {renderDecimalInput("pricing_thr_add_low_uah", "Надбавка LOW, грн", "Абсолютная надбавка в гривне для LOW, когда есть конкурент.", { min: "0" })}
                    {renderDecimalInput("pricing_thr_add_mid_uah", "Надбавка MID, грн", "Абсолютная надбавка в гривне для MID, когда есть конкурент.", { min: "0" })}
                    {renderDecimalInput("pricing_thr_add_high_uah", "Надбавка HIGH, грн", "Абсолютная надбавка в гривне для HIGH, когда есть конкурент.", { min: "0" })}
                    {renderDecimalInput("pricing_comp_discount_share", "Доля скидки относительно конкурента", "Share, а не проценты: 0.01 = 1%. Используется для undercut логики.", { step: "0.000001", min: "0" })}
                    {renderDecimalInput("pricing_comp_delta_min_uah", "Минимальный delta, грн", "Минимальный допустимый отступ от цены конкурента в гривне.", { min: "0" })}
                    {renderDecimalInput("pricing_comp_delta_max_uah", "Максимальный delta, грн", "Максимальный допустимый отступ от цены конкурента в гривне.", { min: "0" })}
                </div>
            </div>

            <div style={{ display: "grid", gap: "12px" }}>
                <div style={{ fontSize: "13px", fontWeight: 700, color: "#475569", letterSpacing: "0.02em" }}>Поведение без конкурентов</div>
                <div style={{ display: "grid", gap: "12px", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))" }}>
                    {renderDecimalInput("pricing_no_comp_add_low_uah", "Надбавка LOW без конкурента, грн", "Абсолютная надбавка в гривне для LOW, когда цены конкурента нет.", { min: "0" })}
                    {renderDecimalInput("pricing_no_comp_add_mid_uah", "Надбавка MID без конкурента, грн", "Абсолютная надбавка в гривне для MID, когда цены конкурента нет.", { min: "0" })}
                    {renderDecimalInput("pricing_no_comp_add_high_uah", "Надбавка HIGH без конкурента, грн", "Абсолютная надбавка в гривне для HIGH, когда цены конкурента нет.", { min: "0" })}
                </div>
            </div>

            <div style={{ display: "grid", gap: "12px" }}>
                <div style={{ fontSize: "13px", fontWeight: 700, color: "#475569", letterSpacing: "0.02em" }}>Jitter</div>
                <FormField label="Включить jitter" helpText="Добавляет случайное смещение после основного расчёта цены. Runtime формулы при этом не меняются.">
                    <label style={{ display: "flex", gap: "10px", alignItems: "center", color: "#0f172a" }}>
                        <input
                            type="checkbox"
                            checked={draft.pricing_jitter_enabled}
                            onChange={(event) => onChange("pricing_jitter_enabled", event.target.checked)}
                        />
                        Включено
                    </label>
                </FormField>
                <div style={{ display: "grid", gap: "12px", gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))" }}>
                    {renderDecimalInput("pricing_jitter_step_uah", "Шаг jitter, грн", "Шаг сетки, по которой выбирается случайное смещение цены.", { min: "0.01" })}
                    {renderDecimalInput("pricing_jitter_min_uah", "Минимальный jitter, грн", "Нижняя граница случайного смещения цены.", {})}
                    {renderDecimalInput("pricing_jitter_max_uah", "Максимальный jitter, грн", "Верхняя граница случайного смещения цены.", {})}
                </div>
            </div>
        </div>
    );
};
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
    const editable = editableSectionKeysExtended.has(section.key);
    const editableKeys = section.key === "target_enterprise"
        ? targetEditableItemKeys
        : section.key === "integration_access"
            ? integrationEditableItemKeys
        : section.key === "master_catalog"
            ? masterEditableItemKeys
            : section.key === "orders_biotus"
                ? biotusEditableItemKeys
                : section.key === "stock_mapping_mode"
                    ? stockEditableItemKeys
                    : section.key === "pricing"
                        ? pricingEditableItemKeys
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
                onEdit={() => onEdit(section.key)}
                onCancel={onCancel}
                onSave={() => onSave(section.key)}
            />
            {editing && section.key === "target_enterprise" && (
                <TargetEnterpriseEditor
                    draft={draft}
                    onChange={onDraftChange}
                />
            )}
            {editing && section.key === "integration_access" && (
                <IntegrationAccessEditor
                    draft={draft}
                    onChange={onDraftChange}
                />
            )}
            {editing && section.key === "master_catalog" && (
                <MasterCatalogEditor
                    draft={draft}
                    onChange={onDraftChange}
                />
            )}
            {editing && section.key === "orders_biotus" && (
                <OrdersBusinessEditor
                    draft={draft}
                    onChange={onDraftChange}
                />
            )}
            {editing && section.key === "stock_mapping_mode" && (
                <StockOperationalEditor
                    draft={draft}
                    onChange={onDraftChange}
                />
            )}
            {editing && section.key === "pricing" && (
                <PricingEditor
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
                                    letterSpacing: "0.02em",
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
    const [savingSectionKey, setSavingSectionKey] = useState("");
    const [editingSectionKey, setEditingSectionKey] = useState("");
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

    const handleEdit = (sectionKey) => {
        setSaveError("");
        setSaveSuccess("");
        setDraft(buildDraftFromViewModel(viewModel));
        setEditingSectionKey(sectionKey);
    };

    const handleCancel = () => {
        setSaveError("");
        setSaveSuccess("");
        setDraft(buildDraftFromViewModel(viewModel));
        setEditingSectionKey("");
    };

    const handleSave = async (sectionKey) => {
        setSavingSectionKey(sectionKey);
        setSaveError("");
        setSaveSuccess("");
        try {
            let updated;
            if (sectionKey === "target_enterprise" || sectionKey === "integration_access" || sectionKey === "stock_mapping_mode") {
                if (sectionKey === "stock_mapping_mode") {
                    const operationalPayload = buildEnterpriseOperationalUpdatePayload(draft);
                    console.log("Business enterprise operational payload:", operationalPayload);
                    await updateBusinessSettingsEnterpriseOperationalScope(operationalPayload);
                    updated = await updateBusinessSettingsControlPlaneScope(buildUpdatePayload(draft));
                } else {
                    const payload = buildEnterpriseOperationalUpdatePayload(draft);
                    console.log("Business enterprise operational payload:", payload);
                    updated = await updateBusinessSettingsEnterpriseOperationalScope(payload);
                }
            } else if (sectionKey === "master_catalog") {
                updated = await updateBusinessSettingsControlPlaneScope(buildUpdatePayload(draft));
            } else if (sectionKey === "orders_biotus") {
                const operationalPayload = buildEnterpriseOperationalUpdatePayload(draft);
                console.log("Business enterprise operational payload:", operationalPayload);
                await updateBusinessSettingsEnterpriseOperationalScope(operationalPayload);
                updated = await updateBusinessSettingsControlPlaneScope(buildUpdatePayload(draft));
            } else if (sectionKey === "pricing") {
                updated = await updateBusinessSettingsPricingScope(buildPricingUpdatePayload(draft));
            } else {
                throw new Error("Неподдерживаемая секция сохранения.");
            }

            setViewModel(updated);
            setEditingSectionKey("");
            setSaveSuccess("Настройки сохранены.");
        } catch (saveRequestError) {
            console.error("Error saving business settings:", saveRequestError);
            setSaveError(saveRequestError?.response?.data?.detail || saveRequestError?.message || "Не удалось сохранить Business Settings.");
        } finally {
            setSavingSectionKey("");
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
                    Единая операторская страница для Business control-plane. Настройки из `business_settings` используются в первую очередь, а ENV нужен только как fallback там, где это ещё не переведено в БД.
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
                                {resolutionLabelByStatus[viewModel.resolution_status] || viewModel.resolution_status}
                            </span>
                            {viewModel.writable_supported ? (
                                <span style={{ ...readonlyBadgeStyle, backgroundColor: "#ffffff" }}>
                                    доступно редактирование
                                </span>
                            ) : (
                                <span style={{ ...readonlyBadgeStyle, backgroundColor: "#ffffff" }}>
                                    только чтение
                                </span>
                            )}
                        </div>
                        <p style={{ ...mutedTextStyle, color: resolutionStyle.color }}>{viewModel.resolution_message}</p>
                        {viewModel.deferred_write_reason && (
                            <p style={mutedTextStyle}>{viewModel.deferred_write_reason}</p>
                        )}
                        {Array.isArray(viewModel.business_candidates) && viewModel.business_candidates.length > 1 && (
                            <div style={{ display: "grid", gap: "8px" }}>
                                <div style={{ fontWeight: 700, color: "#111827" }}>Найденные предприятия Business</div>
                                <div style={{ display: "flex", gap: "8px", flexWrap: "wrap" }}>
                                    {viewModel.business_candidates.map((candidate) => (
                                        <span key={candidate.enterprise_code} style={sourceBadgeStyle}>
                                            {candidate.enterprise_name} ({candidate.enterprise_code})
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

                    {sortSections(viewModel.sections).map((section) => (
                        <SectionCard
                            key={section.key}
                            section={section}
                            editing={editingSectionKey === section.key}
                            saving={savingSectionKey === section.key}
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
