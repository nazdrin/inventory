import React, { useEffect, useState } from "react";
import {
    getBusinessSettingsView,
    updateBusinessSettingsControlPlaneScope,
    updateBusinessSettingsPricingScope,
} from "../api/businessSettingsApi";

const pageStyle = {
    padding: "24px",
    display: "grid",
    gap: "20px",
    width: "100%",
    maxWidth: "1280px",
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

const formGridTwoStyle = {
    display: "grid",
    gap: "16px 20px",
    gridTemplateColumns: "repeat(2, minmax(220px, 1fr))",
    alignItems: "start",
};

const formGridThreeStyle = {
    display: "grid",
    gap: "16px 20px",
    gridTemplateColumns: "repeat(3, minmax(220px, 1fr))",
    alignItems: "start",
};

const formSectionStyle = {
    display: "grid",
    gap: "16px",
    width: "100%",
    marginBottom: "18px",
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

const emptyValue = "—";
const editableSectionKeysExtended = new Set([
    "master_catalog",
    "orders_biotus",
    "pricing",
    "stock_mapping_mode",
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
const stockEditableItemKeys = new Set([
    "business_stock_enabled",
    "business_stock_interval_seconds",
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
    boxSizing: "border-box",
};
const wideInputStyle = {
    ...inputStyle,
};
const checkboxStyle = {
    width: "18px",
    height: "18px",
    accentColor: "#2563eb",
    margin: 0,
    flex: "0 0 auto",
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

const sectionOrder = [
    "target_enterprise",
    "master_catalog",
    "stock_mapping_mode",
    "orders_biotus",
    "pricing",
];

const visibleSectionKeys = new Set([
    "target_enterprise",
    "master_catalog",
    "stock_mapping_mode",
    "orders_biotus",
    "pricing",
]);

const hiddenItemKeys = new Set([
    "master_target_fallback_note",
    "biotus_enterprise_code",
]);

const decimalDisplayKeys = new Set([
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
    "pricing_jitter_step_uah",
    "pricing_jitter_min_uah",
    "pricing_jitter_max_uah",
]);

const sectionMetaByKey = {
    target_enterprise: {
        title: "Рабочее предприятие Business",
        description: "Summary control-plane предприятия, на которое направлены business-пайплайны.",
    },
    master_catalog: {
        title: "Мастер-каталог (Master Catalog)",
        description: "Расписание и параметры публикации мастер-каталога.",
    },
    stock_mapping_mode: {
        title: "Business Stock Scheduler",
        description: "Глобальные параметры запуска business stock pipeline.",
    },
    orders_biotus: {
        title: "Дополнительная обработка заказов",
        description: "Fallback-логика и вспомогательные сценарии заказов, не относящиеся к runtime конкретного enterprise.",
    },
    pricing: {
        title: "Ценообразование (Pricing)",
        description: "Глобальные параметры расчета цен.",
    },
};

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

    if (source === "env_allowlist") {
        return {
            label: "ENV allowlist",
            style: {
                backgroundColor: "#ecfdf5",
                color: "#166534",
            },
        };
    }

    if (source === "default" || source === "computed") {
        return {
            label: "Computed",
            style: {
                backgroundColor: "#f1f5f9",
                color: "#334155",
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

    return null;
};

const normalizeDecimalDisplay = (value) => {
    const raw = String(value ?? "").trim();
    if (!raw || !/^[-+]?\d+(?:[.,]\d+)?$/.test(raw)) {
        return raw;
    }

    const normalized = raw
        .replace(",", ".")
        .replace(/(\.\d*?[1-9])0+$/, "$1")
        .replace(/\.0+$/, "")
        .replace(".", ",");

    return normalized;
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

    if (decimalDisplayKeys.has(item?.key)) {
        return normalizeDecimalDisplay(value);
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
        business_stock_interval_seconds: String(items.get("business_stock_interval_seconds")?.value ?? "1"),
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
        "BUSINESS_STOCK_INTERVAL_MINUTES",
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

const buildInitialOpenSections = () =>
    sectionOrder.reduce((acc, key) => {
        acc[key] = false;
        return acc;
    }, {});

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
    <div style={{ display: "grid", gap: "8px", minWidth: 0, alignContent: "start" }}>
        <div style={{ fontWeight: 700, color: "#111827", fontSize: "14px" }}>{label}</div>
        {children}
        {helpText && <div style={mutedTextStyle}>{helpText}</div>}
    </div>
);

const EditToolbar = ({ editable, saving, onCancel, onSave }) => {
    if (!editable) {
        return null;
    }

    return (
        <div style={{ display: "flex", gap: "10px", flexWrap: "wrap", marginBottom: "16px" }}>
            <button type="button" style={primaryButtonStyle} onClick={onSave} disabled={saving}>
                {saving ? "Сохранение..." : "Сохранить"}
            </button>
            <button type="button" style={secondaryButtonStyle} onClick={onCancel} disabled={saving}>
                Отмена
            </button>
        </div>
    );
};

const TargetEnterpriseEditor = ({ draft, onChange }) => (
    <div style={formSectionStyle}>
        <div style={mutedTextStyle}>
            Управление enterprise runtime перенесено на страницу Business-магазинов. Здесь остаётся только control-plane summary.
        </div>
    </div>
);

const MasterCatalogEditor = ({ draft, onChange }) => (
    <div style={formSectionStyle}>
        <FormField label="Еженедельное обновление">
            <label style={{ display: "flex", gap: "10px", alignItems: "center", color: "#0f172a" }}>
                <input
                    type="checkbox"
                    style={checkboxStyle}
                    checked={draft.master_weekly_enabled}
                    onChange={(event) => onChange("master_weekly_enabled", event.target.checked)}
                />
                Включено
            </label>
        </FormField>
        <div style={formGridThreeStyle}>
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
                    style={checkboxStyle}
                    checked={draft.master_daily_publish_enabled}
                    onChange={(event) => onChange("master_daily_publish_enabled", event.target.checked)}
                />
                Включено
            </label>
        </FormField>
        <div style={formGridThreeStyle}>
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
                    style={checkboxStyle}
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

const AdditionalOrdersProcessingEditor = ({ draft, onChange }) => (
    <div style={{ display: "grid", gap: "16px" }}>
        <div style={{ display: "grid", gap: "6px" }}>
            <div style={{ fontSize: "13px", fontWeight: 700, color: "#475569", letterSpacing: "0.02em" }}>
                Дополнительная обработка заказов
            </div>
            <div style={mutedTextStyle}>
                Настройки обработки заказов, которые не были обработаны основным контуром
            </div>
        </div>
        <FormField label="Включить дополнительную обработку">
            <label style={{ display: "flex", gap: "10px", alignItems: "center", color: "#0f172a" }}>
                <input
                    type="checkbox"
                    style={checkboxStyle}
                    checked={draft.biotus_enable_unhandled_fallback}
                    onChange={(event) => onChange("biotus_enable_unhandled_fallback", event.target.checked)}
                />
                Включено
            </label>
        </FormField>
        <div style={formGridTwoStyle}>
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
                style={wideInputStyle}
                value={draft.biotus_fallback_additional_status_ids}
                onChange={(event) => onChange("biotus_fallback_additional_status_ids", event.target.value)}
                placeholder="9, 19, 18, 20"
            />
        </FormField>
    </div>
);

const StockOperationalEditor = ({ draft, onChange }) => (
    <div style={formSectionStyle}>
        <div style={formGridTwoStyle}>
            <FormField
                label="Включить обработку стока"
                helpText="Включает или выключает обработку остатков."
            >
                <label style={{ display: "flex", gap: "10px", alignItems: "center", color: "#0f172a" }}>
                    <input
                        type="checkbox"
                        style={checkboxStyle}
                        checked={draft.business_stock_enabled}
                        onChange={(event) => onChange("business_stock_enabled", event.target.checked)}
                    />
                    Включено
                </label>
            </FormField>
            <FormField label="Интервал запуска, минут" helpText="Через сколько минут запускается следующий цикл обработки остатков.">
                <input
                    type="number"
                    min="1"
                    style={inputStyle}
                    value={draft.business_stock_interval_seconds}
                    onChange={(event) => onChange("business_stock_interval_seconds", event.target.value)}
                />
            </FormField>
            <div />
            <div style={mutedTextStyle}>
                Enterprise-level stock routing и runtime mode настраиваются на странице Business-магазинов.
            </div>
        </div>
    </div>
);

const OrdersSectionEditor = ({ draft, onChange }) => (
    <div style={formSectionStyle}>
        <AdditionalOrdersProcessingEditor draft={draft} onChange={onChange} />
    </div>
);

const pricingLayoutStyle = {
    display: "grid",
    gap: "16px",
    width: "100%",
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
                <div style={formGridTwoStyle}>
                    {renderDecimalInput("pricing_price_band_low_max", "Верхняя граница LOW", "Если price_opt меньше или равен этому значению, товар попадает в LOW.", { min: "0" })}
                    {renderDecimalInput("pricing_price_band_mid_max", "Верхняя граница MID", "Если price_opt выше LOW, но не выше этого значения, товар попадает в MID. Всё, что выше, идёт в HIGH.", { min: "0" })}
                </div>
            </div>

            <div style={{ display: "grid", gap: "12px" }}>
                <div style={{ fontSize: "13px", fontWeight: 700, color: "#475569", letterSpacing: "0.02em" }}>Реакция на конкурентов</div>
                <div style={formGridThreeStyle}>
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
                <div style={formGridThreeStyle}>
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
                            style={checkboxStyle}
                            checked={draft.pricing_jitter_enabled}
                            onChange={(event) => onChange("pricing_jitter_enabled", event.target.checked)}
                        />
                        Включено
                    </label>
                </FormField>
                <div style={formGridThreeStyle}>
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
    isOpen,
    saving,
    onToggle,
    onCancel,
    onSave,
    draft,
    onDraftChange,
}) => {
    const sectionMeta = sectionMetaByKey[section.key] || {};
    const groups = buildItemGroups(section.items.filter((item) => !hiddenItemKeys.has(item.key)));
    const editable = editableSectionKeysExtended.has(section.key);
    const editableKeys = section.key === "master_catalog"
            ? masterEditableItemKeys
            : section.key === "orders_biotus"
                ? biotusEditableItemKeys
                : section.key === "stock_mapping_mode"
                    ? stockEditableItemKeys
                    : section.key === "pricing"
                        ? pricingEditableItemKeys
            : new Set();
    const visibleGroups = isOpen && editable
        ? groups
            .map((group) => ({
                ...group,
                items: group.items.filter((item) => !editableKeys.has(item.key)),
            }))
            .filter((group) => group.items.length > 0)
        : groups;

    return (
        <div style={{ ...cardStyle, padding: "20px 24px" }}>
            <div
                style={{
                    display: "flex",
                    justifyContent: "space-between",
                    alignItems: "flex-start",
                    gap: "12px",
                    flexWrap: "wrap",
                }}
            >
                <div style={{ display: "grid", gap: "8px" }}>
                    <h2 style={sectionTitleStyle}>{sectionMeta.title || section.title}</h2>
                    {(sectionMeta.description || section.description) && (
                        <p style={mutedTextStyle}>{sectionMeta.description || section.description}</p>
                    )}
                </div>
                <button type="button" style={secondaryButtonStyle} onClick={() => onToggle(section.key)} disabled={saving}>
                    {isOpen ? "Свернуть" : "Развернуть"}
                </button>
            </div>
            {isOpen && (
                <div style={{ display: "grid", gap: "18px", marginTop: "16px" }}>
                    <EditToolbar
                        editable={editable}
                        saving={saving}
                        onCancel={onCancel}
                        onSave={() => onSave(section.key)}
                    />
                    {section.key === "target_enterprise" && (
                        <TargetEnterpriseEditor
                            draft={draft}
                            onChange={onDraftChange}
                        />
                    )}
                    {section.key === "master_catalog" && (
                        <MasterCatalogEditor
                            draft={draft}
                            onChange={onDraftChange}
                        />
                    )}
                    {section.key === "orders_biotus" && (
                        <OrdersSectionEditor
                            draft={draft}
                            onChange={onDraftChange}
                        />
                    )}
                    {section.key === "stock_mapping_mode" && (
                        <StockOperationalEditor
                            draft={draft}
                            onChange={onDraftChange}
                        />
                    )}
                    {section.key === "pricing" && (
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
                                <div
                                    style={{
                                        display: "grid",
                                        gap: "12px",
                                        gridTemplateColumns: "repeat(auto-fit, minmax(220px, 280px))",
                                        alignItems: "start",
                                        justifyContent: "center",
                                    }}
                                >
                                    {group.items.map((item) => (
                                        <SectionItem key={item.key} item={item} />
                                    ))}
                                </div>
                            </div>
                        ))}
                    </div>
                </div>
            )}
        </div>
    );
};

const BusinessSettingsPage = () => {
    const [viewModel, setViewModel] = useState(null);
    const [draft, setDraft] = useState(null);
    const [loading, setLoading] = useState(true);
    const [savingSectionKey, setSavingSectionKey] = useState("");
    const [error, setError] = useState("");
    const [saveError, setSaveError] = useState("");
    const [saveSuccess, setSaveSuccess] = useState("");
    const [openSections, setOpenSections] = useState(buildInitialOpenSections);

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

    useEffect(() => {
        if (viewModel?.sections) {
            setOpenSections((current) => {
                const next = { ...buildInitialOpenSections(), ...current };
                viewModel.sections.forEach((section) => {
                    if (!(section.key in next)) {
                        next[section.key] = false;
                    }
                });
                return next;
            });
        }
    }, [viewModel]);

    const handleDraftChange = (key, value) => {
        setDraft((current) => ({
            ...(current || {}),
            [key]: value,
        }));
    };

    const handleCancel = () => {
        setSaveError("");
        setSaveSuccess("");
        setDraft(buildDraftFromViewModel(viewModel));
    };

    const handleSave = async (sectionKey) => {
        setSavingSectionKey(sectionKey);
        setSaveError("");
        setSaveSuccess("");
        try {
            let updated;
            if (sectionKey === "master_catalog" || sectionKey === "stock_mapping_mode" || sectionKey === "orders_biotus") {
                updated = await updateBusinessSettingsControlPlaneScope(buildUpdatePayload(draft));
            } else if (sectionKey === "pricing") {
                updated = await updateBusinessSettingsPricingScope(buildPricingUpdatePayload(draft));
            } else {
                throw new Error("Неподдерживаемая секция сохранения.");
            }

            setViewModel(updated);
            setSaveSuccess("Настройки сохранены.");
        } catch (saveRequestError) {
            console.error("Error saving business settings:", saveRequestError);
            setSaveError(saveRequestError?.response?.data?.detail || saveRequestError?.message || "Не удалось сохранить Business Settings.");
        } finally {
            setSavingSectionKey("");
        }
    };

    const handleToggleSection = (sectionKey) => {
        setOpenSections((current) => ({
            ...current,
            [sectionKey]: !current[sectionKey],
        }));
        setSaveError("");
        setSaveSuccess("");
        setDraft(buildDraftFromViewModel(viewModel));
    };

    return (
        <div style={pageStyle}>
            <div style={{ ...cardStyle, padding: "20px 24px", display: "grid", gap: "10px" }}>
                <h1 style={{ margin: 0, fontSize: "28px", color: "#111827" }}>Business Settings</h1>
                <p style={mutedTextStyle}>
                    Control-plane страница для scheduler, pricing и общих business pipeline настроек.
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

                    {sortSections(viewModel.sections)
                        .filter((section) => visibleSectionKeys.has(section.key))
                        .map((section) => (
                        <SectionCard
                            key={section.key}
                            section={section}
                            isOpen={Boolean(openSections[section.key])}
                            saving={savingSectionKey === section.key}
                            onToggle={handleToggleSection}
                            onCancel={handleCancel}
                            onSave={handleSave}
                            draft={draft}
                            onDraftChange={handleDraftChange}
                        />
                    ))}
                </>
            )}
        </div>
    );
};

export default BusinessSettingsPage;
