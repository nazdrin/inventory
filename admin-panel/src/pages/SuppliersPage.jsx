import React, { useEffect, useMemo, useState } from "react";
import {
    createSupplier,
    getBusinessStores,
    getBusinessStoreSupplierSettings,
    getBusinessSupplierStoreSettingsOverview,
    getSupplierViewDetail,
    getSuppliersViewList,
    upsertBusinessStoreSupplierSettings,
    updateSupplier,
} from "../api/suppliersApi";

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

const badgeStyle = {
    display: "inline-block",
    padding: "5px 9px",
    borderRadius: "999px",
    fontSize: "12px",
    fontWeight: 600,
    backgroundColor: "#eff6ff",
    color: "#1d4ed8",
};

const inactiveBadgeStyle = {
    ...badgeStyle,
    backgroundColor: "#fef2f2",
    color: "#b91c1c",
};

const collapseButtonStyle = {
    padding: "8px 12px",
    borderRadius: "8px",
    border: "1px solid #cbd5e1",
    backgroundColor: "#ffffff",
    cursor: "pointer",
    fontWeight: 600,
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

const buttonPrimaryStyle = {
    padding: "10px 16px",
    backgroundColor: "#2563eb",
    color: "#ffffff",
    border: "none",
    borderRadius: "8px",
    cursor: "pointer",
    fontWeight: 700,
};

const buttonSecondaryStyle = {
    padding: "10px 16px",
    backgroundColor: "#ffffff",
    color: "#111827",
    border: "1px solid #cbd5e1",
    borderRadius: "8px",
    cursor: "pointer",
    fontWeight: 700,
};

const tableStyle = {
    width: "100%",
    borderCollapse: "collapse",
    fontSize: "14px",
};

const tableHeadCellStyle = {
    textAlign: "left",
    fontSize: "12px",
    fontWeight: 700,
    letterSpacing: "0.04em",
    textTransform: "uppercase",
    color: "#64748b",
    padding: "10px 12px",
    borderBottom: "1px solid #e2e8f0",
};

const tableCellStyle = {
    padding: "12px",
    borderBottom: "1px solid #eef2f7",
    color: "#111827",
    verticalAlign: "top",
};

const truncateText = (value, maxLength = 56) => {
    const text = String(value || "").trim();
    if (!text) {
        return "—";
    }
    if (text.length <= maxLength) {
        return text;
    }
    return `${text.slice(0, maxLength - 1)}…`;
};

const splitSupplierCities = (value) => {
    const raw = String(value || "").trim();
    if (!raw) {
        return [];
    }

    const seen = new Set();
    const items = [];
    raw
        .replace(/\|/g, ",")
        .replace(/;/g, ",")
        .split(",")
        .map((item) => item.trim())
        .filter(Boolean)
        .forEach((item) => {
            const key = item.toLowerCase();
            if (seen.has(key)) {
                return;
            }
            seen.add(key);
            items.push(item);
        });
    return items;
};

const serializeSupplierCities = (cities) =>
    cities
        .map((item) => String(item || "").trim())
        .filter(Boolean)
        .join("; ");

const defaultDraft = {
    is_active: true,
    code: "",
    name: "",
    city: "",
    salesdrive_supplier_id: "",
    biotus_orders_enabled: false,
    np_fulfillment_enabled: false,
    schedule_enabled: false,
    block_start_day: "",
    block_start_time: "",
    block_end_day: "",
    block_end_time: "",
    feed_url: "",
    gdrive_folder: "",
    is_rrp: false,
    profit_percent: "",
    retail_markup: "",
    min_markup_threshold: "",
    priority: 5,
    use_feed_instead_of_gdrive: false,
};

const defaultStoreSettingsDraft = {
    supplier_code: "",
    is_active: true,
    priority_override: "",
    min_markup_threshold: "",
    extra_markup_enabled: false,
    extra_markup_mode: "percent",
    extra_markup_value: "",
    extra_markup_min: "",
    extra_markup_max: "",
    dumping_mode: false,
};

const boolSummary = (value) => (value ? "Да" : "Нет");

const formatStoreLabel = (store) => {
    if (!store) {
        return "";
    }
    const parts = [store.store_name || store.store_code || "Магазин"];
    if (store.store_code) {
        parts.push(store.store_code);
    }
    if (store.tabletki_branch) {
        parts.push(`Branch ${store.tabletki_branch}`);
    }
    return parts.join(" · ");
};

const SupplierInput = ({ label, value, onChange, type = "text", disabled = false, placeholder = "" }) => (
    <label style={{ display: "grid", gap: "6px" }}>
        <span style={{ fontSize: "13px", color: "#64748b", fontWeight: 600 }}>{label}</span>
        <input
            type={type}
            value={value}
            disabled={disabled}
            placeholder={placeholder}
            onChange={(event) => onChange(event.target.value)}
            style={{
                ...inputStyle,
                backgroundColor: disabled ? "#f8fafc" : "#ffffff",
                color: disabled ? "#64748b" : "#111827",
            }}
        />
    </label>
);

const SupplierCheckbox = ({ label, checked, onChange }) => (
    <div style={{ display: "grid", gap: "6px", alignContent: "start" }}>
        <label style={{ fontSize: "13px", color: "#64748b", fontWeight: 600 }}>{label}</label>
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
                checked={checked}
                onChange={(event) => onChange(event.target.checked)}
                style={{ width: "18px", height: "18px" }}
            />
            <span>{checked ? "Включено" : "Выключено"}</span>
        </label>
    </div>
);

const DAY_OPTIONS = [
    { value: "1", label: "Понедельник (Mon)" },
    { value: "2", label: "Вторник (Tue)" },
    { value: "3", label: "Среда (Wed)" },
    { value: "4", label: "Четверг (Thu)" },
    { value: "5", label: "Пятница (Fri)" },
    { value: "6", label: "Суббота (Sat)" },
    { value: "7", label: "Воскресенье (Sun)" },
];

const SupplierSelect = ({ label, value, onChange, options, disabled = false }) => (
    <label style={{ display: "grid", gap: "6px" }}>
        <span style={{ fontSize: "13px", color: "#64748b", fontWeight: 600 }}>{label}</span>
        <select
            value={value}
            disabled={disabled}
            onChange={(event) => onChange(event.target.value)}
            style={{
                ...inputStyle,
                backgroundColor: disabled ? "#f8fafc" : "#ffffff",
                color: disabled ? "#64748b" : "#111827",
            }}
        >
            <option value="">Не выбрано</option>
            {options.map((option) => (
                <option key={option.value} value={option.value}>
                    {option.label}
                </option>
            ))}
        </select>
    </label>
);

const SuppliersPage = () => {
    const [suppliers, setSuppliers] = useState([]);
    const [selectedCode, setSelectedCode] = useState("");
    const [detail, setDetail] = useState(null);
    const [draft, setDraft] = useState(defaultDraft);
    const [isCreating, setIsCreating] = useState(false);
    const [listLoading, setListLoading] = useState(true);
    const [detailLoading, setDetailLoading] = useState(false);
    const [listError, setListError] = useState("");
    const [detailError, setDetailError] = useState("");
    const [saveError, setSaveError] = useState("");
    const [saveSuccess, setSaveSuccess] = useState("");
    const [openTechnical, setOpenTechnical] = useState(false);
    const [showOnlyActive, setShowOnlyActive] = useState(false);
    const [customCityInput, setCustomCityInput] = useState("");
    const [businessStores, setBusinessStores] = useState([]);
    const [storesLoading, setStoresLoading] = useState(true);
    const [storesError, setStoresError] = useState("");
    const [selectedStoreId, setSelectedStoreId] = useState("");
    const [storeSettingsDraft, setStoreSettingsDraft] = useState(defaultStoreSettingsDraft);
    const [storeSettingsOverview, setStoreSettingsOverview] = useState([]);
    const [storeSettingsLoading, setStoreSettingsLoading] = useState(false);
    const [storeSettingsError, setStoreSettingsError] = useState("");
    const [storeSettingsSaveError, setStoreSettingsSaveError] = useState("");
    const [storeSettingsSaveSuccess, setStoreSettingsSaveSuccess] = useState("");

    const loadList = async () => {
        setListLoading(true);
        setListError("");
        try {
            const data = await getSuppliersViewList();
            setSuppliers(data);
            if (!selectedCode && data.length > 0 && !isCreating) {
                setSelectedCode(data[0].code);
            }
        } catch (error) {
            console.error("Ошибка загрузки списка поставщиков:", error);
            setListError("Не удалось загрузить список поставщиков.");
        } finally {
            setListLoading(false);
        }
    };

    const loadBusinessStores = async () => {
        setStoresLoading(true);
        setStoresError("");
        try {
            const data = await getBusinessStores();
            setBusinessStores(Array.isArray(data) ? data : []);
        } catch (error) {
            console.error("Ошибка загрузки business stores:", error);
            setStoresError("Не удалось загрузить список магазинов Business-контура.");
        } finally {
            setStoresLoading(false);
        }
    };

    useEffect(() => {
        loadList();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [isCreating, selectedCode]);

    useEffect(() => {
        loadBusinessStores();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, []);

    useEffect(() => {
        if (!selectedCode || isCreating) {
            setStoreSettingsOverview([]);
            setSelectedStoreId("");
            setStoreSettingsDraft(defaultStoreSettingsDraft);
            return;
        }

        const loadDetail = async () => {
            setDetailLoading(true);
            setDetailError("");
            try {
                const data = await getSupplierViewDetail(selectedCode);
                setDetail(data);
                setDraft({
                    is_active: Boolean(data.is_active),
                    code: data.code || "",
                    name: data.name || "",
                    city: data.cities_raw || "",
                    salesdrive_supplier_id: data.salesdrive_supplier_id ?? "",
                    biotus_orders_enabled: Boolean(data.biotus_orders_enabled),
                    np_fulfillment_enabled: Boolean(data.np_fulfillment_enabled),
                    schedule_enabled: Boolean(data.schedule_enabled),
                    block_start_day: data.block_start_day != null ? String(data.block_start_day) : "",
                    block_start_time: data.block_start_time || "",
                    block_end_day: data.block_end_day != null ? String(data.block_end_day) : "",
                    block_end_time: data.block_end_time || "",
                    feed_url: data.feed_url || "",
                    gdrive_folder: data.gdrive_folder || "",
                    is_rrp: Boolean(data.is_rrp),
                    profit_percent: data.profit_percent ?? "",
                    retail_markup: data.retail_markup ?? "",
                    min_markup_threshold: data.min_markup_threshold ?? "",
                    priority: data.priority ?? 5,
                    use_feed_instead_of_gdrive: Boolean(data.use_feed_instead_of_gdrive),
                });
            } catch (error) {
                console.error("Ошибка загрузки поставщика:", error);
                setDetail(null);
                setDetailError("Не удалось загрузить карточку поставщика.");
            } finally {
                setDetailLoading(false);
            }
        };

        loadDetail();
    }, [isCreating, selectedCode]);

    useEffect(() => {
        if (!selectedCode || isCreating) {
            return;
        }

        const loadOverview = async () => {
            setStoreSettingsLoading(true);
            setStoreSettingsError("");
            setStoreSettingsSaveError("");
            setStoreSettingsSaveSuccess("");
            try {
                const overview = await getBusinessSupplierStoreSettingsOverview(selectedCode);
                setStoreSettingsOverview(Array.isArray(overview) ? overview : []);

                const preferredStoreId =
                    String(selectedStoreId || "").trim()
                    || String((Array.isArray(overview) && overview[0]?.store_id) || "")
                    || String((businessStores[0] || {}).id || "");

                if (preferredStoreId) {
                    setSelectedStoreId(preferredStoreId);
                    const exact = Array.isArray(overview)
                        ? overview.find((item) => String(item.store_id) === preferredStoreId)
                        : null;
                    await applyStoreSettingsDraft(preferredStoreId, selectedCode, exact || null);
                } else {
                    setStoreSettingsDraft(buildStoreSettingsDraft(selectedCode, null));
                }
            } catch (error) {
                console.error("Ошибка загрузки обзора настроек поставщика по магазинам:", error);
                setStoreSettingsOverview([]);
                setStoreSettingsError("Не удалось загрузить настройки поставщика по магазинам.");
                setStoreSettingsDraft(buildStoreSettingsDraft(selectedCode, null));
            } finally {
                setStoreSettingsLoading(false);
            }
        };

        loadOverview();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [isCreating, selectedCode, businessStores]);

    const selectedListItem = useMemo(
        () => suppliers.find((item) => item.code === selectedCode) || null,
        [suppliers, selectedCode]
    );
    const filteredSuppliers = useMemo(
        () => (showOnlyActive ? suppliers.filter((item) => item.is_active) : suppliers),
        [showOnlyActive, suppliers]
    );
    const showEditor = isCreating || Boolean(detail);
    const overviewByStoreId = useMemo(() => {
        const map = new Map();
        storeSettingsOverview.forEach((item) => {
            map.set(String(item.store_id), item);
        });
        return map;
    }, [storeSettingsOverview]);
    const selectedCities = useMemo(() => splitSupplierCities(draft.city), [draft.city]);
    const knownCityOptions = useMemo(() => {
        const seen = new Set();
        const values = [];

        suppliers.forEach((supplier) => {
            (supplier.cities_list || []).forEach((city) => {
                const normalized = String(city || "").trim();
                if (!normalized) {
                    return;
                }
                const key = normalized.toLowerCase();
                if (seen.has(key)) {
                    return;
                }
                seen.add(key);
                values.push(normalized);
            });
        });

        selectedCities.forEach((city) => {
            const normalized = String(city || "").trim();
            if (!normalized) {
                return;
            }
            const key = normalized.toLowerCase();
            if (seen.has(key)) {
                return;
            }
            seen.add(key);
            values.push(normalized);
        });

        return values.sort((left, right) => left.localeCompare(right));
    }, [selectedCities, suppliers]);

    const cardTitle = isCreating ? "Новый поставщик" : "Карточка поставщика";

    const setField = (key, value) => {
        setDraft((prev) => ({
            ...prev,
            [key]: value,
        }));
    };

    const resetCreate = () => {
        setIsCreating(true);
        setSelectedCode("");
        setDetail(null);
        setDetailError("");
        setSaveError("");
        setSaveSuccess("");
        setCustomCityInput("");
        setDraft(defaultDraft);
        setSelectedStoreId("");
        setStoreSettingsOverview([]);
        setStoreSettingsDraft(defaultStoreSettingsDraft);
        setStoreSettingsError("");
        setStoreSettingsSaveError("");
        setStoreSettingsSaveSuccess("");
        setOpenTechnical(false);
    };

    const selectSupplier = (code) => {
        setIsCreating(false);
        setSaveError("");
        setSaveSuccess("");
        setCustomCityInput("");
        setStoreSettingsError("");
        setStoreSettingsSaveError("");
        setStoreSettingsSaveSuccess("");
        setSelectedCode(code);
    };

    const handleCancel = () => {
        setSaveError("");
        setSaveSuccess("");
        setCustomCityInput("");
        setStoreSettingsError("");
        setStoreSettingsSaveError("");
        setStoreSettingsSaveSuccess("");
        if (isCreating) {
            setIsCreating(false);
            if (suppliers.length > 0) {
                setSelectedCode(suppliers[0].code);
            }
            return;
        }

        if (detail) {
            setDraft({
                is_active: Boolean(detail.is_active),
                code: detail.code || "",
                name: detail.name || "",
                city: detail.cities_raw || "",
                salesdrive_supplier_id: detail.salesdrive_supplier_id ?? "",
                biotus_orders_enabled: Boolean(detail.biotus_orders_enabled),
                np_fulfillment_enabled: Boolean(detail.np_fulfillment_enabled),
                schedule_enabled: Boolean(detail.schedule_enabled),
                block_start_day: detail.block_start_day != null ? String(detail.block_start_day) : "",
                block_start_time: detail.block_start_time || "",
                block_end_day: detail.block_end_day != null ? String(detail.block_end_day) : "",
                block_end_time: detail.block_end_time || "",
                feed_url: detail.feed_url || "",
                gdrive_folder: detail.gdrive_folder || "",
                is_rrp: Boolean(detail.is_rrp),
                profit_percent: detail.profit_percent ?? "",
                retail_markup: detail.retail_markup ?? "",
                min_markup_threshold: detail.min_markup_threshold ?? "",
                priority: detail.priority ?? 5,
                use_feed_instead_of_gdrive: Boolean(detail.use_feed_instead_of_gdrive),
            });
            setStoreSettingsDraft(buildStoreSettingsDraft(selectedCode, overviewByStoreId.get(String(selectedStoreId)) || null));
        }
    };

    const buildStoreSettingsDraft = (supplierCode, existing) => ({
        supplier_code: String(supplierCode || ""),
        is_active: existing ? Boolean(existing.is_active) : true,
        priority_override: existing?.priority_override ?? "",
        min_markup_threshold: existing?.min_markup_threshold ?? detail?.min_markup_threshold ?? "",
        extra_markup_enabled: existing ? Boolean(existing.extra_markup_enabled) : false,
        extra_markup_mode: String(existing?.extra_markup_mode || "percent"),
        extra_markup_value: existing?.extra_markup_value ?? "",
        extra_markup_min: existing?.extra_markup_min ?? "",
        extra_markup_max: existing?.extra_markup_max ?? "",
        dumping_mode: existing ? Boolean(existing.dumping_mode) : false,
    });

    const applyStoreSettingsDraft = async (storeId, supplierCode, prefetchedOverview = null) => {
        const existingFromOverview = prefetchedOverview ?? overviewByStoreId.get(String(storeId));
        if (existingFromOverview) {
            setStoreSettingsDraft(buildStoreSettingsDraft(supplierCode, existingFromOverview));
            return;
        }

        try {
            const rows = await getBusinessStoreSupplierSettings(storeId);
            const exact = Array.isArray(rows)
                ? rows.find((item) => String(item.supplier_code || "").trim() === String(supplierCode || "").trim())
                : null;
            setStoreSettingsDraft(buildStoreSettingsDraft(supplierCode, exact || null));
        } catch (error) {
            console.error("Ошибка загрузки store-specific supplier settings:", error);
            setStoreSettingsDraft(buildStoreSettingsDraft(supplierCode, null));
            setStoreSettingsError("Не удалось загрузить настройки поставщика для выбранного магазина.");
        }
    };

    const applyCities = (cities) => {
        setField("city", serializeSupplierCities(cities));
    };

    const toggleCity = (cityName) => {
        const normalized = String(cityName || "").trim();
        if (!normalized) {
            return;
        }

        const existing = splitSupplierCities(draft.city);
        const exists = existing.some((item) => item.toLowerCase() === normalized.toLowerCase());
        if (exists) {
            applyCities(existing.filter((item) => item.toLowerCase() !== normalized.toLowerCase()));
            return;
        }
        applyCities([...existing, normalized]);
    };

    const addCustomCity = () => {
        const normalized = String(customCityInput || "").trim();
        if (!normalized) {
            return;
        }
        const existing = splitSupplierCities(draft.city);
        const exists = existing.some((item) => item.toLowerCase() === normalized.toLowerCase());
        if (!exists) {
            applyCities([...existing, normalized]);
        }
        setCustomCityInput("");
    };

    const removeCity = (cityName) => {
        const normalized = String(cityName || "").trim().toLowerCase();
        if (!normalized) {
            return;
        }
        applyCities(splitSupplierCities(draft.city).filter((item) => item.toLowerCase() !== normalized));
    };

    const buildPayload = () => ({
        code: String(draft.code || "").trim(),
        name: String(draft.name || "").trim(),
        city: String(draft.city || "").trim() || null,
        salesdrive_supplier_id:
            draft.salesdrive_supplier_id === "" ? null : Number(draft.salesdrive_supplier_id),
        biotus_orders_enabled: Boolean(draft.biotus_orders_enabled),
        np_fulfillment_enabled: Boolean(draft.np_fulfillment_enabled),
        schedule_enabled: Boolean(draft.schedule_enabled),
        block_start_day: draft.block_start_day === "" ? null : Number(draft.block_start_day),
        block_start_time: String(draft.block_start_time || "").trim() || null,
        block_end_day: draft.block_end_day === "" ? null : Number(draft.block_end_day),
        block_end_time: String(draft.block_end_time || "").trim() || null,
        feed_url: String(draft.feed_url || "").trim() || null,
        gdrive_folder: String(draft.gdrive_folder || "").trim() || null,
        is_rrp: Boolean(draft.is_rrp),
        profit_percent: draft.profit_percent === "" ? null : Number(draft.profit_percent),
        retail_markup: draft.retail_markup === "" ? null : Number(draft.retail_markup),
        min_markup_threshold: draft.min_markup_threshold === "" ? null : Number(draft.min_markup_threshold),
        is_active: Boolean(draft.is_active),
        priority: draft.priority === "" ? 5 : Number(draft.priority),
        use_feed_instead_of_gdrive: Boolean(draft.use_feed_instead_of_gdrive),
    });

    const setStoreSettingsField = (key, value) => {
        setStoreSettingsDraft((prev) => ({
            ...prev,
            [key]: value,
        }));
    };

    const handleStoreSelection = async (value) => {
        setSelectedStoreId(value);
        setStoreSettingsError("");
        setStoreSettingsSaveError("");
        setStoreSettingsSaveSuccess("");
        if (!value || !selectedCode) {
            setStoreSettingsDraft(buildStoreSettingsDraft(selectedCode, null));
            return;
        }
        await applyStoreSettingsDraft(value, selectedCode);
    };

    const buildStoreSettingsPayload = () => {
        const minMarkupThreshold = String(storeSettingsDraft.min_markup_threshold ?? "").trim();
        if (!minMarkupThreshold) {
            throw new Error("Минимальная ценовая надбавка обязательна.");
        }

        return {
            supplier_code: String(selectedCode || "").trim(),
            is_active: Boolean(storeSettingsDraft.is_active),
            priority_override:
                String(storeSettingsDraft.priority_override ?? "").trim() === ""
                    ? null
                    : Number(storeSettingsDraft.priority_override),
            min_markup_threshold: Number(minMarkupThreshold),
            extra_markup_enabled: Boolean(storeSettingsDraft.extra_markup_enabled),
            extra_markup_mode: storeSettingsDraft.extra_markup_enabled ? "percent" : null,
            extra_markup_value:
                String(storeSettingsDraft.extra_markup_value ?? "").trim() === ""
                    ? null
                    : Number(storeSettingsDraft.extra_markup_value),
            extra_markup_min:
                String(storeSettingsDraft.extra_markup_min ?? "").trim() === ""
                    ? null
                    : Number(storeSettingsDraft.extra_markup_min),
            extra_markup_max:
                String(storeSettingsDraft.extra_markup_max ?? "").trim() === ""
                    ? null
                    : Number(storeSettingsDraft.extra_markup_max),
            dumping_mode: Boolean(storeSettingsDraft.dumping_mode),
        };
    };

    const handleStoreSettingsSave = async () => {
        setStoreSettingsSaveError("");
        setStoreSettingsSaveSuccess("");
        if (!selectedCode) {
            setStoreSettingsSaveError("Сначала выберите поставщика.");
            return;
        }
        if (!selectedStoreId) {
            setStoreSettingsSaveError("Выберите магазин.");
            return;
        }

        let payload;
        try {
            payload = buildStoreSettingsPayload();
        } catch (error) {
            setStoreSettingsSaveError(error.message || "Не удалось подготовить store-specific настройки.");
            return;
        }

        try {
            const saved = await upsertBusinessStoreSupplierSettings(selectedStoreId, selectedCode, payload);
            const selectedStoreRow = businessStores.find((item) => String(item.id) === String(selectedStoreId));
            setStoreSettingsDraft(buildStoreSettingsDraft(selectedCode, saved));
            setStoreSettingsOverview((prev) => {
                const next = prev.filter((item) => String(item.store_id) !== String(selectedStoreId));
                next.push({
                    ...saved,
                    store_id: Number(selectedStoreId),
                    store_code: selectedStoreRow?.store_code || "",
                    store_name: selectedStoreRow?.store_name || "",
                    enterprise_code: selectedStoreRow?.enterprise_code || null,
                    tabletki_branch: selectedStoreRow?.tabletki_branch || null,
                });
                next.sort((left, right) => String(left.store_name || "").localeCompare(String(right.store_name || "")));
                return next;
            });
            setStoreSettingsSaveSuccess("Настройки поставщика для выбранного магазина сохранены.");
        } catch (error) {
            console.error("Ошибка сохранения store-specific supplier settings:", error);
            const detail = error?.response?.data?.detail;
            setStoreSettingsSaveError(
                typeof detail === "string" && detail ? detail : "Не удалось сохранить настройки поставщика для магазина.",
            );
        }
    };

    const handleSave = async () => {
        setSaveError("");
        setSaveSuccess("");

        const payload = buildPayload();
        if (!payload.code || !payload.name) {
            setSaveError("Код и название поставщика обязательны.");
            return;
        }

        try {
            if (isCreating) {
                await createSupplier(payload);
                setSaveSuccess("Поставщик создан.");
            } else {
                await updateSupplier(selectedCode, payload);
                setSaveSuccess("Изменения сохранены.");
            }

            await loadList();
            setIsCreating(false);
            setSelectedCode(payload.code);
        } catch (error) {
            console.error("Ошибка сохранения поставщика:", error);
            setSaveError("Не удалось сохранить поставщика.");
        }
    };

    return (
        <div style={pageStyle}>
            <div style={{ ...cardStyle, padding: "20px 24px" }}>
                <h1 style={{ margin: 0, fontSize: "28px", color: "#111827" }}>Поставщики</h1>
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
                        <h2 style={sectionTitleStyle}>Выбор поставщика</h2>
                    </div>

                    <select
                        value={isCreating ? "__new__" : selectedCode}
                        onChange={(event) => {
                            const value = event.target.value;
                            if (!value) {
                                setSelectedCode("");
                                setDetail(null);
                                return;
                            }
                            if (value === "__new__") {
                                resetCreate();
                                return;
                            }
                            selectSupplier(value);
                        }}
                        style={inputStyle}
                    >
                        {isCreating ? <option value="__new__">Новый поставщик</option> : null}
                        {!isCreating ? <option value="">-- Выберите поставщика --</option> : null}
                        {suppliers.map((supplier) => (
                            <option key={supplier.code} value={supplier.code}>
                                {supplier.display_name}
                            </option>
                        ))}
                    </select>

                    <div style={{ display: "flex", gap: "12px", flexWrap: "wrap" }}>
                        <button onClick={resetCreate} style={buttonPrimaryStyle}>
                            Новый поставщик
                        </button>
                        <button onClick={loadList} style={buttonSecondaryStyle}>
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
                            onChange={(event) => setShowOnlyActive(event.target.checked)}
                            style={{ width: "18px", height: "18px" }}
                        />
                        <span>Только активные</span>
                    </label>

                    {listLoading ? <div style={mutedTextStyle}>Загрузка поставщиков…</div> : null}
                    {listError ? <div style={{ color: "#b91c1c", fontWeight: 600 }}>{listError}</div> : null}
                    {!listLoading && !listError && filteredSuppliers.length === 0 ? (
                        showOnlyActive ? (
                            <div style={mutedTextStyle}>Нет активных поставщиков.</div>
                        ) : (
                            <div style={mutedTextStyle}>Поставщики пока не найдены.</div>
                        )
                    ) : null}

                    <div style={{ display: "grid", gap: "10px" }}>
                        {filteredSuppliers.map((supplier) => {
                            const selected = supplier.code === selectedCode;
                            return (
                                <button
                                    key={supplier.code}
                                    onClick={() => selectSupplier(supplier.code)}
                                    style={{
                                        textAlign: "left",
                                        padding: "14px",
                                        borderRadius: "10px",
                                        border: selected && !isCreating
                                            ? "2px solid #2563eb"
                                            : "1px solid #dbe4ee",
                                        backgroundColor: "#ffffff",
                                        cursor: "pointer",
                                        display: "grid",
                                        gap: "6px",
                                    }}
                                >
                                    <div style={{ fontSize: "15px", fontWeight: 700, color: "#111827" }}>
                                        {supplier.display_name}
                                    </div>
                                    <div style={{ fontSize: "13px", color: "#64748b" }}>
                                        Код: {supplier.code}
                                    </div>
                                    <div style={{ fontSize: "13px", color: "#64748b" }}>
                                        Города: {supplier.cities_list.length > 0 ? supplier.cities_list.join(", ") : "—"}
                                    </div>
                                    <div style={{ fontSize: "13px", color: "#64748b" }}>
                                        Цены: {truncateText(supplier.pricing_summary)}
                                    </div>
                                    <div style={{ display: "flex", gap: "8px", flexWrap: "wrap" }}>
                                        <span style={supplier.is_active ? badgeStyle : inactiveBadgeStyle}>
                                            {supplier.is_active ? "Активен" : "Неактивен"}
                                        </span>
                                    </div>
                                </button>
                            );
                        })}
                    </div>
                </div>

                <div style={{ display: "grid", gap: "20px" }}>
                    <div style={{ ...cardStyle, padding: "18px 20px" }}>
                        <div
                            style={{
                                display: "flex",
                                justifyContent: "space-between",
                                alignItems: "center",
                                gap: "12px",
                                flexWrap: "wrap",
                            }}
                        >
                            <div style={{ display: "grid", gap: "8px" }}>
                                <h2 style={sectionTitleStyle}>{cardTitle}</h2>
                                {!isCreating && selectedListItem ? (
                                    <div style={{ display: "flex", gap: "8px", flexWrap: "wrap", alignItems: "center" }}>
                                        <span style={selectedListItem.is_active ? badgeStyle : inactiveBadgeStyle}>
                                            {selectedListItem.is_active ? "Активен" : "Неактивен"}
                                        </span>
                                        <span style={{ ...mutedTextStyle, fontWeight: 600 }}>
                                            {selectedListItem.display_name}
                                        </span>
                                    </div>
                                ) : null}
                            </div>
                            <div style={{ display: "flex", gap: "10px", flexWrap: "wrap" }}>
                                <button onClick={handleSave} style={buttonPrimaryStyle}>
                                    Сохранить
                                </button>
                                <button onClick={handleCancel} style={buttonSecondaryStyle}>
                                    Отмена
                                </button>
                            </div>
                        </div>
                    </div>

                    {detailLoading ? (
                        <div style={{ ...cardStyle, padding: "20px", ...mutedTextStyle }}>Загрузка данных поставщика…</div>
                    ) : detailError ? (
                        <div style={{ ...cardStyle, padding: "20px", color: "#b91c1c", fontWeight: 600 }}>{detailError}</div>
                    ) : !showEditor ? (
                        isCreating ? null : <div style={{ ...cardStyle, padding: "20px", ...mutedTextStyle }}>Выберите поставщика из списка.</div>
                    ) : (
                        <>
                            {saveError ? (
                                <div style={{ ...cardStyle, padding: "20px", color: "#b91c1c", fontWeight: 600 }}>{saveError}</div>
                            ) : null}
                            {saveSuccess ? (
                                <div style={{ ...cardStyle, padding: "20px", color: "#166534", fontWeight: 600 }}>{saveSuccess}</div>
                            ) : null}

                            <div style={{ ...cardStyle, padding: "18px 20px", display: "grid", gap: "14px" }}>
                                <h2 style={sectionTitleStyle}>Основная информация о поставщике</h2>
                                <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: "12px" }}>
                                    <div style={{ display: "grid", gap: "6px" }}>
                                        <span style={{ fontSize: "13px", color: "#64748b", fontWeight: 600 }}>Код поставщика</span>
                                        <div style={{ fontSize: "15px", fontWeight: 700, color: "#111827" }}>{draft.code || "—"}</div>
                                    </div>
                                    <div style={{ display: "grid", gap: "6px" }}>
                                        <span style={{ fontSize: "13px", color: "#64748b", fontWeight: 600 }}>Название поставщика</span>
                                        <div style={{ fontSize: "15px", fontWeight: 700, color: "#111827" }}>{draft.name || "—"}</div>
                                    </div>
                                    <div style={{ display: "grid", gap: "6px" }}>
                                        <span style={{ fontSize: "13px", color: "#64748b", fontWeight: 600 }}>Статус</span>
                                        <div style={{ fontSize: "14px", color: "#111827" }}>{draft.is_active ? "Активный" : "Неактивный"}</div>
                                    </div>
                                    <div style={{ display: "grid", gap: "6px" }}>
                                        <span style={{ fontSize: "13px", color: "#64748b", fontWeight: 600 }}>Источник / подключение</span>
                                        <div style={{ fontSize: "14px", color: "#111827" }}>{detail?.source_summary || "—"}</div>
                                    </div>
                                    <div style={{ display: "grid", gap: "6px", gridColumn: "1 / -1" }}>
                                        <span style={{ fontSize: "13px", color: "#64748b", fontWeight: 600 }}>Код SalesDrive / supplierlist</span>
                                        <div style={{ fontSize: "14px", color: "#111827" }}>{draft.salesdrive_supplier_id || "—"}</div>
                                    </div>
                                </div>
                            </div>

                            <div style={{ ...cardStyle, padding: "18px 20px", display: "grid", gap: "14px" }}>
                                <h2 style={sectionTitleStyle}>Глобальные настройки поставщика</h2>
                                <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: "12px" }}>
                                    <SupplierCheckbox
                                        label="Активный (Active)"
                                        checked={Boolean(draft.is_active)}
                                        onChange={(value) => setField("is_active", value)}
                                    />
                                    <div />
                                    <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: "12px", gridColumn: "1 / -1" }}>
                                        <SupplierInput
                                            label="Код (Code)"
                                            value={draft.code}
                                            disabled={!isCreating}
                                            onChange={(value) => setField("code", value)}
                                        />
                                        <SupplierInput
                                            label="Название (Name)"
                                            value={draft.name}
                                            onChange={(value) => setField("name", value)}
                                        />
                                        <SupplierInput
                                            label="Код SalesDrive / supplierlist"
                                            type="number"
                                            value={draft.salesdrive_supplier_id}
                                            onChange={(value) => setField("salesdrive_supplier_id", value)}
                                        />
                                    </div>
                                </div>
                            </div>

                            <div style={{ ...cardStyle, padding: "18px 20px", display: "grid", gap: "14px" }}>
                                <h2 style={sectionTitleStyle}>Источник / подключение</h2>
                                <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: "12px" }}>
                                    <SupplierInput
                                        label="Основной источник / URL / токен"
                                        value={draft.feed_url}
                                        onChange={(value) => setField("feed_url", value)}
                                    />
                                    <SupplierInput
                                        label="Дополнительный источник / параметр"
                                        value={draft.gdrive_folder}
                                        onChange={(value) => setField("gdrive_folder", value)}
                                    />
                                </div>
                            </div>

                            <div style={{ ...cardStyle, padding: "18px 20px", display: "grid", gap: "14px" }}>
                                <h2 style={sectionTitleStyle}>Базовые ценовые параметры</h2>
                                <div style={{ display: "grid", gap: "12px" }}>
                                    <SupplierCheckbox
                                        label="Есть РРЦ (RRP)"
                                        checked={Boolean(draft.is_rrp)}
                                        onChange={(value) => setField("is_rrp", value)}
                                    />
                                    <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: "12px" }}>
                                        <SupplierInput
                                            label="Параметр для расчёта оптовой цены (%)"
                                            type="number"
                                            value={draft.profit_percent}
                                            onChange={(value) => setField("profit_percent", value)}
                                        />
                                        <SupplierInput
                                            label="Наценка для розничной цены (%)"
                                            type="number"
                                            value={draft.retail_markup}
                                            onChange={(value) => setField("retail_markup", value)}
                                        />
                                        <SupplierInput
                                            label="Минимальная ценовая надбавка"
                                            type="number"
                                            value={draft.min_markup_threshold}
                                            onChange={(value) => setField("min_markup_threshold", value)}
                                        />
                                        <SupplierInput
                                            label="Приоритет (Priority)"
                                            type="number"
                                            value={draft.priority}
                                            onChange={(value) => setField("priority", value)}
                                        />
                                    </div>
                                </div>
                            </div>

                            <div style={{ ...cardStyle, padding: "18px 20px", display: "grid", gap: "14px" }}>
                                <h2 style={sectionTitleStyle}>Настройки поставщика по магазину</h2>
                                <p style={mutedTextStyle}>
                                    Глобальные настройки поставщика применяются ко всем магазинам по умолчанию.
                                    Параметры ниже переопределяют их только для выбранного магазина.
                                </p>

                                <div style={{ display: "grid", gridTemplateColumns: "minmax(0, 1fr) auto", gap: "12px", alignItems: "end" }}>
                                    <label style={{ display: "grid", gap: "6px" }}>
                                        <span style={{ fontSize: "13px", color: "#64748b", fontWeight: 600 }}>Магазин</span>
                                        <select
                                            value={selectedStoreId}
                                            onChange={(event) => handleStoreSelection(event.target.value)}
                                            style={inputStyle}
                                            disabled={storesLoading || !selectedCode}
                                        >
                                            <option value="">Выберите магазин</option>
                                            {businessStores.map((store) => (
                                                <option key={store.id} value={store.id}>
                                                    {formatStoreLabel(store)}
                                                </option>
                                            ))}
                                        </select>
                                    </label>
                                    <button
                                        type="button"
                                        onClick={handleStoreSettingsSave}
                                        style={buttonPrimaryStyle}
                                        disabled={!selectedStoreId || !selectedCode}
                                    >
                                        Сохранить настройки магазина
                                    </button>
                                </div>

                                {storesError ? (
                                    <div style={{ color: "#b91c1c", fontWeight: 600 }}>{storesError}</div>
                                ) : null}
                                {storeSettingsError ? (
                                    <div style={{ color: "#b91c1c", fontWeight: 600 }}>{storeSettingsError}</div>
                                ) : null}
                                {storeSettingsSaveError ? (
                                    <div style={{ color: "#b91c1c", fontWeight: 600 }}>{storeSettingsSaveError}</div>
                                ) : null}
                                {storeSettingsSaveSuccess ? (
                                    <div style={{ color: "#166534", fontWeight: 600 }}>{storeSettingsSaveSuccess}</div>
                                ) : null}
                                {storeSettingsLoading ? <div style={mutedTextStyle}>Загрузка настроек магазина…</div> : null}

                                <div style={{ display: "grid", gap: "16px" }}>
                                    <div style={{ display: "grid", gap: "12px" }}>
                                        <h3 style={{ margin: 0, fontSize: "16px", color: "#111827" }}>Статус поставщика в магазине</h3>
                                        <div style={{ display: "grid", gridTemplateColumns: "repeat(3, minmax(0, 1fr))", gap: "12px" }}>
                                            <SupplierCheckbox
                                                label="Активный для магазина"
                                                checked={Boolean(storeSettingsDraft.is_active)}
                                                onChange={(value) => setStoreSettingsField("is_active", value)}
                                            />
                                            <SupplierInput
                                                label="Приоритет"
                                                type="number"
                                                value={storeSettingsDraft.priority_override}
                                                onChange={(value) => setStoreSettingsField("priority_override", value)}
                                            />
                                            <SupplierCheckbox
                                                label="Режим демпинга"
                                                checked={Boolean(storeSettingsDraft.dumping_mode)}
                                                onChange={(value) => setStoreSettingsField("dumping_mode", value)}
                                            />
                                        </div>
                                    </div>

                                    <div style={{ display: "grid", gap: "12px" }}>
                                        <h3 style={{ margin: 0, fontSize: "16px", color: "#111827" }}>Ценообразование для магазина</h3>
                                        <p style={{ ...mutedTextStyle, margin: 0 }}>
                                            Эти настройки применяются для выбранной пары `магазин + поставщик`.
                                            Фиксированная дополнительная наценка имеет приоритет над диапазоном min/max.
                                            Если фиксированное значение пустое, для товаров используется стабильная наценка в пределах диапазона.
                                        </p>
                                        <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: "12px" }}>
                                            <SupplierInput
                                                label="Минимальная ценовая надбавка (обязательно)"
                                                type="number"
                                                value={storeSettingsDraft.min_markup_threshold}
                                                onChange={(value) => setStoreSettingsField("min_markup_threshold", value)}
                                            />
                                            <SupplierCheckbox
                                                label="Дополнительная наценка включена"
                                                checked={Boolean(storeSettingsDraft.extra_markup_enabled)}
                                                onChange={(value) => setStoreSettingsField("extra_markup_enabled", value)}
                                            />
                                            <SupplierInput
                                                label="Режим дополнительной наценки"
                                                value={storeSettingsDraft.extra_markup_mode || "percent"}
                                                onChange={(value) => setStoreSettingsField("extra_markup_mode", value)}
                                                disabled
                                            />
                                            <SupplierInput
                                                label="Фиксированная дополнительная наценка (%)"
                                                type="number"
                                                value={storeSettingsDraft.extra_markup_value}
                                                onChange={(value) => setStoreSettingsField("extra_markup_value", value)}
                                            />
                                            <SupplierInput
                                                label="Минимум диапазона доп. наценки (%)"
                                                type="number"
                                                value={storeSettingsDraft.extra_markup_min}
                                                onChange={(value) => setStoreSettingsField("extra_markup_min", value)}
                                            />
                                            <SupplierInput
                                                label="Максимум диапазона доп. наценки (%)"
                                                type="number"
                                                value={storeSettingsDraft.extra_markup_max}
                                                onChange={(value) => setStoreSettingsField("extra_markup_max", value)}
                                            />
                                        </div>
                                    </div>
                                </div>

                                <div style={{ display: "grid", gap: "10px" }}>
                                    <h3 style={{ margin: 0, fontSize: "16px", color: "#111827" }}>Уже настроенные магазины</h3>
                                    {storeSettingsOverview.length === 0 ? (
                                        <div style={mutedTextStyle}>Для этого поставщика пока нет store-specific overrides.</div>
                                    ) : (
                                        <div style={{ overflowX: "auto" }}>
                                            <table style={tableStyle}>
                                                <thead>
                                                    <tr>
                                                        <th style={tableHeadCellStyle}>Магазин</th>
                                                        <th style={tableHeadCellStyle}>Активный</th>
                                                        <th style={tableHeadCellStyle}>Приоритет</th>
                                                        <th style={tableHeadCellStyle}>Мин. надбавка</th>
                                                        <th style={tableHeadCellStyle}>Доп. наценка</th>
                                                        <th style={tableHeadCellStyle}>Режим демпинга</th>
                                                        <th style={tableHeadCellStyle}>Действие</th>
                                                    </tr>
                                                </thead>
                                                <tbody>
                                                    {storeSettingsOverview.map((item) => (
                                                        <tr key={`${item.store_id}-${item.id}`}>
                                                            <td style={tableCellStyle}>
                                                                <div style={{ fontWeight: 600 }}>{item.store_name || item.store_code}</div>
                                                                <div style={{ color: "#64748b", fontSize: "12px" }}>
                                                                    {[item.store_code, item.tabletki_branch ? `Branch ${item.tabletki_branch}` : null]
                                                                        .filter(Boolean)
                                                                        .join(" · ")}
                                                                </div>
                                                            </td>
                                                            <td style={tableCellStyle}>{boolSummary(item.is_active)}</td>
                                                            <td style={tableCellStyle}>{item.priority_override ?? "—"}</td>
                                                            <td style={tableCellStyle}>{item.min_markup_threshold ?? "—"}</td>
                                                            <td style={tableCellStyle}>
                                                                {item.extra_markup_enabled
                                                                    ? `${item.extra_markup_mode || "percent"} / ${item.extra_markup_value ?? "—"}`
                                                                    : "Выключена"}
                                                            </td>
                                                            <td style={tableCellStyle}>{boolSummary(item.dumping_mode)}</td>
                                                            <td style={tableCellStyle}>
                                                                <button
                                                                    type="button"
                                                                    onClick={() => handleStoreSelection(String(item.store_id))}
                                                                    style={buttonSecondaryStyle}
                                                                >
                                                                    Редактировать
                                                                </button>
                                                            </td>
                                                        </tr>
                                                    ))}
                                                </tbody>
                                            </table>
                                        </div>
                                    )}
                                </div>
                            </div>

                            <div style={{ ...cardStyle, padding: "18px 20px", display: "grid", gap: "14px" }}>
                                <h2 style={sectionTitleStyle}>Заказы</h2>
                                <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: "12px" }}>
                                    <SupplierCheckbox
                                        label="Участвует в обработке заказов Biotus"
                                        checked={Boolean(draft.biotus_orders_enabled)}
                                        onChange={(value) => setField("biotus_orders_enabled", value)}
                                    />
                                    <SupplierCheckbox
                                        label="Fulfillment-режим Новой Почты"
                                        checked={Boolean(draft.np_fulfillment_enabled)}
                                        onChange={(value) => setField("np_fulfillment_enabled", value)}
                                    />
                                </div>
                            </div>

                            <div style={{ ...cardStyle, padding: "18px 20px", display: "grid", gap: "14px" }}>
                                <h2 style={sectionTitleStyle}>График недоступности</h2>
                                <p style={mutedTextStyle}>
                                    Здесь задаётся окно, в котором поставщик считается временно недоступным и его offers
                                    не обновляются в dropship pipeline.
                                </p>
                                <div style={{ display: "grid", gap: "12px" }}>
                                    <SupplierCheckbox
                                        label="Включить окно недоступности (Schedule enabled)"
                                        checked={Boolean(draft.schedule_enabled)}
                                        onChange={(value) => setField("schedule_enabled", value)}
                                    />
                                    <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: "12px" }}>
                                        <SupplierSelect
                                            label="Начало: день"
                                            value={draft.block_start_day}
                                            disabled={!draft.schedule_enabled}
                                            options={DAY_OPTIONS}
                                            onChange={(value) => setField("block_start_day", value)}
                                        />
                                        <SupplierInput
                                            label="Начало: время"
                                            type="time"
                                            value={draft.block_start_time}
                                            disabled={!draft.schedule_enabled}
                                            onChange={(value) => setField("block_start_time", value)}
                                        />
                                        <SupplierSelect
                                            label="Конец: день"
                                            value={draft.block_end_day}
                                            disabled={!draft.schedule_enabled}
                                            options={DAY_OPTIONS}
                                            onChange={(value) => setField("block_end_day", value)}
                                        />
                                        <SupplierInput
                                            label="Конец: время"
                                            type="time"
                                            value={draft.block_end_time}
                                            disabled={!draft.schedule_enabled}
                                            onChange={(value) => setField("block_end_time", value)}
                                        />
                                    </div>
                                </div>
                            </div>

                            <div style={{ ...cardStyle, padding: "18px 20px", display: "grid", gap: "14px" }}>
                                <h2 style={sectionTitleStyle}>Города</h2>
                                <p style={mutedTextStyle}>
                                    Временный технический блок. После отвязки нового контура от городов он больше не является
                                    основной рабочей настройкой поставщика.
                                </p>
                                <div style={{ display: "grid", gap: "12px" }}>
                                    <div style={{ display: "grid", gap: "8px" }}>
                                        <div style={{ fontSize: "13px", color: "#64748b", fontWeight: 600 }}>
                                            Выбрано
                                        </div>
                                        {selectedCities.length > 0 ? (
                                            <div style={{ display: "flex", gap: "8px", flexWrap: "wrap" }}>
                                                {selectedCities.map((city) => (
                                                    <button
                                                        key={city}
                                                        type="button"
                                                        onClick={() => removeCity(city)}
                                                        style={{
                                                            padding: "6px 10px",
                                                            borderRadius: "999px",
                                                            border: "1px solid #bfdbfe",
                                                            backgroundColor: "#eff6ff",
                                                            color: "#1d4ed8",
                                                            cursor: "pointer",
                                                            fontWeight: 600,
                                                        }}
                                                    >
                                                        {city} ×
                                                    </button>
                                                ))}
                                            </div>
                                        ) : (
                                            <div style={mutedTextStyle}>Города пока не выбраны.</div>
                                        )}
                                    </div>

                                    <div style={{ display: "grid", gap: "8px" }}>
                                        <div style={{ fontSize: "13px", color: "#64748b", fontWeight: 600 }}>
                                            Известные города
                                        </div>
                                        {knownCityOptions.length > 0 ? (
                                            <div style={{ display: "flex", gap: "8px", flexWrap: "wrap" }}>
                                                {knownCityOptions.map((city) => {
                                                    const selected = selectedCities.some(
                                                        (item) => item.toLowerCase() === city.toLowerCase()
                                                    );
                                                    return (
                                                        <button
                                                            key={city}
                                                            type="button"
                                                            onClick={() => toggleCity(city)}
                                                            style={{
                                                                padding: "8px 12px",
                                                                borderRadius: "999px",
                                                                border: selected ? "1px solid #2563eb" : "1px solid #cbd5e1",
                                                                backgroundColor: selected ? "#2563eb" : "#ffffff",
                                                                color: selected ? "#ffffff" : "#111827",
                                                                cursor: "pointer",
                                                                fontWeight: 600,
                                                            }}
                                                        >
                                                            {city}
                                                        </button>
                                                    );
                                                })}
                                            </div>
                                        ) : (
                                            <div style={mutedTextStyle}>Список будет собираться из уже настроенных поставщиков.</div>
                                        )}
                                    </div>

                                    <div style={{ display: "grid", gridTemplateColumns: "minmax(0, 1fr) auto", gap: "12px", alignItems: "end" }}>
                                        <SupplierInput
                                            label="Добавить город вручную"
                                            value={customCityInput}
                                            onChange={setCustomCityInput}
                                            placeholder="Например: Kyiv"
                                        />
                                        <button type="button" onClick={addCustomCity} style={buttonSecondaryStyle}>
                                            Добавить
                                        </button>
                                    </div>
                                </div>
                            </div>

                            <div style={{ ...cardStyle, padding: "18px 20px", display: "grid", gap: "14px" }}>
                                <div
                                    style={{
                                        display: "flex",
                                        justifyContent: "space-between",
                                        alignItems: "center",
                                        gap: "12px",
                                        flexWrap: "wrap",
                                    }}
                                >
                                    <h2 style={sectionTitleStyle}>Технические настройки</h2>
                                    <button onClick={() => setOpenTechnical((value) => !value)} style={collapseButtonStyle}>
                                        {openTechnical ? "Скрыть" : "Показать"}
                                    </button>
                                </div>
                                {openTechnical ? (
                                    <div style={{ display: "grid", gridTemplateColumns: "repeat(2, minmax(0, 1fr))", gap: "12px" }}>
                                        <SupplierCheckbox
                                            label="Режим демпинга (Dumping mode)"
                                            checked={Boolean(draft.use_feed_instead_of_gdrive)}
                                            onChange={(value) => setField("use_feed_instead_of_gdrive", value)}
                                        />
                                    </div>
                                ) : null}
                            </div>
                        </>
                    )}
                </div>
            </div>
        </div>
    );
};

export default SuppliersPage;
