import React, { useEffect, useMemo, useState } from "react";
import {
    createSupplier,
    getSupplierViewDetail,
    getSuppliersViewList,
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

const defaultDraft = {
    is_active: true,
    code: "",
    name: "",
    city: "",
    salesdrive_supplier_id: "",
    biotus_orders_enabled: false,
    np_fulfillment_enabled: false,
    feed_url: "",
    gdrive_folder: "",
    is_rrp: false,
    profit_percent: "",
    retail_markup: "",
    min_markup_threshold: "",
    priority: 5,
    use_feed_instead_of_gdrive: false,
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

    useEffect(() => {
        loadList();
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [isCreating, selectedCode]);

    useEffect(() => {
        if (!selectedCode || isCreating) {
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

    const selectedListItem = useMemo(
        () => suppliers.find((item) => item.code === selectedCode) || null,
        [suppliers, selectedCode]
    );
    const filteredSuppliers = useMemo(
        () => (showOnlyActive ? suppliers.filter((item) => item.is_active) : suppliers),
        [showOnlyActive, suppliers]
    );
    const showEditor = isCreating || Boolean(detail);

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
        setDraft(defaultDraft);
        setOpenTechnical(false);
    };

    const selectSupplier = (code) => {
        setIsCreating(false);
        setSaveError("");
        setSaveSuccess("");
        setSelectedCode(code);
    };

    const handleCancel = () => {
        setSaveError("");
        setSaveSuccess("");
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
                feed_url: detail.feed_url || "",
                gdrive_folder: detail.gdrive_folder || "",
                is_rrp: Boolean(detail.is_rrp),
                profit_percent: detail.profit_percent ?? "",
                retail_markup: detail.retail_markup ?? "",
                min_markup_threshold: detail.min_markup_threshold ?? "",
                priority: detail.priority ?? 5,
                use_feed_instead_of_gdrive: Boolean(detail.use_feed_instead_of_gdrive),
            });
        }
    };

    const buildPayload = () => ({
        code: String(draft.code || "").trim(),
        name: String(draft.name || "").trim(),
        city: String(draft.city || "").trim() || null,
        salesdrive_supplier_id:
            draft.salesdrive_supplier_id === "" ? null : Number(draft.salesdrive_supplier_id),
        biotus_orders_enabled: Boolean(draft.biotus_orders_enabled),
        np_fulfillment_enabled: Boolean(draft.np_fulfillment_enabled),
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
                                <h2 style={sectionTitleStyle}>Основное</h2>
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
                                            label="Города (через ; )"
                                            value={draft.city}
                                            onChange={(value) => setField("city", value)}
                                            placeholder="Например: Kyiv, Lviv"
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
                                <h2 style={sectionTitleStyle}>Ценообразование</h2>
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
