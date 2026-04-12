import React, { useEffect, useMemo, useState } from "react";
import { useNavigate } from "react-router-dom";
import developerApi from "../api/developerApi";

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

const mutedTextStyle = {
    margin: 0,
    color: "#64748b",
    fontSize: "14px",
    lineHeight: 1.5,
};

const DEFAULT_SETTINGS = {
    developer_login: "",
    developer_password: "",
    endpoint_catalog: "",
    endpoint_stock: "",
    endpoint_orders: "",
    telegram_token_developer: "",
    message_orders: false,
    morion: "",
    tabletki: "",
    barcode: "",
    optima: "",
    badm: "",
    venta: "",
};

const FIELD_LABELS = {
    developer_login: "Логин",
    developer_password: "Пароль",
    message_orders: "Уведомления об успешной выгрузке",
    endpoint_catalog: "Endpoint каталога",
    endpoint_stock: "Endpoint остатков",
    endpoint_orders: "Endpoint заказов",
    morion: "Morion Code",
    tabletki: "Tabletki Code",
    barcode: "Barcode",
    optima: "Optima Code",
    badm: "Badm Code",
    venta: "Venta Code",
};

const SECTIONS = [
    {
        key: "auth",
        title: "Admin / Auth",
        collapsible: false,
        fields: ["developer_login", "developer_password"],
    },
    {
        key: "notifications",
        title: "Уведомления",
        collapsible: false,
        fields: ["message_orders"],
    },
    {
        key: "endpoints",
        title: "Global Endpoints",
        collapsible: true,
        defaultOpen: false,
        fields: ["endpoint_catalog", "endpoint_stock", "endpoint_orders"],
    },
    {
        key: "code_maps",
        title: "Static Code Maps / Maintenance Data",
        collapsible: true,
        defaultOpen: false,
        fields: ["morion", "tabletki", "barcode", "optima", "badm", "venta"],
    },
];

const DeveloperPanel = ({ authUser }) => {
    const navigate = useNavigate();
    const [currentSetting, setCurrentSetting] = useState(DEFAULT_SETTINGS);
    const [error, setError] = useState(null);
    const [saveSuccess, setSaveSuccess] = useState("");
    const [loading, setLoading] = useState(true);
    const [openSections, setOpenSections] = useState({
        endpoints: false,
        code_maps: false,
    });

    useEffect(() => {
        const fetchSettings = async () => {
            setLoading(true);
            setError(null);
            try {
                const data = await developerApi.getSetting();
                setCurrentSetting({
                    ...DEFAULT_SETTINGS,
                    ...data,
                    message_orders: Boolean(data?.message_orders),
                });
            } catch (err) {
                console.error("Ошибка загрузки настроек:", err);
                setError("Не удалось загрузить настройки.");
            } finally {
                setLoading(false);
            }
        };

        fetchSettings();
    }, [authUser.developer_login]);

    const visibleSections = useMemo(() => SECTIONS, []);

    const handleFieldChange = (key, value) => {
        setCurrentSetting((prev) => ({
            ...prev,
            [key]: value,
        }));
    };

    const handleSave = async () => {
        setError(null);
        setSaveSuccess("");

        try {
            await developerApi.updateSetting(currentSetting);
            setSaveSuccess("Изменения сохранены.");
        } catch (err) {
            console.error("Ошибка при сохранении настроек:", err);
            setError("Не удалось сохранить настройки.");
        }
    };

    const handleReset = async () => {
        setError(null);
        setSaveSuccess("");
        setLoading(true);

        try {
            const data = await developerApi.getSetting();
            setCurrentSetting({
                ...DEFAULT_SETTINGS,
                ...data,
                message_orders: Boolean(data?.message_orders),
            });
        } catch (err) {
            console.error("Ошибка при сбросе настроек:", err);
            setError("Не удалось перезагрузить настройки.");
        } finally {
            setLoading(false);
        }
    };

    const toggleSection = (key) => {
        setOpenSections((prev) => ({
            ...prev,
            [key]: !prev[key],
        }));
    };

    const renderField = (fieldKey) => {
        const value = currentSetting[fieldKey];
        const isCheckbox = fieldKey === "message_orders";
        const isPassword = fieldKey === "developer_password";
        const isReadonly = fieldKey === "developer_login";

        return (
            <div
                key={fieldKey}
                style={{
                    display: "grid",
                    gap: "6px",
                    alignContent: "start",
                }}
            >
                <label style={labelStyle}>{FIELD_LABELS[fieldKey]}</label>

                {isCheckbox ? (
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
                ) : (
                    <input
                        type={isPassword ? "password" : "text"}
                        value={value || ""}
                        onChange={(e) => handleFieldChange(fieldKey, e.target.value)}
                        style={{
                            ...inputStyle,
                            backgroundColor: isReadonly ? "#f8fafc" : "#ffffff",
                        }}
                        disabled={isReadonly}
                    />
                )}
            </div>
        );
    };

    return (
        <div style={pageStyle}>
            <div style={{ ...cardStyle, padding: "20px 24px" }}>
                <h1 style={{ margin: 0, fontSize: "28px", color: "#111827" }}>Настройки разработчика</h1>
            </div>

            <div
                style={{
                    display: "grid",
                    gridTemplateColumns: "minmax(0, 1fr)",
                    gap: "20px",
                }}
            >
                <div style={{ ...cardStyle, padding: "18px 20px", display: "grid", gap: "16px" }}>
                    <div style={{ display: "flex", justifyContent: "space-between", gap: "16px", flexWrap: "wrap" }}>
                        <div style={{ display: "flex", gap: "10px", flexWrap: "wrap" }}>
                            <button type="button" style={primaryButtonStyle} onClick={handleSave}>
                                Сохранить
                            </button>
                            <button type="button" style={secondaryButtonStyle} onClick={handleReset}>
                                Сбросить
                            </button>
                        </div>
                    </div>

                    {error ? <div style={{ color: "#b91c1c", fontWeight: 600 }}>{error}</div> : null}
                    {saveSuccess ? <div style={{ color: "#166534", fontWeight: 600 }}>{saveSuccess}</div> : null}
                    {loading ? <div style={mutedTextStyle}>Загрузка настроек…</div> : null}
                </div>

                {visibleSections.map((section) => {
                    const isOpen = section.collapsible ? Boolean(openSections[section.key]) : true;
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
                                </div>
                                {section.collapsible ? (
                                    <button type="button" style={secondaryButtonStyle} onClick={() => toggleSection(section.key)}>
                                        {isOpen ? "Свернуть" : "Развернуть"}
                                    </button>
                                ) : null}
                            </div>

                            {isOpen ? (
                                <div
                                    style={{
                                        display: "grid",
                                        gridTemplateColumns: "repeat(auto-fit, minmax(260px, 1fr))",
                                        gap: "16px 20px",
                                    }}
                                >
                                    {section.fields.map(renderField)}
                                </div>
                            ) : null}
                        </div>
                    );
                })}

                <div
                    style={{
                        ...cardStyle,
                        padding: "20px",
                        display: "grid",
                        gap: "12px",
                    }}
                >
                    <h2 style={{ ...sectionTitleStyle, marginTop: 0 }}>Форматы данных</h2>
                    <button
                        onClick={() => navigate("/formats")}
                        style={{
                            ...primaryButtonStyle,
                            width: "fit-content",
                        }}
                    >
                        Открыть реестр форматов
                    </button>
                </div>
            </div>
        </div>
    );
};

export default DeveloperPanel;
