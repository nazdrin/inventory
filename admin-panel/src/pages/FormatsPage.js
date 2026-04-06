import React, { useEffect, useState } from "react";
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

const mutedTextStyle = {
    margin: 0,
    color: "#64748b",
    fontSize: "14px",
    lineHeight: 1.5,
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

const badgeStyle = {
    display: "inline-block",
    padding: "5px 9px",
    borderRadius: "999px",
    fontSize: "12px",
    fontWeight: 600,
    backgroundColor: "#eff6ff",
    color: "#1d4ed8",
};

const getFormatHint = (formatName) => {
    const hints = {
        GoogleDrive: "Файловый источник через Google Drive",
        Vetmanager: "API-интеграция Vetmanager",
        Checkbox: "API-формат Checkbox",
        Rozetka: "XML / feed-поток Rozetka",
        Prom: "Prom feed / API",
        Bioteca: "AINUR API / несколько магазинов",
        DSN: "XML feed DSN",
        HProfit: "XML feed HProfit",
        Biotus: "XML feed Biotus",
        JetVet: "Google Drive / файловый обмен",
        ComboKeyCRM: "Feed / public key / URL",
        KeyCRM: "Интеграция с KeyCRM",
        Dntrade: "Маршрутизация по store",
        Ftp: "Файловый обмен по FTP",
        FtpMulti: "Несколько файлов по FTP",
        FtpZoomagazin: "FTP Zoomagazin",
        TorgsoftGoogle: "Torgsoft через Google Drive",
        TorgsoftGoogleMulti: "Несколько магазинов через Google Drive",
        FTPTabletki: "Импорт через FTP Tabletki",
        Business: "Business / dropship",
        Blank: "Отключённый / пустой формат",
        Unipro: "Входящая интеграция",
    };

    return hints[formatName] || "Формат данных";
};

const FormatsPage = () => {
    const [formats, setFormats] = useState([]);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState("");
    const [addError, setAddError] = useState("");
    const [newFormat, setNewFormat] = useState("");

    const loadFormats = async () => {
        setLoading(true);
        setError("");
        try {
            const data = await developerApi.getDataFormats();
            const sorted = [...data].sort((a, b) => a.format_name.localeCompare(b.format_name));
            setFormats(sorted);
        } catch (err) {
            console.error("Ошибка загрузки реестра форматов:", err);
            setError("Не удалось загрузить реестр форматов.");
        } finally {
            setLoading(false);
        }
    };

    useEffect(() => {
        loadFormats();
    }, []);

    const handleAddFormat = async (event) => {
        event.preventDefault();
        setAddError("");
        const normalized = newFormat.trim();
        if (!normalized) {
            return;
        }

        try {
            await developerApi.addDataFormat({ format_name: normalized });
            setNewFormat("");
            await loadFormats();
        } catch (err) {
            console.error("Ошибка добавления формата:", err);
            setAddError("Не удалось добавить формат.");
        }
    };

    return (
        <div style={pageStyle}>
            <div style={{ ...cardStyle, padding: "20px 24px" }}>
                <h1 style={{ margin: 0, fontSize: "28px", color: "#111827" }}>Форматы данных</h1>
            </div>

            <div style={{ ...cardStyle, padding: "18px 20px" }}>
                <div style={{ display: "grid", gap: "10px" }}>
                    <h2 style={sectionTitleStyle}>Форматы</h2>
                </div>
            </div>

            <div
                style={{
                    display: "grid",
                    gridTemplateColumns: "minmax(300px, 380px) minmax(0, 1fr)",
                    gap: "20px",
                    alignItems: "start",
                }}
            >
                <div style={{ ...cardStyle, padding: "18px 20px" }}>
                    <div style={{ display: "grid", gap: "12px" }}>
                        <h2 style={sectionTitleStyle}>Добавить формат</h2>
                        <form onSubmit={handleAddFormat} style={{ display: "grid", gap: "12px" }}>
                            <input
                                type="text"
                                value={newFormat}
                                onChange={(e) => setNewFormat(e.target.value)}
                                placeholder="Например: NewFormat"
                                style={inputStyle}
                            />
                            {addError ? <div style={{ color: "#b91c1c", fontWeight: 600 }}>{addError}</div> : null}
                            <button type="submit" style={primaryButtonStyle}>
                                Добавить формат
                            </button>
                        </form>
                    </div>
                </div>

                <div style={{ ...cardStyle, overflow: "hidden" }}>
                    <div
                        style={{
                            padding: "18px 20px",
                            borderBottom: "1px solid #e2e8f0",
                            display: "flex",
                            justifyContent: "space-between",
                            alignItems: "center",
                            gap: "12px",
                        }}
                    >
                        <div>
                            <h2 style={sectionTitleStyle}>Список форматов</h2>
                        </div>
                        <button
                            onClick={loadFormats}
                            style={{
                                padding: "10px 14px",
                                borderRadius: "8px",
                                border: "1px solid #cbd5e1",
                                backgroundColor: "#ffffff",
                                cursor: "pointer",
                                fontWeight: 600,
                            }}
                        >
                            Обновить
                        </button>
                    </div>

                    {loading ? (
                        <div style={{ padding: "20px", ...mutedTextStyle }}>Загрузка форматов…</div>
                    ) : error ? (
                        <div style={{ padding: "20px", color: "#b91c1c", fontWeight: 600 }}>{error}</div>
                    ) : formats.length === 0 ? (
                        <div style={{ padding: "20px", ...mutedTextStyle }}>Форматы пока не найдены.</div>
                    ) : (
                        <div style={{ display: "grid", gap: "12px", padding: "16px" }}>
                            {formats.map((format) => (
                                <div
                                    key={format.id || format.format_name}
                                    style={{
                                        display: "grid",
                                        gap: "8px",
                                        padding: "16px",
                                        border: "1px solid #e2e8f0",
                                        borderRadius: "10px",
                                        backgroundColor: "#ffffff",
                                    }}
                                >
                                    <div
                                        style={{
                                            display: "flex",
                                            justifyContent: "space-between",
                                            alignItems: "flex-start",
                                            gap: "12px",
                                            flexWrap: "wrap",
                                        }}
                                    >
                                        <div style={{ fontSize: "17px", fontWeight: 700, color: "#111827" }}>
                                            {format.format_name}
                                        </div>
                                        <span style={badgeStyle}>формат</span>
                                    </div>
                                    <p style={mutedTextStyle}>{getFormatHint(format.format_name)}</p>
                                </div>
                            ))}
                        </div>
                    )}
                </div>
            </div>
        </div>
    );
};

export default FormatsPage;
