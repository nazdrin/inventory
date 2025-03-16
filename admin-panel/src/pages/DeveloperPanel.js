import React, { useState, useEffect } from "react";
import developerApi from "../api/developerApi";
import Form from "../components/Form";

const DeveloperPanel = ({ authUser }) => {
    const [currentSetting, setCurrentSetting] = useState(null);
    const [editing, setEditing] = useState(false);
    const [error, setError] = useState(null);
    const [dataFormats, setDataFormats] = useState([]);

    useEffect(() => {
        const fetchSettings = async () => {
            try {
                console.log(`📌 Запрашиваем настройки для: ${authUser.developer_login}`);
                const data = await developerApi.getSetting();

                // ✅ Заполняем поля по умолчанию, если они отсутствуют (соответствие схеме)
                const defaultSettings = {
                    developer_login: data.developer_login || "",
                    developer_password: data.developer_password || "",
                    endpoint_catalog: data.endpoint_catalog || "",
                    endpoint_stock: data.endpoint_stock || "",
                    endpoint_orders: data.endpoint_orders || "",
                    telegram_token_developer: data.telegram_token_developer || "",
                    message_orders: Boolean(data.message_orders),  // ✅ Приведение к bool
                    morion: data.morion || "",
                    tabletki: data.tabletki || "",
                    barcode: data.barcode || "",
                    optima: data.optima || "",
                    badm: data.badm || "",
                    venta: data.venta || "",
                };

                console.log("✅ Полученные настройки:", defaultSettings);
                setCurrentSetting(defaultSettings);
            } catch (err) {
                console.error("❌ Ошибка загрузки настроек:", err);
                setError("Failed to load settings.");
            }
        };

        const fetchDataFormats = async () => {
            try {
                console.log("📌 Запрашиваем форматы данных...");
                const formats = await developerApi.getDataFormats();
                console.log("✅ Получены форматы данных:", formats);
                setDataFormats(formats);
            } catch (err) {
                console.error("❌ Ошибка загрузки форматов данных:", err);
            }
        };

        fetchSettings();
        fetchDataFormats();
    }, []);

    useEffect(() => {
        console.log("📌 Обновление currentSetting:", currentSetting);
    }, [currentSetting]);

    const handleSave = async () => {
        console.log("🔹 Перед сохранением, данные:", currentSetting);

        if (!currentSetting || Object.keys(currentSetting).length === 0) {
            console.error("❌ Ошибка: currentSetting пустой!");
            setError("Невозможно сохранить пустые настройки.");
            return;
        }

        try {
            await developerApi.updateSetting(currentSetting);
            console.log("✅ Настройки успешно сохранены!");
            setEditing(false);
        } catch (err) {
            console.error("❌ Ошибка при сохранении настроек:", err);
            setError("Failed to save the settings. Please check the input and try again.");
        }
    };

    const handleAddDataFormat = async (newFormat) => {
        try {
            console.log("📌 Добавляем новый формат:", newFormat);
            await developerApi.addDataFormat(newFormat);
            const formats = await developerApi.getDataFormats();
            setDataFormats(formats);
        } catch (err) {
            console.error("❌ Ошибка добавления формата:", err);
        }
    };

    const developerFields = [
        { name: "developer_login", label: "Login", disabled: true },
        { name: "developer_password", label: "Password", type: "password" },
        { name: "endpoint_catalog", label: "Catalog Endpoint" },
        { name: "endpoint_stock", label: "Stock Endpoint" },
        { name: "endpoint_orders", label: "Orders Endpoint" },
        { name: "telegram_token_developer", label: "Telegram Token" },
        { name: "message_orders", label: "Статус отправки заказов", type: "checkbox" },
        { name: "morion", label: "Morion Code" },
        { name: "tabletki", label: "Tabletki Code" },
        { name: "barcode", label: "Barcode" },
        { name: "optima", label: "Optima Code" },
        { name: "badm", label: "Badm Code" },
        { name: "venta", label: "Venta Code" },
    ];

    return (
        <div style={{
            display: "flex", flexDirection: "column", paddingBottom: "30px", height: "100vh"
        }}>
            <div
                style={{
                    position: "sticky",
                    top: 0,
                    backgroundColor: "#f0f0f0",
                    zIndex: 10,
                    padding: "10px 20px",
                    display: "flex",
                    justifyContent: "space-between",
                    alignItems: "center",
                    borderBottom: "1px solid #ccc",
                }}
            >
                <h1 style={{ margin: 0 }}>Developer Panel</h1>
                <div>
                    <button
                        onClick={handleSave}
                        style={{
                            marginRight: "10px",
                            padding: "10px 20px",
                            fontSize: "16px",
                            backgroundColor: "green",
                            color: "white",
                            border: "none",
                            borderRadius: "5px",
                            cursor: "pointer",
                        }}
                    >
                        Save
                    </button>
                    <button
                        onClick={() => setEditing(false)}
                        style={{
                            padding: "10px 20px",
                            fontSize: "16px",
                            backgroundColor: "red",
                            color: "white",
                            border: "none",
                            borderRadius: "5px",
                            cursor: "pointer",
                        }}
                    >
                        Cancel
                    </button>
                </div>
            </div>

            <div style={{ flex: 1, overflowY: "auto", padding: "20px", display: "flex", justifyContent: "center" }}>
                <div style={{ maxWidth: "600px", width: "100%", paddingBottom: "50px" }}>
                    {error && <p style={{ color: "red" }}>{error}</p>}
                    {currentSetting && (
                        <Form
                            fields={developerFields}
                            values={currentSetting || {}}
                            onChange={setCurrentSetting}
                            onSubmit={handleSave}
                        />
                    )}
                    <div>
                        <h2>Data Formats</h2>
                        <ul>
                            {dataFormats.map((format) => (
                                <li key={format.id}>{format.format_name}</li>
                            ))}
                        </ul>
                        <form
                            onSubmit={(e) => {
                                e.preventDefault();
                                const newFormat = e.target.newFormat.value.trim();
                                if (newFormat) {
                                    handleAddDataFormat({ format_name: newFormat });
                                    e.target.reset();
                                }
                            }}
                            style={{ display: "flex", alignItems: "center", marginTop: "20px" }}
                        >
                            <label htmlFor="newFormat">Add New Format:</label>
                            <input id="newFormat" name="newFormat" type="text" />
                            <button type="submit">Add</button>
                        </form>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default DeveloperPanel;