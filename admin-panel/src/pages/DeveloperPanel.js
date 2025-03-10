import React, { useState, useEffect } from "react";
import developerApi from "../api/developerApi";
import Form from "../components/Form";

const DeveloperPanel = ({ authUser }) => {
    const [currentSetting, setCurrentSetting] = useState(null);
    const [editing, setEditing] = useState(false);
    const [error, setError] = useState(null);
    const [dataFormats, setDataFormats] = useState([]); // Состояние для форматов данных

    useEffect(() => {
        const fetchSettings = async () => {
            try {
                const data = await developerApi.getSetting(authUser.developer_login);
                setCurrentSetting(data);
            } catch (err) {
                setError("Failed to load settings.");
            }
        };

        const fetchDataFormats = async () => {
            try {
                const formats = await developerApi.getDataFormats();
                setDataFormats(formats);
            } catch (err) {
                console.error("Failed to fetch data formats:", err);
            }
        };

        fetchSettings();
        fetchDataFormats();
    }, [authUser]);

    const handleSave = async () => {
        try {
            await developerApi.updateSetting(authUser.developer_login, currentSetting);
            setEditing(false);
        } catch (err) {
            setError("Failed to save the settings. Please check the input and try again.");
        }
    };

    const handleAddDataFormat = async (newFormat) => {
        try {
            await developerApi.addDataFormat(newFormat);
            const formats = await developerApi.getDataFormats(); // Обновляем список форматов
            setDataFormats(formats);
        } catch (err) {
            console.error("Failed to add data format:", err);
        }
    };

    const developerFields = [
        { name: "developer_login", label: "Login", disabled: true },
        { name: "developer_password", label: "Password", type: "password" },
        { name: "endpoint_catalog", label: "Catalog Endpoint" },
        { name: "endpoint_stock", label: "Stock Endpoint" },
        { name: "endpoint_orders", label: "Orders Endpoint" },
        { name: "telegram_token_developer", label: "Endpoint Dntrade" },
        // { name: "catalog_data_retention", label: "Catalog Data Retention (days)", type: "number" },
        // { name: "stock_data_retention", label: "Stock Data Retention (hours)", type: "number" },
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
            {/* Fixed header with buttons */}
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

            {/* Scrollable content */}
            <div style={{ flex: 1, overflowY: "auto", padding: "20px", display: "flex", justifyContent: "center" }}>
                <div style={{ maxWidth: "600px", width: "100%", paddingBottom: "50px" }}>
                    {error && <p style={{ color: "red" }}>{error}</p>}
                    {currentSetting && (
                        <Form
                            fields={developerFields.map((field) => ({
                                ...field,
                                style: { maxWidth: "550px", width: "100%" }, // Ограничиваем ширину полей
                            }))}
                            values={currentSetting}
                            onChange={setCurrentSetting}
                            onSubmit={handleSave}
                        // Убрали кнопки внутри формы
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
                                const newFormat = e.target.newFormat.value;
                                if (newFormat) {
                                    handleAddDataFormat({ format_name: newFormat });
                                    e.target.reset();
                                }
                            }}
                            style={{ display: "flex", alignItems: "center", marginTop: "20px" }}
                        >
                            <label
                                htmlFor="newFormat"
                                style={{ marginRight: "10px", fontSize: "16px", fontWeight: "bold" }}
                            >
                                Add New Format:
                            </label>
                            <input
                                id="newFormat"
                                name="newFormat"
                                type="text"
                                placeholder="Enter format name"
                                style={{
                                    maxWidth: "500px",
                                    padding: "10px",
                                    marginRight: "10px",
                                    fontSize: "16px",
                                }}
                            />
                            <button
                                type="submit"
                                style={{
                                    padding: "10px 20px",
                                    fontSize: "16px",
                                    backgroundColor: "#007BFF",
                                    border: "none",
                                    borderRadius: "5px",
                                    cursor: "pointer",
                                    backgroundColor: '#ffc107',
                                    border: 'none',
                                    borderRadius: '5px',
                                    fontWeight: 'bold'
                                }}
                            >
                                Add
                            </button>
                        </form>
                    </div>
                </div>
            </div>
        </div>
    );
};

export default DeveloperPanel;