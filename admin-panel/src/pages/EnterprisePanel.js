import React, { useEffect, useState } from "react";
import { getEnterprises, createEnterprise, updateEnterprise } from "../api/enterpriseApi";
import developerApi from "../api/developerApi";
import Form from "../components/Form";

const { getDataFormats } = developerApi;

const EnterprisePanel = () => {
    const [filteredEnterprises, setFilteredEnterprises] = useState([]); // Отфильтрованные предприятия
    const [enterprises, setEnterprises] = useState([]);
    const [dataFormats, setDataFormats] = useState([]); // Список форматов данных
    const [selectedEnterprise, setSelectedEnterprise] = useState(null);
    const [originalCode, setOriginalCode] = useState(null);
    const [isEditing, setIsEditing] = useState(false);


    useEffect(() => {
        const fetchEnterprises = async () => {
            try {
                const data = await getEnterprises();

                setEnterprises(data);

                // Фильтруем, только если `data_format` есть и не "Blank"
                const filtered = data.filter(ent => ent.data_format && ent.data_format !== "Blank");
                setFilteredEnterprises(filtered);
            } catch (error) {
                console.error("Error loading enterprises:", error);
            }
        };

        fetchEnterprises();
    }, []);
    useEffect(() => {
        const fetchDataFormats = async () => {
            try {
                const formats = await getDataFormats();
                setDataFormats(formats);
            } catch (error) {
                console.error("Error loading data formats:", error);
            }
        };

        fetchDataFormats();
    }, []);






    const handleSave = async (enterpriseData) => {
        try {
            if (isEditing) {
                await updateEnterprise(originalCode, enterpriseData);
            } else {
                await createEnterprise(enterpriseData);
            }

            // Загружаем заново данные о предприятиях
            const data = await getEnterprises();
            setEnterprises(data);

            // Фильтруем предприятия, у которых data_format не "Blank"
            const filtered = data.filter(ent => ent.data_format && ent.data_format !== "Blank");
            setFilteredEnterprises(filtered); // ✅ Теперь список обновляется!

            setSelectedEnterprise(null);
            setOriginalCode(null);
            setIsEditing(false);
        } catch (error) {
            console.error("Error saving enterprise:", error);
        }
    };



    const handleCancel = () => {
        setSelectedEnterprise(null);
        setOriginalCode(null);
        setIsEditing(false);
    };

    const fields = [
        { name: "enterprise_code", label: "Enterprise Code (Код підприємства)", type: "text" },
        { name: "enterprise_name", label: "Enterprise Name (Назва підприємства)", type: "text" },
        { name: "token", label: "Токен подключения к API предприятия", type: "text" },
        { name: "tabletki_login", label: "Tabletki login (Логин tabletki )", type: "text" },
        { name: "tabletki_password", label: "Tabletki Password (Пароль tabletki)", type: "password" },
        { name: "branch_id", label: "Branch ID (ID філії)", type: "text" },
        {
            name: "data_format",
            label: "Data Format (Формат даних)",
            type: "select",
            options: dataFormats.map((format) => ({ value: format.format_name, label: format.format_name })),
        },
        { name: "single_store", label: "Single Store (Єдиний магазин)", type: "checkbox" },
        { name: "auto_confirm", label: "Автоматичне бронювання", type: "checkbox" },
        { name: "store_serial", label: "Store Serial (Серійний номер магазину)", type: "text" },
        {
            name: "stock_upload_frequency",
            label: "Stock Upload Frequency (Частота оновлення залишків)",
            type: "number",
            min: 10,
            max: 120,
            step: 10,
        },
        {
            name: "catalog_upload_frequency",
            label: "Catalog Upload Frequency (Частота оновлення каталогу)",
            type: "number",
            min: 1,
            max: 7,
            step: 1,
        },
        { name: "stock_correction", label: "Stock Correction (Корекція залишків)", type: "checkbox" },
        { name: "google_drive_folder_id_ref", label: "Google Drive Folder ID Ref (ID папки Google Drive для референсів)", type: "text" },
        { name: "google_drive_folder_id_rest", label: "Google Drive Folder ID Rest (ID папки Google Drive для залишків)", type: "text" },
        { name: "discount_rate", label: "Discount Rate (Розмір знижки)", type: "number", min: -99, max: 99 },
        { name: "last_stock_upload", label: "Last Stock Upload (Останнє оновлення залишків)", type: "datetime-local" },
        { name: "last_catalog_upload", label: "Last Catalog Upload (Останнє оновлення каталогу)", type: "datetime-local" },
    ];

    return (
        <div>
            <div style={{
                position: "sticky",
                top: 0,
                backgroundColor: "#f0f0f0",
                zIndex: 10,
                padding: "10px 20px",
                display: "flex",
                justifyContent: "space-between",
                alignItems: "center",
                borderBottom: "1px solid #ccc",

                // display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "20px"
            }}>
                <h1>Enterprise Settings</h1>


                {selectedEnterprise && (
                    <div>
                        <button
                            style={{
                                padding: "10px 20px",
                                marginRight: "10px",
                                backgroundColor: "green",
                                color: "white",
                                border: "none",
                                cursor: "pointer",
                                borderRadius: "5px",
                            }}
                            onClick={() => handleSave(selectedEnterprise)}
                        >
                            Save
                        </button>
                        <button
                            style={{
                                padding: "10px 20px",
                                backgroundColor: "red",
                                color: "white",
                                border: "none",
                                cursor: "pointer",
                                borderRadius: "5px",
                            }}
                            onClick={handleCancel}
                        >
                            Cancel
                        </button>
                    </div>
                )}
            </div>
            <div style={{
                marginBottom: "10px", marginTop: "10px", marginLeft: "20px"

            }}>
                <label htmlFor="enterprise-select" style={{ marginRight: "10px" }}>
                    Select Enterprise:
                </label>
                <select
                    id="enterprise-select"
                    onChange={(e) => {
                        const selectedCode = e.target.value;
                        const enterprise = enterprises.find((ent) => ent.enterprise_code === selectedCode);
                        setSelectedEnterprise(enterprise || null);
                        setOriginalCode(enterprise?.enterprise_code || null);
                        setIsEditing(!!enterprise);
                    }}
                    value={selectedEnterprise?.enterprise_code || ""}
                    style={{ padding: "10px", width: "300px" }}
                >
                    <option value="">-- Select an Enterprise --</option>
                    {enterprises.map((enterprise) => (
                        <option key={enterprise.enterprise_code} value={enterprise.enterprise_code}>
                            {enterprise.enterprise_name} ({enterprise.enterprise_code})
                        </option>
                    ))}
                </select>
            </div>
            {!selectedEnterprise && (
                <div style={{
                    marginTop: "15px",
                    marginLeft: "15px",
                    padding: "10px",
                    maxWidth: "400px"
                }}>
                    <h3 style={{ marginBottom: "10px", fontSize: "18px" }}>Enterprises with Data Format</h3>
                    <ul style={{ listStyleType: "none", padding: 0 }}>
                        {filteredEnterprises.map(ent => (
                            <li key={ent.enterprise_code} style={{
                                padding: "8px",
                                borderBottom: "1px solid #ccc",
                                fontSize: "16px"
                            }}>
                                <strong>{ent.enterprise_name}</strong> <span style={{ color: "#555" }}>({ent.enterprise_code})</span>
                            </li>
                        ))}
                    </ul>
                </div>
            )}



            {selectedEnterprise && (
                <Form
                    fields={fields}
                    values={selectedEnterprise}
                    onChange={setSelectedEnterprise}
                    onSubmit={() => handleSave(selectedEnterprise)}
                    onCancel={handleCancel}
                    style={{ display: "grid", gap: "20px", maxWidth: "500px", margin: "0 auto" }}
                />
            )}
            {!selectedEnterprise && (
                <button
                    onClick={() => {
                        setSelectedEnterprise({});
                        setOriginalCode(null);
                        setIsEditing(false);
                    }}
                    style={{ padding: "10px 20px", marginTop: "20px", border: 'none', borderRadius: '5px', fontWeight: 'bold', backgroundColor: '#ffc107', display: "block", margin: "0 auto" }}
                >

                    Add New
                </button>
            )
            }
        </div >
    );
};

export default EnterprisePanel;