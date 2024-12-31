import React, { useEffect, useState } from "react";
import { getEnterprises, createEnterprise, updateEnterprise } from "../api/enterpriseApi";
import developerApi from "../api/developerApi";
import Form from "../components/Form";

const { getDataFormats } = developerApi;

const EnterprisePanel = () => {
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
      } catch (error) {
        console.error("Error loading enterprises:", error);
      }
    };

    const fetchDataFormats = async () => {
      try {
        const formats = await getDataFormats();
        setDataFormats(formats);
      } catch (error) {
        console.error("Error loading data formats:", error);
      }
    };

    fetchEnterprises();
    fetchDataFormats();
  }, []);

  const handleSave = async (enterpriseData) => {
    try {
      console.log("Saving enterprise:", enterpriseData);
      if (isEditing) {
        console.log("Updating enterprise with PUT:", originalCode);
        await updateEnterprise(originalCode, enterpriseData);
      } else {
        console.log("Creating new enterprise with POST:", enterpriseData);
        await createEnterprise(enterpriseData);
      }
      const data = await getEnterprises();
      setEnterprises(data);
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
    { name: "enterprise_code", label: "Enterprise Code (Код підприємства)", type: "text", disabled: isEditing },
    { name: "enterprise_name", label: "Enterprise Name (Назва підприємства)", type: "text" },
    { name: "enterprise_login", label: "Enterprise Login (Логін підприємства)", type: "text" },
    { name: "enterprise_password", label: "Enterprise Password (Пароль підприємства)", type: "password" },
    { name: "tabletki_login", label: "Tabletki login (Логин tabletki )", type: "text" },
    { name: "tabletki_password", label: "Tabletki Password (Пароль tabletki)", type: "password" },
    { name: "email", label: "Email (Електронна пошта)", type: "email" },
    { name: "branch_id", label: "Branch ID (ID філії)", type: "text" },
    {
      name: "data_format",
      label: "Data Format (Формат даних)",
      type: "select",
      options: dataFormats.map((format) => ({ value: format.format_name, label: format.format_name })),
    },
    {
      name: "file_format",
      label: "File Format (Формат файлів)",
      type: "select",
      options: [
        { value: "xml", label: "XML" },
        { value: "excel", label: "Excel" },
        { value: "csv", label: "CSV" },
      ],
    },
    {
      name: "data_transfer_method",
      label: "Data Transfer Method (Метод передачі даних)",
      type: "select",
      options: [
        { value: "api", label: "API" },
        { value: "googledrive", label: "Google Drive" },
        { value: "panel", label: "Панель" },
      ],
    },
    { name: "single_store", label: "Single Store (Єдиний магазин)", type: "checkbox" },
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
      <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: "20px" }}>
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
      <div style={{ marginBottom: "20px" }}>
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
          style={{ padding: "10px 20px", marginTop: "20px", display: "block", margin: "0 auto" }}
        >
          Add New
        </button>
      )}
    </div>
  );
};

export default EnterprisePanel;