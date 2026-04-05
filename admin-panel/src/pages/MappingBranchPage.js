import React, { useEffect, useMemo, useState } from "react";
import { createMappingBranch, getMappingBranchViewList, updateMappingBranch } from "../api/mappingBranchAPI";
import { getEnterprises } from "../api/enterpriseApi";

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
    width: "100%",
    padding: "12px",
    backgroundColor: "#fbbf24",
    color: "#111827",
    border: "none",
    borderRadius: "8px",
    cursor: "pointer",
    fontWeight: 700,
};

const badgeStyle = {
    display: "inline-block",
    padding: "4px 8px",
    borderRadius: "999px",
    fontSize: "12px",
    fontWeight: 600,
    backgroundColor: "#eef2ff",
    color: "#334155",
};

const warningBadgeStyle = {
    ...badgeStyle,
    backgroundColor: "#fff4e5",
    color: "#9a3412",
};

const emptyValue = "—";

const formatDate = (value) => {
    if (!value) {
        return emptyValue;
    }

    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
        return value;
    }

    return date.toLocaleString("uk-UA");
};

const EnterpriseInfoItem = ({ label, value }) => (
    <div
        style={{
            backgroundColor: "#f8fafc",
            border: "1px solid #dbe4ee",
            borderRadius: "10px",
            padding: "12px 14px",
            display: "grid",
            gap: "4px",
        }}
    >
        <div style={{ fontSize: "13px", color: "#64748b", fontWeight: 600 }}>{label}</div>
        <div style={{ fontSize: "15px", color: "#111827", fontWeight: 600 }}>{value || emptyValue}</div>
    </div>
);

const MappingBranchPage = () => {
    const [enterprises, setEnterprises] = useState([]);
    const [mappingRecords, setMappingRecords] = useState([]);
    const [selectedEnterpriseCode, setSelectedEnterpriseCode] = useState("");
    const [listLoading, setListLoading] = useState(true);
    const [listError, setListError] = useState("");
    const [saveError, setSaveError] = useState("");
    const [editError, setEditError] = useState("");
    const [editSuccess, setEditSuccess] = useState("");
    const [editingBranch, setEditingBranch] = useState(null);
    const [editingStoreId, setEditingStoreId] = useState("");
    const [editingGoogleFolderId, setEditingGoogleFolderId] = useState("");

    const [branch, setBranch] = useState("");
    const [storeId, setStoreId] = useState("");
    const [googleFolderId, setGoogleFolderId] = useState("");

    const loadMappingViewList = async () => {
        setListLoading(true);
        setListError("");
        try {
            const data = await getMappingBranchViewList();
            setMappingRecords(data);
        } catch (error) {
            console.error("Error fetching mapping branch view list:", error);
            setListError("Не удалось загрузить список маппинга филиалов.");
        } finally {
            setListLoading(false);
        }
    };

    useEffect(() => {
        async function fetchInitialData() {
            try {
                const enterpriseData = await getEnterprises();
                setEnterprises(enterpriseData);

                const firstEnterpriseCode = enterpriseData.find((item) => item.data_format && item.data_format !== "Blank")?.enterprise_code
                    || enterpriseData[0]?.enterprise_code
                    || "";
                setSelectedEnterpriseCode(firstEnterpriseCode);
            } catch (error) {
                console.error("Error fetching enterprises:", error);
            }

            await loadMappingViewList();
        }

        fetchInitialData();
    }, []);

    const selectedEnterprise = useMemo(
        () => enterprises.find((enterprise) => enterprise.enterprise_code === selectedEnterpriseCode) || null,
        [enterprises, selectedEnterpriseCode]
    );

    const filteredMappings = useMemo(
        () => mappingRecords.filter((item) => item.enterprise_code === selectedEnterpriseCode),
        [mappingRecords, selectedEnterpriseCode]
    );

    const editingMapping = useMemo(
        () => filteredMappings.find((item) => item.branch === editingBranch) || null,
        [filteredMappings, editingBranch]
    );

    const handleSave = async () => {
        setSaveError("");
        if (!branch || !storeId || !selectedEnterpriseCode || !googleFolderId) {
            return;
        }

        const mappingData = {
            branch,
            store_id: storeId,
            enterprise_code: selectedEnterpriseCode,
            google_folder_id: googleFolderId.trim() || null,
            id_telegram: [],
        };

        try {
            await createMappingBranch(mappingData);
            setBranch("");
            setStoreId("");
            setGoogleFolderId("");
            alert("Запись успешно добавлена.");
            await loadMappingViewList();
        } catch (error) {
            console.error("Error saving mapping branch:", error);
            setSaveError("Ошибка при сохранении маппинга филиала.");
        }
    };

    const startEdit = (item) => {
        setEditError("");
        setEditSuccess("");
        setEditingBranch(item.branch);
        setEditingStoreId(item.store_mapping_value || "");
        setEditingGoogleFolderId(item.google_folder_id || "");
    };

    const cancelEdit = () => {
        setEditError("");
        setEditSuccess("");
        setEditingBranch(null);
        setEditingStoreId("");
        setEditingGoogleFolderId("");
    };

    const handleUpdate = async () => {
        setEditError("");
        setEditSuccess("");

        const normalizedStoreId = editingStoreId.trim();
        if (!editingBranch || !normalizedStoreId) {
            setEditError("Поле 'Внешний store / mapping' должно быть заполнено.");
            return;
        }

        try {
            const updated = await updateMappingBranch(editingBranch, {
                store_id: normalizedStoreId,
                google_folder_id: editingGoogleFolderId.trim() || null,
            });
            setEditSuccess("Изменения сохранены.");
            setEditingGoogleFolderId(updated.google_folder_id || "");
            setEditingStoreId(updated.store_id || "");
            await loadMappingViewList();
        } catch (error) {
            console.error("Error updating mapping branch:", error);
            setEditError("Ошибка при обновлении маппинга филиала.");
        }
    };

    return (
        <div style={pageStyle}>
            <div style={{ ...cardStyle, padding: "20px 24px" }}>
                <h1 style={{ margin: 0, fontSize: "28px", color: "#111827" }}>Маппинг филиалов</h1>
            </div>

            <div style={{ ...cardStyle, padding: "18px 20px" }}>
                <div style={{ display: "grid", gap: "12px" }}>
                    <h2 style={sectionTitleStyle}>Предприятие</h2>
                    <div style={{ maxWidth: "420px" }}>
                        <label style={labelStyle}>Выберите предприятие</label>
                        <select
                            value={selectedEnterpriseCode}
                            onChange={(e) => setSelectedEnterpriseCode(e.target.value)}
                            style={inputStyle}
                        >
                            <option value="">-- Выберите предприятие --</option>
                            {enterprises.map((enterprise) => (
                                <option key={enterprise.enterprise_code} value={enterprise.enterprise_code}>
                                    {enterprise.enterprise_name} ({enterprise.enterprise_code})
                                </option>
                            ))}
                        </select>
                    </div>
                </div>
            </div>

            <div
                style={{
                    display: "grid",
                    gridTemplateColumns: "minmax(320px, 420px) minmax(0, 1fr)",
                    gap: "20px",
                    alignItems: "start",
                }}
            >
                <div style={{ ...cardStyle, padding: "18px 20px" }}>
                    <div style={{ display: "grid", gap: "12px" }}>
                        <h2 style={sectionTitleStyle}>Добавить маппинг</h2>
                        <p style={mutedTextStyle}>
                            Форма сохранена без изменений логики записи. Новая запись будет добавлена для выбранного предприятия.
                        </p>

                        <div>
                            <label style={labelStyle}>Предприятие</label>
                            <input
                                type="text"
                                value={
                                    selectedEnterprise
                                        ? `${selectedEnterprise.enterprise_name} (${selectedEnterprise.enterprise_code})`
                                        : ""
                                }
                                readOnly
                                style={{ ...inputStyle, backgroundColor: "#f8fafc" }}
                            />
                        </div>

                        <div>
                            <label style={labelStyle}>Филиал</label>
                            <input
                                type="text"
                                placeholder="Например: 30555"
                                value={branch}
                                onChange={(e) => setBranch(e.target.value)}
                                style={inputStyle}
                            />
                        </div>

                        <div>
                            <label style={labelStyle}>Внешний store / mapping</label>
                            <input
                                type="text"
                                placeholder="Введите store_id"
                                value={storeId}
                                onChange={(e) => setStoreId(e.target.value)}
                                style={inputStyle}
                            />
                        </div>

                        <div>
                            <label style={labelStyle}>Google Folder ID</label>
                            <input
                                type="text"
                                placeholder="Введите Google Folder ID (необязательно)"
                                value={googleFolderId}
                                onChange={(e) => setGoogleFolderId(e.target.value)}
                                style={inputStyle}
                            />
                        </div>

                        {saveError ? <div style={{ color: "#b91c1c", fontWeight: 600 }}>{saveError}</div> : null}

                        <button
                            onClick={handleSave}
                            disabled={!branch || !storeId || !selectedEnterpriseCode}
                            style={{
                                ...primaryButtonStyle,
                                opacity: !branch || !storeId || !selectedEnterpriseCode ? 0.6 : 1,
                                cursor: !branch || !storeId || !selectedEnterpriseCode ? "not-allowed" : "pointer",
                            }}
                        >
                            Записать
                        </button>
                    </div>
                </div>

                <div style={{ display: "grid", gap: "20px" }}>
                    <div style={{ ...cardStyle, padding: "18px 20px" }}>
                    <div style={{ display: "grid", gap: "12px" }}>
                        <h2 style={sectionTitleStyle}>Информация по предприятию</h2>
                        {selectedEnterprise ? (
                            <div
                                style={{
                                    display: "grid",
                                    gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))",
                                    gap: "12px",
                                }}
                            >
                                <EnterpriseInfoItem label="Название" value={selectedEnterprise.enterprise_name} />
                                <EnterpriseInfoItem label="Код предприятия" value={selectedEnterprise.enterprise_code} />
                                <EnterpriseInfoItem label="Формат" value={selectedEnterprise.data_format || emptyValue} />
                                <EnterpriseInfoItem
                                    label="Последняя загрузка каталога"
                                    value={formatDate(selectedEnterprise.last_catalog_upload)}
                                />
                                <EnterpriseInfoItem
                                    label="Последняя загрузка остатков"
                                    value={formatDate(selectedEnterprise.last_stock_upload)}
                                />
                            </div>
                        ) : (
                            <p style={mutedTextStyle}>Выберите предприятие, чтобы увидеть краткую информацию.</p>
                        )}
                    </div>
                    </div>

                    <div style={{ ...cardStyle, padding: "18px 20px" }}>
                        <div style={{ display: "grid", gap: "12px" }}>
                            <h2 style={sectionTitleStyle}>Редактировать маппинг</h2>
                            {!editingMapping ? (
                                <p style={mutedTextStyle}>
                                    Выберите строку в списке ниже и нажмите «Редактировать».
                                </p>
                            ) : (
                                <>
                                    <div
                                        style={{
                                            display: "grid",
                                            gridTemplateColumns: "repeat(auto-fit, minmax(220px, 1fr))",
                                            gap: "12px",
                                        }}
                                    >
                                        <EnterpriseInfoItem
                                            label="Предприятие"
                                            value={editingMapping.enterprise_display_label}
                                        />
                                        <EnterpriseInfoItem label="Филиал" value={editingMapping.branch} />
                                    </div>

                                    <div>
                                        <label style={labelStyle}>{editingMapping.semantic_store_label}</label>
                                        <input
                                            type="text"
                                            value={editingStoreId}
                                            onChange={(e) => setEditingStoreId(e.target.value)}
                                            style={inputStyle}
                                        />
                                    </div>

                                    <div>
                                        <label style={labelStyle}>Google Folder ID</label>
                                        <input
                                            type="text"
                                            value={editingGoogleFolderId}
                                            onChange={(e) => setEditingGoogleFolderId(e.target.value)}
                                            style={inputStyle}
                                        />
                                    </div>

                                    <p style={mutedTextStyle}>
                                        Поля «Филиал» и предприятие не редактируются на этом шаге.
                                    </p>

                                    {editError ? <div style={{ color: "#b91c1c", fontWeight: 600 }}>{editError}</div> : null}
                                    {editSuccess ? <div style={{ color: "#166534", fontWeight: 600 }}>{editSuccess}</div> : null}

                                    <div style={{ display: "flex", gap: "10px" }}>
                                        <button
                                            onClick={handleUpdate}
                                            style={{ ...primaryButtonStyle, width: "auto", padding: "12px 18px" }}
                                        >
                                            Сохранить
                                        </button>
                                        <button
                                            onClick={cancelEdit}
                                            style={{
                                                width: "auto",
                                                padding: "12px 18px",
                                                borderRadius: "8px",
                                                border: "1px solid #cbd5e1",
                                                backgroundColor: "#ffffff",
                                                cursor: "pointer",
                                                fontWeight: 600,
                                            }}
                                        >
                                            Отмена
                                        </button>
                                    </div>
                                </>
                            )}
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
                                <h2 style={sectionTitleStyle}>Список маппинга</h2>
                                <p style={mutedTextStyle}>
                                    Показаны только записи для выбранного предприятия.
                                </p>
                            </div>
                            <button
                                onClick={loadMappingViewList}
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

                        {listLoading ? (
                            <div style={{ padding: "20px", ...mutedTextStyle }}>Загрузка списка…</div>
                        ) : listError ? (
                            <div style={{ padding: "20px", color: "#b91c1c", fontWeight: 600 }}>{listError}</div>
                        ) : !selectedEnterpriseCode ? (
                            <div style={{ padding: "20px", ...mutedTextStyle }}>Сначала выберите предприятие.</div>
                        ) : filteredMappings.length === 0 ? (
                            <div style={{ padding: "20px", ...mutedTextStyle }}>
                                Для выбранного предприятия записи маппинга пока не найдены.
                            </div>
                        ) : (
                            <div style={{ display: "grid", gap: "12px", padding: "16px" }}>
                                {filteredMappings.map((item) => (
                                    <div
                                        key={item.mapping_key}
                                        style={{
                                            display: "grid",
                                            gap: "12px",
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
                                            <div style={{ display: "grid", gap: "4px" }}>
                                                <div style={{ fontWeight: 700, color: "#111827" }}>
                                                    Филиал: {item.branch}
                                                </div>
                                                <div style={{ ...mutedTextStyle, fontSize: "12px" }}>
                                                    {item.enterprise_display_label}
                                                </div>
                                            </div>
                                            <button
                                                onClick={() => startEdit(item)}
                                                style={{
                                                    padding: "8px 12px",
                                                    borderRadius: "8px",
                                                    border: "1px solid #cbd5e1",
                                                    backgroundColor: "#ffffff",
                                                    cursor: "pointer",
                                                    fontWeight: 600,
                                                }}
                                            >
                                                Редактировать
                                            </button>
                                        </div>

                                        <div
                                            style={{
                                                display: "grid",
                                                gridTemplateColumns: "repeat(auto-fit, minmax(180px, 1fr))",
                                                gap: "12px",
                                            }}
                                        >
                                            <div style={{ display: "grid", gap: "4px" }}>
                                                <div style={{ ...mutedTextStyle, fontSize: "12px" }}>
                                                    Внешний store / mapping
                                                </div>
                                                <div style={{ color: "#111827", fontWeight: 600 }}>
                                                    {item.store_mapping_value || emptyValue}
                                                </div>
                                                <div style={{ ...mutedTextStyle, fontSize: "12px" }}>
                                                    {item.semantic_store_label}
                                                </div>
                                            </div>

                                            <div style={{ display: "grid", gap: "4px" }}>
                                                <div style={{ ...mutedTextStyle, fontSize: "12px" }}>
                                                    Google Folder ID
                                                </div>
                                                <div style={{ color: "#111827", wordBreak: "break-word" }}>
                                                    {item.google_folder_id || emptyValue}
                                                </div>
                                            </div>

                                            <div style={{ display: "grid", gap: "8px", alignContent: "start" }}>
                                                <div style={{ ...mutedTextStyle, fontSize: "12px" }}>
                                                    Telegram
                                                </div>
                                                <div>
                                                    <span
                                                        style={
                                                            item.has_telegram_target
                                                                ? badgeStyle
                                                                : { ...badgeStyle, backgroundColor: "#f1f5f9" }
                                                        }
                                                    >
                                                        {item.has_telegram_target ? "Есть" : "Нет"}
                                                    </span>
                                                </div>
                                            </div>

                                            <div style={{ display: "grid", gap: "8px", alignContent: "start" }}>
                                                <div style={{ ...mutedTextStyle, fontSize: "12px" }}>Статус</div>
                                                <div>
                                                    {item.conflict_flags.length > 0 ? (
                                                        <span style={warningBadgeStyle}>Конфликт</span>
                                                    ) : (
                                                        <span style={{ ...badgeStyle, backgroundColor: "#ecfdf5", color: "#166534" }}>
                                                            Ок
                                                        </span>
                                                    )}
                                                </div>
                                            </div>
                                        </div>
                                    </div>
                                ))}
                            </div>
                        )}
                    </div>
                </div>
            </div>
        </div>
    );
};

export default MappingBranchPage;
