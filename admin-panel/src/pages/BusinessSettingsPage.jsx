import React, { useEffect, useState } from "react";
import { getBusinessSettingsView } from "../api/businessSettingsApi";

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
};

const readonlyBadgeStyle = {
    ...badgeBaseStyle,
    backgroundColor: "#f8fafc",
    color: "#475569",
};

const statusStyleByResolution = {
    resolved: {
        backgroundColor: "#ecfdf5",
        borderColor: "#bbf7d0",
        color: "#166534",
    },
    ambiguous: {
        backgroundColor: "#fff7ed",
        borderColor: "#fed7aa",
        color: "#9a3412",
    },
    none: {
        backgroundColor: "#fef2f2",
        borderColor: "#fecaca",
        color: "#991b1b",
    },
};

const emptyValue = "—";

const sourceBadgeBySource = {
    env: {
        backgroundColor: "#eff6ff",
        color: "#1d4ed8",
    },
    EnterpriseSettings: {
        backgroundColor: "#eef2ff",
        color: "#4338ca",
    },
    computed: {
        backgroundColor: "#f8fafc",
        color: "#475569",
    },
    transitional: {
        backgroundColor: "#fff7ed",
        color: "#9a3412",
    },
    "secret-hidden": {
        backgroundColor: "#fef2f2",
        color: "#b91c1c",
    },
};

const formatValue = (value) => {
    if (value === null || value === undefined || value === "") {
        return emptyValue;
    }

    if (typeof value === "boolean") {
        return value ? "Да" : "Нет";
    }

    return String(value);
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

const SectionItem = ({ item }) => (
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
                <span style={{ ...sourceBadgeStyle, ...(sourceBadgeBySource[item.source] || {}) }}>{item.source}</span>
                {item.readonly && <span style={readonlyBadgeStyle}>read-only</span>}
            </div>
        </div>
        <div style={{ fontSize: "15px", color: "#0f172a", lineHeight: 1.5 }}>{formatValue(item.value)}</div>
        {item.help_text && <div style={mutedTextStyle}>{item.help_text}</div>}
    </div>
);

const SectionCard = ({ section }) => {
    const groups = buildItemGroups(section.items);

    return (
        <div style={{ ...cardStyle, padding: "20px 24px" }}>
            <div style={{ display: "grid", gap: "8px", marginBottom: "16px" }}>
                <h2 style={sectionTitleStyle}>{section.title}</h2>
                {section.description && <p style={mutedTextStyle}>{section.description}</p>}
            </div>
            <div style={{ display: "grid", gap: "18px" }}>
                {groups.map((group) => (
                    <div key={group.key} style={{ display: "grid", gap: "12px" }}>
                        {group.title && (
                            <div
                                style={{
                                    fontSize: "13px",
                                    fontWeight: 700,
                                    color: "#475569",
                                    textTransform: "uppercase",
                                    letterSpacing: "0.04em",
                                }}
                            >
                                {group.title}
                            </div>
                        )}
                        <div style={{ display: "grid", gap: "12px" }}>
                            {group.items.map((item) => (
                                <SectionItem key={item.key} item={item} />
                            ))}
                        </div>
                    </div>
                ))}
            </div>
        </div>
    );
};

const BusinessSettingsPage = () => {
    const [viewModel, setViewModel] = useState(null);
    const [loading, setLoading] = useState(true);
    const [error, setError] = useState("");

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

    const resolutionStyle = statusStyleByResolution[viewModel?.resolution_status] || statusStyleByResolution.none;

    return (
        <div style={pageStyle}>
            <div style={{ ...cardStyle, padding: "20px 24px", display: "grid", gap: "10px" }}>
                <h1 style={{ margin: 0, fontSize: "28px", color: "#111827" }}>Business Settings</h1>
                <p style={mutedTextStyle}>
                    Отдельный control-plane snapshot для Business contour. Первая версия страницы остаётся в read-only режиме.
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
                    <div
                        style={{
                            ...cardStyle,
                            padding: "20px 24px",
                            backgroundColor: resolutionStyle.backgroundColor,
                            borderColor: resolutionStyle.borderColor,
                            display: "grid",
                            gap: "10px",
                        }}
                    >
                        <div style={{ display: "flex", gap: "8px", alignItems: "center", flexWrap: "wrap" }}>
                            <span style={{ ...badgeBaseStyle, backgroundColor: "#ffffff", color: resolutionStyle.color }}>
                                {viewModel.resolution_status}
                            </span>
                            {!viewModel.writable_supported && (
                                <span style={{ ...readonlyBadgeStyle, backgroundColor: "#ffffff" }}>
                                    orchestration write deferred
                                </span>
                            )}
                        </div>
                        <p style={{ ...mutedTextStyle, color: resolutionStyle.color }}>{viewModel.resolution_message}</p>
                        {viewModel.deferred_write_reason && (
                            <p style={mutedTextStyle}>{viewModel.deferred_write_reason}</p>
                        )}
                        {Array.isArray(viewModel.business_candidates) && viewModel.business_candidates.length > 1 && (
                            <div style={{ display: "grid", gap: "8px" }}>
                                <div style={{ fontWeight: 700, color: "#111827" }}>Найденные Business enterprise</div>
                                <div style={{ display: "flex", gap: "8px", flexWrap: "wrap" }}>
                                    {viewModel.business_candidates.map((candidate) => (
                                        <span key={candidate.enterprise_code} style={sourceBadgeStyle}>
                                            {candidate.enterprise_name} ({candidate.enterprise_code})
                                        </span>
                                    ))}
                                </div>
                            </div>
                        )}
                        {Array.isArray(viewModel.planned_writable_keys) && viewModel.planned_writable_keys.length > 0 && (
                            <div style={{ display: "grid", gap: "8px" }}>
                                <div style={{ fontWeight: 700, color: "#111827" }}>Planned writable scope</div>
                                <div style={{ display: "flex", gap: "8px", flexWrap: "wrap" }}>
                                    {viewModel.planned_writable_keys.map((key) => (
                                        <span key={key} style={readonlyBadgeStyle}>
                                            {key}
                                        </span>
                                    ))}
                                </div>
                            </div>
                        )}
                    </div>

                    {viewModel.sections.map((section) => (
                        <SectionCard key={section.key} section={section} />
                    ))}
                </>
            )}
        </div>
    );
};

export default BusinessSettingsPage;
