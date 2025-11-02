import React, { useEffect, useState } from "react";
import {
	getDropshipEnterprises,
	createDropshipEnterprise,
	updateDropshipEnterprise
} from "../api/dropshipEnterpriseApi";
import Form from "../components/Form";

const DropshipEnterprisePanel = () => {
	const [items, setItems] = useState([]);
	const [activeOnly, setActiveOnly] = useState(false);
	const [filtered, setFiltered] = useState([]);
	const [selected, setSelected] = useState(null);
	const [originalCode, setOriginalCode] = useState(null);
	const [isEditing, setIsEditing] = useState(false);

	// load list
	useEffect(() => {
		const fetchItems = async () => {
			try {
				const data = await getDropshipEnterprises();
				setItems(data);
			} catch (e) {
				console.error("Error loading dropship enterprises:", e);
			}
		};
		fetchItems();
	}, []);

	// filter
	useEffect(() => {
		const f = activeOnly ? items.filter(x => x.is_active) : items;
		setFiltered(f);
	}, [items, activeOnly]);

	const handleSave = async (data) => {
		try {
			if (isEditing) {
				await updateDropshipEnterprise(originalCode, data);
			} else {
				await createDropshipEnterprise(data);
			}
			const refreshed = await getDropshipEnterprises();
			setItems(refreshed);

			setSelected(null);
			setOriginalCode(null);
			setIsEditing(false);
		} catch (e) {
			console.error("Error saving dropship enterprise:", e);
		}
	};

	const handleCancel = () => {
		setSelected(null);
		setOriginalCode(null);
		setIsEditing(false);
	};

	const fields = [
		{ name: "code", label: "Code (Унікальний код)", type: "text" },
		{ name: "name", label: "Name (Назва)", type: "text" },
		{ name: "city", label: "City (Місто)", type: "text" },

		{ name: "feed_url", label: "Feed URL (Посилання на фід)", type: "text" },
		{ name: "gdrive_folder", label: "Google Drive Folder (Папка на GDrive)", type: "text" },

		{ name: "is_rrp", label: "Є РРЦ", type: "checkbox" },
		{ name: "is_wholesale", label: "Опт (інакше роздріб)", type: "checkbox" },

		{ name: "profit_percent", label: "Процент заробітку", type: "number", step: "0.01" },
		{ name: "retail_markup", label: "Націнка для роздрібу", type: "number", step: "0.01" },
		{ name: "min_markup_threshold", label: "Мін. поріг націнки", type: "number", step: "0.01" },

		{ name: "is_active", label: "Активний", type: "checkbox" },
		{ name: "api_orders_enabled", label: "Замовлення через API", type: "checkbox" },
		{ name: "priority", label: "Пріоритет (1–10)", type: "number", min: 1, max: 10, step: 1 },
		{ name: "weekend_work", label: "Працює у вихідні", type: "checkbox" },
		{ name: "use_feed_instead_of_gdrive", label: "Використовувати фід замість GDrive", type: "checkbox" },
	];

	return (
		<div>
			<div style={{
				position: "sticky", top: 0, backgroundColor: "#f0f0f0", zIndex: 10,
				padding: "10px 20px", display: "flex", justifyContent: "space-between",
				alignItems: "center", borderBottom: "1px solid #ccc"
			}}>
				<h1>Dropship Enterprises</h1>
				{selected && (
					<div>
						<button
							style={{
								padding: "10px 20px", marginRight: "10px", backgroundColor: "green",
								color: "white", border: "none", cursor: "pointer", borderRadius: "5px"
							}}
							onClick={() => handleSave(selected)}
						>
							Save
						</button>
						<button
							style={{
								padding: "10px 20px", backgroundColor: "red",
								color: "white", border: "none", cursor: "pointer", borderRadius: "5px"
							}}
							onClick={handleCancel}
						>
							Cancel
						</button>
					</div>
				)}
			</div>

			{!selected && (
				<div style={{ display: "flex", gap: 20, alignItems: "center", margin: "10px 20px" }}>
					<label><input type="checkbox" checked={activeOnly} onChange={e => setActiveOnly(e.target.checked)} /> Показать только активных</label>
					<label htmlFor="dropship-select">Select:</label>
					<select
						id="dropship-select"
						onChange={(e) => {
							const code = e.target.value;
							const found = items.find(x => x.code === code);
							setSelected(found || null);
							setOriginalCode(found?.code || null);
							setIsEditing(!!found);
						}}
						value={selected?.code || ""}
						style={{ padding: "15px", width: "330px", height: "50px", fontSize: "19px" }}
					>
						<option value="">-- Select --</option>
						{items.map(x => (
							<option key={x.code} value={x.code}>
								{x.name} ({x.code})
							</option>
						))}
					</select>
				</div>
			)}

			{!selected && (
				<div style={{ marginTop: 15, marginLeft: 15, padding: 10, maxWidth: 400 }}>
					<h3 style={{ marginBottom: 10, fontSize: 18 }}>Активні dropship-постачальники</h3>
					<ul style={{ listStyleType: "none", padding: 0 }}>
						{filtered.map(x => (
							<li key={x.code} style={{ padding: 8, borderBottom: "1px solid #ccc", fontSize: 16 }}>
								<strong>{x.name}</strong> <span style={{ color: "#555" }}>({x.code})</span>
							</li>
						))}
					</ul>
				</div>
			)}

			{selected && (
				<Form
					fields={fields}
					values={selected}
					onChange={setSelected}
					onSubmit={() => handleSave(selected)}
					onCancel={handleCancel}
					style={{ display: "grid", gap: "20px", maxWidth: "600px", margin: "0 auto" }}
				/>
			)}

			{!selected && (
				<button
					onClick={() => {
						setSelected({
							priority: 5,
							is_active: true,
							is_wholesale: true,
							use_feed_instead_of_gdrive: true,
						});
						setOriginalCode(null);
						setIsEditing(false);
					}}
					style={{
						padding: "10px 20px", marginTop: "20px", border: 'none',
						borderRadius: '5px', fontWeight: 'bold', backgroundColor: '#ffc107',
						display: "block", marginLeft: "auto", marginRight: "auto"
					}}
				>
					Add New
				</button>
			)}
		</div>
	);
};

export default DropshipEnterprisePanel;