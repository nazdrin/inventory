const API_URL = "http://164.92.213.254:8000/developer_panel/developer/settings";
const DATA_FORMATS_URL = "http://164.92.213.254:8000/developer_panel/data_formats";



// Fetch developer settings by login
const getSetting = async (login) => {
  const response = await fetch(`${API_URL}/${login}`);
  if (!response.ok) {
    throw new Error("Failed to fetch settings.");
  }
  return response.json();
};

// Update developer settings by login
const updateSetting = async (login, data) => {
  const response = await fetch(`${API_URL}/${login}`, {
    method: "PUT",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(data),
  });
  if (!response.ok) {
    throw new Error(`Failed to update developer setting with login: ${login}.`);
  }
  return response.json();
};

// Fetch list of data formats
const getDataFormats = async () => {
  const response = await fetch(DATA_FORMATS_URL, {
    method: "GET",
  });
  if (!response.ok) {
    throw new Error("Failed to fetch data formats.");
  }
  return response.json();
};

// Add a new data format
// const addDataFormat = async (newFormat) => {
//   const response = await fetch(DATA_FORMATS_URL, {
//     method: "POST",
//     headers: {
//       "Content-Type": "application/json",
//     },
//     body: JSON.stringify({ format_name: newFormat }), // Обернули в объект с ключом format_name
//   });
//   if (!response.ok) {
//     throw new Error("Failed to add data format.");
//   }
//   return response.json();
// };
const addDataFormat = async (newFormat) => {
  const data = typeof newFormat === "string" ? { format_name: newFormat } : newFormat;
  const response = await fetch(DATA_FORMATS_URL, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(data), // Теперь data всегда в правильном формате
  });
  if (!response.ok) {
    throw new Error("Failed to add data format.");
  }
  return response.json();
};
// Export all functions
export default {
  getSetting,
  updateSetting,
  getDataFormats, // Добавляем функцию getDataFormats
  addDataFormat,  // Добавляем функцию addDataFormat
};