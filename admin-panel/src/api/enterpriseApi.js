import axios from 'axios';

const API_BASE_URL = 'http://127.0.0.1:8000/developer_panel';

// Получить все предприятия
export const getEnterprises = async () => {
  try {
    const response = await axios.get(`${API_BASE_URL}/enterprise/settings/`);
    return response.data;
  } catch (error) {
    console.error('Error fetching enterprises:', error);
    throw error;
  }
};

// Получить предприятие по коду
export const getEnterpriseByCode = async (enterpriseCode) => {
  try {
    const response = await axios.get(`${API_BASE_URL}/enterprise/settings/${enterpriseCode}`);
    return response.data;
  } catch (error) {
    console.error('Error fetching enterprise by code:', error);
    throw error;
  }
};

// Создать новое предприятие
export const createEnterprise = async (enterpriseData) => {
  try {
    const response = await axios.post(`${API_BASE_URL}/enterprise/settings/`, enterpriseData);
    return response.data;
  } catch (error) {
    console.error('Error creating enterprise:', error);
    throw error;
  }
};

// Обновить данные предприятия
export const updateEnterprise = async (enterpriseCode, enterpriseData) => {
  console.log("Sending PUT request to update enterprise with code:", enterpriseCode); // Лог
  const response = await fetch(
    `http://127.0.0.1:8000/developer_panel/enterprise/settings/${enterpriseCode}`,
    {
      method: "PUT",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(enterpriseData),
    }
  );

  if (!response.ok) {
    const errorData = await response.json();
    console.error("Error updating enterprise:", errorData); // Логируем ошибки
    throw new Error(errorData.detail || "Failed to update enterprise.");
  }

  return await response.json();
};