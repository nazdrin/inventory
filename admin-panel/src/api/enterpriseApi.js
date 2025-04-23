import axios from "axios";
import { API_BASE_URL } from "../config"; // Импортируем базовый URL из config.js

// Функция для получения токена из localStorage
const getAuthToken = () => localStorage.getItem("token");

// Конфиг для axios с заголовком Authorization
const getAuthHeaders = () => {
    const token = localStorage.getItem("token");

    if (!token) {
        window.location.href = "/login"; // Перенаправляем на страницу логина, если токена нет
    }

    return {
        headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${token}`,
        }
    };
};

// Получить все предприятия (защищённый эндпоинт)
export const getEnterprises = async () => {
    try {
        const response = await axios.get(`${API_BASE_URL}/enterprise/settings/`, getAuthHeaders());
        return response.data;
    } catch (error) {
        console.error("Error fetching enterprises:", error);
        throw error;
    }
};

// Получить предприятие по коду (защищённый эндпоинт)
export const getEnterpriseByCode = async (enterpriseCode) => {
    try {
        const response = await axios.get(`${API_BASE_URL}/enterprise/settings/${enterpriseCode}`, getAuthHeaders());
        return response.data;
    } catch (error) {
        console.error("Error fetching enterprise by code:", error);
        throw error;
    }
};

// Создать новое предприятие (защищённый эндпоинт)
export const createEnterprise = async (enterpriseData) => {
    try {
        const response = await axios.post(`${API_BASE_URL}/enterprise/settings/`, enterpriseData, getAuthHeaders());
        return response.data;
    } catch (error) {
        console.error("Error creating enterprise:", error);
        throw error;
    }
};

// Обновить данные предприятия (защищённый эндпоинт)
export const updateEnterprise = async (enterpriseCode, enterpriseData) => {
    try {
        const response = await axios.put(`${API_BASE_URL}/enterprise/settings/${enterpriseCode}`, enterpriseData, getAuthHeaders());
        return response.data;
    } catch (error) {
        console.error("Error updating enterprise:", error);
        throw error;
    }
};

// Получение списка branch по enterprise_code (открытый эндпоинт)
export const getMappingBranches = async (enterpriseCode) => {
    try {
        const response = await axios.get(`${API_BASE_URL}/mapping_branch/${enterpriseCode}`);
        return response.data;
    } catch (error) {
        console.error("Error fetching mapping branches:", error);
        throw error;
    }
};

// Создание новой записи в mapping_branch (открытый эндпоинт)
export const createMappingBranch = async (mappingData) => {
    try {
        const response = await axios.post(`${API_BASE_URL}/mapping_branch/`, mappingData);
        return response.data;
    } catch (error) {
        console.error("Error creating mapping branch:", error);
        throw error;
    }
};

// Функция выхода (очистка токена)
export const logout = () => {
    localStorage.removeItem("token");
    window.location.reload(); // Перезагружаем страницу
};