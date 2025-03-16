import axios from "axios";
import { API_BASE_URL } from "../config";

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

// ✅ Создание новой записи в mapping_branch (с авторизацией)
export const createMappingBranch = async (mappingData) => {
    try {
        const response = await axios.post(`${API_BASE_URL}/mapping_branch/`, mappingData, getAuthHeaders());
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
