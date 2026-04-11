import axios from "axios";
import { API_BASE_URL } from "../config";

// Конфиг для axios с заголовком Authorization
const getAuthHeaders = () => {
    const token = localStorage.getItem("token");

    if (!token) {
        window.location.href = "/"; // Перенаправляем на страницу логина, если токена нет
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

export const getMappingBranchViewList = async () => {
    try {
        const response = await axios.get(`${API_BASE_URL}/mapping_branch/view/`, getAuthHeaders());
        return response.data;
    } catch (error) {
        console.error("Error fetching mapping branch view list:", error);
        throw error;
    }
};

export const getMappingBranchViewDetail = async (branch) => {
    try {
        const response = await axios.get(`${API_BASE_URL}/mapping_branch/view/${branch}`, getAuthHeaders());
        return response.data;
    } catch (error) {
        console.error("Error fetching mapping branch detail:", error);
        throw error;
    }
};

export const updateMappingBranch = async (branch, payload) => {
    try {
        const response = await axios.put(`${API_BASE_URL}/mapping_branch/${branch}`, payload, getAuthHeaders());
        return response.data;
    } catch (error) {
        console.error("Error updating mapping branch:", error);
        throw error;
    }
};

// Функция выхода (очистка токена)
export const logout = () => {
    localStorage.removeItem("token");
    window.location.reload(); // Перезагружаем страницу
};
