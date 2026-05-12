import axios from "axios";
import { API_BASE_URL } from "../config"; // Импортируем базовый URL из config.js

const API_URL = `${API_BASE_URL}/developer/settings`;
const DATA_FORMATS_URL = `${API_BASE_URL}/data_formats`;

const clearStoredAuth = () => {
    localStorage.removeItem("token");
    localStorage.removeItem("user_login");
};

const redirectToLogin = () => {
    clearStoredAuth();
    window.location.href = "/";
};

const decodeJwtPayload = (token) => {
    try {
        const [, payload] = token.split(".");
        if (!payload) {
            return null;
        }

        const normalized = payload.replace(/-/g, "+").replace(/_/g, "/");
        const padded = normalized.padEnd(normalized.length + ((4 - normalized.length % 4) % 4), "=");
        return JSON.parse(atob(padded));
    } catch (error) {
        return null;
    }
};

const getStoredLogin = () => {
    const token = localStorage.getItem("token");
    const payload = token ? decodeJwtPayload(token) : null;
    const loginFromToken = payload?.sub;
    const loginFromStorage = localStorage.getItem("user_login");
    const login = loginFromToken || loginFromStorage;

    if (loginFromToken && loginFromStorage !== loginFromToken) {
        localStorage.setItem("user_login", loginFromToken);
    }

    return login;
};

const handleAuthError = (error) => {
    const status = error?.response?.status;
    if (status === 401 || status === 403) {
        redirectToLogin();
    }
};

// 🔹 Функция для получения токена
const getAuthToken = () => {
    const token = localStorage.getItem("token");
    if (!token) {
        console.error("Ошибка: Токен отсутствует");
        redirectToLogin();
        return null;
    }
    return token;
};

// 🔹 Конфиг для axios с заголовком Authorization
const getAuthHeaders = () => {
    const token = getAuthToken();
    return token
        ? {
            headers: {
                "Content-Type": "application/json",
                Authorization: `Bearer ${token}`,
            },
        }
        : {};
};

export { getAuthHeaders, handleAuthError, redirectToLogin, getAuthToken };

// 🔹 Получение настроек разработчика по логину
const getSetting = async () => {
    const login = getStoredLogin();
    if (!login) {
        console.error("Ошибка: Логин пользователя не найден в localStorage");
        redirectToLogin();
        return null;
    }

    try {
        console.log(`📌 Запрашиваем настройки для: ${login}`);
        const response = await axios.get(`${API_URL}/${login}`, getAuthHeaders());
        console.log("✅ Полученные настройки:", response.data);
        return response.data;
    } catch (error) {
        handleAuthError(error);
        console.error("❌ Ошибка получения настроек:", error);
        throw error;
    }
};

// 🔹 Обновление настроек разработчика
const updateSetting = async (data) => {
    const login = getStoredLogin();
    if (!login) {
        console.error("Ошибка: Логин отсутствует");
        redirectToLogin();
        return;
    }

    if (!data || Object.keys(data).length === 0) {
        console.error("🚨 Ошибка: Передаваемые данные пустые!", data);
        return;
    }

    console.log("📡 Отправка данных на сервер:", JSON.stringify(data, null, 2));

    try {
        const response = await axios.put(`${API_URL}/${login}`, data, getAuthHeaders());
        console.log("✅ Ответ сервера:", response.data);
        return response.data;
    } catch (error) {
        handleAuthError(error);
        console.error(`❌ Ошибка при обновлении настроек для ${login}:`, error);
        throw error;
    }
};

// 🔹 Получение списка всех форматов данных
const getDataFormats = async () => {
    try {
        console.log("📌 Запрашиваем форматы данных...");
        const response = await axios.get(DATA_FORMATS_URL, getAuthHeaders());
        console.log("✅ Получены форматы данных:", response.data);
        return response.data;
    } catch (error) {
        handleAuthError(error);
        console.error("❌ Ошибка загрузки форматов данных:", error);
        throw error;
    }
};

// 🔹 Добавление нового формата данных
const addDataFormat = async (newFormat) => {
    const data = typeof newFormat === "string" ? { format_name: newFormat } : newFormat;
    try {
        console.log("📌 Добавляем новый формат:", JSON.stringify(data, null, 2));
        const response = await axios.post(DATA_FORMATS_URL, data, getAuthHeaders());
        console.log("✅ Формат добавлен:", response.data);
        return response.data;
    } catch (error) {
        handleAuthError(error);
        console.error("❌ Ошибка добавления формата данных:", error);
        throw error;
    }
};

// 🔹 Выход из системы (удаление токена и логина)
const logout = () => {
    console.warn("🚪 Выход из системы...");
    redirectToLogin();
};

// 🔹 Экспортируем API-функции
export default {
    getSetting,
    updateSetting,
    getDataFormats,
    addDataFormat,
    logout
};
