import axios from "axios";
import { API_BASE_URL } from "../config"; // Импортируем базовый URL из config.js

const API_URL = `${API_BASE_URL}/developer/settings`;
const DATA_FORMATS_URL = `${API_BASE_URL}/data_formats`;

// 🔹 Функция для получения токена
const getAuthToken = () => {
    const token = localStorage.getItem("token");
    if (!token) {
        console.error("Ошибка: Токен отсутствует");
        window.location.href = "/";
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

// 🔹 Получение настроек разработчика по логину
const getSetting = async () => {
    const login = localStorage.getItem("user_login"); // Берем логин из localStorage
    if (!login) {
        console.error("Ошибка: Логин пользователя не найден в localStorage");
        window.location.href = "/";
        return null;
    }

    try {
        console.log(`📌 Запрашиваем настройки для: ${login}`);
        const response = await axios.get(`${API_URL}/${login}`, getAuthHeaders());
        console.log("✅ Полученные настройки:", response.data);
        return response.data;
    } catch (error) {
        console.error("❌ Ошибка получения настроек:", error);
        throw error;
    }
};

// 🔹 Обновление настроек разработчика
const updateSetting = async (data) => {
    const login = localStorage.getItem("user_login");
    if (!login) {
        console.error("Ошибка: Логин отсутствует");
        window.location.href = "/";
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
        console.error("❌ Ошибка добавления формата данных:", error);
        throw error;
    }
};

// 🔹 Выход из системы (удаление токена и логина)
const logout = () => {
    console.warn("🚪 Выход из системы...");
    localStorage.removeItem("token");
    localStorage.removeItem("user_login");
    window.location.href = "/"; // Перенаправляем на страницу логина
};

// 🔹 Экспортируем API-функции
export default {
    getSetting,
    updateSetting,
    getDataFormats,
    addDataFormat,
    logout
};
