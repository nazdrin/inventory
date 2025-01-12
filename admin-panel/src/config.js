// Импорт значений из переменных окружения
const API_BASE_URL = process.env.REACT_APP_API_BASE_URL || "http://127.0.0.1:8000/developer_panel";
const DATABASE_URL = process.env.DATABASE_URL || "postgresql+asyncpg://postgres:your_password@localhost/inventory_db";

// Экспорт переменных для использования в других модулях
export { API_BASE_URL, DATABASE_URL };

// Логирование для проверки корректности переменных окружения
if (process.env.NODE_ENV === "development") {
    console.log("Running in Development Mode");
    console.log(`API Base URL: ${API_BASE_URL}`);
    console.log(`Database URL: ${DATABASE_URL}`);
}