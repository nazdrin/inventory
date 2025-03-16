import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
import axios from "axios";
import { API_BASE_URL } from "../config"; // Импортируем базовый URL из config.js

const Login = ({ setAuthUser }) => {
    const [login, setLogin] = useState("");
    const [password, setPassword] = useState("");
    const [error, setError] = useState("");
    const navigate = useNavigate();

    const handleLogin = async (e) => {
        e.preventDefault();
        console.log("Отправка данных на сервер:", { developer_login: login, developer_password: password });

        try {
            const response = await axios.post(`${API_BASE_URL}/login/`, {
                developer_login: login,
                developer_password: password
            });

            console.log("Успешный вход:", response.data);

            // Сохранение токена и логина в localStorage
            localStorage.setItem("token", response.data.access_token);
            localStorage.setItem("user_login", login); // 🔹 Теперь логин сохраняется!

            // Устанавливаем пользователя в состояние
            setAuthUser(response.data);

            // Перенаправление после входа
            navigate("/developer");

        } catch (err) {
            console.error("Ошибка запроса:", err);

            // Обрабатываем ошибки
            if (err.response) {
                if (err.response.status === 401) {
                    setError("Неверный логин или пароль.");
                } else {
                    setError(err.response.data.detail || "Произошла ошибка. Попробуйте позже.");
                }
            } else {
                setError("Не удалось подключиться к серверу.");
            }
        }
    };
    return (
        <div style={{
            display: 'flex',
            justifyContent: 'center',
            alignItems: 'center',
            height: '100vh',
            backgroundColor: '#f8f9fa'
        }}>
            <div style={{
                padding: '30px',
                backgroundColor: 'white',
                borderRadius: '8px',
                boxShadow: '0px 0px 10px rgba(0, 0, 0, 0.1)',
                border: '1px solid #ddd',
                width: '400px',
                textAlign: 'center'
            }}>
                <h2 style={{ marginBottom: '20px' }}>Вход в систему</h2>

                {error && <p style={{ color: "red", marginBottom: "10px" }}>{error}</p>}

                <form onSubmit={handleLogin}>
                    {/* Поле логина */}
                    <label style={{ display: 'block', textAlign: 'left', marginBottom: '5px' }}>Логин:</label>
                    <input
                        type="text"
                        value={login}
                        onChange={(e) => setLogin(e.target.value)}
                        placeholder="Введите логин"
                        required
                        style={{
                            width: '100%',
                            padding: '10px',
                            marginBottom: '15px',
                            borderRadius: '5px',
                            border: '1px solid #ccc'
                        }}
                    />

                    {/* Поле пароля */}
                    <label style={{ display: 'block', textAlign: 'left', marginBottom: '5px' }}>Пароль:</label>
                    <input
                        type="password"
                        value={password}
                        onChange={(e) => setPassword(e.target.value)}
                        placeholder="Введите пароль"
                        required
                        style={{
                            width: '100%',
                            padding: '10px',
                            marginBottom: '15px',
                            borderRadius: '5px',
                            border: '1px solid #ccc'
                        }}
                    />

                    {/* Кнопка Войти */}
                    <button
                        type="submit"
                        style={{
                            width: '100%',
                            padding: '10px',
                            backgroundColor: '#ffc107',
                            color: 'black',
                            border: 'none',
                            borderRadius: '5px',
                            cursor: 'pointer',
                            fontWeight: 'bold',
                            marginTop: '10px'
                        }}
                    >
                        Войти
                    </button>
                </form>
            </div>
        </div>
    );
};

export default Login;