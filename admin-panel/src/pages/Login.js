import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
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
            const response = await fetch(`${API_BASE_URL}/login/`, {
                method: "POST",
                headers: {
                    "Content-Type": "application/json",
                },
                body: JSON.stringify({ developer_login: login, developer_password: password }),
            });

            console.log("Ответ сервера:", response);
            if (response.ok) {
                const data = await response.json();
                console.log("Успешный вход:", data);
                setAuthUser(data);
                navigate("/developer");
            } else {
                const errorData = await response.json();
                console.error("Ошибка входа:", errorData);
                setError(errorData.detail || "Неверный логин или пароль.");
            }
        } catch (err) {
            console.error("Ошибка запроса:", err);
            setError("Произошла ошибка. Попробуйте позже.");
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
