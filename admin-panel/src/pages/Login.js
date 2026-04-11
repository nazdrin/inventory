import React, { useState } from "react";
import { useNavigate } from "react-router-dom";
import axios from "axios";
import { API_BASE_URL } from "../config"; // Импортируем базовый URL из config.js

const Login = ({ setAuthUser }) => {
    const [login, setLogin] = useState("");
    const [password, setPassword] = useState("");
    const [error, setError] = useState("");
    const [isSubmitting, setIsSubmitting] = useState(false);
    const [showPassword, setShowPassword] = useState(false);
    const navigate = useNavigate();

    const handleLogin = async (e) => {
        e.preventDefault();
        if (isSubmitting) {
            return;
        }

        setError("");
        setIsSubmitting(true);

        try {
            const response = await axios.post(`${API_BASE_URL}/login/`, {
                developer_login: login,
                developer_password: password
            });

            // Сохранение токена и логина в localStorage
            localStorage.setItem("token", response.data.access_token);
            localStorage.setItem("user_login", login); // 🔹 Теперь логин сохраняется!

            // Устанавливаем пользователя в состояние
            setAuthUser({
                developer_login: login,
            });

            // Перенаправление после входа
            navigate("/developer");

        } catch (err) {
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
        } finally {
            setIsSubmitting(false);
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
                    <label htmlFor="login" style={{ display: 'block', textAlign: 'left', marginBottom: '5px' }}>Логин:</label>
                    <input
                        id="login"
                        name="username"
                        type="text"
                        value={login}
                        onChange={(e) => setLogin(e.target.value)}
                        placeholder="Введите логин"
                        autoComplete="username"
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
                    <label htmlFor="password" style={{ display: 'block', textAlign: 'left', marginBottom: '5px' }}>Пароль:</label>
                    <div style={{ position: "relative", marginBottom: "15px" }}>
                        <input
                            id="password"
                            name="password"
                            type={showPassword ? "text" : "password"}
                            value={password}
                            onChange={(e) => setPassword(e.target.value)}
                            placeholder="Введите пароль"
                            autoComplete="current-password"
                            required
                            style={{
                                width: '100%',
                                padding: '10px',
                                paddingRight: '88px',
                                borderRadius: '5px',
                                border: '1px solid #ccc'
                            }}
                        />
                        <button
                            type="button"
                            onClick={() => setShowPassword((prev) => !prev)}
                            aria-label={showPassword ? "Скрыть пароль" : "Показать пароль"}
                            aria-pressed={showPassword}
                            style={{
                                position: "absolute",
                                top: "50%",
                                right: "10px",
                                transform: "translateY(-50%)",
                                background: "transparent",
                                border: "none",
                                cursor: "pointer",
                                color: "#2563eb",
                                fontWeight: "bold",
                                padding: 0
                            }}
                        >
                            {showPassword ? "Скрыть" : "Показать"}
                        </button>
                    </div>

                    {/* Кнопка Войти */}
                    <button
                        type="submit"
                        disabled={isSubmitting}
                        style={{
                            width: '100%',
                            padding: '10px',
                            backgroundColor: isSubmitting ? '#f2cc63' : '#ffc107',
                            color: 'black',
                            border: 'none',
                            borderRadius: '5px',
                            cursor: isSubmitting ? 'not-allowed' : 'pointer',
                            opacity: isSubmitting ? 0.8 : 1,
                            fontWeight: 'bold',
                            marginTop: '10px'
                        }}
                    >
                        {isSubmitting ? "Входим..." : "Войти"}
                    </button>
                </form>
            </div>
        </div>
    );
};

export default Login;
