import React, { useState } from "react";
import { useNavigate } from "react-router-dom";

const Login = ({ setAuthUser }) => {
  const [login, setLogin] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const navigate = useNavigate();

  const handleLogin = async (e) => {
    e.preventDefault();
    console.log("Credentials being sent to the server:", { developer_login: login, developer_password: password }); // <-- Логируем отправляемые данные
    try {
        const response = await fetch("http://164.92.213.254:8000/developer_panel/login/", {
            method: "POST",
            headers: {
                "Content-Type": "application/json",
            },
            body: JSON.stringify({ developer_login: login, developer_password: password }), // <-- Исправление имен полей
        });

        console.log("Server response:", response); // <-- Логируем ответ сервера
        if (response.ok) {
            const data = await response.json();
            console.log("Login successful:", data); // <-- Логируем данные при успешном входе
            setAuthUser(data);
            navigate("/developer");
        } else {
            const errorData = await response.json();
            console.error("Login failed:", errorData); // <-- Логируем ошибку
            setError(errorData.detail || "Invalid login or password.");
        }
    } catch (err) {
        console.error("Request error:", err); // <-- Логируем исключение
        setError("An error occurred. Please try again later.");
    }
};

  return (
    <div>
      <h1>Login</h1>
      {error && <p style={{ color: "red" }}>{error}</p>}
      <form onSubmit={handleLogin}>
        <div>
          <label>Login:</label>
          <input
            type="text"
            value={login}
            onChange={(e) => setLogin(e.target.value)}
            placeholder="Enter your login"
            required
          />
        </div>
        <div>
          <label>Password:</label>
          <input
            type="password"
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            placeholder="Enter your password"
            required
          />
        </div>
        <button type="submit" style={{ marginTop: "10px" }}>
          Log In
        </button>
      </form>
    </div>
  );
};

export default Login;