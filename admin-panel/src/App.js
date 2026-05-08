import React, { useMemo, useState } from "react";
import { Routes, Route, Navigate, useNavigate } from "react-router-dom";
// import Navbar from "./components/Navbar";
import DeveloperPanel from "./pages/DeveloperPanel";
import EnterprisePanel from "./pages/EnterprisePanel";
import MappingBranchPage from "./pages/MappingBranchPage";
import DropshipEnterprisePanel from "./pages/DropshipEnterprisePanel";
import FormatsPage from "./pages/FormatsPage";
import SuppliersPage from "./pages/SuppliersPage";
import BusinessSettingsPage from "./pages/BusinessSettingsPage";
import BusinessStoresPage from "./pages/BusinessStoresPage";
import PaymentReportsPage from "./pages/PaymentReportsPage";
import OrderReportsPage from "./pages/OrderReportsPage";

import Login from "./pages/Login";

const clearStoredAuth = () => {
    localStorage.removeItem("token");
    localStorage.removeItem("user_login");
};

const decodeJwtPayload = (token) => {
    try {
        const [, payload] = token.split(".");
        if (!payload) {
            return null;
        }

        const normalized = payload.replace(/-/g, "+").replace(/_/g, "/");
        const padded = normalized.padEnd(normalized.length + ((4 - normalized.length % 4) % 4), "=");
        const decoded = atob(padded);
        return JSON.parse(decoded);
    } catch (error) {
        return null;
    }
};

const getStoredAuthUser = () => {
    const token = localStorage.getItem("token");
    const payload = token ? decodeJwtPayload(token) : null;
    const developerLogin = payload?.sub || localStorage.getItem("user_login");

    if (!token || !developerLogin || !payload?.exp) {
        clearStoredAuth();
        return null;
    }

    const nowInSeconds = Math.floor(Date.now() / 1000);
    if (payload.exp <= nowInSeconds) {
        clearStoredAuth();
        return null;
    }

    localStorage.setItem("user_login", developerLogin);
    return { developer_login: developerLogin };
};

const App = () => {
    const [authUser, setAuthUser] = useState(() => getStoredAuthUser());
    const navigate = useNavigate();
    const loginElement = useMemo(() => {
        return authUser ? <Navigate to="/developer" replace /> : <Login setAuthUser={setAuthUser} />;
    }, [authUser]);

    const PrivateRoute = ({ element }) => {
        return authUser ? element : <Navigate to="/" />;
    };

    return (
        <div style={{ backgroundColor: "#f5f5f5", minHeight: "100vh" }}>
            <div
                style={{
                    display: "flex",
                    justifyContent: "center",
                    padding: "10px 0",
                    backgroundColor: "#e0e0e0",
                    position: "sticky",
                    top: 0,
                    zIndex: 10,
                }}
            >
                {authUser && (
                    <>
                        <button
                            style={{
                                padding: "10px 20px",
                                marginRight: "10px",
                                backgroundColor: "#007BFF",
                                color: "white",
                                border: "none",
                                cursor: "pointer",
                                borderRadius: "5px",
                            }}
                            onClick={() => navigate("/developer")}
                        >
                            Developer Panel
                        </button>
                        <button
                            style={{
                                padding: "10px 20px",
                                backgroundColor: "#007BFF",
                                marginRight: 10,
                                color: "white",
                                border: "none",
                                cursor: "pointer",
                                borderRadius: "5px",
                            }}
                            onClick={() => navigate("/enterprise")}
                        >
                            Enterprise Panel
                        </button>
                        <button
                            style={{
                                padding: "10px 20px",
                                backgroundColor: "#007BFF",
                                marginRight: 10,
                                color: "white",
                                border: "none",
                                cursor: "pointer",
                                borderRadius: "5px",
                            }}
                            onClick={() => navigate("/suppliers")}
                        >
                            Suppliers
                        </button>
                        <button
                            style={{
                                padding: "10px 20px",
                                backgroundColor: "#007BFF",
                                color: "white",
                                border: "none",
                                cursor: "pointer",
                                borderRadius: "5px",
                            }}
                            onClick={() => navigate("/mapping_branch")}
                        >
                            Mapping Branch
                        </button>
                        <button
                            style={{
                                padding: "10px 20px",
                                backgroundColor: "#007BFF",
                                marginLeft: 10,
                                color: "white",
                                border: "none",
                                cursor: "pointer",
                                borderRadius: "5px",
                            }}
                            onClick={() => navigate("/formats")}
                        >
                            Formats
                        </button>
                        <button
                            style={{
                                padding: "10px 20px",
                                backgroundColor: "#007BFF",
                                marginLeft: 10,
                                color: "white",
                                border: "none",
                                cursor: "pointer",
                                borderRadius: "5px",
                            }}
                            onClick={() => navigate("/business")}
                        >
                            Business Settings
                        </button>
                        <button
                            style={{
                                padding: "10px 20px",
                                backgroundColor: "#007BFF",
                                marginLeft: 10,
                                color: "white",
                                border: "none",
                                cursor: "pointer",
                                borderRadius: "5px",
                            }}
                            onClick={() => navigate("/business-stores")}
                        >
                            Business-продавцы
                        </button>
                        <button
                            style={{
                                padding: "10px 20px",
                                backgroundColor: "#007BFF",
                                marginLeft: 10,
                                color: "white",
                                border: "none",
                                cursor: "pointer",
                                borderRadius: "5px",
                            }}
                            onClick={() => navigate("/payment-reports")}
                        >
                            Платежи
                        </button>
                        <button
                            style={{
                                padding: "10px 20px",
                                backgroundColor: "#007BFF",
                                marginLeft: 10,
                                color: "white",
                                border: "none",
                                cursor: "pointer",
                                borderRadius: "5px",
                            }}
                            onClick={() => navigate("/order-reports")}
                        >
                            Заказы
                        </button>
                    </>
                )}
            </div>
            <Routes>
                <Route path="/" element={loginElement} />
                <Route path="/login" element={loginElement} />
                <Route
                    path="/developer"
                    element={<PrivateRoute element={<DeveloperPanel authUser={authUser} />} />}
                />
                <Route
                    path="/enterprise"
                    element={<PrivateRoute element={<EnterprisePanel authUser={authUser} />} />}
                />
                <Route
                    path="/suppliers"
                    element={<PrivateRoute element={<SuppliersPage authUser={authUser} />} />}
                />
                <Route
                    path="/dropship-enterprises"
                    element={<PrivateRoute element={<DropshipEnterprisePanel authUser={authUser} />} />}
                />
                <Route
                    path="/mapping_branch"
                    element={<PrivateRoute element={<MappingBranchPage />} />}
                />
                <Route
                    path="/formats"
                    element={<PrivateRoute element={<FormatsPage />} />}
                />
                <Route
                    path="/business"
                    element={<PrivateRoute element={<BusinessSettingsPage authUser={authUser} />} />}
                />
                <Route
                    path="/business-stores"
                    element={<PrivateRoute element={<BusinessStoresPage authUser={authUser} />} />}
                />
                <Route
                    path="/payment-reports"
                    element={<PrivateRoute element={<PaymentReportsPage authUser={authUser} />} />}
                />
                <Route
                    path="/order-reports"
                    element={<PrivateRoute element={<OrderReportsPage authUser={authUser} />} />}
                />
            </Routes>
        </div>
    );
};

export default App;
