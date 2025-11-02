import React, { useState } from "react";
import { Routes, Route, Navigate, useNavigate } from "react-router-dom";
// import Navbar from "./components/Navbar";
import DeveloperPanel from "./pages/DeveloperPanel";
import EnterprisePanel from "./pages/EnterprisePanel";
import MappingBranchPage from "./pages/MappingBranchPage";
import DropshipEnterprisePanel from "./pages/DropshipEnterprisePanel";

import Login from "./pages/Login";

const App = () => {
    const [authUser, setAuthUser] = useState(null);
    const navigate = useNavigate();

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
                            onClick={() => navigate("/dropship-enterprises")}
                        >
                            Dropship Enterprises
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
                    </>
                )}
            </div>
            <Routes>
                <Route path="/" element={<Login setAuthUser={setAuthUser} />} />
                <Route
                    path="/developer"
                    element={<PrivateRoute element={<DeveloperPanel authUser={authUser} />} />}
                />
                <Route
                    path="/enterprise"
                    element={<PrivateRoute element={<EnterprisePanel authUser={authUser} />} />}
                />
                <Route
                    path="/dropship-enterprises"
                    element={<PrivateRoute element={<DropshipEnterprisePanel authUser={authUser} />} />}
                />
                <Route
                    path="/mapping_branch"
                    element={<PrivateRoute element={<MappingBranchPage />} />}
                />
            </Routes>
        </div>
    );
};

export default App;