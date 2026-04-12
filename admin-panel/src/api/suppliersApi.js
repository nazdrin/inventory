import axios from "axios";
import { API_BASE_URL } from "../config";

const getAuthHeaders = () => {
    const token = localStorage.getItem("token");
    if (!token) {
        window.location.href = "/";
        return {};
    }

    return {
        headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${token}`,
        },
    };
};

export const getSuppliersViewList = async () => {
    const response = await axios.get(`${API_BASE_URL}/suppliers/view/`, getAuthHeaders());
    return response.data;
};

export const getSupplierViewDetail = async (code) => {
    const response = await axios.get(`${API_BASE_URL}/suppliers/view/${code}`, getAuthHeaders());
    return response.data;
};

export const createSupplier = async (data) => {
    const response = await axios.post(`${API_BASE_URL}/dropship/enterprises/`, data, getAuthHeaders());
    return response.data;
};

export const updateSupplier = async (code, data) => {
    const response = await axios.put(`${API_BASE_URL}/dropship/enterprises/${code}`, data, getAuthHeaders());
    return response.data;
};
