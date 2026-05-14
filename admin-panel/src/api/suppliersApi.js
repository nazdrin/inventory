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

export const getBusinessStores = async () => {
    const response = await axios.get(`${API_BASE_URL}/business-stores?is_active=true`, getAuthHeaders());
    return response.data;
};

export const getBusinessSupplierStoreSettingsOverview = async (supplierCode) => {
    const response = await axios.get(
        `${API_BASE_URL}/business-suppliers/${supplierCode}/store-settings`,
        getAuthHeaders(),
    );
    return response.data;
};

export const getBusinessStoreSupplierSettings = async (storeId) => {
    const response = await axios.get(
        `${API_BASE_URL}/business-stores/${storeId}/supplier-settings`,
        getAuthHeaders(),
    );
    return response.data;
};

export const upsertBusinessStoreSupplierSettings = async (storeId, supplierCode, data) => {
    const response = await axios.put(
        `${API_BASE_URL}/business-stores/${storeId}/supplier-settings/${supplierCode}`,
        data,
        getAuthHeaders(),
    );
    return response.data;
};
