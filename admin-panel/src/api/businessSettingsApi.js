import axios from "axios";
import { API_BASE_URL } from "../config";

const getAuthHeaders = () => {
    const token = localStorage.getItem("token");

    if (!token) {
        window.location.href = "/login";
    }

    return {
        headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${token}`,
        },
    };
};

export const getBusinessSettingsView = async () => {
    try {
        const response = await axios.get(`${API_BASE_URL}/business/settings/view`, getAuthHeaders());
        return response.data;
    } catch (error) {
        console.error("Error fetching business settings view:", error);
        throw error;
    }
};

export const updateBusinessSettingsControlPlaneScope = async (payload) => {
    try {
        const response = await axios.put(
            `${API_BASE_URL}/business/settings/master-scope`,
            payload,
            getAuthHeaders(),
        );
        return response.data;
    } catch (error) {
        console.error("Error updating business settings master scope:", error);
        throw error;
    }
};

export const updateBusinessSettingsMasterScope = updateBusinessSettingsControlPlaneScope;

export const updateBusinessSettingsEnterpriseOperationalScope = async (payload) => {
    try {
        const response = await axios.put(
            `${API_BASE_URL}/business/settings/enterprise-operational-scope`,
            payload,
            getAuthHeaders(),
        );
        return response.data;
    } catch (error) {
        console.error("Error updating business settings enterprise operational scope:", error);
        throw error;
    }
};

export const updateBusinessSettingsPricingScope = async (payload) => {
    try {
        const response = await axios.put(
            `${API_BASE_URL}/business/settings/pricing-scope`,
            payload,
            getAuthHeaders(),
        );
        return response.data;
    } catch (error) {
        console.error("Error updating business settings pricing scope:", error);
        throw error;
    }
};
