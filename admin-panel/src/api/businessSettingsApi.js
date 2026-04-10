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
