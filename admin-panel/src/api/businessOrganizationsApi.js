import axios from "axios";
import { API_BASE_URL } from "../config";
import { getAuthHeaders } from "./developerApi";

export const getBusinessOrganizations = async (params = {}) => {
    const response = await axios.get(`${API_BASE_URL}/business-organizations`, {
        ...getAuthHeaders(),
        params,
    });
    return response.data || [];
};

export const createBusinessOrganization = async (payload) => {
    const response = await axios.post(`${API_BASE_URL}/business-organizations`, payload, getAuthHeaders());
    return response.data;
};

export const updateBusinessOrganization = async (organizationId, payload) => {
    const response = await axios.put(
        `${API_BASE_URL}/business-organizations/${organizationId}`,
        payload,
        getAuthHeaders(),
    );
    return response.data;
};

export const createBusinessAccount = async (organizationId, payload) => {
    const response = await axios.post(
        `${API_BASE_URL}/business-organizations/${organizationId}/accounts`,
        payload,
        getAuthHeaders(),
    );
    return response.data;
};

export const updateBusinessAccount = async (organizationId, accountId, payload) => {
    const response = await axios.put(
        `${API_BASE_URL}/business-organizations/${organizationId}/accounts/${accountId}`,
        payload,
        getAuthHeaders(),
    );
    return response.data;
};

export const createCheckboxRegister = async (organizationId, payload) => {
    const response = await axios.post(
        `${API_BASE_URL}/business-organizations/${organizationId}/checkbox-registers`,
        payload,
        getAuthHeaders(),
    );
    return response.data;
};

export const updateCheckboxRegister = async (organizationId, registerId, payload) => {
    const response = await axios.put(
        `${API_BASE_URL}/business-organizations/${organizationId}/checkbox-registers/${registerId}`,
        payload,
        getAuthHeaders(),
    );
    return response.data;
};

export const testCheckboxRegister = async (organizationId, registerId, action) => {
    const response = await axios.post(
        `${API_BASE_URL}/business-organizations/${organizationId}/checkbox-registers/${registerId}/test`,
        { action },
        getAuthHeaders(),
    );
    return response.data;
};

export const createCheckboxExclusion = async (organizationId, payload) => {
    const response = await axios.post(
        `${API_BASE_URL}/business-organizations/${organizationId}/checkbox-exclusions`,
        payload,
        getAuthHeaders(),
    );
    return response.data;
};

export const updateCheckboxExclusion = async (organizationId, exclusionId, payload) => {
    const response = await axios.put(
        `${API_BASE_URL}/business-organizations/${organizationId}/checkbox-exclusions/${exclusionId}`,
        payload,
        getAuthHeaders(),
    );
    return response.data;
};
