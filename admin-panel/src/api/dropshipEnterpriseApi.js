import axios from "axios";
import { API_BASE_URL } from "../config";

const getAuthHeaders = () => {
	const token = localStorage.getItem("token");
	if (!token) window.location.href = "/login";
	return {
		headers: {
			"Content-Type": "application/json",
			Authorization: `Bearer ${token}`,
		}
	};
};

export const getDropshipEnterprises = async () => {
	const resp = await axios.get(`${API_BASE_URL}/dropship/enterprises/`, getAuthHeaders());
	return resp.data;
};

export const getDropshipEnterpriseByCode = async (code) => {
	const resp = await axios.get(`${API_BASE_URL}/dropship/enterprises/${code}`, getAuthHeaders());
	return resp.data;
};

export const createDropshipEnterprise = async (data) => {
	const resp = await axios.post(`${API_BASE_URL}/dropship/enterprises/`, data, getAuthHeaders());
	return resp.data;
};

export const updateDropshipEnterprise = async (code, data) => {
	const resp = await axios.put(`${API_BASE_URL}/dropship/enterprises/${code}`, data, getAuthHeaders());
	return resp.data;
};