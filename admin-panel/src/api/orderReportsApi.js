import axios from "axios";
import { API_BASE_URL } from "../config";

const getAuthHeaders = () => {
    const token = localStorage.getItem("token");
    if (!token) {
        window.location.href = "/";
    }
    return {
        headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${token}`,
        },
    };
};

const periodParams = ({ periodFrom, periodTo, enterpriseCode }) => ({
    period_from: periodFrom,
    period_to: periodTo,
    ...(enterpriseCode ? { enterprise_code: enterpriseCode } : {}),
});

export const getOrderReportSummary = async ({ periodFrom, periodTo, enterpriseCode }) => {
    const response = await axios.get(`${API_BASE_URL}/reports/orders/summary`, {
        ...getAuthHeaders(),
        params: periodParams({ periodFrom, periodTo, enterpriseCode }),
    });
    return response.data;
};

export const getOrderReportFunnel = async ({ periodFrom, periodTo, enterpriseCode }) => {
    const response = await axios.get(`${API_BASE_URL}/reports/orders/funnel`, {
        ...getAuthHeaders(),
        params: periodParams({ periodFrom, periodTo, enterpriseCode }),
    });
    return response.data;
};

export const getOrderReportByEnterprise = async ({ periodFrom, periodTo, enterpriseCode }) => {
    const response = await axios.get(`${API_BASE_URL}/reports/orders/by-enterprise`, {
        ...getAuthHeaders(),
        params: periodParams({ periodFrom, periodTo, enterpriseCode }),
    });
    return response.data;
};

export const getOrderReportBySupplier = async ({ periodFrom, periodTo, enterpriseCode }) => {
    const response = await axios.get(`${API_BASE_URL}/reports/orders/by-supplier`, {
        ...getAuthHeaders(),
        params: periodParams({ periodFrom, periodTo, enterpriseCode }),
    });
    return response.data;
};

export const getOrderReportDetails = async ({ periodFrom, periodTo, enterpriseCode, statusGroup, supplierCode }) => {
    const response = await axios.get(`${API_BASE_URL}/reports/orders/details`, {
        ...getAuthHeaders(),
        params: {
            ...periodParams({ periodFrom, periodTo, enterpriseCode }),
            ...(statusGroup ? { status_group: statusGroup } : {}),
            ...(supplierCode ? { supplier_code: supplierCode } : {}),
            limit: 100,
        },
    });
    return response.data;
};

export const getOrderExpenseSettings = async () => {
    const response = await axios.get(`${API_BASE_URL}/reports/orders/expense-settings`, getAuthHeaders());
    return response.data;
};

export const upsertOrderExpenseSetting = async (payload) => {
    const response = await axios.put(`${API_BASE_URL}/reports/orders/expense-settings`, payload, getAuthHeaders());
    return response.data;
};

export const syncOrderReports = async ({ periodFrom, periodTo, enterpriseCode }) => {
    const response = await axios.post(
        `${API_BASE_URL}/reports/orders/sync`,
        {
            period_from: periodFrom,
            period_to: periodTo,
            enterprise_code: enterpriseCode || null,
            limit: 100,
            max_pages: 20,
        },
        getAuthHeaders(),
    );
    return response.data;
};
