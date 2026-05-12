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

const buildPeriodParams = ({ periodFrom, periodTo }) => ({
    period_from: periodFrom,
    period_to: periodTo,
});

export const getPaymentManagementSummary = async ({ periodFrom, periodTo }) => {
    const response = await axios.get(`${API_BASE_URL}/payment-reports/management-summary`, {
        ...getAuthHeaders(),
        params: buildPeriodParams({ periodFrom, periodTo }),
    });
    return response.data;
};

export const getPaymentSummary = async ({ periodFrom, periodTo }) => {
    const response = await axios.get(`${API_BASE_URL}/payment-reports/summary`, {
        ...getAuthHeaders(),
        params: buildPeriodParams({ periodFrom, periodTo }),
    });
    return response.data;
};

export const importSalesDrivePayments = async ({ periodFrom, periodTo, paymentType = "all" }) => {
    const response = await axios.post(
        `${API_BASE_URL}/payment-imports/salesdrive`,
        {
            period_from: periodFrom,
            period_to: periodTo,
            payment_type: paymentType,
        },
        getAuthHeaders(),
    );
    return response.data;
};

export const recalculatePaymentReport = async ({ periodFrom, periodTo }) => {
    const response = await axios.post(
        `${API_BASE_URL}/payment-reports/recalculate`,
        {
            period_from: periodFrom,
            period_to: periodTo,
        },
        getAuthHeaders(),
    );
    return response.data;
};

export const getPaymentImportRuns = async ({ limit = 10 } = {}) => {
    const response = await axios.get(`${API_BASE_URL}/payment-imports`, {
        ...getAuthHeaders(),
        params: { limit },
    });
    return response.data;
};

export const upsertAccountBalanceAdjustment = async (payload) => {
    const response = await axios.post(
        `${API_BASE_URL}/payment-reports/account-balance-adjustments`,
        payload,
        getAuthHeaders(),
    );
    return response.data;
};

export const getPaymentUnmappedCounterparties = async ({ periodFrom, periodTo, limit = 100, examples = 2 }) => {
    const response = await axios.get(`${API_BASE_URL}/payment-reports/unmapped-counterparties`, {
        ...getAuthHeaders(),
        params: { ...buildPeriodParams({ periodFrom, periodTo }), limit, examples },
    });
    return response.data;
};

export const getPaymentCounterpartyMappings = async ({ limit = 500 } = {}) => {
    const response = await axios.get(`${API_BASE_URL}/payment-reports/counterparty-mappings`, {
        ...getAuthHeaders(),
        params: { limit },
    });
    return response.data;
};

export const createPaymentCounterpartyMapping = async (payload) => {
    const response = await axios.post(
        `${API_BASE_URL}/payment-reports/counterparty-mappings`,
        payload,
        getAuthHeaders(),
    );
    return response.data;
};
