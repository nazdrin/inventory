import axios from 'axios';
import { API_BASE_URL } from '../config';

// ✅ Создание новой записи в mapping_branch (убран `getMappingBranches`)
export const createMappingBranch = async (mappingData) => {
    try {
        const response = await axios.post(`${API_BASE_URL}/mapping_branch/`, mappingData);
        return response.data;
    } catch (error) {
        console.error("Error creating mapping branch:", error);
        throw error;
    }
};
