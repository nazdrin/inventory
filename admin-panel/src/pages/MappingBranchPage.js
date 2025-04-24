import React, { useState, useEffect } from 'react';
import { createMappingBranch } from '../api/mappingBranchAPI';
import { getEnterprises } from '../api/enterpriseApi';

const MappingBranchPage = () => {
    const [enterprises, setEnterprises] = useState([]);
    const [selectedEnterprise, setSelectedEnterprise] = useState('');
    const [branch, setBranch] = useState('');
    const [storeId, setStoreId] = useState('');
    const [googleFolderId, setGoogleFolderId] = useState('');

    useEffect(() => {
        async function fetchEnterprises() {
            try {
                const data = await getEnterprises();
                setEnterprises(data);
            } catch (error) {
                console.error('Error fetching enterprises:', error);
            }
        }
        fetchEnterprises();
    }, []);

    const handleSave = async () => {
        if (!branch || !storeId || !selectedEnterprise || !googleFolderId) return;

        const mappingData = {
            branch,
            store_id: storeId,
            enterprise_code: selectedEnterprise,
            google_folder_id: googleFolderId,
            id_telegram: [],
        };

        try {
            await createMappingBranch(mappingData);
            setBranch('');
            setStoreId('');
            setGoogleFolderId('');
            alert("Запись успешно добавлена!");
        } catch (error) {
            console.error('Error saving mapping branch:', error);
            alert("Ошибка при сохранении!");
        }
    };

    return (
        <div style={{
            display: 'flex',
            justifyContent: 'center',
            alignItems: 'center',
            height: '100vh',
            backgroundColor: '#f8f9fa'
        }}>
            <div style={{
                padding: '30px',
                backgroundColor: 'white',
                borderRadius: '8px',
                boxShadow: '0px 0px 10px rgba(0, 0, 0, 0.1)',
                border: '1px solid #ddd',
                width: '400px',
                textAlign: 'center'
            }}>
                <h2 style={{ marginBottom: '20px' }}>Mapping Branch Management</h2>

                <label style={{ display: 'block', textAlign: 'left', marginBottom: '5px' }}>Select Enterprise:</label>
                <select
                    onChange={(e) => setSelectedEnterprise(e.target.value)}
                    value={selectedEnterprise}
                    style={{
                        width: '100%',
                        padding: '10px',
                        marginBottom: '15px',
                        borderRadius: '5px',
                        border: '1px solid #ccc'
                    }}
                >
                    <option value="">-- Select an Enterprise --</option>
                    {enterprises.map((enterprise) => (
                        <option key={enterprise.enterprise_code} value={enterprise.enterprise_code}>
                            {enterprise.enterprise_name} ({enterprise.enterprise_code})
                        </option>
                    ))}
                </select>

                <label style={{ display: 'block', textAlign: 'left', marginBottom: '5px' }}>Branch:</label>
                <input
                    type="text"
                    placeholder="Branch"
                    value={branch}
                    onChange={(e) => setBranch(e.target.value)}
                    style={{
                        width: '100%',
                        padding: '10px',
                        marginBottom: '15px',
                        borderRadius: '5px',
                        border: '1px solid #ccc'
                    }}
                />

                <label style={{ display: 'block', textAlign: 'left', marginBottom: '5px' }}>Store ID:</label>
                <input
                    type="text"
                    placeholder="Store ID"
                    value={storeId}
                    onChange={(e) => setStoreId(e.target.value)}
                    style={{
                        width: '100%',
                        padding: '10px',
                        marginBottom: '15px',
                        borderRadius: '5px',
                        border: '1px solid #ccc'
                    }}
                />

                <label style={{ display: 'block', textAlign: 'left', marginBottom: '5px' }}>Google Folder ID:</label>
                <input
                    type="text"
                    placeholder="Google Folder ID"
                    value={googleFolderId}
                    onChange={(e) => setGoogleFolderId(e.target.value)}
                    style={{
                        width: '100%',
                        padding: '10px',
                        marginBottom: '15px',
                        borderRadius: '5px',
                        border: '1px solid #ccc'
                    }}
                />

                <button
                    onClick={handleSave}
                    disabled={!branch || !storeId || !googleFolderId}
                    style={{
                        width: '100%',
                        padding: '10px',
                        backgroundColor: '#ffc107',
                        color: 'black',
                        border: 'none',
                        borderRadius: '5px',
                        cursor: 'pointer',
                        fontWeight: 'bold',
                        marginTop: '10px'
                    }}
                >
                    Записать
                </button>
            </div>
        </div>
    );
};

export default MappingBranchPage;