import React from "react";

const Table = ({ columns, data, actions }) => {
  console.log("Rendering Table Component");
  console.log("Columns:", columns);
  console.log("Data:", data);

  return (
    <table style={{ width: "100%", borderCollapse: "collapse", marginTop: "20px" }}>
      <thead>
        <tr>
          {columns.map((col) => (
            <th
              key={col.accessor || col}
              style={{ border: "1px solid #ccc", padding: "10px", textAlign: "left" }}
            >
              {col.header || col}
            </th>
          ))}
          {actions && <th style={{ border: "1px solid #ccc", padding: "10px" }}>Actions</th>}
        </tr>
      </thead>
      <tbody>
        {data.map((row, index) => (
          <tr key={index}>
            {columns.map((col) => (
              <td
                key={col.accessor || col}
                style={{ border: "1px solid #ccc", padding: "10px" }}
              >
                {
                  // Проверяем, если значение объекта - преобразуем его в строку JSON
                  typeof row[col.accessor || col] === "object"
                    ? JSON.stringify(row[col.accessor || col])
                    : row[col.accessor || col]
                }
              </td>
            ))}
            {actions && (
              <td style={{ border: "1px solid #ccc", padding: "10px" }}>
                {actions.map((action, i) => (
                  <button
                    key={i}
                    onClick={() => action.handler(row)}
                    style={{
                      marginRight: "5px",
                      padding: "5px 10px",
                      backgroundColor: "#007BFF",
                      color: "#fff",
                      border: "none",
                      cursor: "pointer",
                      borderRadius: "5px",
                    }}
                  >
                    {action.label}
                  </button>
                ))}
              </td>
            )}
          </tr>
        ))}
      </tbody>
    </table>
  );
};

export default Table;