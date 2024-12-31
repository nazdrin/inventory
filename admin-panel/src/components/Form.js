import React from "react";

const Form = ({ fields, values = {}, onChange, onSubmit, style }) => {
  const handleChange = (e) => {
    const { name, value, type, checked } = e.target;
    const fieldValue = type === "checkbox" ? checked : value;
    onChange({ ...values, [name]: fieldValue });
  };

  return (
    <form
      onSubmit={(e) => {
        e.preventDefault();
        onSubmit();
      }}
      style={{
        ...style,
        display: "grid",
        gridTemplateColumns: "330px 1fr", // Лейблы слева, поля справа
        alignItems: "center",
        gap: "10px 20px",
        maxWidth: "770px", // Ограничение ширины формы
        margin: "0 auto", // Центрирование формы
        backgroundColor: "#f8f8f8", // Добавлен светло-серый фон
        border: "1px solid #ddd", // Рамка для улучшения дизайна
        padding: "20px",
        borderRadius: "8px", // Скругленные углы
      }}
    >
      {fields.map((field) => (
        <React.Fragment key={field.name}>
          <label
            htmlFor={field.name}
            style={{
              textAlign: "left", // Выровнять текст лейбла по левой стороне
              fontWeight: "bold",
              marginRight: "10px",
            }}
          >
            {field.label}:
          </label>
          {field.type === "select" ? (
            <select
              id={field.name}
              name={field.name}
              value={values[field.name] || ""}
              onChange={handleChange}
              style={{
                padding: "10px",
                borderRadius: "5px",
                border: "1px solid #ccc",
                maxWidth: "400px", // Ограничение ширины поля
              }}
            >
              {field.options.map((option) => (
                <option key={option.value} value={option.value}>
                  {option.label}
                </option>
              ))}
            </select>
          ) : field.type === "checkbox" ? (
            <input
              id={field.name}
              type="checkbox"
              name={field.name}
              checked={values[field.name] || false}  // Обрабатываем флаг как true/false
              onChange={handleChange}
              style={{
                width: "20px",
                height: "20px",
                borderRadius: "5px",
              }}
            />
          ) : (
            <input
              id={field.name}
              type={field.type || "text"}
              name={field.name}
              value={values[field.name] || ""}
              onChange={handleChange}
              style={{
                width: "100%",
                maxWidth: "400px", // Ограничение ширины поля
                padding: "10px",
                borderRadius: "5px",
                border: "1px solid #ccc",
              }}
              disabled={field.disabled}
            />
          )}
        </React.Fragment>
      ))}
    </form>
  );
};

export default Form;