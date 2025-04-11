COPY INTO {{ table_name }}
FROM '{{ file_path }}'
FILEFORMAT = {{ file_format }}
OPTIONS (
  {{ options }}
)