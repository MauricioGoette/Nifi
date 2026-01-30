package com.lar.nifi.processors;

import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.documentation.*;
import org.apache.nifi.components.*;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.StreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

@Tags({"csv", "excel", "xlsx", "convert", "currency", "numeric", "integer"})
@CapabilityDescription(
        "Convierte archivos CSV a XLSX aplicando formatos personalizados. " +
                "Características: - Conversión básica CSV a XLSX, " +
                "- Campos monetarios con formato contable, " +
                "- Campos numéricos con decimales, " +
                "- Campos enteros sin decimales, " +
                "- Control de decimales (0-6), " +
                "- Redondeo automático a un decimal, " +
                "- Soporta múltiples campos, " +
                "- Agrega extensión .xlsx. <---> By @MGoette"
)
public class CsvToExcelProcessor extends AbstractProcessor {

    // Propiedades
    public static final PropertyDescriptor CURRENCY_COLUMNS = new PropertyDescriptor.Builder()
            .name("Currency Columns")
            .description("Columnas a formatear como moneda (ej. 'Total,Precio' o 'D,E'). Dejar vacío para no formatear columnas como moneda.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor NUMERIC_COLUMNS = new PropertyDescriptor.Builder()
            .name("Numeric Columns")
            .description("Columnas a formatear como numéricas con decimales (ej. 'Porcentaje,Descuento' o 'A,B'). Dejar vacío para no formatear columnas como numéricas.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor INTEGER_COLUMNS = new PropertyDescriptor.Builder()
            .name("Integer Columns")
            .description("Columnas a formatear como números enteros sin decimales (ej. 'Cantidad,Stock' o 'C,D'). Dejar vacío para no formatear columnas como enteros.")
            .required(false)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor CSV_SEPARATOR = new PropertyDescriptor.Builder()
            .name("CSV Separator")
            .description("Separador del CSV (; , | \\t)")
            .required(true)
            .defaultValue(";")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DECIMAL_PLACES = new PropertyDescriptor.Builder()
            .name("Decimal Places")
            .description("Número de decimales a mostrar (0-6). Solo aplica a Currency Columns y Numeric Columns.")
            .required(true)
            .defaultValue("1")  // CAMBIADO A 1 POR DEFECTO
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .build();

    // Relaciones
    private static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Archivo generado correctamente")
            .build();

    private static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Error en la conversión")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        descriptors = Arrays.asList(CURRENCY_COLUMNS, NUMERIC_COLUMNS, INTEGER_COLUMNS, CSV_SEPARATOR, DECIMAL_PLACES);
        relationships = new HashSet<>(Arrays.asList(SUCCESS, FAILURE));
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) return;

        final String currencyColumns = context.getProperty(CURRENCY_COLUMNS).getValue();
        final String numericColumns = context.getProperty(NUMERIC_COLUMNS).getValue();
        final String integerColumns = context.getProperty(INTEGER_COLUMNS).getValue();
        final char csvSeparator = context.getProperty(CSV_SEPARATOR).getValue().charAt(0);
        final int decimalPlaces = context.getProperty(DECIMAL_PLACES).asInteger();
        final AtomicReference<String> errorMessage = new AtomicReference<>();

        // Validar decimal places
        if (decimalPlaces < 0 || decimalPlaces > 6) {
            getLogger().error("Decimal Places debe estar entre 0 y 6");
            session.transfer(flowFile, FAILURE);
            return;
        }

        try {
            flowFile = session.write(flowFile, new StreamCallback() {
                @Override
                public void process(InputStream in, OutputStream out) throws IOException {
                    try (Workbook workbook = new XSSFWorkbook()) {
                        // Leer CSV
                        BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
                        CSVParser parser = CSVFormat.DEFAULT.withDelimiter(csvSeparator).withFirstRecordAsHeader().parse(reader);

                        Map<String, Integer> headerMap = parser.getHeaderMap();
                        List<CSVRecord> records = parser.getRecords();

                        if (records.isEmpty()) {
                            errorMessage.set("CSV vacío");
                            return;
                        }

                        // Crear formatos dinámicos basados en el número de decimales
                        String numericFormat = createNumericFormat(decimalPlaces);
                        String currencyFormat = createCurrencyFormat(decimalPlaces);
                        String integerFormat = "0";  // Formato para enteros

                        // Identificar columnas
                        Set<Integer> currencyColIndices = getColumnIndices(currencyColumns, headerMap);
                        Set<Integer> numericColIndices = getColumnIndices(numericColumns, headerMap);
                        Set<Integer> integerColIndices = getColumnIndices(integerColumns, headerMap);

                        // Estilo para campos numéricos con decimales (solo si hay columnas numéricas)
                        final CellStyle numericStyle;
                        if (!numericColIndices.isEmpty()) {
                            CellStyle style = workbook.createCellStyle();
                            style.setDataFormat(workbook.createDataFormat().getFormat(numericFormat));
                            style.setAlignment(HorizontalAlignment.RIGHT);
                            style.setWrapText(false);
                            numericStyle = style;
                        } else {
                            numericStyle = null;
                        }

                        // Estilo para campos monetarios (solo si hay columnas monetarias)
                        final CellStyle currencyStyle;
                        if (!currencyColIndices.isEmpty()) {
                            CellStyle style = workbook.createCellStyle();
                            style.setDataFormat(workbook.createDataFormat().getFormat(currencyFormat));
                            style.setAlignment(HorizontalAlignment.RIGHT);
                            style.setWrapText(false);
                            currencyStyle = style;
                        } else {
                            currencyStyle = null;
                        }

                        // Estilo para campos enteros (solo si hay columnas enteras)
                        final CellStyle integerStyle;
                        if (!integerColIndices.isEmpty()) {
                            CellStyle style = workbook.createCellStyle();
                            style.setDataFormat(workbook.createDataFormat().getFormat(integerFormat));
                            style.setAlignment(HorizontalAlignment.RIGHT);
                            style.setWrapText(false);
                            integerStyle = style;
                        } else {
                            integerStyle = null;
                        }

                        // Crear hoja Excel
                        Sheet sheet = workbook.createSheet("Datos");

                        // Escribir encabezados
                        Row headerRow = sheet.createRow(0);
                        headerMap.forEach((name, index) -> {
                            Cell cell = headerRow.createCell(index);
                            cell.setCellValue(name);
                        });

                        // Procesar datos
                        for (int i = 0; i < records.size(); i++) {
                            CSVRecord record = records.get(i);
                            Row row = sheet.createRow(i + 1);

                            headerMap.forEach((name, index) -> {
                                String value = record.get(name);
                                Cell cell = row.createCell(index);

                                if (currencyColIndices.contains(index) && currencyStyle != null) {
                                    formatNumericCell(cell, value, currencyStyle, "monetario", decimalPlaces);
                                } else if (numericColIndices.contains(index) && numericStyle != null) {
                                    formatNumericCell(cell, value, numericStyle, "numérico", decimalPlaces);
                                } else if (integerColIndices.contains(index) && integerStyle != null) {
                                    formatIntegerCell(cell, value, integerStyle, "entero");
                                } else {
                                    cell.setCellValue(value);
                                }
                            });
                        }

                        // Ajustar ancho de columnas
                        for (int i = 0; i < headerMap.size(); i++) {
                            sheet.autoSizeColumn(i);
                        }

                        workbook.write(out);
                    } catch (Exception e) {
                        errorMessage.set("Error: " + e.getMessage());
                        throw e;
                    }
                }
            });

            // Cambiar extensión a .xlsx
            String filename = flowFile.getAttribute("filename");
            if (filename != null && !filename.toLowerCase().endsWith(".xlsx")) {
                filename = filename.replaceAll("\\.[^.]+$", "") + ".xlsx";
                flowFile = session.putAttribute(flowFile, "filename", filename);
            }

            session.putAttribute(flowFile, "mime.type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
            session.transfer(flowFile, SUCCESS);

        } catch (Exception e) {
            getLogger().error("Error procesando", e);
            session.transfer(flowFile, FAILURE);
        }
    }

    // Métodos auxiliares
    private Set<Integer> getColumnIndices(String columns, Map<String, Integer> headerMap) {
        Set<Integer> indices = new HashSet<>();
        if (columns == null || columns.trim().isEmpty()) {
            return indices;
        }

        for (String col : columns.split(",")) {
            col = col.trim();
            if (col.matches("[A-Za-z]+")) {
                indices.add(columnLetterToIndex(col.toUpperCase()));
            } else if (headerMap.containsKey(col)) {
                indices.add(headerMap.get(col));
            }
        }
        return indices;
    }

    private void formatNumericCell(Cell cell, String value, CellStyle style, String tipo, int decimalPlaces) {
        if (value == null || value.trim().isEmpty()) {
            cell.setCellValue(value);
            return;
        }

        try {
            // Convertir a número manteniendo el formato decimal correcto
            String cleanValue = cleanNumericValue(value);
            double number = Double.parseDouble(cleanValue);

            // Redondear al número de decimales especificado
            double roundedNumber = roundToDecimals(number, decimalPlaces);

            cell.setCellValue(roundedNumber);
            cell.setCellStyle(style);
        } catch (NumberFormatException e) {
            cell.setCellValue(value);
            getLogger().warn("Valor no numérico en columna " + tipo + ": " + value);
        }
    }

    private void formatIntegerCell(Cell cell, String value, CellStyle style, String tipo) {
        if (value == null || value.trim().isEmpty()) {
            cell.setCellValue(value);
            return;
        }

        try {
            // Para enteros, redondear al número entero más cercano
            String cleanValue = cleanNumericValue(value);
            double number = Double.parseDouble(cleanValue);
            // Redondear al entero más cercano
            long integerValue = Math.round(number);
            cell.setCellValue(integerValue);
            cell.setCellStyle(style);
        } catch (NumberFormatException e) {
            cell.setCellValue(value);
            getLogger().warn("Valor no numérico en columna " + tipo + ": " + value);
        }
    }

    private String cleanNumericValue(String value) {
        if (value == null) return "0";

        // Eliminar espacios
        String cleaned = value.trim();

        // Reemplazar coma por punto para decimales
        cleaned = cleaned.replace(",", ".");

        // Si hay múltiples puntos, mantener solo el último como decimal
        int lastDotIndex = cleaned.lastIndexOf('.');
        if (lastDotIndex != -1) {
            // Eliminar todos los puntos excepto el último
            String before = cleaned.substring(0, lastDotIndex).replace(".", "");
            String after = cleaned.substring(lastDotIndex + 1);
            cleaned = before + "." + after;
        }

        return cleaned;
    }

    private double roundToDecimals(double value, int decimalPlaces) {
        if (decimalPlaces < 0) return value;

        double factor = Math.pow(10, decimalPlaces);
        return Math.round(value * factor) / factor;
    }

    private String createNumericFormat(int decimalPlaces) {
        if (decimalPlaces == 0) {
            return "#,##0";
        } else {
            String decimals = "0." + String.join("", Collections.nCopies(decimalPlaces, "0"));
            return "#,##" + decimals;
        }
    }

    private String createCurrencyFormat(int decimalPlaces) {
        if (decimalPlaces == 0) {
            return "_(\"$\"* #,##0_);_(\"$\"* (#,##0);_(\"$\"* \"-\"??_);_(@_)";
        } else {
            String decimals = "0." + String.join("", Collections.nCopies(decimalPlaces, "0"));
            return "_(\"$\"* #,##" + decimals + "_);_(\"$\"* (#,##" + decimals + ");_(\"$\"* \"-\"??_);_(@_)";
        }
    }

    private int columnLetterToIndex(String column) {
        int index = 0;
        for (char c : column.toCharArray()) {
            index = index * 26 + (c - 'A' + 1);
        }
        return index - 1;
    }
}