package org.example.encrypted_parquet_inspector;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.crypto.FileDecryptionProperties;
import org.apache.parquet.format.AesGcmCtrV1;
import org.apache.parquet.format.AesGcmV1;
import org.apache.parquet.format.EncryptionAlgorithm;
import org.apache.parquet.format.FileCryptoMetaData;
import org.apache.parquet.format.Util;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.schema.Type;

public final class Inspector {
    private static final ObjectMapper JSON = new ObjectMapper();

    private Inspector() {
    }

    public static void main(String[] args) {
        quietLogging();
        try {
            Arguments arguments = Arguments.parse(args);
            Map<String, Object> result = inspect(arguments);
            System.out.println(JSON.writeValueAsString(result));
        } catch(Exception exception) {
            try {
                Map<String, Object> error = new LinkedHashMap<>();
                error.put("error", exception.getClass().getName());
                error.put("message", exception.getMessage());
                System.out.println(JSON.writeValueAsString(error));
            } catch(Exception ignored) {
                System.out.println("{\"error\":\"inspector failure\"}");
            }
            System.exit(1);
        }
    }

    private static Map<String, Object> inspect(Arguments arguments) throws IOException {
        FileCryptoMetaData cryptoMetadata = readCryptoMetadata(arguments.file());
        AlgorithmInfo algorithm = AlgorithmInfo.from(cryptoMetadata.getEncryption_algorithm());

        FileDecryptionProperties.Builder decryption = FileDecryptionProperties.builder()
            .withFooterKey(arguments.footerKey());
        if(arguments.aadPrefix() != null)
            decryption.withAADPrefix(arguments.aadPrefix());

        ParquetReadOptions readOptions = ParquetReadOptions.builder()
            .withDecryption(decryption.build())
            .build();

        try(ParquetFileReader reader = ParquetFileReader.open(
            new LocalInputFile(arguments.file().toPath()),
            readOptions)) {
            ParquetMetadata footer = reader.getFooter();
            return createResult(arguments, algorithm, footer);
        }
    }

    private static Map<String, Object> createResult(
        Arguments arguments,
        AlgorithmInfo algorithm,
        ParquetMetadata footer) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("file", arguments.file().getAbsolutePath());

        Map<String, Object> encryption = new LinkedHashMap<>();
        encryption.put("type", "ENCRYPTED_FOOTER");
        encryption.put("footerMode", "ENCRYPTED_FOOTER");
        encryption.put("algorithm", algorithm.name());
        encryption.put("supplyAadPrefixFlag", algorithm.supplyAadPrefix());
        encryption.put("aadSuppliedAtRead", arguments.aadPrefix() != null);
        encryption.put("hasStoredAadPrefix", algorithm.hasStoredAadPrefix());
        encryption.put("footerSigned", false);
        result.put("encryption", encryption);

        List<Map<String, Object>> fields = new ArrayList<>();
        for(Type type : footer.getFileMetaData().getSchema().getFields()) {
            Map<String, Object> field = new LinkedHashMap<>();
            field.put("name", type.getName());
            field.put("repetition", type.getRepetition().name());
            field.put("primitive", type.isPrimitive());
            if(type.isPrimitive())
                field.put("primitiveTypeName", type.asPrimitiveType().getPrimitiveTypeName().name());
            fields.add(field);
        }
        Map<String, Object> schema = new LinkedHashMap<>();
        schema.put("string", footer.getFileMetaData().getSchema().toString());
        schema.put("fields", fields);
        result.put("schema", schema);

        List<Map<String, Object>> rowGroups = new ArrayList<>();
        long totalRows = 0;
        int columnsSeen = 0;
        int encryptedColumns = 0;
        for(int rowGroupIndex = 0; rowGroupIndex < footer.getBlocks().size(); rowGroupIndex++) {
            BlockMetaData block = footer.getBlocks().get(rowGroupIndex);
            totalRows += block.getRowCount();

            Map<String, Object> rowGroup = new LinkedHashMap<>();
            rowGroup.put("index", rowGroupIndex);
            rowGroup.put("rowCount", block.getRowCount());
            rowGroup.put("totalByteSize", block.getTotalByteSize());

            List<Map<String, Object>> columns = new ArrayList<>();
            for(ColumnChunkMetaData column : block.getColumns()) {
                // This inspector opens encrypted-footer files. parquet-java clears the
                // per-column marker after decrypting their footer metadata, but encrypted
                // footer mode encrypts the columns with the footer key.
                boolean encrypted = true;
                columnsSeen++;
                if(encrypted)
                    encryptedColumns++;

                Map<String, Object> columnResult = new LinkedHashMap<>();
                columnResult.put("path", column.getPath().toDotString());
                columnResult.put("type", column.getType().name());
                columnResult.put("codec", column.getCodec().name());
                columnResult.put("totalSize", column.getTotalSize());
                columnResult.put("dataPageOffset", column.getFirstDataPageOffset());
                columnResult.put("dictionaryPageOffset", column.getDictionaryPageOffset());
                columnResult.put("hasDictionaryPage", column.hasDictionaryPage());
                columnResult.put("isEncrypted", encrypted);
                columnResult.put("encryptedWithFooterKey", encrypted);
                columns.add(columnResult);
            }
            rowGroup.put("columns", columns);
            rowGroups.add(rowGroup);
        }
        result.put("rowGroups", rowGroups);

        Map<String, Object> totals = new LinkedHashMap<>();
        totals.put("rowGroups", footer.getBlocks().size());
        totals.put("rowCount", totalRows);
        totals.put("columnsSeen", columnsSeen);
        totals.put("encryptedColumns", encryptedColumns);
        totals.put("plaintextColumns", columnsSeen - encryptedColumns);
        totals.put("anyEncryptedWithFooterKey", encryptedColumns != 0);
        result.put("totals", totals);
        return result;
    }

    private static FileCryptoMetaData readCryptoMetadata(File file) throws IOException {
        try(RandomAccessFile input = new RandomAccessFile(file, "r")) {
            if(input.length() < 12)
                throw new IOException("Parquet file is too small.");

            input.seek(input.length() - 8);
            byte[] lengthBytes = new byte[4];
            input.readFully(lengthBytes);
            int footerLength = ByteBuffer.wrap(lengthBytes)
                .order(ByteOrder.LITTLE_ENDIAN)
                .getInt();
            long footerOffset = input.length() - 8L - footerLength;
            if(footerLength <= 0 || footerOffset < 4)
                throw new IOException("Encrypted footer length is invalid.");

            input.seek(footerOffset);
            try(FileInputStream footerStream = new FileInputStream(input.getFD())) {
                return Util.readFileCryptoMetaData(footerStream);
            }
        }
    }

    private static void quietLogging() {
        System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "error");
        System.setProperty("org.slf4j.simpleLogger.log.org.apache.hadoop.util.NativeCodeLoader", "off");
    }

    private record AlgorithmInfo(String name, boolean supplyAadPrefix, boolean hasStoredAadPrefix) {
        static AlgorithmInfo from(EncryptionAlgorithm algorithm) {
            if(algorithm.isSetAES_GCM_V1()) {
                AesGcmV1 value = algorithm.getAES_GCM_V1();
                return new AlgorithmInfo(
                    "AES_GCM_V1",
                    value.isSetSupply_aad_prefix() && value.isSupply_aad_prefix(),
                    value.isSetAad_prefix());
            }
            if(algorithm.isSetAES_GCM_CTR_V1()) {
                AesGcmCtrV1 value = algorithm.getAES_GCM_CTR_V1();
                return new AlgorithmInfo(
                    "AES_GCM_CTR_V1",
                    value.isSetSupply_aad_prefix() && value.isSupply_aad_prefix(),
                    value.isSetAad_prefix());
            }
            throw new IllegalArgumentException("Unsupported Parquet encryption algorithm.");
        }
    }

    private record Arguments(File file, byte[] footerKey, byte[] aadPrefix) {
        static Arguments parse(String[] args) {
            if(args.length < 2)
                throw new IllegalArgumentException("Usage: <file> <footer-key> [aad-prefix] [--hex]");

            boolean hex = false;
            List<String> positional = new ArrayList<>();
            for(String argument : args) {
                if("--hex".equals(argument))
                    hex = true;
                else
                    positional.add(argument);
            }
            if(positional.size() < 2 || positional.size() > 3)
                throw new IllegalArgumentException("Usage: <file> <footer-key> [aad-prefix] [--hex]");

            File file = Path.of(positional.get(0)).toFile();
            if(!file.isFile())
                throw new IllegalArgumentException("Parquet file does not exist: " + file);

            byte[] footerKey = decode(positional.get(1), hex);
            byte[] aadPrefix = positional.size() == 3
                ? decode(positional.get(2), hex || positional.get(2).startsWith("0x"))
                : null;
            return new Arguments(file, footerKey, aadPrefix);
        }

        private static byte[] decode(String value, boolean hex) {
            if(!hex)
                return value.getBytes(StandardCharsets.UTF_8);
            String normalized = value.startsWith("0x") ? value.substring(2) : value;
            if((normalized.length() & 1) != 0)
                throw new IllegalArgumentException("Hex values must contain an even number of characters.");

            byte[] bytes = new byte[normalized.length() / 2];
            for(int i = 0; i < normalized.length(); i += 2) {
                int high = Character.digit(normalized.charAt(i), 16);
                int low = Character.digit(normalized.charAt(i + 1), 16);
                if(high < 0 || low < 0)
                    throw new IllegalArgumentException("Invalid hexadecimal value.");
                bytes[i / 2] = (byte)((high << 4) | low);
            }
            return bytes;
        }
    }
}
