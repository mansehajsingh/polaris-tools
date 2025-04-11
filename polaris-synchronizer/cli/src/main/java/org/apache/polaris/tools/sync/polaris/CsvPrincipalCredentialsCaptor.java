package org.apache.polaris.tools.sync.polaris;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.polaris.core.admin.model.PrincipalWithCredentials;
import org.apache.polaris.core.admin.model.PrincipalWithCredentialsCredentials;
import org.apache.polaris.tools.sync.polaris.access.PrincipalCredentialCaptor;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Map;

import static java.nio.charset.StandardCharsets.UTF_8;

public class CsvPrincipalCredentialsCaptor implements PrincipalCredentialCaptor, Closeable {

    private final static String PRINCIPAL_NAME_HEADER = "PrincipalName";

    private final static String TARGET_CLIENT_ID_HEADER = "TargetClientId";

    private final static String TARGET_CLIENT_SECRET_HEADER = "TargetClientSecret";

    private final static String[] HEADERS = {
            PRINCIPAL_NAME_HEADER, TARGET_CLIENT_ID_HEADER, TARGET_CLIENT_SECRET_HEADER };

    private final File file;

    private final Map<String, PrincipalWithCredentials> principalsByName;

    public CsvPrincipalCredentialsCaptor(final File file) throws IOException {
        this.principalsByName = new HashMap<>();
        this.file = file;

        // We reload the file because we don't want to clear any existing data in case the same file
        // is passed twice
        if (this.file.exists()) {
            CSVFormat readerCSVFormat =
                    CSVFormat.DEFAULT.builder().setHeader(HEADERS).setSkipHeaderRecord(true).get();

            CSVParser parser =
                    CSVParser.parse(Files.newBufferedReader(file.toPath(), UTF_8), readerCSVFormat);

            for (CSVRecord record : parser.getRecords()) {
                this.principalsByName.put(
                        record.get(PRINCIPAL_NAME_HEADER),
                        new PrincipalWithCredentials().credentials(
                                new PrincipalWithCredentialsCredentials()
                                        .clientId(record.get(TARGET_CLIENT_ID_HEADER))
                                        .clientSecret(record.get(TARGET_CLIENT_SECRET_HEADER))));

            }

            parser.close();
        }
    }

    @Override
    public void storePrincipal(PrincipalWithCredentials principal) {
        this.principalsByName.put(principal.getPrincipal().getName(), principal);
    }

    @Override
    public void close() throws IOException {
        BufferedWriter writer = Files.newBufferedWriter(file.toPath(), StandardCharsets.UTF_8);

        writer.write(""); // clear file

        CSVFormat csvFormat = CSVFormat.DEFAULT.builder().setHeader(HEADERS).get();

        CSVPrinter csvPrinter = new CSVPrinter(writer, csvFormat);

        principalsByName.forEach((name, principal) -> {
            try {
                csvPrinter.printRecord(name, principal.getCredentials().getClientId(), principal.getCredentials().getClientSecret());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        csvPrinter.flush();
        csvPrinter.close();
    }

}
