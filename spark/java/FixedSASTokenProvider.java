package org.apache.hadoop.fs.azurebfs.sas;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.azurebfs.extensions.SASTokenProvider;

/**
 * A SASTokenProvider implementation that returns a fixed SAS token read from
 * the Hadoop configuration key:
 *   fs.azure.sas.fixed.token.<accountName>
 *
 * This class is not included in the standard Apache hadoop-azure JAR and must
 * be compiled and packaged separately. See docs/abfss-migration.md.
 */
public class FixedSASTokenProvider implements SASTokenProvider {

    private String sasToken;

    @Override
    public void initialize(Configuration conf, String accountName) throws IOException {
        sasToken = conf.get("fs.azure.sas.fixed.token." + accountName);
        if (sasToken == null || sasToken.isEmpty()) {
            throw new IOException(
                "No SAS token configured for account: " + accountName
                + ". Set fs.azure.sas.fixed.token." + accountName
            );
        }
    }

    @Override
    public String getSASToken(String account, String fileSystem, String path, String operation) {
        return sasToken;
    }
}
