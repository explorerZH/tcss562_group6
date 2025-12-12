package lambda;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rdsdata.RdsDataClient;
import software.amazon.awssdk.services.rdsdata.model.ExecuteStatementRequest;
import software.amazon.awssdk.services.rdsdata.model.ExecuteStatementResponse;
import software.amazon.awssdk.services.rdsdata.model.BeginTransactionRequest;
import software.amazon.awssdk.services.rdsdata.model.BeginTransactionResponse;
import software.amazon.awssdk.services.rdsdata.model.CommitTransactionRequest;
import software.amazon.awssdk.services.rdsdata.model.CommitTransactionResponse;
import software.amazon.awssdk.services.rdsdata.model.RollbackTransactionRequest;
import software.amazon.awssdk.services.rdsdata.model.RollbackTransactionResponse;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import java.time.Duration;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.core.ResponseInputStream;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.sql.SQLException;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

/**
 * Simple example utility to download a dataset from a URL, upload it to S3,
 * and trigger a bulk load into Aurora Serverless (MySQL) using
 * a SQL LOAD DATA FROM S3 statement executed through the RDS Data API.
 *
 * Notes:
 * - This example uses the AWS SDK v2 clients: S3Client and RdsDataClient.
 * - For production use run in an AWS Lambda with an appropriate IAM role.
 * - The Aurora cluster must be configured to allow LOAD DATA FROM S3 and
 * the cluster must have an associated role/policy to access the S3 object.
 */
public class AuroraBulkLoader {

    /**
     * Trigger a server-side LOAD DATA FROM S3 for the given S3 object.
     * This assumes the Aurora cluster has an IAM role allowing it to read the
     * S3 object (preferred for server-side bulk loads).
     */
    public static void loadS3ToAurora(String s3Bucket,
            String s3Key,
            Region region,
            String dbResourceArn,
            String dbSecretArn,
            String database,
            String table) {
        executeLoadFromS3(s3Bucket, s3Key, region, dbResourceArn, dbSecretArn, database, table);
    }

    /**
     * Execute the LOAD DATA FROM S3 SQL statement against Aurora via the Data API.
     */
    public static void executeLoadFromS3(String s3Bucket,
            String s3Key,
            Region region,
            String dbResourceArn,
            String dbSecretArn,
            String database,
            String table) {
        // Instead of loading directly into the typed production table (which can
        // fail on malformed rows), load into a staging table with all TEXT
        // columns. After verifying/cleaning the staging table you can INSERT
        // into your final `table` converting types as needed. This avoids
        // errors like "Incorrect integer value" during LOAD.

        // Staging table name
        String stagingTable = "listings_staging";

        // prune the staging table if it exists
        String pruneSql = "DELETE FROM " + stagingTable + ";";
        try (RdsDataClient rds = RdsDataClient.builder()
                .region(region)
                .httpClientBuilder(ApacheHttpClient.builder()
                        .socketTimeout(Duration.ofMinutes(5)))
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .apiCallTimeout(Duration.ofMinutes(5))
                        .apiCallAttemptTimeout(Duration.ofMinutes(1))
                        .build())
                .build()) {

            ExecuteStatementRequest pruneReq = ExecuteStatementRequest.builder()
                    .resourceArn(dbResourceArn)
                    .secretArn(dbSecretArn)
                    .database(database)
                    .sql(pruneSql)
                    .build();
            ExecuteStatementResponse pruneResp = rds.executeStatement(pruneReq);
            System.out.println("Prune staging table response: " + pruneResp);
        }

        // Create a staging table with columns matching the CSV header (all TEXT).
        // Header fields taken from the sample CSV provided.
        String createTableSql = "CREATE TABLE IF NOT EXISTS " + stagingTable + " (\n"
                + "id TEXT, listing_url TEXT, last_scraped TEXT, name TEXT, description TEXT, property_type TEXT, room_type TEXT, host_id TEXT, host_name TEXT, host_since TEXT, host_response_time TEXT, host_response_rate TEXT, host_acceptance_rate TEXT, host_is_superhost TEXT, host_listings_count TEXT, host_identity_verified TEXT, street TEXT, neighbourhood TEXT, neighbourhood_cleansed TEXT, neighbourhood_group_cleansed TEXT, city TEXT, state TEXT, zipcode TEXT, latitude TEXT, longitude TEXT, is_location_exact TEXT, accommodates TEXT, bathrooms TEXT, bedrooms TEXT, beds TEXT, bed_type TEXT, amenities TEXT, square_feet TEXT, price TEXT, weekly_price TEXT, monthly_price TEXT, security_deposit TEXT, cleaning_fee TEXT, guests_included TEXT, extra_people TEXT, minimum_nights TEXT, maximum_nights TEXT, instant_bookable TEXT, cancellation_policy TEXT, has_availability TEXT, availability_30 TEXT, availability_60 TEXT, availability_90 TEXT, availability_365 TEXT, number_of_reviews TEXT, first_review TEXT, last_review TEXT, review_scores_rating TEXT, review_scores_accuracy TEXT, review_scores_cleanliness TEXT, review_scores_checkin TEXT, review_scores_communication TEXT, review_scores_location TEXT, review_scores_value TEXT, reviews_per_month TEXT, price_category TEXT, review_category TEXT, host_category TEXT, availability_category TEXT, room_type_simplified TEXT, is_professional_host TEXT, has_cleaning_fee TEXT, price_per_guest TEXT\n)";

        String loadSql = String.format(
                "LOAD DATA FROM S3 's3://%s/%s' INTO TABLE %s "
                        + "FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\"' LINES TERMINATED BY '\\n' IGNORE 1 LINES;",
                s3Bucket, s3Key, stagingTable);

        // Build an RdsDataClient with longer timeouts. Large LOAD operations
        // can take a long time; increase socket and apiCall timeouts so the
        // SDK does not abort the HTTP read prematurely. We'll run the LOAD
        // inside a transaction and attempt to set a session variable that
        // disables per-session execution limits (MySQL: max_execution_time).
        try (RdsDataClient rds = RdsDataClient.builder()
                .region(region)
                .httpClientBuilder(ApacheHttpClient.builder()
                        .socketTimeout(Duration.ofMinutes(14)))
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .apiCallTimeout(Duration.ofMinutes(14))
                        .apiCallAttemptTimeout(Duration.ofMinutes(2))
                        .build())
                .build()) {

            // Start a transaction so session variables apply to the LOAD
            long totalStartMs = System.currentTimeMillis();

            BeginTransactionResponse tx = rds.beginTransaction(BeginTransactionRequest.builder()
                    .resourceArn(dbResourceArn)
                    .secretArn(dbSecretArn)
                    .database(database)
                    .build());

            String txId = tx.transactionId();
            long txStartMs = System.currentTimeMillis();
            long setStartMs = 0L, setEndMs = 0L, createStartMs = 0L, createEndMs = 0L, loadStartMs = 0L, loadEndMs = 0L,
                    countStartMs = 0L, countEndMs = 0L, insertStartMs = 0L, insertEndMs = 0L, badRowsStartMs = 0L,
                    badRowsEndMs = 0L, commitStartMs = 0L, commitEndMs = 0L;
            long stagingCount = 0L;
            long inserted = -1L;
            int badRowsShown = 0;
            try {
                // Try to disable per-session max execution time for MySQL engines.
                // If this variable isn't supported the DATA API will return an
                // error and we'll continue to attempt the LOAD (cluster config
                // may still cancel it). Adjust or remove this SET if your engine
                // uses a different variable (eg. PostgreSQL uses "statement_timeout").
                String setSql = "SET SESSION max_execution_time=0;";
                ExecuteStatementRequest setReq = ExecuteStatementRequest.builder()
                        .resourceArn(dbResourceArn)
                        .secretArn(dbSecretArn)
                        .database(database)
                        .sql(setSql)
                        .transactionId(txId)
                        .build();

                try {
                    setStartMs = System.currentTimeMillis();
                    ExecuteStatementResponse setResp = rds.executeStatement(setReq);
                    setEndMs = System.currentTimeMillis();
                    System.out.println("SET SESSION response: " + setResp);
                } catch (Exception setEx) {
                    setEndMs = System.currentTimeMillis();
                    // Non-fatal: engine might not support this variable. Log and continue.
                    System.out.println("Warning: SET SESSION failed: " + setEx.getMessage());
                }

                // Ensure staging table exists
                ExecuteStatementRequest createReq = ExecuteStatementRequest.builder()
                        .resourceArn(dbResourceArn)
                        .secretArn(dbSecretArn)
                        .database(database)
                        .sql(createTableSql)
                        .transactionId(txId)
                        .build();
                createStartMs = System.currentTimeMillis();
                ExecuteStatementResponse createResp = rds.executeStatement(createReq);
                createEndMs = System.currentTimeMillis();
                System.out.println("CREATE staging table response: " + createResp);

                // Run the LOAD into the staging table
                ExecuteStatementRequest loadReq = ExecuteStatementRequest.builder()
                        .resourceArn(dbResourceArn)
                        .secretArn(dbSecretArn)
                        .database(database)
                        .sql(loadSql)
                        .transactionId(txId)
                        .build();

                loadStartMs = System.currentTimeMillis();
                ExecuteStatementResponse loadResp = rds.executeStatement(loadReq);
                loadEndMs = System.currentTimeMillis();
                System.out.println("LOAD response: " + loadResp);

                // Count rows in staging
                String countStagingSql = "SELECT COUNT(*) FROM " + stagingTable + ";";
                countStartMs = System.currentTimeMillis();
                ExecuteStatementResponse countStagingResp = rds.executeStatement(ExecuteStatementRequest.builder()
                        .resourceArn(dbResourceArn)
                        .secretArn(dbSecretArn)
                        .database(database)
                        .sql(countStagingSql)
                        .transactionId(txId)
                        .build());
                countEndMs = System.currentTimeMillis();

                try {
                    stagingCount = Long.parseLong(countStagingResp.records().get(0).get(0).stringValue());
                } catch (Exception ex) {
                    System.out.println("Warning: couldn't parse staging count: " + ex.getMessage());
                }

                // Insert-clean SQL: convert common numeric/text fields safely while
                // inserting into the final production table `table`.
                // This maps many fields directly and applies safe CASTs/CASE for
                // numeric fields like host_id and price.
                // Insert only new rows: LEFT JOIN production table and filter
                // out existing primary keys to avoid Duplicate entry errors.
                String insertSql = "INSERT INTO " + table + " (" +
                        "id, listing_url, last_scraped, name, description, property_type, room_type, host_id, host_name, host_since, host_response_time, host_response_rate, host_acceptance_rate, host_is_superhost, host_listings_count, host_identity_verified, street, neighbourhood, neighbourhood_cleansed, neighbourhood_group_cleansed, city, state, zipcode, latitude, longitude, is_location_exact, accommodates, bathrooms, bedrooms, beds, bed_type, amenities, square_feet, price, weekly_price, monthly_price, security_deposit, cleaning_fee, guests_included, extra_people, minimum_nights, maximum_nights, instant_bookable, cancellation_policy, has_availability, availability_30, availability_60, availability_90, availability_365, number_of_reviews, first_review, last_review, review_scores_rating, review_scores_accuracy, review_scores_cleanliness, review_scores_checkin, review_scores_communication, review_scores_location, review_scores_value, reviews_per_month, price_category, review_category, host_category, availability_category, room_type_simplified, is_professional_host, has_cleaning_fee, price_per_guest) "
                        +
                        "SELECT " +
                        // id
                        "TRIM(id), TRIM(listing_url), TRIM(last_scraped), TRIM(name), TRIM(description), TRIM(property_type), TRIM(room_type), "
                        +
                        // host_id -> integer if numeric
                        "(CASE WHEN TRIM(host_id) REGEXP '^[0-9]+$' THEN CAST(TRIM(host_id) AS SIGNED) ELSE NULL END), "
                        +
                        "TRIM(host_name), " +
                        "(CASE WHEN TRIM(host_since) = '' THEN NULL " +
                        "WHEN UPPER(TRIM(host_since)) IN ('N/A','NA','NULL') THEN NULL " +
                        "WHEN TRIM(host_since) REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2}$' THEN STR_TO_DATE(TRIM(host_since), '%m/%d/%y') "
                        +
                        "WHEN TRIM(host_since) REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN STR_TO_DATE(TRIM(host_since), '%m/%d/%Y') "
                        +
                        "WHEN TRIM(host_since) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(TRIM(host_since), '%Y-%m-%d') "
                        +
                        "ELSE NULL END), TRIM(host_response_time), TRIM(host_response_rate), TRIM(host_acceptance_rate), TRIM(host_is_superhost), "
                        +
                        "(CASE WHEN TRIM(host_listings_count) REGEXP '^[0-9]+$' THEN CAST(TRIM(host_listings_count) AS SIGNED) ELSE NULL END), TRIM(host_identity_verified), TRIM(street), TRIM(neighbourhood), TRIM(neighbourhood_cleansed), TRIM(neighbourhood_group_cleansed), TRIM(city), TRIM(state), TRIM(zipcode), "
                        +
                        // latitude/longitude as decimal
                        "(CASE WHEN TRIM(latitude) REGEXP '^[-+]?[0-9]*\\.?[0-9]+$' THEN CAST(TRIM(latitude) AS DECIMAL(12,8)) ELSE NULL END), "
                        +
                        "(CASE WHEN TRIM(longitude) REGEXP '^[-+]?[0-9]*\\.?[0-9]+$' THEN CAST(TRIM(longitude) AS DECIMAL(12,8)) ELSE NULL END), "
                        +
                        "TRIM(is_location_exact), " +
                        // accommodates, bathrooms, bedrooms, beds
                        "(CASE WHEN TRIM(accommodates) REGEXP '^[0-9]+$' THEN CAST(TRIM(accommodates) AS SIGNED) ELSE NULL END), "
                        +
                        "(CASE WHEN TRIM(bathrooms) REGEXP '^[0-9]+(\\.[0-9]+)?$' THEN CAST(TRIM(bathrooms) AS DECIMAL(6,2)) ELSE NULL END), "
                        +
                        "(CASE WHEN TRIM(bedrooms) REGEXP '^[0-9]+(\\.[0-9]+)?$' THEN CAST(TRIM(bedrooms) AS DECIMAL(6,2)) ELSE NULL END), "
                        +
                        "(CASE WHEN TRIM(beds) REGEXP '^[0-9]+(\\.[0-9]+)?$' THEN CAST(TRIM(beds) AS DECIMAL(6,2)) ELSE NULL END), "
                        +
                        "TRIM(bed_type), TRIM(amenities), " +
                        "(CASE WHEN TRIM(square_feet) REGEXP '^[0-9]+$' THEN CAST(TRIM(square_feet) AS SIGNED) ELSE NULL END), "
                        +
                        // price-like fields: strip $ and commas
                        "(CASE WHEN REPLACE(REPLACE(TRIM(price), '\\$', ''), ',', '') REGEXP '^[0-9]+(\\.[0-9]+)?$' THEN CAST(REPLACE(REPLACE(TRIM(price), '\\$', ''), ',', '') AS DECIMAL(10,2)) ELSE NULL END), "
                        +
                        "(CASE WHEN REPLACE(REPLACE(TRIM(weekly_price), '\\$', ''), ',', '') REGEXP '^[0-9]+(\\.[0-9]+)?$' THEN CAST(REPLACE(REPLACE(TRIM(weekly_price), '\\$', ''), ',', '') AS DECIMAL(10,2)) ELSE NULL END), "
                        +
                        "(CASE WHEN REPLACE(REPLACE(TRIM(monthly_price), '\\$', ''), ',', '') REGEXP '^[0-9]+(\\.[0-9]+)?$' THEN CAST(REPLACE(REPLACE(TRIM(monthly_price), '\\$', ''), ',', '') AS DECIMAL(10,2)) ELSE NULL END), "
                        +
                        "(CASE WHEN REPLACE(REPLACE(TRIM(security_deposit), '\\$', ''), ',', '') REGEXP '^[0-9]+(\\.[0-9]+)?$' THEN CAST(REPLACE(REPLACE(TRIM(security_deposit), '\\$', ''), ',', '') AS DECIMAL(10,2)) ELSE NULL END), "
                        +
                        "(CASE WHEN REPLACE(REPLACE(TRIM(cleaning_fee), '\\$', ''), ',', '') REGEXP '^[0-9]+(\\.[0-9]+)?$' THEN CAST(REPLACE(REPLACE(TRIM(cleaning_fee), '\\$', ''), ',', '') AS DECIMAL(10,2)) ELSE NULL END), "
                        +
                        "(CASE WHEN TRIM(guests_included) REGEXP '^[0-9]+$' THEN CAST(TRIM(guests_included) AS SIGNED) ELSE NULL END), "
                        +
                        "(CASE WHEN REPLACE(REPLACE(TRIM(extra_people), '\\$', ''), ',', '') REGEXP '^[0-9]+(\\.[0-9]+)?$' THEN CAST(REPLACE(REPLACE(TRIM(extra_people), '\\$', ''), ',', '') AS DECIMAL(10,2)) ELSE NULL END), "
                        +
                        "(CASE WHEN TRIM(minimum_nights) REGEXP '^[0-9]+$' THEN CAST(TRIM(minimum_nights) AS SIGNED) ELSE NULL END), "
                        +
                        "(CASE WHEN TRIM(maximum_nights) REGEXP '^[0-9]+$' THEN CAST(TRIM(maximum_nights) AS SIGNED) ELSE NULL END), TRIM(instant_bookable), TRIM(cancellation_policy), TRIM(has_availability), "
                        +
                        "(CASE WHEN TRIM(availability_30) REGEXP '^[0-9]+$' THEN CAST(TRIM(availability_30) AS SIGNED) ELSE NULL END), "
                        +
                        "(CASE WHEN TRIM(availability_60) REGEXP '^[0-9]+$' THEN CAST(TRIM(availability_60) AS SIGNED) ELSE NULL END), "
                        +
                        "(CASE WHEN TRIM(availability_90) REGEXP '^[0-9]+$' THEN CAST(TRIM(availability_90) AS SIGNED) ELSE NULL END), "
                        +
                        "(CASE WHEN TRIM(availability_365) REGEXP '^[0-9]+$' THEN CAST(TRIM(availability_365) AS SIGNED) ELSE NULL END), "
                        +
                        "(CASE WHEN TRIM(number_of_reviews) REGEXP '^[0-9]+$' THEN CAST(TRIM(number_of_reviews) AS SIGNED) ELSE NULL END), "
                        +
                        "(CASE WHEN TRIM(first_review) = '' THEN NULL " +
                        "WHEN UPPER(TRIM(first_review)) IN ('N/A','NA','NULL') THEN NULL " +
                        "WHEN TRIM(first_review) REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2}$' THEN STR_TO_DATE(TRIM(first_review), '%m/%d/%y') "
                        +
                        "WHEN TRIM(first_review) REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN STR_TO_DATE(TRIM(first_review), '%m/%d/%Y') "
                        +
                        "WHEN TRIM(first_review) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(TRIM(first_review), '%Y-%m-%d') "
                        +
                        "ELSE NULL END), " +
                        "(CASE WHEN TRIM(last_review) = '' THEN NULL " +
                        "WHEN UPPER(TRIM(last_review)) IN ('N/A','NA','NULL') THEN NULL " +
                        "WHEN TRIM(last_review) REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2}$' THEN STR_TO_DATE(TRIM(last_review), '%m/%d/%y') "
                        +
                        "WHEN TRIM(last_review) REGEXP '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' THEN STR_TO_DATE(TRIM(last_review), '%m/%d/%Y') "
                        +
                        "WHEN TRIM(last_review) REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(TRIM(last_review), '%Y-%m-%d') "
                        +
                        "ELSE NULL END), "
                        +
                        "(CASE WHEN TRIM(review_scores_rating) REGEXP '^[0-9]+(\\.[0-9]+)?$' THEN CAST(TRIM(review_scores_rating) AS DECIMAL(5,2)) ELSE NULL END), "
                        +
                        "(CASE WHEN TRIM(review_scores_accuracy) REGEXP '^[0-9]+$' THEN CAST(TRIM(review_scores_accuracy) AS SIGNED) ELSE NULL END), "
                        +
                        "(CASE WHEN TRIM(review_scores_cleanliness) REGEXP '^[0-9]+$' THEN CAST(TRIM(review_scores_cleanliness) AS SIGNED) ELSE NULL END), "
                        +
                        "(CASE WHEN TRIM(review_scores_checkin) REGEXP '^[0-9]+$' THEN CAST(TRIM(review_scores_checkin) AS SIGNED) ELSE NULL END), "
                        +
                        "(CASE WHEN TRIM(review_scores_communication) REGEXP '^[0-9]+$' THEN CAST(TRIM(review_scores_communication) AS SIGNED) ELSE NULL END), "
                        +
                        "(CASE WHEN TRIM(review_scores_location) REGEXP '^[0-9]+$' THEN CAST(TRIM(review_scores_location) AS SIGNED) ELSE NULL END), "
                        +
                        "(CASE WHEN TRIM(review_scores_value) REGEXP '^[0-9]+$' THEN CAST(TRIM(review_scores_value) AS SIGNED) ELSE NULL END), "
                        +
                        "(CASE WHEN TRIM(reviews_per_month) REGEXP '^[0-9]+(\\.[0-9]+)?$' THEN CAST(TRIM(reviews_per_month) AS DECIMAL(6,2)) ELSE NULL END), TRIM(price_category), TRIM(review_category), TRIM(host_category), TRIM(availability_category), TRIM(room_type_simplified), TRIM(is_professional_host), TRIM(has_cleaning_fee), "
                        +
                        // price_per_guest
                        "(CASE WHEN REPLACE(REPLACE(TRIM(price_per_guest), '\\$', ''), ',', '') REGEXP '^[0-9]+(\\.[0-9]+)?$' THEN CAST(REPLACE(REPLACE(TRIM(price_per_guest), '\\$', ''), ',', '') AS DECIMAL(10,2)) ELSE NULL END) "
                        +
                        "FROM " + stagingTable;

                ExecuteStatementRequest insertReq = ExecuteStatementRequest.builder()
                        .resourceArn(dbResourceArn)
                        .secretArn(dbSecretArn)
                        .database(database)
                        .sql(insertSql)
                        .transactionId(txId)
                        .build();

                insertStartMs = System.currentTimeMillis();
                ExecuteStatementResponse insertResp = rds.executeStatement(insertReq);
                insertEndMs = System.currentTimeMillis();
                System.out.println("INSERT-SELECT response: " + insertResp);

                inserted = insertResp.numberOfRecordsUpdated() != null ? insertResp.numberOfRecordsUpdated() : -1;

                // Report bad rows: show up to 50 rows where host_id is non-numeric
                // or price is non-numeric after stripping $ and commas.
                String badRowsSql = "SELECT id, host_id, price FROM " + stagingTable
                        + " WHERE (NOT (TRIM(host_id) REGEXP '^[0-9]+$')) OR (NOT (REPLACE(REPLACE(TRIM(price),'$',''),',','') REGEXP '^[0-9]+(\\\\.[0-9]+)?$')) LIMIT 50;";
                badRowsStartMs = System.currentTimeMillis();
                ExecuteStatementResponse badRowsResp = rds.executeStatement(ExecuteStatementRequest.builder()
                        .resourceArn(dbResourceArn)
                        .secretArn(dbSecretArn)
                        .database(database)
                        .sql(badRowsSql)
                        .transactionId(txId)
                        .build());
                badRowsEndMs = System.currentTimeMillis();

                StringBuilder badRowsReport = new StringBuilder();
                if (badRowsResp.records() != null && !badRowsResp.records().isEmpty()) {
                    for (java.util.List<software.amazon.awssdk.services.rdsdata.model.Field> row : badRowsResp
                            .records()) {
                        String rid = row.size() > 0 && row.get(0).stringValue() != null ? row.get(0).stringValue() : "";
                        String rh = row.size() > 1 && row.get(1).stringValue() != null ? row.get(1).stringValue() : "";
                        String rp = row.size() > 2 && row.get(2).stringValue() != null ? row.get(2).stringValue() : "";
                        badRowsReport.append(String.format("id=%s host_id=%s price=%s\n", rid, rh, rp));
                    }
                    badRowsShown = badRowsResp.records().size();
                }

                System.out.println("Staging rows: " + stagingCount + ", inserted (approx): " + inserted
                        + ", bad rows shown: " + (badRowsReport.length() > 0 ? badRowsResp.records().size() : 0));
                if (badRowsReport.length() > 0) {
                    System.out.println("Bad rows (id host_id price):\n" + badRowsReport.toString());
                }
                commitStartMs = System.currentTimeMillis();
                CommitTransactionResponse commit = rds.commitTransaction(CommitTransactionRequest.builder()
                        .resourceArn(dbResourceArn)
                        .secretArn(dbSecretArn)
                        .transactionId(txId)
                        .build());
                commitEndMs = System.currentTimeMillis();
                System.out.println("Commit response: " + commit);

                long totalEndMs = System.currentTimeMillis();

                // Emit structured metrics for benchmarking/monitoring
                String metricsJson = "{"
                        + "\"operation\":\"aurora_bulk_load\","
                        + "\"database\":\"" + database + "\"," + "\"table\":\"" + table + "\","
                        + "\"staging_count\":\"" + stagingCount + "\"," + "\"inserted\":\"" + inserted + "\","
                        + "\"bad_rows_shown\":\"" + badRowsShown + "\"," + "\"timings_ms\":{"
                        + "\"set_ms\":\"" + (setEndMs - setStartMs) + "\"," + "\"create_ms\":\""
                        + (createEndMs - createStartMs) + "\"," + "\"load_ms\":\"" + (loadEndMs - loadStartMs) + "\","
                        + "\"count_ms\":\"" + (countEndMs - countStartMs) + "\"," + "\"insert_ms\":\""
                        + (insertEndMs - insertStartMs) + "\"," + "\"badrows_ms\":\"" + (badRowsEndMs - badRowsStartMs)
                        + "\"," + "\"commit_ms\":\"" + (commitEndMs - commitStartMs) + "\"," + "\"total_ms\":\""
                        + (totalEndMs - totalStartMs) + "\"}"
                        + "}";

                System.out.println("METRICS: " + metricsJson);
            } catch (Exception e) {
                // Attempt to rollback; ignore rollback errors to preserve original exception
                long totalFailMs = System.currentTimeMillis();
                try {
                    RollbackTransactionRequest rb = RollbackTransactionRequest.builder()
                            .resourceArn(dbResourceArn)
                            .secretArn(dbSecretArn)
                            .transactionId(txId)
                            .build();
                    RollbackTransactionResponse rbResp = rds.rollbackTransaction(rb);
                    System.out.println("Rollback response: " + rbResp);
                } catch (Exception rbEx) {
                    System.out.println("Rollback failed: " + rbEx.getMessage());
                }

                // Emit failure metrics
                String failMetrics = "{"
                        + "\"operation\":\"aurora_bulk_load\","
                        + "\"database\":\"" + database + "\"," + "\"table\":\"" + table + "\","
                        + "\"error\":\"" + e.getMessage().replaceAll("\"", "\\\\\"") + "\"," + "\"timings_ms\":{"
                        + "\"since_tx_start_ms\":\"" + (totalFailMs - txStartMs) + "\"," + "\"total_ms\":\""
                        + (totalFailMs - totalStartMs) + "\"}"
                        + "}";
                System.out.println("METRICS: " + failMetrics);

                throw e;
            }
        }
    }
    // Removed download/upload helpers — Lambda no longer uploads files to S3.

    /**
     * Download the CSV from S3 and load it into a local SQLite database for
     * benchmarking.
     * sqlitePath can be a file path (e.g. `/tmp/saaf.db`) and sqliteTable the table
     * name.
     */
    public static void loadS3ToSqlite(String s3Bucket, String s3Key, Region region, String sqlitePath,
            String sqliteTable) throws Exception {
        long startMs = System.currentTimeMillis();
        long downloadStartMs = System.currentTimeMillis();
        Path temp = Files.createTempFile("saaf_csv_", ".csv");
        S3Client s3 = S3Client.builder().region(region).build();
        GetObjectRequest gor = GetObjectRequest.builder().bucket(s3Bucket).key(s3Key).build();
        try (ResponseInputStream<GetObjectResponse> in = s3.getObject(gor);
                OutputStream out = Files.newOutputStream(temp, StandardOpenOption.TRUNCATE_EXISTING)) {
            byte[] buf = new byte[8192];
            int n;
            while ((n = in.read(buf)) > 0) {
                out.write(buf, 0, n);
            }
        }
        long downloadEndMs = System.currentTimeMillis();

        // Parse CSV and insert into SQLite
        long parseStartMs = System.currentTimeMillis();
        long rowsLoaded = 0L;
        try (Reader reader = Files.newBufferedReader(temp);
                CSVParser parser = new CSVParser(reader, CSVFormat.DEFAULT.withFirstRecordAsHeader())) {
            if (parser.getHeaderMap() == null || parser.getHeaderMap().isEmpty()) {
                throw new IllegalArgumentException("CSV appears to have no header row");
            }

            java.util.List<String> cols = new java.util.ArrayList<>();
            for (String col : parser.getHeaderMap().keySet()) {
                String sanitized = col == null ? "col" : col.trim().replaceAll("[^A-Za-z0-9_]", "_");
                if (sanitized.length() == 0)
                    sanitized = "col";
                cols.add(sanitized);
            }

            String jdbc = "jdbc:sqlite:" + sqlitePath;
            try (Connection conn = DriverManager.getConnection(jdbc)) {
                conn.setAutoCommit(false);

                // Create table with TEXT columns
                StringBuilder create = new StringBuilder();
                create.append("CREATE TABLE IF NOT EXISTS ").append(sqliteTable).append(" (");
                for (int i = 0; i < cols.size(); i++) {
                    if (i > 0)
                        create.append(", ");
                    create.append('"').append(cols.get(i)).append('"').append(" TEXT");
                }
                create.append(");");
                try (Statement st = conn.createStatement()) {
                    st.execute(create.toString());
                }

                // Prepare insert
                StringBuilder insert = new StringBuilder();
                insert.append("INSERT INTO ").append(sqliteTable).append(" (");
                for (int i = 0; i < cols.size(); i++) {
                    if (i > 0)
                        insert.append(",");
                    insert.append('"').append(cols.get(i)).append('"');
                }
                insert.append(") VALUES (");
                for (int i = 0; i < cols.size(); i++) {
                    if (i > 0)
                        insert.append(',');
                    insert.append('?');
                }
                insert.append(");");

                try (PreparedStatement ps = conn.prepareStatement(insert.toString())) {
                    int batch = 0;
                    final int batchSize = 1000;
                    for (CSVRecord rec : parser) {
                        for (int i = 0; i < cols.size(); i++) {
                            String val = null;
                            try {
                                val = rec.get(i);
                            } catch (Exception ex) {
                                val = null;
                            }
                            ps.setString(i + 1, val);
                        }
                        ps.addBatch();
                        batch++;
                        rowsLoaded++;
                        if (batch >= batchSize) {
                            ps.executeBatch();
                            conn.commit();
                            batch = 0;
                        }
                    }
                    if (batch > 0) {
                        ps.executeBatch();
                        conn.commit();
                    }
                }
                conn.setAutoCommit(true);
            }
            long parseEndMs = System.currentTimeMillis();

            long totalEndMs = System.currentTimeMillis();

            // Emit structured metrics for SQLite load
            String metricsJson = "{" +
                    "\"operation\":\"sqlite_bulk_load\"," +
                    "\"s3_bucket\":\"" + s3Bucket + "\"," +
                    "\"s3_key\":\"" + s3Key.replaceAll("\"", "\\\\\"") + "\"," +
                    "\"sqlite_path\":\"" + sqlitePath + "\"," +
                    "\"sqlite_table\":\"" + sqliteTable + "\"," +
                    "\"rows_loaded\":\"" + rowsLoaded + "\"," +
                    "\"timings_ms\":{\"download_ms\":\"" + (downloadEndMs - downloadStartMs)
                    + "\",\"parse_insert_ms\":\"" + (parseEndMs - parseStartMs) + "\",\"total_ms\":\""
                    + (totalEndMs - startMs) + "\"}}";

            System.out.println("METRICS: " + metricsJson);
        } finally {
            try {
                Files.deleteIfExists(temp);
            } catch (IOException ignored) {
            }
        }
    }

    /**
     * Lambda handler that accepts EventBridge events. Event detail should contain
     * either:
     * - { "url": "https://.../file.csv" }
     * - { "s3Bucket": "bucket", "s3Key": "path/to/file.csv" }
     *
     * Environment variables (recommended):
     * - `S3_BUCKET` (optional if event provides bucket)
     * - `S3_PREFIX` (optional prefix to use when uploading)
     * - `DB_RESOURCE_ARN` (required)
     * - `DB_SECRET_ARN` (required)
     * - `DATABASE` (required)
     * - `TABLE` (required)
     * - `AWS_REGION` (optional; fallback to SDK default)
     */
    public static class LambdaHandler implements RequestHandler<Map<String, Object>, String> {

        @Override
        public String handleRequest(Map<String, Object> event, Context context) {
            context.getLogger().log("Received event: " + event);

            Map<String, Object> detail = null;
            if (event.containsKey("detail") && event.get("detail") instanceof Map) {
                // EventBridge wraps payload in "detail"
                detail = (Map<String, Object>) event.get("detail");
            } else {
                detail = event;
            }

            // Accept S3 event shapes only. For this simplified flow we do not
            // download or upload objects — Aurora must be able to read the
            // S3 object itself.
            String s3Bucket = null;
            String s3Key = null;
            if (detail.get("s3Bucket") != null) {
                s3Bucket = detail.get("s3Bucket").toString();
            } else if (detail.get("bucket") instanceof Map) {
                Object name = ((Map<?, ?>) detail.get("bucket")).get("name");
                if (name != null)
                    s3Bucket = name.toString();
            }
            if (detail.get("s3Key") != null) {
                s3Key = detail.get("s3Key").toString();
            } else if (detail.get("object") instanceof Map) {
                Object k = ((Map<?, ?>) detail.get("object")).get("key");
                if (k != null)
                    s3Key = k.toString();
            }

            if (s3Bucket == null)
                s3Bucket = System.getenv("S3_BUCKET");
            if (s3Key != null) {
                try {
                    s3Key = URLDecoder.decode(s3Key, StandardCharsets.UTF_8.name());
                } catch (Exception ignore) {
                }
            }

            String dbResourceArn = System.getenv("DB_RESOURCE_ARN");
            String dbSecretArn = System.getenv("DB_SECRET_ARN");
            String database = System.getenv("DATABASE");
            String table = System.getenv("TABLE");
            String regionName = System.getenv("AWS_REGION");

            // Check if caller wants to load into SQLite for benchmarking
            boolean targetSqlite = false;
            Object t = detail.get("target");
            if (t != null && t.toString().equalsIgnoreCase("sqlite"))
                targetSqlite = true;
            Object a = detail.get("action");
            if (!targetSqlite && a != null && a.toString().equalsIgnoreCase("sqlite"))
                targetSqlite = true;

            if (!targetSqlite && (dbResourceArn == null || dbSecretArn == null || database == null || table == null)) {
                String msg = "Missing required environment variables: DB_RESOURCE_ARN, DB_SECRET_ARN, DATABASE, TABLE";
                context.getLogger().log(msg);
                throw new IllegalStateException(msg);
            }

            Region region = regionName != null ? Region.of(regionName)
                    : Region.of(System.getenv("AWS_DEFAULT_REGION") != null ? System.getenv("AWS_DEFAULT_REGION")
                            : "us-east-2");

            try {
                if (s3Bucket == null || s3Key == null) {
                    String msg = "Event must contain both 's3Bucket' and 's3Key' (or set S3_BUCKET env).";
                    context.getLogger().log(msg);
                    throw new IllegalArgumentException(msg);
                }

                if (targetSqlite) {
                    String sqlitePath = System.getenv("SQLITE_PATH");
                    if (sqlitePath == null || sqlitePath.isEmpty())
                        sqlitePath = "/tmp/saaf.db";
                    String sqliteTable = System.getenv("SQLITE_TABLE");
                    if (sqliteTable == null || sqliteTable.isEmpty())
                        sqliteTable = "listings_sqlite";
                    loadS3ToSqlite(s3Bucket, s3Key, region, sqlitePath, sqliteTable);
                } else {
                    executeLoadFromS3(s3Bucket, s3Key, region, dbResourceArn, dbSecretArn, database, table);
                }
            } catch (Exception e) {
                context.getLogger().log("Error processing load: " + e.getMessage());
                throw new RuntimeException(e);
            }

            return "OK";
        }
    }

    /**
     * Dedicated Lambda handler that always loads the S3 CSV into SQLite.
     * Use this as the handler for a separate Lambda function if you want
     * one function focused on local SQLite benchmarking.
     *
     * Expected event shape: same S3 object fields as the main handler
     * - { "s3Bucket": "bucket", "s3Key": "path/to/file.csv" }
     * Environment variables:
     * - SQLITE_PATH (optional, default `/tmp/saaf.db`)
     * - SQLITE_TABLE (optional, default `listings_sqlite`)
     */
    public static class SqliteLambdaHandler implements RequestHandler<Map<String, Object>, String> {

        @Override
        public String handleRequest(Map<String, Object> event, Context context) {
            context.getLogger().log("SqliteLambdaHandler received event: " + event);

            Map<String, Object> detail = null;
            if (event.containsKey("detail") && event.get("detail") instanceof Map) {
                detail = (Map<String, Object>) event.get("detail");
            } else {
                detail = event;
            }

            String s3Bucket = null;
            String s3Key = null;
            if (detail.get("s3Bucket") != null) {
                s3Bucket = detail.get("s3Bucket").toString();
            } else if (detail.get("bucket") instanceof Map) {
                Object name = ((Map<?, ?>) detail.get("bucket")).get("name");
                if (name != null)
                    s3Bucket = name.toString();
            }
            if (detail.get("s3Key") != null) {
                s3Key = detail.get("s3Key").toString();
            } else if (detail.get("object") instanceof Map) {
                Object k = ((Map<?, ?>) detail.get("object")).get("key");
                if (k != null)
                    s3Key = k.toString();
            }

            if (s3Bucket == null)
                s3Bucket = System.getenv("S3_BUCKET");
            if (s3Key != null) {
                try {
                    s3Key = URLDecoder.decode(s3Key, StandardCharsets.UTF_8.name());
                } catch (Exception ignore) {
                }
            }

            if (s3Bucket == null || s3Key == null) {
                String msg = "Event must contain both 's3Bucket' and 's3Key' (or set S3_BUCKET env).";
                context.getLogger().log(msg);
                throw new IllegalArgumentException(msg);
            }

            String regionName = System.getenv("AWS_REGION");
            Region region = regionName != null ? Region.of(regionName)
                    : Region.of(System.getenv("AWS_DEFAULT_REGION") != null ? System.getenv("AWS_DEFAULT_REGION")
                            : "us-east-2");

            String sqlitePath = System.getenv("SQLITE_PATH");
            if (sqlitePath == null || sqlitePath.isEmpty())
                sqlitePath = "/tmp/saaf.db";
            String sqliteTable = System.getenv("SQLITE_TABLE");
            if (sqliteTable == null || sqliteTable.isEmpty())
                sqliteTable = "listings_sqlite";

            try {
                loadS3ToSqlite(s3Bucket, s3Key, region, sqlitePath, sqliteTable);
            } catch (Exception e) {
                context.getLogger().log("Error loading to sqlite: " + e.getMessage());
                throw new RuntimeException(e);
            }

            return "OK";
        }
    }

}
