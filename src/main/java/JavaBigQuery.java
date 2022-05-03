/**
 * Created by Kaitav Mehta on 2022-05-03
 */

import com.google.cloud.bigquery.*;

public class JavaBigQuery {
    public static void main(String[] args) throws Exception {

        String projectId = "pcb-poc-datalake1";
        String dataset = "denis_test_0001";
        String insertTable = "testTable";
        // Step 1: Initialize BigQuery service
        BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId("pcb-poc-datalake1")
                .build().getService();

        // Step 2: Prepare query job
        final String INSERT_VEGETABLES =
                "INSERT INTO " + projectId + "." + dataset + "." + insertTable + " (id, field1) VALUES ('1', 'carrot'), ('2', 'beans');";

        QueryJobConfiguration queryConfig =
                QueryJobConfiguration.newBuilder(INSERT_VEGETABLES).build();

        // Step 3: Run the job on BigQuery
        Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).build());
        queryJob = queryJob.waitFor();

        if (queryJob == null) {
            throw new Exception("job no longer exists");
        }
        // once the job is done, check if any error occured
        if (queryJob.getStatus().getError() != null) {
            throw new Exception(queryJob.getStatus().getError().toString());
        }


        // Step 4: Display results
        // Here, we will print the total number of rows that were inserted
        JobStatistics.QueryStatistics stats = queryJob.getStatistics();
        Long rowsInserted = stats.getDmlStats().getInsertedRowCount();
        System.out.printf("%d rows inserted\n", rowsInserted);
    }
}
