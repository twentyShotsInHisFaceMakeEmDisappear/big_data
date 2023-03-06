import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {

    public static void main(String[] args) {
        Dataset<Row> csvDataset = getDataset();

        csvDataset.show();

        Dataset<Row> result = csvDataset.select(csvDataset.col(Repositories.NAME),
                        csvDataset.col(Repositories.PULL_REQUESTS), csvDataset.col(Repositories.FORKS_COUNT))
                .filter(csvDataset.col(Repositories.PULL_REQUESTS).geq(1000))
                .filter(csvDataset.col(Repositories.FORKS_COUNT).leq(10));

        result.show(1000);
    }

    public static Dataset<Row> getDataset() {
        SparkSession sparkSession = SparkSession.builder()
                .master(Values.SESSION_MASTER)
                .appName(Values.SESSION_APPLICATION_NAME)
                .getOrCreate();

        return sparkSession.sqlContext().read().format(Values.SPARK_DATA_FORMAT)
                .option(Values.HEADER_OPTION, Boolean.TRUE).load(Values.DATASET_PATH);
    }

}
