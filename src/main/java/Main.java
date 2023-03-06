import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Main {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("Bigdata lessons")
                .getOrCreate();

        Dataset<Row> csvDataset = sparkSession.sqlContext().read().format(Values.SPARK_DATA_FORMAT)
                .option(Values.HEADER_OPTION, Boolean.TRUE).load(Values.DATASET_PATH);

        csvDataset.show();

        Dataset<Row> result = csvDataset.select(csvDataset.col(Repositories.NAME),
                        csvDataset.col(Repositories.PULL_REQUESTS), csvDataset.col(Repositories.FORKS_COUNT))
                .filter(csvDataset.col(Repositories.PULL_REQUESTS).geq(1000))
                .filter(csvDataset.col(Repositories.FORKS_COUNT).leq(10));

        result.show(1000);
    }

}
