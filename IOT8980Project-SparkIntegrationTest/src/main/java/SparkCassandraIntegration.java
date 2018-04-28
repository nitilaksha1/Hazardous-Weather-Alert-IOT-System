import com.datastax.spark.connector.cql.CassandraConnector;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraTableScanJavaRDD;
import org.apache.avro.ipc.specific.Person;
import org.apache.spark.SparkConf;
import com.datastax.spark.connector.*;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaRDD$;
import org.apache.spark.api.java.JavaSparkContext;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import org.apache.spark.api.java.JavaUtils;
import org.apache.commons.codec.binary.Base64;
import java.util.Arrays;
import java.util.List;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

public class SparkCassandraIntegration {

    public static void main(String [] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("Sample Cassandra Integration")
                .setMaster("local[4]")
                .set("spark.cassandra.connection.host", "localhost");

        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);

        List<SimpleData> people = Arrays.asList(
                new SimpleData(1.0, 24.24),
                new SimpleData(2.0, 13.14),
                new SimpleData(3.0, 24.24)
        );

        JavaRDD<SimpleData> rdd = javaSparkContext.parallelize(people);
        CassandraJavaUtil.javaFunctions(rdd).writerBuilder("test", "simple_table",
                mapToRow(SimpleData.class)).saveToCassandra();
    }
}
