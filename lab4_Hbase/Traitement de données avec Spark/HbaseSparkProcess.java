import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.JavaPairRDD;

public class HbaseSparkProcess {

    public void createHbaseTable() {
        // Configuration HBase
        Configuration config = HBaseConfiguration.create();

        // Configuration Spark
        SparkConf sparkConf = new SparkConf()
                .setAppName("SparkHBaseTest")
                .setMaster("local[4]");

        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        // Indiquer la table HBase à lire
        config.set(TableInputFormat.INPUT_TABLE, "products");

        // Créer un RDD à partir de la table HBase
        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = jsc.newAPIHadoopRDD(
                config,
                TableInputFormat.class,
                ImmutableBytesWritable.class,
                Result.class
        );

        // Afficher le nombre d'enregistrements
        System.out.println("Nombre d'enregistrements: " + hBaseRDD.count());
    }

    public static void main(String[] args) {
        HbaseSparkProcess admin = new HbaseSparkProcess();
        admin.createHbaseTable();
    }
}
