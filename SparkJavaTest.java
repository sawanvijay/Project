import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import java.util.Arrays;
import java.util.List;
import scala.collection.JavaConverters;
import scala.collection.Seq;


public class SparkJavaTest {
	
	
	
	
   

    public static Seq<String> convertListToSeq(List<String> inputList) {
        return JavaConverters.asScalaIteratorConverter(inputList.iterator()).asScala().toSeq();
    }

    public static void main(String[] args) {
    	
    	System.setProperty("hadoop.home.dir", "C:/winutils");
    	
   	 Logger.getLogger("org").setLevel(Level.ERROR);
	    Logger.getLogger("org").setLevel(Level.WARN);
   	 
   	 
 	final SparkSession  spark = SparkSession
             .builder()
             .appName("JavaSparkTest")
             .master("local")
             .getOrCreate();
   	 
   	 
        Dataset<Row> ds = spark.read().option("header",true).csv("C:/Users/vijayvergiya_sa/Desktop/Dataset/spark_file.csv");

        ds.show();
        List<String> columns = Arrays.asList("InvoiceNo","StockCode","Description");

        //using selectExpr
        ds.selectExpr(convertListToSeq(columns)).show(false);

        //using select => this first column will be added to select
        List<String> columns2 = Arrays.asList("StockCode","Description");

        ds.select("InvoiceNo", convertListToSeq(columns2)).show(false);

    }
}
