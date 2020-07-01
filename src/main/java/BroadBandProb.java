
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
public class BroadBandProb {

    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession
                .builder()
                .master("local[*]")
                .appName("BroadBandProb")
                .getOrCreate();

        sparkSession.sparkContext().setLogLevel("ERROR");

        Dataset<Row> broadband = sparkSession.read().option("multiLine", true).json("/Users/313248/IdeaProjects/spark-test/src/main/resources/assignment");

        broadband.createOrReplaceTempView("broadband");

        Dataset<Row> event = sparkSession.sql("" +
                "SELECT " +
                "calc_userid," +
                "eventlaenc," +
                "RANK() over(partition by calc_userid order by timestamp) as rnk,  " +
                "COUNT(*) over(partition by calc_userid,eventlaenc) as cnt " +
                "from broadband");

        event.createOrReplaceTempView("event");

        //  sparkSession.sql("SELECT * FROM event WHERE eventlaenc = '107' and calc_userid = '1481'").show(100);

        Dataset<Row> userActionResult = sparkSession.sql("select calc_userid, " +
                "coalesce(CASE WHEN rnk=1 THEN eventlaenc END,'') AS first_action, " +
                "coalesce(CASE WHEN rnk=1 THEN cnt END,'') as first_action_count, " +
                "coalesce(CASE WHEN rnk=2 THEN eventlaenc END,'') AS second_action," +
                "coalesce(CASE WHEN rnk=2 THEN cnt END,'') as second_action_count " +
                "from event");

        userActionResult.createOrReplaceTempView("user_action_result");

        sparkSession.sql("SELECT COALESCE(tab1.calc_userid,tab2.calc_userid) as calc_userid," +
                "COALESCE(tab1.first_action,'') as first_action," +
                "COALESCE(tab1.first_action_count,'') as first_action_count," +
                "COALESCE(tab2.second_action,'') as second_action," +
                "COALESCE(tab2.second_action_count,'') as second_action_count " +
                " FROM " +
                "(SELECT DISTINCT calc_userid,first_action,first_action_count FROM user_action_result where first_action <> '') tab1 " +
                " FULL OUTER JOIN " +
                "(SELECT DISTINCT calc_userid,second_action,second_action_count FROM user_action_result where second_action <> '') tab2" +
                " ON tab1.calc_userid = tab2.calc_userid").where("calc_userid = 193").show();
    }
}
