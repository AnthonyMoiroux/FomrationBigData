import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions.{col, current_timestamp, lit, round, when}
import org.apache.spark.sql.types._

import java.io._
import scala.reflect.io.Path
object SparkTraining {

  val schema=StructType(
    List(
      StructField("OrderLine",IntegerType, false),
      StructField("Orderid",IntegerType, false),
      StructField("Productid",IntegerType, false),
      StructField("Shipdate",DateType, false),
      StructField("Billdate",DateType, false),
      StructField("Unitprice",DoubleType, false),
      StructField("Numunits",IntegerType, false),
      StructField("Totalprice",DoubleType, false)
    )
  )

  val schemaOrderNotLine=StructType(
    List(
      StructField("Orderid", IntegerType, false),
      StructField("CustomerId", IntegerType, false),
      StructField("CampaignId", IntegerType, false),
      StructField("OrderDate", DateType, false),
      StructField("City", StringType, false),
      StructField("State", StringType, false),
      StructField("ZipCode", StringType, false),
      StructField("PaymentType", StringType, false),
      StructField("TotalPrice", DoubleType, false),
      StructField("NumOrderLines", IntegerType, false),
      StructField("NumUnits", IntegerType, false)
    )
  )




  val schemaORCPARQUET=StructType(
    List(
      StructField("DestinationCountry",StringType, false),
      StructField("OriginCountry",StringType, false),
      StructField("NB",IntegerType, false)
    )
  )



  def main(args : Array[String]) : Unit ={

    //val hiveDir=new File("/D:system/hive")

    val ss=SparkSession.builder()
      .appName("Mon appli Spark")
      .master("local[*]")
     // .enableHiveSupport()
      .config("spark.sql.crossJoin.enabled","true")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.shuffle.compress","true")
     // .config("spark.sql.warehouse.dir",hiveDir.getAbsolutePath)
      //.enableHiveSupport
      .getOrCreate()

    val sc=ss.sparkContext.parallelize(Seq("anthony", "taylor", "juvenal"))
    sc.foreach{s=>println(s)}

    val rdd2 = ss.sparkContext.parallelize(List(34,56,89,78,954))
    rdd2.map(e => e*2).foreach{i => println(i)}

    val rdd3=ss.sparkContext.textFile("C:\\Users\\Administrateur\\Desktop\\data")
      .flatMap(t=> t.split(" "))

    //rdd3.foreach{s=>println(s)}

    //DF

    import ss.implicits._
    val df1=rdd3.toDF()
    //df1.show()

    val df_orders =ss.read
      .format("com.databricks.spark.csv")
      .schema(schema)
      .option("header","true")
      .option("delimiter","\t")
      .option("inferSchema","true")
      .load("C:\\Users\\Administrateur\\Desktop\\data\\orderline.txt")

    val df_ordersNotLine =ss.read
      .format("com.databricks.spark.csv")
      .schema(schemaOrderNotLine)
      .option("header","true")
      .option("delimiter","\t")
      .option("inferSchema","true")
      .load("C:\\Users\\Administrateur\\Desktop\\data\\orders.txt")


    //df_orders.show(5)

    val df_orc =ss.read
      .format("orc")
     // .schema(schemaORCPARQUET)
      .option("header","true")
      .orc("C:\\Users\\Administrateur\\Desktop\\data\\part-r-00000-2c4f7d96-e703-4de3-af1b-1441d172c80f.snappy.orc")
    println("ORC")
    df_orc.show(5)

    val df_parquet =ss.read
      .format("parquet")
      //.schema(schemaORCPARQUET)
      .option("header","true")
      .parquet("C:\\Users\\Administrateur\\Desktop\\data\\part-r-00000-1a9822ba-b8fb-4d8e-844a-ea30d0801b9e.gz.parquet")
    println("PARQUET")
    df_parquet.show(5)

    df_orders.printSchema()

    val listeCols = df_orders.columns
    listeCols.foreach(e=>println(e))

    //selection des colonnes
   /* val df_prix = df_orders.select(
      col(colName="Unitprice"),
      col(colName="Numunits"),
      col(colName="Totalprice"),
      col(colName="OrderLine").as("Order Line ID").cast(StringType)
    )*/

    val df_ordersTaxe=df_orders.withColumn("taxes",col("Numunits")*col("Totalprice")*lit(0.20))
                               .withColumn("Shipdate",current_timestamp())
                               .withColumn("promo",  round(when( col("TotalPrice")<200, 0  )
                                                             .when(col("TotalPrice")>200 && col("Totalprice")<600, 0.05)
                                                             .otherwise(0.07),2))
                               .withColumn("promoAccount", lit(col("promo")*col("Totalprice")))
                               .withColumn("totalBill",round(lit(col("Totalprice")-col("promoAccount")-col("taxes")),2))

    df_ordersTaxe.show(5)

    //val df_orderfiltre = df_ordersTaxes.filter(col("Numunits")===lit(2))
//df_ordersNotLine.show()

    df_orders.printSchema()
    df_ordersNotLine.printSchema()


    val df_join =df_orders.join(df_ordersNotLine,df_orders("Orderid")===df_ordersNotLine("Orderid"),joinType = "inner")
    df_join.show()

  //  val df_hive = ss.sql("SELECT * FROM hiveTable_Client LIMIT=100")
  //

    df_orders.createOrReplaceTempView("table_commandes")
    df_ordersNotLine.createOrReplaceTempView("table_commandesNonLines")
    val df_sql = ss.sql("SELECT * FROM table_commandes")
    df_sql.show(15)

    val df_sql2=ss.sql("SELECT tcn.city, sum(tcn.TotalPrice) FROM table_commandes tc INNER JOIN table_commandesNonLines tcn ON tc.Orderid=tcn.Orderid GROUP BY tcn.city ")
    df_sql2.show(10)

    val df_mysql=ss.read
      .format("jdbc")
      .option("url","jdbc:mysql://127.0.0.1:3306")
      .option("user","consultant")
      .option("password","pwd#86")
      .option("dbtable","(SELECT tc.city, sum(tc.TotalPrice) FROM jea_db.orders tc GROUP BY tc.city) requete")
      .load()
    df_mysql.show(2)


    /*df_mysql.repartition()
      .write
      .format("com.databricks.spark.csv")
      .option("header","true")
      .mode(SaveMode.Append)
      .csv("C:\\Users\\Administrateur\\Desktop\\data\\matable.csv")*/

   /* df_mysql.repartition(1)
      .write
      .partitionBy("city")
      .format("com.databricks.spark.csv")
      .option("header","true")
      .mode(SaveMode.Append)
      .csv("C:\\Users\\Administrateur\\Desktop\\data\\matable.csv")*/


   /* df_ordersNotLine.write
      .partitionBy("City")
      .mode(SaveMode.Overwrite)
      .format("com.databricks.spark.csv")
      .option("header","true")
      .csv("C:\\Users\\Administrateur\\Desktop\\data\\OrdersLine.csv")*/

  /*  df_orders.write
      .format("com.databricks.spark.csv")
      .option("header","true")
      .option("delimiter",";")
      .csv("C:\\Users\\Administrateur\\Desktop\\data\\orderLinecsv.csv")*/
  }


  //jdbc:mysql://127.0.0.1:3306?zeroDateTimeBehavior=CONVERT_TO_NULL&serverTimezone=UTC


}

