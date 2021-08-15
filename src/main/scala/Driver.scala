import org.apache.spark.SparkConf
import org.apache.spark.sql._
import com.databricks.spark.avro._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Encoder, Encoders, SparkSession}
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions
import org.apache.spark.sql.expressions.MutableAggregationBuffer
import org.apache.spark.sql.expressions.UserDefinedAggregateFunction
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import scala.collection.mutable.ListBuffer


object Driver {
  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("Spark-Assignment") // create spark config

    val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate() // create spark session

    val schemaText = spark.sparkContext.wholeTextFiles("./src/main/resources/outputSchema.avsc").collect()(0)._2 // column schema

    val ip1 = "test_data/sample1/part-00000-a4794630-9584-4152-b5ed-595fd7608322.avro"
    val ip2 = "test_data/sample2/part-00000-64940e90-dcc3-4d23-9a38-c7e1e1d24820.avro"
    val ip3 = "test_data/sample3/part-00000-ec4ac14a-4d9b-4675-b2a5-224784c22950.avro"
    val ip4 = "test_data/sample4/part-00000-a3521293-47b8-4b2c-983a-2b973319fd51.avro"
    val ip5 = "test_data/sample4/part-00001-a3521293-47b8-4b2c-983a-2b973319fd51.avro"

    val df1 = spark.read.option("avroSchema", schemaText).avro(ip1)
    val df2 = spark.read.option("avroSchema", schemaText).avro(ip2)
    val df3 = spark.read.option("avroSchema", schemaText).avro(ip3)
    val df4 = spark.read.option("avroSchema", schemaText).avro(ip4)
    val df5 = spark.read.option("avroSchema", schemaText).avro(ip5)

    // Dataframes with diff DPIDs
    val df_4 = df1.toDF()
    val df_212 = df2.union(df3).toDF()
    val df_316 = df4.union(df5).toDF()

    // Intermediate : Join

    val merged1 = df_4.drop("Language").join(df_212.select("Identifier","Language"),"Identifier").drop("DpId")

    var mergedDF = merged1.join(df_316.select("Identifier"),"Identifier")

    mergedDF = mergedDF.withColumn("Gender_dpid",when(col("Gender_dpid").isNull,"4"))

    mergedDF = mergedDF.withColumn("Device_dpid",when(col("Device_dpid").isNull,"4"))

    mergedDF = mergedDF.withColumn("Age_dpid",when(col("Age_dpid").isNull,"4"))

    mergedDF = mergedDF.withColumn("Zipcode_dpid",when(col("Zipcode_dpid").isNull,"4"))

    mergedDF = mergedDF.withColumn("Language_dpid",when(col("Language_dpid").isNull,"212"))

    //mergedDF.show(false) // mergedDF with dropped DPId

    //Record Overlap between two Datasets
    def overlapRecord(xs: Dataset[Row], ys: Dataset[Row]): Dataset[Row] = xs.select("Identifier").intersect(ys.select("Identifier"))

    // Distinct Identifier counts
    def distinctIdentifier(xs: Dataset[Row], ys: Dataset[Row],zs : Dataset[Row]): Long =
      xs.select("Identifier").union(ys.select("Identifier")).union(zs.select("Identifier")).count()

    // Auxiliary Function to group by Age for a given dataset
    def grpByAge(mergedDataset: Dataset[Row]): Dataset[Row] = {

      mergedDataset.groupBy("DpId", "Age").agg(count("Age").as("AgeAggregate")).
        groupBy("DpId").agg(collect_list(struct("Age", "AgeAggregate")).as("Age_count"))

    }
    // Auxiliary Function to group by Gender for a given dataset
    def grpByGender(mergedDataset: Dataset[Row]): Dataset[Row] = {

      mergedDataset.groupBy("DpId", "Gender").agg(count("Gender").as("GenderAggregate")).
        groupBy("DpId").agg(collect_list(struct("Gender", "GenderAggregate")).as("Gender_count"))

    }

    // Auxiliary Function to group by Zip for a given dataset
    def grpByZipcode(mergedDataset: Dataset[Row]): Dataset[Row] = {

      mergedDataset.groupBy("DpId", "Zipcode").agg(count("Zipcode").as("ZipAggregate")).
        groupBy("DpId").agg(collect_list(struct("Zipcode", "ZipAggregate")).as("Zipcode_count"))

    }

    // Age grouping for different DpIds
    def grpByDpIdAge(xs: Dataset[Row], ys: Dataset[Row],zs : Dataset[Row]) : Dataset[Row] = {
      grpByAge(xs).union(grpByAge(ys)).union(grpByAge(zs))
    }

    // Gender grouping for different DpIds
    def grpByDpIdGender(xs: Dataset[Row], ys: Dataset[Row],zs : Dataset[Row]) : Dataset[Row] = {
      grpByGender(xs).union(grpByGender(ys)).union(grpByGender(zs))
    }

    // Zipcode grouping for different DpIds
    def grpByDpIdZipcode(xs: Dataset[Row], ys: Dataset[Row],zs : Dataset[Row]) : Dataset[Row] = {
      grpByZipcode(xs).union(grpByZipcode(ys)).union(grpByZipcode(zs))
    }

    def customBucket(merged : Dataset[Row]) : Dataset[Row] = {

      // assign age_group

      val res = merged.withColumn("age_group",
        when(col("age").isNull,null).
          when(col("age").cast(IntegerType)<=18,"18").
          when(col("age").cast(IntegerType)>=19 && col("age").cast(IntegerType)<=25,"25").
          when(col("age").cast(IntegerType)>=26 && col("age").cast(IntegerType)<=35,"35").
          when(col("age").cast(IntegerType)>=36 && col("age").cast(IntegerType)<=45,"45").
          when(col("age").cast(IntegerType)>=46 && col("age").cast(IntegerType)<=55,"55").
          when(col("age").cast(IntegerType)>=56 && col("age").cast(IntegerType)<=65,"65").
          when(col("age").cast(IntegerType)>=66,"75"))

      val age_count_df = res.groupBy("age_group", "age").agg(count("age").as("ageAggregate")).
        groupBy("age_group").agg(collect_list(struct("age", "ageAggregate")).as("age_count"))
      
      val gender_count_df = res.groupBy("age_group", "gender").agg(count("gender").as("genderAggregate")).
        groupBy("age_group").agg(collect_list(struct("gender", "genderAggregate")).as("gender_count"))

      age_count_df.join(gender_count_df,"age_group")

    }

  }
}
