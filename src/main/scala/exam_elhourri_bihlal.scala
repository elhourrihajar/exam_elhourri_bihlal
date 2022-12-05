import org.apache.spark.sql.SparkSession
import scala.sys.process._
import scala.io.StdIn.readLine
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
object exam_elhourri_bihlal{
               
         def main(args : Array[String]){
                      val spark =  SparkSession.builder.appName("delete").getOrCreate()
                      var df = spark.read.option("header",true).csv("hdfs:/exam_elhourri_bihlal/bronze/GOLD_DATA.csv")
                      var id:String = "";
                      val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
                      val srcPath=new Path("hdfs:/exam_elhourri_bihlal/bronze/GOLD_DATA.csv")  
                      println("-------------------------------")
                      println("please provide id to be deleted") 
                      id = readLine()                      
                      val df2 = df.filter(" id !="+"'"+id+"'")
                      df2.write.format("csv").save("hdfs:/exam_elhourri_bihlal/bronze/data_test")
                      fs.delete(srcPath,true)
                                          
                      println("delete successfully done ")
                      //df2.write.format("csv").save("hdfs:/exam_elhourri_bihlal/bronze/data_test")                       
                      spark.stop()
        }
}
