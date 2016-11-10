import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io.Source
import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed._
import java.io._


// scalac -classpath "$SPARK_HOME/jars/*" Assign2.scala -d a2.jar -optimise
object Assign2 {

    def main(args: Array[String])
    {
        val datafile = args(0)
        val missingfile = args(1)
        val outfile = args(2)

        val ofile = new File(outfile)
        val output = new BufferedWriter(new FileWriter(ofile))

        var conf = new SparkConf().setAppName("Assign2")
        var sc = new SparkContext(conf)

        println(" ------------------------------------ 1: Data Setup --------------------------------------------------- ")
        println("Hello World!!!!")
        /*val fileData = sc.textFile("small.csv").map(_.split(","))//returns an array(String) of each line in file
        val missingFileDAta = sc.textFile("small_missing.csv").map(_.split(","))//.first()//returns an array(string) of each line in missing file
        val outfile = "small.out"
        val ofile = new File(outfile)
        val output = new BufferedWriter(new FileWriter(ofile))
        */
        val fileData = sc.textFile(datafile).map(_.split(","))//returns an array(String) of each line in file
        val missingFileDAta = sc.textFile(missingfile).map(_.split(","))//returns an array(string) of each line in missing file
          /*fileData        = (row, column, value)
            missingFileDAta = (row, column)
          */
        val missingrowValues : Array[Int]  = missingFileDAta.map( x => x(0).toInt).collect
        val missingcolValues : Array[Int]  = missingFileDAta.map( x => x(1).toInt).collect

        val maxRow = fileData.map( x => x(0).toInt).max
        val maxCol = fileData.map( x => x(1).toInt).max
        var matrix = Array.ofDim[Double](maxRow + 1,maxCol + 1)


        val rowValues : Array[Int]  = fileData.map( x => x(0).toInt).collect
        val colValues : Array[Int]  = fileData.map( x => x(1).toInt).collect
        val fileValues : Array[Double] = fileData.map(x => x(2).toDouble).collect

        var avg = fileValues.reduceLeft(_ + _) / (fileValues.size)
        for( i <- 0 to maxRow){
          for(j <- 0 to maxCol){
            matrix(i)(j) = avg
          }
        }

        for( i <- 0 to rowValues.length - 1){
          matrix(rowValues(i))(colValues(i)) = fileValues(i)
        }

        var vectorArray = new Array[Vector](maxRow)
        for( i <- 0 to maxRow - 1){
          vectorArray(i) =  Vectors.dense(matrix(i))
        }

        val rddVectors = sc.parallelize(vectorArray)//rdd.SparkContext.parallelize( vectorArray)
        val rowMatrix = new RowMatrix(rddVectors)
        val svd = rowMatrix.computeSVD(24, true)//23, false , 10)

          //val U: RowMatrix = svd.U  // The U factor is a RowMatrix.
          //val s: Vector = svd.s  // The singular values are stored in a local dense vector.
          //val s = Matrices.diag(svd.s)
        val U = svd.U
        val s = Matrices.diag(svd.s)
        val V = svd.V//val s_RDD = sc.parallelize(Seq(s))
          //val s_Matrix : RowMatrix = new RowMatrix(s_RDD)
          //val Vs_Matrix = V.multiply(s)


          /*s.toString
          val Urows = U.rows
          Urows.collect().foreach(println)
          V.toString()*/

        val A = U.multiply(s).multiply(V.transpose).rows.collect.zipWithIndex
          //println(A.deep.mkString("\n"))
        //println(A.deep.mkString("\n"))     Print Statement

        println(" ------------------------------------  2: Implementation ---------------------------------------------------- ")
        //// println(A(0)._1.toArray())
        for( i <- 0 to missingrowValues.length - 1){
          //matrix(rowValues(i))(colValues(i)) = fileValues(i)
          val vector = A(missingrowValues(i))._1.toArray
          val missRow = missingrowValues(i)
          val missCol = missingcolValues(i)
          val writeVal = vector(missingcolValues(i))
          //println( missRow+","+missCol+","+writeVal)
          output.write( missRow+","+missCol+","+writeVal+"\n")
        }

        output.close()
        System.exit(0)
    }
}
