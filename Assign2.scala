import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io.Source
import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed._
import java.io._

object Assign2 {

    def main(args: Array[String])
    {
        val datafile = args(0)
        val missingfile = args(1)
        val outfile = args(2)

        val ofile = new File(outfile)
        val output = new BufferedWriter(new FileWriter(ofile))

        println(" ------------------------------------ 1: Data Setup --------------------------------------------------- ")
        println("Hello World!!!!")

        val fileData = sc.textFile(datafile).map(_.split(","))//returns an array(String) of each line in file
        val missingFileDAta = sc.textFile(missingfile).map(_.split(",")).first()//returns an array(string) of each line in missing file
          /*fileData        = (row, column, value)
            missingFileDAta = (row, column)
          */
        val maxRow = fileData.map( x => x(0).toInt).max
        val maxCol = fileData.map( x => x(1).toInt).max
        var matrix = Array.ofDim[Double](maxRow + 1,maxCol + 1)
        val rowValues : Array[Int]  = fileData.map( x => x(0).toInt).collect
        val colValues : Array[Int]  = fileData.map( x => x(1).toInt).collect
        val fileValues : Array[Double] = fileData.map(x => x(2).toDouble).collect
        for( i <- 0 to rowValues.length - 1){
          matrix(rowValues(i))(colValues(i)) = fileValues(i)
        }

        var vectorArray = new Array[Vector](maxRow)
        for( i <- 0 to maxRow - 1){
          vectorArray(i) =  Vectors.dense(matrix(i))
        }

        println(" ------------------------------------ 1a matrix setup --------------------------------------------------- ")
        val rddVectors = sc.parallelize(vectorArray)//rdd.SparkContext.parallelize( vectorArray)
        val rowMatrix = new RowMatrix(rddVectors)
        val svd = rowMatrix.computeSVD(24, true)

        val U = svd.U
        val s = Matrices.diag(svd.s)
        val V = svd.V//val s_RDD = sc.parallelize(Seq(s))

        val A = U.multiply(s).multiply(V.transpose).rows.collect
        //println(A.deep.mkString("\n"))     Print Statement

        println(" ------------------------------------  2: Implementation ---------------------------------------------------- ")

        output.close()
        System.exit(0)
    }
}
