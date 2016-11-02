import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io.Source
import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed._
import java.io._



//alias adam-submit="/Users/swaroopakkineni/adam/bin/adam-submit"
//alias adam-shell="/Users/swaroopakkineni/adam/bin/adam-shell"


println("Hello World!!!!")
println(" ------------------------------------ 1: Data Setup --------------------------------------------------- ")

val fileData = sc.textFile("small.csv").map(_.split(","))//returns an array(String) of each line in file
val missingFileDAta = sc.textFile("small_missing.csv").map(_.split(",")).first()//returns an array(string) of each line in missing file
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

val A = U.multiply(s).multiply(V.transpose).rows.collect
//val A = UV_Matrix.multiply(s)

/*
fileData.foreach{ x =>
  //| println(matrix(x(0).toInt)(x(1).toInt))// = x(2).toDouble
  | matrix(x(0).toInt)(x(1).toInt) = x(2).toDouble
  //| matrix(x(0).toInt)(x(1).toInt) = x(1).toDouble
  //matrix(x(0))(x(1)) = x(2)
  | }
*/
println(" ------------------------------------  2: Implementation ---------------------------------------------------- ")

//val file_VectorFormat : = fileData.map( x => Vectors.dense(x(0).toDouble, x(1).toDouble, x(2).toDouble) )
//val file_MatrixFormat : RowMatrix = new RowMatrix(null, file_VectorFormat.collect().length , file_VectorFormat.collect().length)

//val bob = file_MatrixFormat.computeSVD(20 , true)

/*
val maxRow = fileData.map( x => x(0).toInt).max() + 1
val maxCol = fileData.map( x => x(1).toInt).max() + 1
val array = Array.ofDim[Double](maxRow,maxCol)
fileData.foreach( x => array(x(0).toInt )(x(1).toInt ) = x(2).toDouble  )
*/


/*
val r_vector = r.map(_.toString).to[Vector]
val row = fileData.map( x => x.to[Vector] )
val mat : RowMatrix = new RowMatrix(row)
//val row = fileData.map( x => Vectors.dense( x(0), x(1), x(2) ) )
  Step 1) Implement SGD
  Step 2) ......
  Step 3) Profit
*/

/* Notes of Large-Scale Matrix Factorization with Distributed Stochastic Gradient Descent*/
