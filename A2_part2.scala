import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io.Source
import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed._
import java.io._

////////////////////////////////////////////  Matrix Transpose ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
def transposeRowMatrix(m: RowMatrix): RowMatrix = {
   val transposedRowsRDD = m.rows.zipWithIndex.map{case (row, rowIndex) => rowToTransposedTriplet(row, rowIndex)}
     .flatMap(x => x) // now we have triplets (newRowIndex, (newColIndex, value))
     .groupByKey
     .sortByKey().map(_._2) // sort rows and remove row indexes
     .map(buildRow) // restore order of elements in each row and remove column indexes
   new RowMatrix(transposedRowsRDD)
 }


 def rowToTransposedTriplet(row: Vector, rowIndex: Long): Array[(Long, (Long, Double))] = {
   val indexedRow = row.toArray.zipWithIndex
   indexedRow.map{case (value, colIndex) => (colIndex.toLong, (rowIndex, value))}
 }

 def buildRow(rowWithIndexes: Iterable[(Long, Double)]): Vector = {
   val resArr = new Array[Double](rowWithIndexes.size)
   rowWithIndexes.foreach{case (index, value) =>
       resArr(index.toInt) = value
   }
   Vectors.dense(resArr)
 }
////////////////////////////////////////////  Matrix Transpose ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
////////////////////////////////////////////  Matrix Factorization ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
def matrix_factorization(r_matrix: RowMatrix, p_Vector: RowMatrix, q_Vector: RowMatrix, k: Int, steps: Double, alpha: Double, beta: Double) = {
  var q_t = transposeRowMatrix(q)
/*
  U set of Users
  D set of Items
  R = [U * D] contains all the ratings of users ****

Find :
  P = [U * K]
  Q = [D * K]

  Such that R ~ P * transpose(Q) = R^
*/
//return P, Q.T
}


def PQ_calculator(p_Vector: RowMatrix, q_Vector: RowMatrix, k: Int, alpha: Double, beta: Double, eij: Double, i: Int, j: Int) = {
 var p = rowMatrix.rows.map( x => x.toArray).collect()//rowMatrix.rows.toArray().collect()
 var q = rowMatrix.rows.map( x => x.toArray).collect()

 for(kVal <- 0 to k){
   p(i)(kVal) = p(i)(kVal) + (alpha * (2 * eij * q(kVal)(j) - beta * p(i)(kVal) ))
   q(kVal)(j) = q(kVal)(j) + (alpha * (2 * eij * p(i)(kVal)  - beta * q(kVal)(j) ))
 }

 val p_ArrayVector: Array[Vector] = p.map(x => new DenseVector(x))
 val p_parallelize = sc.parallelize(p_ArrayVector)
 val p_RowMatrix = new RowMatrix(p_parallelize)

 val q_ArrayVector: Array[Vector] = q.map(x => new DenseVector(x))
 val q_parallelize = sc.parallelize(q_ArrayVector)
 val q_RowMatrix = new RowMatrix(q_parallelize)

 (p,q)
}
////////////////////////////////////////////  Matrix Factorization ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
println("Hello World!!!!")
println(" ------------------------------------ 1: Data Setup --------------------------------------------------- ")

val fileData = sc.textFile("small.csv").map(_.split(","))//returns an array(String) of each line in file
val missingFileDAta = sc.textFile("small_missing.csv").map(_.split(","))//.first()//returns an array(string) of each line in missing file
val outfile = "small.out"
val ofile = new File(outfile)
val output = new BufferedWriter(new FileWriter(ofile))
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
println(" ------------------------------------ 2: Implementaion --------------------------------------------------- ")
/*
  U set of Users
  D set of Items
  R = [U * D] contains all the ratings of users ****

Find :
  P = [U * K]
  Q = [D * K]

  Such that R ~ P * transpose(Q) = R^
*/
var q = rowMatrix
//var q_t = transposeRowMatrix(q)

val steps = 5000
val alpha = 0.0002
val beta = 0.02
val N = maxRow
val M = maxCol
val K = 2 // ??????????????????????????????????????

//def PQ_calculator(p_Vector: RowMatrix, q_Vector: RowMatrix, k: Int, alpha: Double, beta: Double, eij: Double, i: Int, j: Int) = {
def matrix_factorization(R, P, Q, K, steps=5000, alpha=0.0002, beta=0.02) =


/*
  an array can be made into a DenseVector
*/




































//
