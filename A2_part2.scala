import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io.Source
import org.apache.spark.rdd._
import org.apache.spark.mllib.linalg._
import org.apache.spark.mllib.linalg.distributed._
import java.io._
import scala.util.Random


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
def createRandomMatrix(row : Int, col : Int) = {
  val rand = new scala.util.Random(10)
  val doubleArray = (for (i <- 1 to col) yield rand.nextInt(100)).toArray
  val vector : Vector = new DenseVector( doubleArray.map(x => x.toDouble) )
  val array = new Array[Vector](row)
  for(i <- 0 to row - 1){
    val doubleArray = (for (i <- 1 to col) yield rand.nextInt(100)).toArray
    val vvv : Vector = new DenseVector( doubleArray.map(x => x.toDouble) )
    array(i) = vvv
  }
  val arrayVectors = sc.parallelize(vectorArray)
  val rowMatrix = new RowMatrix(rddVectors)
  rowMatrix
  //val newRDD = new Rdd()//.map( x =>
}
def matrix_factorization(r_matrix: RowMatrix, p_Vector: RowMatrix, q_Vector: RowMatrix, k: Int, steps: Double, alpha: Double, beta: Double) = {
  var q_t = transposeRowMatrix(q)
  val e : Double = 0

  if(e < 0.001){
    val q_transpose = transposeRowMatrix(q)
    break
    //return (e, q_transpose)
  }
  return (0,0)
}
def pow2( theValue: Double) = {
  val power2 = theValue * theValue
  power2
}
def dotProduct(p_rom: RowMatrix, q_rom: RowMatrix) = {
  val temp = q_rom.rows.map( x => x.toArray).collect()
  val tempRows = temp.size
  val tempCols = temp(1).size
  val dm: Matrix = Matrices.dense(tempRows, tempCols, temp.flatten)
  val dot = p_rom.multiply(dm)
  dot
}
//def e_pow(e: Double, r_matrix: RowMatrix, p_Vector: RowMatrix, q_Vector:) = {

//}
def PQ_calculator(p_Vector: RowMatrix, q_Vector: RowMatrix, k: Int, alpha: Double, beta: Double, eij: Double, i: Int, j: Int) = {
 var p = p_Vector.rows.map( x => x.toArray).collect()//rowMatrix.rows.toArray().collect()
 var q = q_Vector.rows.map( x => x.toArray).collect()

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
  U(N) set of Users
  D(M) set of Items
  R = [U * D] contains all the ratings of users ****

Find :
  P = [U(N) * K]
  Q = [D(KM * K]

  Such that R ~ P * transpose(Q) = R^
*/
//var q = rowMatrix
//var q_t = transposeRowMatrix(q)

val steps = 5000
val alpha = 0.0002
val beta = 0.02
val N = maxRow
val M = maxCol
val K = 2 // ??????????????????????????????????????

val p = createRandomMatrix(N, K)
val q = createRandomMatrix(M,K)
//def matrix_factorization(r_matrix: RowMatrix, p_Vector: RowMatrix, q_Vector: RowMatrix, k: Int, steps: Double, alpha: Double, beta: Double) = {

//def PQ_calculator(p_Vector: RowMatrix, q_Vector: RowMatrix, k: Int, alpha: Double, beta: Double, eij: Double, i: Int, j: Int) = {
//def matrix_factorization(R, P, Q, K, steps=5000, alpha=0.0002, beta=0.02) =



//
