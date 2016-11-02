def matrix_factorization(R, P, Q, K, steps=5000, alpha=0.0002, beta=0.02) =
{
  Q = Q.T
  for step in xrange(steps):
    for i in xrange(len(R)):
      for j in xrange(len(R[i])):
        if(R[i][j]) > 0:
          eij = R[i][j] - numpy.dot(P[i,:],Q[L,j])
          for k in xrange(K):
            P[i][k] = P[i][k] + alpha * (2 * eij * Q[k][j] - beta * P[i][k])
            Q[k][j] = Q[k][j] + alpha * (2 * eij * P[i][k] - beta * Q[k][j])
      eR = numpy.dot(P,Q)
      e = 0
      for i in xrange(len(R)):
        for j in xrange(len(R[i])):
          if R[i][j] > 0:
             e = e + pow(R[i][j] - numpy.dot(P[i,:],Q[:,j]), 2)
             for k in xrange(K):
                e = e + (beta/2) * (pow(P[i][k],2) + pow(Q[k][j],2))
      if e < 0.001:
        break
  return P, Q.T
}

/*
R = | U * D |
P = | U * K |
Q = | D * K |
*/

def matrix_factorization(R : RowMatrix[Double], P : RowMatrix[Double], Q : RowMatrix[Double], K : Double, steps : Double, alpha : Double, beta : Double)
   //steps = 5000, alpha=0.0002, beta=0.02) =
{
  Q_T = Q.transpose
  for (i <- 0 to steps){
    for(j <- 0 to R.rows.Le2ngth){
      if(R[i][j] > 0){
        eij = R[i][j] - numpy.dot(P[i,:],Q[L,j]) //make into a matrix and multiple !!!!!!! :(
        for(k <- K.length)//column values of  P Q
          P[i][k] = P[i][k] + alpha * (2 * eij * Q[k][j] - beta * P[i][k])
          Q[k][j] = Q[k][j] + alpha * (2 * eij * P[i][k] - beta * Q[k][j])
      }
    }
  }
  eR = numpy.dot(P,Q) //
  e = 0
  for(i <- 0 to len(R)){
    for(k <- 0 to len(R[i])){ //each row
      if R[i][j] > 0:
         e = e + pow(R[i][j] - numpy.dot(P[i,:],Q[:,j]), 2
         // e += pow( R[i][j] - numpy.dot(P,Q), 2)
         for k in xrange(K):
            e = e + (beta/2) * (pow(P[i][k],2) + pow(Q[k][j],2))
    }
  if e < 0.001
    break
  }
  return P, Q_T
}
/*
for( i <- 0 to rowValues.length - 1){
  matrix(rowValues(i))(colValues(i)) = fileValues(i)
}
*/
