#vm504
# python program to find modularities of different configurations of figure

import numpy as np

def calculateMod(A, k, m):
	Q = 0
	for i in range(len(A)):
		for j in range(len(A)):
			Q += (A[i][j] - (k[i]*k[j])/(2*m))*s[i]*s[j]
	Q = Q/(4*m)
	return Q


# in all cases, the s parameters are same since we are considering same partition
s = [1,1,1,1,-1,-1,-1,-1]

#However, the parameters A, k (degrees), and m change

#1a-i
A = np.array([[0, 1, 1, 1, 0, 0, 1, 0], [1, 0, 1, 1, 0, 0, 0, 0], [1, 1, 0, 1, 0, 0, 0, 0], [1, 1, 1, 0, 0, 0, 0, 0],
		[0, 0, 0, 0, 0, 1, 1, 0], [0, 0, 0, 0, 1, 0, 1, 0], [1, 0, 0, 0, 1, 1, 0, 1], [0, 0, 0, 0, 0, 0, 1, 0]])
k = [4,3,3,3,2,2,4,1]
m = 11
Q = calculateMod(A, k, m) 
print("Modularity for the original graph")
print(Q)

#1a-ii
A = np.array([[0, 1, 1, 1, 0, 0, 0, 0], [1, 0, 1, 1, 0, 0, 0, 0], [1, 1, 0, 1, 0, 0, 0, 0], [1, 1, 1, 0, 0, 0, 0, 0], 
			[0, 0, 0, 0, 0, 1, 1, 0], [0, 0, 0, 0, 1, 0, 1, 0], [0, 0, 0, 0, 1, 1, 0, 1], [0, 0, 0, 0, 0, 0, 1, 0]])
k = [3,3,3,3,2,2,3,1]
m = 10
Q = calculateMod(A, k, m) 
print("Modularity for original graph without AG")
print(Q)

#1b-i
A = np.array([[0, 1, 1, 1, 0, 0, 1, 0],[1, 0, 1, 1, 0, 0, 0, 0],[1, 1, 0, 1, 0, 0, 0, 0],[1, 1, 1, 0, 0, 0, 0, 0],
		[0, 0, 0, 0, 0, 1, 1, 1], [0, 0, 0, 0, 1, 0, 1, 0], [1, 0, 0, 0, 1, 1, 0, 1], [0, 0, 0, 0, 1, 0, 1, 0]])
k = [4,3,3,3,3,2,4,2]
m = 12
Q = calculateMod(A, k, m) 
print("Modularity for the original graph with EH")
print(Q)

#1b-ii
A = np.array([[0, 1, 1, 1, 0, 0, 0, 0], [1, 0, 1, 1, 0, 0, 0, 0], [1, 1, 0, 1, 0, 0, 0, 0], [1, 1, 1, 0, 0, 0, 0, 0],
		[0, 0, 0, 0, 0, 1, 1, 1], [0, 0, 0, 0, 1, 0, 1, 0], [0, 0, 0, 0, 1, 1, 0, 1], [0, 0, 0, 0, 1, 0, 1, 0]])
k = [3,3,3,3,3,2,3,2]
m = 11
Q = calculateMod(A, k, m) 
print("Modularity for the original graph with EH and without AG")
print(Q)

#1c-i
A = np.array([[0, 1, 1, 1, 0, 1, 1, 0], [1, 0, 1, 1, 0, 0, 0, 0], [1, 1, 0, 1, 0, 0, 0, 0], [1, 1, 1, 0, 0, 0, 0, 0],
		[0, 0, 0, 0, 0, 1, 1, 0], [1, 0, 0, 0, 1, 0, 1, 0], [1, 0, 0, 0, 1, 1, 0, 1], [0, 0, 0, 0, 0, 0, 1, 0]])
k = [5,3,3,3,2,3,4,1]
m = 12
Q = calculateMod(A, k, m) 
print("Modularity for the original graph with edge AF")
print(Q)

#1c-ii
A = np.array([[0, 1, 1, 1, 0, 1, 0, 0], [1, 0, 1, 1, 0, 0, 0, 0], [1, 1, 0, 1, 0, 0, 0, 0], [1, 1, 1, 0, 0, 0, 0, 0],
		[0, 0, 0, 0, 0, 1, 1, 0], [1, 0, 0, 0, 1, 0, 1, 0], [0, 0, 0, 0, 1, 1, 0, 1], [0, 0, 0, 0, 0, 0, 1, 0]])
k = [4,3,3,3,2,3,3,1]
m = 11
Q = calculateMod(A, k, m) 
print("Modularity for the original graph with edge AF and without AG")
print(Q)