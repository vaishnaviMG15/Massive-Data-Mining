# Question 3: Implementing Page Rank
import numpy as np
from collections import defaultdict


n = 100
m = 1024
M = np.zeros((n,n))
B = 0.8
k = 40
file = open("graph.txt", 'r')

#go through one pass of graph.txt and collect the out-degree of each vertex

outDegrees = defaultdict(int)

for line in file:
	l = line.split()
	s = int(l[0])
	outDegrees[s] += 1

file.close()

file = open("graph.txt", 'r')

#e = set()
#line_num = 0;
#go through second pass of graph.txt and fill the M matrix
for line in file:
	l = line.split()
	s = int(l[0])
	d = int(l[1])
	#e.add((s, d))
	#line_num += 1
	sIndex = s-1
	dIndex = d-1
	M[dIndex, sIndex] += 1/outDegrees[s]


#print("The number of lines in graph.txt = " + str(line_num))
#1024
#print("The size of set = " + str(len(e)))
#950
#There could be multiple (s, d) edges [like a directed multigraph]
file.close()

oneVector = np.ones(n)
telVector = ((1-B)/n)*oneVector
#Initializing r
r = (1/n) * oneVector

for i in range(k):
	r = telVector + B*np.dot(M, r)

print("rank matrix: ")
print(r)

sortedIndices = np.argsort(r)

#print(sortedIndices)

high5Indices = list(sortedIndices[95:])
low5Indices = list(sortedIndices[:5])
high5Indices.reverse()

#print(high5Indices)
#print(low5Indices)

highOutput = [x+1 for x in high5Indices]
lowOutput = [x+1 for x in low5Indices]

"""
print("node id's with highest rank in descending order of rank: ")
print(highOutput)
print("node id's with lowest rank in ascending order of rank: ")
print(lowOutput)
"""

print("node id's with highest rank: rank")
for i in range(5):
	print(str(highOutput[i]), ": ", str(r[high5Indices[i]]))

print("node id's with lowest rank: rank")
for i in range(5):
	print(str(lowOutput[i]), ": ", str(r[low5Indices[i]]))


