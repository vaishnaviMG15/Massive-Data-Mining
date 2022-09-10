#vm504
# Question 2
import numpy as np
from scipy import linalg

# Laplacian Matrix

lm_list = [[4, -1, -1, -1, 0, 0, -1, 0],[-1, 3, -1, -1, 0, 0, 0, 0], [-1, -1, 3, -1, 0, 0, 0, 0], [-1, -1, -1, 3, 0, 0, 0, 0], [0, 0, 0, 0, 2, -1, -1, 0], [0, 0, 0, 0, -1, 2, -1, 0], [-1, 0, 0, 0, -1, -1, 4, -1], [0, 0, 0, 0, 0, 0, -1, 1]]

lm = np.array(lm_list)

print()
print("Laplacian Matrix: ")
print(lm)
print()

# Computing eigen values and eigen vectors of Laplacian

# evals: list of eigen values
# evecs: matrix whose columns correspond to the eigen vectors of eigen values in evals respectively

evals, evecs = linalg.eigh(lm)

print()
print("The eigen values of Laplacian are: ")
print (evals)
print()
print("The corresponding eigen vectors of Laplacian are: ")
print (evecs)
print()
print()

# ranking eigen values in ascending order

seqToSort = evals.argsort()
seq = seqToSort.tolist()
evals.sort()
evecs = evecs[:, seq]


print("The sorted eigen values of Laplacian are: ")
print (evals)
print()
print("The corresponding eigen vectors of Laplacian are: ")
print (evecs)
print()
print()