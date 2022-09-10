#Question 1, Part e

import numpy as np
from scipy import linalg


M_list = [[1,2],[2,1],[3,4],[4,3]]
M = np.array(M_list)

print("M matrix:")
print(M)

U, S, VT = linalg.svd(M, full_matrices=False)

print("U matrix:")
print(U)
print("Sigma matrix:")
print(S)
print("V transpose matrix:")
print(VT)

MT = M.T 
MTM = np.dot(MT,M)

print("M*M^T matrix:")
print(MTM)

#evals, evecs = linalg.eigh(MTM)
evals, evecs = linalg.eigh(MTM)

print("The eigen values of M^T*M are: ")
print(evals)
print("The eigen vectors of M^T*M are:")
print(evecs)

seqToSort = evals.argsort()
seq = seqToSort.tolist()
seq.reverse()
evals = evals.tolist()
evals.sort(reverse = True)
evecs = evecs[:, seq]

print("SORTED")
print("The eigen values of M^T*M are: ")
print(evals)
print("The eigen vectors of M^T*M are:")
print(evecs)


