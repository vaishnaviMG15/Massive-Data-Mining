#APRIORI ALGORITHM TO FIND FREQUENT ITEMSETS IN BROWSING.TXT
#SUPPORT THRESHOLD = 100
#FREQUENT ITEMSETSIZES = 2,3

from collections import defaultdict
import numpy as np

s = 3
inFile = open("simple2.txt")

#datastructure to store pass 1 info:Default dictionary
#key: itemID, value: counts
pass1Info = defaultdict(int)

#pass 1 through input file
#store information about each item and its counts.
for line in inFile:
	items = line.split()
	for item in items:
		pass1Info[item] += 1

inFile.close()

#Completed Pass 1
#print(pass1Info)

#Make a list of frequent items
frequentItems = [x for x in pass1Info.keys() if pass1Info[x] >= s]
print("These are the frequent items:")
print(frequentItems)

#datastructure for pass 2: 2d array of frequent_items * frequent_items
pass2Info = np.zeros((len(frequentItems), len(frequentItems)))

inFile = open("simple2.txt")

#pass2 through input file
#get counts for pairs of frequent items
for line in inFile:
	items = line.split()
	#print("These are the items of this line")
	#print(items)
	
	#2-nested loop to find pairs
	fitems = [x for x in items if x in frequentItems]
	#print("These are the frequent items of this line")
	#print(fitems)

	for i in range(len(fitems)):
		for j in range(i+1, len(fitems)):
			#Update the count for these items
			item1 = fitems[i]
			item2 = fitems[j]

			p1 = frequentItems.index(item1)
			p2 = frequentItems.index(item2)

			if(p2 < p1):
				temp = p2
				p2 = p1
				p1 = temp

			pass2Info[p1][p2] += 1

#Done with collecting pass2 info
inFile.close()
#print(pass2Info)


#going through pass2Info and recording frequent pairs
frequentPairs = []

for i in range(len(frequentItems)):
	for j in range(len(frequentItems)):
		if pass2Info[i][j] >= s:
			#i, j forms a frequent pair
			frequentPairs.append((frequentItems[i],frequentItems[j]))

print("These are all the frequent pairs")
print(frequentPairs)

#At this point we have all the frequentItems and frequentPairs
#We now need to find the frequent triples.

#Making a list of all possible frequentTriple

frequentTripleCandidates = []

for i in range(len(frequentItems)):
	for j in range(len(frequentPairs)):
		single = frequentItems[i]
		double = frequentPairs[j]
		if not(single in double):
			triple = [single, double[0], double[1]]
			triple.sort()
			if triple not in frequentTripleCandidates:
				frequentTripleCandidates.append(triple)

print("These are all the frequent triple candidates")
print(frequentTripleCandidates)

#array to store corresponding counts
tripleCounts = np.zeros(len(frequentTripleCandidates))

#pass 3 through input file
#get the counts of all triples using a 3-nested loop.

inFile = open("simple2.txt")

for line in inFile:
	items = line.split()
	for i in range(len(items)):
		for j in range(i+1, len(items)):
			for k in range(j+1, len(items)):
				currTriple = [items[i], items[j], items[k]]
				currTriple.sort()
				#print(currTriple)
				if(currTriple in frequentTripleCandidates):
					index = frequentTripleCandidates.index(currTriple)
					tripleCounts[index] += 1

inFile.close()

print("These are all the triple counts")
print(tripleCounts)

#print(s)

#Done with pass 3
#go through triple counts and collect frequent triples
frequentTriples = []

for i in range(len(tripleCounts)):
	if (tripleCounts[i] >= s):
		frequentTriples.append(frequentTripleCandidates[i])


print("These are all the frequent triples")
print(frequentTriples)














