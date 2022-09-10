#APRIORI ALGORITHM TO FIND FREQUENT ITEMSETS IN BROWSING.TXT
#SUPPORT THRESHOLD = 100
#FREQUENT ITEMSETSIZES = 2,3

from collections import defaultdict
import numpy as np

s = 100
ifile = "browsing.txt"

inFile = open(ifile)

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
#print("These are the frequent items:")
#print(frequentItems)
frequentItemCounts = [pass1Info[x] for x in frequentItems]
#print("These are the frequent item counts:")
#print(frequentItemCounts)

#At this point we don't need to store pass1Info anymore
pass1Info.clear()

#datastructure for pass 2: 2d array of frequent_items * frequent_items
pass2Info = np.zeros((len(frequentItems), len(frequentItems)))

inFile = open(ifile)

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
frequentPairCounts = []
for i in range(len(frequentItems)):
	for j in range(len(frequentItems)):
		if pass2Info[i][j] >= s:
		        #i, j forms a frequent pair
                        frequentPairs.append((frequentItems[i],frequentItems[j]))
                        frequentPairCounts.append(pass2Info[i][j])

print("These are all the frequent pairs")
print(frequentPairs)
#print("These are the frequent pair counts:")
#print(frequentPairCounts)

#At this points we don't need to store pass 2 info anymore:
#pass2Info.clear()
pass2Info = None

#each tuple in this list: (a, b, c): Confidence score of rule a->b is c

scoresForPairs = []
#Finding all the association rules for frequent pairs
#Each frequent Pair is accociated with 2 rules
for i in range(len(frequentPairs)):
    pair = frequentPairs[i]
    itemA = pair[0]
    itemB = pair[1]
    
    inA = frequentItems.index(itemA)
    inB = frequentItems.index(itemB)
    
    suppA = frequentItemCounts[inA]
    suppB = frequentItemCounts[inB]
    suppAB = frequentPairCounts[i]
    #calculating conf(A->B)
    confAB = suppAB/suppA
    #calculating conf(B->A)
    confBA = suppAB/suppB
    scoresForPairs.append((itemA, itemB, confAB))
    scoresForPairs.append((itemB, itemA, confBA))


#Sorting the scores for pairs
scoresForPairs.sort(key = lambda x: (-x[2], x[0]))

#Printing all the association Rules for Pairs
for score in scoresForPairs:
    print(str(score[0]) + "->" + str(score[1]) + ": " + str(score[2]))



#At this point we have all the frequentItems and frequentPairs
#We now need to find the frequent triples.

#Making a list of all possible frequentTriple

#Starting multiline comment here
"""
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

#print("These are all the frequent triple candidates")
#print(frequentTripleCandidates)

#array to store corresponding counts
tripleCounts = np.zeros(len(frequentTripleCandidates))
"""

pass3Info = np.zeros((len(frequentItems), len(frequentPairs)))
#pass 3 through input file
#get the counts of all triples using a 3-nested loop.

inFile = open(ifile)

for line in inFile:
        items = line.split()
        fitems = [x for x in items if x in frequentItems]
        for i in range(len(fitems)):
	        for j in range(i+1, len(fitems)):
		        for k in range(j+1, len(fitems)):
                                #current triple being considered
                                currTriple = [fitems[i], fitems[j], fitems[k]]
                                currTriple.sort()

                                #check if (1, 2) is a frequent pair
                                pairIndex = -1
                                if ((currTriple[1], currTriple[2]) in frequentPairs):
                                    pairIndex = frequentPairs.index((currTriple[1], currTriple[2]))
                                elif ((currTriple[2], currTriple[1]) in frequentPairs):
                                    pairIndex = frequentPairs.index((currTriple[2], currTriple[1]))
                                else:
                                    continue

                                #At this point I know that 1,2 is a frequent pair and 0 is a frequent item
                                itemIndex = frequentItems.index(currTriple[0])

                                pass3Info[itemIndex][pairIndex] += 1

                                """
			        currTriple = [fitems[i], fitems[j], fitems[k]]
			        currTriple.sort()
				#print(currTriple)
			        if(currTriple in frequentTripleCandidates):
				        index = frequentTripleCandidates.index(currTriple)
				        tripleCounts[index] += 1
                                """
inFile.close()

#print("These are all the triple counts")
#print(tripleCounts)

#print(s)

#Done with pass 3
#go through triple counts and collect frequent triples
frequentTriples = []
frequentTripleCounts = []

for i in range(len(frequentItems)):
        for j in range(len(frequentPairs)):
	        if (pass3Info[i][j] >= s):
                        #get and sort this triple
                        currTriple = [frequentItems[i], frequentPairs[j][0], frequentPairs[j][1]]
                        currTriple.sort()
                        frequentTriples.append(currTriple)
                        frequentTripleCounts.append(pass3Info[i][j])


print("These are all the frequent triples")
print(frequentTriples)

#print("These are the frequent triple counts")
#print(frequentTripleCounts)


#Printing association rules for frequent triple
#Each frequent triple has 3 association rules
#Each entry (a, b, c, d) where d is the confidence of getting c given a,b
scoresForTriples = []
for i in range(len(frequentTriples)):
    itemA = frequentTriples[i][0]
    itemB = frequentTriples[i][1]
    itemC = frequentTriples[i][2]

    supABC = frequentTripleCounts[i]

    index = -1
    if((itemA,itemB) in frequentPairs):
        index = frequentPairs.index((itemA, itemB))
    else:
        index = frequentPairs.index((itemB, itemA))

    supAB = frequentPairCounts[index]

    if((itemA,itemC) in frequentPairs):
        index = frequentPairs.index((itemA, itemC))
    else:
        index = frequentPairs.index((itemC, itemA))

    supAC = frequentPairCounts[index]

    if((itemB,itemC) in frequentPairs):
        index = frequentPairs.index((itemB, itemC))
    else:
        index = frequentPairs.index((itemC, itemB))

    supBC = frequentPairCounts[index]

    #(a, b) -> c
    confABC = supABC/supAB

    #(a, c) -> b
    confACB = supABC/supAC
    
    #(b, c) -> a
    confBCA = supABC/supBC

    scoresForTriples.append((itemA, itemB, itemC, confABC))
    scoresForTriples.append((itemA, itemC, itemB, confACB))
    scoresForTriples.append((itemB, itemC, itemA, confBCA))


#Print out all rules
scoresForTriples.sort(key = lambda x: (-x[3], x[0], x[1]))

for score in scoresForTriples:
    print("(" + str(score[0]) + "," + str(score[1]) + ")" +  "->" + str(score[2]) + ": " + str(score[3]))


#Ended multiline comments












