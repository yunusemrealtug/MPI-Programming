
from mpi4py import MPI
import sys

comm=MPI.COMM_WORLD
world_size=comm.Get_size()
rank=comm.Get_rank()
mergeMethod=sys.argv[4] #according to arguments which we receive, the program has two working type. 

if (rank==0):
    f = open(sys.argv[2], "r") #input file
    g = open(sys.argv[6], "r") # test file
    listText=f.read().split("<s> ") #text is splitted from start of the sentences
    testText=g.read().split("\n") #test is splitted from the endlines
    listSize=len(listText)-1 #the first element in the list is empty, so remove one for size
    lowerBound=1 
    sendNum=listSize//(world_size-1)
    upperBound=sendNum+1
    remainNum=listSize%(world_size-1) # this part, determines how many sentences will be sent to workers
    for num in range(world_size-1):
        if (remainNum!=0):
            upperBound+=1
            remainNum-=1 #remainders will be shared to first workers
        comm.send(listText[lowerBound:upperBound], dest=(num+1)) #sends all workers
        lowerBound=upperBound
        upperBound+=sendNum
    if (mergeMethod=="WORKERS"):
        res=comm.recv(source=(world_size-1)) #master receives the full data from the last worker
            
    elif (mergeMethod=="MASTER"):
        res={}
        for rcv in range(1, world_size): #it takes data from each worker and merges with the res dictionary
            receivedData=comm.recv(source=(rcv))
            res = {x: res.get(x, 0) + receivedData.get(x, 0) for x in set(res).union(receivedData)}
    for element in testText:
        splittedElem=element.split(" ") #first word of bigram
        if splittedElem[0] not in res.keys(): #if first word is not in the dictionary, there is no answer
            print(element+" does not have defined conditional probability, because 0/0 is the result.")
        else:
            if element in res.keys(): #if both first word and bigram are in the dictionary the result will be calculated
                numBig=res.get(element)
                numUnig=res.get(splittedElem[0])
                print (element+" has conditional probability "+str(numBig/numUnig))
            else: #if the element is not in the dict, it will raise an error when we use dict.get so we checked it
                print(element+ " has conditional probability 0")
        
else:
    partedList=comm.recv(source=0) #each worker takes data from the master
    print (str(rank)+" and its size of list is "+str(len(partedList)))
    data={} #each worker creates a dictionary execute the data
    for sentence in partedList: 
        words=sentence.split(" ")
        iNum=0
        while "</s>" not in words[iNum+1]: #if the later word is </s>, this will be the last word
            bigram=words[iNum]+" "+words[iNum+1] #have the bigram
            unigram=words[iNum] #have the unigram
            if bigram in data.keys(): #if the bigram or unigram is not on the dict we add it, otherwise value is incremented
                val=data.get(bigram)+1
                data.update({bigram:val})
            else:
                data.update({bigram:1})
            if unigram in data.keys():
                val=data.get(unigram)+1
                data.update({unigram:val})
            else:
                data.update({unigram:1})
            iNum+=1
        unigram=words[iNum]
        if unigram in data.keys(): #this is for the last word
            val=data.get(unigram)+1
            data.update({unigram:val})
        else:
            data.update({unigram:1})
    if (mergeMethod=="WORKERS"): #workers receive data from the previous worker
        if (rank==1): #first worker do not receive a data from a worker
            if (world_size==2):
                comm.send(data, dest=0) #if first worker is alone it sends to master what it executed
            else:
                comm.send(data, dest=2) #otherwise sends the second worker
        elif (rank==world_size-1): #if the worker is not first it receives a dict and merges its own dict and if it last sends to master
            recData=comm.recv(source=(rank-1))
            mergedData = {x: recData.get(x, 0) + data.get(x, 0) for x in set(recData).union(data)}
            comm.send(mergedData, dest=0)
        else: # if it is not last sends to next worker
            recData=comm.recv(source=(rank-1))
            mergedData = {x: recData.get(x, 0) + data.get(x, 0) for x in set(recData).union(data)}
            comm.send(mergedData, dest=(rank+1))
    elif (mergeMethod=="MASTER"): #if the method is master, workers send data to only master, nothing else
        comm.send(data, dest=0)
           


