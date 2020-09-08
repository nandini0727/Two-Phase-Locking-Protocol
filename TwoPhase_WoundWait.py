import pandas as pd

#initiate lock table
def initiateLockDF():
    df = pd.DataFrame(columns=['item','state','transactionIDHolding'])
    return df

#initiate transaction table
def initiateTransactionDF():
    df = pd.DataFrame(columns=['transactionID','transactionTimeStamp','transactionStatus'])
    return df

#initiate block table
def initiateBlockDF():
    df = pd.DataFrame(columns=['transactionID','item','operation','blockedBy'])
    return df
    
transactionTable = initiateTransactionDF()
lockingTable = initiateLockDF()
blockTable = initiateBlockDF()

global timeStamp
timeStamp = 0


#method to check if a particular transaction exists in Transaction Table
def checkIfTransactionExists(TID):
    global transactionTable
    global lockingTable
    global blockTable


    transactionList = transactionTable.loc[transactionTable['transactionID'] == TID,['transactionID','transactionTimeStamp','transactionStatus']] 
    
    if transactionList.size == 0:
        return False
    return True


#method to check to return the status of a particular transaction
def getTransactionStatus(TID):
    global transactionTable
    global lockingTable
    global blockTable


    transactionList = transactionTable.loc[transactionTable['transactionID'] == TID,['transactionID','transactionTimeStamp','transactionStatus']] 
    
    return transactionList['transactionStatus'].to_list()[0]


#method to begin transaction
#Adds a new row with transaction ID, time stamp and status = active in Transaction table
def beginTransaction(row, TID):
    global transactionTable
    global lockingTable
    global blockTable
    global timeStamp

    timeStamp = timeStamp +1

    print("Begin Transaction "+TID +" :Record is added to Transaction Table with TID= "+TID+" and timestamp= "+str(timeStamp)+" and state = active")

    row = row.replace('\n','')
    transactionTable = transactionTable.append(pd.Series([TID, timeStamp, 'Active'], index=transactionTable.columns ), ignore_index=True)

    # print('Operation ' + row + ' Begin Transaction ' + TID)


#method to execute the wound wait mechanism
def woundAndWait(TID, lockList, dataItem, row):

    global transactionTable
    global lockingTable
    global blockTable


    currentTimeStamp = getTimeStamp(TID)
    youngerTID = []
    olderTID = []
    for ele in lockList['transactionIDHolding'].to_list():
        if(ele != TID):
            
            if currentTimeStamp > getTimeStamp(ele):
                olderTID.append(ele)
            else:
                youngerTID.append(ele)

    #Current transactions goes into Blocked state if transaction holding the lock is older transaction      
    if olderTID and not youngerTID:
        print("Transaction "+TID+ " goes into blocked state (state = blocked) based on wound wait mechanism and operation "+row+" is added to queue")
        transactionTable.loc[transactionTable['transactionID'] == TID, ['transactionStatus']] = 'Blocked'
        # print("Operations in queue: "+row)
    
    for tid in olderTID:
        blockTable = blockTable.append(pd.Series([TID, dataItem, row, tid], index=blockTable.columns ), ignore_index=True)

    #Current transaction aborts younger transactions holding the lock... Lock is released and current transaction aquires lock
    for tid in youngerTID:
        print("Transaction "+TID+ " is aborted (state = aborted)")
        abort(tid)


# #method to execute wait die mechanism
# def waitDie(TID, lockList, dataItem, row):
#     global transactionTable
#     global lockingTable
#     global blockTable

#     currentTimeStamp = getTimeStamp(TID)
#     youngerTID = []
#     olderTID = []
#     for ele in lockList['transactionIDHolding'].to_list():
#         if(ele != TID):
            
#             if currentTimeStamp > getTimeStamp(ele):
#                 olderTID.append(ele)
#             else:
#                 youngerTID.append(ele)

    
#     #Current transaction aborts itself if the lock on dataitem is held by an older transaction
#     if olderTID and not youngerTID:
#         abort(TID)

#     #Current transaction goes into blocked state if lock on data item is held by younger transaction
#     if youngerTID and not olderTID:
#         transactionTable.loc[transactionTable['transactionID'] == TID, ['transactionStatus']] = 'Blocked'

#     for tid in youngerTID:
#         blockTable = blockTable.append(pd.Series([TID, dataItem, row, tid], index=blockTable.columns ), ignore_index=True)



#method to add read lock record in lock table on a particular data item by a particular transaction
def readLock(row, TID):
    global transactionTable
    global lockingTable
    global blockTable

    row = row.replace('\n','')
    dataItem = row[3]

    lockList = lockingTable.loc[lockingTable['item'] == dataItem,['item','state','transactionIDHolding']]

    #add a entry in locktable with current data item, transaction ID and read lock R if there is no entry in lock table with that particular data item
    if lockList.size == 0:
        print("Transaction "+TID+ " acquired a read lock on data item "+dataItem)
        #print(dataItem +" read locked by transaction "+TID)
        lockingTable = lockingTable.append(pd.Series([dataItem, 'R', TID], index=lockingTable.columns ), ignore_index=True)
    else:
        atleastOneWrite = False
        for rows in lockList.iterrows():
            if rows[1][0] == dataItem and rows[1][1] == 'W':
                atleastOneWrite = True
                break
        
        #if the data item is write locked by any other transaction, wound wait method is executed
        if atleastOneWrite:
            woundAndWait(TID, lockList, dataItem, row)
        else:
            print("Transaction "+TID+ " acquired a read lock on data item "+dataItem)
            # print(dataItem +" read locked by transaction "+TID)
            lockingTable = lockingTable.append(pd.Series([dataItem, 'R', TID], index=lockingTable.columns ), ignore_index=True)


#method to get the time stamp of particular transaction
def getTimeStamp(TID):
 
    global transactionTable
    global lockingTable
    global blockTable


    transactionList = transactionTable.loc[transactionTable['transactionID'] == TID,['transactionID','transactionTimeStamp','transactionStatus']] 
   
    return transactionList['transactionTimeStamp'].to_list()[0]
    

#method to abort a particular transaction
def abort(TID):
    global transactionTable
    global lockingTable
    global blockTable

    #change the status of transaction to "Aborted" in Transaction table
    transactionTable.loc[transactionTable['transactionID'] == TID, ['transactionStatus']] = "Aborted"

    lockList = lockingTable.loc[lockingTable['transactionIDHolding'] == TID,['item','state','transactionIDHolding']]
   
    lockingTable = lockingTable.loc[lockingTable['transactionIDHolding'] != TID]

    blockTable = blockTable.loc[blockTable['transactionID'] != TID]

    operationList = blockTable.loc[blockTable['blockedBy'] == TID,['transactionID','item','operation','blockedBy']]

    # blockedTID = ''
    # newBlockedBy = ''
    # for row in operationList.iterrows():
    #     id = row[1][0]
     
    #     dataitem = row[1][1]
    #     count = lockingTable.loc[(lockingTable['item'] == dataitem) & (lockingTable['transactionIDHolding'] != id)]

    #     if len(count) > 0:
    #         blockedTID = id
    #         newBlockedBy = count['transactionIDHolding'].to_list()[0]
            
    #         operationList = blockTable.loc[(blockTable['blockedBy'] == TID) & (blockTable['transactionID'] != id),['transactionID','item','operation','blockedBy']]
        
    # changedList = blockTable.loc[(blockTable['blockedBy'] == TID) & (blockTable['transactionID']==blockedTID),['transactionID','item','operation','blockedBy']]
    # for row1 in changedList.iterrows():
    #     id = row1[1][0]
     
    #     dataitem = row1[1][1]
    #     blockTable.loc[blockTable['transactionID']==blockedTID, ['blockedBy']] = newBlockedBy

    for row in operationList.iterrows():

        id = row[1][0]
     
        dataitem = row[1][1]
        

        count = blockTable.loc[(blockTable['transactionID'] == id) & (blockTable['item'] == dataitem)]

        if(len(count) > 1):
            blockTable = blockTable.loc[(blockTable['item'] != dataitem) | (blockTable['blockedBy'] != TID)]
        else:
            blockTable = blockTable.loc[(blockTable['item'] != dataitem) | (blockTable['blockedBy'] != TID)]
            print("Operations " + row[1][2]+ " in queue are executed")
            transactionTable.loc[transactionTable['transactionID'] == id, ['transactionStatus']] = 'Active'
            executeOperation(row[1][2], row[1][0])


#method to commit a particular transaction
def commit(TID):
    global transactionTable
    global lockingTable
    global blockTable

    #change the status of transaction to "Committed" in Transaction table
    transactionTable.loc[transactionTable['transactionID'] == TID, ['transactionStatus']] = "Committed"

    lockList = lockingTable.loc[lockingTable['transactionIDHolding'] == TID,['item','state','transactionIDHolding']]
   
    lockingTable = lockingTable.loc[lockingTable['transactionIDHolding'] != TID]

    blockTable = blockTable.loc[blockTable['transactionID'] != TID]

    operationList = blockTable.loc[blockTable['blockedBy'] == TID,['transactionID','item','operation','blockedBy']]

    # blockedTID = ''
    # newBlockedBy = ''
    # for row in operationList.iterrows():
    #     id = row[1][0]
     
    #     dataitem = row[1][1]
    #     count = lockingTable.loc[(lockingTable['item'] == dataitem) & (lockingTable['transactionIDHolding'] != id)]

    #     if len(count) > 0:
    #         blockedTID = id
    #         newBlockedBy = count['transactionIDHolding'].to_list()[0]
            
    #         operationList = blockTable.loc[(blockTable['blockedBy'] == TID) & (blockTable['transactionID'] != id),['transactionID','item','operation','blockedBy']]
        
    # changedList = blockTable.loc[(blockTable['blockedBy'] == TID) & (blockTable['transactionID']==blockedTID),['transactionID','item','operation','blockedBy']]
    # for row1 in changedList.iterrows():
    #     id = row1[1][0]
     
    #     dataitem = row1[1][1]
    #     blockTable.loc[blockTable['transactionID']==blockedTID, ['blockedBy']] = newBlockedBy


    for row in operationList.iterrows():

        id = row[1][0]
     
        dataitem = row[1][1]
        

        count = blockTable.loc[(blockTable['transactionID'] == id) & (blockTable['item'] == dataitem)]

        if(len(count) > 1):
            blockTable = blockTable.loc[(blockTable['item'] != dataitem) | (blockTable['blockedBy'] != TID)]
        else:
            #execute the operations which are blocked by current transaction and change the transaction status to "Active"
            blockTable = blockTable.loc[(blockTable['item'] != dataitem) | (blockTable['blockedBy'] != TID)]
            print("Operations " + row[1][2]+ " in queue are executed")
            transactionTable.loc[transactionTable['transactionID'] == id, ['transactionStatus']] = 'Active'
            executeOperation(row[1][2], row[1][0])





#method to add write lock record in lock table on a particular data item by a particular transaction
def writeLock(row, TID):
    global transactionTable
    global lockingTable
    global blockTable

    row = row.replace('\n','')
    dataItem = row[3]

    lockList = lockingTable.loc[lockingTable['item'] == dataItem,['item','state','transactionIDHolding']]

    if len(lockList) == 0:
        print("Transaction "+TID+ " acquired a write lock on data item "+dataItem)
        lockingTable = lockingTable.append(pd.Series([dataItem, 'W', TID], index=lockingTable.columns ), ignore_index=True)
    elif len(lockList) == 1 and lockList['transactionIDHolding'].to_list()[0] == TID and lockList['state'].to_list()[0] == 'R':
        print("Transaction "+TID+ " upgraded from read lock to write lock on data item "+dataItem)
        lockingTable.loc[lockingTable['item'] == dataItem, ['state']] = 'W'
    else:
        currentTimeStamp = getTimeStamp(TID)
       
        youngerTID = []
        olderTID = []
        for ele in lockList['transactionIDHolding'].to_list():
            if(ele != TID):
               
                if currentTimeStamp > getTimeStamp(ele):
                    olderTID.append(ele)
                else:
                    youngerTID.append(ele)

        # woundAndWait(TID, lockList, dataItem, row)
              
        if olderTID:
            print("Transaction "+TID+ " goes into blocked state (state = blocked) based on wound wait mechanism and operation "+row+" is added to queue")
            transactionTable.loc[transactionTable['transactionID'] == TID, ['transactionStatus']] = 'Blocked'
        
        for tid in olderTID:
            blockTable = blockTable.append(pd.Series([TID, dataItem, row, tid], index=blockTable.columns ), ignore_index=True)


        for tid in youngerTID:
            print("Transaction "+tid+ " is aborted (state = Aborted)")
            abort(tid)

        if not olderTID and youngerTID:
            print("Transaction "+TID+ " upgraded to write lock on data item "+dataItem)
            executeOperation(row, TID)


#method to add operations to queue if transaction goes into Blocked state
def addOperationsToQueue(TID, row):
    global transactionTable
    global lockingTable
    global blockTable

    operationList = blockTable.loc[blockTable['transactionID'] == TID,['transactionID','item','operation','blockedBy']]

    for operation in operationList['blockedBy'].to_list():
        blockTable = blockTable.append(pd.Series([TID, row[3], row, operation], index=blockTable.columns ), ignore_index=True)


#method to add operations to queue if transaction goes into Blocked state
def addOperationToQueue(TID, row):
    global transactionTable
    global lockingTable
    global blockTable

    operationList = blockTable.loc[blockTable['transactionID'] == TID,['transactionID','item','operation','blockedBy']]
    for operation in operationList['blockedBy'].to_list():
        blockTable = blockTable.append(pd.Series([TID, '', row, operation], index=blockTable.columns ), ignore_index=True)


#method to execute operation based on the character (b, r, w, e)
def executeOperation(row, TID):
    if row[0] == 'b':
        if not checkIfTransactionExists(TID):
            beginTransaction(row, TID)
    
    if row[0] == 'r':
        if checkIfTransactionExists(TID):
            status = getTransactionStatus(TID)
            if(status == 'Active'):
                readLock(row, TID)
            elif(status == 'Aborted'):
                print('Transaction '+TID +' is already Aborted. No changes in tables')
            elif(status == 'Blocked'):
                print("Operation "+row+ " added to queue as transaction "+TID+ " is in "+status+ " state")
                addOperationsToQueue(TID, row)
    
    if row[0] == 'w':
        if checkIfTransactionExists(TID):
            status = getTransactionStatus(TID)
            if(status == 'Active'):
                writeLock(row, TID)
            elif(status == 'Aborted'):
                print('Transaction '+TID +' is already Aborted. No changes in tables')
            elif(status == 'Blocked'):
                print("Operation "+row+ " added to queue as transaction "+TID+ " is in "+status+ " state")
                addOperationsToQueue(TID, row)

    if row[0] == 'e':
        if checkIfTransactionExists(TID):
            status = getTransactionStatus(TID)
            if(status == 'Aborted'):
                print('Transaction '+TID +' is already Aborted. No changes in tables')
            elif(status == 'Blocked'):
                print("Operation "+row+ " added to queue as transaction "+TID+ " is in "+status+ " state")
                addOperationToQueue(TID, row)
            elif(status == "Committed"):
                print('Transaction: '+TID +' is already committed')
            else:
                print("Transaction "+TID+" committed")
                commit(TID)




def main():
    
    global transactionTable
    global lockingTable
    global blockTable

    fileName = input("****************************************************************************\nEnter the input file Name, please add '.txt' to end of the file name: \n****************************************************************************\n")
    # opening the input file on the read mode
    
    try:
        inputFile = open(fileName, 'r')
        for row in inputFile.readlines():
            if row:
                row = row.replace(' ', '')
                if row != '\n':
                    currentTransaction = 'T' + row[1]
                    print("\n\n")
                    row = row.replace('\n','')
                    print("operation: "+row)
                    
                    executeOperation(row, currentTransaction)
    
    except:
        print("Please enter the correct name, it is a case sensitive. Make sure to add '.txt' to end of the file name")
        main()
            
   


if __name__ == '__main__':
    main() 