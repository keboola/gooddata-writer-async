w# Keboola GoodData Writer

## Tables for Shared Config
- columns in bold should be primary keys
- columns in italics should be indices

### jobs
**id**,runId,projectId,writerId,token,tokenId,tokenDesc,tokenOwnerName,createdTime,startTime,endTime,command,dataset,parameters,result,gdWriteStartTime,status,logs,debug,definition,_projectIdWriterId,batchId,queueId_

### projects
**pid**,projectId,writerId,backendUrl,accessToken,createdTime,_projectIdWriterId_
 
### users
**uid**,email,projectId,writerId,createdTime,_projectIdWriterId_
 
### projects_to_delete
**pid**,projectId,writerId,dev,createdTime,_deletedTime_

### users_to_delete
**uid**,projectId,writerId,email,dev,createdTime,_deletedTime_