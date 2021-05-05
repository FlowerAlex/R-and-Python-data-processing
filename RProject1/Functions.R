library(sqldf)
library(dplyr)
Votes <- read.csv("Votes.csv.gz")
Posts <- read.csv("Posts.csv.gz")
Users <- read.csv("Users.csv.gz")

head(Votes)

# Zadanie 1.1
res <- sqldf::sqldf(
  "SELECT UpVotesTab.*, Posts.Title FROM
(
SELECT PostId, COUNT(*) AS UpVotes
FROM Votes
WHERE VoteTypeId=2
GROUP BY PostId
) AS UpVotesTab
JOIN Posts ON UpVotesTab.PostId=Posts.Id
WHERE Posts.PostTypeId=1
ORDER BY UpVotesTab.UpVotes DESC
LIMIT 10
"
)
res

#Zadanie 1.2

VotesWithId2 <- Votes[Votes$VoteTypeId==2,] 
upVotesTab <- aggregate(VotesWithId2[,'PostId'],by = list(PostId = VotesWithId2$PostId),FUN = length)
names(upVotesTab)[2] <- 'UpVotes'
resUpVotesTabNames <- names(upVotesTab)

upVotesTab <- merge(upVotesTab,Posts[Posts$PostTypeId==1,],by.x="PostId", by.y ="Id")
upVotesTab <- upVotesTab[order(-upVotesTab$UpVotes),]
res <- subset(upVotesTab,select = c(resUpVotesTabNames,"Title"))
head(res,10)

#Zadanie 1.3

library(dplyr)

upVotesTab <- filter(Votes, VoteTypeId==2)
upVotesTab <- group_by(upVotesTab,PostId)
upVotesTab <- summarise(upVotesTab,UpVotes = length(PostId))
  
upVotesTab <- inner_join(upVotesTab,Posts[Posts$PostTypeId==1,],by = c("PostId" = "Id"))
upVotesTab <- select(upVotesTab,PostId,UpVotes,Title)
upVotesTab <- head(arrange(upVotesTab,desc(upVotesTab$UpVotes)),10)

#zadanie 1.4







#zadanie 2.1
res <- sqldf::sqldf(
  "SELECT Users.DisplayName, Users.Age, Users.Location,
AVG(Posts.Score) as PostsMeanScore,
MAX(Posts.CreationDate) AS LastPostCreationDate
FROM Posts
JOIN Users ON Users.AccountId=Posts.OwnerUserId
WHERE OwnerUserId != -1
GROUP BY OwnerUserId
ORDER BY PostsMeanScore DESC
LIMIT 10
"
)

#Zadanie 2.2

PostsUsers <- merge(Posts[Posts$OwnerUserId != -1,],Users,by.x="OwnerUserId", by.y ="AccountId")
df1 <- aggregate(Score~OwnerUserId,PostsUsers,mean)
colnames(df1)[2] <- "PostsMeanScore"
df2 <- aggregate(CreationDate.x~OwnerUserId,PostsUsers,max)
colnames(df2)[2] <- "LastPostCreationDate"
mergedData <- merge(merge(df1,df2),PostsUsers)
uniqueMergedData <- unique(subset(mergedData,select = c("DisplayName","Age","Location","PostsMeanScore", "LastPostCreationDate")))
res <- head(uniqueMergedData[order(-uniqueMergedData$PostsMeanScore),],10)

#Zadanie 2.3

library(dplyr)

PostsUsers <- inner_join(Posts[Posts$OwnerUserId != -1,],Users,by = c("OwnerUserId" = "AccountId"))
PostsUsers <- group_by(PostsUsers,OwnerUserId,DisplayName,Age,Location)
PostsUsers <- summarise(PostsUsers,PostsMeanScore = mean(Score),LastPostCreationDate = max(CreationDate.x))
PostsUsers <- subset(PostsUsers,select=c("DisplayName","Age","Location","PostsMeanScore","LastPostCreationDate"))

res <- head(arrange(PostsUsers,desc(PostsUsers$PostsMeanScore)),10)

#Zadanie 2.4





#Zadanie 3.1

res <- sqldf::sqldf(
  "SELECT DisplayName, QuestionsNumber, AnswersNumber
FROM
(
SELECT COUNT(*) as AnswersNumber, Users.DisplayName, Users.Id
FROM Users JOIN Posts ON Users.Id = Posts.OwnerUserId
WHERE Posts.PostTypeId = 1
GROUP BY Users.Id
) AS Tab1
JOIN
(
SELECT COUNT(*) as QuestionsNumber, Users.Id
FROM Users JOIN Posts ON Users.Id = Posts.OwnerUserId
WHERE Posts.PostTypeId = 2
GROUP BY Users.Id
) AS Tab2
ON Tab1.Id = Tab2.Id
WHERE QuestionsNumber < AnswersNumber
ORDER BY AnswersNumber DESC"
)

#Zadanie 3.2

UsersPosts <- merge(Users, Posts[Posts$PostTypeId == 1,],by.x ="Id",by.y="OwnerUserId")

Tab1 <- aggregate(UsersPosts[,'Id'],by = list(Id = UsersPosts$Id),FUN = length)
colnames(Tab1)[2] <- "AnswersNumber"
Tab1 <- subset(merge(UsersPosts,Tab1), select = c("AnswersNumber","DisplayName","Id"))

UsersPosts <- merge(Users, Posts[Posts$PostTypeId == 2,],by.x ="Id",by.y="OwnerUserId")
Tab2 <- aggregate(UsersPosts[,'Id'],by = list(Id = UsersPosts$Id),FUN = length)
colnames(Tab2)[2] <- "QuestionsNumber"
Tab2 <- subset(merge(UsersPosts,Tab2), select = c("QuestionsNumber","Id"))

res <- unique(merge(Tab1,Tab2))
res <- res[res$QuestionsNumber < res$AnswersNumber,]
res <- res[order(-res$AnswersNumber),]

#Zadanie 3.3

UsersPosts <- inner_join(Users,Posts[Posts$PostTypeId == 1,],by = c("Id" = "OwnerUserId"))

Tab1 <- group_by(UsersPosts,Id,DisplayName)
Tab1 <- summarise(Tab1,AnswersNumber = length(Id))

UsersPosts <- inner_join(Users,Posts[Posts$PostTypeId == 2,],by = c("Id" = "OwnerUserId"))

Tab2 <- group_by(UsersPosts,Id)
Tab2 <- summarise(Tab2,QuestionsNumber = length(Id))

res <- inner_join(Tab1,Tab2, by="Id")
res <- filter(res,QuestionsNumber < AnswersNumber)
res <- subset(res,select = c("DisplayName", "QuestionsNumber", "AnswersNumber"))
res <- arrange(res,desc(res$AnswersNumber))

# Zadanie 3.4



#Zadanie 4.1

res <- sqldf::sqldf(
  "SELECT
Users.DisplayName,
Users.Age,
Users.Location,
SUM(Posts.FavoriteCount) AS FavoriteTotal,
Posts.Title AS MostFavoriteQuestion,
MAX(Posts.FavoriteCount) AS MostFavoriteQuestionLikes
FROM Posts
JOIN Users ON Users.Id=Posts.OwnerUserId
WHERE Posts.PostTypeId=1
GROUP BY OwnerUserId
ORDER BY FavoriteTotal DESC
LIMIT 10
"
)

#Zadanie 4.2

PostsUsers <- merge(Posts[Posts$PostTypeId == 1,],Users,by.x="OwnerUserId", by.y ="Id")
PostsUsers <- PostsUsers[!is.na(PostsUsers$FavoriteCount),]
df1 <- aggregate(FavoriteCount~OwnerUserId,PostsUsers,sum)
colnames(df1)[2] <- "FavoriteTotal"

df2 <- aggregate(FavoriteCount~OwnerUserId,PostsUsers,max)
colnames(df2)[2] <- "MostFavoriteQuestionLikes"

res <- merge(merge(df1,df2),PostsUsers)
colnames(res)[which(colnames(res)=="Title")] <- "MostFavoriteQuestion"
res <- res[res$FavoriteCount == res$MostFavoriteQuestionLikes,]
res <- unique(subset(res,select = c("DisplayName","Age","Location","FavoriteTotal","MostFavoriteQuestion","MostFavoriteQuestionLikes")))
res <- head(res[order(-res$FavoriteTotal),],10)

#Zadanie 4.3

PostsUsers <- inner_join(Posts[Posts$PostTypeId == 1,],Users,by = c("OwnerUserId" = "Id"))
PostsUsers <- filter(PostsUsers,!is.na(PostsUsers$FavoriteCount))

res <- group_by(PostsUsers,OwnerUserId)
res <- summarise(res, FavoriteTotal = sum(FavoriteCount), MostFavoriteQuestionLikes = max(FavoriteCount))
res <- inner_join(res,PostsUsers,by = c("OwnerUserId" = "OwnerUserId"))
res <- filter(res,FavoriteCount == MostFavoriteQuestionLikes)
res <- rename(res,MostFavoriteQuestion = Title)
res <- subset(res,select = c("DisplayName","Age","Location","FavoriteTotal","MostFavoriteQuestion","MostFavoriteQuestionLikes"))
res2 <- head(arrange(res,desc(res$FavoriteTotal)),10)



#Zadanie 5.1

res <- sqldf::sqldf(
  "SELECT
Questions.Id,
Questions.Title,
BestAnswers.MaxScore,
Posts.Score AS AcceptedScore,
BestAnswers.MaxScore-Posts.Score AS Difference
FROM (
SELECT Id, ParentId, MAX(Score) AS MaxScore
FROM Posts
WHERE PostTypeId==2
GROUP BY ParentId
) AS BestAnswers
JOIN (
SELECT * FROM Posts
WHERE PostTypeId==1
) AS Questions
ON Questions.Id=BestAnswers.ParentId
JOIN Posts ON Questions.AcceptedAnswerId=Posts.Id
ORDER BY Difference DESC
LIMIT 10
"
)

#Zadanie 5.2

BestAnswers <- Posts[Posts$PostTypeId == 2,]
df1 <- aggregate(Score~ParentId,BestAnswers,max)
colnames(df1)[2] <- "MaxScore"

BestAnswers <- merge(df1,BestAnswers, by.x = "ParentId",by.y = "ParentId")
BestAnswers <- BestAnswers[BestAnswers$MaxScore == BestAnswers$Score,]
BestAnswers <- subset(BestAnswers,select = c("Id","ParentId","MaxScore"))

Questions <- Posts[Posts$PostTypeId == 1,]
PostsQuestions <- merge(BestAnswers,Questions,by.x = "ParentId",by.y = "Id")
res <- merge(
  subset(PostsQuestions,select = c("AcceptedAnswerId","MaxScore","Title")),
  subset(Posts,select = c("Id","Score")),
             by.x = "AcceptedAnswerId",by.y = "Id")
res <- merge(res,Posts[,c("Id","AcceptedAnswerId")],by.x = "AcceptedAnswerId",by.y = "AcceptedAnswerId")

Difference <- res[,c("MaxScore")] - res[,c("Score")]

res <- cbind(res,Difference)
colnames(res)[which(colnames(res)=="Score")] <- "AcceptedScore"



res <- subset(res, select = c("Id","Title","MaxScore","AcceptedScore","Difference"))
res2 <- head(res[order(-res$Difference),],10)


# Zadanie 5.3
FilteredPosts <- Posts[Posts$PostTypeId == 2,]
BestAnswers <- group_by(FilteredPosts,ParentId)
BestAnswers <- summarise(BestAnswers,MaxScore = max(Score))
BestAnswers <- inner_join(BestAnswers,FilteredPosts,by = c("ParentId" = "ParentId"))

Questions <- Posts[Posts$PostTypeId == 1,]

res <- inner_join(select(BestAnswers,Id,ParentId,MaxScore),Questions,by = c("ParentId" = "Id"))
res <- select(res,AcceptedAnswerId,MaxScore,Title)
res <- unique(inner_join(res,Posts[,c("Id","Score")],by = c("AcceptedAnswerId" = "Id")))
res <- inner_join(res,Posts[,c("Id","AcceptedAnswerId")], by = c("AcceptedAnswerId" = "AcceptedAnswerId"))
Difference <- res[,c("MaxScore")] - res[,c("Score")]
Difference <- rename(Difference,Difference = MaxScore)
res <- rename(res,AcceptedScore = Score)
res <- bind_cols(res, Difference)
res <- subset(res, select = c("Id","Title","MaxScore","AcceptedScore","Difference"))
res2 <- head(res[order(-res$Difference),],10)



