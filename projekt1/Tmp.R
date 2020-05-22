library(sqldf)
library(dplyr)
library(microbenchmark)
library(data.table)

options(stringsAsFactors = FALSE)
Badges <- read.csv("./travel_stackexchange_com/Badges.csv")
Tags <-  read.csv("./travel_stackexchange_com/Tags.csv")
Comments <-  read.csv("./travel_stackexchange_com/Comments.csv")
PostLinks <-  read.csv("./travel_stackexchange_com/PostLinks.csv")
Posts <-  read.csv("./travel_stackexchange_com/Posts.csv")
Users <-  read.csv("./travel_stackexchange_com/Users.csv")
Votes<-  read.csv("./travel_stackexchange_com/Votes.csv")
badges = data.table(Badges)
users = data.table(Users)
votes <- as.data.table(Votes)
posts <- as.data.table(Posts)
comments <- data.table(Comments)

# -------------------- 1
df <- sqldf("SELECT Posts.Title, UpVotesPerYear.Year, MAX(UpVotesPerYear.Count) AS Count 
       FROM(
            SELECT
                PostId,
                COUNT(*) AS Count,
                STRFTIME('%Y',Votes.CreationDate) AS Year
            FROM Votes
            WHERE VoteTypeId=2
            GROUP BY PostId, Year
       ) AS UpVotesPerYear
      JOIN Posts ON Posts.Id=UpVotesPerYear.PostId
      WHERE Posts.PostTypeId=1
      GROUP BY Year
      ")
# -------------------- 
# Wybrać z tabeli Posts te posty typu 1, które w danym roku(dla każdego z pojawiających się lat) uzyskały najwyższą ilość pozytywnych głosów 
# -------------------- 
UpVotesPerYear <- votes[VoteTypeId == 2,Year:=strftime(CreationDate,"%Y")]
UpVotesPerYear <- votes[,.(Count = .N),by= .(PostId,Year)]

dt <- merge.data.table(UpVotesPerYear,posts,by.x="PostId", by.y = "Id")
dt<- dt[PostTypeId == 1 & !is.na(Year),.SD[which.max(Count)],by= Year][,.(Title,Year, Count)]
all_equal(df,dt)
# --------------------
UpVotesPerYear <- (Votes %>%
  mutate(Year = format(as.Date(CreationDate),"%Y")) %>%
  group_by(PostId,Year) %>%
  filter(VoteTypeId == 2) %>%
  select(PostId, Year) %>%
  count(name="Count"))
  
dp <- right_join(Posts,UpVotesPerYear, by= c("Id" = "PostId"))%>%
  filter(PostTypeId == 1) %>%
  select(Title ,Year ,Count) %>%
  group_by(Year) %>%
  filter (Count == max(Count))

all_equal(df,dp)
# --------------------
VotesWithYears <- data.frame(Votes,lapply(Votes["CreationDate"],FUN = function(x) strftime(x,"%Y")),stringsAsFactors = FALSE)
colnames(VotesWithYears) <- c(colnames(Votes),"Year")
UpVotesPerYear <- as.data.frame(table(VotesWithYears[VotesWithYears$VoteTypeId == 2,c("PostId","Year") ]),stringsAsFactors = FALSE)
colnames(UpVotesPerYear) <- c("PostId","Year", "Count")
x <- merge(UpVotesPerYear,Posts,by.x = "PostId",by.y = "Id",all.x = TRUE, all.y = FALSE)
x2 <- x[x$PostTypeId==1, c("Title", "Year", "Count")]
db <- do.call(rbind, lapply(split(x2,as.factor(x2$Year)), function(x) {return(x[which.max(x$Count),])}))
db <- data.frame(db, stringsAsFactors = FALSE)
all_equal(df,db)
# --------------------
microbenchmark::microbenchmark(sqldf = {df <- sqldf("SELECT Posts.Title, UpVotesPerYear.Year, MAX(UpVotesPerYear.Count) AS Count 
       FROM(
            SELECT
                PostId,
                COUNT(*) AS Count,
                STRFTIME('%Y',Votes.CreationDate) AS Year
            FROM Votes
            WHERE VoteTypeId=2
            GROUP BY PostId, Year
       ) AS UpVotesPerYear
      JOIN Posts ON Posts.Id=UpVotesPerYear.PostId
      WHERE Posts.PostTypeId=1
      GROUP BY Year
      ")},
                               base = {VotesWithYears <- data.frame(Votes,lapply(Votes["CreationDate"],FUN = function(x) strftime(x,"%Y")),stringsAsFactors = FALSE)
                               colnames(VotesWithYears) <- c(colnames(Votes),"Year")
                               UpVotesPerYear <- as.data.frame(table(VotesWithYears[VotesWithYears$VoteTypeId == 2,c("PostId","Year") ]),stringsAsFactors = FALSE)
                               colnames(UpVotesPerYear) <- c("PostId","Year", "Count")
                               x <- merge(UpVotesPerYear,Posts,by.x = "PostId",by.y = "Id",all.x = TRUE, all.y = FALSE)
                               x2 <- x[x$PostTypeId==1, c("Title", "Year", "Count")]
                               db <- do.call(rbind, lapply(split(x2,as.factor(x2$Year)), function(x) {return(x[which.max(x$Count),])}))
                               db <- data.frame(db, stringsAsFactors = FALSE)},
                               dplyr = {UpVotesPerYear <- (Votes %>%
                                                             mutate(Year = format(as.Date(CreationDate),"%Y")) %>%
                                                             group_by(PostId,Year) %>%
                                                             filter(VoteTypeId == 2) %>%
                                                             select(PostId, Year) %>%
                                                             count(name="Count"))
                               
                               dp <- right_join(Posts,UpVotesPerYear, by= c("Id" = "PostId"))%>%
                                 filter(PostTypeId == 1) %>%
                                 select(Title ,Year ,Count) %>%
                                 group_by(Year) %>%
                                 filter (Count == max(Count))
                               },
                               data.table = {
                               UpVotesPerYear <- votes[VoteTypeId == 2,Year:=strftime(CreationDate,"%Y")]
                               UpVotesPerYear <- votes[,.(Count = .N),by= .(PostId,Year)]
                               
                               dt <- merge.data.table(UpVotesPerYear,posts,by.x="PostId", by.y = "Id")
                               dt<- dt[PostTypeId == 1 & !is.na(Year),.SD[which.max(Count)],by= Year][,.(Title,Year, Count)]})
# -------------------- 2
df <- sqldf("
      SELECT
        Users.DisplayName,
        Users.Age,
        Users.Location,
        SUM(Posts.FavoriteCount) AS FavoriteTotal,
        Posts.Title AS MostFavoriteQuestion,
        MAX(Posts.FavoriteCount) AS MostFavoriteQuestionsLikes
      FROM Posts
      JOIN Users ON Users.Id=Posts.OwnerUserId
      WHERE Posts.PostTypeId=1
      GROUP BY OwnerUserId
      ORDER BY FavoriteTotal DESC
      LIMIT 10
      ")
# -------------------- 
# Wybrać z tabeli Users użytkowników mających najwięcyj Polubień(FavoriteCount) ich postów, wypisanie jego najbardziej polubianego postu oraz ile ten post otrzymał polubień
# -------------------- 
bb <- merge.data.table(posts,users,by.x = "OwnerUserId",by.y = "Id")
bb <- bb[PostTypeId==1,.(FavoriteTotal = sum(FavoriteCount,na.rm = TRUE), DisplayName,Age,Location,MostFavoriteQuestion = Title,FavoriteCount),by=OwnerUserId][,.SD[which.max(FavoriteCount)],by = OwnerUserId]
bb <- bb[,.(DisplayName,Age,Location,FavoriteTotal,MostFavoriteQuestion,MostFavoriteQuestionsLikes = FavoriteCount )]
setorder(bb,-FavoriteTotal)
dt<- head(bb,10L)

all_equal(df,dt)
# --------------------
x <- data.frame(merge(Posts,Users,by.x = "OwnerUserId", by.y = "Id"))
x2 <- x[x$PostTypeId == 1, ]
z<- aggregate(x2[,"FavoriteCount"],by = x2["OwnerUserId"],FUN = sum, na.rm = TRUE)
z2 <- aggregate(x2[,"FavoriteCount"],by = x2["OwnerUserId"],FUN = max, na.rm =TRUE)
zz <- merge(z,z2,by = "OwnerUserId")
colnames(zz) <- c("OwnerUserId","FavoriteTotal","MostFavoriteQuestionsLikes")
x3 <- merge(x2,zz,by.x= c("OwnerUserId","FavoriteCount"),by.y = c("OwnerUserId","MostFavoriteQuestionsLikes"))
db <- head(x3[order(x3$FavoriteTotal,decreasing = TRUE),c("DisplayName","Age","Location","FavoriteTotal","MostFavoriteQuestion" = "Title",  "MostFavoriteQuestionsLikes" = "FavoriteCount")],10L)
colnames(db) <- c("DisplayName","Age","Location","FavoriteTotal","MostFavoriteQuestion",  "MostFavoriteQuestionsLikes")
all_equal(df,db)
# --------------------
dp <- Posts %>%
  filter(PostTypeId==1 & !is.na(FavoriteCount)) %>%
  group_by(OwnerUserId) %>%
  mutate(FavoriteTotal = sum(FavoriteCount,na.rm = TRUE),MostFavoriteQuestionsLikes = max(FavoriteCount,na.rm = TRUE)) %>%
  filter(FavoriteCount == max(FavoriteCount,na.rm = TRUE)) %>%
  right_join(Users, by= c("OwnerUserId"="Id")) %>% 
  ungroup() %>%
  select(DisplayName,Age,Location, FavoriteTotal,MostFavoriteQuestion = Title,MostFavoriteQuestionsLikes) %>%
  arrange(desc(FavoriteTotal))%>%
  head(n = 10L) 
  
all_equal(df,dp)
# --------------------
microbenchmark::microbenchmark(sqldf = {df <- sqldf("
      SELECT
        Users.DisplayName,
        Users.Age,
        Users.Location,
        SUM(Posts.FavoriteCount) AS FavoriteTotal,
        Posts.Title AS MostFavoriteQuestion,
        MAX(Posts.FavoriteCount) AS MostFavoriteQuestionsLikes
      FROM Posts
      JOIN Users ON Users.Id=Posts.OwnerUserId
      WHERE Posts.PostTypeId=1
      GROUP BY OwnerUserId
      ORDER BY FavoriteTotal DESC
      LIMIT 10
      ")},
                               base = {x <- data.frame(merge(Posts,Users,by.x = "OwnerUserId", by.y = "Id"))
                               x2 <- x[x$PostTypeId == 1, ]
                               z<- aggregate(x2[,"FavoriteCount"],by = x2["OwnerUserId"],FUN = sum, na.rm = TRUE)
                               z2 <- aggregate(x2[,"FavoriteCount"],by = x2["OwnerUserId"],FUN = max, na.rm =TRUE)
                               zz <- merge(z,z2,by = "OwnerUserId")
                               colnames(zz) <- c("OwnerUserId","FavoriteTotal","MostFavoriteQuestionsLikes")
                               x3 <- merge(x2,zz,by.x= c("OwnerUserId","FavoriteCount"),by.y = c("OwnerUserId","MostFavoriteQuestionsLikes"))
                               db <- head(x3[order(x3$FavoriteTotal,decreasing = TRUE),c("DisplayName","Age","Location","FavoriteTotal","MostFavoriteQuestion" = "Title",  "MostFavoriteQuestionsLikes" = "FavoriteCount")],10L)
                               colnames(db) <- c("DisplayName","Age","Location","FavoriteTotal","MostFavoriteQuestion",  "MostFavoriteQuestionsLikes")},
                               dplyr = {dp <- Posts %>%
                                 filter(PostTypeId==1 & !is.na(FavoriteCount)) %>%
                                 group_by(OwnerUserId) %>%
                                 mutate(FavoriteTotal = sum(FavoriteCount,na.rm = TRUE),MostFavoriteQuestionsLikes = max(FavoriteCount,na.rm = TRUE)) %>%
                                 filter(FavoriteCount == max(FavoriteCount,na.rm = TRUE)) %>%
                                 right_join(Users, by= c("OwnerUserId"="Id")) %>% 
                                 ungroup() %>%
                                 select(DisplayName,Age,Location, FavoriteTotal,MostFavoriteQuestion = Title,MostFavoriteQuestionsLikes) %>%
                                 arrange(desc(FavoriteTotal))%>%
                                 head(n = 10L) },
                               data.table = { bb <- merge.data.table(posts,users,by.x = "OwnerUserId",by.y = "Id")
                               bb <- bb[PostTypeId==1,.(FavoriteTotal = sum(FavoriteCount,na.rm = TRUE), DisplayName,Age,Location,MostFavoriteQuestion = Title,FavoriteCount),by=OwnerUserId][,.SD[which.max(FavoriteCount)],by = OwnerUserId]
                               bb <- bb[,.(DisplayName,Age,Location,FavoriteTotal,MostFavoriteQuestion,MostFavoriteQuestionsLikes = FavoriteCount )]
                               setorder(bb,-FavoriteTotal)
                               dt<- head(bb,10L)})
# -------------------- 3
df <- sqldf("
      SELECT
        Posts.ID,
        Posts.Title,
        Posts2.PositiveAnswerCount
      FROM Posts
      JOIN (
              SELECT
                Posts.ParentID,
                COUNT(*) AS PositiveAnswerCount
              FROM Posts
              WHERE Posts.PostTypeID=2 AND Posts.Score>0
              GROUP BY Posts.ParentID
           ) AS Posts2
           ON Posts.ID=Posts2.ParentID
      ORDER BY Posts2.PositiveAnswerCount DESC
      LIMIT 10
      ")
# --------------------
# Wybierz te posty które miały najwięcej Pozytywnie ocenionych odpowiedzi 
# --------------------
Posts2 <- posts[PostTypeId == 2 & Score > 0, .(PositiveAnswerCount= .N),by = ParentId]
rr <- merge.data.table(posts,Posts2,by.x = "Id",by.y = "ParentId")
setorder(rr,-PositiveAnswerCount)
dt <- head(rr[,.(Id,Title,PositiveAnswerCount)],10L)
all_equal(df,dt)
# --------------------
xx<-Posts[Posts$PostTypeId==2 & Posts$Score>0,]

Posts2 <- as.data.frame(table(xx$ParentId),stringsAsFactors = FALSE)
colnames(Posts2) <- c("ParentId", "PositiveAnswerCount")

xxx <- merge(Posts,Posts2,by.x = "Id", by.y = "ParentId")
db <- head(xxx[order(xxx$PositiveAnswerCount,decreasing = TRUE),c("Id","Title","PositiveAnswerCount")],10L)
all_equal(df,db)
# --------------------
dp <- Posts %>%
  left_join((Posts%>%
              group_by(ParentId) %>%
              filter(PostTypeId == 2 & Score > 0) %>%
              count(name = "PositiveAnswerCount"))
              , by = c("Id" = "ParentId")) %>%
  select(Id,Title,PositiveAnswerCount) %>%
  arrange(desc(PositiveAnswerCount))%>%
  head(n = 10L)
all_equal(df,dp)
# --------------------
microbenchmark::microbenchmark(sqldf = {df <- sqldf("
      SELECT
        Posts.ID,
        Posts.Title,
        Posts2.PositiveAnswerCount
      FROM Posts
      JOIN (
              SELECT
                Posts.ParentID,
                COUNT(*) AS PositiveAnswerCount
              FROM Posts
              WHERE Posts.PostTypeID=2 AND Posts.Score>0
              GROUP BY Posts.ParentID
           ) AS Posts2
           ON Posts.ID=Posts2.ParentID
      ORDER BY Posts2.PositiveAnswerCount DESC
      LIMIT 10
      ")},
                               base = {xx<-Posts[Posts$PostTypeId==2 & Posts$Score>0,]
                               
                               Posts2 <- as.data.frame(table(xx$ParentId),stringsAsFactors = FALSE)
                               colnames(Posts2) <- c("ParentId", "PositiveAnswerCount")
                               
                               xxx <- merge(Posts,Posts2,by.x = "Id", by.y = "ParentId")
                               db <- head(xxx[order(xxx$PositiveAnswerCount,decreasing = TRUE),c("Id","Title","PositiveAnswerCount")],10L)},
                               dplyr = {dp <- Posts %>%
                                 left_join((Posts%>%
                                              group_by(ParentId) %>%
                                              filter(PostTypeId == 2 & Score > 0) %>%
                                              count(name = "PositiveAnswerCount"))
                                           , by = c("Id" = "ParentId")) %>%
                                 select(Id,Title,PositiveAnswerCount) %>%
                                 arrange(desc(PositiveAnswerCount))%>%
                                 head(n = 10L)},
                               data.table = {Posts2 <- posts[PostTypeId == 2 & Score > 0, .(PositiveAnswerCount= .N),by = ParentId]
                               rr <- merge.data.table(posts,Posts2,by.x = "Id",by.y = "ParentId")
                               setorder(rr,-PositiveAnswerCount)
                               dt <- head(rr[,.(Id,Title,PositiveAnswerCount)],10L)})
# -------------------- 4
df <- sqldf("
      SELECT
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
      WHERE Difference>50
      ORDER BY Difference DESC
      ")
# --------------------
# wybranie dla każdego pytania (które spełnia warunek napisany na końcu) ilości punktów które otrzymała najlepsza odpowiedź, ilość punktów które otrzymała zaakceptowana odpowiedź, oraz różnica między tymi wartościami, ogranicz się do pytań, gdzie różnica jest > 50
# --------------------
BestAnswers <- posts[PostTypeId == 2, .SD[which.max(Score)],by = ParentId][,.(Id,ParentId,MaxScore = Score)]
Questions <- posts[PostTypeId == 1,]
dt <- merge.data.table(BestAnswers,Questions,by.x = "ParentId",by.y = "Id",suffixes = c(".BestAnswers",".Questions"))
dt <- merge.data.table(dt,posts,by.x = "AcceptedAnswerId",by.y = "Id", suffixes = c("",".Posts"))
dt <- dt[,Difference := MaxScore - Score.Posts ][Difference > 50,.(Id = ParentId, Title,MaxScore,AcceptedScore = Score.Posts,Difference)]
setorder(dt,-Difference)
all_equal(df,dt)
# --------------------
zz <- Posts[Posts$PostTypeId==2,]
BestAnswers <- aggregate(zz[,"Score"], by= zz["ParentId"],FUN = max, na.rm=TRUE)
colnames(BestAnswers) <- c("ParentId","MaxScore")
BestAnswers <- merge(zz[,c("Id","ParentId","Score")],BestAnswers,by="ParentId")
BestAnswers <- BestAnswers[BestAnswers$MaxScore == BestAnswers$Score,c("ParentId","Id","MaxScore")]
Questions <-Posts[Posts$PostTypeId == 1,]

res <- merge(BestAnswers,Questions, by.x = "ParentId",by.y = "Id",suffixes = c(".BestAnswers",".Questions"))
res <- merge(res,Posts,by.x = "AcceptedAnswerId",by.y = "Id",suffixes = c("",".Posts"))
res$Difference <- res$MaxScore - res$Score.Posts
res <- res[res$Difference > 50,]
res <- res[order(res$Difference,decreasing = TRUE),c("ParentId","Title","MaxScore","Score.Posts","Difference")]
colnames(res)<- c("Id","Title","MaxScore","AcceptedScore","Difference")
db <- res
all_equal(df,db)
# --------------------
dp <- (Posts %>%
  group_by(ParentId) %>%
  filter(PostTypeId == 2) %>%
  mutate(MaxScore = max(Score, na.rm= TRUE)) %>%
  select(Id, ParentId, MaxScore)) %>%
    right_join((Posts %>%
                 filter(PostTypeId==1)),by = c("ParentId" = "Id"),suffix = c(".BestAnswers",".Questions")) %>%
    right_join(Posts, by = c("AcceptedAnswerId" = "Id"),suffix=c("",".Posts")) %>%
    mutate(Difference = (MaxScore - Score.Posts)) %>%
    filter(Difference>50)%>%
  select(Id = ParentId, Title,MaxScore,AcceptedScore = Score.Posts,Difference) %>%
  distinct(Id, .keep_all = TRUE) %>%
  arrange(desc(Difference))
all_equal(df,dp)
# --------------------
microbenchmark::microbenchmark(sqldf = {df <- sqldf("
      SELECT
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
      WHERE Difference>50
      ORDER BY Difference DESC
      ")},
                               base = {zz <- Posts[Posts$PostTypeId==2,]
                               BestAnswers <- aggregate(zz[,"Score"], by= zz["ParentId"],FUN = max, na.rm=TRUE)
                               colnames(BestAnswers) <- c("ParentId","MaxScore")
                               BestAnswers <- merge(zz[,c("Id","ParentId","Score")],BestAnswers,by="ParentId")
                               BestAnswers <- BestAnswers[BestAnswers$MaxScore == BestAnswers$Score,c("ParentId","Id","MaxScore")]
                               Questions <-Posts[Posts$PostTypeId == 1,]
                               
                               res <- merge(BestAnswers,Questions, by.x = "ParentId",by.y = "Id",suffixes = c(".BestAnswers",".Questions"))
                               res <- merge(res,Posts,by.x = "AcceptedAnswerId",by.y = "Id",suffixes = c("",".Posts"))
                               res$Difference <- res$MaxScore - res$Score.Posts
                               res <- res[res$Difference > 50,]
                               res <- res[order(res$Difference,decreasing = TRUE),c("ParentId","Title","MaxScore","Score.Posts","Difference")]
                               colnames(res)<- c("Id","Title","MaxScore","AcceptedScore","Difference")
                               db <- res},
                               dplyr = {dp <- (Posts %>%
                                                 group_by(ParentId) %>%
                                                 filter(PostTypeId == 2) %>%
                                                 mutate(MaxScore = max(Score, na.rm= TRUE)) %>%
                                                 select(Id, ParentId, MaxScore)) %>%
                                 right_join((Posts %>%
                                               filter(PostTypeId==1)),by = c("ParentId" = "Id"),suffix = c(".BestAnswers",".Questions")) %>%
                                 right_join(Posts, by = c("AcceptedAnswerId" = "Id"),suffix=c("",".Posts")) %>%
                                 mutate(Difference = (MaxScore - Score.Posts)) %>%
                                 filter(Difference>50)%>%
                                 select(Id = ParentId, Title,MaxScore,AcceptedScore = Score.Posts,Difference) %>%
                                 distinct(Id, .keep_all = TRUE) %>%
                                 arrange(desc(Difference))
                               all_equal(df,dp)},
                               data.table = { BestAnswers <- posts[PostTypeId == 2, .SD[which.max(Score)],by = ParentId][,.(Id,ParentId,MaxScore = Score)]
                               Questions <- posts[PostTypeId == 1,]
                               dt <- merge.data.table(BestAnswers,Questions,by.x = "ParentId",by.y = "Id",suffixes = c(".BestAnswers",".Questions"))
                               dt <- merge.data.table(dt,posts,by.x = "AcceptedAnswerId",by.y = "Id", suffixes = c("",".Posts"))
                               dt <- dt[,Difference := MaxScore - Score.Posts ][Difference > 50,.(Id = ParentId, Title,MaxScore,AcceptedScore = Score.Posts,Difference)]
                               setorder(dt,-Difference)})
# -------------------- 5
df <- sqldf("
      SELECT
        Posts.Title,
        CmtTotScr.CommentsTotalScore
      FROM (
              SELECT
                PostID,
                UserID,
                SUM(Score) AS CommentsTotalScore
              FROM Comments
              GROUP BY PostID, UserID
           ) AS CmtTotScr
      JOIN Posts ON Posts.ID=CmtTotScr.PostID AND Posts.OwnerUserId=CmtTotScr.UserID
      WHERE Posts.PostTypeId=1
      ORDER BY CmtTotScr.CommentsTotalScore DESC
      LIMIT 10
      ")
# --------------------
# wypisz wszystkie posty typu 1, oraz wypisz ilość punktów zdobytych przez komentarze każdego z uzytkowników (dla każdego użytkownika)
# --------------------
CmtTotScr <- comments[,.(CommentsTotalScore = sapply(.SD,function(x){sum(x,na.rm = TRUE)})),by = .(PostId,UserId),.SDcols="Score"]
dt <- merge.data.table(CmtTotScr,posts,by.x=c("PostId","UserId"),by.y = c("Id","OwnerUserId"))[PostTypeId==1,]
dt <- dt[PostTypeId==1,.(Title,CommentsTotalScore)]
setorder(dt,-CommentsTotalScore)
dt <- head(dt,10L)
all_equal(df,dt)
# --------------------
CmtTotScr <- aggregate(Comments[,"Score"],by = Comments[,c("PostId","UserId")], FUN = sum)
colnames(CmtTotScr) <- c("PostId","UserId", "CommentsTotalScore")
ss <- merge(CmtTotScr,Posts,by.x = c("PostId","UserId"),by.y = c("Id","OwnerUserId"))
ss <- ss[ss$PostTypeId == 1,]
ss <- ss[order(ss$CommentsTotalScore,decreasing = TRUE),c("Title","CommentsTotalScore")]
db <- head(ss,10L)
all_equal(df,db)
# --------------------
dp <- (Comments %>%
    group_by(PostId,UserId) %>%
    summarise(CommentsTotalScore = sum(Score))) %>%
  full_join(Posts, by = c(PostId = "Id", UserId = "OwnerUserId")) %>%
  filter(PostTypeId == 1) %>%
  ungroup() %>%
  select(Title, CommentsTotalScore, -PostId) %>%
  arrange(desc(CommentsTotalScore)) %>%
  head(n = 10L)
all_equal(df,dp)
# --------------------
microbenchmark::microbenchmark(sqldf = {df <- sqldf("
      SELECT
        Posts.Title,
        CmtTotScr.CommentsTotalScore
      FROM (
              SELECT
                PostID,
                UserID,
                SUM(Score) AS CommentsTotalScore
              FROM Comments
              GROUP BY PostID, UserID
           ) AS CmtTotScr
      JOIN Posts ON Posts.ID=CmtTotScr.PostID AND Posts.OwnerUserId=CmtTotScr.UserID
      WHERE Posts.PostTypeId=1
      ORDER BY CmtTotScr.CommentsTotalScore DESC
      LIMIT 10
      ")},
                               base = {CmtTotScr <- aggregate(Comments[,"Score"],by = Comments[,c("PostId","UserId")], FUN = sum)
                               colnames(CmtTotScr) <- c("PostId","UserId", "CommentsTotalScore")
                               ss <- merge(CmtTotScr,Posts,by.x = c("PostId","UserId"),by.y = c("Id","OwnerUserId"))
                               ss <- ss[ss$PostTypeId == 1,]
                               ss <- ss[order(ss$CommentsTotalScore,decreasing = TRUE),c("Title","CommentsTotalScore")]
                               db <- head(ss,10L)},
                               dplyr = {dp <- (Comments %>%
                                                 group_by(PostId,UserId) %>%
                                                 summarise(CommentsTotalScore = sum(Score))) %>%
                                 full_join(Posts, by = c(PostId = "Id", UserId = "OwnerUserId")) %>%
                                 filter(PostTypeId == 1) %>%
                                 ungroup() %>%
                                 select(Title, CommentsTotalScore, -PostId) %>%
                                 arrange(desc(CommentsTotalScore)) %>%
                                 head(n = 10L)},
                               data.table = { CmtTotScr <- comments[,.(CommentsTotalScore = sapply(.SD,function(x){sum(x,na.rm = TRUE)})),by = .(PostId,UserId),.SDcols="Score"]
                               dt <- merge.data.table(CmtTotScr,posts,by.x=c("PostId","UserId"),by.y = c("Id","OwnerUserId"))[PostTypeId==1,]
                               dt <- dt[PostTypeId==1,.(Title,CommentsTotalScore)]
                               setorder(dt,-CommentsTotalScore)
                               dt <- head(dt,10L)})
# -------------------- 6
df <- sqldf("
      SELECT DISTINCT
        Users.Id,
        Users.DisplayName,
        Users.Reputation,
        Users.Age,
        Users.Location
      FROM (
            SELECT
              Name, UserID
            FROM Badges
            WHERE Name IN (
                            SELECT
                              Name
                            FROM Badges
                            WHERE Class=1
                            GROUP BY Name
                            HAVING COUNT(*) BETWEEN 2 AND 10
                          )
            AND Class=1
           ) AS ValuableBadges
      JOIN Users ON ValuableBadges.UserId=Users.Id
      ")
# --------------------
# wybierz tych użytkowników którzy otzymali jedną z Odznak klasy 1 która została wydana między 2 a 10 razy
# --------------------
yy <- badges[Class == 1, .(Cnt = .N) ,by = Name ][Cnt >=2 & Cnt <=10,] 
ValuableBadges <- badges[Class == 1 & Name %in% yy[,Name], .(Name, UserId)]
dt <- merge.data.table(ValuableBadges, users,by.x = "UserId", by.y = "Id")
dt <- dt[,.(Id = UserId,DisplayName,Reputation,Age,Location)]
dt <- unique(dt)
all_equal(df,dt)
# --------------------
ff <- Badges[Badges$Class ==1,]
ff <- as.data.frame(table(ff$Name),stringsAsFactors = FALSE)
colnames(ff) <- c("Name", "freq")
ff <- ff[ff$freq >=2 & ff$freq <=10,"Name"]

ValuableBadges <- Badges[Badges$Name %in% ff & Badges$Class == 1, c("Name","UserId")]
fff<-merge(ValuableBadges,Users,by.x = "UserId",by.y = "Id")
fff<-fff[,c("UserId","DisplayName","Reputation","Age","Location")]
fff <- unique(fff)
colnames(fff)<- c("Id","DisplayName","Reputation","Age","Location")
db <- fff
all_equal(df,db)
# --------------------
dp<-  Badges %>%
    filter(Class == 1 & Name %in% unlist(c(Badges %>%
      filter(Class == 1) %>%
      group_by(Name) %>%
      filter(n() >=2 & n() <=10) %>%
      select(Name)%>%
        distinct()))) %>%
    left_join(Users,c(UserId = "Id")) %>%
    select(Id = UserId, DisplayName, Reputation, Age, Location) %>%
    distinct()
all_equal(df,dp)
# --------------------
microbenchmark::microbenchmark(sqldf = {df <- sqldf("
      SELECT DISTINCT
        Users.Id,
        Users.DisplayName,
        Users.Reputation,
        Users.Age,
        Users.Location
      FROM (
            SELECT
              Name, UserID
            FROM Badges
            WHERE Name IN (
                            SELECT
                              Name
                            FROM Badges
                            WHERE Class=1
                            GROUP BY Name
                            HAVING COUNT(*) BETWEEN 2 AND 10
                          )
            AND Class=1
           ) AS ValuableBadges
      JOIN Users ON ValuableBadges.UserId=Users.Id
      ")},
                               base = {ff <- Badges[Badges$Class ==1,]
                               ff <- as.data.frame(table(ff$Name),stringsAsFactors = FALSE)
                               colnames(ff) <- c("Name", "freq")
                               ff <- ff[ff$freq >=2 & ff$freq <=10,"Name"]
                               
                               ValuableBadges <- Badges[Badges$Name %in% ff & Badges$Class == 1, c("Name","UserId")]
                               fff<-merge(ValuableBadges,Users,by.x = "UserId",by.y = "Id")
                               fff<-fff[,c("UserId","DisplayName","Reputation","Age","Location")]
                               fff <- unique(fff)
                               colnames(fff)<- c("Id","DisplayName","Reputation","Age","Location")
                               db <- fff},
                               dplyr = {dp<-  Badges %>%
                                 filter(Class == 1 & Name %in% unlist(c(Badges %>%
                                                                          filter(Class == 1) %>%
                                                                          group_by(Name) %>%
                                                                          filter(n() >=2 & n() <=10) %>%
                                                                          select(Name)%>%
                                                                          distinct()))) %>%
                                 left_join(Users,c(UserId = "Id")) %>%
                                 select(Id = UserId, DisplayName, Reputation, Age, Location) %>%
                                 distinct()},
                               data.table = { yy <- badges[Class == 1, .(Cnt = .N) ,by = Name ][Cnt >=2 & Cnt <=10,] 
                               ValuableBadges <- badges[Class == 1 & Name %in% yy[,Name], .(Name, UserId)]
                               dt <- merge.data.table(ValuableBadges, users,by.x = "UserId", by.y = "Id")
                               dt <- dt[,.(Id = UserId,DisplayName,Reputation,Age,Location)]
                               dt <- unique(dt)})
# --------------------  7 

df <- sqldf("
      SELECT
        Posts.Title,
        VotesByAge2.OldVotes
      FROM Posts
      JOIN (
            SELECT
              PostId,
              MAX(CASE WHEN VoteDate = 'new' THEN Total ELSE 0 END) NewVotes,
              MAX(CASE WHEN VoteDate = 'old' THEN Total ELSE 0 END) OldVotes,
              SUM(Total) AS Votes
            FROM (
                  SELECT
                    PostId,
                    CASE STRFTIME('%Y', CreationDate)
                      WHEN '2017' THEN 'new'
                      WHEN '2016' THEN 'new'
                      ELSE 'old'
                      END VoteDate,
                    COUNT(*) AS Total
                  FROM Votes
                  WHERE VoteTypeId=2
                  GROUP BY PostId, VoteDate
                 ) AS VotesByAge
            GROUP BY VotesByAge.PostId
            HAVING NewVotes=0
           ) AS VotesByAge2 ON VotesByAge2.PostId=Posts.ID
      WHERE Posts.PostTypeId=1
      ORDER BY VotesByAge2.OldVotes DESC
      LIMIT 10
      ")
# --------------------
#Wybrać i wypisać te posty, kóre maja najwięcej pozytywnych(VoteTypeId ==2) głosów oddanych w latach innych niż (2016 , 2017) i nie mają żadnych głosów oddanych w latach 2017-2016
# --------------------
VotesByAge <- votes[VoteTypeId == 2, .(PostId,VoteDate = ifelse(strftime(CreationDate,"%Y") %in% c("2016","2017"),"new","old"))]
VotesByAge <- VotesByAge[, .( Total = .N),by = .(PostId,VoteDate)]
VotesByAge2 <- VotesByAge[,.(Total,PostId,NewVotes2 = ifelse(VoteDate == "new",Total,0L),OldVotes2 = ifelse(VoteDate == "old",Total,0L))]
VotesByAge2 <- VotesByAge2[,.(NewVotes = max(NewVotes2, na.rm = TRUE),OldVotes = max(OldVotes2,na.rm = TRUE), Votes = sum(Total,na.rm = TRUE) ),by = PostId]
VotesByAge2<- VotesByAge2[NewVotes == 0,]
dt <- merge.data.table(VotesByAge2,Posts,by.x = "PostId",by.y = "Id")
dt <- dt[PostTypeId == 1,.(Title,OldVotes)]
setorder(dt,-OldVotes)
dt <- head(dt,10L)

all_equal(df,dt)
# --------------------
ap <- Votes[Votes$VoteTypeId == 2,c("PostId","CreationDate")]
ap <- transform(ap,VoteDate = ifelse(CreationDate == "2016","new", "old"))
ap$VoteDate <- sapply(ap$CreationDate,function(x) { z <- strftime(x,"%Y")
  if( z == "2017" | z == "2016")  return("new")
  else return("old")
})
ap <- ap[,c("PostId","VoteDate")]
aps <- aggregate(ap[,c("PostId","VoteDate")],by = ap[,c("PostId","VoteDate")], FUN = length)
aps <- aps[,c(1,2,3)]
colnames(aps) <- c("PostId","VoteDate","Total")

VotesByAge<- unique(merge(ap,aps,by=c("PostId","VoteDate")))

VotesByAge2 <- transform(VotesByAge,OldVotes = ifelse(VoteDate == "old",Total,0L),NewVotes = ifelse(VoteDate == "new",Total,0L))
VotesByAge22 <- aggregate(VotesByAge2[,c("OldVotes","NewVotes")],by = VotesByAge2[c("PostId")],FUN = max)
vba222 <- aggregate(VotesByAge2[,c("Total")],by = VotesByAge2[c("PostId")],FUN = sum)
VotesByAge2 <- merge(VotesByAge22,vba222,by="PostId")
VotesByAge2 <- VotesByAge2[VotesByAge2$NewVotes == 0 ,]
colnames(VotesByAge2) <- c("PostId","OldVotes","NewVotes","Votes")

db <- merge(Posts,VotesByAge2,by.x = "Id", by.y = "PostId")
db <- db[db$PostTypeId == 1,]
db <- head(db[order(db$OldVotes,decreasing = TRUE),c("Title", "OldVotes")],10L)

all_equal(df,db)
# --------------------
dp <- Posts %>%
  left_join((((Votes %>%
               filter(VoteTypeId == 2) %>%
                mutate(VoteDate2 = case_when(format(as.Date(CreationDate),"%Y") >= 2016 ~ "new",
                                             TRUE ~ "old"))) %>%
               group_by(PostId, VoteDate2) %>%
               count(name="Total") %>%
               select(PostId, VoteDate = VoteDate2, Total)) %>%
               mutate(
                      OldVotes2 = case_when(VoteDate == "old" ~ Total,
                                                                   TRUE ~ 0L),
                      NewVotes2 = case_when(VoteDate == "new" ~ Total,
                                               TRUE ~ 0L))%>%
                group_by(PostId)%>%
                mutate(Votes = sum(Total),OldVotes = max(OldVotes2,na.rm = TRUE), NewVotes = max(NewVotes2,na.rm = TRUE) ) %>%
                filter(OldVotes2 == OldVotes) %>%
               select(PostId, NewVotes, OldVotes, Votes)
               ) %>% filter(NewVotes == 0 ), by = c( Id = "PostId" )) %>%
  filter(PostTypeId == 1) %>%
  select(Title, OldVotes) %>%
  arrange(desc(OldVotes)) %>%
  head(n = 10L)
all_equal(df,dp)
# --------------------
microbenchmark::microbenchmark(sqldf = {df <- sqldf("
      SELECT
        Posts.Title,
        VotesByAge2.OldVotes
      FROM Posts
      JOIN (
            SELECT
              PostId,
              MAX(CASE WHEN VoteDate = 'new' THEN Total ELSE 0 END) NewVotes,
              MAX(CASE WHEN VoteDate = 'old' THEN Total ELSE 0 END) OldVotes,
              SUM(Total) AS Votes
            FROM (
                  SELECT
                    PostId,
                    CASE STRFTIME('%Y', CreationDate)
                      WHEN '2017' THEN 'new'
                      WHEN '2016' THEN 'new'
                      ELSE 'old'
                      END VoteDate,
                    COUNT(*) AS Total
                  FROM Votes
                  WHERE VoteTypeId=2
                  GROUP BY PostId, VoteDate
                 ) AS VotesByAge
            GROUP BY VotesByAge.PostId
            HAVING NewVotes=0
           ) AS VotesByAge2 ON VotesByAge2.PostId=Posts.ID
      WHERE Posts.PostTypeId=1
      ORDER BY VotesByAge2.OldVotes DESC
      LIMIT 10
      ")},
   base = {ap <- ap[,c("PostId","VoteDate")]
   aps <- aggregate(ap[,c("PostId","VoteDate")],by = ap[,c("PostId","VoteDate")], FUN = length)
   aps <- aps[,c(1,2,3)]
   colnames(aps) <- c("PostId","VoteDate","Total")
   
   VotesByAge<- unique(merge(ap,aps,by=c("PostId","VoteDate")))
   
   VotesByAge2 <- transform(VotesByAge,OldVotes = ifelse(VoteDate == "old",Total,0L),NewVotes = ifelse(VoteDate == "new",Total,0L))
   VotesByAge22 <- aggregate(VotesByAge2[,c("OldVotes","NewVotes")],by = VotesByAge2[c("PostId")],FUN = max)
   vba222 <- aggregate(VotesByAge2[,c("Total")],by = VotesByAge2[c("PostId")],FUN = sum)
   VotesByAge2 <- merge(VotesByAge22,vba222,by="PostId")
   VotesByAge2 <- VotesByAge2[VotesByAge2$NewVotes == 0 ,]
   colnames(VotesByAge2) <- c("PostId","OldVotes","NewVotes","Votes")
   
   db <- merge(Posts,VotesByAge2,by.x = "Id", by.y = "PostId")
   db <- db[db$PostTypeId == 1,]
   db <- head(db[order(db$OldVotes,decreasing = TRUE),c("Title", "OldVotes")],10L)},
   dplyr = {dp <- Posts %>%
     left_join((((Votes %>%
                    filter(VoteTypeId == 2) %>%
                    mutate(VoteDate2 = case_when(format(as.Date(CreationDate),"%Y") >= 2016 ~ "new",
                                                 TRUE ~ "old"))) %>%
                   group_by(PostId, VoteDate2) %>%
                   count(name="Total") %>%
                   select(PostId, VoteDate = VoteDate2, Total)) %>%
                  mutate(
                    OldVotes2 = case_when(VoteDate == "old" ~ Total,
                                          TRUE ~ 0L),
                    NewVotes2 = case_when(VoteDate == "new" ~ Total,
                                          TRUE ~ 0L))%>%
                  group_by(PostId)%>%
                  mutate(Votes = sum(Total),OldVotes = max(OldVotes2,na.rm = TRUE), NewVotes = max(NewVotes2,na.rm = TRUE) ) %>%
                  filter(OldVotes2 == OldVotes) %>%
                  select(PostId, NewVotes, OldVotes, Votes)
     ) %>% filter(NewVotes == 0 ), by = c( Id = "PostId" )) %>%
     filter(PostTypeId == 1) %>%
     select(Title, OldVotes) %>%
     arrange(desc(OldVotes)) %>%
     head(n = 10L)},
   data.table = { VotesByAge <- votes[VoteTypeId == 2, .(PostId,VoteDate = ifelse(strftime(CreationDate,"%Y") %in% c("2016","2017"),"new","old"))]
   VotesByAge <- VotesByAge[, .( Total = .N),by = .(PostId,VoteDate)]
   VotesByAge2 <- VotesByAge[,.(Total,PostId,NewVotes2 = ifelse(VoteDate == "new",Total,0L),OldVotes2 = ifelse(VoteDate == "old",Total,0L))]
   VotesByAge2 <- VotesByAge2[,.(NewVotes = max(NewVotes2, na.rm = TRUE),OldVotes = max(OldVotes2,na.rm = TRUE), Votes = sum(Total,na.rm = TRUE) ),by = PostId]
   VotesByAge2<- VotesByAge2[NewVotes == 0,]
   dt <- merge.data.table(VotesByAge2,Posts,by.x = "PostId",by.y = "Id")
   dt <- dt[PostTypeId == 1,.(Title,OldVotes)]
   setorder(dt,-OldVotes)
   dt <- head(dt,10L)})
# --------------------