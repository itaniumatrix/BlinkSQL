#Factored out from run_Spark.py
#TODO: add import statements in other run_ files

mapQuery = {
    1 : "SELECT count(1) AS numPosts from SparkTempTable",
    2 : "SELECT author, avg(score) as avgScore from SparkTempTable GROUP BY author ORDER BY avgScore DESC limit 10",
    3 : "SELECT count(1) from SparkTempTable WHERE body like '%San Francisco%'",
    4 : "SELECT a.author, a.parent_id, a.score, b.author, b.parent_id, b.subreddit, b.name, b.score,\
            b.link_id from SparkTempTable AS a, SparkTempTable AS b WHERE a.year =b.year AND  a.score > 1000 AND\
            a.parent_id LIKE 't3%' AND a.name = b.parent_id AND b.score > a.score ORDER BY b.score DESC LIMIT 10",
    5 : "SELECT a.score, b.parent_id, b.subreddit, b.score, b.link_id from SparkTempTable AS a, SparkTempTable AS b\
            WHERE a.year =b.year AND  a.score > 1000 AND a.name = b.parent_id AND b.score > a.score ORDER BY b.score DESC LIMIT 10",
    6 : "SELECT a.parent_id, a.score, b.parent_id, b.score from SparkTempTable AS a,\
        (select parent_id, score from SparkTempTable) AS\
    b WHERE a.score > 1000 AND a.parent_id LIKE 't3%' AND a.name = b.parent_id AND\
    b.score > a.score ORDER BY b.score DESC LIMIT 10",
    7 : "SELECT COUNT(1) FROM (SELECT a.author, a.parent_id, a.score, b.author, b.parent_id, b.subreddit, b.name, b.score,\
            b.link_id from SparkTempTable AS a, SparkTempTable AS b WHERE a.year =b.year AND a.score > 1000 AND\
            a.parent_id LIKE 't3%' AND a.name = b.parent_id AND b.score > a.score ORDER BY b.score DESC) AS best"
}

