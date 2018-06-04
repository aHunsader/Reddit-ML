#!/usr/bin/env python3

from __future__ import print_function
from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
import itertools
from itertools import chain
import cleantext
import re
from pyspark.ml.feature import CountVectorizer
from pyspark.sql.functions import split, col
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator, CrossValidatorModel
from pyspark.sql.types import StringType


def main(context):
	"""Main function takes a Spark SQL context."""
	# YOUR CODE HERE
	# YOU MAY ADD OTHER FUNCTIONS AS NEEDED

	# context.udf.register("sanitize", san)
	# context.udf.register("three", remove_first_three)
	# context.udf.register("first", first__f)

	# labeled_data = context.read.format('csv').options(header="true").load('labeled_data.csv')
	# labeled = labeled_data.select(col("`Input.id`").alias("id"), col("labeldjt").alias("trump"))
	# # https://stackoverflow.com/questions/29936156/get-csv-to-spark-dataframe

	# # comments = sqlContext.read.json("comments-minimal.json.bz2")
	# # submissions = sqlContext.read.json("submissions.json.bz2")

	# comments = sqlContext.read.load("comments_data.parquet")
	# submissions = sqlContext.read.load("submissions_data.parquet")

	# comments.createOrReplaceTempView('comments')
	# submissions.createOrReplaceTempView('submissions')
	# labeled.createOrReplaceTempView('labeled')

	# sanitized = sqlContext.sql('select sanitize(body) as san, if(trump = 1, 1, 0) as positive, '
	# 	'if(trump = -1, 1, 0) as negative from comments inner join labeled on comments.id = labeled.id')
	# sanitized = sanitized.withColumn("san", split(col("san"), " ").cast("array<string>").alias("san"))


	# cv = CountVectorizer(inputCol = "san", outputCol = "features", binary=True, minDF=5.0)
	# cvmodel = cv.fit(sanitized)
	# result = cvmodel.transform(sanitized)
	# result.createOrReplaceTempView('results')

	# pos = sqlContext.sql('select positive as label, features from results')
	# neg = sqlContext.sql('select negative as label, features from results')



	# poslr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10).setThreshold(0.2)
	# neglr = LogisticRegression(labelCol="label", featuresCol="features", maxIter=10).setThreshold(0.25)

	# posEvaluator = BinaryClassificationEvaluator()
	# negEvaluator = BinaryClassificationEvaluator()

	# posParamGrid = ParamGridBuilder().addGrid(poslr.regParam, [1.0]).build()
	# negParamGrid = ParamGridBuilder().addGrid(neglr.regParam, [1.0]).build()

	# posCrossval = CrossValidator(
	#     estimator=poslr,
	#     evaluator=posEvaluator,
	#     estimatorParamMaps=posParamGrid,
	#     numFolds=5)
	# negCrossval = CrossValidator(
	#     estimator=neglr,
	#     evaluator=negEvaluator,
	#     estimatorParamMaps=negParamGrid,
	#     numFolds=5)
	
	# posTrain, posTest = pos.randomSplit([0.5, 0.5])
	# negTrain, negTest = neg.randomSplit([0.5, 0.5])

	# print("Training positive classifier...")
	# posModel = posCrossval.fit(posTrain)
	# print("Training negative classifier...")
	# negModel = negCrossval.fit(negTrain)

	# # posModel.save("pos.model")
	# # negModel.save("neg.model")

	# # posModel = CrossValidatorModel.load("pos.model")
	# # negModel = CrossValidatorModel.load("neg.model")


	# whole = sqlContext.sql('select comments.id as id, comments.author_flair_text as state, '
	# 	'comments.created_utc as time, submissions.title as title, submissions.score as story_score, '
	# 	'comments.score as comment_score, sanitize(body) as san from comments inner join '
	# 	'submissions on submissions.id = three(comments.link_id) where body not like "&gt%" '
	# 	'and body not like "%\\s%"')
	# whole = whole.withColumn("san", split(col("san"), " ").cast("array<string>").alias("san"))
	# whole.write.parquet("whole.parquet")

	# # whole = sqlContext.read.load("whole.parquet")

	# whole_result_pos = cvmodel.transform(whole)
	# whole_result_neg = whole_result_pos.select("*")

	# pos_ans = posModel.transform(whole_result_pos)
	# neg_ans = negModel.transform(whole_result_neg)
	# pos_ans.createOrReplaceTempView('positive')
	# neg_ans.createOrReplaceTempView('negative')


	# final = sqlContext.sql('select positive.id as id, positive.state as state, '
	# 	'positive.time as time, positive.title as title, positive.comment_score as comment_score, '
	# 	'positive.story_score as story_score, positive.prediction as pos, negative.prediction as neg '
	# 	'from positive inner join negative on positive.id = negative.id')
	# final.write.parquet("final.parquet")
	final = sqlContext.read.load('final.parquet')
	final.createOrReplaceTempView('final')

	states = ['Alabama', 'Alaska', 'Arizona', 'Arkansas', 'California', 'Colorado', 'Connecticut', 
	'Delaware', 'District of Columbia', 'Florida', 'Georgia', 'Hawaii', 'Idaho', 'Illinois', 'Indiana', 
	'Iowa', 'Kansas', 'Kentucky', 'Louisiana', 'Maine', 'Maryland', 'Massachusetts', 'Michigan', 
	'Minnesota', 'Mississippi', 'Missouri', 'Montana', 'Nebraska', 'Nevada', 'New Hampshire', 
	'New Jersey', 'New Mexico', 'New York', 'North Carolina', 'North Dakota', 'Ohio', 'Oklahoma', 
	'Oregon', 'Pennsylvania', 'Rhode Island', 'South Carolina', 'South Dakota', 'Tennessee', 'Texas', 
	'Utah', 'Vermont', 'Virginia', 'Washington', 'West Virginia', 'Wisconsin', 'Wyoming']
	statesdf = sqlContext.createDataFrame(states, StringType())
	statesdf.createOrReplaceTempView('states')

	percentage_total = sqlContext.sql('select title, 100 * sum(pos) / count(id) as pos, '
		'100 * sum(neg) / count(id) as neg from final group by title')
	percentage_day = sqlContext.sql('select Date(from_unixtime(time)) as date, '
		'100 * sum(pos) / count(id) as pos, 100 * sum(neg) / count(id) as neg '
		'from final group by Date(from_unixtime(time)) order by Date(from_unixtime(time))')
	percentage_state = sqlContext.sql('select state, 100 * sum(pos) / count(id) as pos, '
		'100 * sum(neg) / count(id) as neg from final inner join states on states.value = final.state group by state')
	percentage_comment_score = sqlContext.sql('select comment_score as score, '
		'100 * sum(pos) / count(id) as pos, 100 * sum(neg) / count(id) as neg from final group by comment_score')
	percentage_story_score = sqlContext.sql('select story_score as score, '
		'100 * sum(pos) / count(id) as pos, 100 * sum(neg) / count(id) as neg from final group by story_score')

	# percentage_total.createOrReplaceTempView('total')
	# sqlContext.sql('select title, pos from total order by pos desc limit 10').show(truncate=False)
	# sqlContext.sql('select title, neg from total order by neg desc limit 10').show(truncate=False)


	# percentage_total.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("total.csv")
	percentage_day.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("day.csv")
	# percentage_state.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("state.csv")
	# percentage_comment_score.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("comment_score.csv")
	# percentage_story_score.repartition(1).write.format("com.databricks.spark.csv").option("header", "true").save("story_score.csv")





def san(text):
	return " ".join(cleantext.sanitize(text)[1:3])

def remove_first_three(text):
	return text[3:]

def first__f(vector):
	return vector[1]

if __name__ == "__main__":
    conf = SparkConf().setAppName("CS143 Project 2B")
    conf = conf.setMaster("local[*]")
    sc   = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)
    sc.addPyFile("cleantext.py")
    main(sqlContext)

