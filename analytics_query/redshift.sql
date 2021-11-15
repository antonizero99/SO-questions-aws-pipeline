-- This query answer question of how many questions and answers in each month in the dataset
SELECT d.[year]
	, d.[month]
	, COUNT(q.id)
	, COUNT(q.answerCount)
FROM factQuestion q
	LEFT JOIN dimDate d ON q.date = d.date
GROUP BY d.[year], d.[month]


-- This query answer question of how many questions are there in each types of tag
SELECT qt.Tag
	, COUNT(q.id)
FROM factQuestionTag qt
	LEFT JOIN factQuestion q ON q.id = qt.questionID
	LEFT JOIN dimTag t ON qt.TagID = t.TagID
GROUP BY qt.Tag