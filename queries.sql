

-- Q1-Q5

-- publications
SELECT DEDUP * FROM DIRTY.publications WHERE authors LIKE '%A' OR authors LIKE '%a'
SELECT DEDUP * FROM DIRTY.publications WHERE year > 1995
SELECT DEDUP * FROM DIRTY.publications WHERE year >= 1991
SELECT DEDUP * FROM DIRTY.publications WHERE year >= 1900
SELECT DEDUP * FROM DIRTY.publications WHERE year >= 1900 OR authors LIKE '%e%'

-- projects
SELECT DEDUP * FROM all.projects WHERE funder LIKE '%European%'
SELECT DEDUP * FROM all.projects WHERE fundingLevel1 LIKE '%Behavioral%' OR title LIKE '%Collaborative%'
SELECT DEDUP * FROM all.projects WHERE fundingLevel0 LIKE '%Engineering%' OR fundingLevel0 LIKE '%Biological%' OR fundingLevel0 LIKE '%Education'
SELECT DEDUP * FROM all.projects WHERE fundingLevel0 LIKE '%Directorate%'
SELECT DEDUP * FROM all.projects WHERE funder LIKE '%National%' 

--  PEOPLE --
SELECT DEDUP id FROM all.people200k WHERE state = 'vic'
SELECT DEDUP * FROM all.people200k WHERE state = 'nsw'
SELECT DEDUP * FROM all.people200k WHERE state = 'vic' OR state = 'nsw'
SELECT DEDUP * FROM all.people200k WHERE age > 27
SELECT DEDUP * FROM all.people200k WHERE age > 19

SELECT DEDUP id FROM all.people500k WHERE state = 'vic'
SELECT DEDUP * FROM all.people500k WHERE state = 'nsw'
SELECT DEDUP * FROM all.people500k WHERE state = 'vic' OR state = 'nsw'
SELECT DEDUP * FROM all.people500k WHERE age > 27
SELECT DEDUP * FROM all.people500k WHERE age > 19

SELECT DEDUP id FROM all.people1m WHERE state = 'vic'
SELECT DEDUP * FROM all.people1m WHERE state = 'nsw'
SELECT DEDUP * FROM all.people1m WHERE state = 'vic' OR state = 'nsw'
SELECT DEDUP * FROM all.people1m WHERE age > 27
SELECT DEDUP * FROM all.people1m WHERE age > 19

SELECT DEDUP id FROM all.people1m5k WHERE state = 'vic'
SELECT DEDUP * FROM all.people1m5k WHERE state = 'nsw'
SELECT DEDUP * FROM all.people1m5k WHERE state = 'vic' OR state = 'nsw'
SELECT DEDUP * FROM all.people1m5k WHERE age > 27
SELECT DEDUP * FROM all.people1m5k WHERE age > 19

SELECT DEDUP id FROM all.people2m WHERE state = 'vic'
SELECT DEDUP * FROM all.people2m WHERE state = 'nsw'
SELECT DEDUP * FROM all.people2m WHERE state = 'vic' OR state = 'nsw'
SELECT DEDUP * FROM all.people2m WHERE age > 27
SELECT DEDUP * FROM all.people2m WHERE age > 19

-- 	Q6
SELECT DEDUP * FROM all.projects INNER JOIN all.organisations ON name = organisation WHERE title LIKE '%Collaborative%'

-- Q7
SELECT DEDUP * FROM all.projects INNER JOIN all.organisations ON name = organisation WHERE fundingLevel0 LIKE '%Directorate%'


-- Q8 
SELECT DEDUP * FROM all.people200k INNER JOIN all.organisations ON name = organisation WHERE state = 'vic' 

SELECT DEDUP * FROM all.people500k INNER JOIN all.organisations ON name = organisation WHERE state = 'vic' 

SELECT DEDUP * FROM all.people1m INNER JOIN all.organisations ON name = organisation WHERE state = 'vic' 

SELECT DEDUP * FROM all.peopl1m5k INNER JOIN all.organisations ON name = organisation WHERE state = 'vic' 

SELECT DEDUP * FROM all.people2m INNER JOIN all.organisations ON name = organisation WHERE state = 'vic' 

-- Q9 SCALABILITY

SELECT DEDUP * FROM all.people200k WHERE MOD(rec_id, 10) < 1 
SELECT DEDUP * FROM all.people500k WHERE MOD(rec_id, 10) < 1 
SELECT DEDUP * FROM all.people1m WHERE MOD(rec_id, 10) < 1 
SELECT DEDUP * FROM all.peopl1m5k WHERE MOD(rec_id, 10) < 1 
SELECT DEDUP * FROM all.people2m WHERE MOD(rec_id, 10) < 1 
