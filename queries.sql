

-- Q1-Q5
-- projects
SELECT DEDUP * FROM synthetic.projects WHERE funder LIKE '%European%'
SELECT DEDUP * FROM synthetic.projects WHERE fundingLevel1 LIKE '%Behavioral%' OR title LIKE '%Collaborative%'
SELECT DEDUP * FROM synthetic.projects WHERE fundingLevel0 LIKE '%Engineering%' OR fundingLevel0 LIKE '%Biological%' OR fundingLevel0 LIKE '%Education'
SELECT DEDUP * FROM synthetic.projects WHERE fundingLevel0 LIKE '%Directorate%'
SELECT DEDUP * FROM synthetic.projects WHERE funder LIKE '%National%' 

-- organisations
SELECT DEDUP * FROM synthetic.organisations WHERE country LIKE '%France%'
SELECT DEDUP * FROM synthetic.organisations WHERE country LIKE '%Italy%'
SELECT DEDUP * FROM synthetic.organisations WHERE country LIKE '%Germany%'
SELECT DEDUP * FROM synthetic.organisations WHERE country IN ('Sweden', 'France', 'Italy', 'Germany')
SELECT DEDUP * FROM synthetic.organisations WHERE country IN ('Sweden', 'France', 'Italy', 'Germany', 'United Kingdom', 'India', 'Russia', 'Spain', 'Netherlands')

--  PEOPLE --
SELECT DEDUP id FROM synthetic.people200k WHERE state = 'vic'
SELECT DEDUP * FROM synthetic.people200k WHERE state = 'nsw'
SELECT DEDUP * FROM synthetic.people200k WHERE state = 'vic' OR state = 'nsw'
SELECT DEDUP * FROM synthetic.people200k WHERE age > 27
SELECT DEDUP * FROM synthetic.people200k WHERE age > 19

SELECT DEDUP id FROM synthetic.people500k WHERE state = 'vic'
SELECT DEDUP * FROM synthetic.people500k WHERE state = 'nsw'
SELECT DEDUP * FROM synthetic.people500k WHERE state = 'vic' OR state = 'nsw'
SELECT DEDUP * FROM synthetic.people500k WHERE age > 27
SELECT DEDUP * FROM synthetic.people500k WHERE age > 19

SELECT DEDUP id FROM synthetic.people1m WHERE state = 'vic'
SELECT DEDUP * FROM synthetic.people1m WHERE state = 'nsw'
SELECT DEDUP * FROM synthetic.people1m WHERE state = 'vic' OR state = 'nsw'
SELECT DEDUP * FROM synthetic.people1m WHERE age > 27
SELECT DEDUP * FROM synthetic.people1m WHERE age > 19

SELECT DEDUP id FROM synthetic.people1m5k WHERE state = 'vic'
SELECT DEDUP * FROM synthetic.people1m5k WHERE state = 'nsw'
SELECT DEDUP * FROM synthetic.people1m5k WHERE state = 'vic' OR state = 'nsw'
SELECT DEDUP * FROM synthetic.people1m5k WHERE age > 27
SELECT DEDUP * FROM synthetic.people1m5k WHERE age > 19

SELECT DEDUP id FROM synthetic.people2m WHERE state = 'vic'
SELECT DEDUP * FROM synthetic.people2m WHERE state = 'nsw'
SELECT DEDUP * FROM synthetic.people2m WHERE state = 'vic' OR state = 'nsw'
SELECT DEDUP * FROM synthetic.people2m WHERE age > 27
SELECT DEDUP * FROM synthetic.people2m WHERE age > 19

-- 	Q6
SELECT DEDUP * FROM synthetic.projects INNER JOIN synthetic.organisations ON name = organisation WHERE title LIKE '%Collaborative%'

-- Q7
SELECT DEDUP * FROM synthetic.projects INNER JOIN synthetic.organisations ON name = organisation WHERE fundingLevel0 LIKE '%Directorate%'


-- Q8 
SELECT DEDUP * FROM synthetic.people200k INNER JOIN synthetic.organisations ON name = organisation WHERE state = 'vic' 

SELECT DEDUP * FROM synthetic.people500k INNER JOIN synthetic.organisations ON name = organisation WHERE state = 'vic' 

SELECT DEDUP * FROM synthetic.people1m INNER JOIN synthetic.organisations ON name = organisation WHERE state = 'vic' 

SELECT DEDUP * FROM synthetic.peopl1m5k INNER JOIN synthetic.organisations ON name = organisation WHERE state = 'vic' 

SELECT DEDUP * FROM synthetic.people2m INNER JOIN synthetic.organisations ON name = organisation WHERE state = 'vic' 

-- Q9 SCALABILITY

SELECT DEDUP * FROM synthetic.people200k WHERE MOD(rec_id, 10) < 1 
SELECT DEDUP * FROM synthetic.people500k WHERE MOD(rec_id, 10) < 1 
SELECT DEDUP * FROM synthetic.people1m WHERE MOD(rec_id, 10) < 1 
SELECT DEDUP * FROM synthetic.peopl1m5k WHERE MOD(rec_id, 10) < 1 
SELECT DEDUP * FROM synthetic.people2m WHERE MOD(rec_id, 10) < 1 
