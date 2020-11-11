
-- SINGLE TABLE --
SELECT 1 FROM synthetic.organisations

-- SELECT DEDUP * FROM  synthetic.organisations INNER JOIN synthetic.people100k ON people100k.organisation = name WHERE state = 'vic'

-- SELECT DEDUP * FROM  synthetic.organisations INNER JOIN synthetic.full200k ON full200k.organisation = name  INNER JOIN synthetic.projects ON projects.organisation = name WHERE MOD(organisations.rec_id, 2) < 1 AND MOD(projects.rec_id, 10) < 1

-- SELECT DEDUP * FROM  synthetic.organisations INNER JOIN synthetic.full200k ON full200k.organisation = name  INNER JOIN synthetic.projects ON projects.organisation = name WHERE state = 'vic'
--  PEOPLE --
-- 15%, 25%, 35%, 55%, 70% 
-- SELECT DEDUP * FROM synthetic.people100k WHERE state = 'qld' 
-- SELECT DEDUP id FROM synthetic.people100k WHERE state = 'vic'
-- SELECT DEDUP * FROM synthetic.people100k WHERE state = 'nsw'
-- SELECT DEDUP * FROM synthetic.people100k WHERE state = 'vic' OR state = 'nsw'
-- SELECT DEDUP * FROM synthetic.people100k WHERE age > 27
-- SELECT DEDUP * FROM synthetic.people100k WHERE age > 19

-- projects -- 
-- 5%, 10%, 15%, 40%, 70%, 80%

--SELECT DEDUP * FROM synthetic.projects WHERE title LIKE '%Collaborative%'
--SELECT DEDUP * FROM synthetic.projects WHERE funder LIKE '%European%'
--SELECT DEDUP * FROM synthetic.projects WHERE fundingLevel1 LIKE '%Behavioral%' OR title LIKE '%Collaborative%'
--SELECT DEDUP * FROM synthetic.projects WHERE fundingLevel0 LIKE '%Engineering%' OR fundingLevel0 LIKE '%Biological%' OR fundingLevel0 LIKE '%Education'
-- SELECT DEDUP * FROM synthetic.projects WHERE fundingLevel0 LIKE '%Directorate%'
-- SELECT DEDUP * FROM synthetic.projects WHERE funder LIKE '%National%' 

-- orgs --
-- 2%, 6%, 8%, 10%, 25%, 50%
-- SELECT DEDUP * FROM synthetic.organisations WHERE country LIKE '%Sweden%'
-- SELECT DEDUP * FROM synthetic.organisations WHERE country LIKE '%France%'
-- SELECT DEDUP * FROM synthetic.organisations WHERE country LIKE '%Italy%'
-- SELECT DEDUP * FROM synthetic.organisations WHERE country LIKE '%Germany%'
-- SELECT DEDUP * FROM synthetic.organisations WHERE country IN ('Sweden', 'France', 'Italy', 'Germany')
-- SELECT DEDUP * FROM synthetic.organisations WHERE country IN ('Sweden', 'France', 'Italy', 'Germany', 'United Kingdom', 'India', 'Russia', 'Spain', 'Netherlands')

-- SELECT DEDUP * FROM synthetic.people100k INNER JOIN synthetic.organisations ON name = organisation WHERE state = 'vic' 
--SELECT DEDUP * FROM synthetic.people100k INNER JOIN synthetic.organisations ON name = organisation WHERE age > 19


-- SCALABILITY 
-- SELECT DEDUP * FROM synthetic.full200k WHERE MOD(rec_id, 10) < 1 
-- SELECT DEDUP * FROM synthetic.full500k WHERE MOD(rec_id, 25) < 1 
-- SELECT DEDUP * FROM synthetic.full1m WHERE MOD(rec_id, 50) < 1 
-- SELECT DEDUP * FROM synthetic.full1m5km WHERE MOD(rec_id, 75) < 1 
-- SELECT DEDUP * FROM synthetic.full2m WHERE MOD(rec_id, 100) < 1 

-- SELECT DEDUP * FROM synthetic.full200k WHERE MOD(rec_id, 5) < 1 
-- SELECT DEDUP * FROM synthetic.full500k WHERE MOD(rec_id, 12.5) < 1 
-- SELECT DEDUP * FROM synthetic.full1m WHERE MOD(rec_id, 25) < 1 
-- SELECT DEDUP * FROM synthetic.full1m5km WHERE MOD(rec_id, 37.5) < 1 
-- SELECT DEDUP * FROM synthetic.full2m WHERE MOD(rec_id, 50) < 1

-- SELECT DEDUP * FROM synthetic.full200k WHERE MOD(rec_id, 2) < 1 
-- SELECT DEDUP * FROM synthetic.full500k WHERE MOD(rec_id, 5) < 1 
-- SELECT DEDUP * FROM synthetic.full1m WHERE MOD(rec_id, 10) < 1 
-- SELECT DEDUP * FROM synthetic.full1m5km WHERE MOD(rec_id, 15) < 1 
-- SELECT DEDUP * FROM synthetic.full2m WHERE MOD(rec_id, 20) < 1

-- SELECT DEDUP * FROM synthetic.people50k INNER JOIN synthetic.organisations ON organisations.name = people50k.organisation WHERE age = 80
-- SELECT DEDUP * FROM synthetic.people50k INNER JOIN synthetic.organisations ON organisations.name = people50k.organisation WHERE MOD(organisations.rec_id, 20) < 1 
SELECT DEDUP * FROM synthetic.people50k INNER JOIN synthetic.organisations ON organisations.name = people50k.organisation  WHERE age > 80 AND country LIKE '%Germany%'

-- SELECT DEDUP * FROM synthetic.organisations INNER JOIN synthetic.people50k ON organisations.name = people50k.organisation WHERE age > 80
-- SELECT DEDUP * FROM synthetic.organisations INNER JOIN synthetic.people50k ON organisations.name = people50k.organisation WHERE MOD(organisations.rec_id, 20) < 1
SELECT DEDUP * FROM synthetic.organisations INNER JOIN synthetic.people50k ON organisations.name = people50k.organisation WHERE age > 80 AND country LIKE '%Germany%'
