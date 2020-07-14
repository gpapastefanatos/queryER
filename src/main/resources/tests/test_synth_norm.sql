
-- SINGLE TABLE --

-- PEOPLE --
SELECT * FROM synth.people_full100k WHERE MOD(rec_id, 20) < 1
SELECT * FROM synth.people_full100k WHERE MOD(rec_id, 10) < 1 
SELECT * FROM synth.people_full100k WHERE MOD(rec_id, 6.6) < 1
SELECT * FROM synth.people_full100k WHERE MOD(rec_id, 5) < 1 
SELECT * FROM synth.people_full100k WHERE MOD(rec_id, 4) < 1
SELECT * FROM synth.people_full100k WHERE MOD(rec_id, 3.3) < 1  
SELECT * FROM synth.people_full100k WHERE MOD(rec_id, 2.85) < 1
SELECT * FROM synth.people_full100k WHERE MOD(rec_id, 2.5) < 1  
SELECT * FROM synth.people_full100k WHERE MOD(rec_id, 2.22) < 1
SELECT * FROM synth.people_full100k WHERE MOD(rec_id, 2) < 1 


-- ORGS --
SELECT * FROM synth.orgs_with_dups WHERE MOD(rec_id, 20) < 1
SELECT * FROM synth.orgs_with_dups WHERE MOD(rec_id, 10) < 1 
SELECT * FROM synth.orgs_with_dups WHERE MOD(rec_id, 6.6) < 1
SELECT * FROM synth.orgs_with_dups WHERE MOD(rec_id, 5) < 1 
SELECT * FROM synth.orgs_with_dups WHERE MOD(rec_id, 4) < 1
SELECT * FROM synth.orgs_with_dups WHERE MOD(rec_id, 3.3) < 1  
SELECT * FROM synth.orgs_with_dups WHERE MOD(rec_id, 2.85) < 1
SELECT * FROM synth.orgs_with_dups WHERE MOD(rec_id, 2.5) < 1  
SELECT * FROM synth.orgs_with_dups WHERE MOD(rec_id, 2.22) < 1
SELECT * FROM synth.orgs_with_dups WHERE MOD(rec_id, 2) < 1 


-- synth.-- 
SELECT * FROM synth.projects_with_dups WHERE MOD(rec_id, 20) < 1
SELECT * FROM synth.projects_with_dups WHERE MOD(rec_id, 10) < 1 
SELECT * FROM synth.projects_with_dups WHERE MOD(rec_id, 6.6) < 1
SELECT * FROM synth.projects_with_dups WHERE MOD(rec_id, 5) < 1 
SELECT * FROM synth.projects_with_dups WHERE MOD(rec_id, 4) < 1
SELECT * FROM synth.projects_with_dups WHERE MOD(rec_id, 3.3) < 1  
SELECT * FROM synth.projects_with_dups WHERE MOD(rec_id, 2.85) < 1
SELECT * FROM synth.projects_with_dups WHERE MOD(rec_id, 2.5) < 1  
SELECT * FROM synth.projects_with_dups WHERE MOD(rec_id, 2.22) < 1
SELECT * FROM synth.projects_with_dups WHERE MOD(rec_id, 2) < 1 


-- 1 JOIN --
-- DIRTY-DIRTY, DIRTY-RIGHT, DIRTY-LEFT, CLEAN-CLEAN --
SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation

SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation WHERE MOD(people_full100k.rec_id, 20) < 1
SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation WHERE MOD(orgs_with_dups.rec_id, 20) < 1
SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation WHERE MOD(orgs_with_dups.rec_id, 20) < 1 AND MOD(people_full100k.rec_id, 20) < 1 

SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation WHERE MOD(people_full100k.rec_id, 10) < 1
SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation WHERE MOD(orgs_with_dups.rec_id, 10) < 1
SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation WHERE MOD(orgs_with_dups.rec_id, 10) < 1 AND MOD(people_full100k.rec_id, 10) < 1 

SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation WHERE MOD(people_full100k.rec_id, 6.6) < 1
SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation WHERE MOD(orgs_with_dups.rec_id, 6.6) < 1
SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation WHERE MOD(orgs_with_dups.rec_id, 6.6) < 1 AND MOD(people_full100k.rec_id, 6.6) < 1 

SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation WHERE MOD(people_full100k.rec_id, 5) < 1
SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation WHERE MOD(orgs_with_dups.rec_id, 5) < 1
SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation WHERE MOD(orgs_with_dups.rec_id, 5) < 1 AND MOD(people_full100k.rec_id, 5) < 1 

SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation WHERE MOD(people_full100k.rec_id, 4) < 1
SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation WHERE MOD(orgs_with_dups.rec_id, 4) < 1
SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation WHERE MOD(orgs_with_dups.rec_id, 4) < 1 AND MOD(people_full100k.rec_id, 4) < 1 

SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation WHERE MOD(people_full100k.rec_id, 3.3) < 1
SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation WHERE MOD(orgs_with_dups.rec_id, 3.3) < 1
SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation WHERE MOD(orgs_with_dups.rec_id, 3.3) < 1 AND MOD(people_full100k.rec_id, 3.3) < 1 

SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation WHERE MOD(people_full100k.rec_id, 2.85) < 1
SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation WHERE MOD(orgs_with_dups.rec_id, 2.85) < 1
SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation WHERE MOD(orgs_with_dups.rec_id, 2.85) < 1 AND MOD(people_full100k.rec_id, 2.85) < 1 

SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation WHERE MOD(people_full100k.rec_id, 2.5) < 1
SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation WHERE MOD(orgs_with_dups.rec_id, 2.5) < 1
SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation WHERE MOD(orgs_with_dups.rec_id, 2.5) < 1 AND MOD(people_full100k.rec_id, 2.5) < 1 

SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation WHERE MOD(people_full100k.rec_id, 2.22) < 1
SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation WHERE MOD(orgs_with_dups.rec_id, 2.22) < 1
SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation WHERE MOD(orgs_with_dups.rec_id, 2.22) < 1 AND MOD(people_full100k.rec_id, 2.22) < 1 

SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation WHERE MOD(people_full100k.rec_id, 2) < 1
SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation WHERE MOD(orgs_with_dups.rec_id, 2) < 1
SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation WHERE MOD(orgs_with_dups.rec_id, 2) < 1 AND MOD(people_full100k.rec_id, 2) < 1 

-- 2 JOIN --
SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation INNER JOIN synth.projects_with_dups ON name = projects_with_dups.organisation

SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation INNER JOIN synth.projects_with_dups ON name = projects_with_dups.organisation WHERE MOD(people_full100k.rec_id, 20) < 1

SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation INNER JOIN synth.projects_with_dups ON name = projects_with_dups.organisation WHERE MOD(orgs_with_dups.rec_id, 20) < 1

SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation INNER JOIN synth.projects_with_dups ON name = projects_with_dups.organisation WHERE MOD(orgs_with_dups.rec_id, 20) < 1 AND MOD(people_full100k.rec_id, 20) < 1 

SELECT * FROM synth.people_full100k INNER JOIN synth.orgs_with_dups ON name = organisation INNER JOIN synth.projects_with_dups ON name = projects_with_dups.organisation WHERE MOD(orgs_with_dups.rec_id, 20) < 1 AND MOD(people_full100k.rec_id, 20) < 1 AND MOD(projects_with_dups.rec_id, 20) < 1 

