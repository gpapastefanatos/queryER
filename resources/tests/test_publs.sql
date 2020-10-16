
-- PUBLICATIONS
-- -- SELECT DISTINCT authors FROM DIRTY.publications
-- SELECT DEDUP * FROM DIRTY.publications
SELECT 1 FROM DIRTY.publications
SELECT DEDUP * FROM DIRTY.publications WHERE authors LIKE '%Ioannidis%'
-- SELECT DEDUP * FROM DIRTY.publications WHERE MOD(id, 4) < 1 
SELECT DEDUP * FROM DIRTY.publications WHERE year BETWEEN 1996 AND 2004
SELECT DEDUP * FROM DIRTY.publications WHERE year = 1995
SELECT DEDUP * FROM DIRTY.publications WHERE year = 1996 
SELECT DEDUP * FROM DIRTY.publications WHERE year = 1995
SELECT DEDUP * FROM DIRTY.publications WHERE year = 1994
-- SELECT DEDUP * FROM DIRTY.publications WHERE year >= 1993
-- SELECT DEDUP * FROM DIRTY.publications WHERE year >= 1992
-- SELECT DEDUP * FROM DIRTY.publications WHERE year >= 1991
-- SELECT DEDUP * FROM DIRTY.publications WHERE MOD(id, 2.5) < 1 
-- SELECT DEDUP * FROM DIRTY.publications WHERE year >= 1990
-- SELECT DEDUP * FROM DIRTY.publications WHERE authors LIKE '%Ioannidis%' AND year = 1996
-- SELECT DEDUP * FROM DIRTY.publications WHERE MOD(id, 20) < 1 
-- SELECT DEDUP * FROM DIRTY.publications WHERE MOD(id, 10) < 1 
-- SELECT DEDUP * FROM DIRTY.publications WHERE MOD(id, 6.6) < 1
-- SELECT DEDUP * FROM DIRTY.publications WHERE MOD(id, 5) < 1 
-- SELECT DEDUP * FROM DIRTY.publications WHERE MOD(id, 4) < 1
-- SELECT DEDUP * FROM DIRTY.publications WHERE MOD(id, 3.3) < 1  
-- SELECT DEDUP * FROM DIRTY.publications WHERE MOD(id, 2.85) < 1
-- SELECT DEDUP * FROM DIRTY.publications WHERE MOD(id, 2.5) < 1  
-- SELECT DEDUP * FROM DIRTY.publications WHERE MOD(id, 2.22) < 1
-- SELECT DEDUP * FROM DIRTY.publications WHERE MOD(id, 2) < 1 