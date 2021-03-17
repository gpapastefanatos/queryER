SELECT 1 FROM oag.venues
SELECT DEDUP * FROM oag.venues WHERE DisplayName LIKE '%National%' OR DisplayName LIKE '%International%'
SELECT DEDUP * FROM oag.venues WHERE DisplayName LIKE '%Journal%'
SELECT DEDUP * FROM oag.venues WHERE DisplayName LIKE '%Journal%' OR DisplayName LIKE '%International%' OR DisplayName LIKE '%National%'
SELECT DEDUP * FROM oag.venues WHERE DisplayName LIKE '%Journal%' OR DisplayName LIKE '%International%' OR DisplayName LIKE '%National%' OR DisplayName LIKE '%Proceedings%' OR DisplayName LIKE '%computational%'  OR DisplayName LIKE '%Health%'  OR DisplayName LIKE '%Studies%' OR DisplayName LIKE '%Review%' OR DisplayName LIKE '%European%'