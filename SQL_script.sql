----   QUERY FOR CREAETING PIVOT TABLE WITH STUDENTS PER COMMUNE FROM 2018 TO 2029   -----
-- Data from fondamental
WITH fond AS (
	SELECT school_year_start,
	    CASE
        	WHEN ele_com_id IS NULL AND lower(unaccent(pays_res)) = 'luxembourg' THEN 0
        	WHEN ele_com_id IS NULL AND lower(unaccent(pays_res)) = 'france' THEN 1
        	WHEN ele_com_id IS NULL AND lower(unaccent(pays_res)) = 'allemagne' THEN 2
        	WHEN ele_com_id IS NULL AND lower(unaccent(pays_res)) = 'belgique' THEN 3
        	ELSE COALESCE(ele_com_id, 4)
        END as com_id, ele_id,
	    CASE
	        WHEN cycle_code = 21 THEN 1
	        WHEN cycle_code IN (22, 29) THEN 2
	        WHEN cycle_code = 31 THEN 3
	        WHEN cycle_code IN (32, 39) THEN 4
	        WHEN cycle_code = 41 THEN 5
	        WHEN cycle_code IN (42, 49) THEN 6
	        ELSE cycle_code
	    END AS grade, COUNT(*) AS tot
	FROM dirty.fondamental_rentree_retro
	WHERE school_year_start >= 2020
		and cycle_code IN (21, 22, 29, 31, 32, 39, 41, 42, 49)
	GROUP BY 1, 2, 3, 4
),
--Data from secundaire
sec AS (
	SELECT school_year_start,
		CASE
        	WHEN com_id IS NULL AND lower(unaccent(pays_res)) = 'luxembourg' THEN 0
        	WHEN com_id IS NULL AND lower(unaccent(pays_res)) = 'france' THEN 1
        	WHEN com_id IS NULL AND lower(unaccent(pays_res)) = 'allemagne' THEN 2
        	WHEN com_id IS NULL AND lower(unaccent(pays_res)) = 'belgique' THEN 3
        	ELSE COALESCE(com_id, 4)
        END as com_id, ele_id, niveau as grade, COUNT(*) AS tot
	FROM dirty.pp_rentree_retro_2021
	WHERE school_year_start >= 2020
		and niveau between 7 and 13
	GROUP BY 1, 2, 3, 4
),
-- Concatenate the two tables together
combined AS (
	SELECT *
	FROM fond
	UNION ALL
	SELECT *
	FROM sec
	ORDER BY com_id, school_year_start, grade, ele_id
),
-- Calculate students repeating a grade per commune, year and grade
grade_repeating AS (
	SELECT com_id, grade, school_year_start, COUNT(*) AS number_repeating
	FROM (
	    SELECT com_id, ele_id, school_year_start, grade,
	    	LAG(grade) OVER (PARTITION BY com_id, ele_id ORDER BY school_year_start) AS prev_grade
	    FROM combined
	) subquery
	WHERE grade = prev_grade
	GROUP BY com_id, grade, school_year_start
),
-- Table containing students and their first and last appearance in the database in the range of years 2020-2023
appearance AS (
    SELECT com_id, ele_id, MIN(school_year_start) as first_year,
    	MAX(school_year_start) as last_year
    FROM combined
    GROUP BY com_id, ele_id
),
-- Calculate students immigrating per commune, year and grade
grade_immigrant AS (
	SELECT combined.com_id, school_year_start, grade, COUNT(*) AS number_immigrant
	FROM combined
	JOIN appearance ON combined.com_id = appearance.com_id AND combined.ele_id = appearance.ele_id
	WHERE combined.school_year_start = appearance.first_year
		AND combined.school_year_start > 2020
		AND combined.grade > 1
	GROUP BY combined.com_id, combined.grade, combined.school_year_start
),
-- Calculate students emigrating per commune, year and grade
grade_emigrant AS (
	SELECT combined.com_id, school_year_start, grade, COUNT(*) AS number_emigrant
	FROM combined
	JOIN appearance ON combined.com_id = appearance.com_id
		AND combined.ele_id = appearance.ele_id
	WHERE combined.school_year_start = appearance.last_year
		AND combined.school_year_start < 2023
		AND combined.grade < 13
	GROUP BY combined.com_id, combined.grade, combined.school_year_start
),
-- Calculate average per commune and grade of students repeating a grade
avg_grade_repeating AS (
	SELECT com_id, grade, AVG(number_repeating) AS avg_repeating
	FROM grade_repeating
	GROUP BY com_id, grade
),
-- Calculate average per commune and grade of students immigrating
avg_grade_immigrant AS (
	SELECT com_id, grade, AVG(number_immigrant) AS avg_immigrant
	FROM grade_immigrant
	GROUP BY com_id, grade
),
-- Calculate average per commune and grade of students emigrating
avg_grade_emigrant AS (
	SELECT com_id, grade, AVG(number_emigrant) AS avg_emigrant
	FROM grade_emigrant
	GROUP BY com_id, grade
),
-- Single table in which all the averages are included
combined_avgs AS (
	SELECT COALESCE(a.com_id, b.com_id, c.com_id) as com_id,
	       COALESCE(a.grade, b.grade, c.grade) as grade,
	       a.avg_repeating, b.avg_immigrant, c.avg_emigrant
	FROM avg_grade_repeating a
	FULL OUTER JOIN avg_grade_immigrant b ON a.com_id = b.com_id and a.grade = b.grade
	FULL OUTER JOIN avg_grade_emigrant c ON COALESCE(a.com_id, b.com_id) = c.com_id
		and COALESCE(a.grade, b.grade) = c.grade
	order by com_id asc, grade asc
),
-- Rearranges the table in order to make it more convenient to use now
combined_new as (
	select school_year_start, com_id, grade, count(*) as tot
	from combined
	where school_year_start > 2022
	group by 1, 2, 3
	order by 1, 2, 3
),
-- Computes the number of students from 2024 to 2029 according on the data of 2023 and the repetition and migration factors previously computed
combined_rows as (
	SELECT comb_a.com_id, comb_a.grade + 1 as grade,
		coalesce(lead(tot, 0) over (partition by comb.com_id order by comb.grade), 0)
		- COALESCE(avg_repeating, 0)
		+ COALESCE(LEAD(avg_immigrant) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)  
	    + COALESCE(LEAD(avg_repeating) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
	    - COALESCE(avg_emigrant, 0) AS tot2024,
	    coalesce(lead(tot, -1) over (partition by comb.com_id order by comb.grade), 0)
	    - COALESCE(LEAD(avg_repeating, -1) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
		+ COALESCE(LEAD(avg_immigrant) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)  
		+ COALESCE(LEAD(avg_immigrant, 0) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
	    + COALESCE(LEAD(avg_repeating, 1) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
	    - COALESCE(LEAD(avg_emigrant, -1) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
	    - COALESCE(avg_emigrant, 0) AS tot2025,
	    coalesce(lead(tot, -2) over (partition by comb.com_id order by comb.grade), 0)
	    - COALESCE(LEAD(avg_repeating, -2) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
		+ COALESCE(LEAD(avg_immigrant) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)  
		+ COALESCE(LEAD(avg_immigrant, 0) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
		+ COALESCE(LEAD(avg_immigrant, -1) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
	    + COALESCE(LEAD(avg_repeating, 1) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
	    - COALESCE(LEAD(avg_emigrant, -1) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
	    - COALESCE(LEAD(avg_emigrant, -2) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
	    - COALESCE(avg_emigrant, 0) AS tot2026,
	    coalesce(lead(tot, -3) over (partition by comb.com_id order by comb.grade), 0)
	    - COALESCE(LEAD(avg_repeating, -3) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
		+ COALESCE(LEAD(avg_immigrant) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)  
		+ COALESCE(LEAD(avg_immigrant, 0) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
		+ COALESCE(LEAD(avg_immigrant, -1) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
		+ COALESCE(LEAD(avg_immigrant, -2) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
	    + COALESCE(LEAD(avg_repeating, 1) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
	    - COALESCE(LEAD(avg_emigrant, -1) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
	    - COALESCE(LEAD(avg_emigrant, -2) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
	    - COALESCE(LEAD(avg_emigrant, -3) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
	    - COALESCE(avg_emigrant, 0) AS tot2027,
	    coalesce(lead(tot, -4) over (partition by comb.com_id order by comb.grade), 0)
	    - COALESCE(LEAD(avg_repeating, -4) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
		+ COALESCE(LEAD(avg_immigrant) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)  
		+ COALESCE(LEAD(avg_immigrant, 0) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
		+ COALESCE(LEAD(avg_immigrant, -1) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
		+ COALESCE(LEAD(avg_immigrant, -2) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
		+ COALESCE(LEAD(avg_immigrant, -3) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
	    + COALESCE(LEAD(avg_repeating, 1) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
	    - COALESCE(LEAD(avg_emigrant, -1) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
	    - COALESCE(LEAD(avg_emigrant, -2) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
	    - COALESCE(LEAD(avg_emigrant, -3) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
	    - COALESCE(LEAD(avg_emigrant, -4) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
	    - COALESCE(avg_emigrant, 0) AS tot2028,
	    coalesce(lead(tot, -5) over (partition by comb.com_id order by comb.grade), 0)
	    - COALESCE(LEAD(avg_repeating, -5) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
		+ COALESCE(LEAD(avg_immigrant) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)  
		+ COALESCE(LEAD(avg_immigrant, 0) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
		+ COALESCE(LEAD(avg_immigrant, -1) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
		+ COALESCE(LEAD(avg_immigrant, -2) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
		+ COALESCE(LEAD(avg_immigrant, -3) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
		+ COALESCE(LEAD(avg_immigrant, -4) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
	    + COALESCE(LEAD(avg_repeating, 1) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
	    - COALESCE(LEAD(avg_emigrant, -1) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
	    - COALESCE(LEAD(avg_emigrant, -2) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
	    - COALESCE(LEAD(avg_emigrant, -3) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
	    - COALESCE(LEAD(avg_emigrant, -4) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
	    - COALESCE(LEAD(avg_emigrant, -5) OVER (PARTITION BY comb_a.com_id ORDER BY comb_a.grade), 0)
	    - COALESCE(avg_emigrant, 0) AS tot2029
	FROM combined_avgs comb_a
	LEFT JOIN combined_new comb ON comb.com_id = comb_a.com_id AND comb.grade = comb_a.grade
	WHERE comb.grade < 14 and school_year_start > 2022
	GROUP BY comb_a.com_id, comb.com_id, comb_a.grade, comb.grade, comb.tot, avg_repeating, avg_immigrant, avg_emigrant
),
-- Reorganize table for the future and properly group its rows
future as (
	SELECT com_id, round(SUM(tot2024)) AS "2024", round(SUM(tot2025)) AS "2025", round(SUM(tot2026)) AS "2026", round(SUM(tot2027)) AS "2027", round(SUM(tot2028)) AS "2028", round(SUM(tot2029)) AS "2029"
	FROM combined_rows
	where grade between 7 and 13
	GROUP BY com_id
),
-- Table containing number of students in secundaire per commune and year between 2018 and 2023
sec2 as  (
	select
		case
			WHEN com_id IS NULL AND lower(unaccent(pays_res)) = 'luxembourg' THEN 0
        	WHEN com_id IS NULL AND lower(unaccent(pays_res)) = 'france' THEN 1
        	WHEN com_id IS NULL AND lower(unaccent(pays_res)) = 'allemagne' THEN 2
        	WHEN com_id IS NULL AND lower(unaccent(pays_res)) = 'belgique' THEN 3
        	ELSE COALESCE(com_id, 4)
        END as com_id, school_year_start, round(COUNT(*)) AS tot
	FROM dirty.pp_rentree_retro_2021
	WHERE school_year_start >= 2018 and niveau between 7 and 13
	GROUP BY 1, 2
),
-- Rearranges the data for students in secundaire
pivot_sec2 as (
	SELECT com_id,
		MAX(CASE WHEN school_year_start = 2018 THEN tot END) AS "2018",
		MAX(CASE WHEN school_year_start = 2019 THEN tot END) AS "2019",
		MAX(CASE WHEN school_year_start = 2020 THEN tot END) AS "2020",
		MAX(CASE WHEN school_year_start = 2021 THEN tot END) AS "2021",
		MAX(CASE WHEN school_year_start = 2022 THEN tot END) AS "2022",
		MAX(CASE WHEN school_year_start = 2023 THEN tot END) AS "2023"
	FROM sec2
	GROUP BY com_id
	ORDER BY com_id asc
),
-- Joins the historical data to the forecast for the future years in a single table
everything as (
	select ps.com_id, "2018", "2019", "2020", "2021", "2022", "2023", "2024", "2025", "2026", "2027", "2028", "2029"
	from pivot_sec2 ps
	inner join future fu on ps.com_id = fu.com_id
)
-- Merge with the commune names and print the results
select
	CASE
    	WHEN com_id = 1 THEN 'France'
    	WHEN com_id = 2 THEN 'Allemagne'
    	WHEN com_id = 3 THEN 'Belgique'
    	WHEN com_id = 4 THEN 'completely unknown'
    	WHEN com_id = 0 THEN 'unknown in Luxembourg'
    	ELSE nom
    END as commune, everything.*
from everything
left JOIN masterdata.com as com on com.id = everything.com_id;