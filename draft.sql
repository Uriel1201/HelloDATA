SELECT NAME || '(' || SUBSTR(OCCUPATION,1,1) || ')' FROM OCCUPATIONS; SELECT 'There are a total of' || PROF_C || OCCUPATION || 's.' FROM (SELECT OCCUPATION, COUNT(*) AS PROF_C FROM OCCUPATIONS GROUP BY OCCUPATION ORDER BY 2, 1);
/*
Enter your query here.
Please append a semicolon ";" at the end of the query and enter your query in a single line to avoid error.
*/
