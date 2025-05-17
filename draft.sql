SELECT NAME || '(' || SUBSTR(OCCUPATION,1,1) || ')' FROM OCCUPATIONS; SELECT 'There are a total of' || COUNT(*) || OCCUPATION || 's.' FROM OCCUPATIONS GROUP BY OCCUPATION ORDER BY COUNT(*), OCCUPATION;
/*
Enter your query here.
Please append a semicolon ";" at the end of the query and enter your query in a single line to avoid error.
*/
