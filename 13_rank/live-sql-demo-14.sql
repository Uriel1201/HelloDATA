/* 
13. Rank Without Rank.

Writing a query to rank scores in the
following table without using a window
function. */

SELECT
    S1.ID,
    S1.SCORE,
    COUNT(DISTINCT S2.SCORE) AS RANK
FROM
    SCORES_P13 S1
    LEFT JOIN SCORES_P13 S2 ON S1.SCORE <= S2.SCORE
GROUP BY
    S1.ID,
    S1.SCORE
ORDER BY
    3;