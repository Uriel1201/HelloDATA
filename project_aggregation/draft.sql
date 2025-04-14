/* 
10. Project Aggregation 

Writing a query to return the start and end
dates of each project, and the number of
days it took to complete. */

/* Querying original data*/
select * from projects_p10;

/* Returning the duration of each different project.*/
WITH BEGINNING ( START_PROJECT ) AS ( -- The start of each project 
    SELECT
        START_DATE
    FROM
        PROJECTS_P10
    WHERE
        START_DATE NOT IN (
            SELECT
                END_DATE
            FROM
                PROJECTS_P10
        )
), COMPLETION ( END_PROJECT ) AS ( -- The end of each project 
    SELECT
        END_DATE
    FROM
        PROJECTS_P10
    WHERE
        END_DATE NOT IN (
            SELECT
                START_DATE
            FROM
                PROJECTS_P10
        )
), TIMELINE (
    START_PROJECT,
    END_PROJECT
) AS ( -- The timeline of each project 
    SELECT
        START_PROJECT,
        MIN(END_PROJECT)
    FROM
        BEGINNING,
        COMPLETION
    WHERE
        START_PROJECT < END_PROJECT
    GROUP BY
        START_PROJECT
)
SELECT
    START_PROJECT,
    END_PROJECT,
    END_PROJECT - START_PROJECT AS PROJECT_DURATION
FROM
    TIMELINE
ORDER BY
    3;
