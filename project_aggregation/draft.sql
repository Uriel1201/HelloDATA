WITH BEGINNING ( START_PROJECT ) AS (
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
), COMPLETION ( END_PROJECT ) AS (
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
) timeline (
    start_project,
    end_project
)
SELECT
    START_PROJECT,
    min(END_PROJECT)
FROM
    BEGINNING,
    COMPLETION
where start_project < end_project
group by start_project;
