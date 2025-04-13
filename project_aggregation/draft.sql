with beginning(
  start_project
) as (
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
  ), completion (
  end_project
) as (
SELECT
    end_date
FROM
    PROJECTS_P10
WHERE
    end_date NOT IN (
        SELECT
            start_DATE
        FROM
            PROJECTS_P10
    )
  ) select start_project, end_project
           from beginning, completion;
