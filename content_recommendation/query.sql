SELECT
    *
FROM
    FRIENDS_P6;


SELECT
    *
FROM
    LIKES_P6;


CREATE TABLE LIKES_FRIENDS_p6
    AS
        SELECT
            F.USER_ID,
            LI.PAGE_LIKES
        FROM
                 FRIENDS_P6 F
            INNER JOIN LIKES_P6 LI ON F.FRIEND = LI.USER_ID;
