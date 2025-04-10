select m.uset_id, w.user_id
from mobile_p7 m
full outer join web_p7 w
on m.user_id=w.user_id;

SELECT
    M.USER_ID AS MO,
    W.USER_ID AS WE
FROM
    MOBILE_P7 M
    FULL OUTER JOIN WEB_P7    W ON M.USER_ID = W.USER_ID;
