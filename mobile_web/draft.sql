select m.uset_id, w.user_id
from mobile_p7 m
full outer join web_p7 w
on m.user_id=w.user_id;
