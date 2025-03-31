/* 
05. Super Users.

A company defines its super users as
those who have made at least two
transactions. 
Writing a query to return, for each user, the
date when they become a super user, ordered by oldest super users first.
Users who are not super users should
also be present in the table. */


CREATE TABLE USERS_P5 (
    USER_ID          INTEGER,
    PRODUCT_ID       INTEGER,
    TRANSACTION_DATE DATE
);
