/*  
06. Content Recommendations

Writing a query to return page recommendations
to a social media user based on the
pages that their friends have liked, but
that they have not yet marked as liked. */


CREATE TABLE FRIENDS_P6 (
    USER_ID INTEGER,
    FRIEND  INTEGER
);


CREATE TABLE LIKES_P6 (
    USER_ID    INTEGER,
    PAGE_LIKES CHAR
)
