sql

CREATE TABLE customer_landing (
    customerName VARCHAR(150),
    email VARCHAR(150) NOT NULL,
    phone VARCHAR(25),
    birthDay DATE,
    serialNumber VARCHAR(36),
    registrationDate BIGINT,
    lastUpdateDate BIGINT,
    shareWithResearchAsOfDate BIGINT,
    shareWithPublicAsOfDate BIGINT,
    shareWithFriendsAsOfDate BIGINT
);