
-- student definition
CREATE TABLE student (
	sno varchar(40) NOT NULL,
	sname varchar(20) NOT NULL,
	ssex varchar(20) NOT NULL,
	sbirthday date NULL,
	sclass varchar(20) NULL,
	CONSTRAINT student_pk PRIMARY KEY (sno)
);
