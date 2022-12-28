-- course definition
CREATE TABLE course (
	cno varchar(40) NOT NULL,
	cname varchar(20) NOT NULL,
	tno varchar(20) NOT NULL,
	CONSTRAINT course_pk PRIMARY KEY (cno)
);

-- student definition
CREATE TABLE student (
	sno varchar(40) NOT NULL,
	sname varchar(20) NOT NULL,
	ssex varchar(20) NOT NULL DEFAULT '男'::character varying,
	sbirthday date NULL,
	sclass varchar(20) NULL,
	CONSTRAINT student_pk PRIMARY KEY (sno)
);

-- teacher definition
CREATE TABLE teacher (
	tno varchar(40) NOT NULL,
	tname varchar(20) NOT NULL,
	tsex varchar(20) NOT NULL DEFAULT '男'::character varying,
	tbirthday date NULL,
	prof varchar(20) NOT NULL,
	depart varchar(20) NULL,
	CONSTRAINT teacher_pk PRIMARY KEY (tno)
);

-- test definition
CREATE TABLE test (
	name varchar(20) NULL,
	created_time timestamp NULL,
	creator_id varchar(64) NULL,
	creator_name varchar(100) NULL,
	updated_time timestamp NULL,
	mender_id varchar(64) NULL,
	mender_name varchar(100) NULL,
	"type" varchar(10) NULL
);

CREATE INDEX test_name_idx ON test USING btree (name);

-- score definition
CREATE TABLE score (
	sno varchar(40) NOT NULL,
	cno varchar(40) NOT NULL,
	"degree" numeric(4, 1) NOT NULL,
	CONSTRAINT score_course_fk FOREIGN KEY (cno) REFERENCES course(cno),
	CONSTRAINT score_student_fk FOREIGN KEY (sno) REFERENCES student(sno)
);

CREATE INDEX score_cno_idx ON score USING btree (cno);
CREATE INDEX score_sno_idx ON score USING btree (sno);
