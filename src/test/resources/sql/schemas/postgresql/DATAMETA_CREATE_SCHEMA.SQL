create table datameta(app varchar(38),ver INT4);

alter table datameta
   add constraint PK_DATAMETA primary key (app);
