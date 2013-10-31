-- NOTE: runscripts/hivePosteriors.sh prepares the prerequisites described below

-- the following files are assumed to be located in the current directory
-- credit.xdsl, credit.txt: network and data
-- smile-wide-0.0.1-SNAPSHOT.jar, smile-2013.08.01.jar: SMILE and SMILE-WIDE jars
-- libjsmile.so: the native SMILE library 


add file credit.xdsl;
add file libjsmile.so;
add jar smile-2013.08.01.jar;
add jar smile-wide-0.0.1-SNAPSHOT.jar;
create temporary function posteriors as 'smile.wide.hive.PosteriorsUDF';
create temporary function posteriorsT as 'smile.wide.hive.PosteriorsUDTF';

drop table credit;
create table credit(
SubjectID int,PaymentHistory string,WorkHistory string,Reliability string,Debit string,Income string,RatioDebInc string,Assets string,Worth string,Profession string,FutureIncome string,Age string,CreditWorthiness string
) row format delimited fields terminated by ' ';
load data local inpath 'credit.txt' overwrite into table credit;

set hive.semantic.analyzer.hook=smile.wide.hive.PosteriorsSemanticHook;

-- plain UDF
select age, income, posteriors('credit.xdsl', array('CreditWorthiness', 'reliability'), 'Age', age, 'Income', income) from credit;

-- plain UDF, the semantic hook provides support for easier syntax assuming match between column names and node ids (case-insensitive) 
select age, income, posteriors('credit.xdsl', array('CreditWorthiness', 'Reliability'), age, income) from credit;

-- UDTF: returns table
select age, income, node, outcome, prob from credit
lateral view posteriorsT('credit.xdsl', array('CreditWorthiness', 'Worth'), 'Age', age, 'Income', income) posteriorsTable as node, outcome, prob;

-- UDTF used for single subjectid 
select subjectid, node, outcome, prob from credit 
lateral view posteriorsT('credit.xdsl', array('CreditWorthiness', 'Worth'), 'Age', age, 'Income', income) posteriorsTable as node, outcome, prob
where credit.subjectid=1006;