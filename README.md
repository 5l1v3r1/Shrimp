Shrimp
======

Shrimp is a library of user-defined functions for simplifying the programming in Hadoop and Pig.
</p>
</p>
### NVL:
NVL lets you replace null (returned as a blank) with a string in the results of a query. 
If expr1 is null, then NVL returns expr2. If expr1 is not null, then NVL returns expr1.

    DEFINE NVL com.ttech.shrimp.NVL();
    -- input:
    -- (1,,2)
    A = LOAD '/data/smp' USING PigStorage(',') as (f1:int, f2:chararray, f3:chararray);
    -- NVL(exp1, exp2)
    -- NVL2(string1, value_if_null )
    B = FOREACH A GENERATE NVL(f1, '-1') as f1, NVL(f2, '-1') as f2, NVL(f3, '-1') as f3;
    -- output:
    -- (1, -1, 2)
    dump B;


Source: http://docs.oracle.com/cd/B28359_01/server.111/b28286/functions110.htm

### NVL2:
NVL2 lets you determine the value returned by a query based on whether a specified expression is null or not null. 
If expr1 is not null, then NVL2 returns expr2. If expr1 is null, then NVL2 returns expr3.

    DEFINE NVL2 com.ttech.shrimp.NVL2();
    -- input:
    -- (1,,2)
    -- NVL2(string1, value_if_NOT_null, value_if_null )
    A = LOAD '/data/smp' USING PigStorage(',') as (f1:int, f2:chararray, f3:chararray);
    -- NVL2(exp1, exp2, exp3);
    -- NVL2(string1, value_if_NOT_null, value_if_null )
    B = FOREACH A GENERATE NVL2(f1, f1, '-1');
    -- output:
    -- (1)
    dump B;

Source: http://docs.oracle.com/cd/E11882_01/server.112/e26088/functions120.htm


### SQOOP_DATE_FORMAT:
To be able to load Oracle, date format must be "DD-MM-YYYY HH24:MI:SS".
SQOOP_DATE_FORMAT lets you to convert the format to be able to load to the Oracle.
Input format must be "YYYYMMDDHH24MISS"

    DEFINE SQOOP_DATE_FORMAT com.ttech.shrimp.SQOOP_DATE_FORMAT();
    -- input:
    -- (20130905225900)
    A = LOAD '/data/smp' USING PigStorage(',') as (f1:int, f2:chararray, f3:chararray);
    B = FOREACH A GENERATE SQOOP_DATE_FORMAT(f1);
    -- output:
    -- (2013-09-05 22:59:00)
    dump B;


## Working with the source code

Here are some common tasks when working with the source code.

### Build the JAR

	ant build-jar
	
