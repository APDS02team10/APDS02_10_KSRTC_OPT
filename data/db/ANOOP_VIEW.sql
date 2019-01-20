DROP VIEW `ANOOP_VIEW`;
CREATE VIEW `ANOOP_VIEW` 
   AS select 
        strftime('%Y', ETD_DATETIME)                    as ETD_YEAR ,
        strftime('%m', ETD_DATETIME)                    as ETD_MONTH, 
        strftime('%w', ETD_DATETIME)                    as ETD_DAY_OF_WEEK, 
        (strftime('%m', ETD_DATETIME) + 2)/3            as ETD_QUARTER, 
        (strftime('%H', '2019-01-01 01:03:03') +0)/1    as ETD_HOUR, 
        case strftime('%w', ETD_DATETIME) 
                when 0 or 6 then 'Y' else 'N' 
                end as IS_WEEKEND, 
        date(ETD_DATETIME)                              as ETD_DATE, 
        ETD_CUR_STOP_NAME, 
        ETD_DST_STOP_NAME, 
        ETD_ADULTS + ETD_CHILD                          as ETD_TICKETS_SOLD 
    
    from KSRCTC_BUS
    where ETD_TICKET_TYPE_DESCR <> 'STUDENT PAS';