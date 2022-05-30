--SELECT * FROM person where fullname LIKE '[A]*';

--How many different products seller does sell
explain analyse Select seller.sellerid as seller_id,
       person.nickname as nick,
        --эээээксперимеенты - память, паралелльный поиск в таблице
       Count(sellersproduct.productid)
           as number_Of_DifferentProducts

from seller

Join person on seller.sellerid = person.personid

INNER Join sellersproduct on seller.sellerid = sellersproduct.sellerid
GROUP BY person.nickname, seller.sellerid;


--How many sellers has each product
Select product.productname as product,
Count(sellersproduct.sellerid) as number_of_sellers

From product

Join sellersproduct on
product.productname = sellersproduct.productname
GROUP BY product.productname;

--How many sellers sell each product
Select product.productname as product,
ROUND(Avg(sellersproduct.priceofproduct),2) as overagePrice

From product

Join sellersproduct on
product.productname = sellersproduct.productname
GROUP BY product.productname;


--How much money has each buyer sold
Select buyer.buyerid, person.nickname as nickname, sum(totalsum)

From buyer

Join person on person.personid = buyerid

JOIN productlist p on buyer.buyerid = p.buyerid

GROUP BY buyer.buyerid, person.nickname;

--Mostly popular sellers with the value of chats
Select seller.sellerid, count(channelid) as numberOfChats

From seller

Join communicationchannel on seller.sellerid = communicationchannel.sellerid
GROUP BY seller.sellerid
ORDER BY numberOfChats DESC;

select personid as person_id,
       person.nickname as nick,
       count(channelid) as total

from person
Join communicationchannel on personid = communicationchannel.sellerid OR personid = communicationchannel.buyerid
group by personid,nick
order by total desc;


SELECT buyer.buyerid, person.nickname as nickname, communicationchannel.channelid, communicationchannel.moderatorid

FROM buyer

JOIN person on person.personid = buyerid
JOIN communicationchannel on buyer.buyerid = communicationchannel.buyerid
JOIN stuff on communicationchannel.moderatorid = stuff.stuffid

GROUP BY buyer.buyerid, person.nickname, communicationchannel.channelid;
--RETURNING  billId INTO ;
select * from  get_();
--RETURNS TRIGGER AS $update_currency_exchange$
call update_exchange_for_currency();

CALL create_deal_from_productlist(11,4,'hb');

select get_number_of('buyers');

select get_deals_for_period(timestamp'2004-10-19 10:23:54+02',timestamp'2024-10-19 10:23:54+02');


--найти покупателя, потратившего на форуме больше всех денег на bread   и bread knife    в USD Coin

SELECT productid, productprice from buyedproduct
where productprice = (SELECT MAX(productprice) FROM buyedproduct);




CREATE PROCEDURE create_all_indexes()
language plpgsql
as $$ begin
    create index list on deal(listid);
    create index code on wallet(currencycode);
    create index buy on delivery(deliveryid);
    CREATE INDEX prod1 ON sellersproduct(productid);
    CREATE INDEX prod2 ON sellersproduct(productname);
    create index cur on currency(currencyid);
end;
$$;

CREATE PROCEDURE drop_all_indexes()
language plpgsql
as $$ begin
    drop index buy;
    drop index prod1;
    drop index prod2;
    drop index list;
    drop index code;
    drop index cur;
end;
$$;

drop index cur2;
create index cur2 on historicalcurrencyexchange(currencyfrom, currencyto);

SELECT
    tablename,
    indexname,
    indexdef
FROM
    pg_indexes;


CREATE INDEX contract1 ON smartcontract(contractid);
CREATE INDEX wall2 ON smartcontract(walletid);
CREATE INDEX contract2 ON deal(contractid);
drop index wall1;
drop INDEX wall2 ;
drop INDEX contract1 ;
drop INDEX contract2;


-- CREATE INDEX list1 ON deal(listid);
-- drop index list1;
-- create index wall1 on wallet(walletid);
-- drop index wall1;


SET max_parallel_workers_per_gather TO default;
SET enable_seqscan = off; --принуд вкл/выкл последовательное считываение таблицы и соответсвенно выкл/вкл использования индексов

call create_all_indexes();
call drop_all_indexes();
--without indexes and replaced where = 48.798ms
--with replacement where = 46.993ms
----with all of it = 12.873ms
EXPLAIN ANALYSE
WITH BuyersOfBreadAndBreadKnivesSpentMaxSum as (SELECT SUM(priceofproduct*numberofproducts) as totalSum, deliveryid as buyerId
               from Delivery
               where deliveryid IN (SELECT productid
                                   from sellersproduct
                                   --where productname IN ('bread','bread knife'))
                                   ---replace where productname IN ('bread','bread knife') to:
                                   where productname = 'bread' or productname = 'bread knife')
               and EXISTS(select contractid
                          from Deal
                          WHERE listid = dellistid AND EXISTS(
                                                              select contractid
                                                              from smartcontract
                                                              where deal.contractid
                                                                        = smartcontract.contractid
                                                                and exists(
                                                                    select walletid
                                                                    from wallet
                                                                    where wallet.walletid
                                                                              =
                                                                          smartcontract.walletid
                                                                      and currencycode = 0))
                   )
               group by buyerId
)
select *,(select totalSum*value
          from historicalcurrencyexchange
          where currencyfrom = 0 and currencyto = 4
          order by dateandtime desc
          limit 1) from BuyersOfBreadAndBreadKnivesSpentMaxSum
-- having max(totalSum) >= 1000
WHERE totalSum >= (SELECT max(totalSum) from BuyersOfBreadAndBreadKnivesSpentMaxSum)-1400
ORDER BY totalSum desc;



EXPLAIN ANALYSE
WITH BuyersOfBreadAndBreadKnivesSpentMaxSum as (SELECT SUM(priceofproduct*numberofproducts) as totalSum, deliveryid as buyerId
               from Delivery
               where deliveryid IN (SELECT productid
                                   from sellersproduct
                                   -- replace where productname IN ('bread','bread knife') to:
                                   where productname = 'bread' or productname = 'bread knife')
               and EXISTS(select s.contractid
                          from smartcontract join (select contractid from deal  WHERE listid = dellistid) s on s.contractid = smartcontract.contractid
                          join wallet w on smartcontract.walletid = w.walletid
                          WHERE currencycode = 0
                   )
               group by buyerId
)
select * from BuyersOfBreadAndBreadKnivesSpentMaxSum
WHERE totalSum >= (SELECT max(totalSum) from BuyersOfBreadAndBreadKnivesSpentMaxSum)-1400
ORDER BY totalSum desc;





CREATE PROCEDURE create_all_deals()
LANGUAGE plpgsql
AS $$
    DECLARE
        buyer lineinproductlist;
    BEGIN
        FOR buyer IN
                SELECT * FROM LineInProductList
            LOOP
                CALL create_deal_from_productlist(buyer.listid,cast(random()*35+1 as integer),'hno comment');
            END LOOP;
    end;
    $$;

call create_all_deals();

CREATE PROCEDURE finish_some_payments()
LANGUAGE plpgsql
AS $$
    DECLARE
        iter refcursor;
        rec record;
    BEGIN
        FOR rec IN (SELECT billid as bid FROM paymentinvoice WHERE paymentstatus ='started')
        LOOP
            update paymentinvoice set paymentstatus = 'finished' where billid = rec.bid;
        END LOOP;
    end;
$$;

call finish_some_payments();

update paymentinvoice set paymentstatus = 'finished' where billid = 10537;

-- create index combine on deal(listid,contractid);
-- drop index combine;


call  drop_all_indexes();
call create_all_indexes();

set work_mem ='20MB';
set enable_hashagg = off;
set enable_hashjoin =off;
set enable_bitmapscan = on;
set enable_mergejoin  = on;
set enable_sort =on;
set enable_seqscan =off;

--without indexes and replaced where = 38.325ms
--with replacement where = 24.708 ms
----with all of it = 5.064ms
EXPLAIN ANALYSE
WITH BuyersOfBreadAndBreadKnivesSpentMaxSum as (SELECT SUM(delivery.priceofproduct*numberofproducts) as totalSum, deliveryid as buyerId
               from Delivery
                   join deal on deal.listid = dellistid
                   join smartcontract s on s.contractid = deal.contractid
                   join wallet w on w.walletid = s.walletid
                   join sellersproduct s2 on delivery.deliveryid = s2.productid
               where currencycode = 0 and
                   --productname IN ('bread','bread knife')
                   productname = 'bread' or productname = 'bread knife'
               group by buyerId
)
select *,(select totalSum*value
          from historicalcurrencyexchange
          where currencyfrom = 0 and currencyto = 4
          order by dateandtime desc
          limit 1)
from BuyersOfBreadAndBreadKnivesSpentMaxSum
WHERE totalSum >= (SELECT max(totalSum) from BuyersOfBreadAndBreadKnivesSpentMaxSum)-1400
ORDER BY totalSum desc;

--можно добавить в запрос, что пользователь совершил не менее 5 сделок